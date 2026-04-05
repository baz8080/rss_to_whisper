package com.rsstowhisper.indexing

import com.rsstowhisper.util.getHash
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals

class IndexingServiceTest {
    private fun writeEpisodeJson(
        episodeDir: Path,
        transcript: String?,
        extra: Map<String, Any?> = emptyMap(),
    ) {
        Files.createDirectories(episodeDir)
        val fields =
            buildMap {
                put("episode_title", "Title")
                put("podcast_title", "Pod")
                if (transcript != null) put("episode_transcript", transcript)
                putAll(extra)
            }
        val json =
            fields.entries.joinToString(prefix = "{", postfix = "}") { (k, v) ->
                val value =
                    when (v) {
                        null -> "null"
                        is String -> "\"${v.replace("\"", "\\\"")}\""
                        is Number, is Boolean -> v.toString()
                        is List<*> -> v.joinToString(prefix = "[", postfix = "]") { "\"$it\"" }
                        else -> "\"$v\""
                    }
                "\"$k\":$value"
            }
        Files.writeString(episodeDir.resolve("transcript.json"), json)
    }

    private fun countRows(dbPath: String): Int {
        java.sql.DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("SELECT COUNT(*) FROM episodes")
                rs.next()
                return rs.getInt(1)
            }
        }
    }

    private fun selectAll(dbPath: String): List<Map<String, Any?>> {
        java.sql.DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("SELECT * FROM episodes")
                val cols = (1..rs.metaData.columnCount).map { rs.metaData.getColumnName(it) }
                val rows = mutableListOf<Map<String, Any?>>()
                while (rs.next()) {
                    rows.add(cols.associateWith { rs.getObject(it) })
                }
                return rows
            }
        }
    }

    @Test
    fun `indexAll logs and returns when data dir does not exist`(
        @TempDir tempDir: Path,
    ) {
        val dbPath = tempDir.resolve("db.sqlite").toAbsolutePath().toString()
        SqliteService(dbPath).use { svc ->
            IndexingService(svc).indexAll(tempDir.resolve("nonexistent").toString())
            assertEquals(0, countRows(dbPath))
        }
    }

    @Test
    fun `indexAll finds episodes across podcast directories`(
        @TempDir dataDir: Path,
    ) {
        val ep1 = dataDir.resolve("podA/ep1")
        val ep2 = dataDir.resolve("podA/ep2")
        val ep3 = dataDir.resolve("podB/epX")
        writeEpisodeJson(ep1, "transcript one")
        writeEpisodeJson(ep2, "transcript two")
        writeEpisodeJson(ep3, "transcript three")

        val dbPath = dataDir.resolve("db.sqlite").toAbsolutePath().toString()
        SqliteService(dbPath).use { svc ->
            IndexingService(svc).indexAll(dataDir.toString())
        }

        assertEquals(3, countRows(dbPath))
    }

    @Test
    fun `indexAll assigns id as hash of transcript`(
        @TempDir dataDir: Path,
    ) {
        writeEpisodeJson(dataDir.resolve("pod/ep"), "unique transcript body")

        val dbPath = dataDir.resolve("db.sqlite").toAbsolutePath().toString()
        SqliteService(dbPath).use { svc ->
            IndexingService(svc).indexAll(dataDir.toString())
        }

        val rows = selectAll(dbPath)
        assertEquals(1, rows.size)
        assertEquals(getHash("unique transcript body"), rows[0]["id"])
    }

    @Test
    fun `indexAll skips episode dirs without transcript_json`(
        @TempDir dataDir: Path,
    ) {
        Files.createDirectories(dataDir.resolve("pod/empty-ep"))
        writeEpisodeJson(dataDir.resolve("pod/good-ep"), "text")

        val dbPath = dataDir.resolve("db.sqlite").toAbsolutePath().toString()
        SqliteService(dbPath).use { svc ->
            IndexingService(svc).indexAll(dataDir.toString())
        }

        assertEquals(1, countRows(dbPath))
    }

    @Test
    fun `indexAll skips episodes with empty or missing transcripts`(
        @TempDir dataDir: Path,
    ) {
        writeEpisodeJson(dataDir.resolve("pod/empty-transcript"), "")
        writeEpisodeJson(dataDir.resolve("pod/missing-transcript"), null)
        writeEpisodeJson(dataDir.resolve("pod/good"), "has text")

        val dbPath = dataDir.resolve("db.sqlite").toAbsolutePath().toString()
        SqliteService(dbPath).use { svc ->
            IndexingService(svc).indexAll(dataDir.toString())
        }

        val rows = selectAll(dbPath)
        assertEquals(1, rows.size)
        assertEquals("has text", rows[0]["episode_transcript"])
    }

    @Test
    fun `indexAll ignores invalid json without crashing`(
        @TempDir dataDir: Path,
    ) {
        val bad = dataDir.resolve("pod/bad")
        Files.createDirectories(bad)
        Files.writeString(bad.resolve("transcript.json"), "{not valid json")
        writeEpisodeJson(dataDir.resolve("pod/good"), "real text")

        val dbPath = dataDir.resolve("db.sqlite").toAbsolutePath().toString()
        SqliteService(dbPath).use { svc ->
            IndexingService(svc).indexAll(dataDir.toString())
        }

        assertEquals(1, countRows(dbPath))
    }

    @Test
    fun `indexAll skips non-directory entries under data dir`(
        @TempDir dataDir: Path,
    ) {
        Files.writeString(dataDir.resolve("stray.txt"), "not a dir")
        writeEpisodeJson(dataDir.resolve("pod/ep"), "text")

        val dbPath = dataDir.resolve("db.sqlite").toAbsolutePath().toString()
        SqliteService(dbPath).use { svc ->
            IndexingService(svc).indexAll(dataDir.toString())
        }

        assertEquals(1, countRows(dbPath))
    }
}

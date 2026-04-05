package com.rsstowhisper.indexing

import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SqliteServiceTest {
    private fun newService(): Pair<SqliteService, String> {
        val dbFile = kotlin.io.path.createTempFile("podcasts-test", ".db").toAbsolutePath().toString()
        return SqliteService(dbFile) to dbFile
    }

    private fun sampleEpisode(
        id: String,
        title: String = "Ep",
        transcript: String = "hello world",
        collections: Any? = listOf("daily", "news"),
        authors: Any? = listOf("Alice", "Bob"),
        tags: Any? = listOf("science", "tech"),
        episodeNumber: Any? = 3,
        duration: Any? = 1800,
    ): Map<String, Any?> =
        mapOf(
            "_id" to id,
            "podcast_title" to "Pod",
            "podcast_link" to "https://pod",
            "podcast_language" to "en",
            "podcast_copyright" to "c",
            "podcast_author" to "Author",
            "podcast_image" to "https://img",
            "podcast_type" to "episodic",
            "podcast_collections" to collections,
            "episode_title" to title,
            "episode_published_on" to "2024-01-01",
            "episode_audio_link" to "https://cdn/a.mp3",
            "episode_web_link" to "https://pod/ep",
            "episode_image" to "https://img/e.jpg",
            "episode_summary" to "summary",
            "episode_subtitle" to "subtitle",
            "episode_authors" to authors,
            "episode_number" to episodeNumber,
            "episode_season" to 1,
            "episode_type" to "full",
            "episode_duration" to duration,
            "episode_transcript" to transcript,
            "episode_relative_mp3_path" to "pod/ep/audio.mp3",
            "all_tags" to tags,
        )

    private fun readAllRows(dbPath: String): List<Map<String, Any?>> {
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("SELECT * FROM episodes ORDER BY id")
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
    fun `bulkInsert empty list is no-op`() {
        val (svc, path) = newService()
        svc.use { it.bulkInsert(emptyList()) }
        assertEquals(0, readAllRows(path).size)
    }

    @Test
    fun `bulkInsert writes episodes and joins list fields`() {
        val (svc, path) = newService()
        svc.use {
            it.bulkInsert(listOf(sampleEpisode(id = "abc", title = "First")))
        }

        val rows = readAllRows(path)
        assertEquals(1, rows.size)
        val row = rows[0]
        assertEquals("abc", row["id"])
        assertEquals("First", row["episode_title"])
        assertEquals("daily, news", row["podcast_collections"])
        assertEquals("Alice, Bob", row["episode_authors"])
        assertEquals("science, tech", row["all_tags"])
        assertEquals(3, (row["episode_number"] as Number).toInt())
        assertEquals(1800, (row["episode_duration"] as Number).toInt())
    }

    @Test
    fun `bulkInsert stores null when list fields are missing or not a list`() {
        val (svc, path) = newService()
        svc.use {
            it.bulkInsert(
                listOf(
                    sampleEpisode(id = "x", collections = null, authors = "not a list", tags = emptyList<String>()),
                ),
            )
        }

        val row = readAllRows(path).single()
        assertNull(row["podcast_collections"])
        assertNull(row["episode_authors"]) // non-list value becomes null
        assertEquals("", row["all_tags"]) // empty list joins to empty string
    }

    @Test
    fun `bulkInsert filters null elements from lists`() {
        val (svc, path) = newService()
        svc.use {
            it.bulkInsert(
                listOf(
                    sampleEpisode(id = "y", tags = listOf("one", null, "three")),
                ),
            )
        }
        assertEquals("one, three", readAllRows(path).single()["all_tags"])
    }

    @Test
    fun `bulkInsert replaces rows with same id`() {
        val (svc, path) = newService()
        svc.use {
            it.bulkInsert(listOf(sampleEpisode(id = "dup", title = "Original")))
            it.bulkInsert(listOf(sampleEpisode(id = "dup", title = "Updated")))
        }

        val rows = readAllRows(path)
        assertEquals(1, rows.size)
        assertEquals("Updated", rows[0]["episode_title"])
    }

    @Test
    fun `bulkInsert populates fts index for transcript search`() {
        val (svc, path) = newService()
        svc.use {
            it.bulkInsert(
                listOf(
                    sampleEpisode(id = "a", title = "Astronomy", transcript = "the planet mars is red"),
                    sampleEpisode(id = "b", title = "Biology", transcript = "cells divide"),
                ),
            )
        }

        DriverManager.getConnection("jdbc:sqlite:$path").use { conn ->
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("SELECT rowid FROM episodes_fts WHERE episodes_fts MATCH 'mars'")
                assertTrue(rs.next())
                val hitRowId = rs.getLong("rowid")
                assertTrue(!rs.next())

                val rs2 = stmt.executeQuery("SELECT episode_title FROM episodes WHERE rowid = $hitRowId")
                assertTrue(rs2.next())
                assertEquals("Astronomy", rs2.getString("episode_title"))
            }
        }
    }

    @Test
    fun `rebuildIndex clears existing data`() {
        val (svc, path) = newService()
        svc.use {
            it.bulkInsert(listOf(sampleEpisode(id = "a")))
            assertEquals(1, readAllRows(path).size)
            it.rebuildIndex()
            assertEquals(0, readAllRows(path).size)
        }
    }
}

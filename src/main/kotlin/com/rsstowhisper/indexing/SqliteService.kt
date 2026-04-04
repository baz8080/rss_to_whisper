package com.rsstowhisper.indexing

import org.slf4j.LoggerFactory
import java.io.Closeable
import java.sql.Connection
import java.sql.DriverManager

class SqliteService(dbPath: String) : Closeable {
    private val logger = LoggerFactory.getLogger(SqliteService::class.java)
    private val connection: Connection = DriverManager.getConnection("jdbc:sqlite:$dbPath")

    init {
        connection.createStatement().use { stmt ->
            stmt.execute("PRAGMA journal_mode=WAL")
            stmt.execute("PRAGMA foreign_keys=ON")
        }
        createTables()
    }

    private fun createTables() {
        connection.createStatement().use { stmt ->
            stmt.execute(
                """
                CREATE TABLE IF NOT EXISTS episodes (
                    id TEXT PRIMARY KEY,
                    podcast_title TEXT,
                    podcast_link TEXT,
                    podcast_language TEXT,
                    podcast_copyright TEXT,
                    podcast_author TEXT,
                    podcast_image TEXT,
                    podcast_type TEXT,
                    podcast_collections TEXT,
                    episode_title TEXT,
                    episode_published_on TEXT,
                    episode_audio_link TEXT,
                    episode_web_link TEXT,
                    episode_image TEXT,
                    episode_summary TEXT,
                    episode_subtitle TEXT,
                    episode_authors TEXT,
                    episode_number INTEGER,
                    episode_season INTEGER,
                    episode_type TEXT,
                    episode_duration INTEGER,
                    episode_transcript TEXT,
                    episode_relative_mp3_path TEXT,
                    all_tags TEXT
                )
                """,
            )

            stmt.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS episodes_fts USING fts5(
                    episode_title,
                    episode_transcript,
                    podcast_title,
                    all_tags,
                    content='episodes',
                    content_rowid='rowid'
                )
                """,
            )
        }
    }

    fun rebuildIndex() {
        logger.info("Rebuilding FTS index")
        connection.createStatement().use { stmt ->
            stmt.execute("DROP TABLE IF EXISTS episodes")
            stmt.execute("DROP TABLE IF EXISTS episodes_fts")
        }
        createTables()
    }

    fun bulkInsert(episodes: List<Map<String, Any?>>) {
        if (episodes.isEmpty()) return

        connection.autoCommit = false
        try {
            val sql =
                """
                INSERT OR REPLACE INTO episodes (
                    id, podcast_title, podcast_link, podcast_language, podcast_copyright,
                    podcast_author, podcast_image, podcast_type, podcast_collections,
                    episode_title, episode_published_on, episode_audio_link, episode_web_link,
                    episode_image, episode_summary, episode_subtitle, episode_authors,
                    episode_number, episode_season, episode_type, episode_duration,
                    episode_transcript, episode_relative_mp3_path, all_tags
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

            connection.prepareStatement(sql).use { stmt ->
                for (episode in episodes) {
                    stmt.setString(1, episode["_id"] as? String)
                    stmt.setString(2, episode["podcast_title"] as? String)
                    stmt.setString(3, episode["podcast_link"] as? String)
                    stmt.setString(4, episode["podcast_language"] as? String)
                    stmt.setString(5, episode["podcast_copyright"] as? String)
                    stmt.setString(6, episode["podcast_author"] as? String)
                    stmt.setString(7, episode["podcast_image"] as? String)
                    stmt.setString(8, episode["podcast_type"] as? String)
                    stmt.setString(9, joinList(episode["podcast_collections"]))
                    stmt.setString(10, episode["episode_title"] as? String)
                    stmt.setString(11, episode["episode_published_on"] as? String)
                    stmt.setString(12, episode["episode_audio_link"] as? String)
                    stmt.setString(13, episode["episode_web_link"] as? String)
                    stmt.setString(14, episode["episode_image"] as? String)
                    stmt.setString(15, episode["episode_summary"] as? String)
                    stmt.setString(16, episode["episode_subtitle"] as? String)
                    stmt.setString(17, joinList(episode["episode_authors"]))
                    stmt.setObject(18, episode["episode_number"])
                    stmt.setObject(19, episode["episode_season"])
                    stmt.setString(20, episode["episode_type"] as? String)
                    stmt.setObject(21, episode["episode_duration"])
                    stmt.setString(22, episode["episode_transcript"] as? String)
                    stmt.setString(23, episode["episode_relative_mp3_path"] as? String)
                    stmt.setString(24, joinList(episode["all_tags"]))
                    stmt.addBatch()
                }
                stmt.executeBatch()
            }

            // Rebuild FTS index from content table
            connection.createStatement().use { stmt ->
                stmt.execute("INSERT INTO episodes_fts(episodes_fts) VALUES('rebuild')")
            }

            connection.commit()
            logger.info("Inserted ${episodes.size} episodes")
        } catch (e: Exception) {
            connection.rollback()
            throw e
        } finally {
            connection.autoCommit = true
        }
    }

    override fun close() {
        connection.close()
    }

    private fun joinList(value: Any?): String? {
        val list = value as? List<*> ?: return null
        return list.filterNotNull().joinToString(", ")
    }
}

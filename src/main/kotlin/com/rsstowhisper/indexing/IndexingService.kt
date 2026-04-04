package com.rsstowhisper.indexing

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.rsstowhisper.util.getHash
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.isDirectory

class IndexingService(private val sqliteService: SqliteService) {
    private val logger = LoggerFactory.getLogger(IndexingService::class.java)
    private val jsonMapper =
        ObjectMapper().registerModule(KotlinModule.Builder().build())

    fun indexAll(dataDir: String) {
        val dataPath = Path.of(dataDir)
        if (!Files.isDirectory(dataPath)) {
            logger.error("Data directory does not exist: $dataDir")
            return
        }

        val episodes = mutableListOf<Map<String, Any?>>()

        Files.list(dataPath).use { podcastDirs ->
            for (podcastDir in podcastDirs) {
                if (!podcastDir.isDirectory()) continue

                Files.list(podcastDir).use { episodeDirs ->
                    for (episodeDir in episodeDirs) {
                        if (!episodeDir.isDirectory()) continue

                        val jsonFile = episodeDir.resolve("transcript.json")
                        if (!Files.exists(jsonFile)) continue

                        try {
                            val episode: MutableMap<String, Any?> =
                                jsonMapper.readValue(Files.readString(jsonFile))

                            val transcript = episode["episode_transcript"] as? String
                            if (transcript.isNullOrEmpty()) continue

                            episode["_id"] = getHash(transcript)
                            episodes.add(episode)
                        } catch (e: Exception) {
                            logger.error("Failed to read $jsonFile: ${e.message}")
                        }
                    }
                }
            }
        }

        logger.info("Found ${episodes.size} episodes to index")
        sqliteService.bulkInsert(episodes)
    }
}

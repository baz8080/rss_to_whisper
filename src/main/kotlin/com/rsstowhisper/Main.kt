package com.rsstowhisper

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import com.rsstowhisper.config.AppConfig
import com.rsstowhisper.indexing.IndexingService
import com.rsstowhisper.indexing.SqliteService
import com.rsstowhisper.pipeline.PodcastPipeline
import org.slf4j.LoggerFactory
import java.nio.file.Path

fun main(args: Array<String>) {
    val configFile =
        args.find { it == "-c" || it == "--config" }
            ?.let { args[args.indexOf(it) + 1] }
            ?: "pods.yaml"

    println("Using $configFile config")

    val config: AppConfig =
        try {
            AppConfig.load(configFile)
        } catch (e: Exception) {
            println("Cannot read configuration file: ${e.message}")
            return
        }

    // Configure logging level
    if (config.verbose) {
        val loggerContext = LoggerFactory.getILoggerFactory() as LoggerContext
        loggerContext.getLogger("com.rsstowhisper").level = Level.DEBUG
    }

    val mode = args.firstOrNull { it == "--transcribe" || it == "--index" } ?: "--transcribe"

    when (mode) {
        "--transcribe" -> {
            val pipeline = PodcastPipeline(config)
            pipeline.run()
        }
        "--index" -> {
            val dbPath = Path.of(config.dataDirectory, "podcasts.db").toString()
            SqliteService(dbPath).use { sqlite ->
                if ("--rebuild" in args) {
                    sqlite.rebuildIndex()
                }
                val indexingService = IndexingService(sqlite)
                indexingService.indexAll(config.dataDirectory)
            }
        }
    }
}

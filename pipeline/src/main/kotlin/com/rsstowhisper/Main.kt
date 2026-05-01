package com.rsstowhisper

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import com.rsstowhisper.pipeline.PodcastPipeline
import org.slf4j.LoggerFactory

fun main() {
    val config: AppConfig =
        try {
            AppConfig.load()
        } catch (e: Exception) {
            println("Cannot load configuration: ${e.message}")
            return
        }

    if (config.verbose) {
        val loggerContext = LoggerFactory.getILoggerFactory() as LoggerContext
        loggerContext.getLogger("com.rsstowhisper").level = Level.DEBUG
    }

    val pipeline = PodcastPipeline(config)
    pipeline.run()
}

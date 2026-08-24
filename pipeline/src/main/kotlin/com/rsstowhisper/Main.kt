package com.rsstowhisper

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import com.rsstowhisper.pipeline.PodcastPipeline
import org.slf4j.LoggerFactory

fun main(argv: Array<String>) {
    val args: Args =
        try {
            parseArgs(argv)
        } catch (e: IllegalStateException) {
            println(e.message)
            return
        }

    if (args.help) {
        println(USAGE)
        return
    }

    val config: AppConfig =
        try {
            AppConfig.load(args)
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

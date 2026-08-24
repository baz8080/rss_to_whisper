package com.rsstowhisper

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import com.rsstowhisper.pipeline.PodcastPipeline
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

fun main(argv: Array<String>) {
    val args: Args =
        try {
            parseArgs(argv)
        } catch (e: IllegalStateException) {
            System.err.println(e.message)
            exitProcess(2)
        }

    if (args.help) {
        println(USAGE)
        return
    }

    val config: AppConfig =
        try {
            AppConfig.load(args)
        } catch (e: Exception) {
            System.err.println("Cannot load configuration: ${e.message}")
            exitProcess(1)
        }

    if (config.verbose) {
        val loggerContext = LoggerFactory.getILoggerFactory() as LoggerContext
        loggerContext.getLogger("com.rsstowhisper").level = Level.DEBUG
    }

    val pipeline = PodcastPipeline(config)
    if (!pipeline.run()) {
        exitProcess(1)
    }
}

package com.rsstowhisper

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import com.rsstowhisper.pipeline.PodcastPipeline
import org.slf4j.LoggerFactory

fun main(args: Array<String>) {
    val env = AppConfig.loadDotEnv()
    val configFile =
        args.find { it == "-c" || it == "--config" }
            ?.let { args[args.indexOf(it) + 1] }
            ?: env["PIPELINE_CONFIG_PATH"]
            ?: run {
                println("No config file specified. Use --config or set PIPELINE_CONFIG_PATH in .env")
                return
            }

    println("Using $configFile config")

    val config: AppConfig =
        try {
            AppConfig.load(configFile)
        } catch (e: Exception) {
            println("Cannot read configuration file: ${e.message}")
            return
        }

    if (config.verbose) {
        val loggerContext = LoggerFactory.getILoggerFactory() as LoggerContext
        loggerContext.getLogger("com.rsstowhisper").level = Level.DEBUG
    }

    val pipeline = PodcastPipeline(config)
    pipeline.run()
}

package com.rsstowhisper

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.filter.ThresholdFilter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import ch.qos.logback.core.rolling.RollingFileAppender
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

private const val LOG_PATTERN = "%d{yyyy-MM-dd HH:mm:ss} - %level - %logger{36} - %msg%n"
private const val MAX_HISTORY_DAYS = 14

/** Running WARN and ERROR totals, so a multi-hour run can say whether anything went wrong. */
class RunTally : AppenderBase<ILoggingEvent>() {
    private val warnings = AtomicInteger()
    private val errors = AtomicInteger()

    override fun append(event: ILoggingEvent) {
        when (event.level.toInt()) {
            Level.WARN_INT -> warnings.incrementAndGet()
            Level.ERROR_INT -> errors.incrementAndGet()
        }
    }

    fun summary(logPath: Path?): String {
        val counts = "Run finished: ${warnings.get()} warnings, ${errors.get()} errors"
        return if (logPath != null) "$counts. See $logPath" else counts
    }
}

/**
 * Mirror WARN and above into `<dataDirectory>/logs/pipeline-errors.log`.
 *
 * Attached in code rather than logback.xml so the file lands beside the data and two
 * instances pointed at different data directories do not fight over one file.
 */
fun installErrorLog(dataDirectory: String): Path? {
    val context = LoggerFactory.getILoggerFactory() as LoggerContext

    // createDirectories would create the data directory too, and an unmounted volume or a
    // mistyped --data-dir has to stay the error the run refuses to start on.
    if (!Files.isDirectory(Path.of(dataDirectory))) {
        System.err.println("No error log: the data directory $dataDirectory does not exist")
        return null
    }

    val logPath =
        try {
            val logDir = Path.of(dataDirectory).resolve("logs")
            Files.createDirectories(logDir)
            logDir.resolve("pipeline-errors.log")
        } catch (e: Exception) {
            System.err.println("Could not create the log directory under $dataDirectory: ${e.message}")
            return null
        }

    val fileEncoder =
        PatternLayoutEncoder().apply {
            this.context = context
            pattern = LOG_PATTERN
            start()
        }

    val appender =
        RollingFileAppender<ILoggingEvent>().apply {
            this.context = context
            file = logPath.toString()
            encoder = fileEncoder
            addFilter(
                ThresholdFilter().apply {
                    this.context = context
                    setLevel(Level.WARN.toString())
                    start()
                },
            )
        }

    val policy =
        TimeBasedRollingPolicy<ILoggingEvent>().apply {
            this.context = context
            fileNamePattern = logPath.resolveSibling("pipeline-errors.%d{yyyy-MM-dd}.log").toString()
            maxHistory = MAX_HISTORY_DAYS
            setParent(appender)
            start()
        }

    appender.rollingPolicy = policy

    return try {
        appender.start()
        context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).addAppender(appender)
        logPath
    } catch (e: Exception) {
        System.err.println("Could not open the error log at $logPath: ${e.message}")
        null
    }
}

/** Attach the run tally to the root logger and return it. */
fun installRunTally(): RunTally {
    val context = LoggerFactory.getILoggerFactory() as LoggerContext
    val tally =
        RunTally().apply {
            this.context = context
            start()
        }
    context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).addAppender(tally)
    return tally
}

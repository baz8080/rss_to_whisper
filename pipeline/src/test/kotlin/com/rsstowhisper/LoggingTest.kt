package com.rsstowhisper

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.io.TempDir
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class LoggingTest {
    private val context = LoggerFactory.getILoggerFactory() as LoggerContext
    private val root = context.getLogger(Logger.ROOT_LOGGER_NAME)
    private val added = mutableListOf<Appender<ILoggingEvent>>()

    @AfterEach
    fun detachAppenders() {
        added.forEach {
            it.stop()
            root.detachAppender(it)
        }
        added.clear()
    }

    private fun trackNewRootAppenders(block: () -> Unit) {
        val before = root.iteratorForAppenders().asSequence().toSet()
        block()
        root.iteratorForAppenders().asSequence().filterNot { it in before }.forEach { added.add(it) }
    }

    @Test
    fun `installErrorLog writes warnings and errors to a file under the data directory`(
        @TempDir dataDir: Path,
    ) {
        var logPath: Path? = null
        trackNewRootAppenders { logPath = installErrorLog(dataDir.toString()) }
        assertEquals(dataDir.resolve("logs").resolve("pipeline-errors.log"), logPath)

        val logger = LoggerFactory.getLogger("com.rsstowhisper.LoggingTest")
        logger.info("an info line that must not be logged")
        logger.warn("a warning line")
        logger.error("an error line")

        val contents = Files.readString(logPath!!)
        assertTrue("a warning line" in contents)
        assertTrue("an error line" in contents)
        assertTrue("must not be logged" !in contents)
    }

    @Test
    fun `installErrorLog returns null when the log directory cannot be created`(
        @TempDir dataDir: Path,
    ) {
        // A regular file where the logs directory needs to go.
        Files.writeString(dataDir.resolve("logs"), "not a directory")
        trackNewRootAppenders { assertNull(installErrorLog(dataDir.toString())) }
    }

    @Test
    fun `installRunTally attaches the tally to the root logger`() {
        var tally: RunTally? = null
        trackNewRootAppenders { tally = installRunTally() }
        assertTrue(root.iteratorForAppenders().asSequence().any { it === tally })
    }

    @Test
    fun `run tally counts warnings and errors but not info`() {
        // Attached to a private logger so a sibling suite's logging cannot inflate the counts.
        val tally = RunTally().apply { context = this@LoggingTest.context }
        tally.start()
        val logger = context.getLogger("com.rsstowhisper.tally-test").apply { isAdditive = false }
        logger.addAppender(tally)

        logger.info("ignored")
        logger.warn("one")
        logger.warn("two")
        logger.error("three")

        assertTrue(tally.summary(null).contains("2 warnings, 1 errors"), tally.summary(null))
    }

    @Test
    fun `run tally summary names the log file when there is one`() {
        assertTrue(
            RunTally().summary(Path.of("/tmp/pipeline-errors.log")).endsWith("/tmp/pipeline-errors.log"),
        )
    }
}

package com.rsstowhisper.external

import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.Path

open class Transcriber(private val modelPath: String) {
    private val logger = LoggerFactory.getLogger(Transcriber::class.java)

    init {
        if (!Files.exists(Path.of(modelPath))) {
            throw FileNotFoundException("Model file not found: $modelPath")
        }
    }

    open fun transcribe(wavPath: Path): List<TranscriptSegment> {
        val outputBase = wavPath.resolveSibling(wavPath.fileName.toString().removeSuffix(".wav"))

        val command =
            listOf(
                "whisper-cli",
                "-m", modelPath,
                "-f", wavPath.toString(),
                "-l", "en",
                "--output-csv",
                "--output-file", outputBase.toString(),
                "--prompt", "Hello, welcome to the podcast. This is a transcription with proper punctuation and capitalization.",
            )

        logger.debug("Running: ${command.joinToString(" ")}")

        val process =
            ProcessBuilder(command)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start()

        // Read stderr concurrently to prevent pipe buffer from filling and deadlocking
        val stderrFuture =
            java.util.concurrent.CompletableFuture.supplyAsync {
                process.errorStream.bufferedReader().readText()
            }

        val exitCode = process.waitFor()
        val stderr = stderrFuture.get()

        if (exitCode != 0) {
            throw RuntimeException("whisper-cli failed with exit code $exitCode: $stderr")
        }

        val csvPath = Path.of("$outputBase.csv")
        if (!Files.exists(csvPath)) {
            throw RuntimeException("whisper-cli did not produce expected output file: $csvPath")
        }

        val segments = parseWhisperCsv(Files.readAllLines(csvPath))

        Files.deleteIfExists(csvPath)

        return segments
    }
}

internal fun parseWhisperCsv(lines: List<String>): List<TranscriptSegment> {
    if (lines.isEmpty()) return emptyList()

    return lines
        .drop(1) // skip header row
        .mapNotNull { line ->
            val parts = line.split(",", limit = 3)
            if (parts.size < 3) return@mapNotNull null

            val startMs = parts[0].trim().toLongOrNull() ?: return@mapNotNull null
            val endMs = parts[1].trim().toLongOrNull() ?: return@mapNotNull null
            val text = parts[2].trim().removeSurrounding("\"")

            TranscriptSegment(startMs = startMs, endMs = endMs, text = text)
        }
}

data class TranscriptSegment(
    val startMs: Long,
    val endMs: Long,
    val text: String,
)

package com.rsstowhisper.transcription

import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.Path

class TranscriptionService(private val modelPath: String) {
    private val logger = LoggerFactory.getLogger(TranscriptionService::class.java)

    init {
        if (!Files.exists(Path.of(modelPath))) {
            throw FileNotFoundException("Model file not found: $modelPath")
        }
    }

    fun transcribe(wavPath: Path): List<TranscriptSegment> {
        val outputBase = wavPath.resolveSibling(wavPath.fileName.toString().removeSuffix(".wav"))

        val command =
            listOf(
                "whisper-cli",
                "-m", modelPath,
                "-f", wavPath.toString(),
                "-l", "en",
                "--output-tsv",
                "--output-file", outputBase.toString(),
            )

        logger.debug("Running: ${command.joinToString(" ")}")

        val process =
            ProcessBuilder(command)
                .redirectErrorStream(false)
                .start()

        val stderr = process.errorStream.bufferedReader().readText()
        val exitCode = process.waitFor()

        if (exitCode != 0) {
            throw RuntimeException("whisper-cli failed with exit code $exitCode: $stderr")
        }

        val tsvPath = Path.of("${outputBase}.tsv")
        if (!Files.exists(tsvPath)) {
            throw RuntimeException("whisper-cli did not produce expected output file: $tsvPath")
        }

        val segments = parseTsv(tsvPath)

        // Clean up the whisper-generated TSV since we write our own output files
        Files.deleteIfExists(tsvPath)

        return segments
    }

    private fun parseTsv(tsvPath: Path): List<TranscriptSegment> {
        val lines = Files.readAllLines(tsvPath)
        if (lines.isEmpty()) return emptyList()

        return lines
            .drop(1) // skip header row
            .mapNotNull { line ->
                val parts = line.split("\t", limit = 3)
                if (parts.size < 3) return@mapNotNull null

                val startMs = parts[0].trim().toLongOrNull() ?: return@mapNotNull null
                val endMs = parts[1].trim().toLongOrNull() ?: return@mapNotNull null
                val text = parts[2]

                TranscriptSegment(startMs = startMs, endMs = endMs, text = text)
            }
    }
}

data class TranscriptSegment(
    val startMs: Long,
    val endMs: Long,
    val text: String,
)

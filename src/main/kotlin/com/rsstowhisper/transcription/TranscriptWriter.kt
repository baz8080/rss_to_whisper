package com.rsstowhisper.transcription

import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path

class TranscriptWriter {
    private val logger = LoggerFactory.getLogger(TranscriptWriter::class.java)

    fun writeTranscriptTxt(
        segments: List<TranscriptSegment>,
        path: Path,
    ) {
        val text = segments.joinToString(" ") { it.text.trim() }
        Files.writeString(path, text)
    }

    fun writeTranscriptTsv(
        segments: List<TranscriptSegment>,
        path: Path,
    ) {
        val sb = StringBuilder()
        sb.appendLine("start\tend\ttext")
        for (segment in segments) {
            sb.appendLine("${segment.startMs}\t${segment.endMs}\t${segment.text.trim()}")
        }
        Files.writeString(path, sb.toString())
    }

    fun writeTranscriptWithTiming(
        segments: List<TranscriptSegment>,
        path: Path,
    ): String {
        if (Files.exists(path)) {
            return Files.readString(path)
        }

        val body = buildTranscriptWithTiming(segments)
        Files.writeString(path, body)
        return body
    }

    companion object {
        fun buildTranscriptWithTiming(segments: List<TranscriptSegment>): String {
            val sb = StringBuilder()
            var accumulatedText = ""
            var accumulatedStart: Long? = null

            for (segment in segments) {
                val text = segment.text.trim()
                if (text.isEmpty()) continue

                if (accumulatedText.isEmpty()) {
                    accumulatedStart = segment.startMs
                }

                if (!text.endsWith(".")) {
                    accumulatedText += "$text "
                } else {
                    if (accumulatedText.isNotEmpty()) {
                        sb.appendLine("$accumulatedStart\t${accumulatedText.trim()} $text")
                        accumulatedText = ""
                        accumulatedStart = null
                    } else {
                        sb.appendLine("${segment.startMs}\t$text")
                    }
                }
            }

            if (accumulatedText.isNotBlank()) {
                sb.appendLine("$accumulatedStart\t${accumulatedText.trim()}")
            }

            return sb.toString()
        }
    }
}

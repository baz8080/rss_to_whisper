package com.rsstowhisper.external

import java.nio.file.Files
import java.nio.file.Path

class TranscriptWriter {
    fun writeTranscriptWithTiming(
        segments: List<TranscriptSegment>,
        path: Path,
    ) {
        // First pass: split into sentences with timestamps
        val lines = mutableListOf<Pair<Long, String>>()
        var accumulatedText = ""
        var accumulatedStart: Long? = null

        for (segment in segments) {
            val text = segment.text.trim()
            if (text.isEmpty()) continue

            val sentences = SENTENCE_SPLIT_REGEX.findAll(text).map { it.value }.toList()

            for (sentence in sentences) {
                val trimmed = sentence.trim()
                if (trimmed.isEmpty()) continue

                if (accumulatedStart == null) {
                    accumulatedStart = segment.startMs
                }

                accumulatedText += "$trimmed "

                if (trimmed.endsWith(".") || trimmed.endsWith("?") || trimmed.endsWith("!")) {
                    lines.add(accumulatedStart to accumulatedText.trim())
                    accumulatedText = ""
                    accumulatedStart = null
                }
            }
        }

        if (accumulatedText.isNotBlank()) {
            lines.add(accumulatedStart!! to accumulatedText.trim())
        }

        // Second pass: merge consecutive lines with the same timestamp
        val sb = StringBuilder()
        var i = 0
        while (i < lines.size) {
            val startMs = lines[i].first
            val merged = StringBuilder(lines[i].second)
            while (i + 1 < lines.size && lines[i + 1].first == startMs) {
                i++
                merged.append(" ").append(lines[i].second)
            }
            sb.appendLine("$startMs\t$merged")
            i++
        }

        Files.writeString(path, sb.toString())
    }

    companion object {
        // Split after sentence-ending punctuation followed by a space
        private val SENTENCE_SPLIT_REGEX = Regex(""".*?[.?!](?:\s+|$)|.+$""")
    }
}

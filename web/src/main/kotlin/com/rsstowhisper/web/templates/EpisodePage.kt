package com.rsstowhisper.web.templates

import com.rsstowhisper.web.models.Episode
import kotlinx.html.FlowContent
import kotlinx.html.HTML
import kotlinx.html.TBODY
import kotlinx.html.TD
import kotlinx.html.a
import kotlinx.html.div
import kotlinx.html.h2
import kotlinx.html.h3
import kotlinx.html.p
import kotlinx.html.script
import kotlinx.html.span
import kotlinx.html.table
import kotlinx.html.tbody
import kotlinx.html.td
import kotlinx.html.th
import kotlinx.html.tr
import kotlinx.html.unsafe

fun HTML.episodePage(
    episode: Episode,
    audioBaseUrl: String,
) {
    layout(episode.episodeTitle ?: "Episode") {
        div("episode-detail") {
            a(href = "javascript:history.back()") {
                attributes["class"] = "back-link"
                +"← Back to results"
            }

            div("podcast-name") {
                +(episode.podcastTitle ?: "Unknown Podcast")
            }

            h2 {
                +(episode.episodeTitle ?: "Untitled")
            }

            episodeMetaTable(episode)

            val audioPath = episode.episodeRelativeMp3Path
            if (audioPath != null) {
                val wavPath = audioPath.replace(".mp3", ".wav")
                val audioUrl = "$audioBaseUrl/$wavPath"
                div("audio-player") {
                    unsafe {
                        +"""<audio id="episode-audio" controls preload="none" src="$audioUrl">"""
                        +"""Your browser does not support audio.</audio>"""
                    }
                }
            }

            val transcript = episode.transcript
            if (transcript != null) {
                h3 { +"Transcript" }
                div("transcript") {
                    val lines = parseTranscript(transcript)
                    if (lines.isEmpty()) {
                        p { +transcript }
                    } else {
                        for ((millis, text) in lines) {
                            val seconds = millis / 1000.0
                            val display = formatTimestamp(millis)
                            div("transcript-line") {
                                span("transcript-time") {
                                    attributes["onclick"] = "seekAudio($seconds)"
                                    attributes["title"] = "Jump to $display"
                                    +display
                                }
                                span("transcript-text") { +text }
                            }
                        }
                    }
                }
            }

            script {
                unsafe {
                    +"""
function seekAudio(seconds) {
    var audio = document.getElementById('episode-audio');
    audio.currentTime = seconds;
    if (audio.paused) { audio.play(); }
}
"""
                }
            }
        }
    }
}

private fun FlowContent.episodeMetaTable(episode: Episode) {
    table("episode-meta-table") {
        tbody {
            episode.episodePublishedOn?.let {
                metaRow("Published", it)
            }
            episode.episodeDuration?.let {
                metaRow("Duration", formatDuration(it))
            }
            if (episode.episodeSeason != null && episode.episodeNumber != null) {
                metaRow("Season / Episode", "S${episode.episodeSeason}E${episode.episodeNumber}")
            } else {
                episode.episodeNumber?.let { metaRow("Episode", "$it") }
                episode.episodeSeason?.let { metaRow("Season", "$it") }
            }
            episode.episodeType?.let {
                if (it != "full") metaRow("Type", it)
            }
            episode.podcastCollections?.let {
                if (it.isNotBlank()) metaRow("Collections", it)
            }
            episode.allTags?.let {
                if (it.isNotBlank()) metaRow("Tags", it)
            }
            episode.episodeSummary?.let { summary ->
                if (summary.isNotBlank()) {
                    metaRow("Summary") {
                        unsafe { +if (summary.contains('<')) summary else linkify(summary) }
                    }
                }
            }
            episode.episodeWebLink?.let { link ->
                metaRow("Episode page") {
                    a(href = link) {
                        attributes["target"] = "_blank"
                        +link
                    }
                }
            }
            episode.episodeAudioLink?.let { link ->
                metaRow("Original audio") {
                    a(href = link) {
                        attributes["target"] = "_blank"
                        +"Link"
                    }
                }
            }
        }
    }
}

private fun TBODY.metaRow(
    label: String,
    value: String,
) {
    tr {
        th { +label }
        td { +value }
    }
}

private fun TBODY.metaRow(
    label: String,
    content: TD.() -> Unit,
) {
    tr {
        th { +label }
        td { content() }
    }
}

private val TRANSCRIPT_LINE_REGEX = Regex("""^(\d+)\t(.*)$""")

data class TranscriptLine(val millis: Long, val text: String)

fun parseTranscript(transcript: String): List<TranscriptLine> {
    val lines = mutableListOf<TranscriptLine>()
    for (line in transcript.lines()) {
        val match = TRANSCRIPT_LINE_REGEX.matchEntire(line.trim()) ?: continue
        val millis = match.groupValues[1].toLongOrNull() ?: continue
        val text = match.groupValues[2].trim()
        if (text.isNotBlank()) {
            lines.add(TranscriptLine(millis, text))
        }
    }
    return lines
}

fun formatTimestamp(millis: Long): String {
    val totalSeconds = millis / 1000
    val hours = totalSeconds / 3600
    val minutes = (totalSeconds % 3600) / 60
    val seconds = totalSeconds % 60
    return if (hours > 0) {
        "%d:%02d:%02d".format(hours, minutes, seconds)
    } else {
        "%d:%02d".format(minutes, seconds)
    }
}

private val URL_REGEX = Regex("""https?://[^\s<>"]+""")

private fun escapeHtml(text: String): String =
    text
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")

fun linkify(text: String): String {
    val result = StringBuilder()
    var last = 0
    for (match in URL_REGEX.findAll(text)) {
        result.append(escapeHtml(text.substring(last, match.range.first)))
        val url = match.value
        result.append("""<a href="${escapeHtml(url)}" target="_blank">${escapeHtml(url)}</a>""")
        last = match.range.last + 1
    }
    result.append(escapeHtml(text.substring(last)))
    return result.toString()
}

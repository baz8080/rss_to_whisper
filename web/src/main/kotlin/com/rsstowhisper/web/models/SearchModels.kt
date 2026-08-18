package com.rsstowhisper.web.models

import org.owasp.html.PolicyFactory
import org.owasp.html.Sanitizers

data class Episode(
    val id: String,
    val podcastTitle: String?,
    val podcastImage: String?,
    val podcastCollections: String?,
    val episodeTitle: String?,
    val episodePublishedOn: String?,
    val episodeAudioLink: String?,
    val episodeWebLink: String?,
    val episodeImage: String?,
    val episodeSummary: String?,
    val episodeNumber: Int?,
    val episodeSeason: Int?,
    val episodeType: String?,
    val episodeDuration: Int?,
    val episodeRelativeAudioPath: String?,
    val allTags: String?,
    val snippet: String? = null,
    val transcript: String? = null,
) {
    // Computed properties — accessible from Thymeleaf as episode.formattedDuration etc.
    val formattedDuration: String? get() = episodeDuration?.let { formatDuration(it) }
    val tagList: List<String> get() =
        allTags
            ?.split(",")
            ?.map(String::trim)
            ?.filter(String::isNotBlank)
            ?: emptyList()
    val audioPath: String? get() = episodeRelativeAudioPath

    // The raw snippet is feed-controlled text (it spans episode_title and
    // podcast_title as well as the transcript) with sentinel highlight markers.
    // Escape it, then turn only the sentinels into <mark> -- so a feed cannot
    // smuggle markup through by containing a literal "<mark>".
    val snippetHtml: String? get() = snippet?.let { renderSnippet(it) }
}

data class SearchResult(
    val episodes: List<Episode>,
    val totalCount: Int,
    val page: Int,
    val pageSize: Int,
) {
    val totalPages: Int get() = if (totalCount == 0) 1 else (totalCount + pageSize - 1) / pageSize
    val hasPrevious: Boolean get() = page > 1
    val hasNext: Boolean get() = page < totalPages
}

data class SearchFilters(
    val query: String = "",
    val durations: Set<String> = emptySet(),
    val podcasts: Set<String> = emptySet(),
    val collections: Set<String> = emptySet(),
    val tags: Set<String> = emptySet(),
    val episodeTypes: Set<String> = emptySet(),
    val page: Int = 1,
    val pageSize: Int = 10,
)

data class FilterOptions(
    val podcasts: List<String>,
    val collections: List<String>,
    val episodeTypes: List<String>,
)

enum class DurationCategory(val label: String, val maxSeconds: Int?) {
    SHORT("Short (< 15 min)", 900),
    MEDIUM("Medium (15–45 min)", 2700),
    LONG("Long (> 45 min)", null),
}

// Transcript line — passed to Thymeleaf as a list for the episode detail page.
data class TranscriptLine(val millis: Long, val text: String) {
    val seconds: Double get() = millis / 1000.0
    val display: String get() = formatTimestamp(millis)
}

// --- Utility functions ---

fun SearchFilters.hasActiveFilters(): Boolean =
    query.isNotBlank() || durations.isNotEmpty() || podcasts.isNotEmpty() ||
        collections.isNotEmpty() || tags.isNotEmpty() || episodeTypes.isNotEmpty()

// URLEncoder targets form encoding, where a space becomes '+'. Whether '+' is
// decoded back to a space in a *query string* is up to the server, so spaces are
// re-written as %20, which every decoder agrees on. Any '+' left after encoding
// is an encoded space -- a literal '+' in the input has already become %2B.
private fun urlEncode(value: String): String = java.net.URLEncoder.encode(value, Charsets.UTF_8).replace("+", "%20")

fun buildSearchUrl(filters: SearchFilters): String {
    val params = mutableListOf<String>()
    if (filters.query.isNotBlank()) params.add("q=${urlEncode(filters.query)}")
    filters.durations.forEach { params.add("duration=${urlEncode(it)}") }
    filters.podcasts.forEach { params.add("podcast=${urlEncode(it)}") }
    filters.collections.forEach { params.add("collection=${urlEncode(it)}") }
    filters.tags.forEach { params.add("tag=${urlEncode(it)}") }
    filters.episodeTypes.forEach { params.add("episodeType=${urlEncode(it)}") }
    if (filters.page > 1) params.add("page=${filters.page}")
    return "/search?${params.joinToString("&")}"
}

fun formatDuration(seconds: Int): String {
    val hours = seconds / 3600
    val minutes = (seconds % 3600) / 60
    return if (hours > 0) "${hours}h ${minutes}m" else "${minutes}m"
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

private val VTT_TIMING_REGEX = Regex("""(\d{2}):(\d{2}):(\d{2})\.(\d{3}) --> .*""")

fun parseTranscript(transcript: String): List<TranscriptLine> {
    if (transcript.isBlank()) return emptyList()

    val result = mutableListOf<TranscriptLine>()
    var currentStartMs: Long? = null
    val currentText = StringBuilder()

    fun flush() {
        if (currentStartMs != null && currentText.isNotBlank()) {
            result.add(TranscriptLine(currentStartMs!!, currentText.toString().trim()))
        }
    }

    for (line in transcript.lines()) {
        val trimmed = line.trim()
        val timingMatch = VTT_TIMING_REGEX.matchEntire(trimmed)
        when {
            timingMatch != null -> {
                flush()
                val h = timingMatch.groupValues[1].toLong()
                val m = timingMatch.groupValues[2].toLong()
                val s = timingMatch.groupValues[3].toLong()
                val ms = timingMatch.groupValues[4].toLong()
                currentStartMs = h * 3_600_000L + m * 60_000L + s * 1_000L + ms
                currentText.clear()
            }
            trimmed.isBlank() -> {
                flush()
                currentStartMs = null
                currentText.clear()
            }
            trimmed != "WEBVTT" && currentStartMs != null -> {
                if (currentText.isNotEmpty()) currentText.append(" ")
                currentText.append(trimmed)
            }
        }
    }
    flush()
    return result
}

// Sentinel code points wrapped around FTS5 matches by EpisodeRepository. They
// cannot occur in feed or transcript text, so they survive HTML escaping
// unambiguously and mark exactly what the search engine matched.
const val SNIPPET_MARK_START = "\u0001"
const val SNIPPET_MARK_END = "\u0002"

fun renderSnippet(snippet: String): String =
    escapeHtml(snippet)
        .replace(SNIPPET_MARK_START, "<mark>")
        .replace(SNIPPET_MARK_END, "</mark>")

// Episode summaries arrive as raw HTML from third-party feeds. Anything outside
// this allow-list (script, iframe, event handlers, javascript: URLs) is dropped.
// IMAGES is included because show notes routinely embed artwork; it permits
// <img> only with an http/https src, and no event handlers survive it.
private val SUMMARY_POLICY: PolicyFactory =
    Sanitizers.FORMATTING
        .and(Sanitizers.BLOCKS)
        .and(Sanitizers.IMAGES)
        .and(Sanitizers.LINKS)

fun sanitizeHtml(html: String): String = SUMMARY_POLICY.sanitize(html)

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

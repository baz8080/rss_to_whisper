package com.rsstowhisper.web.models

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
    val episodeTranscript: String?,
    val episodeRelativeMp3Path: String?,
    val allTags: String?,
    val snippet: String? = null,
)

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
    val pageSize: Int = 100,
)

data class FilterOptions(
    val podcasts: List<String>,
    val collections: List<String>,
    val tags: List<String>,
    val episodeTypes: List<String>,
)

enum class DurationCategory(val label: String, val maxSeconds: Int?) {
    SHORT("Short (< 15 min)", 900),
    MEDIUM("Medium (15–45 min)", 2700),
    LONG("Long (> 45 min)", null),
    ;

    companion object {
        fun fromSeconds(seconds: Int?): DurationCategory? {
            if (seconds == null || seconds <= 0) return null
            return when {
                seconds < 900 -> SHORT
                seconds <= 2700 -> MEDIUM
                else -> LONG
            }
        }
    }
}

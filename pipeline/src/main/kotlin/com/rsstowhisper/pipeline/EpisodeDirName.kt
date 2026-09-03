package com.rsstowhisper.pipeline

/**
 * An episode directory name, taken apart.
 *
 * The name is the only metadata left for an episode that has aged out of its feed, so
 * parsing it back is what makes orphan recovery possible at all.
 */
internal data class EpisodeDirName(
    val dirName: String,
    val publishedOn: String,
    val episodeId: String,
    val titleSlug: String,
) {
    val stablePrefix: String get() = "$publishedOn-$episodeId"

    /** Null rather than "", which downstream templates would render as an empty title. */
    val title: String? get() = titleSlug.replace('-', ' ').trim().ifBlank { null }

    companion object {
        // Lowercase hex only: md5Hash8 emits lowercase, so an uppercase match is not ours.
        private val PATTERN = Regex("^(\\d{4}-\\d{2}-\\d{2})-([0-9a-f]{8})-(.*)$")

        fun parse(dirName: String): EpisodeDirName? =
            PATTERN.matchEntire(dirName)?.let { match ->
                val (publishedOn, episodeId, titleSlug) = match.destructured
                EpisodeDirName(dirName, publishedOn, episodeId, titleSlug)
            }

        fun matches(dirName: String): Boolean = PATTERN.matches(dirName)
    }
}

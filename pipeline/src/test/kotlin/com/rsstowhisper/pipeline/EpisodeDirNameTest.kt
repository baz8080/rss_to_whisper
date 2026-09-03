package com.rsstowhisper.pipeline

import com.rsstowhisper.escapeFilename
import java.util.Date
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class EpisodeDirNameTest {
    @Test
    fun `parses a well formed directory name`() {
        val parsed = assertNotNull(EpisodeDirName.parse("2024-03-14-ab12cd34-Some-Episode-Title"))
        assertEquals("2024-03-14", parsed.publishedOn)
        assertEquals("ab12cd34", parsed.episodeId)
        assertEquals("2024-03-14-ab12cd34", parsed.stablePrefix)
        assertEquals("Some Episode Title", parsed.title)
    }

    @Test
    fun `returns null for a name with no date prefix`() {
        assertNull(EpisodeDirName.parse("unknown-date-ab12cd34-Title"))
        assertNull(EpisodeDirName.parse("random-folder"))
        assertNull(EpisodeDirName.parse("podcasts.db"))
    }

    @Test
    fun `returns null for uppercase hex, which this pipeline never produces`() {
        assertNull(EpisodeDirName.parse("2024-03-14-ABCDEF12-Title"))
    }

    @Test
    fun `returns null when the id is not followed by a title separator`() {
        assertNull(EpisodeDirName.parse("2024-03-14-ab12cd34"))
        assertNull(EpisodeDirName.parse("2024-03-14-ab12cd3-Title"))
    }

    @Test
    fun `title is null when the slug is empty`() {
        assertNull(assertNotNull(EpisodeDirName.parse("2024-03-14-ab12cd34-")).title)
        assertNull(assertNotNull(EpisodeDirName.parse("2024-03-14-ab12cd34---")).title)
    }

    @Test
    fun `matches agrees with parse`() {
        assertTrue(EpisodeDirName.matches("2024-03-14-ab12cd34-Title"))
        assertFalse(EpisodeDirName.matches("unknown-date-ab12cd34-Title"))
    }

    /** The load-bearing property: what the pipeline writes is what recovery can read back. */
    @Test
    fun `round trips a directory name the pipeline itself would create`() {
        val entry =
            makeEntry(
                title = "Ep 42: What's *really* going on?",
                publishedDate = Date(1_700_000_000_000L),
                guid = "https://example.com/guid/42",
            )
        val dirName = escapeFilename(PodcastPipeline.getEpisodeDirName(entry))

        val parsed = assertNotNull(EpisodeDirName.parse(dirName))
        assertEquals(PodcastPipeline.episodeStablePrefix(entry), parsed.stablePrefix)
        assertEquals(PodcastPipeline.episodeId(entry), parsed.episodeId)
    }
}

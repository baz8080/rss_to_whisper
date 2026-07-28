package com.rsstowhisper.web.models

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class SearchModelsTest {
    @Nested
    inner class FormatDuration {
        @Test
        fun `zero seconds`() = assertEquals("0m", formatDuration(0))

        @Test
        fun `less than one minute`() = assertEquals("0m", formatDuration(59))

        @Test
        fun `exactly one minute`() = assertEquals("1m", formatDuration(60))

        @Test
        fun `fifty-nine minutes`() = assertEquals("59m", formatDuration(3599))

        @Test
        fun `exactly one hour`() = assertEquals("1h 0m", formatDuration(3600))

        @Test
        fun `one hour and one minute`() = assertEquals("1h 1m", formatDuration(3661))

        @Test
        fun `two hours three minutes`() = assertEquals("2h 3m", formatDuration(7384))
    }

    @Nested
    inner class FormatTimestamp {
        @Test
        fun `zero milliseconds`() = assertEquals("0:00", formatTimestamp(0))

        @Test
        fun `one second`() = assertEquals("0:01", formatTimestamp(1_000))

        @Test
        fun `one minute`() = assertEquals("1:00", formatTimestamp(60_000))

        @Test
        fun `one minute thirty seconds`() = assertEquals("1:30", formatTimestamp(90_000))

        @Test
        fun `fifty-nine minutes fifty-nine seconds`() = assertEquals("59:59", formatTimestamp(3_599_000))

        @Test
        fun `exactly one hour`() = assertEquals("1:00:00", formatTimestamp(3_600_000))

        @Test
        fun `one hour twenty-three minutes forty-five seconds`() = assertEquals("1:23:45", formatTimestamp(5_025_000))
    }

    @Nested
    inner class ParseTranscript {
        @Test
        fun `empty string returns empty list`() = assertTrue(parseTranscript("").isEmpty())

        @Test
        fun `blank string returns empty list`() = assertTrue(parseTranscript("   \n  ").isEmpty())

        @Test
        fun `single cue`() {
            val vtt = "WEBVTT\n\n00:00:01.000 --> 00:00:03.000\nHello world\n"
            val lines = parseTranscript(vtt)
            assertEquals(1, lines.size)
            assertEquals(TranscriptLine(1_000L, "Hello world"), lines[0])
        }

        @Test
        fun `multiple cues`() {
            val vtt =
                "WEBVTT\n\n" +
                    "00:00:00.000 --> 00:00:01.000\nFirst\n\n" +
                    "00:00:01.000 --> 00:00:02.000\nSecond\n\n" +
                    "00:00:02.000 --> 00:00:03.000\nThird\n"
            val lines = parseTranscript(vtt)
            assertEquals(3, lines.size)
            assertEquals("First", lines[0].text)
            assertEquals("Second", lines[1].text)
            assertEquals("Third", lines[2].text)
        }

        @Test
        fun `timestamp converts hours minutes seconds and millis to milliseconds`() {
            val vtt = "WEBVTT\n\n01:02:03.456 --> 01:02:04.000\nSome text\n"
            val lines = parseTranscript(vtt)
            assertEquals(1, lines.size)
            // 1h=3600000 + 2m=120000 + 3s=3000 + 456ms = 3723456
            assertEquals(3_723_456L, lines[0].millis)
        }

        @Test
        fun `WEBVTT header line is not included as text`() {
            val vtt = "WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nHello\n"
            val lines = parseTranscript(vtt)
            assertEquals(1, lines.size)
            assertEquals("Hello", lines[0].text)
        }

        @Test
        fun `multi-line cue text is joined with a space`() {
            val vtt = "WEBVTT\n\n00:00:00.000 --> 00:00:02.000\nLine one\nLine two\n"
            val lines = parseTranscript(vtt)
            assertEquals(1, lines.size)
            assertEquals("Line one Line two", lines[0].text)
        }

        @Test
        fun `leading and trailing whitespace in cue text is trimmed`() {
            val vtt = "WEBVTT\n\n00:00:00.000 --> 00:00:01.000\n  Hello  \n"
            val lines = parseTranscript(vtt)
            assertEquals("Hello", lines[0].text)
        }

        @Test
        fun `cues without text are skipped`() {
            val vtt = "WEBVTT\n\n00:00:00.000 --> 00:00:01.000\n\n00:00:01.000 --> 00:00:02.000\nHello\n"
            val lines = parseTranscript(vtt)
            assertEquals(1, lines.size)
            assertEquals("Hello", lines[0].text)
        }
    }

    @Nested
    inner class Linkify {
        @Test
        fun `plain text is HTML escaped`() = assertEquals("Hello &amp; World", linkify("Hello & World"))

        @Test
        fun `angle brackets are escaped`() = assertEquals("a &lt;b&gt; c", linkify("a <b> c"))

        @Test
        fun `text with no URLs is returned escaped`() = assertEquals("No links here", linkify("No links here"))

        @Test
        fun `URL is wrapped in anchor tag`() {
            val result = linkify("Visit https://example.com today")
            assertEquals(
                """Visit <a href="https://example.com" target="_blank">https://example.com</a> today""",
                result,
            )
        }

        @Test
        fun `multiple URLs are all linkified`() {
            val result = linkify("https://a.com and https://b.com")
            assertTrue(result.contains("""href="https://a.com""""))
            assertTrue(result.contains("""href="https://b.com""""))
        }

        @Test
        fun `surrounding text with special chars is escaped`() {
            val result = linkify("Tom & Jerry at https://example.com")
            assertTrue(result.startsWith("Tom &amp; Jerry at "))
        }
    }

    @Nested
    inner class HasActiveFilters {
        @Test
        fun `empty filters returns false`() = assertFalse(SearchFilters().hasActiveFilters())

        @Test
        fun `blank query returns false`() = assertFalse(SearchFilters(query = "   ").hasActiveFilters())

        @Test
        fun `non-blank query returns true`() = assertTrue(SearchFilters(query = "test").hasActiveFilters())

        @Test
        fun `non-empty durations returns true`() = assertTrue(SearchFilters(durations = setOf("SHORT")).hasActiveFilters())

        @Test
        fun `non-empty podcasts returns true`() = assertTrue(SearchFilters(podcasts = setOf("My Podcast")).hasActiveFilters())

        @Test
        fun `non-empty collections returns true`() = assertTrue(SearchFilters(collections = setOf("Tech")).hasActiveFilters())

        @Test
        fun `non-empty tags returns true`() = assertTrue(SearchFilters(tags = setOf("kotlin")).hasActiveFilters())

        @Test
        fun `non-empty episodeTypes returns true`() = assertTrue(SearchFilters(episodeTypes = setOf("full")).hasActiveFilters())
    }

    @Nested
    inner class BuildSearchUrl {
        @Test
        fun `empty filters produces base URL with no params`() = assertEquals("/search?", buildSearchUrl(SearchFilters()))

        @Test
        fun `query is included`() = assertTrue(buildSearchUrl(SearchFilters(query = "kotlin")).contains("q=kotlin"))

        @Test
        fun `page 1 is omitted`() = assertFalse(buildSearchUrl(SearchFilters(query = "test", page = 1)).contains("page="))

        @Test
        fun `page greater than 1 is included`() = assertTrue(buildSearchUrl(SearchFilters(page = 3)).contains("page=3"))

        @Test
        fun `multiple podcasts are all included`() {
            val url = buildSearchUrl(SearchFilters(podcasts = setOf("PodA", "PodB")))
            assertTrue(url.contains("podcast=PodA"))
            assertTrue(url.contains("podcast=PodB"))
        }

        @Test
        fun `multiple filter types are combined`() {
            val url = buildSearchUrl(SearchFilters(query = "ai", durations = setOf("SHORT"), page = 2))
            assertTrue(url.contains("q=ai"))
            assertTrue(url.contains("duration=SHORT"))
            assertTrue(url.contains("page=2"))
        }
    }

    @Nested
    inner class SearchResultPagination {
        @Test
        fun `zero results gives one total page`() {
            val result = SearchResult(emptyList(), totalCount = 0, page = 1, pageSize = 10)
            assertEquals(1, result.totalPages)
            assertFalse(result.hasPrevious)
            assertFalse(result.hasNext)
        }

        @Test
        fun `results fitting exactly one page`() {
            val result = SearchResult(emptyList(), totalCount = 10, page = 1, pageSize = 10)
            assertEquals(1, result.totalPages)
            assertFalse(result.hasPrevious)
            assertFalse(result.hasNext)
        }

        @Test
        fun `results spanning two pages - first page`() {
            val result = SearchResult(emptyList(), totalCount = 11, page = 1, pageSize = 10)
            assertEquals(2, result.totalPages)
            assertFalse(result.hasPrevious)
            assertTrue(result.hasNext)
        }

        @Test
        fun `results spanning two pages - second page`() {
            val result = SearchResult(emptyList(), totalCount = 11, page = 2, pageSize = 10)
            assertEquals(2, result.totalPages)
            assertTrue(result.hasPrevious)
            assertFalse(result.hasNext)
        }

        @Test
        fun `partial last page is counted as a full page`() {
            val result = SearchResult(emptyList(), totalCount = 21, page = 1, pageSize = 10)
            assertEquals(3, result.totalPages)
        }
    }

    @Nested
    inner class EpisodeComputedProperties {
        private fun episode(
            duration: Int? = null,
            snippet: String? = null,
            audioPath: String? = null,
            tags: String? = null,
        ) = Episode(
            id = "1",
            podcastTitle = null,
            podcastImage = null,
            podcastCollections = null,
            episodeTitle = null,
            episodePublishedOn = null,
            episodeAudioLink = null,
            episodeWebLink = null,
            episodeImage = null,
            episodeSummary = null,
            episodeNumber = null,
            episodeSeason = null,
            episodeType = null,
            episodeDuration = duration,
            episodeRelativeAudioPath = audioPath,
            allTags = tags,
            snippet = snippet,
        )

        @Test
        fun `formattedDuration is null when episodeDuration is null`() = assertNull(episode().formattedDuration)

        @Test
        fun `formattedDuration formats duration correctly`() = assertEquals("1h 0m", episode(duration = 3600).formattedDuration)

        @Test
        fun `tagList is empty when allTags is null`() = assertTrue(episode().tagList.isEmpty())

        @Test
        fun `tagList splits on comma`() = assertEquals(listOf("a", "b"), episode(tags = "a,b").tagList)

        @Test
        fun `tagList trims whitespace from each tag`() = assertEquals(listOf("a", "b"), episode(tags = " a , b ").tagList)

        @Test
        fun `tagList filters out blank entries`() = assertTrue(episode(tags = ",").tagList.isEmpty())

        @Test
        fun `audioPath is null when episodeRelativeAudioPath is null`() = assertNull(episode().audioPath)

        @Test
        fun `audioPath returns episodeRelativeAudioPath directly`() =
            assertEquals("audio/ep.mp3", episode(audioPath = "audio/ep.mp3").audioPath)
    }

    @Nested
    inner class TranscriptLineProperties {
        @Test
        fun `seconds converts milliseconds correctly`() {
            assertEquals(1.5, TranscriptLine(1500L, "text").seconds, 0.001)
        }

        @Test
        fun `display formats timestamp without hours`() = assertEquals("1:00", TranscriptLine(60_000L, "").display)

        @Test
        fun `display formats timestamp with hours`() = assertEquals("1:00:00", TranscriptLine(3_600_000L, "").display)
    }
}

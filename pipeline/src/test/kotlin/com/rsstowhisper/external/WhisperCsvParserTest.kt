package com.rsstowhisper.external

import kotlin.test.Test
import kotlin.test.assertEquals

class WhisperCsvParserTest {
    @Test
    fun `parses valid csv with header`() {
        val lines =
            listOf(
                "start,end,text",
                "0,1000,\"Hello world\"",
                "1000,2500,\"Second line\"",
            )

        val result = parseWhisperCsv(lines)

        assertEquals(
            listOf(
                TranscriptSegment(0, 1000, "Hello world"),
                TranscriptSegment(1000, 2500, "Second line"),
            ),
            result,
        )
    }

    @Test
    fun `returns empty list for empty input`() {
        assertEquals(emptyList(), parseWhisperCsv(emptyList()))
    }

    @Test
    fun `returns empty list when only header is present`() {
        assertEquals(emptyList(), parseWhisperCsv(listOf("start,end,text")))
    }

    @Test
    fun `skips rows with fewer than three columns`() {
        val lines =
            listOf(
                "start,end,text",
                "0,1000",
                "only-one-field",
                "100,200,\"ok\"",
            )

        assertEquals(
            listOf(TranscriptSegment(100, 200, "ok")),
            parseWhisperCsv(lines),
        )
    }

    @Test
    fun `skips rows with non-numeric timestamps`() {
        val lines =
            listOf(
                "start,end,text",
                "abc,1000,\"bad start\"",
                "0,def,\"bad end\"",
                "100,200,\"good\"",
            )

        assertEquals(
            listOf(TranscriptSegment(100, 200, "good")),
            parseWhisperCsv(lines),
        )
    }

    @Test
    fun `preserves commas inside text using limit`() {
        val lines =
            listOf(
                "start,end,text",
                "0,1000,\"hello, world, foo\"",
            )

        assertEquals(
            listOf(TranscriptSegment(0, 1000, "hello, world, foo")),
            parseWhisperCsv(lines),
        )
    }

    @Test
    fun `strips surrounding quotes but leaves unquoted text intact`() {
        val lines =
            listOf(
                "start,end,text",
                "0,1000,unquoted text",
                "1000,2000,\"quoted text\"",
            )

        assertEquals(
            listOf(
                TranscriptSegment(0, 1000, "unquoted text"),
                TranscriptSegment(1000, 2000, "quoted text"),
            ),
            parseWhisperCsv(lines),
        )
    }

    @Test
    fun `trims whitespace around timestamps`() {
        val lines =
            listOf(
                "start,end,text",
                " 0 , 1000 ,\"hello\"",
            )

        assertEquals(
            listOf(TranscriptSegment(0, 1000, "hello")),
            parseWhisperCsv(lines),
        )
    }
}

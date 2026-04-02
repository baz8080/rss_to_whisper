package com.rsstowhisper.transcription

import kotlin.test.Test
import kotlin.test.assertEquals

class TranscriptWriterTest {
    @Test
    fun `test buildTranscriptWithTiming groups sentences`() {
        val segments =
            listOf(
                TranscriptSegment(0, 1000, " Hello world"),
                TranscriptSegment(1000, 2000, " this is a test."),
                TranscriptSegment(2000, 3000, " Another sentence."),
            )

        val result = TranscriptWriter.buildTranscriptWithTiming(segments)

        assertEquals("0\tHello world this is a test.\n2000\tAnother sentence.\n", result)
    }

    @Test
    fun `test buildTranscriptWithTiming handles trailing text without period`() {
        val segments =
            listOf(
                TranscriptSegment(0, 1000, " Hello world"),
                TranscriptSegment(1000, 2000, " no period here"),
            )

        val result = TranscriptWriter.buildTranscriptWithTiming(segments)

        assertEquals("0\tHello world no period here\n", result)
    }

    @Test
    fun `test buildTranscriptWithTiming handles single sentence ending with period`() {
        val segments =
            listOf(
                TranscriptSegment(0, 1000, " Complete sentence."),
            )

        val result = TranscriptWriter.buildTranscriptWithTiming(segments)

        assertEquals("0\tComplete sentence.\n", result)
    }

    @Test
    fun `test buildTranscriptWithTiming handles empty segments`() {
        val result = TranscriptWriter.buildTranscriptWithTiming(emptyList())
        assertEquals("", result)
    }

    @Test
    fun `test buildTranscriptWithTiming handles multiple sentences`() {
        val segments =
            listOf(
                TranscriptSegment(0, 500, " First part"),
                TranscriptSegment(500, 1000, " of sentence."),
                TranscriptSegment(1000, 1500, " Second part"),
                TranscriptSegment(1500, 2000, " of another."),
                TranscriptSegment(2000, 2500, " Trailing"),
            )

        val result = TranscriptWriter.buildTranscriptWithTiming(segments)

        val expected =
            "0\tFirst part of sentence.\n" +
                "1000\tSecond part of another.\n" +
                "2000\tTrailing\n"
        assertEquals(expected, result)
    }

    @Test
    fun `test buildTranscriptWithTiming skips empty text segments`() {
        val segments =
            listOf(
                TranscriptSegment(0, 500, " Hello."),
                TranscriptSegment(500, 1000, "  "),
                TranscriptSegment(1000, 1500, " World."),
            )

        val result = TranscriptWriter.buildTranscriptWithTiming(segments)

        assertEquals("0\tHello.\n1000\tWorld.\n", result)
    }
}

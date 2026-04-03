package com.rsstowhisper.transcription

import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals

class TranscriptWriterTest {
    @Test
    fun `test writeTranscriptTsv writes header and segments`() {
        val segments =
            listOf(
                TranscriptSegment(0, 1000, " Hello world"),
                TranscriptSegment(1000, 2000, " this is a test."),
                TranscriptSegment(2000, 3000, " Another sentence."),
            )

        val tempFile = Files.createTempFile("transcript", ".tsv")
        try {
            TranscriptWriter().writeTranscriptTsv(segments, tempFile)
            val result = Files.readString(tempFile)

            val expected =
                "start\tend\ttext\n" +
                    "0\t1000\tHello world\n" +
                    "1000\t2000\tthis is a test.\n" +
                    "2000\t3000\tAnother sentence.\n"
            assertEquals(expected, result)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `test writeTranscriptTsv handles empty segments`() {
        val tempFile = Files.createTempFile("transcript", ".tsv")
        try {
            TranscriptWriter().writeTranscriptTsv(emptyList(), tempFile)
            val result = Files.readString(tempFile)

            assertEquals("start\tend\ttext\n", result)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `test writeTranscriptWithTiming splits sentences across segments`() {
        val segments =
            listOf(
                TranscriptSegment(0, 14160, " It's Friday, late afternoon,"),
                TranscriptSegment(14160, 14860, " the end of the day."),
                TranscriptSegment(15220, 17680, " Your boss is breathing down your neck."),
                TranscriptSegment(17760, 19880, " Your whole team is on edge."),
            )

        val tempFile = Files.createTempFile("transcript_with_timing", ".tsv")
        try {
            TranscriptWriter().writeTranscriptWithTiming(segments, tempFile)
            val result = Files.readString(tempFile)

            val expected =
                "0\tIt's Friday, late afternoon, the end of the day.\n" +
                    "15220\tYour boss is breathing down your neck.\n" +
                    "17760\tYour whole team is on edge.\n"
            assertEquals(expected, result)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `test writeTranscriptWithTiming splits multiple sentences within one segment`() {
        val segments =
            listOf(
                TranscriptSegment(0, 13980, " I just love astronomers. They're so cute sometimes."),
                TranscriptSegment(13980, 18560, " Every time they see something new in the sky, they give it a new name."),
            )

        val tempFile = Files.createTempFile("transcript_with_timing", ".tsv")
        try {
            TranscriptWriter().writeTranscriptWithTiming(segments, tempFile)
            val result = Files.readString(tempFile)

            val expected =
                "0\tI just love astronomers. They're so cute sometimes.\n" +
                    "13980\tEvery time they see something new in the sky, they give it a new name.\n"
            assertEquals(expected, result)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `test writeTranscriptWithTiming carries incomplete sentence to next segment`() {
        val segments =
            listOf(
                TranscriptSegment(
                    0,
                    13980,
                    " I just love astronomers. They're so cute. Every time they see something new,",
                ),
                TranscriptSegment(13980, 18560, " they give it a new name. And then we figure out"),
            )

        val tempFile = Files.createTempFile("transcript_with_timing", ".tsv")
        try {
            TranscriptWriter().writeTranscriptWithTiming(segments, tempFile)
            val result = Files.readString(tempFile)

            val expected =
                "0\tI just love astronomers. They're so cute." +
                    " Every time they see something new, they give it a new name.\n" +
                    "13980\tAnd then we figure out\n"
            assertEquals(expected, result)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `test writeTranscriptWithTiming handles question marks and exclamation marks`() {
        val segments =
            listOf(
                TranscriptSegment(0, 5000, " How deep are we talking here? Well, pretty deep!"),
                TranscriptSegment(5000, 10000, " And that's amazing."),
            )

        val tempFile = Files.createTempFile("transcript_with_timing", ".tsv")
        try {
            TranscriptWriter().writeTranscriptWithTiming(segments, tempFile)
            val result = Files.readString(tempFile)

            val expected =
                "0\tHow deep are we talking here? Well, pretty deep!\n" +
                    "5000\tAnd that's amazing.\n"
            assertEquals(expected, result)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `test writeTranscriptWithTiming handles trailing text without punctuation`() {
        val segments =
            listOf(
                TranscriptSegment(0, 1000, " Hello world"),
                TranscriptSegment(1000, 2000, " no period here"),
            )

        val tempFile = Files.createTempFile("transcript_with_timing", ".tsv")
        try {
            TranscriptWriter().writeTranscriptWithTiming(segments, tempFile)
            val result = Files.readString(tempFile)

            assertEquals("0\tHello world no period here\n", result)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `test writeTranscriptWithTiming handles empty segments`() {
        val tempFile = Files.createTempFile("transcript_with_timing", ".tsv")
        try {
            TranscriptWriter().writeTranscriptWithTiming(emptyList(), tempFile)
            val result = Files.readString(tempFile)

            assertEquals("", result)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }
}

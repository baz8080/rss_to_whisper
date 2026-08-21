package com.rsstowhisper.external

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.zip.GZIPInputStream
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class WhisperTranscriptionTest {
    private val response =
        """
        {
          "task": "transcribe",
          "duration": 20.5,
          "segments": [
            {
              "id": 0, "start": 1.79, "end": 10.49,
              "text": " From APM, this is a show.",
              "words": [
                {"word": " From", "start": 1.79, "end": 2.04, "probability": 0.91},
                {"word": " APM,", "start": 2.04, "end": 2.51, "probability": 0.72}
              ]
            },
            {
              "id": 1, "start": 10.96, "end": 13.61,
              "text": " Here's the host.",
              "words": [
                {"word": " Here's", "start": 10.96, "end": 11.4, "probability": 0.99}
              ]
            }
          ]
        }
        """.trimIndent()

    @Test
    fun `renders the WebVTT the server would have returned`() {
        val vtt = WhisperTranscription.parse(response).vtt
        assertEquals(
            "WEBVTT\n\n" +
                "00:00:01.790 --> 00:00:10.490\n From APM, this is a show.\n\n" +
                "00:00:10.960 --> 00:00:13.610\n Here's the host.\n\n",
            vtt,
        )
    }

    @Test
    fun `timestamps carry hours`() {
        assertEquals("01:02:03.400", WhisperTranscription.timestamp(3723.4))
        assertEquals("00:00:00.000", WhisperTranscription.timestamp(0.0))
    }

    @Test
    fun `a negative or non-finite time clamps rather than formatting garbage`() {
        assertEquals("00:00:00.000", WhisperTranscription.timestamp(-1.0))
        assertEquals("00:00:00.000", WhisperTranscription.timestamp(Double.NaN))
    }

    @Test
    fun `words carry their segment index so the sidecar can be joined to the cues`() {
        val words = WhisperTranscription.parse(response).words
        assertEquals(3, words.size)
        assertEquals(listOf(0, 0, 1), words.map { it.segment })
        assertEquals(" APM,", words[1].text)
        assertEquals(2.04, words[1].start)
        assertEquals(0.72, words[1].probability)
    }

    @Test
    fun `a word with no timestamps is dropped rather than placed at zero`() {
        val json =
            """
            {"segments":[{"id":0,"start":0.0,"end":1.0,"text":" Hi.",
             "words":[{"word":" Hi.","probability":0.5}]}]}
            """.trimIndent()
        assertTrue(WhisperTranscription.parse(json).words.isEmpty())
    }

    @Test
    fun `a response with no segments is empty rather than throwing`() {
        val parsed = WhisperTranscription.parse("""{"task":"transcribe"}""")
        assertTrue(parsed.isEmpty)
        assertTrue(parsed.words.isEmpty())
    }

    @Test
    fun `a parsed response with segments is not empty`() {
        assertFalse(WhisperTranscription.parse(response).isEmpty)
    }

    @Test
    fun `writeWords emits gzipped newline-delimited json`(
        @TempDir tmp: Path,
    ) {
        val out = tmp.resolve(WhisperTranscription.WORDS_FILENAME)
        WhisperTranscription.parse(response).writeWords(out)

        val lines =
            GZIPInputStream(Files.newInputStream(out)).bufferedReader().readLines()
        assertEquals(3, lines.size)
        assertTrue(lines[0].contains("\"w\":\" From\""))
        assertTrue(lines[0].contains("\"s\":1.79"))
        assertTrue(lines[0].contains("\"seg\":0"))
        assertTrue(lines[2].contains("\"seg\":1"))
    }
}

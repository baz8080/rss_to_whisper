package com.rsstowhisper.external

import okhttp3.MediaType.Companion.toMediaType
import okhttp3.MultipartBody
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.asRequestBody
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Round-trips one real episode through a running whisper.cpp server.
 *
 * Opt-in: needs a server and an audio file, so CI never runs it. The unit
 * tests cover the parse against a fixture; this covers the one thing a fixture
 * cannot, which is whether the WebVTT rendered here is the WebVTT the server
 * itself would have returned for the same audio.
 *
 *   WHISPER_IT_AUDIO=/path/to/audio.mp3 \
 *   WHISPER_IT_SERVER=http://localhost:8080 \
 *     ./gradlew :pipeline:test --tests '*WhisperServerIntegrationTest*'
 */
@EnabledIfEnvironmentVariable(named = "WHISPER_IT_AUDIO", matches = ".+")
class WhisperServerIntegrationTest {
    private val server: String = System.getenv("WHISPER_IT_SERVER") ?: "http://localhost:8080"
    private val audio: Path = Path.of(System.getenv("WHISPER_IT_AUDIO"))

    private val client =
        OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(90, TimeUnit.MINUTES)
            .writeTimeout(30, TimeUnit.MINUTES)
            .build()

    /** The pipeline's own request with response_format swapped back to vtt. */
    private fun serverVtt(): String {
        val body =
            MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("file", audio.fileName.toString(), audio.toFile().asRequestBody("audio/mpeg".toMediaType()))
                .addFormDataPart("language", "en")
                .addFormDataPart("response_format", "vtt")
                .addFormDataPart("vad", "false")
                .addFormDataPart("token_timestamps", "true")
                .addFormDataPart("max_len", Transcriber.DEFAULT_MAX_LEN.toString())
                .addFormDataPart("split_on_word", "true")
                .addFormDataPart("prompt", Transcriber.DEFAULT_INITIAL_PROMPT)
                .addFormDataPart("carry_initial_prompt", "true")
                .build()
        return client.newCall(Request.Builder().url("$server/inference").post(body).build())
            .execute()
            .use { it.body!!.string() }
    }

    @Test
    fun `rendered vtt matches what the server returns for the same audio`() {
        val parsed = WhisperTranscription.parse(Transcriber(server, client).transcribe(audio))
        val reference = serverVtt()

        fun cues(vtt: String) = vtt.lines().map { it.trim() }.filter { it.contains("-->") }

        val ours = cues(parsed.vtt)
        val theirs = cues(reference)
        println("rendered ${ours.size} cues, server ${theirs.size}, ${parsed.words.size} words")
        assertEquals(theirs, ours, "cue timing lines differ from the server's own VTT")
        assertEquals(reference.trim(), parsed.vtt.trim(), "rendered VTT differs from the server's")
    }

    @Test
    fun `word times sit inside their own cue`() {
        val parsed = WhisperTranscription.parse(Transcriber(server, client).transcribe(audio))
        assertTrue(parsed.words.isNotEmpty(), "no word timestamps came back")

        val segments =
            parsed.vtt.lines().filter { it.contains("-->") }.map { line ->
                val (a, b) = line.split("-->").map { seconds(it.trim()) }
                a to b
            }
        // A word may touch its cue's edges but must not sit outside them. This is
        // what VAD breaks: token times stay in compressed time while segment
        // times are remapped, so the two drift apart across the episode.
        val strays =
            parsed.words.filter { w ->
                val (start, end) = segments[w.segment]
                w.start < start - 0.05 || w.end > end + 0.05
            }
        println("${strays.size} of ${parsed.words.size} words outside their cue")
        strays.take(3).forEach { println("  ${it.text} ${it.start}-${it.end} vs cue ${segments[it.segment]}") }
        assertTrue(strays.size < parsed.words.size / 100, "words drifting outside their cues -- is VAD on?")
    }

    private fun seconds(ts: String): Double {
        val (h, m, s) = ts.split(":")
        return h.toInt() * 3600 + m.toInt() * 60 + s.toDouble()
    }
}

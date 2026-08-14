package com.rsstowhisper.external

import okhttp3.MediaType.Companion.toMediaType
import okhttp3.MultipartBody
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.asRequestBody
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.util.concurrent.TimeUnit

open class Transcriber(
    private val serverUrl: String,
    private val httpClient: OkHttpClient =
        OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(90, TimeUnit.MINUTES)
            .writeTimeout(30, TimeUnit.MINUTES)
            .build(),
    /**
     * Maximum segment length in characters. Without this, whisper.cpp will
     * happily emit a single segment spanning an entire episode -- 139 episodes
     * in the corpus are one cue covering the whole show, the worst over 7,200
     * seconds. A cue that long cannot carry a usable timestamp.
     *
     * ~200 characters is roughly 30-40 words, about 12 seconds of speech, so
     * it only bites on the runaway cases.
     */
    private val maxLen: Int = DEFAULT_MAX_LEN,
) {
    private val logger = LoggerFactory.getLogger(Transcriber::class.java)

    /**
     * The whisper.cpp server decodes the upload with miniaudio, which detects the
     * format from the content and resamples to 16 kHz mono itself, so the mp3 can
     * go straight up without a local ffmpeg pass.
     */
    open fun transcribe(audioPath: Path): String {
        val requestBody =
            MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart(
                    "file",
                    audioPath.fileName.toString(),
                    audioPath.toFile().asRequestBody("audio/mpeg".toMediaType()),
                )
                .addFormDataPart("language", "en")
                .addFormDataPart("response_format", "vtt")
                // whisper.cpp only applies max_len when token_timestamps is on:
                // the wrap call is nested inside `if (params.token_timestamps)`
                // in whisper_full. Sending max_len alone is silently ignored.
                .addFormDataPart("token_timestamps", "true")
                .addFormDataPart("max_len", maxLen.toString())
                // Cut on word boundaries rather than mid-token.
                .addFormDataPart("split_on_word", "true")
                .build()

        val request =
            Request.Builder()
                .url("$serverUrl/inference")
                .post(requestBody)
                .build()

        logger.debug("Sending {} to whisper server", audioPath.fileName)

        return httpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                throw RuntimeException("Whisper server returned ${response.code}: ${response.body?.string()}")
            }
            response.body?.string() ?: throw RuntimeException("Whisper server returned empty body")
        }
    }

    companion object {
        const val DEFAULT_MAX_LEN = 200
    }
}

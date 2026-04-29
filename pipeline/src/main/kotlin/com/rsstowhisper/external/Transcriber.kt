package com.rsstowhisper.external

import okhttp3.MediaType.Companion.toMediaType
import okhttp3.MultipartBody
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.asRequestBody
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.util.concurrent.TimeUnit

open class Transcriber(private val serverUrl: String) {
    private val logger = LoggerFactory.getLogger(Transcriber::class.java)
    private val httpClient =
        OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(90, TimeUnit.MINUTES)
            .writeTimeout(30, TimeUnit.MINUTES)
            .build()

    open fun transcribe(wavPath: Path): String {
        val requestBody =
            MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart(
                    "file",
                    wavPath.fileName.toString(),
                    wavPath.toFile().asRequestBody("audio/wav".toMediaType()),
                )
                .addFormDataPart("language", "en")
                .addFormDataPart("response_format", "vtt")
                .addFormDataPart("initial_prompt", PROMPT)
                .build()

        val request =
            Request.Builder()
                .url("$serverUrl/inference")
                .post(requestBody)
                .build()

        logger.debug("Sending {} to whisper server", wavPath.fileName)

        return httpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                throw RuntimeException("Whisper server returned ${response.code}: ${response.body?.string()}")
            }
            response.body?.string() ?: throw RuntimeException("Whisper server returned empty body")
        }
    }

    companion object {
        private const val PROMPT =
            "Hello, welcome to the podcast. This is a transcription with proper punctuation and capitalization."
    }
}

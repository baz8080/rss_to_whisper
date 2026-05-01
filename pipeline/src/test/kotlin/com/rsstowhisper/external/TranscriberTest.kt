package com.rsstowhisper.external

import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class TranscriberTest {
    private fun clientReturning(
        body: String = "",
        responseCode: Int = 200,
        captureRequests: MutableList<okhttp3.Request>? = null,
    ): OkHttpClient =
        OkHttpClient.Builder()
            .addInterceptor { chain ->
                captureRequests?.add(chain.request())
                Response.Builder()
                    .request(chain.request())
                    .protocol(Protocol.HTTP_1_1)
                    .code(responseCode)
                    .message(if (responseCode == 200) "OK" else "Error")
                    .body(body.toResponseBody())
                    .build()
            }
            .build()

    private fun wavFile(tmp: Path): Path = tmp.resolve("audio.wav").also { Files.writeString(it, "fake-wav-data") }

    @Test
    fun `transcribe posts to the inference endpoint`(
        @TempDir tmp: Path,
    ) {
        val requests = mutableListOf<okhttp3.Request>()
        Transcriber("http://whisper-server", clientReturning("WEBVTT\n", captureRequests = requests))
            .transcribe(wavFile(tmp))

        val request = requests.single()
        assertEquals("POST", request.method)
        assertEquals("http://whisper-server/inference", request.url.toString())
    }

    @Test
    fun `transcribe sends file as multipart with correct fields`(
        @TempDir tmp: Path,
    ) {
        val requests = mutableListOf<okhttp3.Request>()
        Transcriber("http://whisper-server", clientReturning("WEBVTT\n", captureRequests = requests))
            .transcribe(wavFile(tmp))

        val body = requests.single().body as okhttp3.MultipartBody
        val partNames = body.parts.mapNotNull { it.headers?.get("Content-Disposition") }
        assertTrue(partNames.any { it.contains("name=\"file\"") })
        assertTrue(partNames.any { it.contains("name=\"language\"") })
        assertTrue(partNames.any { it.contains("name=\"response_format\"") })
    }

    @Test
    fun `transcribe returns response body on success`(
        @TempDir tmp: Path,
    ) {
        val vtt = "WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nHello world\n"
        val result = Transcriber("http://whisper-server", clientReturning(vtt)).transcribe(wavFile(tmp))
        assertEquals(vtt, result)
    }

    @Test
    fun `transcribe throws on non-200 response`(
        @TempDir tmp: Path,
    ) {
        val ex =
            assertFailsWith<RuntimeException> {
                Transcriber("http://whisper-server", clientReturning(responseCode = 500))
                    .transcribe(wavFile(tmp))
            }
        assertTrue(ex.message!!.contains("500"))
    }

    @Test
    fun `transcribe throws on empty body`(
        @TempDir tmp: Path,
    ) {
        val client =
            OkHttpClient.Builder()
                .addInterceptor { chain ->
                    Response.Builder()
                        .request(chain.request())
                        .protocol(Protocol.HTTP_1_1)
                        .code(200)
                        .message("OK")
                        .build()
                }
                .build()

        assertFailsWith<RuntimeException> {
            Transcriber("http://whisper-server", client).transcribe(wavFile(tmp))
        }
    }
}

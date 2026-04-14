package com.rsstowhisper.feed

import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class FeedServiceTest {
    private fun clientCapturingHeaders(
        captured: MutableList<String?>,
        responseCode: Int = 200,
    ): OkHttpClient =
        OkHttpClient.Builder()
            .addInterceptor { chain ->
                captured.add(chain.request().header("User-Agent"))
                Response.Builder()
                    .request(chain.request())
                    .protocol(Protocol.HTTP_1_1)
                    .code(responseCode)
                    .message(if (responseCode == 200) "OK" else "Error")
                    .body("".toResponseBody())
                    .build()
            }
            .build()

    @Test
    fun `fetchFeed sends user-agent header`() {
        val headers = mutableListOf<String?>()
        val service = FeedService(clientCapturingHeaders(headers))

        service.fetchFeed("https://example.com/feed.rss")

        val userAgent = headers.single()
        assertNotNull(userAgent)
        assertTrue(userAgent.startsWith("rss-to-whisper/"), "Expected header to start with 'rss-to-whisper/' but was: $userAgent")
        assertTrue(userAgent.contains("github.com/baz8080/rss_to_whisper"), "Expected header to contain repo URL but was: $userAgent")
    }

    @Test
    fun `downloadAudio sends user-agent header`() {
        val headers = mutableListOf<String?>()
        val service = FeedService(clientCapturingHeaders(headers, responseCode = 404))

        service.downloadAudio("https://example.com/episode.mp3", Path.of("episode.mp3"))

        val userAgent = headers.single()
        assertNotNull(userAgent)
        assertTrue(userAgent.startsWith("rss-to-whisper/"), "Expected header to start with 'rss-to-whisper/' but was: $userAgent")
        assertTrue(userAgent.contains("github.com/baz8080/rss_to_whisper"), "Expected header to contain repo URL but was: $userAgent")
    }
}

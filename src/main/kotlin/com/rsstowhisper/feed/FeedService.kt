package com.rsstowhisper.feed

import com.rometools.rome.feed.synd.SyndFeed
import com.rometools.rome.io.SyndFeedInput
import okhttp3.OkHttpClient
import okhttp3.Request
import org.slf4j.LoggerFactory
import org.xml.sax.InputSource
import java.io.StringReader

class FeedService(private val httpClient: OkHttpClient = OkHttpClient()) {
    private val logger = LoggerFactory.getLogger(FeedService::class.java)

    fun fetchFeed(url: String): SyndFeed? {
        return try {
            val request = Request.Builder().url(url).build()
            val response = httpClient.newCall(request).execute()

            if (response.isSuccessful) {
                val body = response.body?.string()
                response.close()
                if (body != null) {
                    val input = SyndFeedInput()
                    input.build(InputSource(StringReader(body)))
                } else {
                    logger.error("Feed response body was null for $url")
                    null
                }
            } else {
                logger.error("Feed failed to load ${response.code}")
                response.close()
                null
            }
        } catch (e: Exception) {
            logger.error("Failed to get feed: $url", e)
            null
        }
    }
}

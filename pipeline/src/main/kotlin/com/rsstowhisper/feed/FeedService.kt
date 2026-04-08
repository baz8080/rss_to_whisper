package com.rsstowhisper.feed

import com.rometools.rome.feed.synd.SyndFeed
import com.rometools.rome.io.SyndFeedInput
import okhttp3.OkHttpClient
import okhttp3.Request
import org.slf4j.LoggerFactory
import org.xml.sax.InputSource
import java.io.StringReader
import java.nio.file.Files
import java.nio.file.Path

open class FeedService(private val httpClient: OkHttpClient = OkHttpClient()) {
    private val logger = LoggerFactory.getLogger(FeedService::class.java)

    open fun fetchFeed(url: String): SyndFeed? {
        return try {
            val request = Request.Builder().url(url).build()
            val body =
                httpClient.newCall(request).execute().use { response ->
                    if (response.isSuccessful) {
                        response.body?.string()
                    } else {
                        logger.error("Feed failed to load ${response.code}")
                        null
                    }
                }

            if (body != null) {
                SyndFeedInput().build(InputSource(StringReader(body)))
            } else {
                null
            }
        } catch (e: Exception) {
            logger.error("Failed to get feed: $url", e)
            null
        }
    }

    open fun downloadAudio(
        url: String,
        targetPath: Path,
    ) {
        if (Files.exists(targetPath)) {
            logger.debug("Audio is already downloaded")
            return
        }

        logger.debug("Downloading audio")
        val request = Request.Builder().url(url).build()

        try {
            httpClient.newCall(request).execute().use { response ->
                if (response.isSuccessful) {
                    logger.debug("Writing... {}", targetPath)
                    response.body?.byteStream()?.use { input ->
                        Files.newOutputStream(targetPath).use { output ->
                            input.copyTo(output)
                        }
                    }
                } else {
                    logger.error("Error saving file response: ${response.code}")
                }
            }
        } catch (e: Exception) {
            logger.error("Failed to download $url", e)
        }
    }
}

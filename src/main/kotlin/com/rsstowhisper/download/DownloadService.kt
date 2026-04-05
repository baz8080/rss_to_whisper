package com.rsstowhisper.download

import okhttp3.OkHttpClient
import okhttp3.Request
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path

open class DownloadService(private val httpClient: OkHttpClient = OkHttpClient()) {
    private val logger = LoggerFactory.getLogger(DownloadService::class.java)

    open fun downloadIfRequired(
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
                    logger.debug("Writing... $targetPath")
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

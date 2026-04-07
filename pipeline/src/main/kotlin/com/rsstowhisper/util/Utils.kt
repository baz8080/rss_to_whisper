package com.rsstowhisper.util

import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.HexFormat
import kotlin.math.pow

private val logger = LoggerFactory.getLogger("com.rsstowhisper.util.Utils")

private val CONSECUTIVE_DASHES = Regex("-{2,}")

fun getHash(content: String): String = HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(content.toByteArray()))

fun escapeFilename(filename: String?): String {
    if (filename.isNullOrEmpty()) return ""

    val escaped =
        filename
            .map { if (it.isLetterOrDigit()) it else '-' }
            .joinToString("")
            .replace(CONSECUTIVE_DASHES, "-")

    return if (escaped.endsWith("-")) escaped.dropLast(1) else escaped
}

fun timeToSeconds(timeStr: String?): Int {
    if (timeStr.isNullOrBlank()) return 0

    return try {
        val parts = timeStr.split(":")
        parts.reversed().mapIndexed { i, part ->
            val value = part.toDoubleOrNull() ?: 0.0
            value * 60.0.pow(i)
        }.sum().toInt()
    } catch (_: Exception) {
        logger.error("Couldn't parse $timeStr into seconds")
        0
    }
}

fun createPath(
    parentPath: Path,
    directoryName: String,
): Path {
    val pathToCreate = parentPath.resolve(escapeFilename(directoryName))
    Files.createDirectories(pathToCreate)
    return pathToCreate
}

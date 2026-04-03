package com.rsstowhisper.util

import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

private val logger = LoggerFactory.getLogger("com.rsstowhisper.util.Utils")

private val UUID_REGEX =
    Regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

fun isValidUuid(str: String?): Boolean {
    if (str.isNullOrBlank()) return false
    return UUID_REGEX.matches(str)
}

fun getHash(content: String?): String {
    if (content.isNullOrEmpty()) {
        throw IllegalArgumentException("Cannot hash null or empty string")
    }
    val md = MessageDigest.getInstance("MD5")
    md.update(content.toByteArray(Charsets.UTF_8))
    return md.digest().joinToString("") { "%02x".format(it) }
}

fun isWritable(path: String): Boolean = Files.isWritable(Path.of(path))

fun isWritable(path: Path): Boolean = Files.isWritable(path)

fun escapeFilename(filename: String?): String {
    if (filename.isNullOrEmpty()) return ""

    val escaped =
        filename
            .map { if (it.isLetterOrDigit()) it else '-' }
            .joinToString("")
            .replace(Regex("-{2,}"), "-")

    return if (escaped.endsWith("-")) escaped.dropLast(1) else escaped
}

fun timeToSeconds(timeStr: String?): Int {
    if (timeStr.isNullOrBlank()) return 0

    return try {
        val parts = timeStr.split(":")
        parts.reversed().mapIndexed { i, part ->
            val value = part.toDoubleOrNull() ?: 0.0
            var multiplier = 1.0
            repeat(i) { multiplier *= 60.0 }
            value * multiplier
        }.sum().toInt()
    } catch (_: Exception) {
        logger.error("Couldn't parse $timeStr into seconds")
        0
    }
}

fun createPath(
    parentPath: String?,
    directoryName: String?,
): Path? {
    if (parentPath.isNullOrEmpty() || directoryName.isNullOrEmpty()) return null
    return createPath(Path.of(parentPath), directoryName)
}

fun createPath(
    parentPath: Path?,
    directoryName: String?,
): Path? {
    if (parentPath == null || directoryName.isNullOrEmpty()) return null

    val pathToCreate = parentPath.resolve(escapeFilename(directoryName))

    if (!Files.exists(pathToCreate)) {
        Files.createDirectories(pathToCreate)
    }

    return pathToCreate
}

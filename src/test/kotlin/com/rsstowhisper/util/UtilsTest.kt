package com.rsstowhisper.util

import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class UtilsTest {
    @ParameterizedTest
    @MethodSource("uuidTestCases")
    fun `test isValidUuid`(
        input: String?,
        expected: Boolean,
    ) {
        assertEquals(expected, isValidUuid(input))
    }

    @Test
    fun `test getHash known value`() {
        val result = getHash("hello")
        assertEquals("5d41402abc4b2a76b9719d911017c592", result)
    }

    @Test
    fun `test getHash null throws`() {
        assertThrows<IllegalArgumentException> { getHash(null) }
    }

    @Test
    fun `test getHash empty throws`() {
        assertThrows<IllegalArgumentException> { getHash("") }
    }

    @ParameterizedTest
    @CsvSource(
        ",''",
        "'',''",
        "' ',''",
        "file\$name,file-name",
        "hello__world,hello-world",
        "trailing-,trailing",
        "unsafe@chars!,unsafe-chars",
    )
    fun `test escapeFilename`(
        input: String?,
        expected: String,
    ) {
        assertEquals(expected, escapeFilename(input))
    }

    @ParameterizedTest
    @CsvSource(
        ",0",
        "' ',0",
        "'unexpected string',0",
        "50,50",
        "1:30,90",
        "01:0,60",
        "1:1:15,3675",
        "01:02:20,3740",
        "::,0",
        "1:1:1:1,219661",
    )
    fun `test timeToSeconds`(
        input: String?,
        expected: Int,
    ) {
        assertEquals(expected, timeToSeconds(input))
    }

    @ParameterizedTest
    @MethodSource("createPathNullCases")
    fun `test createPath returns null for invalid inputs`(
        parentPath: String?,
        directoryName: String?,
    ) {
        assertEquals(null, createPath(parentPath, directoryName))
    }

    @Test
    fun `test isWritable with temp directory`() {
        val tempDir = java.nio.file.Files.createTempDirectory("test")
        assertTrue(isWritable(tempDir))
        java.nio.file.Files.delete(tempDir)
    }

    @Test
    fun `test isWritable with nonexistent path`() {
        assertFalse(isWritable("/nonexistent/path/that/does/not/exist"))
    }

    companion object {
        @JvmStatic
        fun uuidTestCases(): Stream<org.junit.jupiter.params.provider.Arguments> =
            Stream.of(
                org.junit.jupiter.params.provider.Arguments.of(UUID.randomUUID().toString(), true),
                org.junit.jupiter.params.provider.Arguments.of("123e4567-e89b-12d3-a456-426614174000", true),
                org.junit.jupiter.params.provider.Arguments.of("not-a-uuid", false),
                org.junit.jupiter.params.provider.Arguments.of("12345678-1234-1234-1234-1234567890", false),
                org.junit.jupiter.params.provider.Arguments.of("", false),
                org.junit.jupiter.params.provider.Arguments.of("   ", false),
                org.junit.jupiter.params.provider.Arguments.of(null, false),
            )

        @JvmStatic
        fun createPathNullCases(): Stream<org.junit.jupiter.params.provider.Arguments> =
            Stream.of(
                org.junit.jupiter.params.provider.Arguments.of(null, "pods"),
                org.junit.jupiter.params.provider.Arguments.of(null, ""),
                org.junit.jupiter.params.provider.Arguments.of("pods_parent", null),
                org.junit.jupiter.params.provider.Arguments.of("", null),
                org.junit.jupiter.params.provider.Arguments.of(null, null),
                org.junit.jupiter.params.provider.Arguments.of("", ""),
            )
    }
}

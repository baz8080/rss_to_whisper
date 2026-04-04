package com.rsstowhisper.util

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import kotlin.test.Test
import kotlin.test.assertEquals

class UtilsTest {
    @Test
    fun `test getHash known value`() {
        val result = getHash("hello")
        assertEquals("5d41402abc4b2a76b9719d911017c592", result)
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
}

package com.rsstowhisper

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import kotlin.test.assertEquals

class UtilsTest {
    @ParameterizedTest
    @CsvSource(
        ",''",
        "'',''",
        "' ',''",
        $$"file$name,file-name",
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

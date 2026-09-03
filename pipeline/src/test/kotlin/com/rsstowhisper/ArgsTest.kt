package com.rsstowhisper

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ArgsTest {
    @Test
    fun `parseArgs returns everything unset for empty argv`() {
        assertEquals(Args(), parseArgs(emptyArray()))
    }

    @Test
    fun `parseArgs reads every value flag`() {
        val args =
            parseArgs(
                arrayOf(
                    "--config",
                    "/tmp/pods.yaml",
                    "--data-dir",
                    "/tmp/data",
                    "--whisper-url",
                    "http://localhost:8081",
                ),
            )

        assertEquals("/tmp/pods.yaml", args.configPath)
        assertEquals("/tmp/data", args.dataDirectory)
        assertEquals("http://localhost:8081", args.whisperServerUrl)
    }

    @Test
    fun `parseArgs maps verbose flags to true false and null`() {
        assertEquals(true, parseArgs(arrayOf("--verbose")).verbose)
        assertEquals(false, parseArgs(arrayOf("--no-verbose")).verbose)
        assertNull(parseArgs(arrayOf("--config", "/tmp/pods.yaml")).verbose)
    }

    @Test
    fun `parseArgs recognises both help spellings`() {
        assertTrue(parseArgs(arrayOf("--help")).help)
        assertTrue(parseArgs(arrayOf("-h")).help)
    }

    @Test
    fun `parseArgs takes the last value when a flag is repeated`() {
        assertEquals("/second", parseArgs(arrayOf("--config", "/first", "--config", "/second")).configPath)
    }

    @Test
    fun `parseArgs rejects an unknown option`() {
        val ex = assertFailsWith<IllegalStateException> { parseArgs(arrayOf("--nope")) }
        assertEquals("Unknown option: --nope (try --help)", ex.message)
    }

    @Test
    fun `parseArgs rejects a bare positional`() {
        val ex = assertFailsWith<IllegalStateException> { parseArgs(arrayOf("pods.yaml")) }
        assertEquals("Unexpected argument: pods.yaml (try --help)", ex.message)
    }

    @Test
    fun `parseArgs rejects a value flag at the end of argv`() {
        val ex = assertFailsWith<IllegalStateException> { parseArgs(arrayOf("--whisper-url")) }
        assertEquals("--whisper-url needs a value (try --help)", ex.message)
    }

    @Test
    fun `parseArgs rejects a value flag followed by another flag`() {
        val ex = assertFailsWith<IllegalStateException> { parseArgs(arrayOf("--config", "--verbose")) }
        assertEquals("--config needs a value (try --help)", ex.message)
    }

    @Test
    fun `recover orphan flags are absent unless given`() {
        assertNull(parseArgs(arrayOf()).recoverOrphans)
        assertNull(parseArgs(arrayOf()).orphanRecoveryLimit)
        assertEquals(true, parseArgs(arrayOf("--recover-orphans")).recoverOrphans)
        assertEquals(false, parseArgs(arrayOf("--no-recover-orphans")).recoverOrphans)
    }

    @Test
    fun `orphan limit reads a non-negative number`() {
        assertEquals(25, parseArgs(arrayOf("--orphan-limit", "25")).orphanRecoveryLimit)
        assertEquals(0, parseArgs(arrayOf("--orphan-limit", "0")).orphanRecoveryLimit)
    }

    @Test
    fun `orphan limit rejects a missing or non-numeric value`() {
        assertFailsWith<IllegalStateException> { parseArgs(arrayOf("--orphan-limit")) }
        assertFailsWith<IllegalStateException> { parseArgs(arrayOf("--orphan-limit", "lots")) }
        assertFailsWith<IllegalStateException> { parseArgs(arrayOf("--orphan-limit", "-1")) }
    }
}

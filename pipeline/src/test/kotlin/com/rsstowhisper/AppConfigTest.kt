package com.rsstowhisper

import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class AppConfigTest {
    @Test
    fun `loadDotEnv parses key=value pairs`() {
        val result = AppConfig.loadDotEnv(
            """
            FOO=bar
            BAZ=qux
            """.trimIndent()
        )
        assertEquals(mapOf("FOO" to "bar", "BAZ" to "qux"), result)
    }

    @Test
    fun `loadDotEnv ignores blank lines and comments`() {
        val result = AppConfig.loadDotEnv(
            """
            # a comment
            FOO=bar

            # another comment
            BAZ=qux
            """.trimIndent()
        )
        assertEquals(mapOf("FOO" to "bar", "BAZ" to "qux"), result)
    }

    @Test
    fun `loadDotEnv handles empty input`() {
        assertEquals(emptyMap(), AppConfig.loadDotEnv(""))
    }

    @Test
    fun `load overrides dataDirectory and whisperServerUrl from env`(@TempDir tmp: Path) {
        val yaml = tmp.resolve("pods.yaml").toFile()
        yaml.writeText("data_directory: from-yaml\nwhisper_server_url: from-yaml\nverbose: false\n")

        val config = AppConfig.load(
            mapOf(
                "PIPELINE_CONFIG_PATH" to yaml.absolutePath,
                "PIPELINE_DATA_DIRECTORY" to "/env/data",
                "PIPELINE_WHISPER_SERVER_URL" to "http://env-whisper",
            )
        )

        assertEquals("/env/data", config.dataDirectory)
        assertEquals("http://env-whisper", config.whisperServerUrl)
    }

    @Test
    fun `load sets verbose from env`(@TempDir tmp: Path) {
        val yaml = tmp.resolve("pods.yaml").toFile()
        yaml.writeText("verbose: false\n")

        val config = AppConfig.load(
            mapOf(
                "PIPELINE_CONFIG_PATH" to yaml.absolutePath,
                "PIPELINE_DATA_DIRECTORY" to "/data",
                "PIPELINE_WHISPER_SERVER_URL" to "http://whisper",
                "PIPELINE_VERBOSE" to "true",
            )
        )

        assertEquals(true, config.verbose)
    }

    @Test
    fun `load defaults verbose to false when absent`(@TempDir tmp: Path) {
        val yaml = tmp.resolve("pods.yaml").toFile()
        yaml.writeText("podcasts: []\n")

        val config = AppConfig.load(
            mapOf(
                "PIPELINE_CONFIG_PATH" to yaml.absolutePath,
                "PIPELINE_DATA_DIRECTORY" to "/data",
                "PIPELINE_WHISPER_SERVER_URL" to "http://whisper",
            )
        )

        assertEquals(false, config.verbose)
    }

    @Test
    fun `load errors when PIPELINE_CONFIG_PATH is missing`() {
        val ex = assertFailsWith<IllegalStateException> {
            AppConfig.load(
                mapOf(
                    "PIPELINE_DATA_DIRECTORY" to "/data",
                    "PIPELINE_WHISPER_SERVER_URL" to "http://whisper",
                )
            )
        }
        assertEquals("PIPELINE_CONFIG_PATH must be set in .env", ex.message)
    }

    @Test
    fun `load errors when PIPELINE_DATA_DIRECTORY is missing`(@TempDir tmp: Path) {
        val yaml = tmp.resolve("pods.yaml").toFile()
        yaml.writeText("podcasts: []\n")

        val ex = assertFailsWith<IllegalStateException> {
            AppConfig.load(
                mapOf(
                    "PIPELINE_CONFIG_PATH" to yaml.absolutePath,
                    "PIPELINE_WHISPER_SERVER_URL" to "http://whisper",
                )
            )
        }
        assertEquals("PIPELINE_DATA_DIRECTORY must be set in .env", ex.message)
    }

    @Test
    fun `load errors when PIPELINE_WHISPER_SERVER_URL is missing`(@TempDir tmp: Path) {
        val yaml = tmp.resolve("pods.yaml").toFile()
        yaml.writeText("podcasts: []\n")

        val ex = assertFailsWith<IllegalStateException> {
            AppConfig.load(
                mapOf(
                    "PIPELINE_CONFIG_PATH" to yaml.absolutePath,
                    "PIPELINE_DATA_DIRECTORY" to "/data",
                )
            )
        }
        assertEquals("PIPELINE_WHISPER_SERVER_URL must be set in .env", ex.message)
    }
}

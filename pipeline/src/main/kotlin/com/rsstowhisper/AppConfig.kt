package com.rsstowhisper

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.File

@JsonIgnoreProperties(ignoreUnknown = true)
data class AppConfig(
    val verbose: Boolean = false,
    val dataDirectory: String = "",
    val whisperServerUrl: String = "",
    @param:JsonProperty("skip_after_consecutive") val skipAfterConsecutive: Int = 20,
    val podcasts: List<PodcastConfig> = emptyList(),
) {
    companion object {
        private val mapper =
            ObjectMapper(YAMLFactory())
                .registerModule(KotlinModule.Builder().build())

        fun load(path: String): AppConfig {
            val env = loadDotEnv()
            val raw: AppConfig = mapper.readValue(File(path))

            val dataDirectory =
                env["PIPELINE_DATA_DIRECTORY"]
                    ?: error("PIPELINE_DATA_DIRECTORY must be set in .env")
            val whisperServerUrl =
                env["PIPELINE_WHISPER_SERVER_URL"]
                    ?: error("PIPELINE_WHISPER_SERVER_URL must be set in .env")

            return raw.copy(dataDirectory = dataDirectory, whisperServerUrl = whisperServerUrl)
        }

        internal fun loadDotEnv(): Map<String, String> {
            val file = File(".env")
            if (!file.exists()) return emptyMap()
            return file
                .readLines()
                .filter { it.isNotBlank() && !it.startsWith("#") }
                .mapNotNull { line ->
                    val eq = line.indexOf('=')
                    if (eq < 0) null else line.substring(0, eq).trim() to line.substring(eq + 1).trim()
                }.toMap()
        }
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class PodcastConfig(
    val name: String,
    val url: String,
    val collections: List<String> = emptyList(),
    val excludes: List<String> = emptyList(),
)

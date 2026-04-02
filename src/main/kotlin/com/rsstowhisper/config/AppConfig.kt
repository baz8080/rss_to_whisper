package com.rsstowhisper.config

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
    @JsonProperty("data_directory") val dataDirectory: String,
    @JsonProperty("whisper_model") val whisperModel: String = "tiny",
    @JsonProperty("require_cuda") val requireCuda: Boolean = true,
    @JsonProperty("database_config") val databaseConfig: DatabaseConfig,
    val podcasts: List<PodcastConfig>,
) {
    companion object {
        private val mapper =
            ObjectMapper(YAMLFactory())
                .registerModule(KotlinModule.Builder().build())

        fun load(path: String): AppConfig = mapper.readValue(File(path))
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class DatabaseConfig(
    val server: String,
    @JsonProperty("drop_indices") val dropIndices: Boolean = false,
    @JsonProperty("process_inserts") val processInserts: Boolean = false,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class PodcastConfig(
    val name: String,
    val url: String? = null,
    val collections: List<String> = emptyList(),
    val excludes: List<String> = emptyList(),
)

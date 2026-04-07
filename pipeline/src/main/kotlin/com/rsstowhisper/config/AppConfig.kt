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
    @JsonProperty("skip_after_consecutive") val skipAfterConsecutive: Int = 20,
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
data class PodcastConfig(
    val name: String,
    val url: String? = null,
    val collections: List<String> = emptyList(),
    val excludes: List<String> = emptyList(),
)

package com.rsstowhisper.pipeline

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.rometools.modules.itunes.EntryInformation
import com.rometools.modules.itunes.FeedInformation
import com.rometools.modules.itunes.ITunes
import com.rometools.rome.feed.synd.SyndEntry
import com.rometools.rome.feed.synd.SyndFeed
import com.rsstowhisper.AppConfig
import com.rsstowhisper.PodcastConfig
import com.rsstowhisper.createPath
import com.rsstowhisper.external.AudioConverter
import com.rsstowhisper.external.Transcriber
import com.rsstowhisper.external.TranscriptWriter
import com.rsstowhisper.feed.FeedService
import com.rsstowhisper.timeToSeconds
import okhttp3.OkHttpClient
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Date
import java.util.concurrent.TimeUnit

class PodcastPipeline(
    private val config: AppConfig,
    httpClient: OkHttpClient =
        OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(5, TimeUnit.MINUTES)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build(),
    private val feedService: FeedService = FeedService(httpClient),
    private val audioConverter: AudioConverter = AudioConverter(),
    private val transcriptWriter: TranscriptWriter = TranscriptWriter(),
    private val transcriber: Transcriber = Transcriber(config.whisperModel),
) {
    private val jsonMapper =
        ObjectMapper().apply {
            enable(SerializationFeature.INDENT_OUTPUT)
            configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
        }

    fun run() {
        val dataDir = config.dataDirectory

        if (!Files.isWritable(Path.of(dataDir))) {
            logger.error("The data_dir is missing, or not writable. Cannot continue")
            return
        }

        for (podcast in config.podcasts) {
            processPodcast(podcast, dataDir)
        }
    }

    private fun processPodcast(
        podcast: PodcastConfig,
        dataDir: String,
    ) {
        val podcastUrl = podcast.url
        if (podcastUrl.isNullOrBlank()) {
            logger.error("Skipping podcast with missing URL")
            return
        }

        logger.info("Processing ${podcast.name}")
        val feed = feedService.fetchFeed(podcastUrl)

        if (feed == null) {
            logger.error("Could not fetch feed for ${podcast.name}")
            return
        }

        logger.debug("Downloaded ${podcast.url}")

        val podPath = createPath(Path.of(dataDir), podcast.name)
        val skipThreshold = config.skipAfterConsecutive
        var consecutiveTranscribed = 0

        for (entry in feed.entries) {
            val title = entry.title ?: continue

            if (podcast.excludes.any { exclude -> exclude.lowercase() in title.lowercase() }) {
                logger.debug("Skipping podcast entry because of excludes match")
                continue
            }

            try {
                val audioUrl = findAudioLink(entry)
                if (audioUrl == null) {
                    logger.warn("$title has no mp3 link. Skipping")
                    continue
                }

                val stablePrefix = episodeStablePrefix(entry, audioUrl)
                logger.debug("Processing $stablePrefix")

                val episodeDirPath = findExistingEpisodeDir(podPath, stablePrefix)
                    ?: createPath(podPath, getEpisodeDirName(entry, audioUrl))

                if (Files.exists(episodeDirPath.resolve("transcript.json"))) {
                    consecutiveTranscribed++
                    if (consecutiveTranscribed >= skipThreshold) {
                        logger.debug(
                            "Found $consecutiveTranscribed consecutive transcribed episodes. Skipping to next podcast.",
                        )
                        break
                    }
                    continue
                }
                consecutiveTranscribed = 0

                val mp3Info = getMp3Info(entry, episodeDirPath, dataDir)
                if (mp3Info == null) {
                    logger.warn("$title has no mp3 link. Skipping")
                    continue
                }

                feedService.downloadAudio(mp3Info.url, mp3Info.filePath)
                transcribeEpisode(mp3Info, episodeDirPath)

                writeEpisodeJson(feed, entry, mp3Info, episodeDirPath, podcast.collections)
            } catch (e: Exception) {
                logger.error("Couldn't process episode entry: ${entry.title}")
                logger.error(e.message, e)
            }
        }
    }

    private fun writeEpisodeJson(
        feed: SyndFeed,
        entry: SyndEntry,
        mp3Info: Mp3Info,
        episodeDirPath: Path,
        collections: List<String>,
    ) {
        val jsonPath = episodeDirPath.resolve("transcript.json")
        if (Files.exists(jsonPath)) return

        val timingPath = episodeDirPath.resolve("transcript_with_timing.tsv")
        val transcriptText = if (Files.exists(timingPath)) Files.readString(timingPath) else ""
        if (transcriptText.isEmpty()) return

        val episodeDict =
            buildEpisodeDict(feed, entry, transcriptText, mp3Info.localFilePath, collections)

        if (episodeDict != null) {
            Files.writeString(jsonPath, jsonMapper.writeValueAsString(episodeDict))
            Files.deleteIfExists(timingPath)
        }
    }

    private fun transcribeEpisode(
        mp3Info: Mp3Info,
        episodePath: Path,
    ) {
        logger.debug("Starting transcription in {}", episodePath)
        val startTime = System.currentTimeMillis()

        val wavPath = audioConverter.mp3ToWav(mp3Info.filePath)
        val segments = transcriber.transcribe(wavPath)

        transcriptWriter.writeTranscriptWithTiming(segments, episodePath.resolve("transcript_with_timing.tsv"))

        Files.deleteIfExists(mp3Info.filePath)
        Files.deleteIfExists(episodePath.resolve("transcript.txt"))

        val elapsedMinutes = (System.currentTimeMillis() - startTime) / 60000.0
        logger.debug("Transcribed in: ${"%.2f".format(elapsedMinutes)} Minutes")
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PodcastPipeline::class.java)
        private val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        private val AUDIO_MP3_TYPES = setOf("audio/mpeg", "audio/mp3")

        private fun formatDate(date: Date): String = date.toInstant().atZone(ZoneOffset.UTC).toLocalDate().format(dateFormat)

        fun audioLinkHash(audioUrl: String): String {
            val bytes = MessageDigest.getInstance("MD5").digest(audioUrl.toByteArray())
            return bytes.joinToString("") { "%02x".format(it) }.take(8)
        }

        fun episodeStablePrefix(entry: SyndEntry, audioUrl: String): String {
            val date = if (entry.publishedDate != null) formatDate(entry.publishedDate) else "unknown-date"
            return "$date-${audioLinkHash(audioUrl)}"
        }

        fun getEpisodeDirName(entry: SyndEntry, audioUrl: String): String =
            "${episodeStablePrefix(entry, audioUrl)}-${entry.title ?: "unknown"}"

        fun findExistingEpisodeDir(podPath: Path, stablePrefix: String): Path? =
            podPath.toFile()
                .listFiles()
                ?.firstOrNull { it.isDirectory && it.name.startsWith("$stablePrefix-") }
                ?.toPath()

        fun getMp3Info(
            entry: SyndEntry,
            episodePath: Path,
            dataDir: String,
        ): Mp3Info? {
            for (enclosure in entry.enclosures) {
                if (enclosure.type in AUDIO_MP3_TYPES) {
                    val filePath = episodePath.resolve("audio.mp3")
                    val relativePath = Path.of(dataDir).relativize(filePath).toString()

                    return Mp3Info(
                        url = enclosure.url,
                        filePath = filePath,
                        length = enclosure.length,
                        localFilePath = relativePath,
                    )
                }
            }

            for (link in entry.links) {
                if (link.type in AUDIO_MP3_TYPES) {
                    val filePath = episodePath.resolve("audio.mp3")
                    val relativePath = Path.of(dataDir).relativize(filePath).toString()

                    return Mp3Info(
                        url = link.href,
                        filePath = filePath,
                        length = link.length,
                        localFilePath = relativePath,
                    )
                }
            }

            return null
        }

        fun buildEpisodeDict(
            feed: SyndFeed,
            entry: SyndEntry,
            transcript: String,
            relativeMp3Path: String,
            collections: List<String>? = null,
        ): Map<String, Any?>? {
            if (transcript.isEmpty()) return null

            val audioLink = findAudioLink(entry)
            if (audioLink == null) {
                logger.error("Skipping episode because it has no MP3")
                return null
            }

            return try {
                val feedItunes = feed.getModule(ITunes.URI) as? FeedInformation
                val entryItunes = entry.getModule(ITunes.URI) as? EntryInformation

                mapOf(
                    "_id" to audioLinkHash(audioLink),
                    "podcast_collections" to (collections ?: emptyList()),
                    "podcast_title" to feed.title,
                    "podcast_link" to feed.link,
                    "podcast_language" to feed.language,
                    "podcast_copyright" to feed.copyright,
                    "podcast_author" to (feed.author ?: feedItunes?.author),
                    "podcast_image" to (feed.image?.url ?: feedItunes?.imageUri),
                    "podcast_type" to feedItunes?.type,
                    "episode_title" to entry.title,
                    "all_tags" to collectTags(feed, entry, entryItunes),
                    "episode_published_on" to entry.publishedDate?.let { formatDate(it) },
                    "episode_audio_link" to audioLink,
                    "episode_web_link" to entry.link,
                    "episode_image" to getEpisodeImage(entry, entryItunes),
                    "episode_summary" to (entry.description?.value ?: entryItunes?.summary),
                    "episode_subtitle" to entryItunes?.subtitle,
                    "episode_authors" to
                        entry.authors?.map {
                            (it as? com.rometools.rome.feed.synd.SyndPerson)?.name
                        },
                    "episode_number" to entryItunes?.episode,
                    "episode_season" to entryItunes?.season,
                    "episode_type" to entryItunes?.episodeType,
                    "episode_duration" to parseDuration(entryItunes),
                    "episode_transcript" to transcript,
                    "episode_relative_mp3_path" to relativeMp3Path,
                )
            } catch (e: Exception) {
                logger.error("Error getting podcast metadata", e)
                null
            }
        }

        private fun findAudioLink(entry: SyndEntry): String? =
            entry.enclosures
                .firstOrNull { it.type in AUDIO_MP3_TYPES }
                ?.url
                ?: entry.links
                    .firstOrNull { it.rel == "enclosure" }
                    ?.href

        private fun collectTags(
            feed: SyndFeed,
            entry: SyndEntry,
            entryItunes: EntryInformation?,
        ): List<String> {
            val tags = mutableListOf<String>()
            feed.categories?.forEach { tags.add(it.name) }
            entryItunes?.keywords?.forEach { tags.add(it) }
            entry.categories?.forEach { tags.add(it.name) }
            return tags.map { it.trim().lowercase() }.distinct().filter { it.length > 2 }
        }

        private fun getEpisodeImage(
            entry: SyndEntry,
            entryItunes: EntryInformation?,
        ): String? =
            entryItunes?.imageUri
                ?: entry.foreignMarkup?.find { it.name == "image" }?.getAttributeValue("href")

        private fun parseDuration(entryItunes: EntryInformation?): Any? =
            entryItunes?.duration?.let { duration ->
                val ms = duration.milliseconds
                if (ms > 0) {
                    ms / 1000
                } else {
                    val durationStr = duration.toString()
                    if (":" in durationStr) timeToSeconds(durationStr) else null
                }
            }
    }
}

data class Mp3Info(
    val url: String,
    val filePath: Path,
    val length: Long,
    val localFilePath: String,
)

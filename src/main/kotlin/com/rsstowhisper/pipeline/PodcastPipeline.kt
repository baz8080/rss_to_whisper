package com.rsstowhisper.pipeline

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.rometools.modules.itunes.EntryInformation
import com.rometools.modules.itunes.FeedInformation
import com.rometools.modules.itunes.ITunes
import com.rometools.rome.feed.synd.SyndEntry
import com.rometools.rome.feed.synd.SyndFeed
import com.rsstowhisper.audio.AudioConverter
import com.rsstowhisper.config.AppConfig
import com.rsstowhisper.config.PodcastConfig
import com.rsstowhisper.download.DownloadService
import com.rsstowhisper.feed.FeedService
import com.rsstowhisper.indexing.ElasticService
import com.rsstowhisper.transcription.TranscriptWriter
import com.rsstowhisper.transcription.TranscriptionService
import com.rsstowhisper.util.createPath
import com.rsstowhisper.util.getHash
import com.rsstowhisper.util.isWritable
import com.rsstowhisper.util.timeToSeconds
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.text.SimpleDateFormat

class PodcastPipeline(
    private val config: AppConfig,
    private val feedService: FeedService = FeedService(),
    private val downloadService: DownloadService = DownloadService(),
    private val audioConverter: AudioConverter = AudioConverter(),
    private val transcriptWriter: TranscriptWriter = TranscriptWriter(),
) {
    private val logger = LoggerFactory.getLogger(PodcastPipeline::class.java)
    private val jsonMapper =
        ObjectMapper().apply {
            enable(SerializationFeature.INDENT_OUTPUT)
            configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
        }

    private var transcriptionService: TranscriptionService? = null

    fun run() {
        val dataDir = config.dataDirectory

        if (!isWritable(dataDir)) {
            logger.error("The data_dir is missing, or not writable. Cannot continue")
            return
        }

        val elasticClient =
            initElasticClient(
                config.databaseConfig.server,
                System.getenv("ELASTIC_API_KEY") ?: io.github.cdimascio.dotenv.dotenv { ignoreIfMissing = true }["ELASTIC_API_KEY"],
                config.databaseConfig.dropIndices,
            )

        for (podcast in config.podcasts) {
            processPodcast(podcast, dataDir, elasticClient)
        }
    }

    private fun initElasticClient(
        host: String,
        apiKey: String?,
        dropIndices: Boolean,
    ): ElasticService {
        val service = ElasticService(host, apiKey)
        if (dropIndices) {
            service.dropAndRecreateIndex("podcasts")
        }
        return service
    }

    private fun processPodcast(
        podcast: PodcastConfig,
        dataDir: String,
        elasticClient: ElasticService,
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

        val podPath = createPath(dataDir, podcast.name)
        if (podPath == null) {
            logger.error("Cannot get a path to write podcast to")
            return
        }

        val episodeDicts = mutableListOf<Map<String, Any?>>()

        for (entry in feed.entries) {
            val title = entry.title ?: continue

            if (podcast.excludes.any { exclude -> exclude.lowercase() in title.lowercase() }) {
                logger.debug("Skipping podcast entry because of excludes match")
                continue
            }

            try {
                val entryTitleAndDate = getEpisodeTitleWithDate(entry)
                logger.debug("Processing $entryTitleAndDate")

                val episodeDirPath = createPath(podPath, entryTitleAndDate)
                if (episodeDirPath == null) {
                    logger.error("Failed to make directory for the episode")
                    continue
                }

                val mp3Info = getMp3Info(entry, episodeDirPath, dataDir)
                if (mp3Info == null) {
                    logger.warn("$title has no mp3 link. Skipping")
                    continue
                }

                downloadService.downloadIfRequired(mp3Info.url, mp3Info.filePath)

                if (transcriptionService == null) {
                    transcriptionService = TranscriptionService(config.whisperModel)
                }

                val wasAlreadyTranscribed = transcribeIfRequired(mp3Info, episodeDirPath)

                if (wasAlreadyTranscribed) {
                    logger.debug("Found already transcribed episode. Skipping to next podcast.")
                    break
                }

                val timingPath = episodeDirPath.resolve("transcript_with_timing.tsv")
                val transcriptText = if (Files.exists(timingPath)) Files.readString(timingPath) else ""

                val episodeDict =
                    buildEpisodeDict(
                        feed,
                        entry,
                        transcriptText,
                        mp3Info.localFilePath,
                        podcast.collections,
                    )

                if (episodeDict != null) {
                    episodeDicts.add(episodeDict)

                    val jsonPath = episodeDirPath.resolve("transcript.json")
                    if (!Files.exists(jsonPath)) {
                        val jsonData = jsonMapper.writeValueAsString(episodeDict)
                        Files.writeString(jsonPath, jsonData)
                    }
                }
            } catch (e: Exception) {
                logger.error("Couldn't process episode entry: ${entry.title}")
                logger.error(e.message, e)
            }
        }

        if (config.databaseConfig.processInserts) {
            elasticClient.bulkIndex(episodeDicts)
        }
    }

    private fun transcribeIfRequired(
        mp3Info: Mp3Info,
        episodePath: Path,
    ): Boolean {
        val markerFile = episodePath.resolve("transcribed")
        val alreadyTranscribed = Files.exists(markerFile)

        if (!alreadyTranscribed) {
            logger.debug("Starting transcription in $episodePath")
            val startTime = System.currentTimeMillis()

            val wavPath = audioConverter.mp3ToWav(mp3Info.filePath)
            val segments = transcriptionService!!.transcribe(wavPath)

            transcriptWriter.writeTranscriptTsv(segments, episodePath.resolve("transcript.tsv"))
            transcriptWriter.writeTranscriptWithTiming(segments, episodePath.resolve("transcript_with_timing.tsv"))

            // Clean up: remove MP3 (keep WAV) and legacy txt
            Files.deleteIfExists(mp3Info.filePath)
            Files.deleteIfExists(episodePath.resolve("transcript.txt"))

            Files.createFile(markerFile)

            val elapsedMinutes = (System.currentTimeMillis() - startTime) / 60000.0
            logger.debug("Transcribed in: ${"%.2f".format(elapsedMinutes)} Minutes")
        } else {
            logger.debug("Audio is already transcribed.")
        }

        return alreadyTranscribed
    }

    companion object {
        private val dateFormat = SimpleDateFormat("yyyy-MM-dd")

        fun getEpisodeTitleWithDate(entry: SyndEntry): String {
            val date =
                if (entry.publishedDate != null) {
                    dateFormat.format(entry.publishedDate)
                } else {
                    "unknown-date"
                }
            return "$date-${entry.title}"
        }

        fun getMp3Info(
            entry: SyndEntry,
            episodePath: Path,
            dataDir: String,
        ): Mp3Info? {
            for (enclosure in entry.enclosures) {
                if (enclosure.type == "audio/mpeg" || enclosure.type == "audio/mp3") {
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

            // Also check links if no enclosures match
            for (link in entry.links) {
                if (link.type == "audio/mpeg" || link.type == "audio/mp3") {
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
            val safeCollections = collections ?: emptyList()

            if (transcript.isEmpty()) return null

            val id = getHash(transcript)

            // Find audio link
            val audioLink =
                entry.enclosures
                    .firstOrNull { it.type == "audio/mpeg" || it.type == "audio/mp3" }
                    ?.url
                    ?: entry.links
                        .firstOrNull { it.rel == "enclosure" }
                        ?.href

            if (audioLink == null) {
                LoggerFactory.getLogger(PodcastPipeline::class.java)
                    .error("Skipping episode because it has no MP3")
                return null
            }

            return try {
                val allTags = mutableListOf<String>()

                // Podcast tags
                feed.categories?.forEach { allTags.add(it.name) }

                // iTunes feed info
                val feedItunes = feed.getModule(ITunes.URI) as? FeedInformation

                // Podcast metadata
                val podcastImage = feed.image?.url ?: feedItunes?.imageUri
                val podcastType = feedItunes?.type

                // Episode iTunes metadata
                val entryItunes = entry.getModule(ITunes.URI) as? EntryInformation

                // Episode keywords from iTunes
                entryItunes?.keywords?.forEach { allTags.add(it) }

                // Episode tags
                entry.categories?.forEach { allTags.add(it.name) }

                // Duration - ROME's Duration has getMilliseconds()
                val episodeDuration: Any? =
                    entryItunes?.duration?.let { duration ->
                        val ms = duration.milliseconds
                        if (ms > 0) {
                            ms / 1000
                        } else {
                            val durationStr = duration.toString()
                            if (":" in durationStr) timeToSeconds(durationStr) else null
                        }
                    }

                // Episode published date
                val episodePublishedOn =
                    if (entry.publishedDate != null) {
                        dateFormat.format(entry.publishedDate)
                    } else {
                        null
                    }

                // Episode image
                val episodeImage =
                    entryItunes?.imageUri
                        ?: entry.foreignMarkup?.find { it.name == "image" }?.getAttributeValue("href")

                // Tag normalisation & deduplication
                val normalisedTags =
                    allTags
                        .map { it.lowercase() }
                        .distinct()

                mapOf(
                    "_id" to id,
                    "_index" to "podcasts",
                    "podcast_collections" to safeCollections,
                    "podcast_title" to feed.title,
                    "podcast_link" to feed.link,
                    "podcast_language" to feed.language,
                    "podcast_copyright" to feed.copyright,
                    "podcast_author" to (feed.author ?: feedItunes?.author),
                    "podcast_image" to podcastImage,
                    "podcast_type" to podcastType,
                    "episode_title" to entry.title,
                    "all_tags" to normalisedTags,
                    "episode_published_on" to episodePublishedOn,
                    "episode_audio_link" to audioLink,
                    "episode_web_link" to entry.link,
                    "episode_image" to episodeImage,
                    "episode_summary" to (entry.description?.value ?: entryItunes?.summary),
                    "episode_subtitle" to entryItunes?.subtitle,
                    "episode_authors" to entry.authors?.map { (it as? com.rometools.rome.feed.synd.SyndPerson)?.name },
                    "episode_number" to entryItunes?.episode,
                    "episode_season" to entryItunes?.season,
                    "episode_type" to entryItunes?.episodeType,
                    "episode_duration" to episodeDuration,
                    "episode_transcript" to transcript,
                    "episode_relative_mp3_path" to relativeMp3Path,
                )
            } catch (e: Exception) {
                LoggerFactory.getLogger(PodcastPipeline::class.java)
                    .error("Error getting podcast metadata", e)
                null
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

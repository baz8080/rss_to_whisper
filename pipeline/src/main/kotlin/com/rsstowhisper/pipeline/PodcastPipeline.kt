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
import com.rsstowhisper.escapeFilename
import com.rsstowhisper.external.Transcriber
import com.rsstowhisper.external.WhisperTranscription
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
    private val transcriber: Transcriber = Transcriber(config.whisperServerUrl),
) {
    // Anchored on word boundaries so "repeat" does not also swallow "repeating".
    private val excludeKeywordRegex: Regex? =
        config.excludeTitleKeywords
            .filter { it.isNotBlank() }
            .takeIf { it.isNotEmpty() }
            ?.joinToString("|") { """\b${Regex.escape(it.trim())}\b""" }
            ?.toRegex(RegexOption.IGNORE_CASE)

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
        logger.info("Processing ${podcast.name}")
        val feed = feedService.fetchFeed(podcast.url)

        if (feed == null) {
            logger.error("Could not fetch feed for ${podcast.name}")
            return
        }

        logger.debug("Downloaded ${podcast.url}")

        val podPath = createPath(Path.of(dataDir), podcast.name)
        val skipThreshold = config.skipAfterConsecutive
        val minDuration = podcast.minEpisodeDurationSeconds ?: config.minEpisodeDurationSeconds
        var consecutiveTranscribed = 0

        for (entry in feed.entries) {
            val title = entry.title ?: continue

            if (podcast.excludes.any { exclude -> exclude.lowercase() in title.lowercase() }) {
                logger.debug("Skipping podcast entry because of excludes match")
                continue
            }

            if (excludeKeywordRegex?.containsMatchIn(title) == true) {
                logger.debug("Skipping $title because of a global keyword match")
                continue
            }

            // A feed that omits itunes:duration must not be filtered out on a guess.
            val duration = parseDuration(entry.getModule(ITunes.URI) as? EntryInformation)
            if (duration != null && duration < minDuration) {
                logger.debug("Skipping $title because it is ${duration}s, under the ${minDuration}s minimum")
                continue
            }

            if (entry.uri.isNullOrBlank()) {
                logger.warn("$title has no GUID. Skipping")
                continue
            }

            try {
                val audioUrl = findAudioLink(entry)
                if (audioUrl == null) {
                    logger.warn("$title has no mp3 link. Skipping")
                    continue
                }

                val stablePrefix = episodeStablePrefix(entry)
                logger.debug("Processing $stablePrefix")

                val episodeDirPath =
                    findExistingEpisodeDir(podPath, stablePrefix)
                        ?: createPath(podPath, getEpisodeDirName(entry))

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

                if (!feedService.downloadAudio(mp3Info.url, mp3Info.filePath)) {
                    logger.warn("Could not download audio for $title. Skipping")
                    continue
                }
                val transcription = WhisperTranscription.parse(transcribeEpisode(mp3Info, episodeDirPath))
                writeEpisodeJson(feed, entry, mp3Info, episodeDirPath, podcast.collections, transcription)
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
        transcription: WhisperTranscription,
    ) {
        val jsonPath = episodeDirPath.resolve("transcript.json")
        if (Files.exists(jsonPath)) return
        if (transcription.isEmpty) {
            logger.warn("Transcription for ${entry.title} came back empty; not writing transcript.json")
            return
        }

        val episodeDict =
            buildEpisodeDict(feed, entry, transcription.vtt, mp3Info.localFilePath, collections)

        if (episodeDict != null) {
            // Words FIRST. transcript.json existing is what marks an episode
            // done, both here and for anything reading the tree, so writing it
            // last means a crash in between leaves the episode to be redone
            // rather than leaving it permanently without its sidecar.
            writeWords(transcription, episodeDirPath, entry)
            Files.writeString(jsonPath, jsonMapper.writeValueAsString(episodeDict))
        }
    }

    private fun writeWords(
        transcription: WhisperTranscription,
        episodeDirPath: Path,
        entry: SyndEntry,
    ) {
        if (transcription.words.isEmpty()) {
            logger.warn("No word timestamps for ${entry.title}; is token_timestamps still set?")
            return
        }
        try {
            transcription.writeWords(episodeDirPath.resolve(WhisperTranscription.WORDS_FILENAME))
        } catch (e: Exception) {
            // Not fatal. The transcript is the artifact the pipeline exists to
            // produce; the sidecar is an enrichment and can be rebuilt.
            logger.error("Could not write word timestamps for ${entry.title}", e)
        }
    }

    private fun transcribeEpisode(
        mp3Info: Mp3Info,
        episodePath: Path,
    ): String {
        logger.debug("Starting transcription in {}", episodePath)
        val startTime = System.currentTimeMillis()

        val vtt = transcriber.transcribe(mp3Info.filePath)

        // The mp3 is now the retained artifact -- the whisper server decodes and
        // resamples it itself, so the old audio.wav is dead weight.
        Files.deleteIfExists(episodePath.resolve("audio.wav"))
        Files.deleteIfExists(episodePath.resolve("transcript.txt"))

        val elapsedMinutes = (System.currentTimeMillis() - startTime) / 60000.0
        logger.debug("Transcribed in: ${"%.2f".format(elapsedMinutes)} Minutes")

        return vtt
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PodcastPipeline::class.java)
        private val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        private val AUDIO_MP3_TYPES = setOf("audio/mpeg", "audio/mp3")

        private fun formatDate(date: Date): String = date.toInstant().atZone(ZoneOffset.UTC).toLocalDate().format(dateFormat)

        fun md5Hash8(value: String): String {
            val bytes = MessageDigest.getInstance("MD5").digest(value.toByteArray())
            return bytes.joinToString("") { "%02x".format(it) }.take(8)
        }

        fun episodeId(entry: SyndEntry): String = md5Hash8(entry.uri!!)

        fun episodeStablePrefix(entry: SyndEntry): String {
            val date = if (entry.publishedDate != null) formatDate(entry.publishedDate) else "unknown-date"
            return "$date-${episodeId(entry)}"
        }

        /**
         * A directory name with no `-<hex8>-` component cannot be resolved back
         * to its audio: 97 episodes once stored one, and the keys pointed at
         * nothing until a repair pass derived them from disk. Nothing about the
         * construction below can produce that today, which is exactly why it is
         * worth asserting -- a silent recurrence costs a re-ingest to find.
         */
        internal val EPISODE_DIR_PREFIX = Regex("^\\d{4}-\\d{2}-\\d{2}-[0-9a-f]{8}-")

        fun getEpisodeDirName(entry: SyndEntry): String {
            val name = "${episodeStablePrefix(entry)}-${entry.title ?: "unknown"}"
            require(EPISODE_DIR_PREFIX.containsMatchIn(escapeFilename(name))) {
                "Episode directory name is missing its date-id prefix: $name"
            }
            return name
        }

        fun findExistingEpisodeDir(
            podPath: Path,
            stablePrefix: String,
        ): Path? =
            podPath.toFile()
                .listFiles()
                ?.firstOrNull { it.isDirectory && it.name.startsWith("$stablePrefix-") }
                ?.toPath()

        fun getMp3Info(
            entry: SyndEntry,
            episodePath: Path,
            dataDir: String,
        ): Mp3Info? {
            val source = findAudioSource(entry) ?: return null
            val filePath = episodePath.resolve("audio.mp3")

            return Mp3Info(
                url = source.url,
                filePath = filePath,
                length = source.length,
                localFilePath = Path.of(dataDir).relativize(filePath).toString(),
            )
        }

        fun buildEpisodeDict(
            feed: SyndFeed,
            entry: SyndEntry,
            transcript: String,
            relativeAudioPath: String,
            collections: List<String>? = null,
        ): Map<String, Any?>? {
            if (transcript.isEmpty()) return null

            val guid = entry.uri?.takeIf { it.isNotBlank() }
            if (guid == null) {
                logger.error("Skipping episode because it has no GUID")
                return null
            }

            val audioLink = findAudioLink(entry)
            if (audioLink == null) {
                logger.error("Skipping episode because it has no MP3")
                return null
            }

            return try {
                val feedItunes = feed.getModule(ITunes.URI) as? FeedInformation
                val entryItunes = entry.getModule(ITunes.URI) as? EntryInformation

                mapOf<String, Any?>(
                    "_id" to episodeId(entry),
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
                            it?.name
                        },
                    "episode_number" to entryItunes?.episode,
                    "episode_season" to entryItunes?.season,
                    "episode_type" to entryItunes?.episodeType,
                    "episode_duration" to parseDuration(entryItunes),
                    "episode_transcript" to transcript,
                    "episode_relative_audio_path" to relativeAudioPath,
                )
            } catch (e: Exception) {
                logger.error("Error getting podcast metadata", e)
                null
            }
        }

        private data class AudioSource(val url: String, val length: Long)

        // Single source of truth for locating an episode's audio, in strict
        // preference order: mp3 enclosure, then mp3-typed link, then a link that
        // declares itself an enclosure without saying what it is. Keeping one
        // predicate means findAudioLink and getMp3Info can never disagree about
        // whether an episode has audio.
        //
        // The last fallback is deliberately limited to *untyped* links. A link
        // that says rel="enclosure" type="video/mp4" is telling us it is not
        // audio; downloading it as audio.mp3 would leave a file whisper cannot
        // decode on disk, and since the file's presence is what marks a download
        // as done, every later run would re-upload and re-transcribe it forever.
        private fun findAudioSource(entry: SyndEntry): AudioSource? {
            entry.enclosures
                .firstOrNull { it.type in AUDIO_MP3_TYPES }
                ?.let { return AudioSource(it.url, it.length) }

            val links = entry.links.orEmpty()
            val link =
                links.firstOrNull { it.type in AUDIO_MP3_TYPES }
                    ?: links.firstOrNull { it.rel == "enclosure" && it.type.isNullOrBlank() }

            return link?.let { AudioSource(it.href, it.length) }
        }

        private fun findAudioLink(entry: SyndEntry): String? = findAudioSource(entry)?.url

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

        internal fun parseDuration(entryItunes: EntryInformation?): Int? =
            entryItunes?.duration?.let { duration ->
                val ms = duration.milliseconds
                if (ms > 0) {
                    (ms / 1000).toInt()
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

package com.rsstowhisper.pipeline

import com.rometools.rome.feed.synd.SyndEnclosureImpl
import com.rometools.rome.feed.synd.SyndEntryImpl
import com.rometools.rome.feed.synd.SyndFeed
import com.rometools.rome.feed.synd.SyndFeedImpl
import com.rsstowhisper.AppConfig
import com.rsstowhisper.PodcastConfig
import com.rsstowhisper.escapeFilename
import com.rsstowhisper.external.AudioConverter
import com.rsstowhisper.external.Transcriber
import com.rsstowhisper.feed.FeedService
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.Date
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private const val FAKE_SERVER_URL = "http://localhost:9000"

private val MINIMAL_VTT = "WEBVTT\n\n00:00:00.000 --> 00:00:01.000\n Hello world.\n"

class PodcastPipelineRunTest {
    private class FakeFeedService(private val feeds: Map<String, SyndFeed?>) : FeedService() {
        val requestedUrls = mutableListOf<String>()
        val downloads = mutableListOf<Pair<String, Path>>()

        override fun fetchFeed(url: String): SyndFeed? {
            requestedUrls.add(url)
            return feeds[url]
        }

        override fun downloadAudio(
            url: String,
            targetPath: Path,
        ) {
            downloads.add(url to targetPath)
            Files.createDirectories(targetPath.parent)
            Files.writeString(targetPath, "fake-mp3-bytes")
        }
    }

    private class FakeAudioConverter : AudioConverter() {
        override fun mp3ToWav(mp3Path: Path): Path {
            val wavPath = mp3Path.resolveSibling("audio.wav")
            Files.writeString(wavPath, "fake-wav-bytes")
            return wavPath
        }
    }

    private class FakeTranscriber(
        serverUrl: String,
        private val vtt: String,
    ) : Transcriber(serverUrl) {
        val calls = mutableListOf<Path>()

        override fun transcribe(wavPath: Path): String {
            calls.add(wavPath)
            return vtt
        }
    }

    private fun makeEntry(
        title: String?,
        audioUrl: String? = "https://cdn/ep.mp3",
        publishedDate: Date? = Date(1_700_000_000_000L),
        guid: String? = "https://example.com/guid/$title",
    ): SyndEntryImpl =
        SyndEntryImpl().apply {
            this.title = title
            this.link = "https://example.com/ep"
            this.publishedDate = publishedDate
            this.uri = guid
            this.enclosures =
                if (audioUrl != null) {
                    listOf(
                        SyndEnclosureImpl().apply {
                            url = audioUrl
                            type = "audio/mpeg"
                            length = 1L
                        },
                    )
                } else {
                    emptyList()
                }
        }

    private fun makeFeed(vararg entries: SyndEntryImpl): SyndFeed =
        SyndFeedImpl().apply {
            this.feedType = "rss_2.0"
            this.title = "Test Podcast"
            this.link = "https://example.com"
            this.entries = entries.toMutableList()
        }

    private fun buildPipeline(
        dataDir: Path,
        podcasts: List<PodcastConfig>,
        feed: SyndFeed?,
        vtt: String = MINIMAL_VTT,
        feedUrl: String = "https://feed",
        skipAfterConsecutive: Int = 20,
    ): Triple<PodcastPipeline, FakeTranscriber, FakeFeedService> {
        val config =
            AppConfig(
                dataDirectory = dataDir.toAbsolutePath().toString(),
                whisperServerUrl = FAKE_SERVER_URL,
                skipAfterConsecutive = skipAfterConsecutive,
                podcasts = podcasts,
            )
        val feedSvc = FakeFeedService(mapOf(feedUrl to feed))
        val txSvc = FakeTranscriber(FAKE_SERVER_URL, vtt)
        val pipeline =
            PodcastPipeline(
                config = config,
                feedService = feedSvc,
                audioConverter = FakeAudioConverter(),
                transcriber = txSvc,
            )
        return Triple(pipeline, txSvc, feedSvc)
    }

    @Test
    fun `run transcribes episode and writes json output`(
        @TempDir tempDir: Path,
    ) {
        val feed = makeFeed(makeEntry("My Episode"))
        val (pipeline, txSvc, _) =
            buildPipeline(
                tempDir,
                listOf(PodcastConfig(name = "Show", url = "https://feed")),
                feed,
            )

        pipeline.run()

        val podcastDir = tempDir.resolve("Show")
        assertTrue(Files.isDirectory(podcastDir))
        val episodeDir = Files.list(podcastDir).use { it.toList() }.single()
        assertTrue(Files.exists(episodeDir.resolve("transcript.json")))
        assertFalse(Files.exists(episodeDir.resolve("audio.mp3"))) // mp3 cleaned up
        assertTrue(Files.exists(episodeDir.resolve("audio.wav"))) // wav retained
        assertEquals(1, txSvc.calls.size)

        val json = Files.readString(episodeDir.resolve("transcript.json"))
        assertTrue(json.contains("\"episode_title\""))
        assertTrue(json.contains("My Episode"))
    }

    @Test
    fun `run skips entries whose title matches excludes case-insensitively`(
        @TempDir tempDir: Path,
    ) {
        val feed =
            makeFeed(
                makeEntry("Bonus: Patreon Only"),
                makeEntry("Episode 42: Real Content"),
            )
        val (pipeline, txSvc, _) =
            buildPipeline(
                tempDir,
                listOf(
                    PodcastConfig(
                        name = "Show",
                        url = "https://feed",
                        excludes = listOf("PATREON"),
                    ),
                ),
                feed,
            )

        pipeline.run()

        assertEquals(1, txSvc.calls.size)
        val episodes = Files.list(tempDir.resolve("Show")).use { it.toList() }
        assertEquals(1, episodes.size)
        assertTrue(episodes[0].fileName.toString().contains("Real-Content"))
    }

    @Test
    fun `run continues past isolated transcribed episodes and transcribes gaps`(
        @TempDir tempDir: Path,
    ) {
        val newEntry = makeEntry("New Episode", audioUrl = "https://cdn/new.mp3")
        val oldEntry = makeEntry("Old Episode", audioUrl = "https://cdn/old.mp3")
        val feed = makeFeed(newEntry, oldEntry)

        val (pipeline, txSvc, _) =
            buildPipeline(
                tempDir,
                listOf(PodcastConfig(name = "Show", url = "https://feed")),
                feed,
                skipAfterConsecutive = 2,
            )

        val podcastDir = Files.createDirectories(tempDir.resolve("Show"))
        val newEpisodeDir =
            Files.createDirectories(
                podcastDir.resolve(escapeFilename(PodcastPipeline.getEpisodeDirName(newEntry))),
            )
        Files.writeString(newEpisodeDir.resolve("transcript.json"), "{}")

        pipeline.run()

        assertEquals(1, txSvc.calls.size)
        val oldDir = podcastDir.resolve(escapeFilename(PodcastPipeline.getEpisodeDirName(oldEntry)))
        assertTrue(Files.exists(oldDir.resolve("transcript.json")))
    }

    @Test
    fun `run skips remainder of feed after threshold consecutive transcribed`(
        @TempDir tempDir: Path,
    ) {
        val e1 = makeEntry("Ep One")
        val e2 = makeEntry("Ep Two")
        val e3 = makeEntry("Ep Three")
        val e4 = makeEntry("Ep Four")
        val feed = makeFeed(e1, e2, e3, e4)

        val (pipeline, txSvc, _) =
            buildPipeline(
                tempDir,
                listOf(PodcastConfig(name = "Show", url = "https://feed")),
                feed,
                skipAfterConsecutive = 3,
            )

        val podcastDir = Files.createDirectories(tempDir.resolve("Show"))
        for (e in listOf(e1, e2, e3)) {
            val d =
                Files.createDirectories(
                    podcastDir.resolve(escapeFilename(PodcastPipeline.getEpisodeDirName(e))),
                )
            Files.writeString(d.resolve("transcript.json"), "{}")
        }

        pipeline.run()

        assertEquals(0, txSvc.calls.size)
        val fourthDir = podcastDir.resolve(escapeFilename(PodcastPipeline.getEpisodeDirName(e4)))
        assertFalse(Files.exists(fourthDir))
    }

    @Test
    fun `run continues past podcasts whose feed fails to fetch`(
        @TempDir tempDir: Path,
    ) {
        val (pipeline, txSvc, feedSvc) =
            buildPipeline(
                tempDir,
                listOf(PodcastConfig(name = "Broken", url = "https://feed")),
                feed = null,
            )

        pipeline.run()

        assertEquals(1, feedSvc.requestedUrls.size)
        assertEquals(0, txSvc.calls.size)
    }

    @Test
    fun `run skips entries without audio enclosure but processes siblings`(
        @TempDir tempDir: Path,
    ) {
        val feed =
            makeFeed(
                makeEntry("No Audio", audioUrl = null),
                makeEntry("Has Audio"),
            )
        val (pipeline, txSvc, _) =
            buildPipeline(
                tempDir,
                listOf(PodcastConfig(name = "Show", url = "https://feed")),
                feed,
            )

        pipeline.run()

        assertEquals(1, txSvc.calls.size)
        val episodeDirs = Files.list(tempDir.resolve("Show")).use { it.toList() }
        assertEquals(1, episodeDirs.size)
        assertTrue(episodeDirs[0].fileName.toString().contains("Has-Audio"))
        assertTrue(Files.exists(episodeDirs[0].resolve("transcript.json")))
    }

    @Test
    fun `run skips entries with null title`(
        @TempDir tempDir: Path,
    ) {
        val feed = makeFeed(makeEntry(title = null), makeEntry("Good"))
        val (pipeline, txSvc, _) =
            buildPipeline(
                tempDir,
                listOf(PodcastConfig(name = "Show", url = "https://feed")),
                feed,
            )

        pipeline.run()

        assertEquals(1, txSvc.calls.size)
    }

    @Test
    fun `run bails out early when data directory is not writable`(
        @TempDir tempDir: Path,
    ) {
        val missing = tempDir.resolve("does-not-exist")
        val config =
            AppConfig(
                dataDirectory = missing.toAbsolutePath().toString(),
                whisperServerUrl = FAKE_SERVER_URL,
                podcasts = listOf(PodcastConfig(name = "Show", url = "https://feed")),
            )
        val feedSvc = FakeFeedService(mapOf("https://feed" to makeFeed(makeEntry("E"))))
        val txSvc = FakeTranscriber(FAKE_SERVER_URL, MINIMAL_VTT)
        val pipeline =
            PodcastPipeline(
                config = config,
                feedService = feedSvc,
                audioConverter = FakeAudioConverter(),
                transcriber = txSvc,
            )

        pipeline.run()

        assertEquals(0, txSvc.calls.size)
        assertTrue(feedSvc.requestedUrls.isEmpty())
    }

    @Test
    fun `run produces transcript json with expected fields and content`(
        @TempDir tempDir: Path,
    ) {
        val vtt =
            "WEBVTT\n\n" +
                "00:00:00.000 --> 00:00:01.500\n First sentence.\n\n" +
                "00:00:01.500 --> 00:00:03.000\n Second sentence.\n"
        val feed = makeFeed(makeEntry("Episode"))
        val (pipeline, _, _) =
            buildPipeline(
                tempDir,
                listOf(PodcastConfig(name = "Show", url = "https://feed", collections = listOf("tech"))),
                feed,
                vtt = vtt,
            )

        pipeline.run()

        val episodeDir = Files.list(tempDir.resolve("Show")).use { it.toList() }.single()
        val json = Files.readString(episodeDir.resolve("transcript.json"))
        assertTrue(json.contains("First sentence."))
        assertTrue(json.contains("Second sentence."))
        assertTrue(json.contains("\"podcast_collections\""))
        assertTrue(json.contains("tech"))
        assertTrue(json.contains("\"episode_relative_audio_path\""))
        assertTrue(json.contains("Show/"))
    }
}

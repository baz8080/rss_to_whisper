package com.rsstowhisper.config

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AppConfigTest {
    @Test
    fun `test loads pods yaml`() {
        val config = AppConfig.load("pods.yaml")

        assertEquals("/Volumes/rss_to_whisper", config.dataDirectory)
        assertEquals("turbo", config.whisperModel)
        assertFalse(config.requireCuda)
        assertFalse(config.verbose)
        assertEquals("http://nasty.local:9200", config.databaseConfig.server)
        assertFalse(config.databaseConfig.dropIndices)
        assertFalse(config.databaseConfig.processInserts)
        assertTrue(config.podcasts.isNotEmpty())
    }

    @Test
    fun `test first podcast has expected values`() {
        val config = AppConfig.load("pods.yaml")
        val first = config.podcasts.first()

        assertEquals("Ask a Spaceman", first.name)
        assertEquals("http://feeds.libsyn.com/60664", first.url)
        assertEquals(listOf("science", "space", "astrophysics"), first.collections)
        assertTrue(first.excludes.isEmpty())
    }

    @Test
    fun `test podcast with excludes`() {
        val config = AppConfig.load("pods.yaml")
        val shiteTalk = config.podcasts.first { it.name == "Shite Talk" }

        assertEquals(listOf("talking shite"), shiteTalk.excludes)
        assertEquals(listOf("irish history", "history"), shiteTalk.collections)
    }

    @Test
    fun `test podcast with typo collection is ignored`() {
        val config = AppConfig.load("pods.yaml")
        val titanium = config.podcasts.first { it.name == "Titanium Physicists" }

        // pods.yaml has "collection:" (singular typo) instead of "collections:"
        // Jackson ignores unknown properties, so collections defaults to emptyList()
        assertEquals(emptyList(), titanium.collections)
    }

    @Test
    fun `test all podcasts have names`() {
        val config = AppConfig.load("pods.yaml")
        config.podcasts.forEach { podcast ->
            assertTrue(podcast.name.isNotBlank(), "Podcast should have a name")
        }
    }
}

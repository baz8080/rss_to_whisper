package com.rsstowhisper.web

import com.rsstowhisper.web.db.EpisodeRepository
import com.rsstowhisper.web.models.Episode
import com.rsstowhisper.web.models.FilterOptions
import com.rsstowhisper.web.models.SearchFilters
import com.rsstowhisper.web.models.SearchResult
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import jakarta.ws.rs.core.Response
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.thymeleaf.TemplateEngine
import org.thymeleaf.context.IContext

class SearchResourceTest {
    private val repository: EpisodeRepository = mockk()

    // TemplateEngine is a concrete class; MockK can subclass it.
    private val templateEngine: TemplateEngine = mockk()

    // Construct the resource manually — all fields are lateinit var so we can
    // set them directly without the CDI container.
    private val resource =
        SearchResource().also {
            it.repository = repository
            it.templateEngine = templateEngine
            it.audioBaseUrl = "http://audio.example.com/" // trailing slash — to verify trimming
        }

    // --- index ---

    @Test
    fun `index redirects to search`() {
        val response = resource.index()
        assertEquals(Response.Status.SEE_OTHER.statusCode, response.status)
        assertEquals("/search", response.location.toString())
    }

    // --- search ---

    @Test
    fun `search renders full template when no HTMX header`() {
        stubSearchDependencies()
        every { templateEngine.process("search", any<IContext>()) } returns "<html>full</html>"

        val result =
            resource.search("", emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), 1, null)

        assertEquals("<html>full</html>", result)
    }

    @Test
    fun `search renders partial fragment for HTMX requests`() {
        stubSearchDependencies()
        every { templateEngine.process("search", setOf("app"), any<IContext>()) } returns "<div>partial</div>"

        val result =
            resource.search("", emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), 1, "true")

        assertEquals("<div>partial</div>", result)
    }

    @Test
    fun `search trims whitespace from query before passing to repository`() {
        val captured = slot<SearchFilters>()
        every { repository.search(capture(captured)) } returns emptySearchResult()
        every { repository.getFilterOptions(any()) } returns emptyFilterOptions()
        every { templateEngine.process("search", any<IContext>()) } returns ""

        resource.search("  kotlin  ", emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), 1, null)

        assertEquals("kotlin", captured.captured.query)
    }

    @Test
    fun `search coerces page below 1 up to 1`() {
        val captured = slot<SearchFilters>()
        every { repository.search(capture(captured)) } returns emptySearchResult()
        every { repository.getFilterOptions(any()) } returns emptyFilterOptions()
        every { templateEngine.process("search", any<IContext>()) } returns ""

        resource.search("", emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), -5, null)

        assertEquals(1, captured.captured.page)
    }

    // --- episode ---

    @Test
    fun `episode returns 404 when episode not found`() {
        every { repository.getEpisodeById("missing") } returns null

        val response = resource.episode("missing")

        assertEquals(Response.Status.NOT_FOUND.statusCode, response.status)
    }

    @Test
    fun `episode returns 200 with rendered HTML when found`() {
        every { repository.getEpisodeById("ep1") } returns minimalEpisode()
        every { templateEngine.process("episode", any<IContext>()) } returns "<html>episode</html>"

        val response = resource.episode("ep1")

        assertEquals(Response.Status.OK.statusCode, response.status)
        assertEquals("<html>episode</html>", response.entity)
    }

    @Test
    fun `episode linkifies plain-text summary`() {
        every { repository.getEpisodeById("ep1") } returns minimalEpisode(summary = "Visit https://example.com for more")

        val ctxSlot = slot<IContext>()
        every { templateEngine.process("episode", capture(ctxSlot)) } returns ""

        resource.episode("ep1")

        val linkifiedSummary = ctxSlot.captured.getVariable("linkifiedSummary") as String
        assertTrue(
            linkifiedSummary.contains("""href="https://example.com""""),
            "Expected linkified URL in: $linkifiedSummary",
        )
    }

    @Test
    fun `episode does not linkify summary that already contains HTML`() {
        val htmlSummary = "<p>Already <b>formatted</b></p>"
        every { repository.getEpisodeById("ep1") } returns minimalEpisode(summary = htmlSummary)

        val ctxSlot = slot<IContext>()
        every { templateEngine.process("episode", capture(ctxSlot)) } returns ""

        resource.episode("ep1")

        val linkifiedSummary = ctxSlot.captured.getVariable("linkifiedSummary") as String
        assertEquals(htmlSummary, linkifiedSummary)
    }

    @Test
    fun `episode trims trailing slash from audio base URL`() {
        every { repository.getEpisodeById("ep1") } returns minimalEpisode()

        val ctxSlot = slot<IContext>()
        every { templateEngine.process("episode", capture(ctxSlot)) } returns ""

        resource.episode("ep1")

        val audioBaseUrl = ctxSlot.captured.getVariable("audioBaseUrl") as String
        assertEquals("http://audio.example.com", audioBaseUrl)
    }

    @Test
    fun `episode provides empty transcript lines when transcript is null`() {
        every { repository.getEpisodeById("ep1") } returns minimalEpisode(transcript = null)

        val ctxSlot = slot<IContext>()
        every { templateEngine.process("episode", capture(ctxSlot)) } returns ""

        resource.episode("ep1")

        @Suppress("UNCHECKED_CAST")
        val transcriptLines = ctxSlot.captured.getVariable("transcriptLines") as List<*>
        assertTrue(transcriptLines.isEmpty())
    }

    @Test
    fun `episode parses transcript lines when transcript is present`() {
        every { repository.getEpisodeById("ep1") } returns minimalEpisode(transcript = "0\tHello\n1000\tWorld")

        val ctxSlot = slot<IContext>()
        every { templateEngine.process("episode", capture(ctxSlot)) } returns ""

        resource.episode("ep1")

        @Suppress("UNCHECKED_CAST")
        val transcriptLines = ctxSlot.captured.getVariable("transcriptLines") as List<*>
        assertEquals(2, transcriptLines.size)
    }

    // --- helpers ---

    private fun stubSearchDependencies() {
        every { repository.search(any()) } returns emptySearchResult()
        every { repository.getFilterOptions(any()) } returns emptyFilterOptions()
    }

    private fun emptySearchResult() = SearchResult(emptyList(), 0, 1, 10)

    private fun emptyFilterOptions() = FilterOptions(emptyList(), emptyList(), emptyList())

    private fun minimalEpisode(
        summary: String? = null,
        transcript: String? = null,
    ) = Episode(
        id = "ep1",
        podcastTitle = null,
        podcastImage = null,
        podcastCollections = null,
        episodeTitle = null,
        episodePublishedOn = null,
        episodeAudioLink = null,
        episodeWebLink = null,
        episodeImage = null,
        episodeSummary = summary,
        episodeNumber = null,
        episodeSeason = null,
        episodeType = null,
        episodeDuration = null,
        episodeRelativeMp3Path = null,
        allTags = null,
        transcript = transcript,
    )
}

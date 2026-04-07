package com.rsstowhisper.web.routes

import com.rsstowhisper.web.db.EpisodeRepository
import com.rsstowhisper.web.models.SearchFilters
import com.rsstowhisper.web.templates.appContent
import com.rsstowhisper.web.templates.searchPage
import io.ktor.http.ContentType
import io.ktor.server.html.respondHtml
import io.ktor.server.response.respondRedirect
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import kotlinx.html.div
import kotlinx.html.id
import kotlinx.html.stream.createHTML

fun Route.searchRoutes(
    repository: EpisodeRepository,
    audioBaseUrl: String,
) {
    get("/") {
        call.respondRedirect("/search")
    }

    get("/search") {
        val filters = parseFilters(call.parameters)
        val result = repository.search(filters)
        val filterOptions = repository.getFilterOptions(filters.query)

        if (call.request.headers["HX-Request"] == "true") {
            val html =
                createHTML().div {
                    id = "app"
                    appContent(result, filters, filterOptions, audioBaseUrl)
                }
            call.respondText(html, ContentType.Text.Html)
        } else {
            call.respondHtml {
                searchPage(result, filters, filterOptions, audioBaseUrl)
            }
        }
    }
}

private fun parseFilters(params: io.ktor.http.Parameters): SearchFilters {
    return SearchFilters(
        query = params["q"]?.trim() ?: "",
        durations = params.getAll("duration")?.toSet() ?: emptySet(),
        podcasts = params.getAll("podcast")?.toSet() ?: emptySet(),
        collections = params.getAll("collection")?.toSet() ?: emptySet(),
        tags = params.getAll("tag")?.toSet() ?: emptySet(),
        episodeTypes = params.getAll("episodeType")?.toSet() ?: emptySet(),
        page = (params["page"]?.toIntOrNull() ?: 1).coerceAtLeast(1),
    )
}

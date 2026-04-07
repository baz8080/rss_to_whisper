package com.rsstowhisper.web.templates

import com.rsstowhisper.web.models.DurationCategory
import com.rsstowhisper.web.models.FilterOptions
import com.rsstowhisper.web.models.SearchFilters
import com.rsstowhisper.web.models.SearchResult
import kotlinx.html.FlowContent
import kotlinx.html.FormMethod
import kotlinx.html.HTML
import kotlinx.html.a
import kotlinx.html.article
import kotlinx.html.button
import kotlinx.html.checkBoxInput
import kotlinx.html.details
import kotlinx.html.div
import kotlinx.html.form
import kotlinx.html.h3
import kotlinx.html.id
import kotlinx.html.label
import kotlinx.html.nav
import kotlinx.html.p
import kotlinx.html.small
import kotlinx.html.span
import kotlinx.html.summary
import kotlinx.html.textInput
import kotlinx.html.unsafe

fun HTML.searchPage(
    result: SearchResult,
    filters: SearchFilters,
    filterOptions: FilterOptions,
    audioBaseUrl: String,
) {
    layout("Podcast Search") {
        div {
            id = "app"
            appContent(result, filters, filterOptions, audioBaseUrl)
        }
    }
}

fun FlowContent.appContent(
    result: SearchResult,
    filters: SearchFilters,
    filterOptions: FilterOptions,
    audioBaseUrl: String,
) {
    div("app-layout") {
        nav("sidebar") {
            filterSidebar(filters, filterOptions)
        }

        div("main-content") {
            div("search-bar") {
                textInput {
                    id = "search-input"
                    name = "q"
                    placeholder = "Search episodes…"
                    value = filters.query
                    attributes["type"] = "search"
                    attributes["hx-get"] = "/search"
                    attributes["hx-target"] = "#app"
                    attributes["hx-swap"] = "outerHTML"
                    attributes["hx-push-url"] = "true"
                    attributes["hx-trigger"] = "input changed delay:300ms, search"
                    attributes["hx-include"] = "#filter-form"
                    attributes["hx-indicator"] = "#loading"
                }
            }

            span("htmx-indicator") {
                id = "loading"
                attributes["aria-busy"] = "true"
                +"Searching…"
            }

            resultsPanel(result, filters, audioBaseUrl)
        }
    }
}

private fun FlowContent.filterSidebar(
    filters: SearchFilters,
    filterOptions: FilterOptions,
) {
    form {
        id = "filter-form"
        action = "/search"
        method = FormMethod.get
        attributes["hx-get"] = "/search"
        attributes["hx-target"] = "#app"
        attributes["hx-swap"] = "outerHTML"
        attributes["hx-push-url"] = "true"
        attributes["hx-trigger"] = "change from:input[type='checkbox']"
        attributes["hx-include"] = "#search-input"
        attributes["hx-indicator"] = "#loading"

        filterGroup("Duration", open = true) {
            for (cat in DurationCategory.entries) {
                val name = cat.name.lowercase()
                filterCheckbox("duration", name, cat.label, name in filters.durations)
            }
        }

        if (filterOptions.podcasts.size > 1) {
            filterGroup("Podcast", open = true) {
                for (podcast in filterOptions.podcasts) {
                    filterCheckbox(
                        "podcast",
                        podcast,
                        podcast,
                        podcast in filters.podcasts,
                    )
                }
            }
        }

        if (filterOptions.collections.isNotEmpty()) {
            filterGroup("Collections") {
                for (collection in filterOptions.collections) {
                    filterCheckbox(
                        "collection",
                        collection,
                        collection,
                        collection in filters.collections,
                    )
                }
            }
        }

        if (filterOptions.tags.isNotEmpty()) {
            filterGroup("Tags") {
                for (tag in filterOptions.tags) {
                    filterCheckbox("tag", tag, tag, tag in filters.tags)
                }
            }
        }

        if (filterOptions.episodeTypes.size > 1) {
            filterGroup("Episode Type") {
                for (type in filterOptions.episodeTypes) {
                    filterCheckbox(
                        "episodeType",
                        type,
                        type,
                        type in filters.episodeTypes,
                    )
                }
            }
        }
    }
}

private fun FlowContent.resultsPanel(
    result: SearchResult,
    filters: SearchFilters,
    audioBaseUrl: String,
) {
    if (result.totalCount > 0) {
        p("results-info") {
            +"${result.totalCount} results"
            if (filters.query.isNotBlank()) {
                +" for \"${filters.query}\""
            }
            +" · Page ${result.page} of ${result.totalPages}"
        }
    }

    if (result.episodes.isEmpty() && filters.hasActiveFilters()) {
        p { +"No episodes found. Try adjusting your search or filters." }
    } else if (result.episodes.isEmpty()) {
        p { +"Search for episodes using the search bar above." }
    }

    for (episode in result.episodes) {
        article("episode-card") {
            div("podcast-name") {
                +(episode.podcastTitle ?: "Unknown Podcast")
            }
            h3 {
                if (episode.episodeWebLink != null) {
                    a(href = episode.episodeWebLink) {
                        +(episode.episodeTitle ?: "Untitled")
                    }
                } else {
                    +(episode.episodeTitle ?: "Untitled")
                }
            }
            div("meta") {
                episode.episodePublishedOn?.let { span { +it } }
                episode.episodeDuration?.let { span { +formatDuration(it) } }
                if (episode.episodeSeason != null && episode.episodeNumber != null) {
                    span { +"S${episode.episodeSeason}E${episode.episodeNumber}" }
                }
                episode.episodeType?.let {
                    if (it != "full") span("tag-pill") { +it }
                }
            }

            if (episode.snippet != null) {
                div("snippet") {
                    unsafe { +stripTimestamps(episode.snippet) }
                }
            }

            episode.allTags?.let { tags ->
                div {
                    tags.split(",")
                        .map(String::trim)
                        .filter(String::isNotBlank)
                        .forEach { tag -> span("tag-pill") { +tag } }
                }
            }

            val audioPath = episode.episodeRelativeMp3Path
            if (audioPath != null) {
                val wavPath = audioPath.replace(".mp3", ".wav")
                val audioUrl = "$audioBaseUrl/$wavPath"
                div("audio-player") {
                    unsafe {
                        +"""<audio controls preload="none" src="$audioUrl">"""
                        +"""Your browser does not support audio.</audio>"""
                    }
                }
            }

            div("actions") {
                episode.episodeWebLink?.let { link ->
                    a(href = link) {
                        attributes["target"] = "_blank"
                        +"Episode page"
                    }
                }
            }
        }
    }

    if (result.totalPages > 1) {
        div("pagination") {
            if (result.hasPrevious) {
                button {
                    attributes["hx-get"] =
                        buildSearchUrl(
                            filters.copy(page = filters.page - 1),
                        )
                    attributes["hx-target"] = "#app"
                    attributes["hx-swap"] = "outerHTML"
                    attributes["hx-push-url"] = "true"
                    +"Previous"
                }
            }
            span { +"Page ${result.page} of ${result.totalPages}" }
            if (result.hasNext) {
                button {
                    attributes["hx-get"] =
                        buildSearchUrl(
                            filters.copy(page = filters.page + 1),
                        )
                    attributes["hx-target"] = "#app"
                    attributes["hx-swap"] = "outerHTML"
                    attributes["hx-push-url"] = "true"
                    +"Next"
                }
            }
        }
    }
}

private fun FlowContent.filterGroup(
    title: String,
    open: Boolean = false,
    content: FlowContent.() -> Unit,
) {
    details("filter-group") {
        if (open) {
            this.open = true
        }
        summary { +title }
        div("filter-list") {
            content()
        }
    }
}

private fun FlowContent.filterCheckbox(
    name: String,
    value: String,
    labelText: String,
    checked: Boolean,
) {
    label {
        checkBoxInput {
            this.name = name
            this.value = value
            this.checked = checked
        }
        small { +labelText }
    }
}

private fun SearchFilters.hasActiveFilters(): Boolean =
    query.isNotBlank() || durations.isNotEmpty() || podcasts.isNotEmpty() ||
        collections.isNotEmpty() || tags.isNotEmpty() || episodeTypes.isNotEmpty()

private fun buildSearchUrl(filters: SearchFilters): String {
    val params = mutableListOf<String>()
    if (filters.query.isNotBlank()) params.add("q=${filters.query}")
    filters.durations.forEach { params.add("duration=$it") }
    filters.podcasts.forEach { params.add("podcast=$it") }
    filters.collections.forEach { params.add("collection=$it") }
    filters.tags.forEach { params.add("tag=$it") }
    filters.episodeTypes.forEach { params.add("episodeType=$it") }
    if (filters.page > 1) params.add("page=${filters.page}")
    return "/search?${params.joinToString("&")}"
}

fun formatDuration(seconds: Int): String {
    val hours = seconds / 3600
    val minutes = (seconds % 3600) / 60
    return if (hours > 0) "${hours}h ${minutes}m" else "${minutes}m"
}

private val TIMESTAMP_REGEX = Regex("""\d{4,}\t""")

private fun stripTimestamps(snippet: String): String = TIMESTAMP_REGEX.replace(snippet, " ").replace("  ", " ")

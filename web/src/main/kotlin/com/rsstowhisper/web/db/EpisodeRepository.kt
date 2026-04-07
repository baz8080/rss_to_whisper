package com.rsstowhisper.web.db

import com.rsstowhisper.web.models.DurationCategory
import com.rsstowhisper.web.models.Episode
import com.rsstowhisper.web.models.FilterOptions
import com.rsstowhisper.web.models.SearchFilters
import com.rsstowhisper.web.models.SearchResult
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

class EpisodeRepository(dbPath: String) {
    private val conn: Connection =
        DriverManager.getConnection("jdbc:sqlite:$dbPath").apply {
            createStatement().execute("PRAGMA query_only=ON")
            createStatement().execute("PRAGMA mmap_size=268435456")
        }

    private var cachedFilterQuery: String? = null
    private var cachedFilterOptions: FilterOptions? = null

    fun search(filters: SearchFilters): SearchResult {
        val hasQuery = filters.query.isNotBlank()
        val params = mutableListOf<Any>()

        val whereClauses = mutableListOf<String>()

        if (hasQuery) {
            whereClauses.add("episodes_fts MATCH ?")
            params.add(filters.query)
        }

        addDurationFilter(filters, whereClauses, params)
        addSetFilter(filters.podcasts, "e.podcast_title", whereClauses, params)
        addCollectionsFilter(filters.collections, whereClauses, params)
        addTagsFilter(filters.tags, whereClauses, params)
        addSetFilter(filters.episodeTypes, "e.episode_type", whereClauses, params)

        val whereClause = if (whereClauses.isEmpty()) "" else "WHERE ${whereClauses.joinToString(" AND ")}"

        val countSql =
            if (hasQuery) {
                "SELECT COUNT(*) FROM episodes e JOIN episodes_fts ON e.rowid = episodes_fts.rowid $whereClause"
            } else {
                "SELECT COUNT(*) FROM episodes e $whereClause"
            }

        val totalCount =
            conn.prepareStatement(countSql).use { stmt ->
                params.forEachIndexed { i, p -> setParam(stmt, i + 1, p) }
                stmt.executeQuery().use { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }

        val snippetExpr =
            if (hasQuery) {
                "snippet(episodes_fts, '<mark>', '</mark>', '…', -1, 40)"
            } else {
                "NULL"
            }

        val selectSql =
            if (hasQuery) {
                """SELECT $SEARCH_COLUMNS, $snippetExpr AS snippet
                   FROM episodes e
                   JOIN episodes_fts ON e.rowid = episodes_fts.rowid
                   $whereClause
                   ORDER BY e.episode_published_on DESC
                   LIMIT ? OFFSET ?"""
            } else {
                """SELECT $SEARCH_COLUMNS, NULL AS snippet
                   FROM episodes e
                   $whereClause
                   ORDER BY e.episode_published_on DESC
                   LIMIT ? OFFSET ?"""
            }

        val offset = (filters.page - 1) * filters.pageSize
        val episodes =
            conn.prepareStatement(selectSql).use { stmt ->
                params.forEachIndexed { i, p -> setParam(stmt, i + 1, p) }
                val paramOffset = params.size
                stmt.setInt(paramOffset + 1, filters.pageSize)
                stmt.setInt(paramOffset + 2, offset)

                stmt.executeQuery().use { rs ->
                    val results = mutableListOf<Episode>()
                    while (rs.next()) {
                        results.add(mapEpisode(rs))
                    }
                    results
                }
            }

        return SearchResult(episodes, totalCount, filters.page, filters.pageSize)
    }

    fun getEpisodeById(id: String): Episode? {
        val sql = "SELECT $SEARCH_COLUMNS, e.episode_transcript FROM episodes e WHERE e.id = ?"
        return conn.prepareStatement(sql).use { stmt ->
            stmt.setString(1, id)
            stmt.executeQuery().use { rs ->
                if (rs.next()) mapEpisode(rs, includeTranscript = true) else null
            }
        }
    }

    fun getFilterOptions(query: String): FilterOptions {
        if (query == cachedFilterQuery) return cachedFilterOptions!!

        val hasQuery = query.isNotBlank()
        val fromClause =
            if (hasQuery) {
                "FROM episodes e JOIN episodes_fts ON e.rowid = episodes_fts.rowid"
            } else {
                "FROM episodes e"
            }
        val wherePrefix = if (hasQuery) "WHERE episodes_fts MATCH ? AND" else "WHERE"

        fun queryDistinct(column: String): List<String> {
            val sql = "SELECT DISTINCT $column $fromClause $wherePrefix $column IS NOT NULL ORDER BY $column"
            return conn.prepareStatement(sql).use { stmt ->
                if (hasQuery) stmt.setString(1, query)
                stmt.executeQuery().use { rs ->
                    generateSequence {
                        if (rs.next()) rs.getString(1) else null
                    }.toList()
                }
            }
        }

        fun splitCsv(column: String): List<String> {
            val sql = "SELECT DISTINCT $column $fromClause $wherePrefix $column IS NOT NULL"
            return conn.prepareStatement(sql).use { stmt ->
                if (hasQuery) stmt.setString(1, query)
                stmt.executeQuery().use { rs ->
                    generateSequence {
                        if (rs.next()) rs.getString(1) else null
                    }
                        .flatMap { it.split(",").map(String::trim) }
                        .filter { it.isNotBlank() }
                        .toSortedSet()
                        .toList()
                }
            }
        }

        val options =
            FilterOptions(
                podcasts = queryDistinct("e.podcast_title"),
                collections = splitCsv("e.podcast_collections"),
                episodeTypes = queryDistinct("e.episode_type"),
            )
        cachedFilterQuery = query
        cachedFilterOptions = options
        return options
    }

    private fun addDurationFilter(
        filters: SearchFilters,
        clauses: MutableList<String>,
        params: MutableList<Any>,
    ) {
        if (filters.durations.isEmpty()) return
        val short = DurationCategory.SHORT.maxSeconds
        val medium = DurationCategory.MEDIUM.maxSeconds
        val conditions = mutableListOf<String>()
        for (d in filters.durations) {
            when (d.uppercase()) {
                "SHORT" ->
                    conditions.add(
                        "(e.episode_duration > 0 AND e.episode_duration < $short)",
                    )
                "MEDIUM" ->
                    conditions.add(
                        "(e.episode_duration >= $short AND e.episode_duration <= $medium)",
                    )
                "LONG" ->
                    conditions.add(
                        "(e.episode_duration > $medium)",
                    )
            }
        }
        if (conditions.isNotEmpty()) {
            clauses.add("(${conditions.joinToString(" OR ")})")
        }
    }

    private fun addSetFilter(
        values: Set<String>,
        column: String,
        clauses: MutableList<String>,
        params: MutableList<Any>,
    ) {
        if (values.isEmpty()) return
        val placeholders = values.joinToString(",") { "?" }
        clauses.add("$column IN ($placeholders)")
        params.addAll(values)
    }

    private fun addCollectionsFilter(
        values: Set<String>,
        clauses: MutableList<String>,
        params: MutableList<Any>,
    ) {
        if (values.isEmpty()) return
        // Collections are comma-separated in the column, so use LIKE for each
        val conditions = values.map { "e.podcast_collections LIKE ?" }
        clauses.add("(${conditions.joinToString(" OR ")})")
        values.forEach { params.add("%$it%") }
    }

    private fun addTagsFilter(
        values: Set<String>,
        clauses: MutableList<String>,
        params: MutableList<Any>,
    ) {
        if (values.isEmpty()) return
        val conditions = values.map { "e.all_tags LIKE ?" }
        clauses.add("(${conditions.joinToString(" OR ")})")
        values.forEach { params.add("%$it%") }
    }

    private fun setParam(
        stmt: java.sql.PreparedStatement,
        index: Int,
        value: Any,
    ) {
        when (value) {
            is String -> stmt.setString(index, value)
            is Int -> stmt.setInt(index, value)
            else -> stmt.setObject(index, value)
        }
    }

    private fun mapEpisode(
        rs: ResultSet,
        includeTranscript: Boolean = false,
    ): Episode =
        Episode(
            id = rs.getString("id"),
            podcastTitle = rs.getString("podcast_title"),
            podcastImage = rs.getString("podcast_image"),
            podcastCollections = rs.getString("podcast_collections"),
            episodeTitle = rs.getString("episode_title"),
            episodePublishedOn = rs.getString("episode_published_on"),
            episodeAudioLink = rs.getString("episode_audio_link"),
            episodeWebLink = rs.getString("episode_web_link"),
            episodeImage = rs.getString("episode_image"),
            episodeSummary = rs.getString("episode_summary"),
            episodeNumber = rs.getObject("episode_number") as? Int,
            episodeSeason = rs.getObject("episode_season") as? Int,
            episodeType = rs.getString("episode_type"),
            episodeDuration = rs.getObject("episode_duration") as? Int,
            episodeRelativeMp3Path = rs.getString("episode_relative_mp3_path"),
            allTags = rs.getString("all_tags"),
            snippet = runCatching { rs.getString("snippet") }.getOrNull(),
            transcript = if (includeTranscript) rs.getString("episode_transcript") else null,
        )

    companion object {
        private const val SEARCH_COLUMNS =
            """e.id, e.podcast_title, e.podcast_image, e.podcast_collections,
               e.episode_title, e.episode_published_on, e.episode_audio_link,
               e.episode_web_link, e.episode_image, e.episode_summary,
               e.episode_number, e.episode_season, e.episode_type,
               e.episode_duration, e.episode_relative_mp3_path, e.all_tags"""
    }
}

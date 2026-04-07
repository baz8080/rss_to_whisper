package com.rsstowhisper.web

import com.rsstowhisper.web.db.EpisodeRepository
import com.rsstowhisper.web.routes.searchRoutes
import io.ktor.server.engine.embeddedServer
import io.ktor.server.http.content.staticResources
import io.ktor.server.netty.Netty
import io.ktor.server.routing.routing

fun main(args: Array<String>) {
    val dbPath = args.getOrNull(0) ?: error("Usage: web <db-path> [audio-base-url] [port]")
    val audioBaseUrl = (args.getOrNull(1) ?: "/audio").trimEnd('/')
    val port = args.getOrNull(2)?.toIntOrNull() ?: 8080

    val repository = EpisodeRepository(dbPath)

    println("Starting Podcast Search on http://localhost:$port")
    println("Database: $dbPath")
    println("Audio base URL: $audioBaseUrl")

    embeddedServer(Netty, port = port) {
        routing {
            staticResources("/static", "static")
            searchRoutes(repository, audioBaseUrl)
        }
    }.start(wait = true)
}

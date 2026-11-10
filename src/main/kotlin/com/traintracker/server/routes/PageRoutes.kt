package com.traintracker.server.routes

import com.traintracker.server.Config
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.queryString
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import java.io.File
import java.util.concurrent.ConcurrentHashMap

private const val INDEX_HTML: String = "${Config.DATA_DIR}/index.html"
private val staticHtmlCache = ConcurrentHashMap<String, String>()

/** Query parameters the single page app keeps board state in: time, operator filter and timeframe. */
private val PAGE_QUERY_KEYS = setOf("t", "op", "tf", "from", "to")

/** Serves a page, read once and cached. A query string with any other parameter gets a 404. */
private suspend fun ApplicationCall.serveStaticHtml(path: String, notFoundMsg: String = "Not found") {
    if (request.queryString().isNotEmpty() && !PAGE_QUERY_KEYS.containsAll(request.queryParameters.names())) {
        respond(HttpStatusCode.NotFound)
        return
    }
    val html = staticHtmlCache.getOrPut(path) {
        val file = File(path)
        if (file.exists()) file.readText() else ""
    }
    if (html.isNotEmpty()) {
        respondText(html, ContentType.Text.Html)
    } else {
        respond(HttpStatusCode.NotFound, notFoundMsg)
    }
}

fun Route.pageRoutes() {
    get("/") { call.serveStaticHtml(INDEX_HTML) }
    get("/dashboard") { call.serveStaticHtml("${Config.DATA_DIR}/dashboard.html", "Dashboard not found") }
    // Client-side routes of the single page app
    listOf(
        "/station/{crs}",
        "/station/{crs}/{date}",
        "/service/{serviceId}",
        "/service/{serviceId}/{a}",
        "/service/{serviceId}/{a}/{b}",
        "/unit/{unitCode}",
        "/unit/{unitCode}/{date}",
        "/toc/{tocCode}",
        "/toc/{tocCode}/{date}",
        "/headcode/{hc}",
        "/headcode/{hc}/{date}",
        "/pride"
    ).forEach { path ->
        get(path) { call.serveStaticHtml(INDEX_HTML) }
    }
}

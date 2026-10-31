package com.traintracker.server.routes

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

private const val INDEX_HTML: String = "/opt/traintracker/index.html"
private val staticHtmlCache = ConcurrentHashMap<String, String>()

private suspend fun ApplicationCall.serveStaticHtml(path: String, notFoundMsg: String = "Not found") {
    if (request.queryString().isNotEmpty()) {
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
    get("/dashboard") { call.serveStaticHtml("/opt/traintracker/dashboard.html", "Dashboard not found") }
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

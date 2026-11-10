package com.traintracker.server.routes

import com.traintracker.server.kb.KbClient
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/** National Rail Knowledgebase: incidents, service indicators, operators and stations. */
fun Route.kbRoutes() {
    get("/kb/station-messages") {
        val crs = call.request.queryParameters["crs"]?.uppercase()?.trim()
        if (crs.isNullOrBlank()) return@get call.badRequest("crs parameter required")
        if (!isValidCrs(crs)) return@get call.badRequest("crs must be exactly 3 letters")
        val messages = withContext(Dispatchers.IO) { KbClient.getStationMessages(crs) }
        if (messages == null) {
            call.respond(HttpStatusCode.ServiceUnavailable, mapOf("error" to "upstream unavailable"))
        } else {
            call.respond(messages)
        }
    }
    get("/kb/incidents") {
        val crs = call.request.queryParameters["crs"]?.uppercase()?.trim() ?: ""
        call.respond(withContext(Dispatchers.IO) { KbClient.getIncidents(crs) })
    }
    get("/kb/incidents/by-toc/{toc}") {
        val toc = call.pathParam("toc").uppercase()
        if (toc.isBlank()) return@get call.badRequest("toc parameter required")
        call.respond(withContext(Dispatchers.IO) { KbClient.getIncidentsByToc(toc) })
    }
    get("/kb/nsi") {
        call.respond(withContext(Dispatchers.IO) { KbClient.getNsi() })
    }
    get("/kb/toc") {
        call.respond(withContext(Dispatchers.IO) { KbClient.getToc() })
    }
    get("/kb/station/{crs}") {
        val crs = call.validCrs() ?: return@get
        val station = withContext(Dispatchers.IO) { KbClient.getStation(crs) }
        if (station == null) {
            call.respond(HttpStatusCode.NotFound, mapOf("error" to "Station $crs not found"))
        } else {
            call.respond(station)
        }
    }
    adminPost("/kb/station/cache/clear") {
        KbClient.clearStationCache()
        respondOk()
    }
    get("/kb/station/{crs}/raw") {
        val crs = call.validCrs() ?: return@get
        val raw = withContext(Dispatchers.IO) { KbClient.getRawStation(crs) }
        if (raw == null) {
            call.respond(HttpStatusCode.NotFound, "Station $crs not found")
        } else {
            call.respondText(raw, ContentType.Application.Xml)
        }
    }
    get("/kb/station/{crs}/messages") {
        val crs = call.validCrs() ?: return@get
        val msgs = withContext(Dispatchers.IO) { KbClient.getStationMessages(crs) }
        if (msgs == null) {
            call.respond(buildJsonObject {
                put("crs", crs)
                put("disruptions", buildJsonArray {})
                put("stationAlerts", "")
            })
        } else {
            call.respond(buildJsonObject {
                put("crs", msgs.crs)
                put("stationAlerts", if (msgs.disruptions.isEmpty()) msgs.stationAlerts else "")
                put("disruptions", buildJsonArray {
                    msgs.disruptions.forEach { d ->
                        add(buildJsonObject {
                            put("category", d.category)
                            put("severity", d.severity)
                            put("summary", d.summary)
                        })
                    }
                })
            })
        }
    }
}

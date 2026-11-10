package com.traintracker.server.routes

import com.traintracker.server.auth.changedBy
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.LocationData
import com.traintracker.server.cif.SmartLookup
import com.traintracker.server.database.AdminData
import com.traintracker.server.database.Overrides
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

fun Route.adminRoutes() {
    locationRoutes()
    workQueueRoutes()
    operatorRoutes()
    smartBerthRoutes()
    feedbackRoutes()
}

/** TIPLOC names/CRS and TSC operators. */
private fun Route.locationRoutes() {
    adminGet("/admin/crs-lookup") {
        val crs = param("crs", true)
        respond(buildJsonObject {
            put("crs", crs)
            put("tiplocs", Json.encodeToJsonElement(CorpusLookup.tiplocFromCrs(crs)))
        })
    }
    adminGet("/admin/tiploc-lookup") {
        val tiploc = param("tiploc", true)
        respond(mapOf("tiploc" to tiploc, "name" to (CorpusLookup.nameFromTiploc(tiploc) ?: "")))
    }
    adminPost("/admin/tiploc-crs") {
        val tiploc = param("tiploc", true)
        val crs = param("crs", true)
        if (tiploc.isEmpty() || crs.isEmpty()) return@adminPost badRequest("tiploc and crs required")
        val oldCrs = CorpusLookup.crsFromTiploc(tiploc) ?: "(none)"
        val editor = changedBy()
        CorpusLookup.updateTiplocCrs(tiploc, crs, editor)
        Overrides.logCrsChange(tiploc, oldCrs, crs, editor)
        respond(buildJsonObject {
            put("status", "ok")
            put("tiploc", tiploc)
            put("crs", crs)
            put("oldCrs", oldCrs)
        })
    }
    adminPost("/admin/tiploc-name") {
        val tiploc = param("tiploc", true)
        val name = param("name")
        if (tiploc.isEmpty() || name.isEmpty()) return@adminPost badRequest("tiploc and name required")
        val oldName = CorpusLookup.nameFromTiploc(tiploc)
        val editor = changedBy()
        CorpusLookup.updateTiplocName(tiploc, name, editor)
        Overrides.logNameChange(tiploc, oldName, name, editor)
        AdminData.deleteMissingTiploc(tiploc)
        // A newly named TIPLOC also gets its STANOX from PIF/NLC, unless it already has one.
        val stanox = LocationData.stanoxForTiploc(tiploc)?.let { Overrides.setStanoxIfEmpty(tiploc, it, editor) } ?: ""
        respondOk("tiploc" to tiploc, "name" to name, "stanox" to stanox)
    }
    adminPost("/admin/tsc-operator") {
        val tsc = param("tsc", true)
        val operator = param("operator", true)
        if (tsc.isEmpty() || operator.isEmpty()) return@adminPost badRequest("tsc and operator required")
        val oldOperator = CorpusLookup.operatorFromTsc(tsc) ?: "(none)"
        val editor = changedBy()
        CorpusLookup.updateTscOperator(tsc, operator)
        Overrides.logOperatorChange(tsc, oldOperator, operator, editor)
        AdminData.deleteMissingTsc(tsc)
        respondOk("tsc" to tsc, "operator" to operator)
    }
}

/** Items waiting for an admin: missing TIPLOC names, missing TSC operators, SMART gaps, CIF change alerts. */
private fun Route.workQueueRoutes() {
    adminGet("/admin/missing-tiplocs") {
        respond(mapOf("missing" to AdminData.missingTiplocs()))
    }
    adminDelete("/admin/missing-tiplocs") {
        val tiploc = param("tiploc", true)
        if (tiploc.isEmpty()) return@adminDelete badRequest("tiploc required")
        AdminData.deleteMissingTiploc(tiploc)
        respondOk("tiploc" to tiploc)
    }
    adminGet("/admin/missing-tscs") {
        respond(mapOf("missing" to AdminData.missingTscs()))
    }
    adminDelete("/admin/missing-tscs") {
        val tsc = param("tsc", true)
        if (tsc.isEmpty()) return@adminDelete badRequest("tsc required")
        AdminData.deleteMissingTsc(tsc)
        respondOk("tsc" to tsc)
    }
    adminGet("/admin/missing-smart-stanox") {
        respond(mapOf("missing" to AdminData.smartGaps().map { gap ->
            mapOf(
                "stanox" to gap.key,
                "seenCount" to gap.seenCount,
                "firstSeen" to gap.firstSeen,
                "lastSeen" to gap.lastSeen,
                "headcode" to gap.headcode,
                "uid" to stanoxLabel(gap.key.substringBefore('|')),
            )
        }))
    }
    adminDelete("/admin/missing-smart-stanox") {
        val key = param("stanox")
        if (key.isEmpty()) return@adminDelete badRequest("stanox required")
        AdminData.deleteSmartGap(key)
        respondOk("stanox" to key)
    }
    adminGet("/admin/cif-change-alerts") {
        respond(mapOf("alerts" to AdminData.cifChangeAlerts()))
    }
    adminDelete("/admin/cif-change-alerts") {
        val tiploc = param("tiploc")
        if (tiploc.isEmpty()) return@adminDelete badRequest("tiploc required")
        AdminData.dismissCifChangeAlert(
            tiploc,
            parameters["oldDesc"] ?: "",
            parameters["oldCrs"] ?: "",
            parameters["newDesc"] ?: "",
            parameters["newCrs"] ?: ""
        )
        respondOk("tiploc" to tiploc)
    }
}

/** "Name (CRS)" for a STANOX, from its TIPLOC's name or else any TIPLOC sharing its CRS. */
private fun stanoxLabel(stanox: String): String {
    val crs = CorpusLookup.crsFromStanox(stanox)
    val label = CorpusLookup.tiplocFromStanox(stanox)?.let { CorpusLookup.nameFromTiploc(it) }
        ?: crs?.let { CorpusLookup.tiplocFromCrs(it).firstNotNullOfOrNull { t -> CorpusLookup.nameFromTiploc(t) } }
    return if (label != null && crs != null) "$label ($crs)" else label ?: crs ?: ""
}

private val OPERATOR_CODE = Regex("^[A-Z0-9]{2,10}$")
private val COLOUR = Regex("^#[0-9A-Fa-f]{6}$")

private fun Route.operatorRoutes() {
    get("/operators") {
        call.respond(mapOf("operators" to AdminData.operators()))
    }
    adminPost("/admin/operators") {
        val code = param("code", true)
        val name = param("name")
        val colour = param("colour")
        if (!OPERATOR_CODE.matches(code)) return@adminPost badRequest("code must be 2-10 letters or numbers")
        if (name.isEmpty() || name.length > 60) return@adminPost badRequest("name required, 60 characters max")
        if (colour.isNotEmpty() && !COLOUR.matches(colour)) return@adminPost badRequest("colour must look like #1A2B3C")
        AdminData.upsertOperator(code, name, colour)
        respondOk("code" to code)
    }
    adminDelete("/admin/operators") {
        val code = param("code", true)
        if (code.isEmpty()) return@adminDelete badRequest("code required")
        AdminData.deleteOperator(code)
        respondOk("code" to code)
    }
}

/** Manual SMART berth steps. */
private fun Route.smartBerthRoutes() {
    adminGet("/admin/smart-berths") {
        val berths = SmartLookup.listBerths(param("stanox"), param("td"), param("berth"), parameters["limit"]?.toIntOrNull() ?: 200)
        respond(mapOf("berths" to berths))
    }
    adminPost("/admin/smart-berth") {
        val offset = parameters["offset"]?.trim()?.toIntOrNull() ?: return@adminPost badRequest("offset (whole seconds) required")
        val error = SmartLookup.addBerth(
            param("td"), param("from"), param("to"), param("stanox"), param("event"), param("platform"), param("stepType"),
            offset, changedBy()
        )
        if (error != null) return@adminPost badRequest(error)
        respondOk()
    }
    adminDelete("/admin/smart-berth") {
        val id = parameters["id"]?.trim()?.toLongOrNull() ?: return@adminDelete badRequest("id required")
        SmartLookup.deleteBerth(id)
        respondOk("id" to id.toString())
    }
}

private fun Route.feedbackRoutes() {
    post("/feedback") {
        val body = call.receiveText()
        val json = try {
            Json.parseToJsonElement(body).jsonObject
        } catch (e: Exception) {
            return@post call.badRequest("Invalid JSON body")
        }
        val message = json["message"]?.jsonPrimitive?.content?.trim() ?: ""
        val page = json["page"]?.jsonPrimitive?.content?.trim() ?: ""
        if (message.isEmpty()) return@post call.badRequest("message required")
        if (message.length > 2000) return@post call.badRequest("message too long")
        // Spam guard: "website" is a hidden honeypot field and real users take a few seconds to type.
        val website = json["website"]?.jsonPrimitive?.content ?: ""
        val elapsed = json["elapsed"]?.jsonPrimitive?.content?.toLongOrNull() ?: 0L
        if (website.isNotEmpty() || elapsed < 2000L) {
            call.respond(HttpStatusCode.TooManyRequests, mapOf("error" to "Not sent -- please wait a few seconds and try again."))
            return@post
        }
        AdminData.submitFeedback(message, page.take(200))
        call.respond(mapOf("status" to "ok"))
    }
    adminGet("/admin/feedback") {
        respond(mapOf("feedback" to AdminData.feedback()))
    }
    adminDelete("/admin/feedback") {
        val id = param("id")
        if (id.isEmpty()) return@adminDelete badRequest("id required")
        AdminData.dismissFeedback(id)
        respondOk("id" to id)
    }
}

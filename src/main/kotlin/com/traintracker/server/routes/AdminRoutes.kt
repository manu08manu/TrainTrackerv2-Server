package com.traintracker.server.routes

import com.traintracker.server.auth.AuthDatabase
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.database.AppDatabase
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.post
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

fun Route.adminRoutes() {
    adminGet("/admin/crs-lookup") {
        val crs = param("crs", true)
        val tiplocs = CorpusLookup.tiplocFromCrs(crs)
        respond(
            buildJsonObject {
                put("crs", crs)
                put("tiplocs", Json.encodeToJsonElement(tiplocs))
            }
        )
    }
    adminGet("/admin/tiploc-lookup") {
        val tiploc = param("tiploc", true)
        respond(mapOf("tiploc" to tiploc, "name" to (CorpusLookup.nameFromTiploc(tiploc) ?: "")))
    }
    adminPost("/admin/tiploc-crs") {
        val tiploc = param("tiploc", true)
        val crs = param("crs", true)
        if (tiploc.isEmpty() || crs.isEmpty()) {
            badRequest("tiploc and crs required")
            return@adminPost
        }
        val oldCrs = CorpusLookup.crsFromTiploc(tiploc) ?: "(none)"
        val editor = changedBy()
        CorpusLookup.updateTiplocCrs(tiploc, crs, editor)
        AuthDatabase.logTiplocCrsChange(tiploc, oldCrs, crs, editor)
        respond(
            buildJsonObject {
                put("status", "ok")
                put("tiploc", tiploc)
                put("crs", crs)
                put("oldCrs", oldCrs)
            }
        )
    }
    adminPost("/admin/tiploc-name") {
        val tiploc = param("tiploc", true)
        val name = param("name")
        if (tiploc.isEmpty() || name.isEmpty()) {
            badRequest("tiploc and name required")
            return@adminPost
        }
        val oldName = CorpusLookup.nameFromTiploc(tiploc)
        val editor = changedBy()
        CorpusLookup.updateTiplocName(tiploc, name, editor)
        AuthDatabase.logTiplocChange(tiploc, oldName, name, editor)
        AppDatabase.deleteMissingTiploc(tiploc)
        respondOk("tiploc" to tiploc, "name" to name)
    }
    adminPost("/admin/tsc-operator") {
        val tsc = param("tsc", true)
        val operator = param("operator", true)
        if (tsc.isEmpty() || operator.isEmpty()) {
            badRequest("tsc and operator required")
            return@adminPost
        }
        val oldOperator = CorpusLookup.operatorFromTsc(tsc) ?: "(none)"
        val editor = changedBy()
        CorpusLookup.updateTscOperator(tsc, operator, editor)
        AuthDatabase.logOperatorChange(tsc, oldOperator, operator, editor)
        AppDatabase.deleteMissingTsc(tsc)
        respondOk("tsc" to tsc, "operator" to operator)
    }
    adminGet("/admin/missing-tiplocs") {
        respond(mapOf("missing" to AppDatabase.getMissingTiplocs()))
    }
    adminDelete("/admin/missing-tiplocs") {
        val tiploc = param("tiploc", true)
        if (tiploc.isEmpty()) {
            badRequest("tiploc required")
            return@adminDelete
        }
        AppDatabase.deleteMissingTiploc(tiploc)
        respondOk("tiploc" to tiploc)
    }
    adminGet("/admin/missing-tscs") {
        respond(mapOf("missing" to AppDatabase.getMissingTscs()))
    }
    adminDelete("/admin/missing-tscs") {
        val tsc = param("tsc", true)
        if (tsc.isEmpty()) {
            badRequest("tsc required")
            return@adminDelete
        }
        AppDatabase.deleteMissingTsc(tsc)
        respondOk("tsc" to tsc)
    }
    adminGet("/admin/cif-change-alerts") {
        respond(mapOf("alerts" to AppDatabase.getCifChangeAlerts()))
    }
    adminDelete("/admin/cif-change-alerts") {
        val tiploc = param("tiploc")
        if (tiploc.isEmpty()) {
            badRequest("tiploc required")
            return@adminDelete
        }
        AppDatabase.dismissCifChangeAlert(
            tiploc,
            parameters["oldDesc"] ?: "",
            parameters["oldCrs"] ?: "",
            parameters["newDesc"] ?: "",
            parameters["newCrs"] ?: ""
        )
        respondOk("tiploc" to tiploc)
    }
    post("/feedback") {
        val body = call.receiveText()
        val json = try {
            Json.parseToJsonElement(body).jsonObject
        } catch (e: Exception) {
            call.badRequest("Invalid JSON body")
            return@post
        }
        val message = json["message"]?.jsonPrimitive?.content?.trim() ?: ""
        val page = json["page"]?.jsonPrimitive?.content?.trim() ?: ""
        if (message.isEmpty()) {
            call.badRequest("message required")
            return@post
        }
        if (message.length > 2000) {
            call.badRequest("message too long")
            return@post
        }
        val website = json["website"]?.jsonPrimitive?.content ?: ""
        val elapsed = json["elapsed"]?.jsonPrimitive?.content?.toLongOrNull() ?: 0L
        if (website.isNotEmpty() || elapsed < 2000L) {
            call.respond(HttpStatusCode.TooManyRequests, mapOf("error" to "Not sent -- please wait a few seconds and try again."))
            return@post
        }
        AppDatabase.submitFeedback(message, page.take(200))
        call.respond(mapOf("status" to "ok"))
    }
    adminGet("/admin/feedback") {
        respond(mapOf("feedback" to AppDatabase.getFeedback()))
    }
    adminDelete("/admin/feedback") {
        val id = param("id")
        if (id.isEmpty()) {
            badRequest("id required")
            return@adminDelete
        }
        AppDatabase.dismissFeedback(id)
        respondOk("id" to id)
    }
}

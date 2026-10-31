package com.traintracker.server.routes

import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption

private const val PRIDE_FILE: String = "/opt/traintracker/pride_liveries.json"
private val prettyJson = Json { prettyPrint = true }

fun Route.prideRoutes() {
    get("/pride") {
        val f = File(PRIDE_FILE)
        val text = if (f.exists()) f.readText() else "{}"
        val ok = try {
            Json.parseToJsonElement(text)
            true
        } catch (e: Exception) {
            false
        }
        if (ok) {
            call.respondText(text, ContentType.Application.Json)
        } else {
            call.respondText(
                "{\"error\":\"pride_liveries.json is not valid JSON\"}",
                ContentType.Application.Json,
                HttpStatusCode.InternalServerError
            )
        }
    }
    adminPut("/admin/pride") {
        val root = try {
            Json.parseToJsonElement(receiveText()).jsonObject
        } catch (e: Exception) {
            null
        }
        val unitRe = Regex("^[0-9]{5,6}$")
        var err: String? = if (root == null) "body must be a JSON object" else null
        if (root != null) {
            for ((op, v) in root) {
                val arr = v as? JsonArray
                if (op.isBlank() || arr == null) {
                    err = "bad operator entry: $op"
                    break
                }
                for (item in arr) {
                    val u = ((item as? JsonObject)?.get("unit") as? JsonPrimitive)?.contentOrNull
                    if (u == null || !unitRe.matches(u)) {
                        err = "bad unit under $op"
                        break
                    }
                }
                if (err != null) break
            }
        }
        if (err != null) {
            badRequest(err)
            return@adminPut
        }
        try {
            val f = File(PRIDE_FILE)
            if (f.exists()) f.copyTo(File("${f.path}.bak"), true)
            val tmp = File("${f.path}.tmp")
            tmp.writeText(prettyJson.encodeToString(JsonObject.serializer(), root!!))
            Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            respond(
                buildJsonObject {
                    put("ok", true)
                    put("operators", root.size)
                }
            )
        } catch (e: Exception) {
            respond(HttpStatusCode.InternalServerError, mapOf("error" to "could not write file: ${e.message}"))
        }
    }
}

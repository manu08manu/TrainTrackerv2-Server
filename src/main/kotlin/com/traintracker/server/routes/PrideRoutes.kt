package com.traintracker.server.routes

import com.traintracker.server.Config
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.put
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption

private const val PRIDE_FILE: String = "${Config.DATA_DIR}/pride_liveries.json"
private val UNIT_NUMBER = Regex("^[0-9]{5,6}$")
private val prettyJson = Json { prettyPrint = true }

/** Units in pride liveries, per operator: { "OP": [ { "unit": "123456", ... }, ... ], ... }. */
fun Route.prideRoutes() {
    get("/pride") {
        val f = File(PRIDE_FILE)
        val text = if (f.exists()) f.readText() else "{}"
        val validJson = parseJson(text) != null
        if (validJson) {
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
        val root = parseJson(receiveText()) as? JsonObject
            ?: return@adminPut badRequest("body must be a JSON object")
        validationError(root)?.let { return@adminPut badRequest(it) }
        try {
            val f = File(PRIDE_FILE)
            if (f.exists()) f.copyTo(File("${f.path}.bak"), true)
            val tmp = File("${f.path}.tmp")
            tmp.writeText(prettyJson.encodeToString(JsonObject.serializer(), root))
            Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            respond(buildJsonObject {
                put("ok", true)
                put("operators", root.size)
            })
        } catch (e: Exception) {
            respond(HttpStatusCode.InternalServerError, mapOf("error" to "could not write file: ${e.message}"))
        }
    }
}

private fun parseJson(text: String): JsonElement? =
    try {
        Json.parseToJsonElement(text)
    } catch (e: Exception) {
        null
    }

/** Every operator must map to an array of objects whose "unit" is a 5 or 6 digit number. */
private fun validationError(root: JsonObject): String? {
    for ((op, v) in root) {
        val arr = v as? JsonArray
        if (op.isBlank() || arr == null) return "bad operator entry: $op"
        for (item in arr) {
            val unit = ((item as? JsonObject)?.get("unit") as? JsonPrimitive)?.contentOrNull
            if (unit == null || !UNIT_NUMBER.matches(unit)) return "bad unit under $op"
        }
    }
    return null
}

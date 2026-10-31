package com.traintracker.server.routes

import com.traintracker.server.auth.AuthDatabase
import com.traintracker.server.auth.checkAuth
import com.traintracker.server.cif.SmartLookup
import io.ktor.server.request.header
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post

fun Route.smartBerthRoutes() {
    get("/admin/smart-berths") {
        if (!call.checkAuth()) return@get
        val p = call.parameters
        val berths = SmartLookup.listBerths(
            p["stanox"]?.trim() ?: "",
            p["td"]?.trim() ?: "",
            p["berth"]?.trim() ?: "",
            p["limit"]?.toIntOrNull() ?: 200
        )
        call.respond(mapOf("berths" to berths))
    }
    post("/admin/smart-berth") {
        if (!call.checkAuth()) return@post
        val p = call.parameters
        val offset = p["offset"]?.trim()?.toIntOrNull()
        if (offset == null) {
            call.badRequest("offset (whole seconds) required")
            return@post
        }
        val by = AuthDatabase.validateSession(call.request.header("Authorization")?.removePrefix("Bearer ")?.trim() ?: "") ?: "unknown"
        val err = SmartLookup.addBerth(
            p["td"] ?: "",
            p["from"] ?: "",
            p["to"] ?: "",
            p["stanox"] ?: "",
            p["event"] ?: "",
            p["platform"] ?: "",
            p["stepType"] ?: "",
            offset,
            by
        )
        if (err != null) {
            call.badRequest(err)
            return@post
        }
        call.respond(mapOf("status" to "ok"))
    }
    delete("/admin/smart-berth") {
        if (!call.checkAuth()) return@delete
        val id = call.parameters["id"]?.trim()?.toLongOrNull()
        if (id == null) {
            call.badRequest("id required")
            return@delete
        }
        SmartLookup.deleteBerth(id)
        call.respond(mapOf("status" to "ok", "id" to id.toString()))
    }
}

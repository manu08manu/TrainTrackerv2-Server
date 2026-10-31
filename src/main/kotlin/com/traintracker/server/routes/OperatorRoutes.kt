package com.traintracker.server.routes

import com.traintracker.server.auth.AuthDatabase
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get

fun Route.operatorRoutes() {
    get("/operators") {
        call.respond(mapOf("operators" to AuthDatabase.getOperators()))
    }
    adminPost("/admin/operators") {
        val code = param("code", true)
        val name = param("name")
        val colour = param("colour")
        if (!Regex("^[A-Z0-9]{2,10}$").matches(code)) {
            badRequest("code must be 2-10 letters or numbers")
            return@adminPost
        }
        if (name.isEmpty() || name.length > 60) {
            badRequest("name required, 60 characters max")
            return@adminPost
        }
        if (colour.isNotEmpty() && !Regex("^#[0-9A-Fa-f]{6}$").matches(colour)) {
            badRequest("colour must look like #1A2B3C")
            return@adminPost
        }
        AuthDatabase.upsertOperator(code, name, colour)
        respondOk("code" to code)
    }
    adminDelete("/admin/operators") {
        val code = param("code", true)
        if (code.isEmpty()) {
            badRequest("code required")
            return@adminDelete
        }
        AuthDatabase.deleteOperator(code)
        respondOk("code" to code)
    }
}

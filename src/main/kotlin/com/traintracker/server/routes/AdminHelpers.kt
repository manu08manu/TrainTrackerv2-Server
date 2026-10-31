package com.traintracker.server.routes

import com.traintracker.server.auth.checkAuth
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.put

internal fun Route.adminGet(path: String, handler: suspend ApplicationCall.() -> Unit) {
    get(path) {
        if (call.checkAuth()) call.handler()
    }
}

internal fun Route.adminPost(path: String, handler: suspend ApplicationCall.() -> Unit) {
    post(path) {
        if (call.checkAuth()) call.handler()
    }
}

internal fun Route.adminDelete(path: String, handler: suspend ApplicationCall.() -> Unit) {
    delete(path) {
        if (call.checkAuth()) call.handler()
    }
}

internal fun Route.adminPut(path: String, handler: suspend ApplicationCall.() -> Unit) {
    put(path) {
        if (call.checkAuth()) call.handler()
    }
}

internal fun ApplicationCall.param(name: String, uppercase: Boolean = false): String {
    val v = parameters[name]?.trim() ?: ""
    return if (uppercase) v.uppercase() else v
}

internal suspend fun ApplicationCall.respondOk(vararg details: Pair<String, String?>) {
    respond(mapOf("status" to "ok", *details))
}

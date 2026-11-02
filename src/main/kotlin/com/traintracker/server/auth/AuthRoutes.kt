package com.traintracker.server.auth

import com.traintracker.server.routes.adminGet
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.header
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import org.json.JSONObject

fun Route.authRoutes() {
    post("/auth/login") {
        val body = JSONObject(call.receiveText())
        val email = body.optString("email", "").trim()
        val password = body.optString("password", "")
        val token = AuthDatabase.login(email, password)
        if (token == null) {
            call.respond(HttpStatusCode.Unauthorized, mapOf("error" to "Invalid email or password"))
        } else {
            call.respond(mapOf("token" to token, "email" to email))
        }
    }

    post("/auth/logout") {
        val token = call.request.header("Authorization")?.removePrefix("Bearer ")?.trim()
        if (token != null) AuthDatabase.logout(token)
        call.respond(mapOf("status" to "ok"))
    }

    get("/auth/me") {
        val token = call.request.header("Authorization")?.removePrefix("Bearer ")?.trim()
        val email = if (token != null) AuthDatabase.validateSession(token) else null
        if (email == null) {
            call.respond(HttpStatusCode.Unauthorized, mapOf("error" to "Not authenticated"))
        } else {
            call.respond(mapOf("email" to email))
        }
    }

    post("/auth/change-password") {
        val token = call.request.header("Authorization")?.removePrefix("Bearer ")?.trim()
        val email = if (token != null) AuthDatabase.validateSession(token) else null
        if (email == null) {
            call.respond(HttpStatusCode.Unauthorized, mapOf("error" to "Authentication required"))
            return@post
        }
        val body = JSONObject(call.receiveText())
        val newPassword = body.optString("password", "")
        if (newPassword.length !in 8..128) {
            call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Password (8-128 chars) required"))
            return@post
        }
        AuthDatabase.setUser(email, newPassword)
        call.respond(mapOf("status" to "ok"))
    }

    post("/admin/master-reset") {
        val remote = call.request.local.remoteHost
        if (!setOf("127.0.0.1", "::1", "0:0:0:0:0:0:0:1", "localhost").contains(remote)) {
            call.respond(HttpStatusCode.Forbidden, mapOf("error" to "Local access only"))
            return@post
        }
        val body = JSONObject(call.receiveText())
        val email = body.optString("email", "").trim()
        val password = body.optString("password", "")
        if (email.isEmpty() || password.length !in 8..128) {
            call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Email and password (8-128 chars) required"))
            return@post
        }
        AuthDatabase.setUser(email, password)
        call.respond(mapOf("status" to "ok", "message" to "User set: $email"))
    }

    adminGet("/admin/users") {
        respond(mapOf("users" to AuthDatabase.listUsers()))
    }

    post("/admin/users") {
        if (call.requireMasterAdmin() == null) return@post
        val body = JSONObject(call.receiveText())
        val email = body.optString("email", "").trim()
        val password = body.optString("password", "")
        if (email.isEmpty() || password.length !in 8..128) {
            call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Email and password (8-128 chars) required"))
            return@post
        }
        AuthDatabase.setUser(email, password)
        call.respond(mapOf("status" to "ok"))
    }

    delete("/admin/users/{email}") {
        if (call.requireMasterAdmin() == null) return@delete
        val email = call.parameters["email"]?.trim() ?: return@delete call.respond(HttpStatusCode.BadRequest, "email required")
        if (AuthDatabase.isMasterAdmin(email) && AuthDatabase.countMasterAdmins() <= 1) {
            call.respond(HttpStatusCode.Conflict, mapOf("error" to "Cannot remove the last master admin"))
            return@delete
        }
        AuthDatabase.deleteUser(email)
        call.respond(mapOf("status" to "ok"))
    }

    post("/admin/users/{email}/promote") {
        if (call.requireMasterAdmin() == null) return@post
        val email = call.parameters["email"]?.trim() ?: return@post call.respond(HttpStatusCode.BadRequest, "email required")
        AuthDatabase.setMasterAdmin(email, true)
        call.respond(mapOf("status" to "ok"))
    }

    post("/admin/users/{email}/demote") {
        if (call.requireMasterAdmin() == null) return@post
        val email = call.parameters["email"]?.trim() ?: return@post call.respond(HttpStatusCode.BadRequest, "email required")
        val ok = AuthDatabase.setMasterAdmin(email, false)
        if (!ok) {
            call.respond(HttpStatusCode.Conflict, mapOf("error" to "Cannot remove the last master admin"))
            return@post
        }
        // Live bytecode responds twice here (two consecutive respond calls); kept as-is for identical behaviour.
        call.respond(mapOf("status" to "ok"))
        call.respond(mapOf("status" to "ok"))
    }

    adminGet("/admin/tiploc-log") {
        val rows = AuthDatabase.getTiplocAuditLog(100)
        respond(mapOf("log" to rows))
    }

    adminGet("/admin/tiploc-crs-log") {
        val rows = AuthDatabase.getTiplocCrsAuditLog(100)
        respond(mapOf("log" to rows))
    }

    adminGet("/admin/tsc-operator-log") {
        val rows = AuthDatabase.getTscOperatorAuditLog(100)
        respond(mapOf("log" to rows))
    }

    adminGet("/admin/recent-changes") {
        val rows = AuthDatabase.getCombinedAuditLog(100)
        respond(mapOf("log" to rows))
    }
}

suspend fun ApplicationCall.checkAuth(): Boolean {
    val token = request.header("Authorization")?.removePrefix("Bearer ")?.trim()
    return if (token != null && AuthDatabase.validateSession(token) != null) {
        true
    } else {
        respond(HttpStatusCode.Unauthorized, mapOf("error" to "Authentication required"))
        false
    }
}

suspend fun ApplicationCall.requireMasterAdmin(): String? {
    val token = request.header("Authorization")?.removePrefix("Bearer ")?.trim()
    val email = if (token != null) AuthDatabase.validateSession(token) else null
    if (email == null) {
        respond(HttpStatusCode.Unauthorized, mapOf("error" to "Authentication required"))
        return null
    }
    if (!AuthDatabase.isMasterAdmin(email)) {
        respond(HttpStatusCode.Forbidden, mapOf("error" to "Master admin access required"))
        return null
    }
    return email
}

package com.traintracker.server.auth

import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import org.json.JSONObject

fun Route.authRoutes() {

    // ── POST /api/auth/login ──────────────────────────────────────────────
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

    // ── POST /api/auth/logout ─────────────────────────────────────────────
    post("/auth/logout") {
        val token = call.request.header("Authorization")?.removePrefix("Bearer ")?.trim()
        if (token != null) AuthDatabase.logout(token)
        call.respond(mapOf("status" to "ok"))
    }

    // ── GET /api/auth/me ──────────────────────────────────────────────────
    get("/auth/me") {
        val token = call.request.header("Authorization")?.removePrefix("Bearer ")?.trim()
        val email = if (token != null) AuthDatabase.validateSession(token) else null
        if (email == null) {
            call.respond(HttpStatusCode.Unauthorized, mapOf("error" to "Not authenticated"))
        } else {
            call.respond(mapOf("email" to email))
        }
    }

    // ── POST /api/admin/master-reset — localhost only ─────────────────────
    // To reset your password from the server:
    //   curl -s -X POST http://localhost:8080/api/admin/master-reset \
    //     -H 'Content-Type: application/json' \
    //     -d '{"email":"you@example.com","password":"newpassword"}'
    post("/admin/master-reset") {
        val remote = call.request.local.remoteHost
        if (remote !in setOf("127.0.0.1", "::1", "0:0:0:0:0:0:0:1", "localhost")) {
            call.respond(HttpStatusCode.Forbidden, mapOf("error" to "Local access only"))
            return@post
        }
        val body = JSONObject(call.receiveText())
        val email = body.optString("email", "").trim()
        val password = body.optString("password", "")
        if (email.isEmpty() || password.length < 8) {
            call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Email and password (8+ chars) required"))
            return@post
        }
        AuthDatabase.setUser(email, password)
        call.respond(mapOf("status" to "ok", "message" to "User set: $email"))
    }

    // ── GET /api/admin/users ──────────────────────────────────────────────
    get("/admin/users") {
        if (!call.checkAuth()) return@get
        call.respond(mapOf("users" to AuthDatabase.listUsers()))
    }

    // ── POST /api/admin/users — create / reset user ───────────────────────
    post("/admin/users") {
        if (!call.checkAuth()) return@post
        val body = JSONObject(call.receiveText())
        val email = body.optString("email", "").trim()
        val password = body.optString("password", "")
        if (email.isEmpty() || password.length < 8) {
            call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Email and password (8+ chars) required"))
            return@post
        }
        AuthDatabase.setUser(email, password)
        call.respond(mapOf("status" to "ok"))
    }

    // ── DELETE /api/admin/users/{email} ───────────────────────────────────
    delete("/admin/users/{email}") {
        if (!call.checkAuth()) return@delete
        val email = call.parameters["email"]?.trim()
            ?: return@delete call.respond(HttpStatusCode.BadRequest, "email required")
        AuthDatabase.deleteUser(email)
        call.respond(mapOf("status" to "ok"))
    }

    // ── GET /api/admin/tiploc-log ─────────────────────────────────────────
    get("/admin/tiploc-log") {
        if (!call.checkAuth()) return@get
        val rows = AuthDatabase.getTiplocAuditLog(100)
        call.respond(mapOf("log" to rows))
    }

}
/** Validates Bearer token. Responds 401 and returns false if invalid. */
suspend fun ApplicationCall.checkAuth(): Boolean {
    val token = request.header("Authorization")?.removePrefix("Bearer ")?.trim()
    return if (token != null && AuthDatabase.validateSession(token) != null) {
        true
    } else {
        respond(HttpStatusCode.Unauthorized, mapOf("error" to "Authentication required"))
        false
    }
}

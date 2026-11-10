package com.traintracker.server.auth

import com.traintracker.server.database.Overrides
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

fun ApplicationCall.bearerToken(): String? = request.header("Authorization")?.removePrefix("Bearer ")?.trim()

/** Email of the signed-in admin, or null. */
fun ApplicationCall.sessionEmail(): String? = bearerToken()?.let { AuthDatabase.validateSession(it) }

/** Email of the signed-in admin for audit records, or "unknown". */
fun ApplicationCall.changedBy(): String = AuthDatabase.validateSession(bearerToken() ?: "") ?: "unknown"

/** Responds 401 and returns false unless the request carries a valid session. */
suspend fun ApplicationCall.checkAuth(): Boolean {
    if (sessionEmail() != null) return true
    respond(HttpStatusCode.Unauthorized, mapOf("error" to "Authentication required"))
    return false
}

/** Responds 401/403 and returns null unless the caller is a master admin; otherwise returns their email. */
suspend fun ApplicationCall.requireMasterAdmin(): String? {
    val email = sessionEmail()
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

private const val PASSWORD_RULE = "Email and password (8-128 chars) required"

fun Route.authRoutes() {
    post("/auth/login") {
        val body = JSONObject(call.receiveText())
        val email = body.optString("email", "").trim()
        val token = AuthDatabase.login(email, body.optString("password", ""))
        if (token == null) {
            call.respond(HttpStatusCode.Unauthorized, mapOf("error" to "Invalid email or password"))
        } else {
            call.respond(mapOf("token" to token, "email" to email))
        }
    }

    post("/auth/logout") {
        call.bearerToken()?.let { AuthDatabase.logout(it) }
        call.respond(mapOf("status" to "ok"))
    }

    get("/auth/me") {
        val email = call.sessionEmail()
        if (email == null) {
            call.respond(HttpStatusCode.Unauthorized, mapOf("error" to "Not authenticated"))
        } else {
            call.respond(mapOf("email" to email))
        }
    }

    post("/auth/change-password") {
        val email = call.sessionEmail()
        if (email == null) {
            call.respond(HttpStatusCode.Unauthorized, mapOf("error" to "Authentication required"))
            return@post
        }
        val newPassword = JSONObject(call.receiveText()).optString("password", "")
        if (newPassword.length !in 8..128) {
            call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Password (8-128 chars) required"))
            return@post
        }
        AuthDatabase.setUser(email, newPassword)
        call.respond(mapOf("status" to "ok"))
    }

    // Sets any user's password; only accepted from the server itself.
    post("/admin/master-reset") {
        if (call.request.local.remoteHost !in setOf("127.0.0.1", "::1", "0:0:0:0:0:0:0:1", "localhost")) {
            call.respond(HttpStatusCode.Forbidden, mapOf("error" to "Local access only"))
            return@post
        }
        val body = JSONObject(call.receiveText())
        val email = body.optString("email", "").trim()
        val password = body.optString("password", "")
        if (email.isEmpty() || password.length !in 8..128) {
            call.respond(HttpStatusCode.BadRequest, mapOf("error" to PASSWORD_RULE))
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
            call.respond(HttpStatusCode.BadRequest, mapOf("error" to PASSWORD_RULE))
            return@post
        }
        AuthDatabase.setUser(email, password)
        call.respond(mapOf("status" to "ok"))
    }

    delete("/admin/users/{email}") {
        if (call.requireMasterAdmin() == null) return@delete
        val email = call.parameters["email"]!!.trim()
        if (AuthDatabase.isMasterAdmin(email) && AuthDatabase.countMasterAdmins() <= 1) {
            call.respond(HttpStatusCode.Conflict, mapOf("error" to "Cannot remove the last master admin"))
            return@delete
        }
        AuthDatabase.deleteUser(email)
        call.respond(mapOf("status" to "ok"))
    }

    post("/admin/users/{email}/promote") {
        if (call.requireMasterAdmin() == null) return@post
        AuthDatabase.setMasterAdmin(call.parameters["email"]!!.trim(), true)
        call.respond(mapOf("status" to "ok"))
    }

    post("/admin/users/{email}/demote") {
        if (call.requireMasterAdmin() == null) return@post
        if (!AuthDatabase.setMasterAdmin(call.parameters["email"]!!.trim(), false)) {
            call.respond(HttpStatusCode.Conflict, mapOf("error" to "Cannot remove the last master admin"))
            return@post
        }
        call.respond(mapOf("status" to "ok"))
    }

    adminGet("/admin/tiploc-log") {
        respond(mapOf("log" to Overrides.auditLog(Overrides.Audit.NAME, 100)))
    }

    adminGet("/admin/tiploc-crs-log") {
        respond(mapOf("log" to Overrides.auditLog(Overrides.Audit.CRS, 100)))
    }

    adminGet("/admin/tsc-operator-log") {
        respond(mapOf("log" to Overrides.auditLog(Overrides.Audit.OPERATOR, 100)))
    }

    adminGet("/admin/recent-changes") {
        respond(mapOf("log" to Overrides.combinedAuditLog(100)))
    }
}

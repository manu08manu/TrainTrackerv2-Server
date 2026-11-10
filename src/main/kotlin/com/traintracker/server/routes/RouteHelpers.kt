package com.traintracker.server.routes

import com.traintracker.server.auth.checkAuth
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import java.time.LocalDate
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.jdbc.transactions.transaction

// ---- Admin routes: the handler only runs for a signed-in admin ----------------------------------------

internal fun Route.adminGet(path: String, handler: suspend ApplicationCall.() -> Unit) {
    get(path) { if (call.checkAuth()) call.handler() }
}

internal fun Route.adminPost(path: String, handler: suspend ApplicationCall.() -> Unit) {
    post(path) { if (call.checkAuth()) call.handler() }
}

internal fun Route.adminDelete(path: String, handler: suspend ApplicationCall.() -> Unit) {
    delete(path) { if (call.checkAuth()) call.handler() }
}

internal fun Route.adminPut(path: String, handler: suspend ApplicationCall.() -> Unit) {
    put(path) { if (call.checkAuth()) call.handler() }
}

// ---- Parameters and responses -----------------------------------------------------------------------

/** A query or path parameter, trimmed; "" when absent. */
internal fun ApplicationCall.param(name: String, uppercase: Boolean = false): String {
    val v = parameters[name]?.trim() ?: ""
    return if (uppercase) v.uppercase() else v
}

/** A path parameter, trimmed. Always present once its route has matched. */
internal fun ApplicationCall.pathParam(name: String): String = parameters[name]!!.trim()

/** The "date" parameter when it is 10 characters long, else today. */
internal fun ApplicationCall.dateParam(): String =
    parameters["date"]?.trim()?.takeIf { it.length == 10 } ?: LocalDate.now().toString()

internal fun isValidCrs(crs: String): Boolean = crs.length == 3 && crs.all { it.isLetter() }

/** The "crs" parameter, uppercased; responds 400 and returns null when it is missing or invalid. */
internal suspend fun ApplicationCall.validCrs(): String? {
    val crs = parameters["crs"]?.uppercase()?.trim()
    if (crs == null) {
        respond(HttpStatusCode.BadRequest, "crs required")
        return null
    }
    if (!isValidCrs(crs)) {
        respond(HttpStatusCode.BadRequest, "crs must be exactly 3 letters")
        return null
    }
    return crs
}

internal suspend fun ApplicationCall.badRequest(msg: String) {
    respond(HttpStatusCode.BadRequest, mapOf("error" to msg))
}

internal suspend fun ApplicationCall.respondOk(vararg details: Pair<String, String?>) {
    respond(mapOf("status" to "ok", *details))
}

/** Runs [block] in a database transaction on the IO dispatcher. */
internal suspend fun <T> dbQuery(block: JdbcTransaction.() -> T): T = withContext(Dispatchers.IO) { transaction { block() } }

// ---- Clock times ("HH:mm") --------------------------------------------------------------------------

internal fun String.toMinutes(): Int =
    split(":").let { p -> (p.getOrNull(0)?.toIntOrNull() ?: 0) * 60 + (p.getOrNull(1)?.toIntOrNull() ?: 0) }

/** actual - scheduled in minutes, folded into [-720, 720). */
internal fun wrapDelta(actual: String, sched: String): Int = (actual.toMinutes() - sched.toMinutes() + 720).mod(1440) - 720

/** actual - scheduled in minutes, moved by a day when more than 12 hours apart. */
internal fun minutesLate(actual: String, sched: String): Int {
    val diff = actual.toMinutes() - sched.toMinutes()
    return when {
        diff < -720 -> diff + 1440
        diff > 720 -> diff - 1440
        else -> diff
    }
}

/** Minutes after midnight (any day) -> "HH:mm". */
internal fun clockTime(minutes: Int): String {
    val m = (minutes + 1440) % 1440
    return "%02d:%02d".format(m / 60, m % 60)
}

/** Sorts by "HH:mm" time, putting times earlier than [start] after midnight. */
internal fun <T> List<T>.sortedFromClock(start: String, time: (T) -> String): List<T> {
    val s = start.replace(":", "").toIntOrNull() ?: 0
    return sortedBy {
        val t = time(it).replace(":", "").toIntOrNull() ?: 0
        if (t < s) t + 2400 else t
    }
}

internal fun isPassengerHeadcode(headcode: String): Boolean = headcode.firstOrNull()?.let { "1239".contains(it) } == true

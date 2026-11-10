package com.traintracker.server.routes

import com.traintracker.server.Config
import com.traintracker.server.auth.authRoutes
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.database.Allocations
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.HeadcodeAliases
import com.traintracker.server.database.Schedules
import com.traintracker.server.database.TrainLocations
import com.traintracker.server.database.Movements
import com.traintracker.server.kafka.TrainLocation
import com.traintracker.server.kafka.trainLocations
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.http.content.staticFiles
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import java.io.File
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.greaterEq
import org.jetbrains.exposed.v1.core.max
import org.jetbrains.exposed.v1.core.min
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll

fun Application.configureRoutes() {
    routing {
        pageRoutes()
        staticFiles("/css", File("${Config.DATA_DIR}/static/css"))
        staticFiles("/js", File("${Config.DATA_DIR}/static/js"))
        staticFiles("/icons", File("${Config.DATA_DIR}/static/icons"))
        route("/api") {
            authRoutes()
            adminRoutes()
            prideRoutes()
            statusRoutes()
            boardRoutes()
            serviceDetailRoutes()
            serviceHistoryRoutes()
            punctualityRoutes()
            trustRoutes()
            allocationRoutes()
            kbRoutes()
        }
    }
}

private fun Route.statusRoutes() {
    get("/status") {
        call.respond(AppDatabase.status(trainLocations.isNotEmpty(), trainLocations.size))
    }
    // Written daily by scripts/cif_name_watch.sh
    get("/cif-changes") {
        val file = File("${Config.DATA_DIR}/cif_changes.json")
        val json = if (file.exists()) file.readText() else "{\"last_run\":null,\"checked\":0,\"change_count\":0,\"changes\":[]}"
        call.respondText(json, ContentType.Application.Json)
    }
    get("/stations") {
        val q = call.parameters["q"]?.trim() ?: ""
        if (q.length < 2) return@get call.respond(emptyList<Map<String, String>>())
        call.respond(CorpusLookup.searchStations(q, 20))
    }
    // Dates the board can show: from the oldest TRUST movement to the end of the timetable.
    get("/board-date-range") {
        val range = dbQuery {
            val minEventTs = Movements.eventTs.min()
            val minDate = Movements.select(minEventTs).singleOrNull()?.get(minEventTs)?.toLocalDate()?.toString()
            val maxScheduleEnd = Schedules.scheduleEndDate.max()
            val maxDate = Schedules.select(maxScheduleEnd).where { Schedules.stpIndicator eq 'P' }.singleOrNull()?.get(maxScheduleEnd)
            BoardDateRangeResponse(minDate ?: "2026-01-01", maxDate ?: "2026-12-31")
        }
        call.respond(range)
    }
}

// ---- Boards and searches -----------------------------------------------------------------------------

/** ("HH:mm", "HH:mm") from now + offset, [windowMinutes] long. */
private fun timeWindow(windowMinutes: Int, offsetMinutes: Int = 0): Pair<String, String> {
    val fmt = DateTimeFormatter.ofPattern("HH:mm")
    val start = LocalTime.now().plusMinutes(offsetMinutes.toLong())
    return start.format(fmt) to start.plusMinutes(windowMinutes.toLong()).format(fmt)
}

/** The "from" parameter ("HH:mm") as minutes after midnight, or null when absent or invalid. */
private fun ApplicationCall.fromMinutes(): Int? {
    val raw = parameters["from"]?.trim() ?: return null
    if (!Regex("^([01]\\d|2[0-3]):[0-5]\\d$").matches(raw)) return null
    return raw.toMinutes()
}

/** A window of [windowMinutes] starting at [fromMinutes] after midnight, ending no later than 23:59. */
private fun fixedWindow(fromMinutes: Int, windowMinutes: Int): Pair<String, String> =
    clockTime(fromMinutes) to clockTime(minOf(fromMinutes + windowMinutes.coerceIn(1, 1439), 1439))

/** Runs a service search off the request thread and adds units and associations. */
private suspend fun searchServices(date: String, query: () -> List<ServiceResponse>): List<ServiceResponse> =
    withContext(Dispatchers.IO) { annotateSplits(enrichWithUnits(query(), date)) }

/** Responds with a whole-day search result, or 404 with [notFound] when there is none. */
private suspend fun ApplicationCall.respondSearch(key: String, type: String, services: List<ServiceResponse>, date: String, notFound: String) {
    if (services.isEmpty()) {
        respond(HttpStatusCode.NotFound, notFound)
    } else {
        respond(BoardResponse(key, type, "00:00", "23:59", services.map { if (it.serviceDate.isEmpty()) it.copy(serviceDate = date) else it }))
    }
}

/**
 * Departure or arrival board for passenger trains. Today the window is clamped to start no earlier than midnight.
 * A window starting before 13:00 also shows yesterday's trains running past midnight.
 */
private suspend fun ApplicationCall.respondBoard(mode: BoardMode) {
    val crs = parameters["crs"]?.uppercase()?.trim() ?: return respond(HttpStatusCode.BadRequest, "crs required")
    val window = parameters["window"]?.toIntOrNull() ?: 120
    val offsetRaw = parameters["offset"]?.toIntOrNull() ?: 0
    val date = dateParam()
    val from = fromMinutes()
    val (start, end) = if (from != null) {
        fixedWindow(from, window)
    } else if (date == LocalDate.now().toString()) {
        val nowMins = LocalTime.now().let { it.hour * 60 + it.minute }
        val offset = maxOf(offsetRaw, -nowMins)
        if (offset >= 1440 - nowMins) return respond(BoardResponse(crs, mode.name, "23:59", "23:59", emptyList()))
        timeWindow(window, offset)
    } else {
        timeWindow(window, offsetRaw)
    }
    val early = end >= start && start.toMinutes() < 780
    val services = withContext(Dispatchers.IO) {
        val own = annotateSplits(enrichWithUnits(
            queryBoard(crs, start, end, mode, true, true, date, if (early) RolledFilter.EXCLUDE else RolledFilter.ANY), date
        ))
        if (!early) {
            own
        } else {
            val prevDate = try {
                LocalDate.parse(date).minusDays(1L).toString()
            } catch (e: Exception) {
                date
            }
            val overnight = annotateSplits(enrichWithUnits(queryBoard(crs, start, end, mode, true, true, prevDate, RolledFilter.ONLY), prevDate))
            (own + overnight).sortedBy { s ->
                val sm = s.scheduledTime.toMinutes()
                if (s.isCancelled || s.actualTime.isEmpty()) sm else sm + wrapDelta(s.actualTime, s.scheduledTime)
            }
        }
    }
    respond(BoardResponse(crs, mode.name, start, end, services))
}

private fun Route.boardRoutes() {
    get("/departures") { call.respondBoard(BoardMode.DEPARTURES) }
    get("/arrivals") { call.respondBoard(BoardMode.ARRIVALS) }
    // Every call, including passing points and empty stock, without the activation filter.
    get("/allservices") {
        val crs = call.parameters["crs"]?.uppercase()?.trim() ?: return@get call.respond(HttpStatusCode.BadRequest, "crs required")
        val window = call.parameters["window"]?.toIntOrNull() ?: 120
        val offset = call.parameters["offset"]?.toIntOrNull() ?: 0
        val date = call.dateParam()
        val (start, end) = call.fromMinutes()?.let { fixedWindow(it, window) } ?: timeWindow(window, offset)
        val services = withContext(Dispatchers.IO) { enrichWithUnits(queryBoard(crs, start, end, BoardMode.ALL, false, false, date), date) }
        call.respond(BoardResponse(crs, "ALL", start, end, services))
    }
    get("/toc/{code}") {
        val toc = call.pathParam("code").uppercase().filter { it.isLetterOrDigit() }
        val date = call.dateParam()
        call.respondSearch(toc, "TOC", searchServices(date) { queryByToc(toc, date) }, date, "No services found for operator $toc")
    }
    get("/headcode/{headcode}") {
        val headcode = call.pathParam("headcode").uppercase()
        val safeHc = headcode.filter { it.isLetterOrDigit() }
        val date = call.dateParam()
        var services = searchServices(date) { queryByHeadcode(safeHc, date) }
        if (services.isEmpty()) {
            // Search under the other headcode of a TRUST change of identity, showing the requested one.
            val aliasHc = dbQuery {
                HeadcodeAliases.selectAll().where { HeadcodeAliases.newHeadcode eq safeHc }.firstOrNull()?.get(HeadcodeAliases.oldHeadcode)
                    ?: HeadcodeAliases.selectAll().where { HeadcodeAliases.oldHeadcode eq safeHc }.firstOrNull()?.get(HeadcodeAliases.newHeadcode)
            }
            if (aliasHc != null && aliasHc != safeHc) {
                services = searchServices(date) { queryByHeadcode(aliasHc, date) }.map { it.copy(headcode = safeHc) }
            }
        }
        if (services.isNotEmpty()) {
            // A headcode changed today is shown under its new headcode.
            val renamed = dbQuery {
                HeadcodeAliases.selectAll().where { HeadcodeAliases.oldHeadcode eq safeHc }.firstOrNull()
                    ?.takeIf { it[HeadcodeAliases.updatedAt].toLocalDate() == LocalDate.now() }
                    ?.let { it[HeadcodeAliases.newHeadcode] to it[HeadcodeAliases.uid] }
            }
            if (renamed != null) {
                val (newHc, uid) = renamed
                services = services.map { if (uid.isEmpty() || uid == it.uid) it.copy(headcode = newHc) else it }
            }
        }
        call.respondSearch(headcode, "HEADCODE", services, date, "No schedule found for headcode $headcode")
    }
    get("/unit/{unit}") {
        val unit = call.pathParam("unit").uppercase()
        val date = call.dateParam()
        val services = withContext(Dispatchers.IO) { queryByUnit(unit.filter { it.isLetterOrDigit() }, date) }
        call.respondSearch(unit, "UNIT", services, date, "No services found for unit $unit")
    }
}

// ---- Live train positions from TRUST -------------------------------------------------------------------

private fun TrainLocation.toResponse() = TrainLocationResponse(
    headcode, rid, stationName, crs, actualTime, eventType, delayMinutes, (System.currentTimeMillis() - updatedEpochMs) / 1000
)

private fun ResultRow.toLocationResponse() = TrainLocationResponse(
    this[TrainLocations.headcode],
    this[TrainLocations.rid],
    this[TrainLocations.stationName],
    this[TrainLocations.crs],
    this[TrainLocations.actualTime],
    this[TrainLocations.eventType],
    this[TrainLocations.delayMinutes],
    -1L
)

private fun Route.trustRoutes() {
    // Last departure of a headcode, from memory or else the last 24 hours in the database.
    get("/trust/{headcode}") {
        val headcode = call.pathParam("headcode").uppercase()
        trainLocations.values.filter { it.headcode == headcode }.maxByOrNull { it.updatedEpochMs }
            ?.let { return@get call.respond(it.toResponse()) }
        val dbLoc = dbQuery {
            TrainLocations.selectAll().where {
                (TrainLocations.headcode eq headcode) and (TrainLocations.updatedAt greaterEq LocalDateTime.now().minusHours(24))
            }.orderBy(TrainLocations.updatedAt, SortOrder.DESC).limit(1).firstOrNull()
        }
        if (dbLoc != null) call.respond(dbLoc.toLocationResponse()) else call.respond(HttpStatusCode.NotFound, "No location data for $headcode")
    }
    get("/trust/locate/{rid}") {
        val rid = call.pathParam("rid")
        trainLocations.values.firstOrNull { it.rid == rid }?.let { return@get call.respond(it.toResponse()) }
        val dbLoc = dbQuery {
            TrainLocations.selectAll().where {
                (TrainLocations.rid eq rid) and (TrainLocations.updatedAt greaterEq LocalDateTime.now().minusHours(24))
            }.singleOrNull()
        }
        if (dbLoc != null) call.respond(dbLoc.toLocationResponse()) else call.respond(HttpStatusCode.NotFound, "No location data for rid=$rid")
    }
    // The latest 50 movements of a headcode (optionally one uid) in the last 3 hours.
    get("/trust/movements") {
        val headcode = call.parameters["headcode"]?.uppercase()?.trim()
            ?: return@get call.respond(HttpStatusCode.BadRequest, "headcode required")
        val safeHc = headcode.filter { it.isLetterOrDigit() }
        val safeUid = (call.parameters["uid"]?.uppercase()?.trim() ?: "").filter { it.isLetterOrDigit() }
        val movements = dbQuery {
            Movements.selectAll().where {
                (Movements.headcode eq safeHc) and
                    (if (safeUid.isNotEmpty()) Movements.uid eq safeUid else Op.TRUE) and
                    (Movements.eventTs greaterEq LocalDateTime.now().minusHours(3))
            }.orderBy(Movements.eventTs, SortOrder.DESC).limit(50).map { row ->
                MovementResponse(
                    crs = row[Movements.crs] ?: row[Movements.stanox],
                    eventType = row[Movements.eventType],
                    scheduledTime = row[Movements.scheduledTime] ?: "",
                    actualTime = row[Movements.actualTime] ?: "",
                    platform = row[Movements.platform] ?: "",
                    isCancelled = row[Movements.isCancelled]
                )
            }
        }
        call.respond(MovementsResponse(headcode, movements))
    }
}

// ---- Historical Service Performance ----------------------------------------------------------------

/** The JSON object body; responds 400 and returns null when it is not one. */
private suspend fun ApplicationCall.jsonBody(): JsonObject? {
    val text = receiveText()
    return try {
        Json.parseToJsonElement(text).jsonObject
    } catch (e: Exception) {
        badRequest("Invalid JSON body")
        null
    }
}

// ---- Unit allocations -----------------------------------------------------------------------------------

private fun Allocations.Allocation.toResponse() = AllocationResponse(
    coreId, headcode, serviceDate, operator, units, vehicles, unitCount, vehicles.size, updatedAt
)

private fun Route.allocationRoutes() {
    // By core ID, else every allocation of a headcode (one object, or an array when there are several).
    get("/allocation/{param}") {
        val param = call.pathParam("param")
        val results = Allocations.byCoreId(param)?.let { listOf(it) }
            ?: Allocations.byHeadcode(param.uppercase(), call.request.queryParameters["date"])
        when (results.size) {
            0 -> call.respond(HttpStatusCode.NotFound, "No allocation data for $param")
            1 -> call.respond(results.first().toResponse())
            else -> call.respond(results.map { it.toResponse() })
        }
    }
    get("/allocation/uid/{uid}") {
        val uid = call.pathParam("uid")
        val result = Allocations.byUid(uid, call.request.queryParameters["date"])
        if (result == null) call.respond(HttpStatusCode.NotFound, "No allocation data for uid $uid") else call.respond(result.toResponse())
    }
    get("/allocation/history/{uid}") {
        val uid = call.pathParam("uid").filter { it.isLetterOrDigit() }
        val date = call.request.queryParameters["date"]?.filter { it.isDigit() || it == '-' }?.takeIf { it.isNotEmpty() }
            ?: LocalDate.now().toString()
        call.respond(Allocations.changes(uid, date).map {
            AllocationChangeResponse(it.changedAt, it.headcode, it.oldUnits, it.newUnits, it.oldCoaches, it.newCoaches)
        })
    }
}

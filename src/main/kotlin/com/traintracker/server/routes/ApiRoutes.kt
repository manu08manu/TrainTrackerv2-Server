package com.traintracker.server.routes

import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.Schedules
import com.traintracker.server.database.TrainLocations
import com.traintracker.server.database.TrustMovements
import com.traintracker.server.kafka.trainLocations
import com.traintracker.server.hsp.HspClient
import com.traintracker.server.hsp.HspMetricsRequest
import io.ktor.http.*
import io.ktor.server.request.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.LocalTime
import java.time.format.DateTimeFormatter

// ─── Response models ──────────────────────────────────────────────────────────

@Serializable
data class ServiceResponse(
    val uid: String,
    val headcode: String,
    val atocCode: String,
    val scheduledTime: String,
    val platform: String?,
    val isPass: Boolean,
    val stopType: String,
    val originTiploc: String,
    val destTiploc: String,
    val originCrs: String?,
    val destCrs: String?,
    val actualTime: String = "",
    val isCancelled: Boolean = false,
    val cancelReason: String? = null
)

@Serializable
data class CallingPointResponse(
    val tiploc: String,
    val crs: String?,
    val scheduledTime: String,
    val platform: String?,
    val isPass: Boolean,
    val stopType: String,
    val actualTime: String = "",
    val isCancelled: Boolean = false
)

@Serializable
data class ServiceDetailResponse(
    val uid: String,
    val atCrs: String,
    val previous: List<CallingPointResponse>,
    val subsequent: List<CallingPointResponse>
)

@Serializable
data class TrainLocationResponse(
    val headcode: String,
    val stationName: String,
    val crs: String?,
    val actualTime: String,
    val eventType: String,
    val delayMinutes: Int,
    val ageSeconds: Long
)

@Serializable
data class BoardResponse(
    val crs: String,
    val boardType: String,
    val windowStart: String,
    val windowEnd: String,
    val services: List<ServiceResponse>
)

@Serializable
data class AllocationResponse(
    val coreId:      String,
    val headcode:    String,
    val serviceDate: String,
    val operator:    String,
    val units:       List<String>,
    val vehicles:    List<String>,
    val unitCount:   Int,
    val coachCount:  Int
)

@Serializable
data class StatusResponse(
    val cifLastDownload: String?,
    val trustConnected: Boolean,
    val trainLocationsCount: Int
)

@Serializable
data class MovementResponse(
    val crs: String,
    val eventType: String,
    val scheduledTime: String,
    val actualTime: String,
    val platform: String,
    val isCancelled: Boolean
)

@Serializable
data class MovementsResponse(
    val headcode: String,
    val movements: List<MovementResponse>
)


fun Application.configureRoutes() {
    routing {
        route("/api") {

            // ── GET /api/status ────────────────────────────────────────────
            get("/status") {
                call.respond(
                    StatusResponse(
                        cifLastDownload     = AppDatabase.getCifMeta("last_download_date"),
                        trustConnected      = trainLocations.isNotEmpty(),
                        trainLocationsCount = trainLocations.size
                    )
                )
            }

            // ── GET /api/departures?crs=MAN&window=120 ────────────────────
            get("/departures") {
                val crs    = call.parameters["crs"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "crs required")
                val window = call.parameters["window"]?.toIntOrNull() ?: 120
                val offset = call.parameters["offset"]?.toIntOrNull() ?: 0
                val (start, end) = timeWindow(window, offset)
                val services = queryBoard(crs, start, end, arrivalsOnly = false, passengerOnly = true, excludePassing = true)
                val board = BoardResponse(crs, "DEPARTURES", start, end, services)
                call.respond(board)
            }

            // ── GET /api/arrivals?crs=MAN&window=120 ──────────────────────
            get("/arrivals") {
                val crs    = call.parameters["crs"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "crs required")
                val window = call.parameters["window"]?.toIntOrNull() ?: 120
                val offset = call.parameters["offset"]?.toIntOrNull() ?: 0
                val (start, end) = timeWindow(window, offset)
                val services = queryBoard(crs, start, end, arrivalsOnly = true, passengerOnly = true, excludePassing = true)
                val board = BoardResponse(crs, "ARRIVALS", start, end, services)
                call.respond(board)
            }

            // ── GET /api/service/{uid}?crs=MAN ────────────────────────────
            get("/service/{uid}") {
                val uid   = call.parameters["uid"]?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "uid required")
                val atCrs = call.parameters["crs"]?.uppercase()?.trim() ?: ""

                val callingPoints = transaction {
                    Schedules.select { Schedules.uid eq uid }
                        .orderBy(Schedules.scheduledTime, SortOrder.ASC)
                        .toList()
                        .filter { it[Schedules.stpIndicator] != 'C' }
                        .let { rows ->
                            val priority = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)
                            val bestStp = rows.minOfOrNull { priority[it[Schedules.stpIndicator]] ?: 99 }
                            val bestRows = rows.filter { (priority[it[Schedules.stpIndicator]] ?: 99) == bestStp }
                            // If the overlay only has LO+LT (no intermediates), inherit LI stops from P schedule
                            val bestSttpChar = priority.entries.find { it.value == bestStp }?.key
                            if (bestSttpChar != null && bestSttpChar != 'P' &&
                                bestRows.none { it[Schedules.stopType] == "LI" }) {
                                val loTime = bestRows.firstOrNull { it[Schedules.stopType] == "LO" }?.get(Schedules.scheduledTime) ?: "00:00"
                                val ltTime = bestRows.firstOrNull { it[Schedules.stopType] == "LT" }?.get(Schedules.scheduledTime) ?: "99:99"
                                val pRows = rows.filter { it[Schedules.stpIndicator] == 'P' && it[Schedules.stopType] == "LI" && it[Schedules.scheduledTime] > loTime && it[Schedules.scheduledTime] < ltTime }
                                (bestRows + pRows).sortedBy { it[Schedules.scheduledTime] }
                            } else bestRows
                        }
                        .let { rows ->
                            val priority = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)
                            val bestStp = rows.minOfOrNull { priority[it[Schedules.stpIndicator]] ?: 99 }
                            val destRow = rows.firstOrNull {
                                (priority[it[Schedules.stpIndicator]] ?: 99) == bestStp &&
                                it[Schedules.stopType] == "LT"
                            }
                            val destTiploc = destRow?.get(Schedules.tiploc)
                            if (destTiploc != null) {
                                val sorted = rows.sortedBy { it[Schedules.scheduledTime] }
                                val ltIndex = sorted.indexOfFirst {
                                    it[Schedules.tiploc] == destTiploc && it[Schedules.stopType] == "LT"
                                }.let { if (it < 0) sorted.indexOfFirst { r -> r[Schedules.tiploc] == destTiploc } else it }
                                val deduped = sorted.filterIndexed { i, r ->
                                    r[Schedules.tiploc] != destTiploc || i == ltIndex
                                }
                                val finalIdx = deduped.indexOfFirst { it[Schedules.tiploc] == destTiploc }
                                if (finalIdx >= 0) deduped.subList(0, finalIdx + 1) else deduped
                            } else rows.sortedBy { it[Schedules.scheduledTime] }
                        }
                        .map { row ->
                            CallingPointResponse(
                                tiploc        = row[Schedules.tiploc],
                                crs           = row[Schedules.crs],
                                scheduledTime = row[Schedules.scheduledTime],
                                platform      = row[Schedules.platform],
                                isPass        = row[Schedules.isPass],
                                stopType      = row[Schedules.stopType]
                            )
                        }
                }

                if (callingPoints.isEmpty()) {
                    call.respond(HttpStatusCode.NotFound, "Service $uid not found")
                    return@get
                }

                val atIndex    = callingPoints.indexOfFirst { it.crs == atCrs }
                    .let { if (it < 0 && atCrs.isNotEmpty()) callingPoints.indexOfFirst { cp -> cp.tiploc.startsWith(atCrs) } else if (it < 0) 0 else it }
                val previous   = if (atCrs.isNotEmpty() && atIndex > 0) callingPoints.subList(0, atIndex) else emptyList()
                val subsequent = if (atCrs.isEmpty()) callingPoints else if (atIndex >= 0 && atIndex + 1 < callingPoints.size) callingPoints.subList(atIndex + 1, callingPoints.size) else emptyList()

                call.respond(ServiceDetailResponse(uid, atCrs, previous, subsequent))
            }

            // ── GET /api/allservices?crs=MAN&window=120 ───────────────────
            get("/allservices") {
                val crs    = call.parameters["crs"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "crs required")
                val window = call.parameters["window"]?.toIntOrNull() ?: 120
                val offset = call.parameters["offset"]?.toIntOrNull() ?: 0
                val (start, end) = timeWindow(window, offset)
                val services = queryBoard(crs, start, end, arrivalsOnly = true, passengerOnly = false, excludePassing = false)
                call.respond(BoardResponse(crs, "ALL", start, end, services))
            }

            // ── GET /api/trust/{headcode} ─────────────────────────────────
            get("/trust/{headcode}") {
                val headcode = call.parameters["headcode"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "headcode required")

                val loc = trainLocations[headcode]
                if (loc != null) {
                    call.respond(TrainLocationResponse(
                        headcode     = loc.headcode,
                        stationName  = loc.stationName,
                        crs          = loc.crs,
                        actualTime   = loc.actualTime,
                        eventType    = loc.eventType,
                        delayMinutes = loc.delayMinutes,
                        ageSeconds   = (System.currentTimeMillis() - loc.updatedEpochMs) / 1000
                    ))
                } else {
                    val dbLoc = transaction {
                        TrainLocations.select { TrainLocations.headcode eq headcode }.singleOrNull()
                    }
                    if (dbLoc != null) {
                        call.respond(TrainLocationResponse(
                            headcode     = dbLoc[TrainLocations.headcode],
                            stationName  = dbLoc[TrainLocations.stationName],
                            crs          = dbLoc[TrainLocations.crs],
                            actualTime   = dbLoc[TrainLocations.actualTime],
                            eventType    = dbLoc[TrainLocations.eventType],
                            delayMinutes = dbLoc[TrainLocations.delayMinutes],
                            ageSeconds   = -1
                        ))
                    } else {
                        call.respond(HttpStatusCode.NotFound, "No location data for $headcode")
                    }
                }
            }

            // ── GET /api/trust/movements?headcode=1A34 ────────────────────
            get("/trust/movements") {
                val headcode = call.parameters["headcode"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "headcode required")

                val safeHc = headcode.filter { it.isLetterOrDigit() }
                val movements = transaction {
                    val result = mutableListOf<MovementResponse>()
                    exec("SELECT crs, stanox, event_type, scheduled_time, actual_time, platform, is_cancelled FROM trust_movements WHERE headcode='$safeHc' AND event_ts >= datetime('now', '-3 hours') ORDER BY event_ts DESC LIMIT 50") { rs ->
                        while (rs.next()) {
                            result.add(MovementResponse(
                                crs           = rs.getString("crs") ?: rs.getString("stanox") ?: "",
                                eventType     = rs.getString("event_type") ?: "",
                                scheduledTime = rs.getString("scheduled_time") ?: "",
                                actualTime    = rs.getString("actual_time") ?: "",
                                platform      = rs.getString("platform") ?: "",
                                isCancelled   = rs.getBoolean("is_cancelled")
                            ))
                        }
                    }
                    result
                }
                call.respond(MovementsResponse(headcode, movements))
            }

            // ── GET /api/headcode/{headcode} ──────────────────────────────
            // Returns all CIF schedule entries for a headcode today, enriched
            // with TRUST actual times and cancellation data. The response uses
            // the same ServiceResponse shape as /api/departures so the Android
            // board UI can render it without any changes.
            get("/headcode/{headcode}") {
                val headcode = call.parameters["headcode"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "headcode required")
                val safeHc = headcode.filter { it.isLetterOrDigit() }

                val services = queryByHeadcode(safeHc)
                if (services.isEmpty()) {
                    call.respond(HttpStatusCode.NotFound, "No schedule found for headcode $headcode")
                } else {
                    call.respond(BoardResponse(
                        crs         = headcode,
                        boardType   = "HEADCODE",
                        windowStart = "00:00",
                        windowEnd   = "23:59",
                        services    = services
                    ))
                }
            }

            // ── POST /api/hsp/metrics ──────────────────────────────────────
            post("/hsp/metrics") {
                if (!HspClient.isAvailable) {
                    call.respond(HttpStatusCode.ServiceUnavailable, mapOf("error" to "HSP not configured on server"))
                    return@post
                }
                val body = call.receiveText()
                val json = try { Json.parseToJsonElement(body).jsonObject } catch (_: Exception) {
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid JSON body"))
                    return@post
                }
                val fromLoc = json["from_loc"]?.jsonPrimitive?.content?.uppercase()
                val fromDate = json["from_date"]?.jsonPrimitive?.content
                if (fromLoc == null || fromDate == null) {
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "from_loc and from_date required"))
                    return@post
                }
                val req = HspMetricsRequest(
                    from_loc  = fromLoc,
                    to_loc    = json["to_loc"]?.jsonPrimitive?.content?.uppercase() ?: fromLoc,
                    from_time = json["from_time"]?.jsonPrimitive?.content ?: "0000",
                    to_time   = json["to_time"]?.jsonPrimitive?.content ?: "2359",
                    from_date = fromDate,
                    to_date   = json["to_date"]?.jsonPrimitive?.content ?: fromDate,
                    days      = json["days"]?.jsonPrimitive?.content ?: HspClient.daysParam(fromDate)
                )
                val result = withContext(Dispatchers.IO) { HspClient.getMetrics(req) }
                if (result != null) call.respond(result)
                else call.respond(HttpStatusCode.BadGateway, mapOf("error" to "HSP API request failed"))
            }

            // ── GET /api/hsp/metrics/stream ────────────────────────────────
            // Returns full merged result as JSON — client treats as single progress event
            // Query params: from, to, date, from_time (opt), to_time (opt)
            get("/hsp/metrics/stream") {
                if (!HspClient.isAvailable) {
                    call.respond(HttpStatusCode.ServiceUnavailable, mapOf("error" to "HSP not configured"))
                    return@get
                }
                val from     = call.parameters["from"]?.uppercase()?.trim()
                val to       = call.parameters["to"]?.uppercase()?.trim()
                val date     = call.parameters["date"]?.trim()
                val fromTime = call.parameters["from_time"]?.trim() ?: "0000"
                val toTime   = call.parameters["to_time"]?.trim()   ?: "2359"

                if (from == null || to == null || date == null) {
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "from, to, date required"))
                    return@get
                }

                val req = HspMetricsRequest(
                    from_loc  = from,
                    to_loc    = to,
                    from_time = fromTime,
                    to_time   = toTime,
                    from_date = date,
                    to_date   = date,
                    days      = HspClient.daysParam(date)
                )
                val result = withContext(Dispatchers.IO) { HspClient.getMetrics(req) }
                if (result != null) call.respond(result)
                else call.respond(HttpStatusCode.BadGateway, mapOf("error" to "HSP API request failed"))
            }

            // ── POST /api/hsp/details ──────────────────────────────────────
            post("/hsp/details") {
                if (!HspClient.isAvailable) {
                    call.respond(HttpStatusCode.ServiceUnavailable, mapOf("error" to "HSP not configured on server"))
                    return@post
                }
                val body = call.receiveText()
                val json = try { Json.parseToJsonElement(body).jsonObject } catch (_: Exception) {
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid JSON body"))
                    return@post
                }
                val rid = json["rid"]?.jsonPrimitive?.content?.trim()
                if (rid.isNullOrEmpty()) {
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "rid required"))
                    return@post
                }
                val scheduledDep = json["scheduled_dep"]?.jsonPrimitive?.content?.trim() ?: ""
                val result = withContext(Dispatchers.IO) { HspClient.getDetails(rid) }
                if (result != null) {
                    val unitInfo = AppDatabase.getHspUnit(rid, scheduledDep)
                    val enriched = buildJsonObject {
                        put("rid",      result.rid)
                        put("date",     result.date)
                        put("tocCode",  result.tocCode)
                        put("unit",     unitInfo?.units?.firstOrNull() ?: "")
                        put("units",    Json.encodeToJsonElement(unitInfo?.units ?: emptyList<String>()))
                        put("vehicles", Json.encodeToJsonElement(unitInfo?.vehicles ?: emptyList<String>()))
                        put("unitCount", unitInfo?.unitCount ?: 0)
                        put("locations", Json.encodeToJsonElement(result.locations))
                    }
                    call.respond(enriched)
                } else {
                    call.respond(HttpStatusCode.NotFound, mapOf("error" to "No HSP detail found for rid=$rid"))
                }
            }

            // ── GET /api/allocation/{param}?date=2026-03-27 ───────────────
            get("/allocation/{param}") {
                val param = call.parameters["param"]?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "coreId or headcode required")
                val date = call.request.queryParameters["date"]

                val results = AppDatabase.getAllocationByCoreId(param)
                    ?.let { listOf(it) }
                    ?: AppDatabase.getAllocationsByHeadcode(param.uppercase(), date)

                when {
                    results.isEmpty() ->
                        call.respond(HttpStatusCode.NotFound, "No allocation data for $param")
                    results.size == 1 -> {
                        val r = results.first()
                        call.respond(AllocationResponse(
                            coreId      = r.coreId,
                            headcode    = r.headcode,
                            serviceDate = r.serviceDate,
                            operator    = r.operator,
                            units       = r.units,
                            vehicles    = r.vehicles,
                            unitCount   = r.unitCount,
                            coachCount  = r.vehicles.size
                        ))
                    }
                    else ->
                        call.respond(results.map { r ->
                            AllocationResponse(
                                coreId      = r.coreId,
                                headcode    = r.headcode,
                                serviceDate = r.serviceDate,
                                operator    = r.operator,
                                units       = r.units,
                                vehicles    = r.vehicles,
                                unitCount   = r.unitCount,
                                coachCount  = r.vehicles.size
                            )
                        })
                }
            }
            // ── POST /api/admin/snapshot-units?date=2026-03-27 ────────────────────
            post("/admin/snapshot-units") {
                val date = call.parameters["date"] ?: java.time.LocalDate.now().toString()
                withContext(Dispatchers.IO) { AppDatabase.snapshotUnitAllocations(date) }
                call.respond(mapOf("status" to "ok", "date" to date))
            }

        }
    }
}

// ─── Query helpers ────────────────────────────────────────────────────────────

/**
 * Returns all schedule rows for [headcode] across every station, deduplicated by UID
 * to one row per service (the origin LO stop), then enriched with TRUST data.
 *
 * The returned list uses the same [ServiceResponse] shape as queryBoard() so the
 * Android app can display a headcode search result identically to a normal board.
 */
private fun queryByHeadcode(headcode: String): List<ServiceResponse> {
    return transaction {
        // 1. Grab every schedule row for this headcode (all STP indicators, all stops)
        val allRows = Schedules.select { Schedules.headcode eq headcode }
            .orderBy(Schedules.scheduledTime, SortOrder.ASC)
            .toList()

        if (allRows.isEmpty()) return@transaction emptyList()

        // 2. For each UID, pick the highest-priority STP row (N > O > P, drop C)
        val priority = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)
        val byUid = allRows.groupBy { it[Schedules.uid] }
        val bestRows = byUid.values.mapNotNull { rows ->
            val bestStp = rows.minOfOrNull { priority[it[Schedules.stpIndicator]] ?: 99 } ?: return@mapNotNull null
            if (priority.entries.find { it.value == bestStp }?.key == 'C') return@mapNotNull null
            // Use the LO (origin) stop row to represent the service
            rows.filter { (priority[it[Schedules.stpIndicator]] ?: 99) == bestStp }
                .minByOrNull { if (it[Schedules.stopType] == "LO") 0 else 1 }
        }

        // 3. Collect TRUST data for this headcode from the last 3 hours
        //    Key: scheduledTime (the LO departure time is unique per service)
        val trustMap = mutableMapOf<String, Triple<String, Boolean, String?>>()
        try {
            exec(
                "SELECT scheduled_time, actual_time, is_cancelled, cancel_reason " +
                "FROM trust_movements " +
                "WHERE headcode='$headcode' AND event_ts >= datetime('now', '-3 hours') " +
                "ORDER BY event_ts DESC"
            ) { rs ->
                while (rs.next()) {
                    val scht = rs.getString("scheduled_time") ?: continue
                    if (!trustMap.containsKey(scht)) {
                        trustMap[scht] = Triple(
                            rs.getString("actual_time") ?: "",
                            rs.getBoolean("is_cancelled"),
                            rs.getString("cancel_reason")
                        )
                    }
                }
            }
        } catch (_: Exception) {}

        // 4. Build ServiceResponse for each service, sorted by scheduled time
        bestRows.sortedBy { it[Schedules.scheduledTime] }.map { row ->
            val scht       = row[Schedules.scheduledTime]
            val trustEntry = trustMap[scht]
            ServiceResponse(
                uid           = row[Schedules.uid],
                headcode      = row[Schedules.headcode],
                atocCode      = row[Schedules.atocCode],
                scheduledTime = scht,
                platform      = row[Schedules.platform],
                isPass        = row[Schedules.isPass],
                stopType      = row[Schedules.stopType],
                originTiploc  = row[Schedules.originTiploc],
                destTiploc    = row[Schedules.destTiploc],
                originCrs     = row[Schedules.originCrs],
                destCrs       = row[Schedules.destCrs],
                actualTime    = trustEntry?.first ?: "",
                isCancelled   = trustEntry?.second ?: false,
                cancelReason  = trustEntry?.third
            )
        }
    }
}

private fun timeWindow(windowMinutes: Int, offsetMinutes: Int = 0): Pair<String, String> {
    val fmt = DateTimeFormatter.ofPattern("HH:mm")
    val start = LocalTime.now().plusMinutes(offsetMinutes.toLong())
    val end = start.plusMinutes(windowMinutes.toLong())
    val startStr = start.format(fmt)
    val endStr = end.format(fmt)
    return if (endStr < startStr) startStr to "23:59" else startStr to endStr
}

private fun queryBoard(
    crs: String,
    windowStart: String,
    windowEnd: String,
    arrivalsOnly: Boolean,
    passengerOnly: Boolean = false,
    excludePassing: Boolean = false
): List<ServiceResponse> {
    val crsGroup = listOf(crs)
    return transaction {
        val needed = listOf(
            Schedules.uid, Schedules.headcode, Schedules.atocCode,
            Schedules.scheduledTime, Schedules.platform, Schedules.isPass,
            Schedules.stopType, Schedules.stpIndicator, Schedules.crs,
            Schedules.originTiploc, Schedules.destTiploc,
            Schedules.originCrs, Schedules.destCrs
        )
        Schedules.slice(needed).select {
            (Schedules.crs inList crsGroup) and
            (Schedules.scheduledTime greaterEq windowStart) and
            (Schedules.scheduledTime lessEq windowEnd) and
            if (arrivalsOnly)
                (Schedules.stopType neq "LO")
            else
                (Schedules.stopType neq "LT")
        }
        .orderBy(Schedules.scheduledTime, SortOrder.ASC)
        .toList()
        .filter { row ->
            (!excludePassing || !row[Schedules.isPass]) &&
            (!passengerOnly || row[Schedules.headcode].firstOrNull()?.let { it in "1239" } == true)
        }
        .groupBy { it[Schedules.uid] }
        .values
        .mapNotNull { rows ->
            val priority = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)
            val best = rows.minByOrNull { priority[it[Schedules.stpIndicator]] ?: 99 }!!
            if (best[Schedules.stpIndicator] == 'P') {
                val hasOverride = Schedules.select { Schedules.uid eq best[Schedules.uid] }
                    .any { it[Schedules.stpIndicator] == 'O' || it[Schedules.stpIndicator] == 'N' }
                if (hasOverride) return@mapNotNull null
            }
            best
        }
        .filter { it[Schedules.stpIndicator] != 'C' }
        .sortedBy { it[Schedules.scheduledTime] }
        .let { sortedRows ->
            val safeCrsQ     = crs.filter { it.isLetterOrDigit() }
            val safeWinStart = windowStart.filter { it.isDigit() || it == ':' }
            val safeWinEnd   = windowEnd.filter   { it.isDigit() || it == ':' }
            val trustMap = mutableMapOf<String, Triple<String, Boolean, String?>>()
            try {
                exec("SELECT headcode, scheduled_time, actual_time, is_cancelled, cancel_reason FROM trust_movements WHERE crs='$safeCrsQ' AND scheduled_time >= time('$safeWinStart', '-5 minutes') AND scheduled_time <= time('$safeWinEnd', '+5 minutes') AND event_ts >= datetime('now', '-3 hours') ORDER BY event_ts DESC") { rs ->
                    while (rs.next()) {
                        val hc   = rs.getString("headcode")       ?: return@exec
                        val scht = rs.getString("scheduled_time") ?: return@exec
                        val key  = "$hc|$scht"
                        if (!trustMap.containsKey(key)) {
                            trustMap[key] = Triple(rs.getString("actual_time") ?: "", rs.getBoolean("is_cancelled"), rs.getString("cancel_reason"))
                        }
                    }
                }
            } catch (_: Exception) {}
            sortedRows.map { row ->
                val hc                 = row[Schedules.headcode]
                val scht               = row[Schedules.scheduledTime]
                val trustEntry         = trustMap["$hc|$scht"]
                val actualTime         = trustEntry?.first ?: ""
                val isCancelledByTrust = trustEntry?.second ?: false
                val cancelReasonCode   = trustEntry?.third
                ServiceResponse(
                    uid           = row[Schedules.uid],
                    headcode      = row[Schedules.headcode],
                    atocCode      = row[Schedules.atocCode],
                    scheduledTime = row[Schedules.scheduledTime],
                    platform      = row[Schedules.platform],
                    isPass        = row[Schedules.isPass],
                    stopType      = row[Schedules.stopType],
                    originTiploc  = row[Schedules.originTiploc],
                    destTiploc    = row[Schedules.destTiploc],
                    originCrs     = row[Schedules.originCrs],
                    destCrs       = row[Schedules.destCrs],
                    actualTime    = actualTime,
                    isCancelled   = isCancelledByTrust,
                    cancelReason  = cancelReasonCode
                )
            }
        }
    }
}

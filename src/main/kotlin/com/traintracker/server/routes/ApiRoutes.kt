package com.traintracker.server.routes

import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.crsfromTiplocFallback
import com.traintracker.server.database.AssociationRecord
import com.traintracker.server.kb.KbClient

import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.database.Schedules
import com.traintracker.server.database.TrainLocations
import com.traintracker.server.kafka.trainLocations
import com.traintracker.server.hsp.HspClient
import com.traintracker.server.hsp.HspMetricsRequest
import io.ktor.http.*
import io.ktor.server.request.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import com.traintracker.server.auth.authRoutes
import com.traintracker.server.auth.checkAuth
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.selectAll
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
    val cancelReason: String? = null,
    val units: List<String> = emptyList(),
    val vehicles: List<String> = emptyList(),
    val unitJoinTiploc: String? = null,
    val couplingTiploc: String? = null,
    val couplingTiplocName: String? = null,
    val couplingAssocType: String? = null,
    val coupledFromUid: String? = null,
    val coupledFromHeadcode: String? = null,
    val formsUid: String? = null,
    val formsHeadcode: String? = null,
    val splitTiploc: String? = null,
    val splitTiplocName: String? = null,
    val splitToUid: String? = null,
    val splitToHeadcode: String? = null,
    val splitToDestName: String? = null,
    val destName: String? = null,
    val originName: String? = null
)

@Serializable
data class CallingPointResponse(
    val tiploc: String,
    val crs: String?,
    val name: String?,
    val scheduledTime: String,
    val platform: String?,
    val isPass: Boolean,
    val stopType: String,
    val actualTime: String = "",
    val isCancelled: Boolean = false,
    val cancelReason: String? = null,
    val isPredicted: Boolean = false,
    val length: Int? = null
)

@Serializable
data class ServiceDetailResponse(
    val uid: String,
    val atCrs: String,
    val previous: List<CallingPointResponse>,
    val subsequent: List<CallingPointResponse>,
    val serviceType: String = "NORMAL"
)

@Serializable
data class TrainLocationResponse(
    val headcode: String,
    val rid: String,
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
    val trainLocationsCount: Int,
    val scheduleCount: Int = 0,
    val uniqueServiceCount: Int = 0,
    val trustMovementCount: Int = 0,
    val allocationsToday: Int = 0,
    val hspUnitCount: Int = 0,
    val tiplocNameCount: Int = 0
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
        // ── GET /dashboard ───────────────────────────────────────────────
        get("/dashboard") {
            val file = java.io.File("/opt/traintracker/dashboard.html")
            if (file.exists()) {
                call.respondText(file.readText(), io.ktor.http.ContentType.Text.Html)
            } else {
                call.respond(io.ktor.http.HttpStatusCode.NotFound, "Dashboard not found")
            }
        }
        route("/api") {
            authRoutes()

            // ── GET /api/status ────────────────────────────────────────────
            get("/status") {
                call.respond(
                    AppDatabase.getEnrichedStatus(trainLocations.isNotEmpty(), trainLocations.size)
                )
            }

            // ── GET /api/departures?crs=MAN&window=120 ────────────────────
            get("/departures") {
                val crs    = call.parameters["crs"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "crs required")
                val window = call.parameters["window"]?.toIntOrNull() ?: 120
                val offset = call.parameters["offset"]?.toIntOrNull() ?: 0
                val (start, end) = timeWindow(window, offset)
                val services = annotateSplits(enrichWithUnits(queryBoard(crs, start, end, arrivalsOnly = false, passengerOnly = true, excludePassing = true)))
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
                val services = annotateSplits(enrichWithUnits(queryBoard(crs, start, end, arrivalsOnly = true, passengerOnly = true, excludePassing = true)))
                val board = BoardResponse(crs, "ARRIVALS", start, end, services)
                call.respond(board)
            }

            // ── GET /api/service/{uid}?crs=MAN ────────────────────────────
            get("/service/{uid}") {
                val uid   = call.parameters["uid"]?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "uid required")
                val atCrs = call.parameters["crs"]?.uppercase()?.trim() ?: ""

                val trustMapForService = mutableMapOf<String, Triple<String, Boolean, String?>>()
                val callingPoints = transaction {
                    Schedules.selectAll().where { Schedules.uid eq uid }
                        .orderBy(Schedules.scheduledTime, SortOrder.ASC)
                        .toList()
                                        .let { rows ->
                            val priority = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)
                            val bestStp = rows.minOfOrNull { priority[it[Schedules.stpIndicator]] ?: 99 }
                            var bestRows = rows.filter { (priority[it[Schedules.stpIndicator]] ?: 99) == bestStp }
                            // If atCrs is provided and the winning LO doesn't match it,
                            // but a lower-priority schedule's LO does, use that instead.
                            // This handles joining services where O schedule = joining portion only.
                            if (atCrs.isNotEmpty()) {
                                val bestHasMatchingLo = bestRows.any { it[Schedules.stopType] == "LO" && it[Schedules.crs] == atCrs }
                                if (!bestHasMatchingLo) {
                                    val altStp = priority.entries
                                        .sortedBy { it.value }
                                        .firstOrNull { (k, _) ->
                                            k != priority.entries.find { it.value == bestStp }?.key &&
                                            rows.any { it[Schedules.stpIndicator] == k && it[Schedules.stopType] == "LO" && it[Schedules.crs] == atCrs }
                                        }
                                    if (altStp != null) {
                                        bestRows = rows.filter { it[Schedules.stpIndicator] == altStp.key }
                                    }
                                }
                            }
                            // Inherit missing stops from P schedule where overlay is incomplete
                            val bestSttpChar = priority.entries.find { it.value == (priority[bestRows.firstOrNull()?.get(Schedules.stpIndicator)] ?: 99) }?.key
                            if (bestSttpChar != null && bestSttpChar != 'P') {
                                val hasOwnLo = bestRows.any { it[Schedules.stopType] == "LO" }
                                val hasOwnLt = bestRows.any { it[Schedules.stopType] == "LT" }
                                val hasOwnLi = bestRows.any { it[Schedules.stopType] == "LI" }
                                when {
                                    !hasOwnLo && !hasOwnLi && hasOwnLt -> {
                                        // Destination-only override: O provides only LT (early termination/diversion)
                                        // Use all P stops up to and including the new LT tiploc
                                        val oLt = bestRows.first { it[Schedules.stopType] == "LT" }
                                        val oLtTiploc = oLt[Schedules.tiploc]
                                        val pRows = rows.filter { it[Schedules.stpIndicator] == 'P' }
                                            .sortedBy { it[Schedules.scheduledTime] }
                                        val cutIdx = pRows.indexOfFirst { it[Schedules.tiploc] == oLtTiploc }
                                        if (cutIdx >= 0) {
                                            // Found the new terminus in P — use P stops up to that point, then O's LT
                                            pRows.subList(0, cutIdx) + oLt
                                        } else {
                                            // New terminus not in P schedule — use all P stops then O's LT
                                            pRows.filter { it[Schedules.stopType] != "LT" } + oLt
                                        }
                                    }
                                    !hasOwnLo && !hasOwnLt -> {
                                        // Partial stop override: O rows replace specific tiplocs within a P service
                                        val overriddenTiplocs = bestRows.map { it[Schedules.tiploc] }.toSet()
                                        val pRows = rows.filter {
                                            it[Schedules.stpIndicator] == 'P' && it[Schedules.tiploc] !in overriddenTiplocs
                                        }
                                        (bestRows + pRows).sortedBy { it[Schedules.scheduledTime] }
                                    }
                                    hasOwnLo && !hasOwnLi -> {
                                        // New origin overlay: O provides LO (and optionally LT), inherit P LI rows
                                        // after the new origin by tiploc position to avoid time-ordering issues
                                        // where O origin time > P intermediate times (e.g. EAL 07:00 > STL P 06:59)
                                        val oLoTime = bestRows.first { it[Schedules.stopType] == "LO" }[Schedules.scheduledTime]
                                        val oLtTime = bestRows.firstOrNull { it[Schedules.stopType] == "LT" }?.get(Schedules.scheduledTime)
                                        val oOriginTiploc = bestRows.first { it[Schedules.stopType] == "LO" }[Schedules.tiploc]
                                        val pSorted = rows.filter { it[Schedules.stpIndicator] == 'P' }
                                            .sortedBy { it[Schedules.scheduledTime] }
                                        val oOriginIdxInP = pSorted.indexOfFirst { it[Schedules.tiploc] == oOriginTiploc }
                                        val pLi = pSorted.filter { row ->
                                            row[Schedules.stopType] == "LI" &&
                                            row[Schedules.tiploc] != oOriginTiploc &&
                                            (if (oOriginIdxInP >= 0)
                                                pSorted.indexOfFirst { it[Schedules.tiploc] == row[Schedules.tiploc] } > oOriginIdxInP
                                            else
                                                row[Schedules.scheduledTime] > oLoTime) &&
                                            (oLtTime == null || row[Schedules.scheduledTime] < oLtTime)
                                        }
                                        val pLt = if (!hasOwnLt)
                                            pSorted.filter { it[Schedules.stopType] == "LT" }
                                        else emptyList()
                                        (bestRows + pLi + pLt).sortedBy { row ->
                                            if (row[Schedules.tiploc] == oOriginTiploc) oOriginIdxInP
                                            else pSorted.indexOfFirst { it[Schedules.tiploc] == row[Schedules.tiploc] }.takeIf { it >= 0 } ?: Int.MAX_VALUE
                                        }
                                    }
                                    else -> {
                                        // Full overlay with own LO, LI and/or LT — use as-is
                                        bestRows.sortedBy { it[Schedules.scheduledTime] }
                                    }
                                }
                            } else bestRows
                        }
                        .let { rows ->
                            // Find destination from LT row regardless of STP —
                            // merged lists intentionally contain mixed STP indicators
                            val destTiploc = rows.firstOrNull { it[Schedules.stopType] == "LT" }?.get(Schedules.tiploc)
                            if (destTiploc != null) {
                                // Midnight-wrap: if LO time > LT time, sort post-midnight stops as 24:xx+
                                val loT = rows.firstOrNull { it[Schedules.stopType] == "LO" }?.get(Schedules.scheduledTime) ?: "00:00"
                                val ltT = rows.firstOrNull { it[Schedules.stopType] == "LT" }?.get(Schedules.scheduledTime) ?: "23:59"
                                val sorted = if (loT > ltT) {
                                    val loMins = loT.replace(":", "").toIntOrNull() ?: 0
                                    rows.sortedBy { r ->
                                        val mins = r[Schedules.scheduledTime].replace(":", "").toIntOrNull() ?: 0
                                        if (mins < loMins) mins + 2400 else mins
                                    }
                                } else rows
                                val ltIndex = sorted.indexOfFirst {
                                    it[Schedules.tiploc] == destTiploc && it[Schedules.stopType] == "LT"
                                }.let { if (it < 0) sorted.indexOfFirst { r -> r[Schedules.tiploc] == destTiploc } else it }
                                val deduped = sorted.filterIndexed { i, r ->
                                    r[Schedules.tiploc] != destTiploc || r[Schedules.stopType] == "LT" || r[Schedules.stopType] == "LO" || i == ltIndex
                                }
                                val finalIdx = deduped.indexOfFirst { it[Schedules.tiploc] == destTiploc && it[Schedules.stopType] == "LT" }
                                    .let { if (it < 0) deduped.indexOfFirst { r -> r[Schedules.tiploc] == destTiploc } else it }
                                if (finalIdx >= 0) deduped.subList(0, finalIdx + 1) else deduped
                            } else rows.sortedBy { it[Schedules.scheduledTime] }
                        }
                        .also { rows ->
                            // Build TRUST map for this service keyed by crs|scheduledTime
                            val headcode = rows.firstOrNull()?.get(Schedules.headcode) ?: ""
                            val safeHc = headcode.filter { it.isLetterOrDigit() }
                            if (safeHc.isNotEmpty()) {
                                exec("SELECT crs, stanox, scheduled_time, actual_time, is_cancelled, cancel_reason FROM trust_movements WHERE uid='$uid' AND event_ts >= datetime('now', '-12 hours') ORDER BY event_ts DESC") { rs ->
                                    while (rs.next()) {
                                        val scht = rs.getString("scheduled_time") ?: continue
                                        val isCancelled = rs.getBoolean("is_cancelled")
                                        val cancelReason = rs.getString("cancel_reason")
                                        // Times stored in UTC
                                        val actualT = rs.getString("actual_time") ?: ""
                                        val utcScht = scht
                                        // Keyed by scheduledTime only — UID already uniquely identifies the service,
                                        // no risk of cross-train contamination. Also store CRS-keyed fallback.
                                        val crs = rs.getString("crs")?.takeIf { it.isNotEmpty() }
                                            ?: CorpusLookup.crsFromStanox(rs.getString("stanox") ?: "")
                                        for (key in listOfNotNull(utcScht, scht.takeIf { it != utcScht },
                                                crs?.let { "$it|$utcScht" }, crs?.let { "$it|$scht" })) {
                                            if (!trustMapForService.containsKey(key)) {
                                                trustMapForService[key] = Triple(actualT, isCancelled, cancelReason)
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        .map { row ->
                            val tiploc = row[Schedules.tiploc]
                            val crs = row[Schedules.crs]?.takeIf { it.isNotEmpty() }
                                ?: CorpusLookup.crsFromTiploc(tiploc)
                            val scht = row[Schedules.scheduledTime]
                            val trustEntry = trustMapForService[scht]
                                ?: if (crs != null) trustMapForService["$crs|$scht"] else null
                            CallingPointResponse(
                                tiploc        = tiploc,
                                crs           = crs,
                                name          = CorpusLookup.nameFromTiploc(tiploc),
                                scheduledTime = row[Schedules.scheduledTime],
                                platform      = row[Schedules.platform],
                                isPass        = row[Schedules.isPass],
                                stopType      = row[Schedules.stopType],
                                actualTime    = trustEntry?.first ?: "",
                                isCancelled   = trustEntry?.second ?: false,
                                cancelReason  = trustEntry?.third
                            )
                        }
                        .let { rawCps ->
                            // If the origin (LO) is cancelled and no stop has any actual time,
                            // the service never ran — propagate the LO cancellation to all stops.
                            val loCancel = rawCps.firstOrNull { it.stopType == "LO" }
                                ?.takeIf { it.isCancelled }
                            val serviceNeverRan = loCancel != null && rawCps.none { it.actualTime.isNotEmpty() } && rawCps.none { !it.isCancelled && it.stopType != "LO" }
                            val propagatedCps = if (serviceNeverRan) {
                                rawCps.map { cp ->
                                    if (!cp.isCancelled)
                                        cp.copy(isCancelled = true, cancelReason = loCancel!!.cancelReason)
                                    else cp
                                }
                            } else rawCps
                            // Propagate short-working cancellation: if a non-terminal stop is cancelled
                            // and TRUST hasn't sent individual cancellations for subsequent stops,
                            // mark all following stops as cancelled too.
                            // Don't propagate if subsequent stops already have explicit TRUST cancellations
                            // (that indicates a COO where cancelled stops are at the start, not the end).
                            val firstCancelIdx = propagatedCps.indexOfFirst { it.isCancelled && it.stopType != "LT" }
                            val hasTrustCancelAfterFirst = firstCancelIdx >= 0 &&
                                (propagatedCps.drop(firstCancelIdx + 1).any { it.isCancelled } ||
                                 propagatedCps.drop(firstCancelIdx + 1).any { it.actualTime.isNotEmpty() || trustMapForService.containsKey(it.scheduledTime) })
                            // Backward propagation: if a stop is cancelled and TRUST movements exist
                            // at later stops (COO pattern), mark all stops before the cancellation as cancelled too.
                            val backPropCps = if (hasTrustCancelAfterFirst && firstCancelIdx > 0) {
                                val cancelReason = propagatedCps[firstCancelIdx].cancelReason
                                propagatedCps.mapIndexed { idx, cp ->
                                    if (idx < firstCancelIdx && !cp.isCancelled)
                                        cp.copy(isCancelled = true, cancelReason = cancelReason)
                                    else cp
                                }
                            } else propagatedCps
                            data class SwState(val cps: List<CallingPointResponse>, val cancelled: Boolean, val reason: String?)
                            val propagatedCps2 = if (hasTrustCancelAfterFirst) backPropCps else
                                propagatedCps.fold(SwState(emptyList(), false, null)) { state, cp ->
                                    when {
                                        cp.isCancelled && cp.stopType != "LT" ->
                                            state.copy(cps = state.cps + cp, cancelled = true, reason = cp.cancelReason)
                                        state.cancelled && !cp.isCancelled && cp.actualTime.isEmpty() ->
                                            state.copy(cps = state.cps + cp.copy(isCancelled = true, cancelReason = state.reason))
                                        else -> state.copy(cps = state.cps + cp)
                                    }
                                }.cps
                            // Propagate last known delay to future stops without actual times.
                            // Only update lastDelayMins from TRUST-confirmed passenger stops (not passing points).
                            // Only propagate positive delays (don't project early running).
                            val trustConfirmedTimes = trustMapForService.keys.toSet()
                            var lastDelayMins = 0
                            var lastReasonCode: String? = null
                            propagatedCps2.map { cp ->
                                val isTrustConfirmed = cp.actualTime.isNotEmpty() &&
                                    trustConfirmedTimes.contains(cp.scheduledTime)
                                if (isTrustConfirmed) {
                                    // Update delay only from TRUST-confirmed stops
                                    val sch = cp.scheduledTime.split(":").let { p -> (p.getOrNull(0)?.toIntOrNull() ?: 0) * 60 + (p.getOrNull(1)?.toIntOrNull() ?: 0) }
                                    val act = cp.actualTime.split(":").let { p -> (p.getOrNull(0)?.toIntOrNull() ?: 0) * 60 + (p.getOrNull(1)?.toIntOrNull() ?: 0) }
                                    var diff = act - sch
                                    if (diff < -120) diff += 1440
                                    if (diff >= 0) {
                                        lastDelayMins = diff  // Only update on late/on-time, not early
                                        if (cp.cancelReason != null) lastReasonCode = cp.cancelReason
                                    }
                                    // If actual == scheduled on a pass/junction (no CRS) and we know there's
                                    // a delay from a prior stop, TRUST likely hasn't reported this point yet
                                    // — project the delay forward. For proper passenger stops (has CRS),
                                    // trust the TRUST actual as genuine recovery.
                                    if (diff == 0 && lastDelayMins > 0 && cp.crs == null) {
                                        val schMins = sch
                                        val projMins = (schMins + lastDelayMins + 1440) % 1440
                                        cp.copy(actualTime = "%02d:%02d".format(projMins / 60, projMins % 60),
                                                cancelReason = lastReasonCode,
                                                isPredicted = true)
                                    } else cp
                                } else if (cp.actualTime.isEmpty() && lastDelayMins > 0 && !cp.isCancelled) {
                                    // Project positive delay onto stops with no TRUST actual
                                    val schMins = cp.scheduledTime.split(":").let { p -> (p.getOrNull(0)?.toIntOrNull() ?: 0) * 60 + (p.getOrNull(1)?.toIntOrNull() ?: 0) }
                                    val projMins = (schMins + lastDelayMins + 1440) % 1440
                                    cp.copy(actualTime = "%02d:%02d".format(projMins / 60, projMins % 60),
                                            cancelReason = lastReasonCode,
                                            isPredicted = true)
                                } else cp
                            }
                        }
                        .let { cps ->
                            // Fix midnight-wrap: if LO time > LT time, service crosses midnight.
                            // Re-sort by treating post-midnight times as 24:xx+
                            val loTime = cps.firstOrNull { it.stopType == "LO" }?.scheduledTime ?: "00:00"
                            val ltTime = cps.firstOrNull { it.stopType == "LT" }?.scheduledTime ?: "23:59"
                            if (loTime > ltTime) {
                                // Post-midnight stops (earlier than LO time) get +2400 so they sort after pre-midnight stops
                                cps.sortedBy { cp ->
                                    val mins = cp.scheduledTime.replace(":", "").toIntOrNull() ?: 0
                                    val loMins = loTime.replace(":", "").toIntOrNull() ?: 0
                                    if (mins < loMins) mins + 2400 else mins
                                }
                            } else cps
                        }
                }

                if (callingPoints.isEmpty()) {
                    call.respond(HttpStatusCode.NotFound, "Service $uid not found")
                    return@get
                }

                // Annotate coach counts from unit allocation + CIF associations
                val annotatedCps = run {
                    val assocs = AppDatabase.getAssociationsForUids(setOf(uid))
                    val serviceAssocs = assocs[uid]?.filter { it.assocType == "VV" }
                    if (!serviceAssocs.isNullOrEmpty()) {
                        val today = java.time.LocalDate.now().toString()
                        val mainAlloc = AppDatabase.getAllocationByUid(uid, today)
                        val mainCoaches = mainAlloc?.vehicles?.size ?: 0
                        if (mainCoaches > 0) {
                            val splitCounts = mutableMapOf<String, Int>()
                            for (assoc in serviceAssocs) {
                                val splitAlloc = AppDatabase.getAllocationByUid(assoc.assocUid, today)
                                val splitCoaches = splitAlloc?.vehicles?.size ?: 0
                                if (splitCoaches > 0) splitCounts[assoc.assocTiploc] = mainCoaches - splitCoaches
                            }
                            if (splitCounts.isNotEmpty()) {
                                var current = mainCoaches
                                callingPoints.map { cp ->
                                    val after = splitCounts[cp.tiploc]
                                    if (after != null) current = after
                                    cp.copy(length = current)
                                }
                            } else callingPoints.map { it.copy(length = mainCoaches) }
                        } else callingPoints
                    } else callingPoints
                }

                val atIndex    = annotatedCps.indexOfFirst { it.crs == atCrs }
                    .let { if (it < 0 && atCrs.isNotEmpty()) annotatedCps.indexOfFirst { cp -> cp.tiploc.startsWith(atCrs) } else if (it < 0) 0 else it }
                val previous   = if (atCrs.isNotEmpty() && atIndex > 0) annotatedCps.subList(0, atIndex) else emptyList()
                // atIndex stop is included as the first entry in subsequent so the app can show its actual time
                val subsequent = if (atCrs.isEmpty()) annotatedCps else if (atIndex >= 0) annotatedCps.subList(atIndex, annotatedCps.size) else emptyList()

                val headcode = annotatedCps.firstOrNull()?.tiploc?.let {
                    transaction { Schedules.selectAll().where { Schedules.uid eq uid }.firstOrNull()?.get(Schedules.headcode) ?: "" }
                } ?: ""
                val serviceType = when {
                    headcode.startsWith("0B") -> "BUS_REPLACEMENT"
                    headcode.startsWith("0C") -> "COACH_REPLACEMENT"
                    else -> "NORMAL"
                }
                call.respond(ServiceDetailResponse(uid, atCrs, previous, subsequent, serviceType))
            }

            // ── GET /api/allservices?crs=MAN&window=120 ───────────────────
            get("/allservices") {
                val crs    = call.parameters["crs"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "crs required")
                val window = call.parameters["window"]?.toIntOrNull() ?: 120
                val offset = call.parameters["offset"]?.toIntOrNull() ?: 0
                val (start, end) = timeWindow(window, offset)
                val services = enrichWithUnits(queryBoard(crs, start, end, arrivalsOnly = false, passengerOnly = false, excludePassing = false))
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
                        rid          = loc.rid,
                        stationName  = loc.stationName,
                        crs          = loc.crs,
                        actualTime   = loc.actualTime,
                        eventType    = loc.eventType,
                        delayMinutes = loc.delayMinutes,
                        ageSeconds   = (System.currentTimeMillis() - loc.updatedEpochMs) / 1000
                    ))
                } else {
                    val dbLoc = transaction {
                        TrainLocations.selectAll().where { TrainLocations.headcode eq headcode }.singleOrNull()
                    }
                    if (dbLoc != null) {
                        call.respond(TrainLocationResponse(
                            headcode     = dbLoc[TrainLocations.headcode],
                            rid          = dbLoc[TrainLocations.rid],
                            stationName  = dbLoc[TrainLocations.stationName],
                            crs          = dbLoc[TrainLocations.crs],
                            actualTime   = dbLoc[TrainLocations.actualTime],
                            eventType    = dbLoc[TrainLocations.eventType],
                            delayMinutes = dbLoc[TrainLocations.delayMinutes],
                            ageSeconds   = -1L
                        ))
                    } else {
                        call.respond(HttpStatusCode.NotFound, "No location data for $headcode")
                    }
                }
            }

            // ── GET /api/trust/locate/{rid} ───────────────────────────────
            get("/trust/locate/{rid}") {
                val rid = call.parameters["rid"]?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "rid required")
                // Search in-memory first
                val loc = trainLocations.values.firstOrNull { it.rid == rid }
                if (loc != null) {
                    call.respond(TrainLocationResponse(
                        headcode     = loc.headcode,
                        rid          = loc.rid,
                        stationName  = loc.stationName,
                        crs          = loc.crs,
                        actualTime   = loc.actualTime,
                        eventType    = loc.eventType,
                        delayMinutes = loc.delayMinutes,
                        ageSeconds   = (System.currentTimeMillis() - loc.updatedEpochMs) / 1000
                    ))
                } else {
                    val dbLoc = transaction {
                        TrainLocations.selectAll().where { TrainLocations.rid eq rid }.singleOrNull()
                    }
                    if (dbLoc != null) {
                        call.respond(TrainLocationResponse(
                            headcode     = dbLoc[TrainLocations.headcode],
                            rid          = dbLoc[TrainLocations.rid],
                            stationName  = dbLoc[TrainLocations.stationName],
                            crs          = dbLoc[TrainLocations.crs],
                            actualTime   = dbLoc[TrainLocations.actualTime],
                            eventType    = dbLoc[TrainLocations.eventType],
                            delayMinutes = dbLoc[TrainLocations.delayMinutes],
                            ageSeconds   = -1L
                        ))
                    } else {
                        call.respond(HttpStatusCode.NotFound, "No location data for rid=$rid")
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
                    // Times stored in UTC — no conversion needed
                    fun bstToUtc(t: String): String = t
                    exec("SELECT crs, stanox, event_type, scheduled_time, actual_time, platform, is_cancelled FROM trust_movements WHERE headcode='$safeHc' AND event_ts >= datetime('now', '-3 hours') ORDER BY event_ts DESC LIMIT 50") { rs ->
                        while (rs.next()) {
                            result.add(MovementResponse(
                                crs           = rs.getString("crs") ?: rs.getString("stanox") ?: "",
                                eventType     = rs.getString("event_type") ?: "",
                                scheduledTime = bstToUtc(rs.getString("scheduled_time") ?: ""),
                                actualTime    = bstToUtc(rs.getString("actual_time") ?: ""),
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

                val services = annotateSplits(enrichWithUnits(queryByHeadcode(safeHc)))
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


            // ── GET /api/unit/{unit} ──────────────────────────────────────
            get("/unit/{unit}") {
                val unit = call.parameters["unit"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "unit required")
                val safeUnit = unit.filter { it.isLetterOrDigit() }

                val services = queryByUnit(safeUnit)
                if (services.isEmpty()) {
                    call.respond(HttpStatusCode.NotFound, "No services found for unit $unit")
                } else {
                    call.respond(BoardResponse(
                        crs         = unit,
                        boardType   = "UNIT",
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
                val scheduledDep  = json["scheduled_dep"]?.jsonPrimitive?.content?.trim() ?: ""
                val originCrsRaw  = json["origin_crs"]?.jsonPrimitive?.content?.trim() ?: ""
                val originTiploc  = json["origin_tiploc"]?.jsonPrimitive?.content?.trim() ?: ""
                val scheduledArr  = json["scheduled_arr"]?.jsonPrimitive?.content?.trim() ?: ""
                val destTiploc    = json["dest_tiploc"]?.jsonPrimitive?.content?.trim() ?: ""
                // originCrsRaw is the CRS sent by the app (e.g. "COR" for Corby) — use directly.
                // If empty, fall back to resolving originTiploc via CorpusLookup.
                val originCrs     = originCrsRaw.ifEmpty {
                    CorpusLookup.crsFromTiploc(originTiploc) ?: ""
                }
                val result = withContext(Dispatchers.IO) { HspClient.getDetails(rid) }
                if (result != null) {
                val unitInfo = AppDatabase.getHspUnit(rid, scheduledDep, originCrs, scheduledArr, destTiploc)
                    val enriched = buildJsonObject {
                        put("rid",      result.rid)
                        put("date",     result.date)
                        put("tocCode",  result.tocCode)
                        put("unit",     unitInfo?.units?.firstOrNull() ?: "")
                        put("units",    Json.encodeToJsonElement(unitInfo?.units ?: emptyList<String>()))
                        put("vehicles", Json.encodeToJsonElement(unitInfo?.vehicles ?: emptyList<String>()))
                        put("unitCount", unitInfo?.unitCount ?: 0)
                        put("locations", buildJsonArray {
                            for (loc in result.locations) {
                                add(buildJsonObject {
                                    // HSP API returns CRS codes in the location field, not TIPLOCs
                                    val crs  = loc.tiploc.uppercase().trim()
                                    val name = ""
                                    put("tiploc",        crs)
                                    put("crs",           crs)
                                    put("name",          name)
                                    put("scheduledDep",  loc.scheduledDep)
                                    put("scheduledArr",  loc.scheduledArr)
                                    put("actualDep",     loc.actualDep)
                                    put("actualArr",     loc.actualArr)
                                    put("cancelReason",  loc.cancelReason)
                                })
                            }
                        })
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

            // ── GET /api/allocation/uid/{uid} ─────────────────────────────
            get("/allocation/uid/{uid}") {
                val uid = call.parameters["uid"]?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "uid required")
                val date = call.request.queryParameters["date"]
                val result = AppDatabase.getAllocationByUid(uid, date)
                if (result == null) {
                    call.respond(HttpStatusCode.NotFound, "No allocation data for uid $uid")
                } else {
                    call.respond(AllocationResponse(
                        coreId      = result.coreId,
                        headcode    = result.headcode,
                        serviceDate = result.serviceDate,
                        operator    = result.operator,
                        units       = result.units,
                        vehicles    = result.vehicles,
                        unitCount   = result.unitCount,
                        coachCount  = result.vehicles.size
                    ))
                }
            }
            // ── GET /api/admin/tiploc-lookup?tiploc=WDON ────────────────────────────
            get("/admin/tiploc-lookup") {
                if (!call.checkAuth()) return@get
                val tiploc = call.parameters["tiploc"]?.uppercase()?.trim() ?: ""
                val name = CorpusLookup.nameFromTiploc(tiploc)
                call.respond(mapOf("tiploc" to tiploc, "name" to (name ?: "")))
            }
            // ── POST /api/admin/tiploc-name?tiploc=WDON&name=Wimbledon ─────────────
            post("/admin/tiploc-name") {
                if (!call.checkAuth()) return@post
                val tiploc = call.parameters["tiploc"]?.uppercase()?.trim()
                val name   = call.parameters["name"]?.trim()
                if (tiploc.isNullOrEmpty() || name.isNullOrEmpty()) {
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "tiploc and name required"))
                    return@post
                }
                val oldName = CorpusLookup.nameFromTiploc(tiploc)
                val changedBy = com.traintracker.server.auth.AuthDatabase.validateSession(
                    call.request.header("Authorization")?.removePrefix("Bearer ")?.trim() ?: ""
                ) ?: "unknown"
                CorpusLookup.updateTiplocName(tiploc, name)
                com.traintracker.server.auth.AuthDatabase.logTiplocChange(tiploc, oldName, name, changedBy)
                call.respond(mapOf("status" to "ok", "tiploc" to tiploc, "name" to name))
            }
            // ── POST /api/admin/snapshot-units?date=2026-03-27 ────────────────────
            post("/admin/snapshot-units") {
                if (!call.checkAuth()) return@post
                val date = call.parameters["date"] ?: java.time.LocalDate.now().toString()
                withContext(Dispatchers.IO) { AppDatabase.snapshotUnitAllocations(date) }
                call.respond(mapOf("status" to "ok", "date" to date))
            }


            // ── GET /api/kb/incidents ──────────────────────────────────────
            get("/kb/incidents") {
                val incidents = withContext(Dispatchers.IO) { KbClient.getIncidents() }
                call.respond(buildJsonArray {
                    for (inc in incidents) add(buildJsonObject {
                        put("id",          inc.id)
                        put("summary",     inc.summary)
                        put("description", inc.description)
                        put("isPlanned",   inc.isPlanned)
                        put("startTime",   inc.startTime)
                        put("endTime",     inc.endTime)
                        put("operators",   kotlinx.serialization.json.Json.encodeToJsonElement(inc.operators))
                    })
                })
            }

            // ── GET /api/kb/nsi ────────────────────────────────────────────
            get("/kb/nsi") {
                val nsi = withContext(Dispatchers.IO) { KbClient.getNsi() }
                call.respond(buildJsonArray {
                    for (entry in nsi) add(buildJsonObject {
                        put("tocCode",           entry.tocCode)
                        put("tocName",           entry.tocName)
                        put("status",            entry.status)
                        put("statusDescription", entry.statusDescription)
                        put("twitterHandle",     entry.twitterHandle)
                        put("additionalInfo",    entry.additionalInfo)
                        put("disruptions", buildJsonArray {
                            for (d in entry.disruptions) add(buildJsonObject {
                                put("detail", d.detail)
                                put("url",    d.url)
                            })
                        })
                    })
                })
            }

            // ── GET /api/kb/toc ────────────────────────────────────────────
            get("/kb/toc") {
                val tocs = withContext(Dispatchers.IO) { KbClient.getToc() }
                call.respond(buildJsonArray {
                    for (t in tocs) add(buildJsonObject {
                        put("code",                 t.code)
                        put("name",                 t.name)
                        put("website",              t.website)
                        put("customerServicePhone", t.customerServicePhone)
                        put("assistedTravelPhone",  t.assistedTravelPhone)
                        put("assistedTravelUrl",    t.assistedTravelUrl)
                        put("lostPropertyUrl",      t.lostPropertyUrl)
                    })
                })
            }

            // ── GET /api/kb/station/{crs} ──────────────────────────────────
            get("/kb/station/{crs}") {
                val crs = call.parameters["crs"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "crs required")
                val station = withContext(Dispatchers.IO) { KbClient.getStation(crs) }
                if (station == null) {
                    call.respond(HttpStatusCode.NotFound, mapOf("error" to "Station $crs not found"))
                } else {
                    call.respond(buildJsonObject {
                        put("crs",               station.crs)
                        put("name",              station.name)
                        put("address",           station.address)
                        put("telephone",         station.telephone)
                        put("staffingNote",      station.staffingNote)
                        put("ticketOfficeHours", station.ticketOfficeHours)
                        put("sstmAvailability",  station.sstmAvailability)
                        put("stepFreeAccess",    station.stepFreeAccess)
                        put("assistanceAvail",   station.assistanceAvail)
                        put("wifi",              station.wifi)
                        put("toilets",           station.toilets)
                        put("waitingRoom",       station.waitingRoom)
                        put("cctv",              station.cctv)
                        put("taxi",              station.taxi)
                        put("carParking",        station.carParking)
                    })
                }
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
        val allRows = Schedules.selectAll().where { Schedules.headcode eq headcode }
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

        // 3. Collect TRUST data keyed by UID — eliminates cross-train headcode contamination
        val trustMap = mutableMapOf<String, Triple<String, Boolean, String?>>()
        val uidInClause = bestRows.map { "'${it[Schedules.uid]}'" }.toSet().joinToString(",")
        if (uidInClause.isNotEmpty()) {
            try {
                exec(
                    "SELECT uid, scheduled_time, actual_time, is_cancelled, cancel_reason " +
                    "FROM trust_movements " +
                    "WHERE uid IN ($uidInClause) AND event_ts >= datetime('now', '-12 hours') " +
                    "ORDER BY event_ts DESC"
                ) { rs ->
                    while (rs.next()) {
                        val uid  = rs.getString("uid")?.takeIf { it.isNotEmpty() } ?: continue
                        val scht = rs.getString("scheduled_time") ?: continue
                        val key  = "$uid|$scht"
                        if (!trustMap.containsKey(key)) {
                            trustMap[key] = Triple(
                                rs.getString("actual_time") ?: "",
                                rs.getBoolean("is_cancelled"),
                                rs.getString("cancel_reason")
                            )
                        }
                        // Also store by UID alone for cancellation rows
                        val isCancelled = rs.getBoolean("is_cancelled")
                        if (isCancelled && !trustMap.containsKey(uid)) {
                            trustMap[uid] = Triple(rs.getString("actual_time") ?: "", true, rs.getString("cancel_reason"))
                        }
                    }
                }
            } catch (_: Exception) {}
        }

        // 4. Build ServiceResponse for each service, sorted by scheduled time
        val uids = bestRows.map { it[Schedules.uid] }.toSet()
        val assocMap = AppDatabase.getAssociationsForUids(uids)
        val reverseAssocMap = AppDatabase.getReverseAssociationsForUids(uids)
        bestRows.sortedBy { it[Schedules.scheduledTime] }.map { row ->
            val scht       = row[Schedules.scheduledTime]
            val uid        = row[Schedules.uid]
            val trustEntry = trustMap["$uid|$scht"] ?: if (trustMap[uid]?.second == true) trustMap[uid] else null
            var svc = ServiceResponse(
                uid           = row[Schedules.uid],
                headcode      = row[Schedules.headcode],
                atocCode      = row[Schedules.atocCode],
                scheduledTime = scht,
                platform      = row[Schedules.platform],
                isPass        = row[Schedules.isPass],
                stopType      = row[Schedules.stopType],
                originTiploc  = row[Schedules.originTiploc],
                destTiploc    = row[Schedules.destTiploc],
                originCrs     = row[Schedules.originCrs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(row[Schedules.originTiploc]),
                destCrs       = row[Schedules.destCrs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(row[Schedules.destTiploc]),
                actualTime    = trustEntry?.first ?: "",
                isCancelled   = trustEntry?.second ?: false,
                cancelReason  = trustEntry?.third,
                destName      = CorpusLookup.nameFromTiploc(row[Schedules.destTiploc]),
                originName    = CorpusLookup.nameFromTiploc(row[Schedules.originTiploc])
            )
            val assocs = assocMap[uid]
            if (!assocs.isNullOrEmpty()) {
                for (assoc in assocs) {
                    var assocHeadcode = ""
                    try {
                        exec("SELECT headcode FROM schedules WHERE uid = '${assoc.assocUid}' AND stop_type = 'LO' AND stp_indicator != 'C' LIMIT 1") { rs ->
                            if (rs.next()) assocHeadcode = rs.getString("headcode") ?: ""
                        }
                    } catch (_: Exception) {}
                    val tiplocCrs = CorpusLookup.crsFromTiploc(assoc.assocTiploc) ?: crsfromTiplocFallback(assoc.assocTiploc) ?: assoc.assocTiploc
                    if (assoc.assocType in listOf("VV", "JJ")) {
                        val isPassengerSplit = assocHeadcode.firstOrNull()?.let { it in "1239" } == true
                        // Suppress if assocTiploc is the terminus of this service
                        var assocTiplocIsTerminus = false
                        try {
                            exec("SELECT 1 FROM schedules WHERE uid = '${uid}' AND tiploc = '${assoc.assocTiploc}' AND stop_type = 'LT' AND stp_indicator != 'C' LIMIT 1") { rs ->
                                if (rs.next()) assocTiplocIsTerminus = true
                            }
                        } catch (_: Exception) {}
                        // Suppress if split portion doesn't actually originate at assocTiploc
                        var assocEffectiveLo = ""
                        try {
                            val priority = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)
                            exec("SELECT tiploc, stp_indicator FROM schedules WHERE uid = '${assoc.assocUid}' AND stop_type = 'LO' AND stp_indicator != 'C'") { rs ->
                                var bestPriority = 99
                                while (rs.next()) {
                                    val stp = rs.getString("stp_indicator")?.firstOrNull() ?: continue
                                    val p = priority[stp] ?: 99
                                    if (p < bestPriority) {
                                        bestPriority = p
                                        assocEffectiveLo = rs.getString("tiploc") ?: ""
                                    }
                                }
                            }
                        } catch (_: Exception) {}
                        if (isPassengerSplit && !assocTiplocIsTerminus &&
                            (assocEffectiveLo == assoc.assocTiploc || assocEffectiveLo.isEmpty())) {
                            var splitDestName: String? = null
                            try {
                                exec("SELECT dest_tiploc FROM schedules WHERE uid = '${assoc.assocUid}' AND stop_type = 'LT' AND stp_indicator != 'C' LIMIT 1") { rs ->
                                    if (rs.next()) splitDestName = CorpusLookup.nameFromTiploc(rs.getString("dest_tiploc") ?: "")
                                }
                            } catch (_: Exception) {}
                            svc = svc.copy(
                                splitTiploc     = assoc.assocTiploc,
                                splitTiplocName = tiplocCrs,
                                splitToHeadcode = assocHeadcode,
                                splitToUid      = assoc.assocUid,
                                splitToDestName = splitDestName
                            )
                            break
                        }
                    }
                }
            }
            // Formed-from detection (reverse VV association)
            val reverseAssocs = reverseAssocMap[uid]
            if (reverseAssocs != null && reverseAssocs.isNotEmpty() && svc.couplingTiploc.isNullOrEmpty()) {
                for (assoc in reverseAssocs) {
                    if (assoc.assocType in listOf("VV", "JJ")) {
                        var parentHeadcode = ""
                        try {
                            exec("SELECT headcode FROM schedules WHERE uid = '${assoc.mainUid}' AND stop_type = 'LO' AND stp_indicator != 'C' LIMIT 1") { rs ->
                                if (rs.next()) parentHeadcode = rs.getString("headcode") ?: ""
                            }
                        } catch (_: Exception) {}
                        val tiplocCrs = CorpusLookup.crsFromTiploc(assoc.assocTiploc) ?: crsfromTiplocFallback(assoc.assocTiploc) ?: assoc.assocTiploc
                        if (parentHeadcode.isNotEmpty()) {
                            svc = svc.copy(
                                couplingTiploc      = assoc.assocTiploc,
                                couplingTiplocName  = tiplocCrs,
                                coupledFromUid      = assoc.mainUid,
                                coupledFromHeadcode = parentHeadcode,
                                couplingAssocType   = assoc.assocType
                            )
                            break
                        }
                    }
                }
            }
            svc
        }
    }
}


private fun queryByUnit(unit: String): List<ServiceResponse> {
    return transaction {
        val today = java.time.LocalDate.now().toString()
        val rows = mutableListOf<ServiceResponse>()
        try {
            exec(
                "SELECT ac.headcode, ac.units, ac.vehicles, ac.operator, ac.core_id " +
                "FROM allocation_consists ac " +
                "WHERE (',' || ac.units || ',') LIKE '%,$unit,%' AND ac.service_date = '$today' AND SUBSTR(ac.units, 1, 3) = SUBSTR('$unit', 1, 3)"
            ) { rs ->
                while (rs.next()) {
                    val hc = rs.getString("headcode") ?: return@exec
                    val coreId = rs.getString("core_id") ?: return@exec
                    val safeHc = hc.filter { it.isLetterOrDigit() }
                    val uid = if (coreId.length > hc.length + 2)
                        coreId.substring(hc.length, coreId.length - 2) else null
                    val sched = if (uid != null) {
                        Schedules.selectAll().where { Schedules.uid eq uid }
                            .orderBy(Schedules.scheduledTime, SortOrder.ASC)
                            .toList()
                            .filter { it[Schedules.stpIndicator] != 'C' }
                    } else {
                        Schedules.selectAll().where { Schedules.headcode eq safeHc }
                            .orderBy(Schedules.scheduledTime, SortOrder.ASC)
                            .toList()
                            .filter { it[Schedules.stpIndicator] != 'C' }
                    }
                    val priority = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)
                    val bestStp = sched.minOfOrNull { priority[it[Schedules.stpIndicator]] ?: 99 }
                    val best = sched.filter { (priority[it[Schedules.stpIndicator]] ?: 99) == bestStp }
                    val loRow = best.firstOrNull { it[Schedules.stopType] == "LO" } ?: best.firstOrNull() ?: return@exec
                    val unitList = (rs.getString("units") ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() }
                    val vehicleList = (rs.getString("vehicles") ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() }
                    val joinTiploc = if (loRow[Schedules.stopType] == "LI") loRow[Schedules.tiploc] else loRow[Schedules.originTiploc]
                    // Fetch full calling points for this service (needed for coupling detection)
                    val allStops = best.map { it[Schedules.tiploc] }.toSet()
                    rows.add(ServiceResponse(
                        uid           = loRow[Schedules.uid],
                        headcode      = hc,
                        atocCode      = loRow[Schedules.atocCode],
                        scheduledTime = loRow[Schedules.scheduledTime],
                        platform      = loRow[Schedules.platform],
                        isPass        = loRow[Schedules.isPass],
                        stopType      = loRow[Schedules.stopType],
                        originTiploc  = loRow[Schedules.originTiploc],
                        destTiploc    = loRow[Schedules.destTiploc],
                        originCrs     = loRow[Schedules.originCrs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(loRow[Schedules.originTiploc]),
                        destCrs       = loRow[Schedules.destCrs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(loRow[Schedules.destTiploc]),
                        actualTime    = "",
                        isCancelled   = false,
                        cancelReason  = null,
                        units         = unitList,
                        vehicles      = vehicleList,
                        unitJoinTiploc = joinTiploc,
                        destName      = CorpusLookup.nameFromTiploc(loRow[Schedules.destTiploc]),
                        originName    = CorpusLookup.nameFromTiploc(loRow[Schedules.originTiploc])
                    ))
                }
            }
        } catch (_: Exception) {}

        // ── Expand rows to include direct split partners only ────────────────────
        // Find services that originate at a tiploc on any primary service's route
        // and share at least one unit with that primary service.
        val seenUids = rows.map { it.uid }.toMutableSet()
        val extraRows = mutableListOf<ServiceResponse>()
        // Collect all tiplocs from primary services — batched in one query
        val primaryTiplocs = mutableMapOf<String, MutableSet<String>>()
        if (rows.isNotEmpty()) {
            try {
                val inClause = rows.map { "'${it.uid}'" }.toSet().joinToString(",")
                exec("SELECT uid, tiploc FROM schedules WHERE uid IN ($inClause) AND stp_indicator != 'C'") { rs ->
                    while (rs.next()) {
                        val u = rs.getString("uid") ?: continue
                        val t = rs.getString("tiploc") ?: continue
                        primaryTiplocs.getOrPut(u) { mutableSetOf() }.add(t)
                    }
                }
            } catch (_: Exception) {}
        }
        // For each primary service, find split partners via tiploc + unit overlap
        for (svc in rows.filter { it.units.size > 1 && unit in it.units }) {
            val myTiplocs2 = primaryTiplocs[svc.uid] ?: continue
            // Only expand on the partner units (not the searched unit itself)
            for (u in svc.units.filter { it != unit }) {
                try {
                    exec(
                        "SELECT ac.headcode, ac.units, ac.vehicles, ac.operator, ac.core_id " +
                        "FROM allocation_consists ac " +
                        "WHERE (',' || ac.units || ',') LIKE '%,$u,%' AND ac.service_date = '$today' " +
                        "AND ac.core_id NOT IN (${seenUids.joinToString(",") { "'${it.take(6)}'" }.ifEmpty { "''" }})"
                    ) { rs ->
                        while (rs.next()) {
                            val hc2 = rs.getString("headcode") ?: continue
                            val coreId2 = rs.getString("core_id") ?: continue
                            // Derive UID from coreId: format is headcode+uid+2digitseq
                            // Try all possible uid lengths (4-8 chars) and validate against schedules
                            val uid2 = try {
                                val hcLen = hc2.length
                                val candidate = (4..8).mapNotNull { uidLen ->
                                    if (hcLen + uidLen + 2 == coreId2.length)
                                        coreId2.substring(hcLen, hcLen + uidLen)
                                    else null
                                }.firstOrNull { candidate ->
                                    var exists = false
                                    exec("SELECT 1 FROM schedules WHERE uid = '$candidate' AND headcode = '$hc2' LIMIT 1") { rs2 ->
                                        if (rs2.next()) exists = true
                                    }
                                    exists
                                }
                                candidate
                            } catch (_: Exception) { null }
                            if (uid2 == null || uid2 in seenUids) continue
                            // Check this service originates at a tiploc on our route
                            val originTiploc2 = try {
                                var t = ""
                                exec("SELECT origin_tiploc FROM schedules WHERE uid = '$uid2' AND stop_type = 'LO' LIMIT 1") { rs2 ->
                                    if (rs2.next()) t = rs2.getString("origin_tiploc") ?: ""
                                }
                                t
                            } catch (_: Exception) { "" }
                            if (originTiploc2.isEmpty() || !myTiplocs2.contains(originTiploc2) || originTiploc2 == svc.originTiploc) continue
                            // Skip if the primary service terminates at the same point the partner originates
                            // (that's a join/coupling, handled separately, not a split)
                            if (svc.destTiploc == originTiploc2) continue
                            val sched2 = Schedules.selectAll().where { Schedules.uid eq uid2 }
                                .orderBy(Schedules.scheduledTime, SortOrder.ASC)
                                .toList()
                                .filter { it[Schedules.stpIndicator] != 'C' }
                            val priority2 = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)
                            val bestStp2 = sched2.minOfOrNull { priority2[it[Schedules.stpIndicator]] ?: 99 }
                            val best2 = sched2.filter { (priority2[it[Schedules.stpIndicator]] ?: 99) == bestStp2 }
                            val loRow2 = best2.firstOrNull { it[Schedules.stopType] == "LO" } ?: best2.firstOrNull() ?: continue
                            val unitList2 = (rs.getString("units") ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() }
                            val vehicleList2 = (rs.getString("vehicles") ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() }
                            val joinTiploc2 = if (loRow2[Schedules.stopType] == "LI") loRow2[Schedules.tiploc] else loRow2[Schedules.originTiploc]
                            seenUids.add(uid2)
                            extraRows.add(ServiceResponse(
                                uid           = loRow2[Schedules.uid],
                                headcode      = hc2,
                                atocCode      = loRow2[Schedules.atocCode],
                                scheduledTime = loRow2[Schedules.scheduledTime],
                                platform      = loRow2[Schedules.platform],
                                isPass        = loRow2[Schedules.isPass],
                                stopType      = loRow2[Schedules.stopType],
                                originTiploc  = loRow2[Schedules.originTiploc],
                                destTiploc    = loRow2[Schedules.destTiploc],
                                originCrs     = loRow2[Schedules.originCrs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(loRow2[Schedules.originTiploc]),
                                destCrs       = loRow2[Schedules.destCrs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(loRow2[Schedules.destTiploc]),
                                actualTime    = "",
                                isCancelled   = false,
                                cancelReason  = null,
                                units         = unitList2,
                                vehicles      = vehicleList2,
                                unitJoinTiploc = joinTiploc2,
                                destName      = CorpusLookup.nameFromTiploc(loRow2[Schedules.destTiploc]),
                                originName    = CorpusLookup.nameFromTiploc(loRow2[Schedules.originTiploc])
                            ))
                        }
                    }
                } catch (_: Exception) {}
            }
        }
                // Filter extraRows: only keep split partners whose units don't continue
        // on any primary service after the split (i.e. they genuinely leave the diagram)
        val primaryUnitsByTime = rows.flatMap { svc ->
            svc.units.map { u -> u to svc.scheduledTime }
        }.groupBy({ it.first }, { it.second })
        val filteredExtra = extraRows.filter { extra ->
            extra.units.all { u ->
                val primaryTimes = primaryUnitsByTime[u] ?: return@all true
                // Keep if the unit doesn't appear on any primary service after this split
                primaryTimes.none { t -> t > extra.scheduledTime }
            }
        }
        // allRows includes companion unit services for split detection
        val allRows = rows + filteredExtra
        val sorted = allRows.sortedBy { it.scheduledTime }


        // Build a map of uid -> full set of tiplocs — batched in one query
        val tiplocMap = mutableMapOf<String, MutableSet<String>>()
        if (sorted.isNotEmpty()) {
            try {
                val inClause = sorted.map { "'${it.uid}'" }.toSet().joinToString(",")
                exec("SELECT uid, tiploc FROM schedules WHERE uid IN ($inClause) AND stp_indicator != 'C' ORDER BY scheduled_time") { rs ->
                    while (rs.next()) {
                        val u = rs.getString("uid") ?: continue
                        val t = rs.getString("tiploc") ?: continue
                        tiplocMap.getOrPut(u) { mutableSetOf() }.add(t)
                    }
                }
            } catch (_: Exception) {}
        }

        // For each service, find what it forms next and any split portion
        sorted.map { svc ->
            val myTiplocs = tiplocMap[svc.uid] ?: emptySet()

            // SPLIT: another service originates mid-route on my route and takes a strict subset
            // of my units forward (some units split off, the rest continue with me)
            val splitPortion = sorted.filter { other ->
                other.uid != svc.uid &&
                other.originTiploc != svc.originTiploc &&
                other.originTiploc != svc.destTiploc &&
                myTiplocs.contains(other.originTiploc) &&
                other.units.isNotEmpty() && svc.units.size > 1 &&
                other.units.all { it in svc.units } &&
                other.units.size < svc.units.size &&
                svc.units.contains(unit) &&
                other.scheduledTime >= svc.scheduledTime &&
                other.scheduledTime.split(":").let { (h, m) -> h.toIntOrNull()?.times(60)?.plus(m.toIntOrNull() ?: 0) ?: 0 }
                    .minus(svc.scheduledTime.split(":").let { (h, m) -> h.toIntOrNull()?.times(60)?.plus(m.toIntOrNull() ?: 0) ?: 0 }) <= 90 &&
                (other.units.contains(unit) || svc.units.filter { it != unit }.any { it in other.units })
            }.minByOrNull { it.scheduledTime }

            // FORMS NEXT: next service in the diagram sharing a unit with this one
            val formsNext = sorted.filter { other ->
                other.uid != svc.uid &&
                other.scheduledTime > svc.scheduledTime &&
                other.units.any { it in svc.units }
            }.minByOrNull { it.scheduledTime }

            var result = svc
            if (splitPortion != null) {
                result = result.copy(
                    splitTiploc     = splitPortion.originTiploc,
                    splitTiplocName = splitPortion.originCrs?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(splitPortion.originTiploc),
                    splitToUid      = splitPortion.uid,
                    splitToHeadcode = splitPortion.headcode
                )
            }
            // formsNext: only for services that actually carry the searched unit
            val formsSearchedUnit = if (svc.units.contains(unit)) sorted.filter { other ->
                other.uid != svc.uid &&
                other.scheduledTime > svc.scheduledTime &&
                other.units.contains(unit)
            }.minByOrNull { it.scheduledTime } else null
            if (formsSearchedUnit != null) {
                result = result.copy(
                    formsUid      = formsSearchedUnit.uid,
                    formsHeadcode = formsSearchedUnit.headcode
                )
            }
            result
        }.filter { it.units.isEmpty() || it.units.contains(unit) }  // remove companion unit services from final output
    }
}
private fun annotateSplits(services: List<ServiceResponse>): List<ServiceResponse> {
    if (services.isEmpty()) return services
    val today = java.time.LocalDate.now().toString()
    return transaction {
        // Batch fetch allocations for all UIDs on the board
        val uids = services.map { it.uid }.toSet()

        // ── Primary: CIF associations (VV=divide, NP=join) ───────────────────────────
        val assocMap = AppDatabase.getAssociationsForUids(uids)
        val reverseAssocMap = AppDatabase.getReverseAssociationsForUids(uids)

        services.map { svc ->
            // ── Try CIF association first ────────────────────────────────────────────────────
            val assocs = assocMap[svc.uid]
            if (!assocs.isNullOrEmpty()) {
                var splitTiploc: String? = null
                var splitTiplocName: String? = null
                var splitToHeadcode: String? = null
                var splitToUid2: String? = null
                var splitToDestName2: String? = null
                var couplingTiploc2: String? = null
                var couplingTiplocName2: String? = null
                var coupledFromHeadcode2: String? = null
                for (assoc in assocs) {
                    var assocHeadcode = ""
                    try {
                        exec("SELECT headcode FROM schedules WHERE uid = '${assoc.assocUid}' AND stop_type = 'LO' AND stp_indicator != 'C' LIMIT 1") { rs ->
                            if (rs.next()) assocHeadcode = rs.getString("headcode") ?: ""
                        }
                    } catch (_: Exception) {}
                    val tiplocCrs = CorpusLookup.crsFromTiploc(assoc.assocTiploc) ?: crsfromTiplocFallback(assoc.assocTiploc) ?: assoc.assocTiploc
                    when (assoc.assocType) {
                        "VV" -> { // divide — main continues, assocUid splits off at assocTiploc
                            // Verify the split portion actually starts from assocTiploc (not diverted by engineering works)
                            var assocEffectiveLo = ""
                            try {
                                val priority = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)
                                exec("SELECT tiploc, stp_indicator FROM schedules WHERE uid = '${assoc.assocUid}' AND stop_type = 'LO' AND stp_indicator != 'C'") { rs ->
                                    var bestPriority = 99
                                    while (rs.next()) {
                                        val stp = rs.getString("stp_indicator")?.firstOrNull() ?: continue
                                        val p = priority[stp] ?: 99
                                        if (p < bestPriority) {
                                            bestPriority = p
                                            assocEffectiveLo = rs.getString("tiploc") ?: ""
                                        }
                                    }
                                }
                            } catch (_: Exception) {}
                            // Only show split if the split portion is a passenger service (not ECS/stock move)
                            val isPassengerSplit = assocHeadcode.firstOrNull()?.let { it in "1239" } == true
                            if ((assocEffectiveLo == assoc.assocTiploc || assocEffectiveLo.isEmpty()) && isPassengerSplit) {
                                splitTiploc     = assoc.assocTiploc
                                splitTiplocName = tiplocCrs
                                splitToHeadcode = assocHeadcode
                                splitToUid2     = assoc.assocUid
                                try {
                                    exec("SELECT dest_tiploc FROM schedules WHERE uid = '${assoc.assocUid}' AND stop_type = 'LT' AND stp_indicator != 'C' LIMIT 1") { rs ->
                                        if (rs.next()) splitToDestName2 = CorpusLookup.nameFromTiploc(rs.getString("dest_tiploc") ?: "")
                                    }
                                } catch (_: Exception) {}
                            }
                        }
                        "JJ", "NP" -> { // join — assocUid joins main at assocTiploc
                            // Only show coupling if the assocUid doesn't originate at the assocTiploc
                            // (if it does, it's just the train forming its next working, not a passenger coupling)
                            var assocOriginTiploc = ""
                            try {
                                exec("SELECT tiploc FROM schedules WHERE uid = '${assoc.assocUid}' AND stop_type = 'LO' AND stp_indicator != 'C' LIMIT 1") { rs ->
                                    if (rs.next()) assocOriginTiploc = rs.getString("tiploc") ?: ""
                                }
                            } catch (_: Exception) {}
                            // Also suppress if assocTiploc is a terminus of the main service
                            var assocTiplocIsTerminus = false
                            try {
                                exec("SELECT 1 FROM schedules WHERE uid = '${svc.uid}' AND tiploc = '${assoc.assocTiploc}' AND stop_type = 'LT' AND stp_indicator != 'C' LIMIT 1") { rs ->
                                    if (rs.next()) assocTiplocIsTerminus = true
                                }
                            } catch (_: Exception) {}
                            if (assocOriginTiploc != assoc.assocTiploc && !assocTiplocIsTerminus) {
                                couplingTiploc2      = assoc.assocTiploc
                                couplingTiplocName2  = tiplocCrs
                                coupledFromHeadcode2 = assocHeadcode
                            }
                        }
                    }
                }
                return@map when {
                    splitTiploc != null -> svc.copy(
                        splitTiploc     = splitTiploc,
                        splitTiplocName = splitTiplocName,
                        splitToHeadcode = splitToHeadcode,
                        splitToUid      = splitToUid2,
                        splitToDestName = splitToDestName2
                    )
                    couplingTiploc2 != null -> svc.copy(
                        couplingTiploc      = couplingTiploc2,
                        couplingTiplocName  = couplingTiplocName2,
                        coupledFromUid      = assocs.firstOrNull { it.assocTiploc == couplingTiploc2 }?.assocUid,
                        coupledFromHeadcode = coupledFromHeadcode2,
                        couplingAssocType   = assocs.firstOrNull { it.assocTiploc == couplingTiploc2 }?.assocType ?: "NP"
                    )
                    else -> svc
                }
            }
            // Check if this service is the split-off portion of another service (formed from)
            val reverseAssocs = reverseAssocMap[svc.uid]
            if (reverseAssocs != null && reverseAssocs.isNotEmpty() && svc.couplingTiploc == null) {
                for (assoc in reverseAssocs) {
                    if (assoc.assocType in listOf("VV", "JJ")) {
                        var parentHeadcode = ""
                        try {
                            exec("SELECT headcode FROM schedules WHERE uid = '${assoc.mainUid}' AND stop_type = 'LO' AND stp_indicator != 'C' LIMIT 1") { rs ->
                                if (rs.next()) parentHeadcode = rs.getString("headcode") ?: ""
                            }
                        } catch (_: Exception) {}
                        val tiplocCrs = CorpusLookup.crsFromTiploc(assoc.assocTiploc) ?: crsfromTiplocFallback(assoc.assocTiploc) ?: assoc.assocTiploc
                        if (parentHeadcode.isNotEmpty()) {
                            return@map svc.copy(
                                couplingTiploc      = assoc.assocTiploc,
                                couplingTiplocName  = tiplocCrs,
                                coupledFromUid      = assoc.mainUid,
                                coupledFromHeadcode = parentHeadcode,
                                couplingAssocType   = assoc.assocType
                            )
                        }
                    }
                }
            }

            svc
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

private fun enrichWithUnits(services: List<ServiceResponse>): List<ServiceResponse> {
    val today = java.time.LocalDate.now().toString()
    return services.map { svc ->
        if (svc.units.isNotEmpty()) return@map svc
        val alloc = AppDatabase.getAllocationByUid(svc.uid, today)
        if (alloc != null) svc.copy(units = alloc.units, vehicles = alloc.vehicles)
        else svc
    }
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
        Schedules.select(needed).where {
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
        .let { groups ->
            val uidsWithOverride = mutableSetOf<String>()
            val pUids = groups.mapNotNull { rows ->
                val priority = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)
                val best = rows.minByOrNull { priority[it[Schedules.stpIndicator]] ?: 99 } ?: return@mapNotNull null
                if (best[Schedules.stpIndicator] == 'P') best[Schedules.uid] else null
            }
            if (pUids.isNotEmpty()) {
                val inClause = pUids.joinToString(",") { "'$it'" }
                exec("SELECT DISTINCT uid FROM schedules WHERE uid IN ($inClause) AND stp_indicator IN ('O','N')") { rs ->
                    while (rs.next()) uidsWithOverride.add(rs.getString("uid") ?: "")
                }
            }
            // Build set of UIDs whose WINNING schedule actually calls at this CRS
            // This filters out services where a lower-priority schedule calls at the CRS
            // but the winning override (e.g. engineering amendment) does not
            val winnerRows = mutableListOf<org.jetbrains.exposed.sql.ResultRow>()
            val pFallbackUids = mutableSetOf<String>()
            groups.forEach { rows ->
                val priority = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)
                val best = rows.minByOrNull { priority[it[Schedules.stpIndicator]] ?: 99 } ?: return@forEach
                if (best[Schedules.stpIndicator] == 'P' && best[Schedules.uid] in uidsWithOverride) {
                    // P suppressed by override — but override may not call at this CRS (partial diversion)
                    // Keep as fallback candidate; validated below
                    pFallbackUids.add(best[Schedules.uid])
                } else {
                    winnerRows.add(best)
                }
            }
            // Validate winnerRows: verify winning STP actually calls at queried CRS
            val validUids = mutableSetOf<String>()
            if (winnerRows.isNotEmpty()) {
                val inClause = winnerRows.map { "'${it[Schedules.uid]}'" }.toSet().joinToString(",")
                val safeCrsVal = crs.filter { it.isLetterOrDigit() }
                exec("SELECT DISTINCT uid, stp_indicator FROM schedules WHERE uid IN ($inClause) AND crs='$safeCrsVal'") { rs ->
                    while (rs.next()) {
                        val uid = rs.getString("uid") ?: continue
                        val stp = rs.getString("stp_indicator")?.firstOrNull() ?: continue
                        val winnerStp = winnerRows.find { it[Schedules.uid] == uid }?.get(Schedules.stpIndicator)
                        if (stp == winnerStp) validUids.add(uid)
                    }
                }
            }
            // For pFallbackUids: P suppressed by override that doesn't call at this CRS
            // Verify the O/N override genuinely has no row here, then use P row instead
            val pFallbackRows = if (pFallbackUids.isNotEmpty()) {
                val safeCrsVal = crs.filter { it.isLetterOrDigit() }
                // Remove any UIDs where the O/N override DOES call at this CRS (shouldn't show P)
                val overrideCallsHere = mutableSetOf<String>()
                val inClause2 = pFallbackUids.joinToString(",") { "'$it'" }
                exec("SELECT DISTINCT uid FROM schedules WHERE uid IN ($inClause2) AND stp_indicator IN ('O','N') AND crs='$safeCrsVal'") { rs ->
                    while (rs.next()) overrideCallsHere.add(rs.getString("uid") ?: "")
                }
                // Also exclude UIDs where the O override has moved the origin (LO) to a different CRS
                // — the service no longer starts from this CRS
                val overrideMovedOrigin = mutableSetOf<String>()
                exec("SELECT DISTINCT uid FROM schedules WHERE uid IN ($inClause2) AND stp_indicator IN ('O','N') AND stop_type='LO' AND crs!='$safeCrsVal'") { rs ->
                    while (rs.next()) overrideMovedOrigin.add(rs.getString("uid") ?: "")
                }
                val genuineFallbacks = pFallbackUids - overrideCallsHere - overrideMovedOrigin
                val pRows = mutableListOf<org.jetbrains.exposed.sql.ResultRow>()
                for (uid in genuineFallbacks) {
                    Schedules.select(needed).where {
                        (Schedules.uid eq uid) and
                        (Schedules.stpIndicator eq 'P') and
                        (Schedules.crs eq safeCrsVal)
                    }.firstOrNull()?.let { pRows.add(it) }
                }
                pRows
            } else emptyList()
            (winnerRows.filter { it[Schedules.uid] in validUids } + pFallbackRows)
                .sortedBy { it[Schedules.scheduledTime] }
        }
        .sortedBy { it[Schedules.scheduledTime] }
        .let { sortedRows ->
            val safeCrsQ     = crs.filter { it.isLetterOrDigit() }
            val safeWinStart = windowStart.filter { it.isDigit() || it == ':' }
            val safeWinEnd   = windowEnd.filter   { it.isDigit() || it == ':' }
            val trustMap = mutableMapOf<String, Triple<String, Boolean, String?>>()
            val bstOffset = if (java.time.ZonedDateTime.now(java.time.ZoneId.of("Europe/London")).zone.rules.isDaylightSavings(java.time.Instant.now())) 60 else 0
            fun bstToUtc(t: String): String {
                if (bstOffset == 0 || t.isEmpty()) return t
                val p = t.split(":")
                if (p.size != 2) return t
                val m = (p[0].toIntOrNull() ?: 0) * 60 + (p[1].toIntOrNull() ?: 0) - bstOffset
                return "%02d:%02d".format((m + 1440) % 1440 / 60, (m + 1440) % 1440 % 60)
            }
            // Query by UID — eliminates cross-train headcode contamination entirely.
            val uidInClause = sortedRows.map { "'${it[Schedules.uid]}'" }.toSet().joinToString(",")
            if (uidInClause.isNotEmpty()) {
                try {
                    exec("SELECT uid, scheduled_time, actual_time, is_cancelled, cancel_reason FROM trust_movements WHERE uid IN ($uidInClause) AND event_ts >= datetime('now', '-12 hours') ORDER BY event_ts DESC") { rs ->
                        while (rs.next()) {
                            val uid  = rs.getString("uid")?.takeIf { it.isNotEmpty() } ?: continue
                            val scht = rs.getString("scheduled_time") ?: continue
                            val actualTime = rs.getString("actual_time") ?: ""
                            val isCancelled = rs.getBoolean("is_cancelled")
                            val reason = rs.getString("cancel_reason")
                            val triple = Triple(actualTime, isCancelled, reason)
                            val stopKey = "$uid|$scht"
                            if (!trustMap.containsKey(stopKey)) trustMap[stopKey] = triple
                            // Only store UID-level entry for cancellations
                            if (isCancelled && !trustMap.containsKey(uid)) trustMap[uid] = triple
                        }
                    }
                } catch (_: Exception) {}
            }
            // Build maps of uid -> O-schedule LT/LO overrides (engineering works diversions)
            val oLtOverrides = mutableMapOf<String, Pair<String, String?>>() // uid -> (tiploc, crs) early termination
            val oLoOverrides = mutableMapOf<String, Pair<String, String?>>() // uid -> (tiploc, crs) late origin
            val uidInClause2 = sortedRows.map { "'${it[Schedules.uid]}'" }.toSet().joinToString(",")
            if (uidInClause2.isNotEmpty()) {
                try {
                    exec("SELECT uid, tiploc, crs, stop_type FROM schedules WHERE uid IN ($uidInClause2) AND stp_indicator='O' AND stop_type IN ('LT','LO')") { rs ->
                        while (rs.next()) {
                            val uid = rs.getString("uid") ?: continue
                            val tiploc = rs.getString("tiploc") ?: continue
                            val crs = rs.getString("crs")?.takeIf { it.isNotEmpty() }
                            when (rs.getString("stop_type")) {
                                "LT" -> oLtOverrides[uid] = Pair(tiploc, crs)
                                "LO" -> oLoOverrides[uid] = Pair(tiploc, crs)
                            }
                        }
                    }
                } catch (_: Exception) {}
            }

            sortedRows.map { row ->
                val scht               = row[Schedules.scheduledTime]
                val stopEntry          = trustMap["${row[Schedules.uid]}|$scht"]
                val uidEntry           = trustMap[row[Schedules.uid]]
                // Only use UID-only fallback for cancellations, not movement times
                val trustEntry         = stopEntry ?: if (uidEntry?.second == true) uidEntry else null
                val actualTime         = trustEntry?.first ?: ""
                val isCancelledByTrust = trustEntry?.second ?: false
                val cancelReasonCode   = trustEntry?.third
                // Use O-schedule overrides for origin/destination if present (engineering works)
                val oLtOverride = oLtOverrides[row[Schedules.uid]]
                val oLoOverride = oLoOverrides[row[Schedules.uid]]
                val effectiveDestTiploc = oLtOverride?.first ?: row[Schedules.destTiploc]
                val effectiveDestCrs = oLtOverride?.second
                    ?: row[Schedules.destCrs]?.takeIf { it.isNotEmpty() }
                    ?: CorpusLookup.crsFromTiploc(row[Schedules.destTiploc])
                val effectiveOriginTiploc = oLoOverride?.first ?: row[Schedules.originTiploc]
                val effectiveOriginCrs = oLoOverride?.second
                    ?: row[Schedules.originCrs]?.takeIf { it.isNotEmpty() }
                    ?: CorpusLookup.crsFromTiploc(row[Schedules.originTiploc])
                ServiceResponse(
                    uid           = row[Schedules.uid],
                    headcode      = row[Schedules.headcode],
                    atocCode      = row[Schedules.atocCode],
                    scheduledTime = row[Schedules.scheduledTime],
                    platform      = row[Schedules.platform],
                    isPass        = row[Schedules.isPass],
                    stopType      = row[Schedules.stopType],
                    originTiploc  = effectiveOriginTiploc,
                    destTiploc    = effectiveDestTiploc,
                    originCrs     = effectiveOriginCrs,
                    destCrs       = effectiveDestCrs,
                    actualTime    = actualTime,
                    isCancelled   = isCancelledByTrust || row[Schedules.stpIndicator] == 'C',
                    cancelReason  = cancelReasonCode,
                    destName      = CorpusLookup.nameFromTiploc(effectiveDestTiploc),
                    originName    = CorpusLookup.nameFromTiploc(row[Schedules.originTiploc])
                )
            }
        }
    }
}

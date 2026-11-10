package com.traintracker.server.routes

import com.traintracker.server.ReasonCodes
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.ScheduleCancellations
import com.traintracker.server.database.Allocations
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.CANCELLED_ONLY_SQL
import com.traintracker.server.database.Schedules
import com.traintracker.server.database.activationsFor
import com.traintracker.server.database.query
import com.traintracker.server.database.queryFirst
import com.traintracker.server.database.scheduleDateScope
import com.traintracker.server.database.serviceDayArgs
import com.traintracker.server.database.serviceDaySql
import com.traintracker.server.database.toIsoUtc
import com.traintracker.server.kafka.LinxMovementWriter
import com.traintracker.server.database.unitsCsv
import io.ktor.http.HttpStatusCode
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import java.time.LocalDateTime
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.jdbc.selectAll

/** Movements report some stations under a different CRS from the timetable. */
private val MOVEMENT_CRS_ALIASES = mapOf("PDX" to "PAD", "FDX" to "FOG", "LSX" to "LST", "ABX" to "ABW", "WHX" to "WWC")

/** A service's route as built from the timetable and live data. */
private class ServiceRoute(
    val headcode: String,
    val tocCode: String,
    val tiplocNames: Map<String, String?>,
    val callingPoints: List<CallingPointResponse>,
)

/**
 * Reported (and TD) times for one service, keyed by scheduled time and by "CRS|scheduled time":
 * [latest] is (actual time, cancelled, reason) merged over all events; the others are the first arrival and
 * departure times seen, newest first, with seconds when TD supplied them.
 */
private class MovementTimes {
    val latest = mutableMapOf<String, Triple<String, Boolean, String?>>()
    val arrivals = mutableMapOf<String, String>()
    val departures = mutableMapOf<String, String>()
    val arrivalSecs = mutableMapOf<String, String>()
    val departureSecs = mutableMapOf<String, String>()
    /** Keys cancelled only because the train now starts later (change of origin). */
    val changeOfOriginKeys = mutableSetOf<String>()
    /** Every reported movement, newest first, for calls whose times do not match exactly. */
    val reports = mutableListOf<Report>()

    class Report(val crs: String, val scheduled: String, val actual: String, val actualSec: String, val eventType: String, val cancelled: Boolean, val reason: String?)

    /**
     * The report at [crs] whose scheduled time is nearest [time], within 10 minutes, preferring [eventType].
     * Movements carry working-timetable times and the timetable rows public ones, which can differ by a
     * few minutes (a terminus booked 02:14 public is 02:12 in the WTT), so an exact match can miss.
     */
    fun nearest(crs: String?, time: String?, eventType: String): Report? {
        if (crs == null || time.isNullOrEmpty()) return null
        return reports.asSequence()
            .filter { it.crs == crs && !it.cancelled && it.actual.isNotEmpty() }
            .map { it to kotlin.math.abs(minutesLate(it.scheduled, time)) }
            .filter { it.second <= 10 }
            .minWithOrNull(compareBy<Pair<Report, Int>> { it.first.eventType != eventType }.thenBy { it.second })?.first
    }
}

/** The value for a scheduled time, by time alone or else by "CRS|time". */
private fun Map<String, String>.at(time: String?, crs: String?): String? =
    time?.let { t -> this[t] ?: crs?.let { this["$it|$t"] } }

fun Route.serviceDetailRoutes() {
    get("/service/{uid}") {
        val uid = call.pathParam("uid")
        val atCrs = call.parameters["crs"]?.uppercase()?.trim() ?: ""
        val svcDate = call.parameters["date"]?.filter { it.isDigit() || it == '-' }?.takeIf { it.length == 10 }
            ?: defaultServiceDate(uid)
        val route = dbQuery { serviceRoute(uid, atCrs, svcDate) }

        // A train activated from a later station (change of origin) starts there: the calls before it stay in
        // the list, shown as cancelled, and the page names the new origin as the start.
        val activationOriginCrs = dbQuery {
            activationsFor(listOf(uid), svcDate)[uid]?.originStanox?.let { CorpusLookup.crsFromStanox(it) }
        }
        val cps = cancelBeforeOrigin(route.callingPoints, activationOriginCrs)
        if (cps.isEmpty()) return@get call.respond(HttpStatusCode.NotFound, "Service $uid not found")

        // Split at the requested station into calls before it and from it onwards.
        val atIndex = cps.indexOfFirst { it.crs == atCrs }.let {
            when {
                it < 0 && atCrs.isNotEmpty() -> cps.indexOfFirst { cp -> cp.tiploc.startsWith(atCrs) }
                it < 0 -> 0
                else -> it
            }
        }
        val previous = if (atCrs.isNotEmpty() && atIndex > 0) cps.subList(0, atIndex) else emptyList()
        val subsequent = if (atCrs.isNotEmpty() && atIndex >= 0) cps.subList(atIndex, cps.size) else cps
        val allCps = previous + subsequent
        val headcode = route.headcode

        val association = associationsFor(uid, svcDate, route, cps)

        // The previous and next workings of the units, unless an association already explains them.
        val originTiploc = cps.firstOrNull { it.stopType == "LO" }?.tiploc
        val destTiploc = cps.firstOrNull { it.stopType == "LT" }?.tiploc
        val backwardExplained = association.splitFromUid != null ||
            (association.couplingTiploc != null && association.couplingTiploc == originTiploc)
        val forwardExplained = association.formsUid != null ||
            (association.couplingTiploc != null && association.couplingTiploc == destTiploc)
        val prevWorkings = if (!backwardExplained) dbQuery { Allocations.previousWorkings(uid, svcDate) } else emptyList()
        val nextWorkings = if (!forwardExplained) dbQuery { Allocations.nextWorkings(uid, svcDate) } else emptyList()

        val legData = dbQuery { Allocations.legData(uid, svcDate) }
        val activation = dbQuery { activationsFor(listOf(uid), svcDate)[uid] }
        val tiplocsInOrder = allCps.map { it.tiploc }
        call.respond(
            ServiceDetailResponse(
                uid = uid,
                atCrs = atCrs,
                headcode = headcode,
                atocCode = route.tocCode,
                previous = withTrainLength(previous, legData.spans, tiplocsInOrder),
                subsequent = withTrainLength(subsequent, legData.spans, tiplocsInOrder),
                serviceType = when {
                    headcode.startsWith("0B") -> "BUS_REPLACEMENT"
                    headcode.startsWith("0C") -> "COACH_REPLACEMENT"
                    else -> "NORMAL"
                },
                isCancelled = allCps.isNotEmpty() && allCps.all { it.isCancelled },
                isPartiallyCancelled = allCps.any { it.isCancelled } && allCps.any { !it.isCancelled },
                cancelReason = allCps.firstOrNull { it.isCancelled && it.cancelReason != null }?.cancelReason,
                association = association,
                prevWorkingUid = prevWorkings.firstOrNull()?.first,
                prevWorkingHeadcode = prevWorkings.firstOrNull()?.second,
                prevWorkingDate = prevWorkings.firstOrNull()?.third,
                nextWorkingUid = nextWorkings.firstOrNull()?.first,
                nextWorkingHeadcode = nextWorkings.firstOrNull()?.second,
                nextWorkingDate = nextWorkings.firstOrNull()?.third,
                nextWorkingAll = nextWorkings.map { StockNeighbourCandidate(it.first, it.second, it.third) },
                activatedAt = activation?.updatedAt?.let { toIsoUtc(it) },
                activationOrigin = activation?.originStanox?.let { CorpusLookup.tiplocFromStanox(it) },
                legs = legData.groups.map { g ->
                    UnitLegResponse(
                        g.fromTiploc,
                        g.toTiploc,
                        allCps.firstOrNull { it.tiploc == g.fromTiploc }?.name,
                        allCps.firstOrNull { it.tiploc == g.toTiploc }?.name,
                        g.fromTime,
                        g.toTime,
                        g.units,
                        g.coachCount,
                        g.vehicles
                    )
                },
                serviceDate = svcDate
            )
        )
    }
}

// ---- Calling points ------------------------------------------------------------------------------------

/** The calling points of a service on a date, with live times, cancellations, delays, platforms and TD-only points. */
private fun JdbcTransaction.serviceRoute(uid: String, atCrs: String, svcDate: String): ServiceRoute {
    val allRows = Schedules.selectAll().where { (Schedules.uid eq uid) and scheduleDateScope(svcDate) }
        .orderBy(Schedules.stopSequence, SortOrder.ASC).toList()
    val rows = trimToDestination(selectVariantRows(allRows, atCrs))

    val headcode = rows.firstOrNull()?.get(Schedules.headcode) ?: ""
    val tdOverlay = TdOverlay.load(headcode, svcDate)
    val timetableCancelled = ScheduleCancellations.isCancelled(uid, svcDate)
    val firstRow = rows.firstOrNull()
    val tocCode = firstRow?.let { CorpusLookup.operatorForTsc(it[Schedules.tsc]) } ?: ""
    firstRow?.let { CorpusLookup.recordMissingTscIfNoOverride(it[Schedules.tsc], headcode, uid) }
    val moves = if (headcode.any { it.isLetterOrDigit() }) movementTimes(uid, svcDate, tdOverlay) else MovementTimes()

    val tiplocs = rows.map { it[Schedules.tiploc] }.toSet()
    val tiplocNames = CorpusLookup.namesFromTiplocs(tiplocs)
    CorpusLookup.recordMissingFromNames(tiplocs, headcode, uid)

    val darwinDb = AppDatabase.darwinPlatforms(uid, tiplocs)
    val movementPlatforms = AppDatabase.movementPlatforms(listOf(uid))
    val ttCancelled = timetableCancelled && moves.arrivals.isEmpty() && moves.departures.isEmpty()
    val cps = rows.map { row ->
        val tiploc = row[Schedules.tiploc]
        val crs = row[Schedules.crs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(tiploc)
        val scht = row[Schedules.scheduledTime]
        val schedArr = row[Schedules.scheduledArrival]
        val schedDep = row[Schedules.scheduledDeparture]
        val exactMove = moves.latest[scht] ?: crs?.let { moves.latest["$it|$scht"] }
        // No exact match: the nearest report here (arrival at the terminus, departure elsewhere).
        val terminus = row[Schedules.stopType] == "LT"
        val near = if (exactMove == null) {
            moves.nearest(crs, if (terminus) schedArr ?: scht else schedDep ?: scht, if (terminus) "ARRIVAL" else "DEPARTURE")
        } else {
            null
        }
        val nearArr = near?.takeIf { it.eventType == "ARRIVAL" }
        val nearDep = near?.takeIf { it.eventType == "DEPARTURE" }
        val (platform, confirmed) = resolvePlatform(uid, tiploc, crs, row[Schedules.platform], darwinDb[tiploc], movementPlatforms)
        CallingPointResponse(
            tiploc,
            crs,
            tiplocNames[tiploc],
            scht,
            schedArr,
            schedDep,
            platform,
            confirmed,
            row[Schedules.platform],
            row[Schedules.isPass],
            row[Schedules.stopType],
            exactMove?.first ?: near?.actual ?: "",
            moves.arrivals.at(schedArr, crs) ?: nearArr?.actual ?: "",
            moves.departures.at(schedDep, crs) ?: moves.departures.at(scht, crs) ?: nearDep?.actual ?: "",
            moves.arrivalSecs.at(schedArr, crs) ?: nearArr?.actualSec ?: "",
            moves.departureSecs.at(schedDep, crs) ?: moves.departureSecs.at(scht, crs) ?: nearDep?.actualSec ?: "",
            (exactMove?.second ?: false) || ttCancelled,
            exactMove?.third ?: if (ttCancelled) TIMETABLE_CANCELLED else null
        )
    }
    val spread = applyCancellations(cps) { cp ->
        cp.scheduledTime in moves.changeOfOriginKeys || "${cp.crs}|${cp.scheduledTime}" in moves.changeOfOriginKeys
    }
    // Calls matched to the nearest report count as reported, so later estimates build on them.
    val reportedKeys = moves.latest.keys + spread.filter { it.actualTime.isNotEmpty() && !it.isPredicted }.map { it.effectiveTime() }
    val projected = projectDelays(spread, reportedKeys)
    val loTime = projected.firstOrNull { it.stopType == "LO" }?.effectiveTime() ?: "00:00"
    val ltTime = projected.firstOrNull { it.stopType == "LT" }?.effectiveTime() ?: "23:59"
    val ordered = if (loTime > ltTime) projected.sortedFromClock(loTime) { it.effectiveTime() } else projected
    return ServiceRoute(headcode, tocCode, tiplocNames, RoutePoints.merge(ordered, uid, svcDate, headcode))
}

/**
 * The schedule rows to show: the strongest STP variant (or, when a station was requested and the strongest
 * variant starts elsewhere, the strongest one starting there). A partial overlay is completed from the
 * permanent schedule.
 */
private fun selectVariantRows(rows: List<ResultRow>, atCrs: String): List<ResultRow> {
    val bestRank = rows.minOfOrNull { stpRank(it) }
    var bestRows = rows.filter { stpRank(it) == bestRank }
    if (atCrs.isNotEmpty()) {
        val hasLo = bestRows.any { it[Schedules.stopType] == "LO" }
        val hasLoHere = bestRows.any { it[Schedules.stopType] == "LO" && it[Schedules.crs] == atCrs }
        if (hasLo && !hasLoHere) {
            val bestStp = STP_PRIORITY.entries.find { it.value == bestRank }?.key
            val altStp = STP_PRIORITY.entries.sortedBy { it.value }.map { it.key }.firstOrNull { stp ->
                stp != bestStp && rows.any { it[Schedules.stpIndicator] == stp && it[Schedules.stopType] == "LO" && it[Schedules.crs] == atCrs }
            }
            if (altStp != null) bestRows = rows.filter { it[Schedules.stpIndicator] == altStp }
        }
    }
    val chosenStp = bestRows.firstOrNull()?.get(Schedules.stpIndicator)?.takeIf { it in STP_PRIORITY }
    return if (chosenStp != null && chosenStp != 'P') completeOverlay(bestRows, rows) else bestRows
}

/** Fills in the parts of the route an overlay (or VSTP) leaves out, from the permanent schedule. */
private fun completeOverlay(overlay: List<ResultRow>, rows: List<ResultRow>): List<ResultRow> {
    val hasLo = overlay.any { it[Schedules.stopType] == "LO" }
    val hasLt = overlay.any { it[Schedules.stopType] == "LT" }
    val hasLi = overlay.any { it[Schedules.stopType] == "LI" }
    val pRows = rows.filter { it[Schedules.stpIndicator] == 'P' }.sortedBy { it[Schedules.scheduledTime] }
    return when {
        // Overlay is only a new terminus: the permanent route up to it.
        !hasLo && !hasLi && hasLt -> {
            val oLt = overlay.first { it[Schedules.stopType] == "LT" }
            val cutIdx = pRows.indexOfFirst { it[Schedules.tiploc] == oLt[Schedules.tiploc] }
            if (cutIdx >= 0) pRows.subList(0, cutIdx) + oLt else pRows.filter { it[Schedules.stopType] != "LT" } + oLt
        }
        // Overlay is a set of intermediate points.
        !hasLo && !hasLt -> {
            val overridden = overlay.map { it[Schedules.tiploc] }.toSet()
            val overlapsPermanent = pRows.any { it[Schedules.tiploc] in overridden }
            val onlyPassingPoints = overlay.all { it[Schedules.crs].isNullOrEmpty() && it[Schedules.isPass] }
            if (onlyPassingPoints || overlapsPermanent) {
                (overlay + pRows.filter { it[Schedules.tiploc] !in overridden }).sortedBy { it[Schedules.scheduledTime] }
            } else {
                val oLast = overlay.maxOf { it[Schedules.scheduledTime] }
                val pAfterByTime = pRows.filter { it[Schedules.scheduledTime] > oLast }
                val pAfterTiplocs = pAfterByTime.map { it[Schedules.tiploc] }.toSet()
                val firstPAfterIdx = pRows.indexOfFirst { it[Schedules.tiploc] in pAfterTiplocs }
                val pBeforeCandidate = if (firstPAfterIdx > 0) pRows.subList(0, firstPAfterIdx) else emptyList()
                val pBeforeHasStations = pBeforeCandidate.any { it[Schedules.stopType] == "LI" && !it[Schedules.crs].isNullOrEmpty() }
                val pBefore = if (pBeforeHasStations) pBeforeCandidate.filter { it[Schedules.stopType] == "LO" } else pBeforeCandidate
                val pAfter = if (firstPAfterIdx >= 0) pRows.subList(firstPAfterIdx, pRows.size) else pAfterByTime
                val overlayCrs = overlay.mapNotNull { it[Schedules.crs].takeIf { c -> !c.isNullOrEmpty() } }.toSet()
                val notInOverlay = { r: ResultRow -> r[Schedules.crs].isNullOrEmpty() || r[Schedules.crs] !in overlayCrs }
                pBefore.filter(notInOverlay) + overlay + pAfter.filter(notInOverlay)
            }
        }
        // Overlay has its own origin but no intermediate points: borrow the permanent ones between its origin and terminus.
        hasLo && !hasLi -> {
            val oLo = overlay.first { it[Schedules.stopType] == "LO" }
            val oLoTime = oLo[Schedules.scheduledTime]
            val oLtTime = overlay.firstOrNull { it[Schedules.stopType] == "LT" }?.get(Schedules.scheduledTime)
            val oOriginTiploc = oLo[Schedules.tiploc]
            fun pIndex(tiploc: String) = pRows.indexOfFirst { it[Schedules.tiploc] == tiploc }
            val originIdx = pIndex(oOriginTiploc)
            val pLi = pRows.filter { row ->
                row[Schedules.stopType] == "LI" && row[Schedules.tiploc] != oOriginTiploc &&
                    (if (originIdx >= 0) pIndex(row[Schedules.tiploc]) > originIdx else row[Schedules.scheduledTime] > oLoTime) &&
                    (oLtTime == null || row[Schedules.scheduledTime] < oLtTime)
            }
            val pLt = if (!hasLt) pRows.filter { it[Schedules.stopType] == "LT" } else emptyList()
            (overlay + pLi + pLt).sortedBy { row ->
                if (row[Schedules.tiploc] == oOriginTiploc) originIdx else pIndex(row[Schedules.tiploc]).takeIf { it >= 0 } ?: Int.MAX_VALUE
            }
        }
        else -> overlay.sortedBy { it[Schedules.scheduledTime] }
    }
}

/** Orders rows from the origin (across midnight) and ends them at the terminus, dropping earlier visits to it. */
private fun trimToDestination(rows: List<ResultRow>): List<ResultRow> {
    val destTiploc = rows.firstOrNull { it[Schedules.stopType] == "LT" }?.get(Schedules.tiploc)
        ?: return rows.sortedBy { it[Schedules.scheduledTime] }
    val loT = rows.firstOrNull { it[Schedules.stopType] == "LO" }?.get(Schedules.scheduledTime) ?: "00:00"
    val ltT = rows.firstOrNull { it[Schedules.stopType] == "LT" }?.get(Schedules.scheduledTime) ?: "23:59"
    val sorted = if (loT > ltT) rows.sortedFromClock(loT) { it[Schedules.scheduledTime] } else rows
    fun terminusIndex(list: List<ResultRow>) =
        list.indexOfFirst { it[Schedules.tiploc] == destTiploc && it[Schedules.stopType] == "LT" }
            .let { if (it < 0) list.indexOfFirst { r -> r[Schedules.tiploc] == destTiploc } else it }
    val ltIndex = terminusIndex(sorted)
    val deduped = sorted.filterIndexed { i, r ->
        r[Schedules.tiploc] != destTiploc || r[Schedules.stopType] == "LT" || r[Schedules.stopType] == "LO" || i == ltIndex
    }
    val finalIdx = terminusIndex(deduped)
    return if (finalIdx >= 0) deduped.subList(0, finalIdx + 1) else deduped
}

/** Movements for the service day, with TD times (to the second) where they match. */
private fun JdbcTransaction.movementTimes(uid: String, svcDate: String, tdOverlay: TdOverlay): MovementTimes {
    val times = MovementTimes()
    // When a train ID has only cancellations, use that train ID's events alone.
    val cancelledOnlyTrainId = queryFirst(
        "SELECT train_id FROM movements WHERE uid=? AND ${serviceDaySql("event_ts")} GROUP BY train_id HAVING $CANCELLED_ONLY_SQL LIMIT 1",
        listOf(uid) + serviceDayArgs(svcDate)
    ) { it.getString(1) }
    query(
        "SELECT crs, stanox, scheduled_time, actual_time, event_type, is_cancelled, cancel_reason, event_ts FROM movements WHERE uid=? ${if (cancelledOnlyTrainId != null) "AND train_id=?" else ""} AND ${serviceDaySql("event_ts")} ORDER BY event_ts DESC",
        listOfNotNull(uid, cancelledOnlyTrainId) + serviceDayArgs(svcDate)
    ) { rs ->
        while (rs.next()) {
            val scht = rs.getString("scheduled_time") ?: continue
            val isCancelled = rs.getBoolean("is_cancelled")
            val cancelReason = ReasonCodes.resolve(rs.getString("cancel_reason"))
            val eventType = rs.getString("event_type") ?: ""
            val td = tdOverlay.resolve(rs.getString("stanox") ?: "", eventType, rs.getString("event_ts"))
            val actual = td?.minute ?: rs.getString("actual_time") ?: ""
            val actualSec = td?.seconds ?: ""
            val crs = (rs.getString("crs")?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromStanox(rs.getString("stanox") ?: ""))
                ?.let { MOVEMENT_CRS_ALIASES[it] ?: it }
            if (crs != null) times.reports += MovementTimes.Report(crs, scht, actual, actualSec, eventType, isCancelled, cancelReason)
            for (key in listOfNotNull(scht, crs?.let { "$it|$scht" })) {
                if (eventType == LinxMovementWriter.COO_CANCELLATION) times.changeOfOriginKeys += key
                val existing = times.latest[key]
                times.latest[key] = if (existing == null) {
                    Triple(actual, isCancelled, cancelReason)
                } else {
                    Triple(actual.ifEmpty { existing.first }, existing.second || isCancelled, cancelReason ?: existing.third)
                }
                if (actual.isEmpty()) continue
                when (eventType) {
                    "ARRIVAL" -> if (times.arrivals.putIfAbsent(key, actual) == null && actualSec.isNotEmpty()) times.arrivalSecs[key] = actualSec
                    "DEPARTURE" -> if (times.departures.putIfAbsent(key, actual) == null && actualSec.isNotEmpty()) times.departureSecs[key] = actualSec
                }
            }
        }
    }
    return times
}

/**
 * Cancellations as shown: [propagateCancellations], except that a call cancelled by a change of origin
 * ([isChangeOfOrigin]) stays cancelled itself without cancelling the rest of the run.
 */
internal fun applyCancellations(
    cps: List<CallingPointResponse>,
    isChangeOfOrigin: (CallingPointResponse) -> Boolean,
): List<CallingPointResponse> {
    val cooIdx = cps.indices.filter { cps[it].isCancelled && isChangeOfOrigin(cps[it]) }.toSet()
    return propagateCancellations(cps.mapIndexed { i, cp -> if (i in cooIdx) cp.copy(isCancelled = false, cancelReason = null) else cp })
        .mapIndexed { i, cp -> if (i in cooIdx) cps[i] else cp }
}

/**
 * A train activated from a later station ([activationOriginCrs], after a change of origin) starts there: the
 * calls before it stay in the list but are cancelled (unless they somehow ran), with the reason given on any
 * of them. Unchanged when the activation origin is the timetabled one, the terminus, or not in the list.
 */
internal fun cancelBeforeOrigin(all: List<CallingPointResponse>, activationOriginCrs: String?): List<CallingPointResponse> {
    val startIdx = if (activationOriginCrs != null) all.indexOfFirst { it.crs == activationOriginCrs } else -1
    val terminusCrs = all.lastOrNull { it.stopType == "LT" }?.crs
    if (startIdx <= 0 || activationOriginCrs == terminusCrs) return all
    val reason = all.take(startIdx).firstOrNull { it.isCancelled && it.cancelReason != null }?.cancelReason
    return all.mapIndexed { i, cp ->
        if (i < startIdx && !cp.isCancelled && cp.actualTime.isEmpty()) cp.copy(isCancelled = true, cancelReason = reason) else cp
    }
}

/**
 * Spreads cancellations: a service cancelled at origin that never ran is cancelled throughout; a cancellation
 * before any running is carried back to the origin; otherwise calls after a cancellation without an actual
 * time are cancelled too.
 */
internal fun propagateCancellations(raw: List<CallingPointResponse>): List<CallingPointResponse> {
    val loCancel = raw.firstOrNull { it.stopType == "LO" }?.takeIf { it.isCancelled }
    val neverRan = loCancel != null && raw.none { it.actualTime.isNotEmpty() } && raw.none { !it.isCancelled && it.stopType != "LO" }
    val cps = if (neverRan) {
        raw.map { if (!it.isCancelled) it.copy(isCancelled = true, cancelReason = loCancel.cancelReason) else it }
    } else {
        raw
    }
    val firstCancelIdx = cps.indexOfFirst { it.isCancelled && it.stopType != "LT" }
    val cancelledLater = firstCancelIdx >= 0 && cps.drop(firstCancelIdx + 1).any { it.isCancelled }
    if (cancelledLater) {
        val ranBefore = firstCancelIdx > 0 && cps.take(firstCancelIdx).any { it.actualTime.isNotEmpty() }
        if (firstCancelIdx == 0 || ranBefore) return cps
        val reason = cps[firstCancelIdx].cancelReason
        return cps.mapIndexed { idx, cp -> if (idx < firstCancelIdx && !cp.isCancelled) cp.copy(isCancelled = true, cancelReason = reason) else cp }
    }
    var cancelled = false
    var reason: String? = null
    return cps.map { cp ->
        when {
            cp.isCancelled && cp.stopType != "LT" -> {
                cancelled = true
                reason = cp.cancelReason
                cp
            }
            cancelled && !cp.isCancelled && cp.actualTime.isEmpty() -> cp.copy(isCancelled = true, cancelReason = reason)
            else -> cp
        }
    }
}

/** Calls after the latest reported delay, still without an actual time, get a predicted time. */
private fun projectDelays(cps: List<CallingPointResponse>, reportedKeys: Set<String>): List<CallingPointResponse> {
    var lastDelay = 0
    var lastReason: String? = null
    return cps.map { cp ->
        if (cp.actualTime.isNotEmpty() && cp.effectiveTime() in reportedKeys) {
            val late = minutesLate(cp.actualTime, cp.effectiveTime())
            if (late >= 0) {
                lastDelay = late
                if (cp.cancelReason != null) lastReason = cp.cancelReason
            }
            cp
        } else if (cp.actualTime.isEmpty() && lastDelay > 0 && !cp.isCancelled) {
            cp.copy(actualTime = clockTime(cp.effectiveTime().toMinutes() + lastDelay), cancelReason = lastReason, isPredicted = true)
        } else {
            cp
        }
    }
}

/** Train length (vehicles) at each call, from the allocation legs covering it; null when unknown. */
private fun withTrainLength(
    cps: List<CallingPointResponse>,
    legs: List<Allocations.LegSpan>,
    tiplocsInOrder: List<String>,
): List<CallingPointResponse> {
    if (legs.isEmpty()) return cps.map { it.copy(length = null) }
    val minOriginIdx = legs.mapNotNull { leg -> leg.originTiploc?.let { tiplocsInOrder.indexOf(it) }?.takeIf { it >= 0 } }.minOrNull()
    return cps.map { cp ->
        val idx = tiplocsInOrder.indexOf(cp.tiploc)
        if (idx < 0) return@map cp.copy(length = null)
        var total = 0
        var matchedAny = false
        for (leg in legs) {
            val originIdx = leg.originTiploc?.let { tiplocsInOrder.indexOf(it) } ?: -1
            val destIdx = leg.destTiploc?.let { tiplocsInOrder.indexOf(it) } ?: -1
            if (originIdx < 0 || destIdx < 0) continue
            val originMatches = originIdx < idx || (originIdx == idx && idx == minOriginIdx)
            if (originMatches && idx <= destIdx) {
                total += leg.vehicleCount
                matchedAny = true
            }
        }
        cp.copy(length = if (matchedAny) total else null)
    }
}

// ---- Associations ----------------------------------------------------------------------------------------

/**
 * CIF associations, minus links to wholly cancelled trains, plus the forms chain and splits inferred from
 * the units when the CIF has none.
 */
private suspend fun associationsFor(uid: String, svcDate: String, route: ServiceRoute, cps: List<CallingPointResponse>): AssociationInfo {
    val ctx = dbQuery { buildAssociationContext(setOf(uid), svcDate) }
    val stub = ServiceResponse(
        uid = uid,
        headcode = route.headcode,
        atocCode = route.tocCode,
        scheduledTime = cps.firstOrNull { it.stopType == "LO" }?.effectiveTime() ?: "",
        platform = null,
        isPass = false,
        stopType = "",
        originTiploc = "",
        destTiploc = "",
        originCrs = null,
        destCrs = null
    )
    var a = applyAssociations(stub, ctx, svcDate).association
    a.formsUid?.let { forms ->
        if (dbQuery { isUidWhollyCancelled(forms, svcDate) }) a = a.copy(formsUid = null, formsHeadcode = null, formsDate = null)
    }
    a.coupledFromUid?.let { parent ->
        if (dbQuery { isUidWhollyCancelled(parent, svcDate) }) {
            a = a.copy(couplingTiploc = null, couplingTiplocName = null, couplingAssocType = null, coupledFromUid = null, coupledFromHeadcode = null)
        }
    }
    a.formsUid?.let { forms ->
        val chain = walkFormsChain(forms, a.formsDate ?: svcDate)
        chain.firstOrNull()?.let { a = a.copy(formsNextUid = it.uid, formsNextHeadcode = it.headcode, formsNextDate = it.date) }
        if (!isPassengerHeadcode(a.formsHeadcode ?: "")) {
            chain.firstOrNull { isPassengerHeadcode(it.headcode) }?.let {
                a = a.copy(formsNextPassengerUid = it.uid, formsNextPassengerHeadcode = it.headcode, formsNextPassengerDate = it.date)
            }
        }
    }
    // Portions inferred from units, kept when the allocation legs agree that those units divide there.
    val inferred = dbQuery {
        detectUnitBasedSplits(uid, svcDate).filter { candidate ->
            val leaving = candidate.matchedTiploc?.let { unitsDividingAt(uid, svcDate, it) }
            leaving == null || (unitsCsv(candidate.uid, svcDate) ?: "").split(",").map { it.trim() }.any { it in leaving }
        }
    }
    val splits = a.splitToAll + inferred.filter { ic -> a.splitToAll.none { it.uid == ic.uid } }
    if (splits.isNotEmpty()) {
        val enriched = dbQuery {
            splits.map { sc ->
                val (joinsUid, joinsHeadcode) = lookupJoinsInto(sc.uid, svcDate)
                sc.copy(joinsUid = joinsUid, joinsHeadcode = joinsHeadcode, units = unitsCsv(sc.uid, svcDate), coachCount = lookupCoachCount(sc.uid, svcDate))
            }
        }
        val matchedTiploc = inferred.firstOrNull()?.matchedTiploc
        a = a.copy(
            splitToAll = enriched,
            splitTiploc = a.splitTiploc ?: matchedTiploc,
            splitTiplocName = a.splitTiplocName ?: matchedTiploc?.let { route.tiplocNames[it] }
        )
    }
    if (a.splitFromUid == null) {
        dbQuery { detectUnitBasedSplitParent(uid, svcDate) }?.let { a = a.copy(splitFromUid = it.uid, splitFromHeadcode = it.headcode) }
    }
    return a
}

/** Yesterday when a service that ran past midnight yesterday is still within 2 hours of its arrival; else today. */
private suspend fun defaultServiceDate(uid: String): String = dbQuery {
    val now = LocalDateTime.now()
    val today = now.toLocalDate().toString()
    val yesterday = now.toLocalDate().minusDays(1).toString()
    val nowMins = now.hour * 60 + now.minute
    val rows = Schedules.selectAll().where { (Schedules.uid eq uid) and scheduleDateScope(yesterday) }.toList()
    val loMax = rows.filter { it[Schedules.stopType] == "LO" }.maxOfOrNull { it[Schedules.scheduledTime] }
    val ltMax = rows.filter { it[Schedules.stopType] == "LT" }.maxOfOrNull { it[Schedules.scheduledTime] }
    if (loMax != null && ltMax != null && ltMax < loMax && nowMins <= ltMax.toMinutes() + 120) yesterday else today
}

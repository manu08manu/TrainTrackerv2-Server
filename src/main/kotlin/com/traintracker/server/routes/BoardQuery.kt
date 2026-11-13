package com.traintracker.server.routes

import com.traintracker.server.ReasonCodes
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.ScheduleCancellations
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.CANCELLED_ONLY_SQL
import com.traintracker.server.database.SCHEDULE_DATE_SCOPE_SQL
import com.traintracker.server.database.Schedules
import com.traintracker.server.database.effectiveDateFor
import com.traintracker.server.database.placeholders
import com.traintracker.server.database.query
import com.traintracker.server.database.scheduleDateScope
import com.traintracker.server.database.scheduleDateScopeArgs
import com.traintracker.server.database.serviceDayArgs
import com.traintracker.server.database.serviceDaySql
import com.traintracker.server.kafka.LinxMovementWriter
import java.time.LocalDate
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.greaterEq
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.core.lessEq
import org.jetbrains.exposed.v1.core.neq
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.transactions.transaction

internal enum class BoardMode { DEPARTURES, ARRIVALS, ALL }

/** Whether to keep services whose call here is after midnight relative to their own origin ("rolled over"). */
internal enum class RolledFilter { ANY, ONLY, EXCLUDE }

private val BOARD_COLUMNS = listOf(
    Schedules.uid, Schedules.headcode, Schedules.scheduledTime, Schedules.platform, Schedules.isPass, Schedules.stopType,
    Schedules.stpIndicator, Schedules.crs, Schedules.tiploc, Schedules.originTiploc, Schedules.destTiploc,
    Schedules.originCrs, Schedules.destCrs, Schedules.scheduleStartDate, Schedules.tsc, Schedules.scheduledArrival,
    Schedules.scheduledDeparture
)

/**
 * The services calling at [crs] between [windowStart] and [windowEnd] ("HH:mm"; the window may wrap midnight)
 * on [date], with live times, cancellations and platforms. Departure and arrival boards only show activated trains.
 */
internal fun queryBoard(
    crs: String,
    windowStart: String,
    windowEnd: String,
    mode: BoardMode,
    passengerOnly: Boolean = false,
    excludePassing: Boolean = false,
    date: String = LocalDate.now().toString(),
    rolled: RolledFilter = RolledFilter.ANY,
): List<ServiceResponse> {
    val effectiveDate = effectiveDateFor(date)
    val wraps = windowEnd < windowStart
    return transaction {
        var rows = callsInWindow(crs, windowStart, windowEnd, mode, effectiveDate)
        if (wraps) rows = dropCallsBeforeOrigin(rows, windowStart, effectiveDate)
        if (rolled != RolledFilter.ANY && rows.isNotEmpty()) rows = filterRolled(rows, rolled, effectiveDate)
        rows = rows.filter { row ->
            (!excludePassing || !row[Schedules.isPass]) && (!passengerOnly || isPassengerHeadcode(row[Schedules.headcode]))
        }
        val groups = rows.groupBy { it[Schedules.uid] }.values.map { choosePermanentVariant(it) }
        val winners = chooseWinners(groups, crs, windowStart, windowEnd, effectiveDate)
        val sorted = if (wraps) winners.sortedFromClock(windowStart) { it[Schedules.scheduledTime] } else winners.sortedBy { it[Schedules.scheduledTime] }
        boardServices(sorted, crs, mode, date, windowStart, windowEnd)
    }
}

/** Schedule rows at [crs] in the time window, of the kinds the board mode shows. */
private fun callsInWindow(crs: String, windowStart: String, windowEnd: String, mode: BoardMode, effectiveDate: String): List<ResultRow> =
    Schedules.select(BOARD_COLUMNS).where {
        val inWindow = if (windowEnd < windowStart) {
            (Schedules.scheduledTime greaterEq windowStart) or (Schedules.scheduledTime lessEq windowEnd)
        } else {
            (Schedules.scheduledTime greaterEq windowStart) and (Schedules.scheduledTime lessEq windowEnd)
        }
        scheduleDateScope(effectiveDate) and (Schedules.crs inList listOf(crs)) and inWindow and
            when (mode) {
                BoardMode.ARRIVALS -> Schedules.stopType neq "LO"
                BoardMode.DEPARTURES -> Schedules.stopType neq "LT"
                BoardMode.ALL -> Op.TRUE
            }
    }.orderBy(Schedules.scheduledTime, SortOrder.ASC).toList()

/** uid -> scheduled time at its origin (LO). */
private fun JdbcTransaction.originTimes(uids: Collection<String>, effectiveDate: String): Map<String, String> {
    val times = mutableMapOf<String, String>()
    query(
        "SELECT uid, MAX(scheduled_time) AS lo FROM schedules WHERE uid IN (${placeholders(uids.size)}) AND stop_type='LO' AND $SCHEDULE_DATE_SCOPE_SQL GROUP BY uid",
        uids.toList() + scheduleDateScopeArgs(effectiveDate)
    ) { rs ->
        while (rs.next()) times[rs.getString("uid") ?: continue] = rs.getString("lo") ?: ""
    }
    return times
}

/** In a window that wraps midnight, drops after-midnight calls of trains that only start after them. */
private fun JdbcTransaction.dropCallsBeforeOrigin(rows: List<ResultRow>, windowStart: String, effectiveDate: String): List<ResultRow> {
    val afterMidnight = rows.filter { it[Schedules.scheduledTime] < windowStart }.map { it[Schedules.uid] }.toSet()
    if (afterMidnight.isEmpty()) return rows
    val loTimes = originTimes(afterMidnight, effectiveDate)
    return rows.filter { r ->
        val t = r[Schedules.scheduledTime]
        !(t < windowStart && (loTimes[r[Schedules.uid]] ?: "") <= t)
    }
}

/** A call is rolled over when its time is earlier than its train's origin time. */
private fun JdbcTransaction.filterRolled(rows: List<ResultRow>, rolled: RolledFilter, effectiveDate: String): List<ResultRow> {
    val loTimes = originTimes(rows.map { it[Schedules.uid] }.toSet(), effectiveDate)
    return rows.filter { r ->
        val lo = loTimes[r[Schedules.uid]] ?: ""
        val isRolled = lo.isNotEmpty() && r[Schedules.scheduledTime] < lo
        if (rolled == RolledFilter.ONLY) isRolled else !isRolled
    }
}

/**
 * When a uid has several permanent (P) variants and an overlay (O), keeps only the P variant whose origin
 * time is the latest one not after the overlay's first call (else the earliest-starting variant).
 */
private fun choosePermanentVariant(rows: List<ResultRow>): List<ResultRow> {
    val pVariants = rows.filter { it[Schedules.stpIndicator] == 'P' }.groupBy { it[Schedules.scheduleStartDate] }
    if (pVariants.size <= 1) return rows
    val oRows = rows.filter { it[Schedules.stpIndicator] == 'O' }
    if (oRows.isEmpty()) return rows
    val oFirstTime = oRows.minOf { it[Schedules.scheduledTime] }
    val bestStartDate = pVariants.entries.mapNotNull { (startDate, pRows) ->
        val loTime = pRows.firstOrNull { it[Schedules.stopType] == "LO" }?.get(Schedules.scheduledTime) ?: return@mapNotNull null
        startDate to loTime
    }.filter { (_, loTime) -> loTime <= oFirstTime }
        .maxByOrNull { (_, loTime) -> loTime }?.first ?: pVariants.keys.minOrNull()
    val bestPRows = pVariants[bestStartDate] ?: rows.filter { it[Schedules.stpIndicator] == 'P' }
    return bestPRows + rows.filter { it[Schedules.stpIndicator] != 'P' }
}

/**
 * One row per train to show. The strongest STP variant wins, except that a permanent row is dropped when the
 * train is cancelled today, and replaced by its overlay's call here when the overlay still calls here.
 */
private fun JdbcTransaction.chooseWinners(
    groups: List<List<ResultRow>>,
    crs: String,
    windowStart: String,
    windowEnd: String,
    effectiveDate: String,
): List<ResultRow> {
    val safeCrs = crs.filter { it.isLetterOrDigit() }
    val scope = scheduleDateScopeArgs(effectiveDate)
    val bests = groups.mapNotNull { rows -> rows.minByOrNull { stpRank(it) } }

    // Permanent winners that also have an overlay/VSTP or a cancellation today.
    val pUids = bests.filter { it[Schedules.stpIndicator] == 'P' }.map { it[Schedules.uid] }
    val uidsWithOverride = mutableSetOf<String>()
    val uidsCancelled = mutableSetOf<String>()
    if (pUids.isNotEmpty()) {
        query(
            "SELECT DISTINCT uid, stp_indicator FROM schedules WHERE uid IN (${placeholders(pUids.size)}) AND stp_indicator IN ('O','N','C') AND $SCHEDULE_DATE_SCOPE_SQL",
            pUids + scope
        ) { rs ->
            while (rs.next()) {
                val uid = rs.getString("uid") ?: continue
                when (rs.getString("stp_indicator")) {
                    "O", "N" -> uidsWithOverride.add(uid)
                    "C" -> uidsCancelled.add(uid)
                }
            }
        }
    }
    val winnerRows = mutableListOf<ResultRow>()
    val pFallbackUids = mutableSetOf<String>()
    for (best in bests) {
        val uid = best[Schedules.uid]
        when {
            best[Schedules.stpIndicator] == 'C' -> {}
            best[Schedules.stpIndicator] == 'P' && uid in uidsCancelled -> {}
            best[Schedules.stpIndicator] == 'P' && uid in uidsWithOverride -> pFallbackUids.add(uid)
            else -> winnerRows.add(best)
        }
    }

    // A winner must call here in its own STP variant, and an overlay must not have moved its origin or terminus past this call.
    val validUids = mutableSetOf<String>()
    if (winnerRows.isNotEmpty()) {
        val winnerStp = winnerRows.associate { it[Schedules.uid] to it[Schedules.stpIndicator] }
        query(
            "SELECT DISTINCT uid, stp_indicator FROM schedules WHERE uid IN (${placeholders(winnerStp.size)}) AND crs=? AND $SCHEDULE_DATE_SCOPE_SQL",
            winnerStp.keys.toList() + safeCrs + scope
        ) { rs ->
            while (rs.next()) {
                val uid = rs.getString("uid") ?: continue
                val stp = rs.getString("stp_indicator")?.firstOrNull() ?: continue
                if (stp == winnerStp[uid]) validUids.add(uid)
            }
        }
    }
    if (validUids.isNotEmpty()) {
        val pTimeHere = mutableMapOf<String, String>()
        val oLtTime = mutableMapOf<String, String>()
        val oLoTime = mutableMapOf<String, String>()
        query(
            "SELECT uid, stp_indicator, stop_type, scheduled_time FROM schedules WHERE uid IN (${placeholders(validUids.size)}) AND ((stp_indicator='P' AND crs=?) OR (stp_indicator='O' AND stop_type IN ('LT','LO'))) AND $SCHEDULE_DATE_SCOPE_SQL",
            validUids.toList() + safeCrs + scope
        ) { rs ->
            while (rs.next()) {
                val uid = rs.getString("uid") ?: continue
                val t = rs.getString("scheduled_time") ?: continue
                val stp = rs.getString("stp_indicator") ?: continue
                val stop = rs.getString("stop_type") ?: continue
                if (stp == "P") {
                    pTimeHere[uid] = t
                } else {
                    when (stop) {
                        "LT" -> oLtTime[uid] = t
                        "LO" -> oLoTime[uid] = t
                    }
                }
            }
        }
        validUids -= validUids.filter { uid -> overlayMissesCall(pTimeHere[uid], oLtTime[uid], oLoTime[uid]) }.toSet()
    }

    val pFallbackRows = if (pFallbackUids.isNotEmpty()) permanentFallbacks(pFallbackUids, safeCrs, scope) else emptyList()
    val combined = (winnerRows.filter { it[Schedules.uid] in validUids } + pFallbackRows).sortedBy { it[Schedules.scheduledTime] }

    // A permanent intermediate call within a minute of a VSTP train starting here is the same train: drop it.
    val vstpStarts = mutableSetOf<String>()
    query(
        "SELECT scheduled_time FROM schedules WHERE stp_indicator='N' AND stop_type='LO' AND crs=? AND scheduled_time >= ? AND scheduled_time <= ? AND $SCHEDULE_DATE_SCOPE_SQL",
        listOf(safeCrs, windowStart, windowEnd) + scope
    ) { rs ->
        while (rs.next()) {
            val t = rs.getString("scheduled_time") ?: break
            val hhmm = t.replace(":", "").toIntOrNull() ?: break
            val base = (hhmm / 100) * 60 + hhmm % 100
            for (delta in -1..1) vstpStarts.add("$safeCrs|${clockTime(base + delta)}")
        }
    }
    return combined.filter { row ->
        row[Schedules.stpIndicator] != 'P' || row[Schedules.stopType] != "LI" ||
            "$safeCrs|${row[Schedules.scheduledTime]}" !in vstpStarts
    }
}

/** True when the overlay ends at or before the permanent call time here, or starts after it. */
private fun overlayMissesCall(pTime: String?, overlayEnd: String?, overlayStart: String?): Boolean {
    val pMins = pTime?.toMinutes() ?: return false
    if (overlayEnd != null && wrapAwareMinutes(overlayEnd, pMins) <= pMins) return true
    return overlayStart != null && wrapAwareMinutes(overlayStart, pMins) > pMins
}

/** Minutes of [t], moved to the next day when more than 12 hours before [anchorMinutes]. */
private fun wrapAwareMinutes(t: String, anchorMinutes: Int): Int {
    val m = t.toMinutes()
    return if (m < anchorMinutes - 720) m + 1440 else m
}

/**
 * Permanent rows to show for trains whose overlay/VSTP does not itself call here: kept unless the override
 * calls here, moved the origin, or no longer reaches this call.
 */
private fun JdbcTransaction.permanentFallbacks(uids: Set<String>, safeCrs: String, scope: List<String>): List<ResultRow> {
    val overrideCallsHere = mutableSetOf<String>()
    val overrideMovedOrigin = mutableSetOf<String>()
    query(
        "SELECT DISTINCT uid, stop_type, crs FROM schedules WHERE uid IN (${placeholders(uids.size)}) AND stp_indicator IN ('O','N') AND (crs=? OR stop_type='LO') AND $SCHEDULE_DATE_SCOPE_SQL",
        uids.toList() + safeCrs + scope
    ) { rs ->
        while (rs.next()) {
            val uid = rs.getString("uid") ?: continue
            val stop = rs.getString("stop_type") ?: continue
            val c = rs.getString("crs") ?: ""
            if (c == safeCrs) overrideCallsHere.add(uid)
            if (stop == "LO" && c != safeCrs) overrideMovedOrigin.add(uid)
        }
    }
    val candidates = uids - overrideCallsHere - overrideMovedOrigin
    val noLongerCallsHere = mutableSetOf<String>()
    if (candidates.isNotEmpty()) {
        val pTimeHere = mutableMapOf<String, String>()
        val oLtTime = mutableMapOf<String, String>()
        val oFirstTime = mutableMapOf<String, String>()
        query(
            "SELECT uid, stp_indicator, stop_type, scheduled_time FROM schedules WHERE uid IN (${placeholders(candidates.size)}) AND ((stp_indicator='P' AND crs=?) OR stp_indicator='O') AND $SCHEDULE_DATE_SCOPE_SQL",
            candidates.toList() + safeCrs + scope
        ) { rs ->
            while (rs.next()) {
                val uid = rs.getString("uid") ?: continue
                val t = rs.getString("scheduled_time") ?: continue
                val stp = rs.getString("stp_indicator") ?: continue
                val stop = rs.getString("stop_type") ?: continue
                if (stp == "P") {
                    pTimeHere[uid] = t
                } else {
                    if (stop == "LT") oLtTime[uid] = t
                    if (oFirstTime[uid] == null || t < oFirstTime[uid]!!) oFirstTime[uid] = t
                }
            }
        }
        candidates.filterTo(noLongerCallsHere) { overlayMissesCall(pTimeHere[it], oLtTime[it], oFirstTime[it]) }
    }
    return (uids - overrideCallsHere - overrideMovedOrigin - noLongerCallsHere).mapNotNull { uid ->
        Schedules.select(BOARD_COLUMNS).where {
            (Schedules.uid eq uid) and (Schedules.stpIndicator eq 'P') and (Schedules.crs eq safeCrs)
        }.firstOrNull()
    }
}

/** Builds the board rows: reported actual or predicted times, cancellations, platforms, origin and destination. */
private fun JdbcTransaction.boardServices(
    rows: List<ResultRow>,
    crs: String,
    mode: BoardMode,
    date: String,
    windowStart: String,
    windowEnd: String,
): List<ServiceResponse> {
    val safeCrs = crs.filter { it.isLetterOrDigit() }
    val uids = rows.map { it[Schedules.uid] }.toSet()
    // "uid|scheduled time" -> latest (actual time, cancelled, reason); "uid" -> latest cancellation
    val moves = mutableMapOf<String, Triple<String, Boolean, String?>>()
    val moveReasons = mutableMapOf<String, String?>()
    val lastDelay = mutableMapOf<String, Int>()   // uid -> minutes late at its latest reported call
    val running = mutableSetOf<String>()          // uids with any reported (not cancelled) movement
    if (uids.isNotEmpty()) {
        try {
            val args = uids.toList() + serviceDayArgs(date)
            // When a train ID has only cancellations, use that train ID's events alone.
            val cancelledOnlyTrainId = mutableMapOf<String, String>()
            query(
                "SELECT uid, train_id FROM movements WHERE uid IN (${placeholders(uids.size)}) AND ${serviceDaySql("event_ts")} GROUP BY uid, train_id HAVING $CANCELLED_ONLY_SQL",
                args
            ) { rs ->
                while (rs.next()) cancelledOnlyTrainId[rs.getString("uid") ?: continue] = rs.getString("train_id") ?: continue
            }
            query(
                "SELECT uid, train_id, scheduled_time, actual_time, is_cancelled, cancel_reason, event_type FROM movements WHERE uid IN (${placeholders(uids.size)}) AND ${serviceDaySql("event_ts")} ORDER BY event_ts DESC",
                args
            ) { rs ->
                while (rs.next()) {
                    val uid = rs.getString("uid")?.takeIf { it.isNotEmpty() } ?: continue
                    val expectedTrainId = cancelledOnlyTrainId[uid]
                    if (expectedTrainId != null && (rs.getString("train_id") ?: "") != expectedTrainId) continue
                    val scht = rs.getString("scheduled_time") ?: continue
                    val actualTime = rs.getString("actual_time") ?: ""
                    val isCancelled = rs.getBoolean("is_cancelled")
                    val reasonCode = rs.getString("cancel_reason")
                    val entry = Triple(actualTime, isCancelled, ReasonCodes.resolve(reasonCode))
                    val stopKey = "$uid|$scht"
                    moves.putIfAbsent(stopKey, entry)
                    if (!moveReasons.containsKey(stopKey)) moveReasons[stopKey] = reasonCode
                    // A change-of-origin cancellation cancels its own stop only, not the rest of the run.
                    if (isCancelled && rs.getString("event_type") != LinxMovementWriter.COO_CANCELLATION) {
                        moves.putIfAbsent(uid, entry)
                        if (!moveReasons.containsKey(uid)) moveReasons[uid] = reasonCode
                    }
                    if (!isCancelled && actualTime.isNotEmpty()) running += uid
                    if (!isCancelled && actualTime.isNotEmpty() && uid !in lastDelay) {
                        val late = minutesLate(actualTime, scht)
                        if (late >= 0) lastDelay[uid] = late
                    }
                }
            }
        } catch (e: Exception) {
        }
    }
    val overrides = originDestOverrides(uids, date)
    val darwinDb = AppDatabase.darwinPlatforms(uids, date)
    val movementPlatforms = AppDatabase.movementPlatforms(uids, date)
    val activatedUids = AppDatabase.activatedUids(uids, date)
    val services = rows.mapNotNull { row ->
        val uid = row[Schedules.uid]
        val scht = row[Schedules.scheduledTime]
        val stopKey = "$uid|$scht"
        val stopEntry = moves[stopKey]
        val uidCancel = moves[uid]?.takeIf { it.second }
        val cancelEntry = stopEntry ?: uidCancel
        val cancelReasonCode = when {
            stopEntry != null -> moveReasons[stopKey]
            uidCancel != null -> moveReasons[uid]
            else -> null
        }
        val actualTime = stopEntry?.first ?: ""
        val reportedCancelled = (cancelEntry?.second ?: false) || row[Schedules.stpIndicator] == 'C'
        val timetableCancelled = !reportedCancelled && actualTime.isEmpty() && uid !in lastDelay &&
            ScheduleCancellations.isCancelled(uid, date)
        val isCancelled = reportedCancelled || timetableCancelled
        // Planned cancellations (PG/PD) and trains not activated today are left off departure and arrival boards.
        if (mode != BoardMode.ALL && ((isCancelled && cancelReasonCode in setOf("PG", "PD")) || uid !in activatedUids)) {
            return@mapNotNull null
        }
        val delay = lastDelay[uid]
        val predicted = actualTime.isEmpty() && !isCancelled && delay != null && delay > 0
        val (platform, confirmed) = resolvePlatform(
            uid, row[Schedules.tiploc], row[Schedules.crs], row[Schedules.platform], darwinDb[uid to row[Schedules.tiploc]], movementPlatforms
        )
        val (originTiploc, originCrs) = overrides.origin(row)
        val (destTiploc, destCrs) = overrides.dest(row)
        scheduledServiceResponse(
            row = row,
            platform = platform,
            platformConfirmed = confirmed,
            origin = originTiploc to originCrs,
            destination = destTiploc to destCrs,
            actualTime = if (predicted) clockTime(scht.toMinutes() + delay) else actualTime,
            isCancelled = isCancelled,
            cancelReason = cancelEntry?.third ?: if (timetableCancelled) TIMETABLE_CANCELLED else null,
            isPredicted = predicted,
            isRunning = uid in running,
            link = "/?service=$uid&crs=$safeCrs&date=$date",
            serviceDate = date,
        )
    }.withResolvedNames()
    // Order by the time shown: scheduled when cancelled, else actual or predicted.
    val wrapsMidnight = windowEnd < windowStart
    val windowStartMins = windowStart.toMinutes()
    return services.sortedBy { service ->
        val shown = if (service.isCancelled) service.scheduledTime else service.actualTime.ifEmpty { service.scheduledTime }
        val mins = shown.toMinutes()
        if (wrapsMidnight && mins < windowStartMins) mins + 1440 else mins
    }
}

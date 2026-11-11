package com.traintracker.server.routes

import com.traintracker.server.Retention

import com.traintracker.server.ReasonCodes
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.ScheduleCancellations
import com.traintracker.server.database.query
import com.traintracker.server.database.queryFirst
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import java.time.LocalDate
import java.time.LocalDateTime
import kotlinx.serialization.Serializable
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction

/** How a service ran on one day. */
@Serializable
data class ServiceHistoryDay(
    val date: String,
    /** early, ontime, late, partcancelled, cancelled, notactivated, planned (cancelled in the timetable), notscheduled, nodata */
    val status: String,
    /** Minutes late (negative when early) at the last reported call. */
    val delay: Int? = null,
    /** Where [delay] was measured. */
    val at: String? = null,
    val reason: String? = null,
)

@Serializable
data class ServiceHistoryResponse(
    val uid: String,
    val from: String,
    val to: String,
    val daysScheduled: Int,
    val daysRun: Int,
    val early: Int,
    val onTime: Int,
    val late: Int,
    val lateMinor: Int,
    val lateModerate: Int,
    val lateSevere: Int,
    val partCancelled: Int,
    val cancelled: Int,
    val notActivated: Int,
    val plannedCancelled: Int,
    val noData: Int,
    val averageDelay: Double? = null,
    val days: List<ServiceHistoryDay>,
)

private class ScheduleWindow(val stp: Char, val start: String, val end: String, val days: String) {
    fun covers(date: LocalDate): Boolean {
        val d = date.toString()
        return start <= d && d <= end && days.getOrNull(date.dayOfWeek.value - 1) == '1'
    }
}

/** One TRUST train ID: an activation or the movements reported under it. */
private class Run {
    var activated = false
    var firstSeen: LocalDateTime? = null
    var lastReport: Triple<LocalDateTime, Int, String>? = null // (time, minutes late, stanox)
    var ranSomewhere = false
    var cancelled = false
    var cancelReason: String? = null
}

private const val HISTORY_DAYS = Retention.SERVICE_HISTORY_DAYS

/** "2026-11-10 11:43:49.579" -> LocalDateTime, or null. */
private fun parseTs(raw: String?): LocalDateTime? = try {
    raw?.takeIf { it.length >= 19 }?.let { LocalDateTime.parse(it.take(19).replace(' ', 'T')) }
} catch (e: Exception) {
    null
}

/**
 * The service date of a TRUST train ID first seen at [ts]: the date ending in the train ID's day of month whose
 * service day (12 hours before to 36 hours after its start) contains [ts].
 */
private fun serviceDateOf(trainId: String, ts: LocalDateTime): LocalDate? {
    val dom = trainId.takeLast(2).toIntOrNull() ?: return null
    val day = ts.toLocalDate()
    return listOf(day, day.minusDays(1), day.plusDays(1)).firstOrNull { d ->
        d.dayOfMonth == dom && !ts.isBefore(d.atStartOfDay().minusHours(12)) && ts.isBefore(d.atStartOfDay().plusHours(36))
    }
}

/** How the service with [uid] ran on each of the last [HISTORY_DAYS] full days. */
internal fun JdbcTransaction.serviceHistory(uid: String): ServiceHistoryResponse {
    val today = LocalDate.now()
    val from = today.minusDays(HISTORY_DAYS.toLong())
    val to = today.minusDays(1)

    val windows = mutableListOf<ScheduleWindow>()
    query(
        "SELECT DISTINCT stp_indicator, schedule_start_date, schedule_end_date, days_run FROM schedules WHERE uid = ? AND stop_type = 'LO'",
        listOf(uid)
    ) { rs ->
        while (rs.next()) {
            val stp = rs.getString(1)?.firstOrNull() ?: continue
            windows += ScheduleWindow(stp, rs.getString(2) ?: "", rs.getString(3) ?: "", rs.getString(4) ?: "1111111")
        }
    }

    val runs = mutableMapOf<String, Run>()
    val since = from.atStartOfDay().minusHours(12).toString().replace('T', ' ')
    val until = today.atStartOfDay().plusHours(36).toString().replace('T', ' ')
    query(
        "SELECT train_id, updated_at FROM train_activations WHERE uid = ? AND updated_at >= ? AND updated_at < ?",
        listOf(uid, since, until)
    ) { rs ->
        while (rs.next()) {
            val trainId = rs.getString(1) ?: continue
            val ts = parseTs(rs.getString(2)) ?: continue
            val run = runs.getOrPut(trainId) { Run() }
            run.activated = true
            if (run.firstSeen?.isAfter(ts) != false) run.firstSeen = ts
        }
    }
    query(
        "SELECT train_id, stanox, scheduled_time, actual_time, is_cancelled, cancel_reason, event_ts FROM movements WHERE uid = ? AND event_ts >= ? AND event_ts < ? ORDER BY event_ts",
        listOf(uid, since, until)
    ) { rs ->
        while (rs.next()) {
            val trainId = rs.getString("train_id") ?: continue
            val ts = parseTs(rs.getString("event_ts")) ?: continue
            val run = runs.getOrPut(trainId) { Run() }
            if (run.firstSeen?.isAfter(ts) != false) run.firstSeen = ts
            if (rs.getBoolean("is_cancelled")) {
                run.cancelled = true
                run.cancelReason = ReasonCodes.resolve(rs.getString("cancel_reason")) ?: run.cancelReason
                continue
            }
            val sched = rs.getString("scheduled_time").orEmpty()
            val actual = rs.getString("actual_time").orEmpty()
            if (actual.isEmpty()) continue
            run.ranSomewhere = true
            if (sched.isNotEmpty()) run.lastReport = Triple(ts, minutesLate(actual, sched), rs.getString("stanox").orEmpty())
        }
    }

    // Merge train IDs (a train can be re-activated) into service days.
    val byDate = mutableMapOf<LocalDate, MutableList<Run>>()
    runs.forEach { (trainId, run) ->
        val date = run.firstSeen?.let { serviceDateOf(trainId, it) } ?: return@forEach
        byDate.getOrPut(date) { mutableListOf() } += run
    }

    // STANOX -> name of the service's own timetabled locations, for "at" below.
    val stanoxNames = mutableMapOf<String, String>()
    query(
        "SELECT p.stanox, p.tiploc, p.name FROM pif_loc p JOIN (SELECT DISTINCT tiploc FROM schedules WHERE uid = ?) s ON s.tiploc = p.tiploc WHERE p.stanox IS NOT NULL AND p.stanox != ''",
        listOf(uid)
    ) { rs ->
        while (rs.next()) {
            val stanox = rs.getString(1) ?: continue
            val name = CorpusLookup.nameFromTiploc(rs.getString(2) ?: "") ?: rs.getString(3)?.takeIf { it.isNotEmpty() } ?: continue
            stanoxNames.putIfAbsent(stanox, name)
        }
    }
    fun stanoxName(stanox: String): String? = stanoxNames[stanox]
        ?: CorpusLookup.tiplocFromStanox(stanox)?.let { CorpusLookup.nameFromTiploc(it) }

    // Days before movement data begins cannot be judged.
    val dataStart = queryFirst("SELECT MIN(event_ts) FROM movements") { parseTs(it.getString(1)) }
        ?.toLocalDate()?.plusDays(1) ?: from

    val days = generateSequence(from) { it.plusDays(1) }.takeWhile { !it.isAfter(to) }.map { date ->
        val dayRuns = byDate[date].orEmpty()
        val best = windows.filter { it.covers(date) }.minByOrNull { STP_PRIORITY[it.stp] ?: 99 }
        val ran = dayRuns.any { it.ranSomewhere }
        val anyCancel = dayRuns.any { it.cancelled }
        val reason = dayRuns.firstNotNullOfOrNull { it.cancelReason }
        val scheduled = (best != null && best.stp != 'C') || dayRuns.isNotEmpty()
        val plannedCancel = !ran && dayRuns.isEmpty() && (best?.stp == 'C' || ScheduleCancellations.isCancelled(uid, date.toString()))
        when {
            date.isBefore(dataStart) -> ServiceHistoryDay(date.toString(), "nodata")
            plannedCancel -> ServiceHistoryDay(date.toString(), "planned", reason = TIMETABLE_CANCELLED)
            !scheduled -> ServiceHistoryDay(date.toString(), "notscheduled")
            ran && anyCancel -> ServiceHistoryDay(date.toString(), "partcancelled", reason = reason)
            ran -> {
                val last = dayRuns.mapNotNull { it.lastReport }.maxByOrNull { it.first }
                val delay = last?.second ?: 0
                val at = last?.third?.takeIf { it.isNotEmpty() }?.let { stanoxName(it) }
                val status = when {
                    delay < 0 -> "early"
                    delay == 0 -> "ontime"
                    else -> "late"
                }
                ServiceHistoryDay(date.toString(), status, delay = last?.second, at = at)
            }
            anyCancel -> ServiceHistoryDay(date.toString(), "cancelled", reason = reason)
            else -> ServiceHistoryDay(date.toString(), "notactivated")
        }
    }.toList()

    fun n(status: String) = days.count { it.status == status }
    val late = days.filter { it.status == "late" }
    val ranDays = days.filter { it.status in setOf("early", "ontime", "late") }
    return ServiceHistoryResponse(
        uid = uid,
        from = from.toString(),
        to = to.toString(),
        daysScheduled = days.count { it.status !in setOf("notscheduled", "planned", "nodata") },
        daysRun = ranDays.size + n("partcancelled"),
        early = n("early"),
        onTime = n("ontime"),
        late = late.size,
        lateMinor = late.count { (it.delay ?: 0) in 1..4 },
        lateModerate = late.count { (it.delay ?: 0) in 5..14 },
        lateSevere = late.count { (it.delay ?: 0) >= 15 },
        partCancelled = n("partcancelled"),
        cancelled = n("cancelled"),
        notActivated = n("notactivated"),
        plannedCancelled = n("planned"),
        noData = n("nodata"),
        averageDelay = ranDays.mapNotNull { it.delay }.takeIf { it.isNotEmpty() }?.average()?.let { Math.round(it * 10) / 10.0 },
        days = days,
    )
}

fun Route.serviceHistoryRoutes() {
    get("/service/{uid}/history") {
        val uid = call.pathParam("uid").filter { it.isLetterOrDigit() }.take(6)
        call.respond(dbQuery { serviceHistory(uid) })
    }
}

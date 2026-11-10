package com.traintracker.server.routes

import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.ScheduleCancellations
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.SCHEDULE_DATE_SCOPE_SQL
import com.traintracker.server.database.activationsFor
import com.traintracker.server.database.execUpdate
import com.traintracker.server.database.placeholders
import com.traintracker.server.database.query
import com.traintracker.server.database.queryFirst
import com.traintracker.server.database.queryList
import com.traintracker.server.database.scheduleDateScopeArgs
import com.traintracker.server.database.serviceDayArgs
import com.traintracker.server.database.serviceDaySql
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import java.time.LocalDate
import java.time.LocalDateTime
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("Punctuality")

/*
 * Punctuality of passenger calls over the last 30 days, by station and by operator.
 *
 * Each finished service day is worked out once ([Punctuality.buildDay]) with the service page's own rules:
 * the day's timetable (strongest STP variant, passes left out), the movements reported against it, and the
 * same cancellation logic (spreading, change of origin). Every call is counted at the station it calls at,
 * by its departure (arrival at the terminus), in the bands the site uses everywhere: early, on time
 * (0 min), 1-4, 5-14 and 15+ minutes late, cancelled, or not reported. Counts are stored per station,
 * operator and half-hour of the scheduled time, so a card for any timeframe is a quick sum.
 */

/** Outcome of one call: [status] is early, ontime, late, cancelled or noreport. */
internal data class CallOutcome(val crs: String, val slotTime: String, val status: String, val delay: Int?)

/** A reported movement of one service: crs, scheduled and actual time, event type, cancellation. */
internal data class ReportedMovement(
    val stanox: String?, val crs: String?, val scheduledTime: String?, val actualTime: String?, val eventType: String,
    val isCancelled: Boolean, val cancelReason: String?,
)

/** A timetabled stop (not a pass) of one service. */
internal data class TimetabledStop(
    val crs: String, val stanox: String?, val scheduledTime: String, val scheduledArrival: String?, val scheduledDeparture: String?, val stopType: String,
)

private val CRS_ALIASES = mapOf("PDX" to "PAD", "FDX" to "FOG", "LSX" to "LST", "ABX" to "ABW", "WHX" to "WWC")

/** Minutes between two "HH:MM" times, either way round, across midnight. */
private fun gapMinutes(a: String, b: String): Int = kotlin.math.abs(minutesLate(a, b))

/** How far a reported scheduled time may sit from the timetable's: WTT and public times differ by a few minutes. */
private const val MATCH_WINDOW_MINUTES = 10

/**
 * How each stop of one service went, using the service page's rules for cancellations ([applyCancellations],
 * and [cancelBeforeOrigin] for stops before a later activation origin [activationOriginCrs]).
 *
 * A stop takes the movement of the right kind at its location whose scheduled time is nearest its own,
 * within [MATCH_WINDOW_MINUTES]: movements carry working-timetable times, the timetable rows public ones
 * (a terminus can be 02:12 in one and 02:14 in the other). Location is the STANOX where known, as one
 * STANOX can carry another station's CRS (Clapham Junction reports as XCP), else the CRS.
 */
internal fun callOutcomes(stops: List<TimetabledStop>, moves: List<ReportedMovement>, activationOriginCrs: String?): List<CallOutcome> {
    fun crsKey(crs: String) = "C:${CRS_ALIASES[crs] ?: crs}"
    val byLocation = HashMap<String, MutableList<ReportedMovement>>()
    for (m in moves) {
        if (m.scheduledTime.isNullOrEmpty()) continue
        m.stanox?.let { byLocation.getOrPut("S:$it") { mutableListOf() } += m }
        m.crs?.let { byLocation.getOrPut(crsKey(it)) { mutableListOf() } += m }
    }
    fun TimetabledStop.nearest(time: String, accept: (ReportedMovement) -> Boolean): ReportedMovement? {
        val here = stanox?.let { byLocation["S:$it"] } ?: byLocation[crsKey(crs)] ?: return null
        return here.filter(accept)
            .map { it to gapMinutes(it.scheduledTime!!, time) }
            .filter { it.second <= MATCH_WINDOW_MINUTES }
            .minByOrNull { it.second }?.first
    }
    val cooStops = HashSet<CallingPointResponse>()
    val cps = stops.map { s ->
        val dep = s.nearest(s.scheduledDeparture ?: s.scheduledTime) { !it.isCancelled && it.eventType == "DEPARTURE" && !it.actualTime.isNullOrEmpty() }
        val arr = s.nearest(s.scheduledArrival ?: s.scheduledTime) { !it.isCancelled && it.eventType == "ARRIVAL" && !it.actualTime.isNullOrEmpty() }
        val cancel = s.nearest(s.scheduledTime) { it.isCancelled }
        CallingPointResponse(
            tiploc = s.crs, crs = s.crs, name = null, scheduledTime = s.scheduledTime,
            scheduledArrival = s.scheduledArrival, scheduledDeparture = s.scheduledDeparture,
            platform = null, isPass = false, stopType = s.stopType,
            actualTime = dep?.actualTime ?: arr?.actualTime ?: "", actualArrival = arr?.actualTime ?: "", actualDeparture = dep?.actualTime ?: "",
            isCancelled = cancel != null, cancelReason = cancel?.cancelReason,
        ).also { if (cancel?.eventType == "COO_CANCELLED") cooStops += it }
    }
    val shown = cancelBeforeOrigin(applyCancellations(cps) { it in cooStops }, activationOriginCrs)
    return shown.map { cp ->
        val terminus = cp.stopType == "LT"
        val slotTime = (if (terminus) cp.scheduledArrival else cp.scheduledDeparture) ?: cp.scheduledTime
        val delay = when {
            cp.isCancelled -> null
            !terminus && cp.actualDeparture.isNotEmpty() -> minutesLate(cp.actualDeparture, cp.scheduledDeparture ?: cp.scheduledTime)
            cp.actualArrival.isNotEmpty() -> minutesLate(cp.actualArrival, cp.scheduledArrival ?: cp.scheduledTime)
            else -> null
        }
        val status = when {
            cp.isCancelled -> "cancelled"
            delay == null -> "noreport"
            delay < 0 -> "early"
            delay == 0 -> "ontime"
            else -> "late"
        }
        CallOutcome(cp.crs!!, slotTime, status, delay)
    }
}

/** Counts for one station, operator and half-hour on one day (or summed over many). */
internal class PunctualityCounts {
    var total = 0
    var early = 0
    var onTime = 0
    var lateMinor = 0
    var lateModerate = 0
    var lateSevere = 0
    var cancelled = 0
    var noReport = 0
    var delaySum = 0L

    fun add(o: CallOutcome) {
        total++
        when (o.status) {
            "early" -> early++
            "ontime" -> onTime++
            "late" -> when (o.delay ?: 0) {
                in 1..4 -> lateMinor++
                in 5..14 -> lateModerate++
                else -> lateSevere++
            }
            "cancelled" -> cancelled++
            else -> noReport++
        }
        o.delay?.let { delaySum += it }
    }

    /** Adds a row read with [SUMS] starting at column [from]. */
    fun addSums(rs: java.sql.ResultSet, from: Int) {
        total += rs.getInt(from); early += rs.getInt(from + 1); onTime += rs.getInt(from + 2); lateMinor += rs.getInt(from + 3)
        lateModerate += rs.getInt(from + 4); lateSevere += rs.getInt(from + 5); cancelled += rs.getInt(from + 6)
        noReport += rs.getInt(from + 7); delaySum += rs.getLong(from + 8)
    }

    val reported get() = early + onTime + lateMinor + lateModerate + lateSevere

    /** Mean minutes late of reported calls, to one decimal place. */
    val averageDelay get() = if (reported > 0) Math.round(delaySum * 10.0 / reported) / 10.0 else null

    companion object {
        /** The punctuality_daily columns summed, in the order [addSums] reads them. */
        const val SUMS = "sum(total), sum(early), sum(on_time), sum(late_minor), sum(late_moderate), sum(late_severe), sum(cancelled), sum(no_report), sum(delay_sum)"
    }
}

@Serializable
data class PunctualityResponse(
    val crs: String? = null,
    val toc: String? = null,
    /** First and last service date counted (only finished days are counted). */
    val fromDate: String? = null,
    val toDate: String? = null,
    val days: Int,
    /** Time-of-day window ("HH:MM", half-hour steps), by scheduled time. */
    val timeFrom: String,
    val timeTo: String,
    val calls: Int,
    val early: Int,
    val onTime: Int,
    val lateMinor: Int,
    val lateModerate: Int,
    val lateSevere: Int,
    val cancelled: Int,
    val noReport: Int,
    /** Mean minutes late (negative = early) of reported calls. */
    val averageDelay: Double? = null,
)

/** One operator in the league: its points per call (0-100) and the counts behind them. */
@Serializable
data class LeagueRow(
    val toc: String,
    val score: Double,
    /** Calls that scored: reported or cancelled. */
    val scored: Int,
    val calls: Int,
    val early: Int,
    val onTime: Int,
    val lateMinor: Int,
    val lateModerate: Int,
    val lateSevere: Int,
    val cancelled: Int,
    val noReport: Int,
    val averageDelay: Double? = null,
)

@Serializable
data class LeagueResponse(
    val days: Int,
    val fromDate: String? = null,
    val toDate: String? = null,
    val timeFrom: String,
    val timeTo: String,
    /** Best first. */
    val rows: List<LeagueRow>,
    /** Operators left out: too few scored calls, or fewer than half their calls reported. */
    val notEnough: List<String>,
)

internal object Punctuality {
    const val HISTORY_DAYS = 30L
    private const val PLANNED_CANCELLATION = "PD"
    /** Bump when the counting rules change, so stored days are rebuilt. 2: planned (PD) cancellations left out. */
    private const val RULES_VERSION = "2"
    private const val RULES_KEY = "punctuality_rules"
    private const val SLOT_MINUTES = 30

    private class ScheduleRow(
        val uid: String, val stp: Char, val start: String, val headcode: String, val tsc: String, val stop: TimetabledStop,
    )

    /** A worked-out day: counts per (crs, toc, half-hour slot). */
    class Day(val date: String, val services: Int, val calls: Int, val counts: Map<Triple<String, String, Int>, PunctualityCounts>)

    /** Works out [date]'s calls and replaces its stored counts. Returns the number of calls counted. */
    fun buildDay(date: LocalDate): Int {
        val started = System.currentTimeMillis()
        val day = computeDay(date)
        val d = day.date
        transaction {
            execUpdate("DELETE FROM punctuality_daily WHERE date = ?", listOf(d))
            for ((k, c) in day.counts) {
                execUpdate(
                    "INSERT INTO punctuality_daily (date, crs, toc, slot, total, early, on_time, late_minor, late_moderate, late_severe, cancelled, no_report, delay_sum) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    listOf(d, k.first, k.second, k.third.toString(), c.total.toString(), c.early.toString(), c.onTime.toString(), c.lateMinor.toString(),
                        c.lateModerate.toString(), c.lateSevere.toString(), c.cancelled.toString(), c.noReport.toString(), c.delaySum.toString())
                )
            }
            execUpdate("INSERT OR REPLACE INTO punctuality_days (date, built_at, calls) VALUES (?, ?, ?)", listOf(d, LocalDateTime.now().toString(), day.calls.toString()))
        }
        log.info("Punctuality for $d: ${day.services} services, ${day.calls} calls, ${day.counts.size} rows in ${(System.currentTimeMillis() - started) / 1000}s")
        return day.calls
    }

    /** Works out [date]'s calls without storing anything. */
    fun computeDay(date: LocalDate): Day {
        val d = date.toString()
        // The day's passenger timetable: the strongest STP variant of each uid (latest start date on a tie).
        val byUid = transaction {
            // TIPLOC -> STANOX: NLC first, PIF last so PIF wins where both have one.
            val tiplocStanox = HashMap<String, String>()
            for (table in listOf("nlc_loc", "pif_loc")) {
                query("SELECT tiploc, stanox FROM $table WHERE stanox IS NOT NULL AND stanox != ''") { rs ->
                    while (rs.next()) tiplocStanox[rs.getString(1)] = rs.getString(2)
                }
            }
            val rows = ArrayList<ScheduleRow>()
            query(
                "SELECT uid, stp_indicator, schedule_start_date, headcode, tsc, tiploc, crs, scheduled_time, scheduled_arrival, scheduled_departure, stop_type " +
                    "FROM schedules WHERE is_pass = 0 AND $SCHEDULE_DATE_SCOPE_SQL ORDER BY uid, stop_sequence",
                scheduleDateScopeArgs(d)
            ) { rs ->
                while (rs.next()) {
                    val headcode = rs.getString(4) ?: continue
                    if (!isPassengerHeadcode(headcode)) continue
                    val tiploc = rs.getString(6) ?: continue
                    val stanox = tiplocStanox[tiploc]
                    val crs = rs.getString(7)?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(tiploc) ?: continue
                    val scht = rs.getString(8)?.takeIf { it.isNotEmpty() } ?: continue
                    rows += ScheduleRow(
                        rs.getString(1), rs.getString(2)?.firstOrNull() ?: 'P', rs.getString(3) ?: "", headcode, rs.getString(5) ?: "",
                        TimetabledStop(crs, stanox, scht, rs.getString(9), rs.getString(10), rs.getString(11) ?: ""),
                    )
                }
            }
            rows.groupBy { it.uid }.mapNotNull { (uid, all) ->
                val best = all.minWith(compareBy<ScheduleRow> { STP_PRIORITY[it.stp] ?: 99 }.thenByDescending { it.start })
                if (best.stp == 'C' || ScheduleCancellations.isCancelled(uid, d)) return@mapNotNull null
                uid to all.filter { it.stp == best.stp && it.start == best.start }
            }.toMap()
        }
        val origins = transaction { activationsFor(byUid.keys, d) }
        val counts = HashMap<Triple<String, String, Int>, PunctualityCounts>()
        var calls = 0
        // Movements per uid, in chunks so no single read transaction runs for the whole day.
        for (chunk in byUid.entries.chunked(500)) {
            transaction {
                for ((uid, rows) in chunk) {
                    val moves = ArrayList<ReportedMovement>()
                    query(
                        "SELECT stanox, crs, scheduled_time, actual_time, event_type, is_cancelled, cancel_reason FROM movements WHERE uid = ? AND ${serviceDaySql("event_ts")}",
                        listOf(uid) + serviceDayArgs(d)
                    ) { rs ->
                        while (rs.next()) {
                            moves += ReportedMovement(
                                rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5) ?: "", rs.getBoolean(6), rs.getString(7),
                            )
                        }
                    }
                    // Planned not to run (cancelled with reason PD, "system-generated planned cancellation"), and
                    // it didn't: not part of the day's service, so not counted. PD is the most common reason.
                    if (moves.any { it.isCancelled && it.cancelReason == PLANNED_CANCELLATION } && moves.none { !it.actualTime.isNullOrEmpty() }) continue
                    val originCrs = origins[uid]?.originStanox?.let { CorpusLookup.crsFromStanox(it) }
                    val toc = CorpusLookup.operatorForTsc(rows.first().tsc)
                    for (o in callOutcomes(rows.map { it.stop }, moves, originCrs)) {
                        val slot = o.slotTime.toMinutes() / SLOT_MINUTES
                        counts.getOrPut(Triple(o.crs, toc, slot)) { PunctualityCounts() }.add(o)
                        calls++
                    }
                }
            }
        }
        return Day(d, byUid.size, calls, counts)
    }

    /**
     * Days that need working out: the last [HISTORY_DAYS] finished days (a service day ends at noon the next
     * day) from the first full day of movement data, either never built or built before they were final.
     */
    fun daysToBuild(now: LocalDateTime = LocalDateTime.now()): List<LocalDate> = transaction {
        val dataStart = queryFirst("SELECT MIN(event_ts) FROM movements") { it.getString(1) }
            ?.take(10)?.let { LocalDate.parse(it).plusDays(1) } ?: return@transaction emptyList()
        val built = HashMap<String, LocalDateTime>()
        query("SELECT date, built_at FROM punctuality_days") { rs ->
            while (rs.next()) built[rs.getString(1)] = LocalDateTime.parse(rs.getString(2))
        }
        val today = now.toLocalDate()
        generateSequence(today.minusDays(1)) { it.minusDays(1) }
            .takeWhile { !it.isBefore(today.minusDays(HISTORY_DAYS)) && !it.isBefore(dataStart) }
            .filter { day ->
                val final = day.plusDays(1).atTime(12, 0)
                val at = built[day.toString()]
                !now.isBefore(final) && (at == null || at.isBefore(final))
            }
            .toList()
    }

    /** Builds missing days now and then, hourly; yesterday first. Old days are pruned. */
    fun start(scope: CoroutineScope) = scope.launch {
        delay(5 * 60_000L)   // let startup and the Kafka backlog settle
        // When the counting rules change, every stored day is worked out again.
        if (AppDatabase.getCifMeta(RULES_KEY) != RULES_VERSION) {
            transaction { execUpdate("DELETE FROM punctuality_days") }
            AppDatabase.setCifMeta(RULES_KEY, RULES_VERSION)
            log.info("Punctuality rules changed: rebuilding every stored day")
        }
        while (isActive) {
            try {
                transaction {
                    execUpdate("DELETE FROM punctuality_daily WHERE date < ?", listOf(LocalDate.now().minusDays(HISTORY_DAYS + 1).toString()))
                    execUpdate("DELETE FROM punctuality_days WHERE date < ?", listOf(LocalDate.now().minusDays(HISTORY_DAYS + 1).toString()))
                }
                for (day in daysToBuild()) {
                    if (!isActive) break
                    buildDay(day)
                }
            } catch (e: Exception) {
                log.warn("Punctuality build failed: ${e.message}")
            }
            delay(60 * 60_000L)
        }
    }

    /**
     * The half-hour slots from [from] (rounded down) to before [to], so 06:00-09:30 is 06:00-09:29; wraps past
     * midnight when [to] is earlier. [sql] filters punctuality_daily with [args].
     */
    class SlotWindow(from: String, to: String) {
        private val fromSlot = from.toMinutes() / SLOT_MINUTES
        private val toSlot = maxOf(0, to.toMinutes() - 1) / SLOT_MINUTES
        val sql = if (toSlot >= fromSlot) "slot BETWEEN ? AND ?" else "(slot >= ? OR slot <= ?)"
        val args = listOf(fromSlot.toString(), toSlot.toString())
        val timeFrom = clockTime(fromSlot * SLOT_MINUTES)
        val timeTo = clockTime(minOf((toSlot + 1) * SLOT_MINUTES, 1439))
    }

    /** Sums the stored counts for a station and/or operator over the last [HISTORY_DAYS] days and a time window. */
    fun summary(crs: String?, toc: String?, from: String, to: String): PunctualityResponse = transaction {
        val since = LocalDate.now().minusDays(HISTORY_DAYS).toString()
        val window = SlotWindow(from, to)
        val where = listOfNotNull(crs?.let { "crs = ?" to it }, toc?.let { "toc = ?" to it })
        val c = PunctualityCounts()
        var firstDate: String? = null
        var lastDate: String? = null
        val days = HashSet<String>()
        query(
            "SELECT date, ${PunctualityCounts.SUMS} FROM punctuality_daily " +
                "WHERE ${where.joinToString(" AND ") { it.first }} AND date >= ? AND ${window.sql} GROUP BY date",
            where.map { it.second } + since + window.args
        ) { rs ->
            while (rs.next()) {
                val date = rs.getString(1)
                days += date
                if (firstDate == null || date < firstDate!!) firstDate = date
                if (lastDate == null || date > lastDate!!) lastDate = date
                c.addSums(rs, 2)
            }
        }
        PunctualityResponse(
            crs = crs, toc = toc, fromDate = firstDate, toDate = lastDate, days = days.size,
            timeFrom = window.timeFrom, timeTo = window.timeTo,
            calls = c.total, early = c.early, onTime = c.onTime, lateMinor = c.lateMinor, lateModerate = c.lateModerate,
            lateSevere = c.lateSevere, cancelled = c.cancelled, noReport = c.noReport,
            averageDelay = c.averageDelay,
        )
    }
}

/**
 * Operators ranked by points per call over the last [League.PERIODS] finished days: early or on time 100,
 * 1-4 min late 75, 5-14 late 40, 15+ late 10, cancelled 0. Calls with no report don't score.
 */
internal object League {
    val PERIODS = setOf(1, 7, 30)
    private const val MIN_SCORED_PER_DAY = 35

    /**
     * Not ranked. London Underground is only in the data for the Bakerloo's Queens Park-Harrow section, where
     * most of its cancellations are Tube-side (reason TX: two in three trains all day on some Sundays).
     */
    private val NOT_RANKED = listOf("LT")

    fun table(days: Int, from: String, to: String): LeagueResponse = transaction {
        val dates = queryList("SELECT date FROM punctuality_days ORDER BY date DESC LIMIT ?", listOf(days.toString())) { it.getString(1) }
        val window = Punctuality.SlotWindow(from, to)
        val rows = ArrayList<LeagueRow>()
        val notEnough = ArrayList<String>()
        if (dates.isNotEmpty()) {
            query(
                "SELECT toc, ${PunctualityCounts.SUMS} FROM punctuality_daily " +
                    "WHERE toc != '' AND toc NOT IN (${placeholders(NOT_RANKED.size)}) AND date IN (${placeholders(dates.size)}) " +
                    "AND ${window.sql} GROUP BY toc",
                NOT_RANKED + dates + window.args
            ) { rs ->
                while (rs.next()) {
                    val toc = rs.getString(1)
                    val c = PunctualityCounts().apply { addSums(rs, 2) }
                    val scored = c.reported + c.cancelled
                    if (scored < MIN_SCORED_PER_DAY * dates.size || c.noReport * 2 > c.total) {
                        if (c.total > 0) notEnough += toc
                        continue
                    }
                    val points = 100.0 * (c.early + c.onTime) + 75.0 * c.lateMinor + 40.0 * c.lateModerate + 10.0 * c.lateSevere
                    rows += LeagueRow(
                        toc, Math.round(points * 10 / scored) / 10.0, scored, c.total, c.early, c.onTime, c.lateMinor,
                        c.lateModerate, c.lateSevere, c.cancelled, c.noReport, c.averageDelay,
                    )
                }
            }
        }
        LeagueResponse(
            days = dates.size, fromDate = dates.lastOrNull(), toDate = dates.firstOrNull(), timeFrom = window.timeFrom, timeTo = window.timeTo,
            rows = rows.sortedWith(compareByDescending<LeagueRow> { it.score }.thenByDescending { it.scored }), notEnough = notEnough.sorted(),
        )
    }
}

private val HHMM = Regex("^([01]\\d|2[0-3]):[0-5]\\d$")

/** The from/to time-of-day query parameters ("HH:MM"), all day by default. */
private fun ApplicationCall.timeWindow(): Pair<String, String> =
    (parameters["from"]?.takeIf { it.matches(HHMM) } ?: "00:00") to (parameters["to"]?.takeIf { it.matches(HHMM) } ?: "23:59")

fun Route.punctualityRoutes() {
    get("/punctuality") {
        val crs = call.parameters["crs"]?.uppercase()?.filter { it.isLetter() }?.take(3)?.takeIf { it.length == 3 }
        val toc = call.parameters["toc"]?.uppercase()?.filter { it.isLetterOrDigit() }?.take(4)?.takeIf { it.isNotEmpty() }
        if (crs == null && toc == null) return@get call.respond(HttpStatusCode.BadRequest, "crs or toc required")
        val (from, to) = call.timeWindow()
        call.respond(dbQuery { Punctuality.summary(crs, toc, from, to) })
    }
    get("/league") {
        val days = call.parameters["days"]?.toIntOrNull()?.takeIf { it in League.PERIODS } ?: 30
        val (from, to) = call.timeWindow()
        call.respond(dbQuery { League.table(days, from, to) })
    }
}

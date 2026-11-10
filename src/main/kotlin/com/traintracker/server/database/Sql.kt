package com.traintracker.server.database

import java.sql.ResultSet
import java.time.Instant
import java.time.LocalDate
import java.util.concurrent.ConcurrentHashMap
import org.jetbrains.exposed.v1.core.IColumnType
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.VarCharColumnType
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.greaterEq
import org.jetbrains.exposed.v1.core.lessEq
import org.jetbrains.exposed.v1.core.like
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.jdbc.transactions.transaction

// Raw SQL helpers. Every parameter is bound as text, which is how all raw SQL in this project is parameterised.

private val TEXT: IColumnType<*> = VarCharColumnType()

private fun textArgs(values: Iterable<String?>): List<Pair<IColumnType<*>, Any?>> = values.map { TEXT to it }

/** Runs [sql] and hands the result set to [block]. */
fun JdbcTransaction.query(sql: String, args: List<String?> = emptyList(), block: (ResultSet) -> Unit) {
    exec(sql, textArgs(args)) { block(it) }
}

/** One value per row; rows mapped to null are skipped. */
fun <T> JdbcTransaction.queryList(sql: String, args: List<String?> = emptyList(), map: (ResultSet) -> T?): List<T> {
    val out = ArrayList<T>()
    query(sql, args) { rs -> while (rs.next()) map(rs)?.let(out::add) }
    return out
}

/** The first row mapped, or null when there are no rows. */
fun <T> JdbcTransaction.queryFirst(sql: String, args: List<String?> = emptyList(), map: (ResultSet) -> T?): T? {
    var out: T? = null
    query(sql, args) { rs -> if (rs.next()) out = map(rs) }
    return out
}

fun JdbcTransaction.count(sql: String, args: List<String?> = emptyList()): Int = queryFirst(sql, args) { it.getInt(1) } ?: 0

fun JdbcTransaction.execUpdate(sql: String, args: List<String?> = emptyList()) {
    exec(sql, textArgs(args))
}

/** "?,?,?" for an IN (...) list. */
fun placeholders(count: Int): String = List(count) { "?" }.joinToString(",")

/** Quotes a literal for the bulk multi-row INSERTs that are built as text. */
fun sqlQuote(s: String): String = "'${s.replace("'", "''")}'"

/** "2026-01-02 03:04:05" -> "2026-01-02T03:04:05Z"; null or empty -> "". */
fun toIsoUtc(raw: String?): String {
    if (raw.isNullOrEmpty()) return ""
    val s = raw.replace(' ', 'T')
    return if (s.endsWith("Z")) s else "${s}Z"
}

/** Current UTC time in SQLite's "yyyy-MM-dd HH:mm:ss" form. */
fun utcStamp(): String = Instant.now().toString().replace('T', ' ').take(19)

/** Splits a comma separated column (units, vehicles) into trimmed, non-empty values. */
fun String?.csvList(): List<String> = orEmpty().split(",").map { it.trim() }.filter { it.isNotEmpty() }

// ---- Schedule date scope -------------------------------------------------------------------------
// A schedule row applies to a date when the date is inside its validity window and its days_run has a
// '1' on that weekday. Rows with an empty schedule_date (none today) always match.

const val SCHEDULE_DATE_SCOPE_SQL: String =
    "((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '')"

/** LIKE pattern matching the days_run string for [date]: '1' at its weekday (Mon=1..Sun=7), '_' elsewhere. */
fun dayRunLikePattern(date: String): String {
    val dow = LocalDate.parse(date).dayOfWeek.value
    return (1..7).joinToString("") { if (it == dow) "1" else "_" }
}

fun scheduleDateScope(date: String): Op<Boolean> =
    ((Schedules.scheduleStartDate lessEq date) and
        (Schedules.scheduleEndDate greaterEq date) and
        (Schedules.daysRun like dayRunLikePattern(date))) or
        (Schedules.scheduleDate eq "")

fun scheduleDateScopeArgs(date: String): List<String> = listOf(date, date, dayRunLikePattern(date))

// ---- TRUST service day -----------------------------------------------------------------------------
// A TRUST train ID ends with the day of month it runs on, and its events fall between 12 hours before
// and 36 hours after the start of that day.

fun serviceDaySql(tsColumn: String): String =
    "$tsColumn >= datetime(?, '-12 hours') AND $tsColumn < datetime(?, '+36 hours') AND train_id LIKE '%' || ?"

fun serviceDayArgs(date: String): List<String> = listOf(date, date, date.takeLast(2))

/** HAVING clause for a train ID that has only cancellation events. */
const val CANCELLED_ONLY_SQL: String =
    "SUM(CASE WHEN is_cancelled=0 THEN 1 ELSE 0 END)=0 AND SUM(CASE WHEN is_cancelled=1 THEN 1 ELSE 0 END)>0"

// ---- Effective timetable date ----------------------------------------------------------------------

private val effectiveDateCache = ConcurrentHashMap<String, Pair<Long, String>>()
private const val EFFECTIVE_DATE_TTL_MS = 30_000L

/**
 * The date whose timetable to use for [requested]. Only today can differ: until today's CIF has been
 * loaded, the most recently loaded date is used instead.
 */
fun effectiveDateFor(requested: String): String {
    val now = System.currentTimeMillis()
    effectiveDateCache[requested]?.let { (at, cached) ->
        if (now - at < EFFECTIVE_DATE_TTL_MS) return cached
    }
    if (requested != LocalDate.now().toString()) return requested

    val resolved = try {
        transaction {
            val loadedToday = queryFirst(
                "SELECT 1 FROM schedules WHERE schedule_date = ? AND stp_indicator != 'N' LIMIT 1",
                listOf(requested)
            ) { true } != null
            if (loadedToday) {
                requested
            } else {
                queryFirst("SELECT MAX(schedule_date) AS d FROM schedules WHERE schedule_date != '' AND stp_indicator != 'N'") {
                    it.getString("d")
                }?.takeIf { it.isNotBlank() } ?: requested
            }
        }
    } catch (e: Exception) {
        requested
    }

    if (effectiveDateCache.size > 8) effectiveDateCache.clear()
    effectiveDateCache[requested] = now to resolved
    return resolved
}

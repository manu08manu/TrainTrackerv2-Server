package com.traintracker.server.cif

import java.time.LocalDate
import java.util.concurrent.atomic.AtomicReference
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.json.JSONObject
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("ScheduleCancellations")

/** STP cancellation ('C') windows from the CIF: a uid is cancelled on dates inside a window on its listed weekdays. */
object ScheduleCancellations {
    private class Window(val uid: String, val start: String, val end: String, val days: String) {
        fun covers(date: LocalDate): Boolean {
            val d = date.toString()
            return start <= d && d <= end && days[date.dayOfWeek.value - 1] == '1'
        }
    }

    private val pending = ArrayList<Window>()
    private val cache = AtomicReference<Map<String, List<Window>>?>(null)

    /** Starts collecting for a new CIF import. */
    fun begin() {
        synchronized(pending) { pending.clear() }
    }

    /** Collects a well-formed cancellation that has not already ended. */
    fun collect(sched: JSONObject) {
        if (sched.optString("transaction_type", "") == "Delete") return
        val uid = sched.optString("CIF_train_uid", "").trim()
        val start = sched.optString("schedule_start_date", "").take(10)
        val end = sched.optString("schedule_end_date", "").take(10)
        val days = sched.optString("schedule_days_runs", "").ifEmpty { "1111111" }
        if (uid.isEmpty() || uid.length > 6 || !uid.all { it.isLetterOrDigit() }) return
        if (start.length != 10 || end.length != 10) return
        if (!start.all { it.isDigit() || it == '-' } || !end.all { it.isDigit() || it == '-' }) return
        if (days.length != 7 || !days.all { it == '0' || it == '1' }) return
        if (end < LocalDate.now().toString()) return
        synchronized(pending) { pending.add(Window(uid, start, end, days)) }
    }

    /** Replaces the stored windows with those collected since begin(). */
    fun persist() {
        val list = synchronized(pending) { ArrayList(pending) }
        try {
            transaction {
                exec("DELETE FROM cif_cancellations")
                // Values were validated in collect(), so the INSERT is built as text.
                for (chunk in list.chunked(400)) {
                    val values = chunk.joinToString(",") { "('${it.uid}','${it.start}','${it.end}','${it.days}')" }
                    exec("INSERT OR IGNORE INTO cif_cancellations (uid, start_date, end_date, days) VALUES $values")
                }
            }
            cache.set(list.groupBy { it.uid })
            log.info("cif_cancellations: ${list.size} cancellation windows stored")
        } catch (e: Exception) {
            log.warn("cif_cancellations persist failed: ${e.message}")
        }
    }

    fun isCancelled(uid: String, date: String): Boolean {
        val windows = (cache.get() ?: load())[uid] ?: return false
        val d = try {
            LocalDate.parse(date)
        } catch (e: Exception) {
            return false
        }
        return windows.any { it.covers(d) }
    }

    /** TRUST wins: cancelled if TRUST says so, running if TRUST has an actual time, else the timetable decides. */
    fun effectiveCancelled(uid: String, date: String, trustCancelled: Boolean, hasActualTime: Boolean): Boolean {
        if (trustCancelled) return true
        if (hasActualTime) return false
        return isCancelled(uid, date)
    }

    private fun load(): Map<String, List<Window>> {
        val loaded = HashMap<String, MutableList<Window>>()
        try {
            transaction {
                exec("SELECT uid, start_date, end_date, days FROM cif_cancellations") { rs ->
                    while (rs.next()) {
                        val w = Window(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4))
                        loaded.getOrPut(w.uid) { mutableListOf() }.add(w)
                    }
                }
            }
            cache.set(loaded)
        } catch (e: Exception) {
            log.warn("cif_cancellations load failed: ${e.message}")
        }
        return loaded
    }
}

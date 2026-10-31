package com.traintracker.server.cif

import java.time.LocalDate
import java.util.concurrent.atomic.AtomicReference
import org.jetbrains.exposed.sql.transactions.transaction
import org.json.JSONObject
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("ScheduleCancellations")

object ScheduleCancellations {
    private val pending = ArrayList<String>()
    private val cache = AtomicReference<Map<String, List<String>>?>(null)
    private var tableReady = false

    private fun ensureTable() {
        if (tableReady) return
        transaction {
            exec("CREATE TABLE IF NOT EXISTS cif_cancellations (uid TEXT NOT NULL, start_date TEXT NOT NULL, end_date TEXT NOT NULL, days TEXT NOT NULL, PRIMARY KEY (uid, start_date, end_date, days)) WITHOUT ROWID")
        }
        tableReady = true
    }

    fun begin() {
        synchronized(pending) { pending.clear() }
    }

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
        synchronized(pending) { pending.add("$uid,$start,$end,$days") }
    }

    fun persist() {
        val list = synchronized(pending) { ArrayList(pending) }
        try {
            ensureTable()
            transaction {
                exec("DELETE FROM cif_cancellations")
                for (chunk in list.chunked(400)) {
                    val values = chunk.joinToString(",") {
                        val p = it.split(",")
                        "('${p[0]}','${p[1]}','${p[2]}','${p[3]}')"
                    }
                    exec("INSERT OR IGNORE INTO cif_cancellations (uid, start_date, end_date, days) VALUES $values")
                }
            }
            cache.set(list.groupBy({ it.substringBefore(",") }, { it.substringAfter(",") }))
            log.info("cif_cancellations: ${list.size} cancellation windows stored")
        } catch (e: Exception) {
            log.warn("cif_cancellations persist failed: ${e.message}")
        }
    }

    fun isCancelled(uid: String, date: String): Boolean {
        val map = cache.get() ?: load()
        val windows = map[uid] ?: return false
        val d = try {
            LocalDate.parse(date)
        } catch (e: Exception) {
            return false
        }
        val ds = d.toString()
        val dow = d.dayOfWeek.value - 1
        return windows.any {
            val p = it.split(",")
            p[0] <= ds && ds <= p[1] && p[2][dow] == '1'
        }
    }

    fun effectiveCancelled(uid: String, date: String, trustCancelled: Boolean, hasActualTime: Boolean): Boolean {
        if (trustCancelled) return true
        if (hasActualTime) return false
        return isCancelled(uid, date)
    }

    private fun load(): Map<String, List<String>> {
        val loaded = HashMap<String, MutableList<String>>()
        try {
            ensureTable()
            transaction {
                exec("SELECT uid, start_date, end_date, days FROM cif_cancellations") { rs ->
                    while (rs.next()) {
                        loaded.getOrPut(rs.getString(1)) { mutableListOf() }.add("${rs.getString(2)},${rs.getString(3)},${rs.getString(4)}")
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

package com.traintracker.server.cif

import com.traintracker.server.database.execUpdate
import com.traintracker.server.database.queryList
import com.traintracker.server.database.sqlQuote
import com.traintracker.server.rdm.RdmFeed
import com.traintracker.server.rdm.RdmFiles
import com.traintracker.server.rdm.RdmWatcher
import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("SmartLookup")

data class SmartEntry(
    val td: String,
    val stepType: String,
    val fromBerth: String,
    val toBerth: String,
    val stanox: String,
    val stanme: String,
    val event: String,
    val platform: String,
    val toLine: String,
    val offsetSec: Int?,
    val route: String,
    val fromLine: String,
    val comment: String
)

class SmartMatch(val kind: Int, val entries: List<SmartEntry>)

object SmartLookup {
    const val KIND_EXACT = 1
    const val KIND_FROM = 2
    const val KIND_TO = 3
    @Volatile
    private var index: Map<String, List<SmartEntry>> = emptyMap()
    @Volatile
    private var smartEntries: List<SmartEntry> = emptyList()
    @Volatile
    private var smartAreas: Set<String> = emptySet()
    @Volatile
    private var smartCoverage: Set<String> = emptySet()
    @Volatile
    private var dbIndex: Map<String, List<SmartEntry>> = emptyMap()
    @Volatile
    private var dbAreas: Set<String> = emptySet()
    @Volatile
    private var dbCoverage: Set<String> = emptySet()
    @Volatile
    private var dbSynced = false
    private val dbLock = Any()

    fun keyCount(): Int = index.size

    fun hasArea(td: String): Boolean = smartAreas.contains(td) || dbAreas.contains(td)

    fun isLoaded(): Boolean = index.isNotEmpty() || dbIndex.isNotEmpty()

    fun hasCoverage(stanox: String, type: String): Boolean {
        val k = "$stanox|${if (type == "ARRIVAL") "A" else "D"}"
        return smartCoverage.contains(k) || dbCoverage.contains(k)
    }

    private fun buildCoverage(parsed: Map<String, List<SmartEntry>>): Set<String> {
        val cov = HashSet<String>()
        for (list in parsed.values) {
            for (e in list) {
                if (e.event == "A" || e.event == "C") cov.add("${e.stanox}|A")
                if (e.event == "B" || e.event == "D") cov.add("${e.stanox}|D")
            }
        }
        return cov
    }

    fun resolve(td: String, from: String, to: String): SmartMatch? {
        val db = dbIndex
        val sm = index
        if (from.isNotEmpty() && to.isNotEmpty()) {
            val k = "$td|$from|$to"
            (db[k] ?: sm[k])?.let { return SmartMatch(KIND_EXACT, it) }
        }
        if (from.isNotEmpty()) {
            val k = "$td|$from|"
            (db[k] ?: sm[k])?.let { return SmartMatch(KIND_FROM, it) }
        }
        if (to.isNotEmpty()) {
            val k = "$td||$to"
            (db[k] ?: sm[k])?.let { return SmartMatch(KIND_TO, it) }
        }
        return null
    }

    fun start(scope: CoroutineScope): Job {
        RdmWatcher.register(
            RdmFeed("smart-berths", RdmFiles.ofType("berth-steps-.csv"), tick = { ensureDbSynced() }) { file, isNew ->
                loadFromFile(file, isNew)
            }
        )
        return RdmWatcher.ensureStarted(scope)
    }

    private fun loadFromFile(file: File, isNew: Boolean) {
        val entries = parseCsv(file)
        if (entries.isEmpty()) error("SMART CSV parsed to zero entries: ${file.name}")
        install(entries, isNew)
    }

    private fun parseCsv(f: File): List<SmartEntry> {
        val out = ArrayList<SmartEntry>()
        for (r in RdmFiles.readCsv(f)) {
            if (r.size < 12) continue
            fun g(i: Int) = if (i < r.size) r[i].trim() else ""
            val td = g(0)
            if (td.isEmpty() || g(1).equals("STEPTYPE", ignoreCase = true)) continue
            out.add(
                SmartEntry(
                    stepType = g(1), fromBerth = g(2), toBerth = g(3), stanox = g(4), stanme = g(5),
                    event = g(6), platform = g(7), toLine = g(8), offsetSec = g(9).toIntOrNull(),
                    route = g(10), fromLine = g(11), td = td, comment = g(12)
                )
            )
        }
        return out
    }

    private fun buildIndex(entries: List<SmartEntry>, label: String): Map<String, List<SmartEntry>> {
        val map = HashMap<String, MutableList<SmartEntry>>(entries.size * 2 + 16)
        val fromOnly = HashMap<String, MutableList<SmartEntry>>()
        for (e in entries) {
            val td = e.td
            val from = e.fromBerth
            val to = e.toBerth
            map.getOrPut("$td|$from|$to") { mutableListOf() }.add(e)
            if (from.isNotEmpty() && to.isNotEmpty()) {
                map.getOrPut("$td|$from|") { mutableListOf() }.add(e)
                map.getOrPut("$td||$to") { mutableListOf() }.add(e)
                fromOnly.getOrPut("$td|$from") { mutableListOf() }.add(e)
            }
        }
        var fallbackAdded = 0
        for ((key, list) in fromOnly) {
            val parts = key.split("|", limit = 2)
            val td = parts[0]
            val from = parts[1]
            val toKey = "$td||$from"
            if (map.containsKey(toKey)) continue
            val stanoxes = list.mapTo(HashSet()) { it.stanox }
            if (stanoxes.size == 1) {
                map[toKey] = list
                fallbackAdded++
            }
        }
        if (fallbackAdded > 0) log.info("$label: added $fallbackAdded FROMBERTH-fallback TO-keys")
        return map
    }

    private fun install(entries: List<SmartEntry>, isNew: Boolean) {
        val parsed = buildIndex(entries, "SMART")
        val a = HashSet<String>()
        for (e in entries) a.add(e.td)
        smartAreas = a
        smartCoverage = buildCoverage(parsed)
        smartEntries = entries
        index = parsed
        log.info("SMART index built: ${parsed.size} keys, ${a.size} TD areas")
        syncFromSmart(entries, isNew)
        reloadOverrides()
    }

    private fun rowKey(e: SmartEntry): String = "${e.td}|${e.fromBerth}|${e.toBerth}|${e.stanox}|${e.event}|${e.toLine}|${e.fromLine}|${e.route}|${e.stepType}"

    private fun sig(e: SmartEntry): String =
        "${rowKey(e)}|${e.platform}|${e.stepType}|${e.offsetSec?.toString() ?: ""}|${e.comment}|${e.stanme}"

    private fun loadDbEntries(): List<SmartEntry> {
        val out = ArrayList<SmartEntry>()
        transaction {
            exec("SELECT TD, StepType, FromBerth, ToBerth, Stanox, Stanme, Event, Platform, ToLine, BerthOffset, Route, FromLine, Comment FROM smart_berths ORDER BY id") { rs ->
                while (rs.next()) {
                    val off = rs.getInt(10)
                    val offNull = rs.wasNull()
                    out.add(
                        SmartEntry(
                            td = rs.getString(1) ?: "",
                            stepType = rs.getString(2) ?: "",
                            fromBerth = rs.getString(3) ?: "",
                            toBerth = rs.getString(4) ?: "",
                            stanox = rs.getString(5) ?: "",
                            stanme = rs.getString(6) ?: "",
                            event = rs.getString(7) ?: "",
                            platform = rs.getString(8) ?: "",
                            toLine = rs.getString(9) ?: "",
                            offsetSec = if (offNull) null else off,
                            route = rs.getString(11) ?: "",
                            fromLine = rs.getString(12) ?: "",
                            comment = rs.getString(13) ?: ""
                        )
                    )
                }
            }
        }
        return out
    }

    private fun syncFromSmart(entries: List<SmartEntry>, applyUpdates: Boolean = false) {
        try {
            val dbEntries = loadDbEntries()
            val dbRows = HashMap<String, ArrayList<String>>(dbEntries.size * 2 + 16)
            for (e in dbEntries) dbRows.getOrPut(rowKey(e)) { ArrayList() }.add(sig(e))
            val first = dbEntries.isEmpty()
            val day = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE)
            val tag = "${if (first) "smart-import-" else "smart-new-"}$day"
            val changedTag = "smart-changed-$day"
            val distinct = LinkedHashSet(entries)
            val dlCount = HashMap<String, Int>()
            for (e in distinct) dlCount[rowKey(e)] = (dlCount[rowKey(e)] ?: 0) + 1
            val toInsert = ArrayList<SmartEntry>()
            val toUpdate = ArrayList<SmartEntry>()
            var skipped = 0
            for (e in distinct) {
                val k = rowKey(e)
                val have = dbRows[k]
                if (have == null) {
                    toInsert.add(e)
                } else if (!have.contains(sig(e))) {
                    if (applyUpdates && have.size == 1 && dlCount[k] == 1) toUpdate.add(e) else skipped++
                }
            }
            for (chunk in toInsert.chunked(400)) {
                val values = chunk.joinToString(",") { e ->
                    listOf(e.td, e.stepType, e.fromBerth, e.toBerth, e.stanox, e.stanme, e.event, e.platform, e.toLine).joinToString(",", "(", ",") { sqlQuote(it) } +
                        "${e.offsetSec?.toString() ?: "NULL"},${sqlQuote(e.route)},${sqlQuote(e.fromLine)},${sqlQuote(e.comment)},'$tag')"
                }
                transaction {
                    exec("INSERT INTO smart_berths (TD, StepType, FromBerth, ToBerth, Stanox, Stanme, Event, Platform, ToLine, BerthOffset, Route, FromLine, Comment, Source) VALUES $values")
                }
            }
            for (chunk in toUpdate.chunked(200)) {
                transaction {
                    for (e in chunk) {
                        execUpdate(
                            "UPDATE smart_berths SET platform = ?, StepType = ?, BerthOffset = ${e.offsetSec?.toString() ?: "NULL"}, Comment = ?, Stanme = ?, Source = ?, ChangedBy = NULL, ChangedAt = datetime('now') WHERE td = ? AND FromBerth = ? AND ToBerth = ? AND Stanox = ? AND Event = ? AND ToLine = ? AND FromLine = ? AND Route = ? AND StepType = ?",
                            listOf(e.platform, e.stepType, e.comment, e.stanme, changedTag, e.td, e.fromBerth, e.toBerth, e.stanox, e.event, e.toLine, e.fromLine, e.route, e.stepType)
                        )
                    }
                }
            }
            dbSynced = true
            if (toInsert.isNotEmpty() || toUpdate.isNotEmpty() || skipped > 0) {
                log.info("SMART DB sync: added ${toInsert.size} rows ($tag); updated ${toUpdate.size} rows ($changedTag); $skipped differing rows left as they are")
            }
        } catch (e: Exception) {
            log.warn("SMART DB sync failed, using SMART reference only: ${e.message}")
        }
    }

    private fun ensureDbSynced() {
        if (!dbSynced && smartEntries.isNotEmpty()) {
            syncFromSmart(smartEntries)
            reloadOverrides()
        }
    }

    fun reloadOverrides() {
        synchronized(dbLock) {
            try {
                    val entries = loadDbEntries()
                val idx = buildIndex(entries, "SMART DB")
                val a = HashSet<String>()
                for (e in entries) a.add(e.td)
                dbAreas = a
                dbCoverage = buildCoverage(idx)
                dbIndex = idx
                log.info("SMART DB loaded: ${entries.size} rows, ${idx.size} keys")
            } catch (e: Exception) {
                log.warn("SMART DB load failed, using SMART reference only: ${e.message}")
            }
        }
    }

    fun listBerths(stanox: String, td: String, berth: String, limit: Int): List<Map<String, String>> {
        val sx = stanox.filter { it.isLetterOrDigit() }
        val t = td.filter { it.isLetterOrDigit() }.uppercase()
        val b = berth.filter { it.isLetterOrDigit() }.uppercase()
        val where = ArrayList<String>()
        val args = ArrayList<String>()
        if (sx.isNotEmpty()) { where.add("stanox = ?"); args.add(sx) }
        if (t.isNotEmpty()) { where.add("td = ?"); args.add(t) }
        if (b.isNotEmpty()) { where.add("(FromBerth = ? OR ToBerth = ?)"); args.add(b); args.add(b) }
        val keys = listOf(
            "id", "td", "stepType", "fromBerth", "toBerth", "stanox", "stanme", "event", "platform", "toLine",
            "offsetSec", "route", "fromLine", "comment", "source", "changedBy", "changedAt"
        )
        return try {
            transaction {
                queryList(
                    "SELECT id, TD, StepType, FromBerth, ToBerth, Stanox, Stanme, Event, Platform, ToLine, BerthOffset, Route, FromLine, Comment, Source, ChangedBy, ChangedAt FROM smart_berths" +
                        (if (where.isEmpty()) "" else " WHERE ${where.joinToString(" AND ")}") +
                        " ORDER BY TD, FromBerth, ToBerth LIMIT ${limit.coerceIn(1, 500)}",
                    args
                ) { rs -> keys.withIndex().associate { (i, key) -> key to (rs.getString(i + 1) ?: "") } }
            }
        } catch (e: Exception) {
            log.warn("listBerths failed: ${e.message}")
            emptyList()
        }
    }

    /** Adds a manual berth step. Returns an error message, or null on success. */
    fun addBerth(
        td: String,
        from: String,
        to: String,
        stanox: String,
        event: String,
        platform: String,
        stepType: String,
        offsetSec: Int,
        changedBy: String
    ): String? {
        val t = td.trim().uppercase()
        val f = from.trim().uppercase()
        val o = to.trim().uppercase()
        val sx = stanox.trim()
        val ev = event.trim().uppercase()
        val pl = platform.trim().uppercase()
        val st = stepType.trim().uppercase()
        if (t.isEmpty() || t.length > 4 || !t.all { it.isLetterOrDigit() }) return "td must be 1-4 letters/digits"
        if (f.length > 6 || o.length > 6 || !f.all { it.isLetterOrDigit() } || !o.all { it.isLetterOrDigit() }) return "berths must be up to 6 letters/digits"
        if (f.isEmpty() && o.isEmpty()) return "a from berth or a to berth is required"
        if (!sx.matches(Regex("\\d{5}"))) return "stanox must be 5 digits"
        if (ev != "A" && ev != "B" && ev != "C" && ev != "D") return "event must be A, B, C or D"
        if (pl.length > 3 || !pl.all { it.isLetterOrDigit() }) return "platform must be up to 3 letters/digits"
        if (st.length > 2 || !st.all { it.isLetterOrDigit() }) return "step type must be up to 2 letters/digits"
        if (offsetSec < -600 || offsetSec > 600) return "offset must be within 600 seconds"
        try {
            transaction {
                execUpdate(
                    "INSERT INTO smart_berths (TD, StepType, FromBerth, ToBerth, Stanox, Event, Platform, BerthOffset, Source, ChangedBy) VALUES (?, ?, ?, ?, ?, ?, ?, $offsetSec, 'manual', ?)",
                    listOf(t, st, f, o, sx, ev, pl, changedBy)
                )
            }
        } catch (e: Exception) {
            return "database error: ${e.message}"
        }
        reloadOverrides()
        return null
    }

    fun deleteBerth(id: Long) {
        try {
            transaction { exec("DELETE FROM smart_berths WHERE id = $id") }
        } catch (e: Exception) {
            log.warn("deleteBerth failed: ${e.message}")
            return
        }
        reloadOverrides()
    }
}

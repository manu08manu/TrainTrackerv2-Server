package com.traintracker.server.cif

import com.traintracker.server.Config
import com.traintracker.server.database.AppDatabase
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpRedirect
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.isSuccess
import io.ktor.utils.io.readAvailable
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.Base64
import java.util.concurrent.CancellationException
import java.util.zip.GZIPInputStream
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.transactions.transaction
import org.json.JSONObject
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("SmartLookup")

data class SmartEntry(
    val td: String,
    val fromBerth: String,
    val toBerth: String,
    val stanox: String,
    val event: String,
    val platform: String,
    val stepType: String,
    val offsetSec: Int?
)

class SmartMatch(val kind: Int, val entries: List<SmartEntry>)

object SmartLookup {
    const val KIND_EXACT = 1
    const val KIND_FROM = 2
    const val KIND_TO = 3
    private const val URL = "https://publicdatafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=SMART"
    private const val META_KEY = "smart_last_download_date"
    private val cacheFile = File("/opt/traintracker/smart_cache.json.gz")
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
    @Volatile
    private var tableReady = false
    private val dbLock = Any()
    private val http = HttpClient(CIO) {
        install(HttpRedirect) { checkHttpMethod = false }
        engine { requestTimeout = 0L }
    }

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

    fun start(scope: CoroutineScope): Job = scope.launch(Dispatchers.IO) {
        if (cacheFile.exists()) {
            try {
                install(parseEntries(cacheFile), "cache")
            } catch (e: Exception) {
                log.warn("SMART cache unreadable: ${e.message}")
            }
        }
        try {
            refresh(index.isEmpty())
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("SMART download failed on startup: ${e.message}")
        }
        ensureDbSynced()
        while (isActive) {
            delay(3600000L)
            ensureDbSynced()
            try {
                refresh(false)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("SMART refresh failed: ${e.message}")
            }
        }
    }

    private suspend fun refresh(force: Boolean) {
        val today = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
        val last = AppDatabase.getCifMeta(META_KEY)
        val due = force || (last != today && LocalTime.now().hour >= 3)
        if (!due) return
        log.info("Downloading SMART extract (last=$last)")
        val tmp = File("${cacheFile.path}.tmp")
        download(tmp)
        val entries = parseEntries(tmp)
        if (entries.isEmpty()) {
            tmp.delete()
            error("SMART download parsed to zero entries")
        }
        Files.move(tmp.toPath(), cacheFile.toPath(), StandardCopyOption.REPLACE_EXISTING)
        install(entries, "download")
        AppDatabase.setCifMeta(META_KEY, today)
    }

    private suspend fun download(dest: File) {
        val creds = Base64.getEncoder().encodeToString("${Config.scheduleUsername}:${Config.schedulePassword}".toByteArray())
        val response = http.get(URL) {
            header("Authorization", "Basic $creds")
        }
        if (!response.status.isSuccess()) error("SMART download failed: HTTP ${response.status}")
        val channel = response.bodyAsChannel()
        FileOutputStream(dest).buffered().use { out ->
            val buffer = ByteArray(65536)
            while (true) {
                val n = channel.readAvailable(buffer)
                if (n == -1) break
                if (n > 0) out.write(buffer, 0, n)
            }
        }
    }

    private fun parseEntries(file: File): List<SmartEntry> {
        val text = GZIPInputStream(FileInputStream(file).buffered()).use { String(it.readBytes(), Charsets.UTF_8) }
        val arr = JSONObject(text).getJSONArray("BERTHDATA")
        val out = ArrayList<SmartEntry>(arr.length())
        for (i in 0 until arr.length()) {
            val o = arr.getJSONObject(i)
            val td = o.optString("TD", "").trim()
            val from = o.optString("FROMBERTH", "").trim()
            val to = o.optString("TOBERTH", "").trim()
            val stanox = o.optString("STANOX", "").trim()
            if (td.isEmpty() || stanox.isEmpty() || (from.isEmpty() && to.isEmpty())) continue
            out.add(
                SmartEntry(
                    td, from, to, stanox,
                    o.optString("EVENT", "").trim(),
                    o.optString("PLATFORM", "").trim(),
                    o.optString("STEPTYPE", "").trim(),
                    o.optString("BERTHOFFSET", "").trim().toIntOrNull()
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

    private fun install(entries: List<SmartEntry>, source: String) {
        val parsed = buildIndex(entries, "SMART")
        val a = HashSet<String>()
        for (e in entries) a.add(e.td)
        smartAreas = a
        smartCoverage = buildCoverage(parsed)
        smartEntries = entries
        index = parsed
        log.info("SMART loaded from $source: ${parsed.size} keys, ${a.size} TD areas")
        syncFromSmart(entries, source == "download")
        reloadOverrides()
    }

    private fun sq(s: String): String = s.replace("'", "''")

    private fun primaryKey(e: SmartEntry): String = "${e.td}|${e.fromBerth}|${e.toBerth}"

    private fun rowKey(e: SmartEntry): String = "${e.td}|${e.fromBerth}|${e.toBerth}|${e.stanox}|${e.event}"

    private fun sig(e: SmartEntry): String =
        "${e.td}|${e.fromBerth}|${e.toBerth}|${e.stanox}|${e.event}|${e.platform}|${e.stepType}|${e.offsetSec?.toString() ?: ""}"

    fun ensureTable() {
        if (tableReady) return
        transaction {
            exec("CREATE TABLE IF NOT EXISTS smart_berths (id INTEGER PRIMARY KEY AUTOINCREMENT, td TEXT NOT NULL, from_berth TEXT NOT NULL DEFAULT '', to_berth TEXT NOT NULL DEFAULT '', stanox TEXT NOT NULL, event TEXT NOT NULL DEFAULT '', platform TEXT NOT NULL DEFAULT '', step_type TEXT NOT NULL DEFAULT '', offset_sec INTEGER, source TEXT NOT NULL DEFAULT '', changed_by TEXT, changed_at TEXT DEFAULT (datetime('now')))")
            exec("CREATE INDEX IF NOT EXISTS idx_smart_berths_stanox ON smart_berths(stanox)")
            exec("CREATE INDEX IF NOT EXISTS idx_smart_berths_key ON smart_berths(td, from_berth, to_berth)")
        }
        tableReady = true
    }

    private fun loadDbEntries(): List<SmartEntry> {
        val out = ArrayList<SmartEntry>()
        transaction {
            exec("SELECT td, from_berth, to_berth, stanox, event, platform, step_type, offset_sec FROM smart_berths ORDER BY id") { rs ->
                while (rs.next()) {
                    val off = rs.getInt(8)
                    val offNull = rs.wasNull()
                    out.add(
                        SmartEntry(
                            rs.getString(1) ?: "",
                            rs.getString(2) ?: "",
                            rs.getString(3) ?: "",
                            rs.getString(4) ?: "",
                            rs.getString(5) ?: "",
                            rs.getString(6) ?: "",
                            rs.getString(7) ?: "",
                            if (offNull) null else off
                        )
                    )
                }
            }
        }
        return out
    }

    private fun syncFromSmart(entries: List<SmartEntry>, applyUpdates: Boolean = false) {
        try {
            ensureTable()
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
                    "('${sq(e.td)}','${sq(e.fromBerth)}','${sq(e.toBerth)}','${sq(e.stanox)}','${sq(e.event)}','${sq(e.platform)}','${sq(e.stepType)}',${e.offsetSec?.toString() ?: "NULL"},'$tag')"
                }
                transaction {
                    exec("INSERT INTO smart_berths (td, from_berth, to_berth, stanox, event, platform, step_type, offset_sec, source) VALUES $values")
                }
            }
            for (chunk in toUpdate.chunked(200)) {
                transaction {
                    for (e in chunk) {
                        exec("UPDATE smart_berths SET platform = '${sq(e.platform)}', step_type = '${sq(e.stepType)}', offset_sec = ${e.offsetSec?.toString() ?: "NULL"}, source = '$changedTag', changed_by = NULL, changed_at = datetime('now') WHERE td = '${sq(e.td)}' AND from_berth = '${sq(e.fromBerth)}' AND to_berth = '${sq(e.toBerth)}' AND stanox = '${sq(e.stanox)}' AND event = '${sq(e.event)}'")
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
                ensureTable()
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
        val out = ArrayList<Map<String, String>>()
        try {
            ensureTable()
            val sx = stanox.filter { it.isLetterOrDigit() }
            val t = td.filter { it.isLetterOrDigit() }.uppercase()
            val b = berth.filter { it.isLetterOrDigit() }.uppercase()
            val where = ArrayList<String>()
            if (sx.isNotEmpty()) where.add("stanox = '$sx'")
            if (t.isNotEmpty()) where.add("td = '$t'")
            if (b.isNotEmpty()) where.add("(from_berth = '$b' OR to_berth = '$b')")
            val sql = "SELECT id, td, from_berth, to_berth, stanox, event, platform, step_type, offset_sec, source, changed_by, changed_at FROM smart_berths${if (where.isEmpty()) "" else " WHERE ${where.joinToString(" AND ")}"} ORDER BY td, from_berth, to_berth LIMIT ${limit.coerceIn(1, 500)}"
            transaction {
                exec(sql) { rs ->
                    while (rs.next()) {
                        out.add(
                            mapOf(
                                "id" to (rs.getString(1) ?: ""),
                                "td" to (rs.getString(2) ?: ""),
                                "fromBerth" to (rs.getString(3) ?: ""),
                                "toBerth" to (rs.getString(4) ?: ""),
                                "stanox" to (rs.getString(5) ?: ""),
                                "event" to (rs.getString(6) ?: ""),
                                "platform" to (rs.getString(7) ?: ""),
                                "stepType" to (rs.getString(8) ?: ""),
                                "offsetSec" to (rs.getString(9) ?: ""),
                                "source" to (rs.getString(10) ?: ""),
                                "changedBy" to (rs.getString(11) ?: ""),
                                "changedAt" to (rs.getString(12) ?: "")
                            )
                        )
                    }
                }
            }
        } catch (e: Exception) {
            log.warn("listBerths failed: ${e.message}")
        }
        return out
    }

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
            ensureTable()
            transaction {
                exec("INSERT INTO smart_berths (td, from_berth, to_berth, stanox, event, platform, step_type, offset_sec, source, changed_by) VALUES ('$t','$f','$o','$sx','$ev','${sq(pl)}','${sq(st)}',$offsetSec,'manual','${sq(changedBy)}')")
            }
        } catch (e: Exception) {
            return "database error: ${e.message}"
        }
        reloadOverrides()
        return null
    }

    fun deleteBerth(id: Long) {
        try {
            ensureTable()
            transaction { exec("DELETE FROM smart_berths WHERE id = $id") }
        } catch (e: Exception) {
            log.warn("deleteBerth failed: ${e.message}")
            return
        }
        reloadOverrides()
    }
}

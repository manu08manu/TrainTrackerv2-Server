package com.traintracker.server.cif

import com.traintracker.server.auth.AuthDatabase
import com.traintracker.server.database.AppDatabase
import java.util.concurrent.ConcurrentHashMap
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("CorpusLookup")

private class MissingItemTracker(private val table: String, private val keyColumn: String) {
    private val counts = ConcurrentHashMap<String, Int>()
    private val headcodes = ConcurrentHashMap<String, String>()
    private val uids = ConcurrentHashMap<String, String>()

    fun record(key: String, headcode: String = "", uid: String = "") {
        counts.merge(key, 1) { a, b -> a + b }
        if (headcode.isNotEmpty()) headcodes[key] = headcode
        if (uid.isNotEmpty()) uids[key] = uid
    }

    fun remove(key: String) {
        counts.remove(key)
        headcodes.remove(key)
        uids.remove(key)
    }

    fun flush() {
        if (counts.isEmpty()) return
        val snapshot = HashMap(counts)
        counts.clear()
        val headcodeSnapshot = HashMap(headcodes)
        val uidSnapshot = HashMap(uids)
        try {
            transaction {
                val values = snapshot.entries.joinToString(",") { (k, c) ->
                    val hc = (headcodeSnapshot[k] ?: "").replace("'", "''")
                    val u = (uidSnapshot[k] ?: "").replace("'", "''")
                    "('${k.replace("'", "''")}', $c, '$hc', '$u')"
                }
                exec("INSERT INTO $table ($keyColumn, seen_count, headcode, uid) VALUES $values ON CONFLICT($keyColumn) DO UPDATE SET last_seen=datetime('now'), seen_count=seen_count+excluded.seen_count, headcode=CASE WHEN excluded.headcode != '' THEN excluded.headcode ELSE $table.headcode END, uid=CASE WHEN excluded.uid != '' THEN excluded.uid ELSE $table.uid END")
            }
            log.debug("Flushed ${snapshot.size} entries to $table")
        } catch (e: Exception) {
            log.warn("$table flush error: ${e.message}")
            snapshot.forEach { (k, c) -> counts.merge(k, c) { a, b -> a + b } }
        }
    }
}

object CorpusLookup {
    private val tiplocToCrs = HashMap<String, String>(10000)
    private val stanoxToCrs = HashMap<String, String>(10000)
    private val tiplocToName = HashMap<String, String>(12000)
    private val missingTiplocTracker = MissingItemTracker("missing_tiplocs", "tiploc")
    private val tscToOperator = HashMap<String, String>(3000)
    private val missingTscTracker = MissingItemTracker("missing_tscs", "tsc")
    private val stanoxToTiploc = HashMap<String, String>(10000)
    @Volatile
    private var tiplocToCifStanox: Map<String, String> = emptyMap()
    @Volatile
    private var stanoxCrsOverrides: Map<String, String> = emptyMap()
    @Volatile
    private var stanoxTiplocOverrides: Map<String, String> = emptyMap()
    private val tiplocToCifName = HashMap<String, String>(12000)
    private val noSmartTracker = MissingItemTracker("missing_smart_stanox", "stanox")
    private var noSmartTableReady = false
    private val crsToName = HashMap<String, String>(3000)
    @Volatile
    private var crsMapBuilt = false

    private fun normaliseCrs(crs: String?): String? {
        if (crs.isNullOrEmpty()) return null
        val resolved = crs
        return if (resolved.length <= 3) resolved else null
    }

    fun init() {
        try {
            val overrides = AuthDatabase.loadCrsOverrides()
            for ((k, v) in overrides) tiplocToCrs[k] = v
            log.info("CorpusLookup: loaded ${overrides.size} CRS overrides from DB")
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to load CRS overrides from DB: ${e.message}")
        }
        log.info("CorpusLookup: loaded ${tiplocToCrs.size} manual mappings")
        try {
            val overrides = AuthDatabase.loadNameOverrides()
            for ((k, v) in overrides) tiplocToName[k] = v
            log.info("CorpusLookup: loaded ${overrides.size} TIPLOC name overrides from DB")
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to load TIPLOC name overrides from DB: ${e.message}")
        }
        try {
            val tscOverrides = AuthDatabase.loadOperatorOverrides()
            for ((k, v) in tscOverrides) tscToOperator[k] = v
            log.info("CorpusLookup: loaded ${tscOverrides.size} TSC operator overrides from DB")
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to load TSC operator overrides from DB: ${e.message}")
        }
        reloadStanoxOverrides()
    }

    fun mergeFromFeed(feedEntries: Map<String, String>) {
        val dbOverrides = try {
            AuthDatabase.loadCrsOverrides()
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to reload CRS overrides before merge: ${e.message}")
            emptyMap()
        }
        for ((tiploc, crs) in feedEntries) {
            if (!dbOverrides.containsKey(tiploc)) tiplocToCrs[tiploc] = crs
        }
        for ((k, v) in dbOverrides) tiplocToCrs[k] = v
        log.info("CorpusLookup: merged ${feedEntries.size} feed entries (${dbOverrides.size} overrides protected), total=${tiplocToCrs.size}")
    }

    fun mergeStanoxFromFeed(stanoxEntries: Map<String, String>) {
        for ((stanox, crs) in stanoxEntries) {
            if (stanox.isNotEmpty() && crs.isNotEmpty()) stanoxToCrs[stanox] = crs
        }
        log.info("CorpusLookup: merged ${stanoxEntries.size} stanox entries")
    }

    fun cifStanoxForTiploc(tiploc: String): String? = tiplocToCifStanox[tiploc]

    fun mergeStanoxToTiplocFromFeed(entries: Map<String, String>) {
        stanoxToTiploc.putAll(entries)
        val byTiploc = HashMap<String, String>(stanoxToTiploc.size)
        val multi = HashSet<String>()
        for ((sx, tpx) in stanoxToTiploc) {
            val prev = byTiploc[tpx]
            if (prev != null && prev != sx) multi.add(tpx) else byTiploc[tpx] = sx
        }
        for (tp in multi) byTiploc.remove(tp)
        tiplocToCifStanox = byTiploc
        log.info("CorpusLookup: merged ${entries.size} stanox->tiploc entries")
    }

    fun reloadStanoxOverrides() {
        try {
            stanoxCrsOverrides = AuthDatabase.loadStanoxCrsOverrides()
            stanoxTiplocOverrides = AuthDatabase.loadStanoxTiplocOverrides()
            log.info("CorpusLookup: loaded ${stanoxCrsOverrides.size} STANOX->CRS and ${stanoxTiplocOverrides.size} STANOX->TIPLOC overrides from DB")
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to load STANOX overrides from DB: ${e.message}")
        }
    }

    fun tiplocFromStanox(stanox: String): String? = stanoxTiplocOverrides[stanox] ?: stanoxToTiploc[stanox]

    fun crsFromTiploc(tiploc: String): String? = normaliseCrs(tiplocToCrs[tiploc])

    fun crsFromStanox(stanox: String): String? = normaliseCrs(stanoxCrsOverrides[stanox] ?: stanoxToCrs[stanox])

    fun tiplocFromCrs(crs: String): List<String> =
        tiplocToCrs.entries.filter { it.value.equals(crs, ignoreCase = true) }
            .map { it.key }.sorted()

    private fun recordMissing(tracker: MissingItemTracker, item: String, headcode: String = "", uid: String = "") {
        tracker.record(item, headcode, uid)
    }

    private fun recordMissingTiploc(tiploc: String, headcode: String = "", uid: String = "") {
        recordMissing(missingTiplocTracker, tiploc, headcode, uid)
    }

    fun recordMissingFromNames(tiplocs: Set<String>, headcode: String = "", uid: String = "") {
        for (tiploc in tiplocs) {
            if (tiplocToName[tiploc] == null) recordMissingTiploc(tiploc, headcode, uid)
        }
    }

    fun operatorFromTsc(tsc: String): String? = tscToOperator[tsc]

    fun operatorForTsc(tsc: String): String = tsc.takeIf { it.isNotEmpty() }?.let { operatorFromTsc(it) } ?: ""

    fun tscsForOperator(operator: String): List<String> =
        tscToOperator.filterValues { it.equals(operator, ignoreCase = true) }.keys.toList()

    fun recordMissingTsc(tsc: String, headcode: String = "", uid: String = "") {
        recordMissing(missingTscTracker, tsc, headcode, uid)
    }

    fun recordMissingTscIfNoOverride(tsc: String, headcode: String = "", uid: String = "") {
        if (tsc.isNotEmpty() && tscToOperator[tsc] == null) recordMissingTsc(tsc, headcode, uid)
    }

    fun mergeCifNamesFromFeed(entries: Map<String, String>) {
        tiplocToCifName.putAll(entries)
        log.info("CorpusLookup: merged ${entries.size} CIF tps_description entries")
    }

    fun nameFromTiploc(tiploc: String): String? = tiplocToName[tiploc] ?: tiplocToCifName[tiploc]

    fun namesFromTiplocs(tiplocs: Set<String>): Map<String, String?> {
        if (tiplocs.isEmpty()) return emptyMap()
        val result = HashMap<String, String?>(tiplocs.size)
        for (t in tiplocs) result[t] = tiplocToName[t] ?: tiplocToCifName[t]
        return result
    }

    fun checkTrustMovementForMissingName(stanox: String, headcode: String = "") {
        // RECONSTRUCTED: the decompiler merged tiploc and uid into one variable; live bytecode
        // (locals tiploc/uid, `?: run { return }`).
        val tiploc = tiplocFromStanox(stanox) ?: run { return }
        if (tiplocToName[tiploc] == null) {
            val uid = if (headcode.isNotEmpty()) AppDatabase.uidForHeadcode(headcode) ?: "" else ""
            recordMissingTiploc(tiploc, headcode, uid)
        }
    }

    fun checkTrustMovementForMissingAtoc(headcode: String) {
        if (headcode.isEmpty()) return
        val (tsc, uid) = AppDatabase.tscAndUidForHeadcode(headcode) ?: return
        recordMissingTscIfNoOverride(tsc, headcode, uid)
    }

    fun flushMissingTiplocs() {
        missingTiplocTracker.flush()
    }

    fun flushMissingTscs() {
        missingTscTracker.flush()
    }

    fun recordNoSmartCoverage(stanox: String, type: String, headcode: String = "") {
        noSmartTracker.record("$stanox|${if (type == "ARRIVAL") "ARR" else "DEP"}", headcode)
    }

    fun flushNoSmartCoverage() {
        if (!noSmartTableReady) {
            transaction {
                exec("CREATE TABLE IF NOT EXISTS missing_smart_stanox (stanox TEXT PRIMARY KEY, first_seen TEXT DEFAULT (datetime('now')), last_seen TEXT DEFAULT (datetime('now')), seen_count INTEGER DEFAULT 1, headcode TEXT DEFAULT '', uid TEXT DEFAULT '')")
            }
            noSmartTableReady = true
        }
        noSmartTracker.flush()
    }

    fun tiplocNameCount(): Int = tiplocToName.size

    fun searchStations(query: String, limit: Int = 20): List<Map<String, String>> {
        val q = query.lowercase().trim()
        if (!crsMapBuilt) buildCrsMap()
        data class Candidate(val crs: String, val name: String, val priority: Int)
        return crsToName.mapNotNull { (crs, name) ->
            val nl = name.lowercase()
            val p = when {
                crs.lowercase() == q -> 0
                nl.startsWith(q) -> 1
                nl.contains(q) -> 2
                else -> return@mapNotNull null
            }
            Candidate(crs, name, p)
        }.sortedWith(compareBy({ it.priority }, { it.name }))
            .take(limit)
            .map { mapOf("crs" to it.crs, "name" to it.name) }
    }

    private fun buildCrsMap() {
        synchronized(crsToName) {
            if (crsMapBuilt) return
            try {
                transaction {
                    exec("SELECT DISTINCT crs, tiploc FROM schedules WHERE crs != '' AND crs IS NOT NULL") { rs ->
                        while (rs.next()) {
                            val crs = rs.getString("crs") ?: continue
                            val tiploc = rs.getString("tiploc") ?: continue
                            if (crsToName.containsKey(crs)) continue
                            val name = nameFromTiploc(tiploc)
                            if (!name.isNullOrEmpty() && !name.startsWith(".")) crsToName[crs] = name
                        }
                    }
                }
            } catch (e: Exception) {
                log.warn("searchStations: failed to build CRS map: ${e.message}")
            }
            for ((tiploc, crs) in tiplocToCrs) {
                if (crsToName.containsKey(crs)) continue
                val name = nameFromTiploc(tiploc) ?: continue
                if (name.startsWith(".")) continue
                crsToName[crs] = name
            }
            crsMapBuilt = true
        }
    }

    fun updateTiplocCrs(tiploc: String, crs: String, changedBy: String) {
        try {
            AuthDatabase.setCrsOverride(tiploc, crs, changedBy)
            tiplocToCrs[tiploc] = crs
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to persist CRS override to DB: ${e.message}")
            throw e
        }
        try {
            AuthDatabase.updateSchedulesCrs(tiploc, crs)
            log.info("CorpusLookup: updated schedules CRS for $tiploc -> $crs")
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to update schedules CRS for $tiploc: ${e.message}")
        }
    }

    private fun updateOverride(
        map: HashMap<String, String>,
        tracker: MissingItemTracker,
        key: String,
        value: String,
        changedBy: String,
        entityLabel: String,
        persist: (String, String, String) -> Unit
    ) {
        try {
            persist(key, value, changedBy)
            map[key] = value
            tracker.remove(key)
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to persist $entityLabel override to DB: ${e.message}")
            throw e
        }
    }

    fun updateTiplocName(tiploc: String, name: String, changedBy: String) {
        updateOverride(tiplocToName, missingTiplocTracker, tiploc, name, changedBy, "TIPLOC name", AuthDatabase::setNameOverride)
    }

    fun updateTscOperator(tsc: String, operator: String, changedBy: String) {
        updateOverride(tscToOperator, missingTscTracker, tsc, operator, changedBy, "TSC operator", AuthDatabase::setOperatorOverride)
    }
}

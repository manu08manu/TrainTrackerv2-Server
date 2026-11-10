package com.traintracker.server.cif

import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.Overrides
import java.util.concurrent.ConcurrentHashMap
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("CorpusLookup")

/** Counts sightings of unknown items in memory and adds them to a "missing" table on flush(). */
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

/**
 * In-memory location and operator lookups: TIPLOC/STANOX/CRS/name and TSC -> operator.
 * Feed data comes from LocationData; manual overrides (Overrides) always win.
 */
object CorpusLookup {
    private val tiplocToCrs = HashMap<String, String>(10000)
    private val stanoxToCrs = HashMap<String, String>(10000)
    private val tiplocToName = HashMap<String, String>(12000)
    private val missingTiplocTracker = MissingItemTracker("missing_tiplocs", "tiploc")
    private val tscToOperator = HashMap<String, String>(3000)
    private val missingTscTracker = MissingItemTracker("missing_tscs", "tsc")
    private val stanoxToTiploc = HashMap<String, String>(10000)
    @Volatile
    private var stanoxCrsOverrides: Map<String, String> = emptyMap()
    @Volatile
    private var stanoxTiplocOverrides: Map<String, String> = emptyMap()
    private val tiplocToCifName = HashMap<String, String>(12000)
    private val noSmartTracker = MissingItemTracker("missing_smart_stanox", "stanox")
    private val crsToName = HashMap<String, String>(3000)
    @Volatile
    private var crsMapBuilt = false

    /** A CRS longer than 3 characters is not a CRS. */
    private fun normaliseCrs(crs: String?): String? = crs?.takeIf { it.isNotEmpty() && it.length <= 3 }

    /** Loads the manual overrides at startup. */
    fun init() {
        loadOverrides("CRS", tiplocToCrs, Overrides::tiplocCrs)
        loadOverrides("TIPLOC name", tiplocToName, Overrides::tiplocNames)
        loadOverrides("TSC operator", tscToOperator, Overrides::tscOperators)
        reloadStanoxOverrides()
    }

    private fun loadOverrides(label: String, into: HashMap<String, String>, load: () -> Map<String, String>) {
        try {
            val overrides = load()
            into.putAll(overrides)
            log.info("CorpusLookup: loaded ${overrides.size} $label overrides from DB")
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to load $label overrides from DB: ${e.message}")
        }
    }

    /** Applies feed TIPLOC -> CRS entries; overrides keep precedence. */
    fun mergeFromFeed(feedEntries: Map<String, String>) {
        val dbOverrides = try {
            Overrides.tiplocCrs()
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

    fun mergeStanoxToTiplocFromFeed(entries: Map<String, String>) {
        stanoxToTiploc.putAll(entries)
        log.info("CorpusLookup: merged ${entries.size} stanox->tiploc entries")
    }

    fun reloadStanoxOverrides() {
        try {
            stanoxCrsOverrides = Overrides.stanoxCrs()
            stanoxTiplocOverrides = Overrides.stanoxTiplocs()
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

    /** Records each TIPLOC that has no name override. */
    fun recordMissingFromNames(tiplocs: Set<String>, headcode: String = "", uid: String = "") {
        for (tiploc in tiplocs) {
            if (tiplocToName[tiploc] == null) missingTiplocTracker.record(tiploc, headcode, uid)
        }
    }

    fun operatorFromTsc(tsc: String): String? = tscToOperator[tsc]

    fun operatorForTsc(tsc: String): String = tsc.takeIf { it.isNotEmpty() }?.let { operatorFromTsc(it) } ?: ""

    fun tscsForOperator(operator: String): List<String> =
        tscToOperator.filterValues { it.equals(operator, ignoreCase = true) }.keys.toList()

    fun recordMissingTscIfNoOverride(tsc: String, headcode: String = "", uid: String = "") {
        if (tsc.isNotEmpty() && tscToOperator[tsc] == null) missingTscTracker.record(tsc, headcode, uid)
    }

    fun mergeCifNamesFromFeed(entries: Map<String, String>) {
        tiplocToCifName.putAll(entries)
        log.info("CorpusLookup: merged ${entries.size} CIF tps_description entries")
    }

    fun nameFromTiploc(tiploc: String): String? = tiplocToName[tiploc] ?: tiplocToCifName[tiploc]

    fun namesFromTiplocs(tiplocs: Set<String>): Map<String, String?> = tiplocs.associateWith { nameFromTiploc(it) }

    /** Records the TIPLOC of a movement when it has no name override. */
    fun checkMovementForMissingName(stanox: String, headcode: String = "") {
        val tiploc = tiplocFromStanox(stanox) ?: return
        if (tiplocToName[tiploc] == null) {
            val uid = if (headcode.isNotEmpty()) AppDatabase.scheduleForHeadcode(headcode)?.second ?: "" else ""
            missingTiplocTracker.record(tiploc, headcode, uid)
        }
    }

    /** Records the TSC of today's schedule for a headcode when it has no operator. */
    fun checkMovementForMissingAtoc(headcode: String) {
        if (headcode.isEmpty()) return
        val (tsc, uid) = AppDatabase.scheduleForHeadcode(headcode) ?: return
        recordMissingTscIfNoOverride(tsc, headcode, uid)
    }

    fun flushMissingTiplocs() = missingTiplocTracker.flush()

    fun flushMissingTscs() = missingTscTracker.flush()

    fun recordNoSmartCoverage(stanox: String, type: String, headcode: String = "") {
        noSmartTracker.record("$stanox|${if (type == "ARRIVAL") "ARR" else "DEP"}", headcode)
    }

    fun flushNoSmartCoverage() = noSmartTracker.flush()

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
            Overrides.setCrs(tiploc, crs, changedBy)
            tiplocToCrs[tiploc] = crs
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to persist CRS override to DB: ${e.message}")
            throw e
        }
        try {
            Overrides.applyCrsToSchedules(tiploc, crs)
            log.info("CorpusLookup: updated schedules CRS for $tiploc -> $crs")
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to update schedules CRS for $tiploc: ${e.message}")
        }
    }

    fun updateTiplocName(tiploc: String, name: String, changedBy: String) =
        updateOverride(tiplocToName, missingTiplocTracker, tiploc, name, "TIPLOC name") { Overrides.setName(tiploc, name, changedBy) }

    fun updateTscOperator(tsc: String, operator: String) =
        updateOverride(tscToOperator, missingTscTracker, tsc, operator, "TSC operator") { Overrides.setOperator(tsc, operator) }

    private fun updateOverride(
        map: HashMap<String, String>,
        tracker: MissingItemTracker,
        key: String,
        value: String,
        label: String,
        persist: () -> Unit
    ) {
        try {
            persist()
            map[key] = value
            tracker.remove(key)
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to persist $label override to DB: ${e.message}")
            throw e
        }
    }
}

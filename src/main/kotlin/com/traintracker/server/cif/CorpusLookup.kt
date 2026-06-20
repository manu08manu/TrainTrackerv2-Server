package com.traintracker.server.cif

import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("CorpusLookup")

// ─── TIPLOC → CRS / Name lookup ──────────────────────────────────────────────

object CorpusLookup {
    private val tiplocToCrs  = HashMap<String, String>(10000)
    private val stanoxToCrs  = HashMap<String, String>(10000)
    private val tiplocToName = HashMap<String, String>(12000)




    private fun normaliseCrs(crs: String?): String? {
        if (crs.isNullOrEmpty()) return null
        val resolved = crs
        return if (resolved.length <= 3) resolved else null
    }

    /** Called once at startup — loads manual mappings and persisted tiploc names. */
    fun init() {
        // CRS overrides loaded from database
        try {
            val overrides = com.traintracker.server.auth.AuthDatabase.loadCrsOverrides()
            for ((tiploc, crs) in overrides) tiplocToCrs[tiploc] = crs
            log.info("CorpusLookup: loaded ${overrides.size} CRS overrides from DB")
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to load CRS overrides from DB: ${e.message}")
        }
        log.info("CorpusLookup: loaded ${tiplocToCrs.size} manual mappings")
        // TIPLOC name overrides loaded from database
        try {
            val overrides = com.traintracker.server.auth.AuthDatabase.loadNameOverrides()
            for ((tiploc, name) in overrides) tiplocToName[tiploc] = name
            log.info("CorpusLookup: loaded ${overrides.size} TIPLOC name overrides from DB")
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to load TIPLOC name overrides from DB: ${e.message}")
        }
    }

    /**
     * Merge TiplocV1 records from the CIF feed.
     * Must be called BEFORE schedule parsing so CRS lookups are populated.
     * Manual mappings always take priority over feed entries.
     */
    fun mergeFromFeed(feedEntries: Map<String, String>) {
        // Load DB overrides first so they take priority over feed entries
        val dbOverrides = try {
            com.traintracker.server.auth.AuthDatabase.loadCrsOverrides()
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to reload CRS overrides before merge: ${e.message}")
            emptyMap()
        }
        for ((tiploc, crs) in feedEntries) {
            // Only apply feed entry if no manual override exists for this tiploc
            if (!dbOverrides.containsKey(tiploc)) tiplocToCrs[tiploc] = crs
        }
        // Re-apply all DB overrides to guarantee they win
        for ((tiploc, crs) in dbOverrides) tiplocToCrs[tiploc] = crs
        log.info("CorpusLookup: merged ${feedEntries.size} feed entries (${dbOverrides.size} overrides protected), total=${tiplocToCrs.size}")
    }

    fun mergeStanoxFromFeed(stanoxEntries: Map<String, String>) {
        for ((stanox, crs) in stanoxEntries) {
            if (stanox.isNotEmpty() && crs.isNotEmpty())
                stanoxToCrs[stanox] = crs
        }
        log.info("CorpusLookup: merged ${stanoxEntries.size} stanox entries")
    }

    fun crsFromTiploc(tiploc: String): String? = normaliseCrs(tiplocToCrs[tiploc])
    fun crsFromStanox(stanox: String): String? = normaliseCrs(stanoxToCrs[stanox])
    fun tiplocFromCrs(crs: String): List<String> = tiplocToCrs.entries
        .filter { it.value.equals(crs, ignoreCase = true) }
        .map { it.key }
        .sorted()
    fun nameFromTiploc(tiploc: String): String? = tiplocToName[tiploc]
    fun tiplocNameCount(): Int = tiplocToName.size

    // ── Station search ────────────────────────────────────────────────────────

    // CRS -> name map, built from the schedules table on first search
    private val crsToName = HashMap<String, String>(3000)
    @Volatile private var crsMapBuilt = false

    fun searchStations(query: String, limit: Int = 20): List<Map<String, String>> {
        val q = query.lowercase().trim()
        if (!crsMapBuilt) buildCrsMap()

        data class Candidate(val crs: String, val name: String, val priority: Int)
        return crsToName.mapNotNull { (crs, name) ->
            val nl = name.lowercase()
            val p = when {
                crs.lowercase() == q -> 0
                nl.startsWith(q)     -> 1
                nl.contains(q)       -> 2
                else                 -> return@mapNotNull null
            }
            Candidate(crs, name, p)
        }
        .sortedWith(compareBy({ it.priority }, { it.name }))
        .take(limit)
        .map { mapOf("crs" to it.crs, "name" to it.name) }
    }

    private fun buildCrsMap() {
        synchronized(crsToName) {
            if (crsMapBuilt) return
            try {
                // Use the existing Exposed connection pool rather than opening a raw JDBC connection
                transaction {
                    exec("SELECT DISTINCT crs, tiploc FROM schedules WHERE crs != '' AND crs IS NOT NULL") { rs ->
                        while (rs.next()) {
                            val crs    = rs.getString("crs") ?: continue
                            val tiploc = rs.getString("tiploc") ?: continue
                            if (!crsToName.containsKey(crs)) {
                                val name = tiplocToName[tiploc]
                                if (!name.isNullOrEmpty() && !name.startsWith("."))
                                    crsToName[crs] = name
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                log.warn("searchStations: failed to build CRS map: ${e.message}")
            }
            // Also fill from tiplocToCrs for anything still missing
            for ((tiploc, crs) in tiplocToCrs) {
                if (crsToName.containsKey(crs)) continue
                val name = tiplocToName[tiploc] ?: continue
                if (!name.startsWith(".")) crsToName[crs] = name
            }
            crsMapBuilt = true
        }
    }

    fun updateTiplocCrs(tiploc: String, crs: String, changedBy: String = "admin") {
        tiplocToCrs[tiploc] = crs
        try {
            com.traintracker.server.auth.AuthDatabase.setCrsOverride(tiploc, crs, changedBy)
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to persist CRS override to DB: ${e.message}")
        }
        try {
            com.traintracker.server.auth.AuthDatabase.updateSchedulesCrs(tiploc, crs)
            log.info("CorpusLookup: updated schedules CRS for $tiploc -> $crs")
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to update schedules CRS for $tiploc: ${e.message}")
        }
    }

    fun updateTiplocName(tiploc: String, name: String, changedBy: String = "admin") {
        tiplocToName[tiploc] = name
        try {
            com.traintracker.server.auth.AuthDatabase.setNameOverride(tiploc, name, changedBy)
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to persist TIPLOC name override to DB: ${e.message}")
        }
    }
}

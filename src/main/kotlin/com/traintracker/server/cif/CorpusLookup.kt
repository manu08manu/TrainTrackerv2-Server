package com.traintracker.server.cif

import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("CorpusLookup")

// ─── TIPLOC → CRS / Name lookup ──────────────────────────────────────────────

object CorpusLookup {
    private val tiplocToCrs  = HashMap<String, String>(10000)
    private val stanoxToCrs  = HashMap<String, String>(10000)
    private val tiplocToName = HashMap<String, String>(12000)

    /**
     * Platform-level TIPLOCs that the CIF feed doesn't map to a CRS.
     * These are sub-station TIPLOCs (platforms, bays, junctions within a station)
     * that need manual resolution to their parent station CRS.
     */
    private val manualMappings = mapOf(
        // London Waterloo
        "WATRLMN" to "WAT", "WATRLWC" to "WAT", "WATRCHS" to "WAT",
        "WATRLOW" to "WAT", "WATRCLJ" to "WAT", "WATRBAL" to "WAT", "WATRCAT" to "WAT",
        // London Victoria
        "VICTRMX" to "VIC", "VICTRML" to "VIC", "VICTRIA" to "VIC", "VICTRVS" to "VIC",
        // London Paddington
        "PADTON"  to "PAD", "PADTLL"  to "PAD", "PADTONA" to "PAD",
        // London Liverpool Street
        "LIVST"   to "LST", "LIVSTLL" to "LST", "LIVSTEXT" to "LST",
        // London Euston
        "EUSTON"  to "EUS", "EUSTLL"  to "EUS", "EUSTONAL" to "EUS",
        // London Kings Cross
        "KNGX"    to "KGX", "KNGXLL"  to "KGX", "KNGXHLS" to "KGX",
        // Clapham Junction
        "CLPHMJN" to "CLJ", "CLPHMJC" to "CLJ", "CLPHMJM" to "CLJ", "CLPHMJW" to "CLJ",
        // Manchester
        "MNCRPIC" to "MAN", "MNCRVIC" to "MAN", "MNCRAIR" to "MIA",
        // Glasgow
        "GLGQUEN" to "GLC", "GLGQLDC" to "GLC", "GLGCQHL" to "GLC",
        // Birmingham
        "BHAMNWS" to "BHM", "BHAMNS"  to "BHM", "BHAMINT" to "BHI",
        // Leeds
        "LEEDSSL" to "LDS", "LEEDSNL" to "LDS",
        // Brighton
        "BRGHTNM" to "BTN", "BRGHTNB" to "BTN",
        // Edinburgh
        "EDINBUR" to "EDB", "EDINBTP" to "EDB",
        // Bristol
        "BRSTLTM" to "BRI", "BRSTLHS" to "BRI",
        // Reading
        "RDNGSTN" to "RDG", "RDNGKBJ" to "RDG",
        // London Bridge
        "LNDNBDG" to "LBG", "LNDNBDGE" to "LBG", "LNDNBDC" to "LBG", "LNDNBDE" to "LBG",
        "LNDNB9"  to "LBG", "LNDNB10" to "LBG", "LNDNB11" to "LBG", "LNDNB12" to "LBG",
        "LNDNB13" to "LBG", "LNDNB14" to "LBG", "LNDNB15" to "LBG", "LNDNB16" to "LBG",
        "LNDNASJ" to "LBG", "LNDNBCJ" to "LBG",
        // London Charing Cross
        "CHARCRS" to "CHX", "CHARCRJ" to "CHX",
        // London Cannon Street
        "CNNSTBT" to "CST", "CNNSTNJ" to "CST",
        // London Fenchurch Street
        "FNCHRST" to "FST",
        // London Blackfriars
        "BLKFRSJ" to "BFR",
        // Gatwick Airport
        "GATWICK" to "GTW", "GATWNRJ" to "GTW",
        // Heathrow
        "HTRWAPT" to "HXX",
        // Sheffield
        "SHFFLDN" to "SHF", "SHFFDVS" to "SHF",
        // Liverpool
        "LVRPLSH" to "LIV", "LVRPLLN" to "LIV",
        // Newcastle
        "NWCSTLE" to "NCL", "NWCSTLJ" to "NCL",
        // Nottingham
        "NTNGHAM" to "NOT",
        // Leicester
        "LCSTR"   to "LEI",
        // Crewe
        "CREWEMD" to "CRE", "CREWNTH" to "CRE",
        // Wembley Central
        "WMBYDC"  to "WMB", "WMBYEFR" to "WMB", "WMBYEFT" to "WMB",
        "WMBYHS"  to "WMB", "WMBYLCS" to "WMB",
        // Bushey DC
        "BUSHYDC" to "BSH",
        // Harrow & Wealdstone DC
        "HROW307" to "HRW", "HROWDC"  to "HRW",
        // Watford Junction area
        "WATFDFH" to "WFJ", "WATFDGB" to "WFJ",
        "WATFDY"  to "WFJ", "WATFJDC" to "WFJ", "WATFJSJ" to "WFJ",
        // Harlesden junction
        "HARLSJN" to "HDN",
        // Wimbledon
        "WIMB827" to "WIM",
        // Vauxhall
        "VAUXHLW" to "VXH", "VAUXHLM" to "VXH",
        // Victoria extras
        "VICTRIC" to "VIC", "VICTRIE" to "VIC",
        "FAVRSHM" to "FAV", "FAVRUPS" to "FAV UPS",
        "DOVERP"  to "DVP",
        "RAMSGTE" to "RAM", "RAMSGTD" to "RAM",
        "VICTRCR" to "VIC", "VICTCRB" to "VIC", "VICTCRS" to "VIC",
        "VICT9"  to "VIC", "VICT10" to "VIC", "VICT11" to "VIC",
        "VICT12" to "VIC", "VICT13" to "VIC", "VICT14" to "VIC",
        "VICT15" to "VIC", "VICT16" to "VIC", "VICT17" to "VIC",
        "VICT18" to "VIC", "VICT19" to "VIC",
        // Reading Platforms 4A/4B
        "RDNG4AB" to "RDG",
        // Richmond North London Line
        "RCHMNDNL" to "RMD", "RICHNLL" to "RMD",
        // Clapham Jct extras
        "CLPHMJ1" to "CLJ", "CLPHMJ2" to "CLJ",
        "CLPHMMS" to "CLJ", "CLPHMYS" to "CLJ",
    )

    private val CRS_ALIASES = mapOf(
        "SPL" to "STP", "SPX" to "STP", "ASI" to "AFK", "GCL" to "GLC",
        "LVL" to "LIV", "WJH" to "WIJ", "WJL" to "WIJ", "XRO" to "RET",
        "HEZ" to "HEW", "TAH" to "TAM", "HII" to "HHY", "XHZ" to "HHY",
        "FDX" to "ZFD", "ABX" to "ABW", "WHX" to "ZLW",
        "BLU" to "BLY", "EBF" to "EBD", "GQL" to "GLQ",
        "ALE" to "LPY", "WPH" to "WOP",
    )

    private fun normaliseCrs(crs: String?): String? {
        if (crs.isNullOrEmpty()) return null
        val resolved = CRS_ALIASES[crs] ?: crs
        return if (resolved.length <= 3) resolved else null
    }

    /** Called once at startup — loads manual mappings and persisted tiploc names. */
    fun init() {
        tiplocToCrs.putAll(manualMappings)
        log.info("CorpusLookup: loaded ${tiplocToCrs.size} manual mappings")
        val namesFile = java.io.File("/opt/traintracker/tiploc_names.json")
        if (namesFile.exists()) {
            try {
                val json = org.json.JSONObject(namesFile.readText())
                for (key in json.keys()) tiplocToName[key] = json.getString(key)
                log.info("CorpusLookup: loaded ${tiplocToName.size} TIPLOC names")
            } catch (e: Exception) {
                log.warn("CorpusLookup: failed to load tiploc_names.json: ${e.message}")
            }
        }
    }

    /**
     * Merge TiplocV1 records from the CIF feed.
     * Must be called BEFORE schedule parsing so CRS lookups are populated.
     * Manual mappings always take priority over feed entries.
     */
    fun mergeFromFeed(feedEntries: Map<String, String>) {
        for ((tiploc, crs) in feedEntries) {
            if (!manualMappings.containsKey(tiploc)) {
                tiplocToCrs[tiploc] = CRS_ALIASES[crs] ?: crs
            }
        }
        log.info("CorpusLookup: merged ${feedEntries.size} feed entries, total=${tiplocToCrs.size}")
    }

    fun mergeStanoxFromFeed(stanoxEntries: Map<String, String>) {
        for ((stanox, crs) in stanoxEntries) {
            if (stanox.isNotEmpty() && crs.isNotEmpty())
                stanoxToCrs[stanox] = CRS_ALIASES[crs] ?: crs
        }
        log.info("CorpusLookup: merged ${stanoxEntries.size} stanox entries")
    }

    fun crsFromTiploc(tiploc: String): String? = normaliseCrs(tiplocToCrs[tiploc])
    fun crsFromStanox(stanox: String): String? = normaliseCrs(stanoxToCrs[stanox])
    fun nameFromTiploc(tiploc: String): String? = tiplocToName[tiploc]
    fun tiplocNameCount(): Int = tiplocToName.size

    // ── Station search ────────────────────────────────────────────────────────

    // CRS -> name map, built from the schedules table on first search
    private val crsToName = HashMap<String, String>(3000)
    @Volatile private var crsMapBuilt = false

    fun searchStations(query: String, limit: Int = 20): List<Map<String, String>> {
        val q = query.lowercase().trim()
        if (!crsMapBuilt) buildCrsMap()

        val seen = LinkedHashMap<String, Map<String, String>>()
        // Priority: exact CRS match → starts-with → contains
        for ((crs, name) in crsToName) {
            if (crs.lowercase() == q) seen[crs] = mapOf("crs" to crs, "name" to name)
        }
        for ((crs, name) in crsToName) {
            if (!seen.containsKey(crs) && name.lowercase().startsWith(q))
                seen[crs] = mapOf("crs" to crs, "name" to name)
        }
        for ((crs, name) in crsToName) {
            if (!seen.containsKey(crs) && name.lowercase().contains(q))
                seen[crs] = mapOf("crs" to crs, "name" to name)
        }
        return seen.values
            .sortedWith(compareBy({ !it["name"]!!.lowercase().startsWith(q) }, { it["name"] }))
            .take(limit)
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

    fun updateTiplocName(tiploc: String, name: String) {
        tiplocToName[tiploc] = name
        try {
            val namesFile = java.io.File("/opt/traintracker/tiploc_names.json")
            val json = org.json.JSONObject()
            for ((k, v) in tiplocToName) json.put(k, v)
            namesFile.writeText(json.toString())
        } catch (e: Exception) {
            log.warn("CorpusLookup: failed to persist tiploc name update: ${e.message}")
        }
    }
}

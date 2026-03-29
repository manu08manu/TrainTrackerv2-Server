package com.traintracker.server.cif

import com.traintracker.server.Config
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.Schedules
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.atomic.AtomicBoolean
import org.slf4j.LoggerFactory
import java.io.BufferedReader
import java.io.InputStreamReader
import java.time.DayOfWeek
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.zip.GZIPInputStream

private val log = LoggerFactory.getLogger("CifParser")

// ─── TIPLOC → CRS lookup ──────────────────────────────────────────────────────

object CorpusLookup {
    private val tiplocToCrs = HashMap<String, String>(10000)
    private val stanoxToCrs = HashMap<String, String>(10000)

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
        "LONDBDG" to "LBG", "LONDBDE" to "LBG",
        // London Charing Cross
        "CHARCRS" to "CHX", "CHARCRJ" to "CHX",
        // London Cannon Street
        "CNNSTBT" to "CST", "CNNSTNJ" to "CST",
        // London Fenchurch Street
        "FNCHRST" to "FST",
        // London Blackfriars
        "BLKFRSJ" to "BFR",
        // London Moorgate
        "MRGТNJN" to "MOG",
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
        // Wembley Central DC/freight TIPLOCs → WMB
        "WMBYDC"  to "WMB", "WMBYEFR" to "WMB", "WMBYEFT" to "WMB",
        "WMBYHS"  to "WMB", "WMBYLCS" to "WMB",
        // Bushey DC → BSH
        "BUSHYDC" to "BSH",
        // Harrow & Wealdstone DC → HRW
        "HROW307" to "HRW", "HROWDC"  to "HRW",
        // Watford Junction area → WFJ
        "WATFDFH" to "WFJ", "WATFDGB" to "WFJ",
        "WATFDY"  to "WFJ", "WATFJDC" to "WFJ", "WATFJSJ" to "WFJ",
        // Harlesden junction → HDN
        "HARLSJN" to "HDN",
        // Wimbledon signal/platform TIPLOCs → WIM
        "WIMB827" to "WIM",
        // Vauxhall platform TIPLOCs → VXH
        "VAUXHLW" to "VXH", "VAUXHLM" to "VXH",
        // Victoria platform/zone TIPLOCs → VIC
        // (VICTRIA, VICTRMX, VICTRML, VICTRVS already above)
        "VICTRIC" to "VIC",                                    // Victoria Central Platforms
        "VICTRIE" to "VIC",                                    // Victoria Eastern Platforms (shows as "Victoria E")
        "VICTRCR" to "VIC", "VICTCRB" to "VIC", "VICTCRS" to "VIC",  // Carriage Roads
        "VICT9"  to "VIC", "VICT10" to "VIC", "VICT11" to "VIC",
        "VICT12" to "VIC", "VICT13" to "VIC", "VICT14" to "VIC",
        "VICT15" to "VIC", "VICT16" to "VIC", "VICT17" to "VIC",
        "VICT18" to "VIC", "VICT19" to "VIC",
        // Reading Platforms 4A/4B → RDG (same station from passenger perspective)
        "RDNG4AB" to "RDG",
        // Richmond North London Line → RMD
        "RCHMNDNL" to "RMD", "RICHNLL" to "RMD",
        "CLPHMJ1" to "CLJ", "CLPHMJ2" to "CLJ",  // Clapham Jct Platforms 1-2 (Overground)
        "CLPHMMS" to "CLJ", "CLPHMYS" to "CLJ",  // Clapham Jct Middle/Yard Sidings
    )

    private val CRS_ALIASES = mapOf(
        "SPL" to "STP",
        "SPX" to "STP",
        "ASI" to "AFK",
        "GCL" to "GLC",
        "LVL" to "LIV",
        "WJH" to "WIJ",
        "WJL" to "WIJ",
        "XRO" to "RET",
        "HEZ" to "HEW",
        "TAH" to "TAM",
        "HII" to "HHY",
        "XHZ" to "HHY",
        // Elizabeth line → NR equivalents
        "FDX" to "ZFD",   // Farringdon EL → Farringdon
        "ABX" to "ABW",   // Abbey Wood EL → Abbey Wood
        "WHX" to "ZLW",   // Whitechapel EL → Whitechapel
        // Bletchley — BLU (LO) → BLY
        "BLU" to "BLY",
        // Ebbsfleet — EBF → EBD
        "EBF" to "EBD",
        // Glasgow Queen Street Low Level → GLQ
        "GQL" to "GLQ",
        // Liverpool South Parkway — ALE (Allerton, defunct) → LPY
        "ALE" to "LPY",
        // Worcestershire Parkway — WPH → WOP
        "WPH" to "WOP",
    )

    private fun normaliseCrs(crs: String?): String? =
        if (crs.isNullOrEmpty()) null else CRS_ALIASES[crs] ?: crs

    /** Called once at startup — loads manual mappings into the lookup. */
    fun init() {
        tiplocToCrs.putAll(manualMappings)
        log.info("CorpusLookup: loaded ${tiplocToCrs.size} manual mappings")
    }

    /**
     * Merge TiplocV1 records collected from the CIF feed.
     * Must be called BEFORE any schedule parsing so CRS lookups are populated.
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
}

// ─── Data model ───────────────────────────────────────────────────────────────

data class CifStop(
    val uid: String,
    val headcode: String,
    val atocCode: String,
    val stpIndicator: Char,
    val tiploc: String,
    val crs: String?,
    val scheduledTime: String,
    val platform: String?,
    val isPass: Boolean,
    val stopType: String,
    val originTiploc: String,
    val destTiploc: String,
    val originCrs: String?,
    val destCrs: String?
)

// ─── Downloader ───────────────────────────────────────────────────────────────

object CifDownloader {

    private val http = HttpClient(CIO) {
        install(HttpRedirect) {
            checkHttpMethod = false
        }
        engine {
            requestTimeout = 0
        }
    }

    suspend fun downloadIfNeeded() {
        val today = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
        val lastDownload = AppDatabase.getCifMeta("last_download_date")
        if (lastDownload == today) {
            log.info("CIF up to date (last download: $today)")
            return
        }
        log.info("Downloading CIF schedule for $today…")
        download()
        AppDatabase.setCifMeta("last_download_date", today)
    }

    private suspend fun download() {
        val credentials = java.util.Base64.getEncoder()
            .encodeToString("${Config.scheduleUsername}:${Config.schedulePassword}".toByteArray())

        val response = http.get(Config.scheduleUrl) {
            header("Authorization", "Basic $credentials")
        }

        if (!response.status.isSuccess()) {
            error("CIF download failed: HTTP ${response.status}")
        }

        val bytes = response.readBytes()
        log.info("CIF downloaded ${bytes.size / 1024 / 1024}MB — parsing and inserting…")

        // Clear table once, then stream inserts in chunks as parsing proceeds
        transaction { Schedules.deleteAll() }
        val total = CifParser.parseAndPersist(bytes)
        log.info("CIF import complete — $total stops inserted")
    }
}

// ─── JSON CIF Parser ──────────────────────────────────────────────────────────

object CifParser {
    private val debugLoggedOnce = AtomicBoolean(false)

    /**
     * Collect all TiplocV1 records from a gzipped CIF byte array.
     * This is the first pass — must complete before any schedule parsing.
     */
    private data class TiplocData(val tiplocToCrs: Map<String, String>, val stanoxToCrs: Map<String, String>)

    private fun collectTiplocs(gzipBytes: ByteArray): TiplocData {
        val feedTiplocs = HashMap<String, String>(8000)
        val feedStanox  = HashMap<String, String>(8000)
        GZIPInputStream(gzipBytes.inputStream()).use { gis ->
            BufferedReader(InputStreamReader(gis, Charsets.UTF_8)).use { reader ->
                var line = reader.readLine()
                while (line != null) {
                    val trimmed = line.trim()
                    if (trimmed.startsWith("{\"TiplocV1\"")) {
                        try {
                            val obj = JSONObject(trimmed).optJSONObject("TiplocV1")
                            if (obj != null) {
                                val tiplocCode = obj.optString("tiploc_code", "").trim()
                                val crs = obj.optString("crs_code", "").trim()
                                    .ifEmpty { obj.optString("CRS", "").trim() }
                                val stanox = obj.optString("stanox", "").trim()
                                if (tiplocCode.isNotEmpty() && crs.isNotEmpty())
                                    feedTiplocs[tiplocCode] = crs
                                if (stanox.isNotEmpty() && crs.isNotEmpty())
                                    feedStanox[stanox] = crs
                            }
                        } catch (_: Exception) {}
                    }
                    line = reader.readLine()
                }
            }
        }
        return TiplocData(feedTiplocs, feedStanox)
    }

    /**
     * Parse and persist directly in chunks — avoids holding 700k stops in memory.
     *
     * TWO-PASS approach:
     *   Pass 1 — collect all TiplocV1 records and merge into CorpusLookup.
     *   Pass 2 — parse schedules, now with a fully populated CRS lookup.
     *
     * This fixes the bug where stops were written to the DB with crs=NULL because
     * mergeFromFeed() was previously called after parsing had already finished.
     */
    fun parseAndPersist(gzipBytes: ByteArray): Int {
        // ── Pass 1: collect TIPLOCs ──────────────────────────────────────
        val tiplocData = collectTiplocs(gzipBytes)
        CorpusLookup.mergeFromFeed(tiplocData.tiplocToCrs)
        CorpusLookup.mergeStanoxFromFeed(tiplocData.stanoxToCrs)

        // ── Pass 2: parse schedules with fully populated lookup ──────────
        val today    = LocalDate.now()
        val todayDow = today.dayOfWeek
        val buffer   = mutableListOf<CifStop>()
        var total    = 0
        var scheduleCount = 0
        var validCount    = 0

        fun flush() {
            if (buffer.isEmpty()) return
            transaction {
                Schedules.batchInsert(buffer, ignore = true) { s ->
                    this[Schedules.uid]           = s.uid
                    this[Schedules.headcode]      = s.headcode
                    this[Schedules.atocCode]      = s.atocCode
                    this[Schedules.stpIndicator]  = s.stpIndicator.toString().first()
                    this[Schedules.tiploc]        = s.tiploc
                    this[Schedules.crs]           = s.crs
                    this[Schedules.scheduledTime] = s.scheduledTime
                    this[Schedules.platform]      = s.platform
                    this[Schedules.isPass]        = s.isPass
                    this[Schedules.stopType]      = s.stopType
                    this[Schedules.originTiploc]  = s.originTiploc
                    this[Schedules.destTiploc]    = s.destTiploc
                    this[Schedules.originCrs]     = s.originCrs
                    this[Schedules.destCrs]       = s.destCrs
                }
            }
            total += buffer.size
            buffer.clear()
        }

        GZIPInputStream(gzipBytes.inputStream()).use { gis ->
            BufferedReader(InputStreamReader(gis, Charsets.UTF_8)).use { reader ->
                var line = reader.readLine()
                while (line != null) {
                    val trimmed = line.trim()
                    if (trimmed.startsWith("{\"JsonScheduleV1\"")) {
                        scheduleCount++
                        try {
                            val sched = JSONObject(trimmed).optJSONObject("JsonScheduleV1")
                            if (sched != null) {
                                val parsed = parseSchedule(sched, today, todayDow)
                                if (parsed.isNotEmpty()) {
                                    buffer.addAll(parsed)
                                    validCount++
                                    if (buffer.size >= 5000) flush()
                                }
                            }
                        } catch (_: Exception) {}
                    }
                    line = reader.readLine()
                }
            }
        }

        flush() // flush remaining
        log.info("CIF JSON: $scheduleCount schedules, $validCount run today → $total stops")
        return total
    }

    /**
     * Entry point for VSTP Create messages.
     * Takes a JsonScheduleV1 JSONObject (already unwrapped from the Kafka envelope)
     * and returns the CifStop rows to insert — or an empty list if the service
     * isn't running today (wrong date range, wrong day-of-week, or no stops).
     */
    fun parseScheduleForVstp(sched: JSONObject): List<CifStop> {
        val today    = LocalDate.now()
        val todayDow = today.dayOfWeek
        return parseSchedule(sched, today, todayDow)
    }

    fun parse(gzipBytes: ByteArray): List<CifStop> {
        // ── Pass 1: collect TIPLOCs ──────────────────────────────────────
        val tiplocData = collectTiplocs(gzipBytes)
        CorpusLookup.mergeFromFeed(tiplocData.tiplocToCrs)
        CorpusLookup.mergeStanoxFromFeed(tiplocData.stanoxToCrs)

        // ── Pass 2: parse schedules ──────────────────────────────────────
        val today    = LocalDate.now()
        val todayDow = today.dayOfWeek
        val stops    = mutableListOf<CifStop>()
        var scheduleCount = 0
        var validCount    = 0

        GZIPInputStream(gzipBytes.inputStream()).use { gis ->
            BufferedReader(InputStreamReader(gis, Charsets.UTF_8)).use { reader ->
                var line = reader.readLine()
                while (line != null) {
                    val trimmed = line.trim()
                    if (trimmed.startsWith("{\"JsonScheduleV1\"")) {
                        scheduleCount++
                        try {
                            val sched = JSONObject(trimmed).optJSONObject("JsonScheduleV1")
                            if (sched != null) {
                                val parsed = parseSchedule(sched, today, todayDow)
                                if (parsed.isNotEmpty()) {
                                    stops.addAll(parsed)
                                    validCount++
                                }
                            }
                        } catch (_: Exception) {}
                    }
                    line = reader.readLine()
                }
            }
        }

        log.info("CIF JSON: $scheduleCount schedules, $validCount run today → ${stops.size} stops")
        return stops
    }

    private fun parseSchedule(
        sched: JSONObject,
        today: LocalDate,
        todayDow: DayOfWeek
    ): List<CifStop> {
        val stpIndicator = sched.optString("CIF_stp_indicator", "").firstOrNull() ?: return emptyList()
        if (stpIndicator == 'C') return emptyList()
        if (sched.optString("transaction_type", "") == "Delete") return emptyList()

        val validFromStr = sched.optString("schedule_start_date", "")
        val validToStr   = sched.optString("schedule_end_date", "")
        val validFrom    = parseDate(validFromStr) ?: return emptyList()
        val validTo      = parseDate(validToStr)   ?: return emptyList()
        if (today.isBefore(validFrom) || today.isAfter(validTo)) return emptyList()

        val daysRun = sched.optString("schedule_days_runs", "")
        if (daysRun.length < 7 || daysRun[todayDow.value - 1] != '1') return emptyList()

        val uid      = sched.optString("CIF_train_uid", "").trim()
        val segArray = sched.optJSONArray("schedule_segment")
        val seg = if (segArray != null && segArray.length() > 0)
            segArray.optJSONObject(0)
        else
            sched.optJSONObject("schedule_segment")
        ?: return emptyList()

        val headcode = seg.optString("signalling_id", "").trim()
        if (headcode.isEmpty()) return emptyList()

        val atocCode = sched.optString("atoc_code", "").trim()
        val locs     = seg.optJSONArray("schedule_location") ?: return emptyList()
        if (locs.length() == 0) return emptyList()

        var originTiploc = ""
        var destTiploc   = ""
        for (i in 0 until locs.length()) {
            val loc = locs.optJSONObject(i) ?: continue
            when (loc.optString("location_type", "")) {
                "LO" -> originTiploc = loc.optString("tiploc_code", "").trim()
                "LT" -> destTiploc   = loc.optString("tiploc_code", "").trim()
            }
        }

        val originCrs = CorpusLookup.crsFromTiploc(originTiploc)
        val destCrs   = CorpusLookup.crsFromTiploc(destTiploc)
        val stops     = mutableListOf<CifStop>()

        for (i in 0 until locs.length()) {
            val loc     = locs.optJSONObject(i) ?: continue
            val locType = loc.optString("location_type", "")
            val tiploc  = loc.optString("tiploc_code", "").trim()
            if (tiploc.isEmpty()) continue

            val pubDep = loc.optString("public_departure", "").trim()
            val pubArr = loc.optString("public_arrival", "").trim()
            val wttDep = loc.optString("departure", "").trim()
            val wttArr = loc.optString("arrival", "").trim()
            val pass   = loc.optString("pass", "").trim()

            val isPass    = locType == "LI" && pubDep.isEmpty() && pubArr.isEmpty() && pass.isNotEmpty()
            val schedTime = when {
                locType == "LO" -> formatHHMM(pubDep.ifEmpty { wttDep })
                locType == "LT" -> formatHHMM(pubArr.ifEmpty { wttArr })
                pubDep.isNotEmpty() -> formatHHMM(pubDep)
                pubArr.isNotEmpty() -> formatHHMM(pubArr)
                pass.isNotEmpty()   -> formatHHMM(pass)
                wttDep.isNotEmpty() -> formatHHMM(wttDep)
                wttArr.isNotEmpty() -> formatHHMM(wttArr)
                else -> ""
            }
            if (schedTime.isEmpty() && locType != "LT") continue

            stops += CifStop(
                uid           = uid,
                headcode      = headcode,
                atocCode      = atocCode,
                stpIndicator  = stpIndicator,
                tiploc        = tiploc,
                crs           = CorpusLookup.crsFromTiploc(tiploc),
                scheduledTime = schedTime,
                platform      = loc.optString("platform", "").trim().ifEmpty { null },
                isPass        = isPass,
                stopType      = locType.ifEmpty { "LI" },
                originTiploc  = originTiploc,
                destTiploc    = destTiploc,
                originCrs     = originCrs,
                destCrs       = destCrs
            )
        }
        return stops
    }

    private fun parseDate(raw: String): LocalDate? = try {
        when {
            raw.isEmpty()     -> null
            raw.contains('-') -> LocalDate.parse(raw, DateTimeFormatter.ISO_LOCAL_DATE)
            raw.contains('/') -> LocalDate.parse(raw, DateTimeFormatter.ofPattern("dd/MM/yyyy"))
            raw.length == 6   -> LocalDate.parse(raw, DateTimeFormatter.ofPattern("yyMMdd"))
            else              -> null
        }
    } catch (_: Exception) { null }

    private fun formatHHMM(raw: String): String {
        val s = raw.trimEnd('H', 'h').trim()
        return when {
            s.length >= 4 && s.all { it.isDigit() } -> "${s.substring(0, 2)}:${s.substring(2, 4)}"
            s.contains(':') && s.length >= 5         -> s.substring(0, 5)
            else -> ""
        }
    }
}

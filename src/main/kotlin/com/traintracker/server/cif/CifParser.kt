package com.traintracker.server.cif

import com.traintracker.server.Config
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.AssociationRecord
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
    val destCrs: String?,
    val scheduleStartDate: String = ""
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
        // Persist stanox map so it can be reloaded on restart without re-downloading
        AppDatabase.saveStanoxMap(tiplocData.stanoxToCrs)

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
                    this[Schedules.destCrs]           = s.destCrs
                    this[Schedules.scheduleStartDate] = s.scheduleStartDate
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

        // -- Pass 3: parse JsonAssociationV1 records
        val associations = mutableListOf<AssociationRecord>()
        val assocStream = GZIPInputStream(gzipBytes.inputStream())
        BufferedReader(InputStreamReader(assocStream, Charsets.UTF_8)).use { reader ->
            var aline = reader.readLine()
            while (aline != null) {
                val trimmed = aline.trim()
                if (trimmed.startsWith("{\"JsonAssociationV1\"")) {
                    try {
                        val assoc = JSONObject(trimmed).optJSONObject("JsonAssociationV1")
                        if (assoc != null) {
                            val mainUid     = assoc.optString("main_train_uid", "").trim()
                            val assocUid    = assoc.optString("assoc_train_uid", "").trim()
                            val assocTiploc = assoc.optString("location", "").trim()
                            val assocType   = assoc.optString("category", "").trim()
                            val stpInd      = assoc.optString("CIF_stp_indicator", "").firstOrNull()
                            val validFrom   = parseDate(assoc.optString("assoc_start_date", ""))
                            val validTo     = parseDate(assoc.optString("assoc_end_date", ""))
                            val daysRun     = assoc.optString("assoc_days", "")
                            val okStp   = stpInd != null && stpInd != 'C'
                            val okDates = validFrom != null && validTo != null &&
                                          !today.isBefore(validFrom) && !today.isAfter(validTo)
                            val okDow   = daysRun.length < 7 || daysRun[todayDow.value - 1] == '1'
                            if (okStp && okDates && okDow &&
                                mainUid.isNotEmpty() && assocUid.isNotEmpty() && assocTiploc.isNotEmpty()) {
                                associations.add(AssociationRecord(mainUid, assocUid, assocTiploc, assocType, stpInd!!))
                            }
                        }
                    } catch (_: Exception) {}
                }
                aline = reader.readLine()
            }
        }
        AppDatabase.replaceAssociations(associations)
        log.info("CIF associations: ${associations.size} parsed and stored")

        // ── Pass 4: re-apply manual CRS overrides to schedules table ──────────────
        // This ensures any tiploc_crs_overrides set via the dashboard survive the
        // full schedules wipe+reinsert that happens on every nightly CIF refresh.
        try {
            val overrideCount = transaction {
                var count = 0
                exec(
                    "UPDATE schedules SET crs = (" +
                    "SELECT crs FROM tiploc_crs_overrides o WHERE o.tiploc = schedules.tiploc" +
                    ") WHERE tiploc IN (SELECT tiploc FROM tiploc_crs_overrides)"
                )
                exec("SELECT changes() as n") { rs -> if (rs.next()) count = rs.getInt("n") }
                count
            }
            log.info("CIF post-import: applied $overrideCount CRS override(s) to schedules table")
        } catch (e: Exception) {
            log.warn("CIF post-import: failed to apply CRS overrides: ${e.message}")
        }

        return total
    }

    /**
     * Entry point for VSTP Create messages.
     * Takes a JsonScheduleV1 JSONObject (already unwrapped from the Kafka envelope)
     * and returns the CifStop rows to insert — or an empty list if the service
     * isn't running today (wrong date range, wrong day-of-week, or no stops).
     */
    /**
     * Parse a VSTP schedule from the Kafka feed (VSTPCIFMsgV1 format).
     * This format differs from the CIF JSON format:
     *  - tiploc is at location.tiploc.tiploc_id (not tiploc_code)
     *  - times use scheduled_departure_time / public_departure_time (not departure / public_departure)
     *  - location type is inferred from CIF_activity (TB=origin, TF=dest)
     *  - schedule metadata fields use CIF_ prefix
     */
    fun parseScheduleForVstpKafka(sched: JSONObject): List<CifStop> {
        val today    = LocalDate.now()
        val todayDow = today.dayOfWeek
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
        if (uid.isEmpty()) return emptyList()
        val atocCode = sched.optString("atoc_code", "").trim()
        val segArray = sched.optJSONArray("schedule_segment")
        val seg = if (segArray != null && segArray.length() > 0)
            segArray.optJSONObject(0)
        else
            sched.optJSONObject("schedule_segment")
        ?: return emptyList()
        val headcode = seg.optString("signalling_id", "").trim()
        if (headcode.isEmpty()) return emptyList()
        val locs = seg.optJSONArray("schedule_location") ?: return emptyList()
        if (locs.length() == 0) return emptyList()
        // Determine origin/dest tiplocs from CIF_activity
        var originTiploc = ""
        var destTiploc   = ""
        for (i in 0 until locs.length()) {
            val loc      = locs.optJSONObject(i) ?: continue
            val activity = loc.optString("CIF_activity", "").trim()
            val tiploc   = loc.optJSONObject("location")
                ?.optJSONObject("tiploc")
                ?.optString("tiploc_id", "")?.trim() ?: ""
            when (activity) {
                "TB" -> originTiploc = tiploc
                "TF" -> destTiploc   = tiploc
            }
        }
        val originCrs = CorpusLookup.crsFromTiploc(originTiploc)
        val destCrs   = CorpusLookup.crsFromTiploc(destTiploc)
        val stops = mutableListOf<CifStop>()
        for (i in 0 until locs.length()) {
            val loc      = locs.optJSONObject(i) ?: continue
            val activity = loc.optString("CIF_activity", "").trim()
            val tiploc   = loc.optJSONObject("location")
                ?.optJSONObject("tiploc")
                ?.optString("tiploc_id", "")?.trim() ?: ""
            if (tiploc.isEmpty()) continue
            val locType = when (activity) {
                "TB" -> "LO"
                "TF" -> "LT"
                else -> "LI"
            }
            val pubDep  = loc.optString("public_departure_time", "").trim().replace(" ", "")
            val pubArr  = loc.optString("public_arrival_time", "").trim().replace(" ", "")
            val wttDep  = loc.optString("scheduled_departure_time", "").trim().replace(" ", "")
            val wttArr  = loc.optString("scheduled_arrival_time", "").trim().replace(" ", "")
            val pass    = loc.optString("scheduled_pass_time", "").trim().replace(" ", "")
            val crs     = CorpusLookup.crsFromTiploc(tiploc)
            val isPass  = locType == "LI" && pubDep.isEmpty() && pubArr.isEmpty() && pass.isNotEmpty()
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
                crs           = crs,
                scheduledTime = schedTime,
                platform      = "",
                isPass        = isPass,
                stopType      = locType,
                originTiploc  = originTiploc,
                destTiploc    = destTiploc,
                originCrs     = originCrs,
                destCrs       = destCrs
            )
        }
        return stops
    }

    /**
     * Parse a VSTP schedule from the Kafka feed (VSTPCIFMsgV1 format).
     * This format differs from the CIF JSON format:
     *  - tiploc is at location.tiploc.tiploc_id (not tiploc_code)
     *  - times use scheduled_departure_time / public_departure_time (not departure / public_departure)
     *  - location type is inferred from CIF_activity (TB=origin, TF=dest)
     *  - schedule metadata fields use CIF_ prefix
     */

    fun parseScheduleForVstp(sched: JSONObject): List<CifStop> {
        val today    = LocalDate.now()
        val todayDow = today.dayOfWeek
        return parseSchedule(sched, today, todayDow)
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
                uid               = uid,
                headcode          = headcode,
                atocCode          = atocCode,
                stpIndicator      = stpIndicator,
                tiploc            = tiploc,
                crs               = CorpusLookup.crsFromTiploc(tiploc),
                scheduledTime     = schedTime,
                platform          = loc.optString("platform", "").trim().ifEmpty { null },
                isPass            = isPass,
                stopType          = locType.ifEmpty { "LI" },
                originTiploc      = originTiploc,
                destTiploc        = destTiploc,
                originCrs         = originCrs,
                destCrs           = destCrs,
                scheduleStartDate = validFromStr
            )
        }
        return stops
    }

    private fun parseDate(raw: String): LocalDate? = try {
        when {
            raw.isEmpty()     -> null
            raw.contains('-') -> LocalDate.parse(raw.substringBefore('T'), DateTimeFormatter.ISO_LOCAL_DATE)
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

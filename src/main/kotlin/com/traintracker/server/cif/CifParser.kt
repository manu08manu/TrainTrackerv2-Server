package com.traintracker.server.cif

import com.traintracker.server.Config
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.AssociationRecord
import com.traintracker.server.database.Schedules
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpRedirect
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.isSuccess
import io.ktor.utils.io.readAvailable
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStreamReader
import java.time.DayOfWeek
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.GZIPInputStream
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.json.JSONObject
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("CifParser")

data class CifStop(
    val uid: String,
    val headcode: String,
    val tsc: String = "",
    val stpIndicator: Char,
    val tiploc: String,
    val crs: String?,
    val scheduledTime: String,
    val scheduledArrival: String? = null,
    val scheduledDeparture: String? = null,
    val platform: String?,
    val isPass: Boolean,
    val stopType: String,
    val originTiploc: String,
    val destTiploc: String,
    val originCrs: String?,
    val destCrs: String?,
    val scheduleStartDate: String = "",
    val scheduleEndDate: String = "",
    val sequence: Int = 0,
    val daysRun: String = "1111111"
)

object CifParser {
    private val debugLoggedOnce = AtomicBoolean(false)

    private data class TiplocData(
        val allTiplocs: Set<String>,
        val assocLines: List<String>
    )

    private fun collectTiplocs(gzipFile: File): TiplocData {
        val allTiplocs = HashSet<String>(16000)
        val assocLines = mutableListOf<String>()
        GZIPInputStream(FileInputStream(gzipFile)).use { gis ->
            BufferedReader(InputStreamReader(gis, Charsets.UTF_8)).use { reader ->
                var line = reader.readLine()
                while (line != null) {
                    val trimmed = line.trim()
                    if (trimmed.startsWith("{\"TiplocV1\"")) {
                        try {
                            val obj = JSONObject(trimmed).optJSONObject("TiplocV1")
                            if (obj != null) {
                                val tiplocCode = obj.optString("tiploc_code", "").trim()
                                if (tiplocCode.isNotEmpty()) allTiplocs.add(tiplocCode)
                            }
                        } catch (e: Exception) {
                        }
                    } else if (trimmed.startsWith("{\"JsonAssociationV1\"")) {
                        assocLines.add(trimmed)
                    }
                    line = reader.readLine()
                }
            }
        }
        return TiplocData(allTiplocs, assocLines)
    }

    fun parseAndPersist(gzipFile: File): Int {
        val tiplocData = collectTiplocs(gzipFile)
        ScheduleCancellations.begin()
        LocationData.onCif(tiplocData.allTiplocs)
        val today = LocalDate.now()
        val todayDow = today.dayOfWeek
        val buffer = mutableListOf<CifStop>()
        var total = 0
        var scheduleCount = 0
        var validCount = 0

        fun flush() {
            if (buffer.isEmpty()) return
            transaction {
                Schedules.batchInsert(buffer, ignore = true) { s ->
                    this[Schedules.uid] = s.uid
                    this[Schedules.headcode] = s.headcode
                    this[Schedules.tsc] = s.tsc
                    this[Schedules.stpIndicator] = s.stpIndicator.toString().first()
                    this[Schedules.tiploc] = s.tiploc
                    this[Schedules.crs] = s.crs
                    this[Schedules.scheduledTime] = s.scheduledTime
                    this[Schedules.scheduledArrival] = s.scheduledArrival
                    this[Schedules.scheduledDeparture] = s.scheduledDeparture
                    this[Schedules.platform] = s.platform
                    this[Schedules.isPass] = s.isPass
                    this[Schedules.stopType] = s.stopType
                    this[Schedules.originTiploc] = s.originTiploc
                    this[Schedules.destTiploc] = s.destTiploc
                    this[Schedules.originCrs] = s.originCrs
                    this[Schedules.destCrs] = s.destCrs
                    this[Schedules.scheduleStartDate] = s.scheduleStartDate
                    this[Schedules.scheduleEndDate] = s.scheduleEndDate
                    this[Schedules.scheduleDate] = today.toString()
                    this[Schedules.stopSequence] = s.sequence
                    this[Schedules.daysRun] = s.daysRun
                }
            }
            total += buffer.size
            buffer.clear()
        }

        GZIPInputStream(FileInputStream(gzipFile)).use { gis ->
            BufferedReader(InputStreamReader(gis, Charsets.UTF_8)).use { reader ->
                var line = reader.readLine()
                while (line != null) {
                    val trimmed = line.trim()
                    if (trimmed.startsWith("{\"JsonScheduleV1\"")) {
                        scheduleCount++
                        try {
                            val sched = JSONObject(trimmed).optJSONObject("JsonScheduleV1")
                            if (sched != null && sched.optString("CIF_stp_indicator", "") == "C") {
                                ScheduleCancellations.collect(sched)
                            }
                            if (sched != null) {
                                val parsed = parseSchedule(sched, today, todayDow)
                                if (parsed.isNotEmpty()) {
                                    buffer.addAll(parsed)
                                    validCount++
                                    if (buffer.size >= 5000) flush()
                                }
                            }
                        } catch (e: Exception) {
                        }
                    }
                    line = reader.readLine()
                }
            }
        }
        flush()
        ScheduleCancellations.persist()
        log.info("CIF JSON: $scheduleCount schedules, $validCount run today → $total stops")

        val associations = mutableListOf<AssociationRecord>()
        for (trimmed in tiplocData.assocLines) {
            try {
                val assoc = JSONObject(trimmed).optJSONObject("JsonAssociationV1")
                if (assoc == null) continue
                val mainUid = assoc.optString("main_train_uid", "").trim()
                val assocUid = assoc.optString("assoc_train_uid", "").trim()
                val assocTiploc = assoc.optString("location", "").trim()
                val assocType = assoc.optString("category", "").trim()
                val stpInd = assoc.optString("CIF_stp_indicator", "").firstOrNull()
                val validFrom = parseDate(assoc.optString("assoc_start_date", ""))
                val validTo = parseDate(assoc.optString("assoc_end_date", ""))
                val daysRun = assoc.optString("assoc_days", "")
                val dateInd = assoc.optString("date_indicator", "").firstOrNull()
                val okStp = stpInd != null
                val okDates = validFrom != null && validTo != null && !today.isBefore(validFrom) && !today.isAfter(validTo)
                val okDow = daysRun.length < 7 || daysRun[todayDow.value - 1] == '1'
                if (okStp && okDates && okDow && mainUid.isNotEmpty() && assocUid.isNotEmpty() && assocTiploc.isNotEmpty()) {
                    associations.add(AssociationRecord(mainUid, assocUid, assocTiploc, assocType, stpInd, dateInd))
                }
            } catch (e: Exception) {
            }
        }
        AppDatabase.replaceAssociations(associations)
        log.info("CIF associations: ${associations.size} parsed and stored")

        try {
            val overrideCount = transaction {
                var count = 0
                exec("UPDATE schedules SET crs = (SELECT crs FROM tiploc_crs_overrides o WHERE o.tiploc = schedules.tiploc) WHERE tiploc IN (SELECT tiploc FROM tiploc_crs_overrides WHERE LENGTH(crs) != 0)")
                exec("SELECT changes() as n") { rs -> if (rs.next()) count = rs.getInt("n") }
                count
            }
            log.info("CIF post-import: applied $overrideCount CRS override(s) to schedules table")
        } catch (e: Exception) {
            log.warn("CIF post-import: failed to apply CRS overrides: ${e.message}")
        }
        return total
    }

    fun parseScheduleForVstpKafka(sched: JSONObject): List<CifStop> {
        val today = LocalDate.now()
        val todayDow = today.dayOfWeek
        val stpIndicator = sched.optString("CIF_stp_indicator", "").firstOrNull() ?: return emptyList()
        if (stpIndicator == 'C') return emptyList()
        if (sched.optString("transaction_type", "") == "Delete") return emptyList()
        val validFromStr = sched.optString("schedule_start_date", "")
        val validToStr = sched.optString("schedule_end_date", "")
        val validFrom = parseDate(validFromStr) ?: return emptyList()
        val validTo = parseDate(validToStr) ?: return emptyList()
        if (today.isBefore(validFrom) || today.isAfter(validTo)) return emptyList()
        val daysRun = sched.optString("schedule_days_runs", "")
        if (daysRun.length < 7 || daysRun[todayDow.value - 1] != '1') return emptyList()
        val uid = sched.optString("CIF_train_uid", "").trim()
        if (uid.isEmpty()) return emptyList()
        val segArray = sched.optJSONArray("schedule_segment")
        val seg = if (segArray != null && segArray.length() > 0) segArray.optJSONObject(0)
        else sched.optJSONObject("schedule_segment") ?: return emptyList()
        val headcode = seg.optString("signalling_id", "").trim()
        val tscCode = seg.optString("CIF_train_service_code", "").trim()
        if (headcode.isEmpty()) return emptyList()
        val locs = seg.optJSONArray("schedule_location") ?: return emptyList()
        if (locs.length() == 0) return emptyList()

        var originTiploc = ""
        var destTiploc = ""
        for (i in 0 until locs.length()) {
            val loc = locs.optJSONObject(i) ?: continue
            val activity = loc.optString("CIF_activity", "").trim()
            val tiploc = loc.optJSONObject("location")
                ?.optJSONObject("tiploc")
                ?.optString("tiploc_id", "")
                ?.trim()
                ?: ""
            when (activity) {
                "TB" -> originTiploc = tiploc
                "TF" -> destTiploc = tiploc
            }
        }
        val originCrs = CorpusLookup.crsFromTiploc(originTiploc)
        val destCrs = CorpusLookup.crsFromTiploc(destTiploc)

        val stops = mutableListOf<CifStop>()
        for (i in 0 until locs.length()) {
            val loc = locs.optJSONObject(i) ?: continue
            val activity = loc.optString("CIF_activity", "").trim()
            val tiploc = loc.optJSONObject("location")
                ?.optJSONObject("tiploc")
                ?.optString("tiploc_id", "")
                ?.trim()
                ?: ""
            if (tiploc.isEmpty()) continue
            val locType = when (activity) {
                "TB" -> "LO"
                "TF" -> "LT"
                else -> "LI"
            }
            val pubDep = loc.optString("public_departure_time", "").trim().replace(" ", "")
            val pubArr = loc.optString("public_arrival_time", "").trim().replace(" ", "")
            val wttDep = loc.optString("scheduled_departure_time", "").trim().replace(" ", "")
            val wttArr = loc.optString("scheduled_arrival_time", "").trim().replace(" ", "")
            val pass = loc.optString("scheduled_pass_time", "").trim().replace(" ", "")
            val crs = CorpusLookup.crsFromTiploc(tiploc)
            val isPass = locType == "LI" && pubDep.isEmpty() && pubArr.isEmpty() && pass.isNotEmpty()
            val schedArr = formatHHMM(pubArr.ifEmpty { wttArr }).ifEmpty { null }
            val schedDep = formatHHMM(pubDep.ifEmpty { wttDep }).ifEmpty { null }
            val schedTime = when {
                locType == "LO" -> schedDep ?: ""
                locType == "LT" -> schedArr ?: ""
                pubDep.isNotEmpty() -> formatHHMM(pubDep)
                pubArr.isNotEmpty() -> formatHHMM(pubArr)
                pass.isNotEmpty() -> formatHHMM(pass)
                wttDep.isNotEmpty() -> formatHHMM(wttDep)
                wttArr.isNotEmpty() -> formatHHMM(wttArr)
                else -> ""
            }
            if (schedTime.isEmpty() && locType != "LT") continue
            stops += CifStop(
                uid = uid,
                headcode = headcode,
                tsc = tscCode,
                stpIndicator = stpIndicator,
                tiploc = tiploc,
                crs = crs,
                scheduledTime = schedTime,
                scheduledArrival = schedArr,
                scheduledDeparture = schedDep,
                platform = loc.optString("CIF_platform", "").trim().ifEmpty { loc.optString("platform", "").trim() }.ifEmpty { null },
                isPass = isPass,
                stopType = locType,
                originTiploc = originTiploc,
                destTiploc = destTiploc,
                originCrs = originCrs,
                destCrs = destCrs,
                scheduleStartDate = validFromStr,
                scheduleEndDate = validToStr,
                sequence = i
            )
        }
        return stops
    }

    private fun parseSchedule(sched: JSONObject, today: LocalDate, todayDow: DayOfWeek): List<CifStop> {
        val stpIndicator = sched.optString("CIF_stp_indicator", "").firstOrNull() ?: return emptyList()
        if (stpIndicator == 'C') return emptyList()
        if (sched.optString("transaction_type", "") == "Delete") return emptyList()
        val validFromStr = sched.optString("schedule_start_date", "")
        val validToStr = sched.optString("schedule_end_date", "")
        val validFrom = parseDate(validFromStr) ?: return emptyList()
        val validTo = parseDate(validToStr) ?: return emptyList()
        if (today.isBefore(validFrom) || today.isAfter(validTo)) return emptyList()
        val scheduleDaysRun = sched.optString("schedule_days_runs", "").ifEmpty { "1111111" }
        val uid = sched.optString("CIF_train_uid", "").trim()
        val segArray = sched.optJSONArray("schedule_segment")
        val seg = if (segArray != null && segArray.length() > 0) segArray.optJSONObject(0)
        else sched.optJSONObject("schedule_segment") ?: return emptyList()
        val headcode = seg.optString("signalling_id", "").trim()
        if (headcode.isEmpty()) return emptyList()
        val tscCode = seg.optString("CIF_train_service_code", "").trim()
        val locs = seg.optJSONArray("schedule_location") ?: return emptyList()
        if (locs.length() == 0) return emptyList()

        var originTiploc = ""
        var destTiploc = ""
        for (i in 0 until locs.length()) {
            val loc = locs.optJSONObject(i) ?: continue
            when (loc.optString("location_type", "")) {
                "LO" -> originTiploc = loc.optString("tiploc_code", "").trim()
                "LT" -> destTiploc = loc.optString("tiploc_code", "").trim()
            }
        }
        val originCrs = CorpusLookup.crsFromTiploc(originTiploc)
        val destCrs = CorpusLookup.crsFromTiploc(destTiploc)

        val stops = mutableListOf<CifStop>()
        for (i in 0 until locs.length()) {
            val loc = locs.optJSONObject(i) ?: continue
            val locType = loc.optString("location_type", "")
            val tiploc = loc.optString("tiploc_code", "").trim()
            if (tiploc.isEmpty()) continue
            val pubDep = loc.optString("public_departure", "").trim()
            val pubArr = loc.optString("public_arrival", "").trim()
            val wttDep = loc.optString("departure", "").trim()
            val wttArr = loc.optString("arrival", "").trim()
            val pass = loc.optString("pass", "").trim()
            val isPass = locType == "LI" && pubDep.isEmpty() && pubArr.isEmpty() && pass.isNotEmpty()
            val schedArr = formatHHMM(pubArr.ifEmpty { wttArr }).ifEmpty { null }
            val schedDep = formatHHMM(pubDep.ifEmpty { wttDep }).ifEmpty { null }
            val schedTime = when {
                locType == "LO" -> schedDep ?: ""
                locType == "LT" -> schedArr ?: ""
                pubDep.isNotEmpty() -> formatHHMM(pubDep)
                pubArr.isNotEmpty() -> formatHHMM(pubArr)
                pass.isNotEmpty() -> formatHHMM(pass)
                wttDep.isNotEmpty() -> formatHHMM(wttDep)
                wttArr.isNotEmpty() -> formatHHMM(wttArr)
                else -> ""
            }
            if (schedTime.isEmpty() && locType != "LT") continue
            stops += CifStop(
                uid = uid,
                headcode = headcode,
                tsc = tscCode,
                stpIndicator = stpIndicator,
                tiploc = tiploc,
                crs = CorpusLookup.crsFromTiploc(tiploc),
                scheduledTime = schedTime,
                scheduledArrival = schedArr,
                scheduledDeparture = schedDep,
                platform = loc.optString("CIF_platform", "").trim().ifEmpty { loc.optString("platform", "").trim() }.ifEmpty { null },
                isPass = isPass,
                stopType = locType.ifEmpty { "LI" },
                originTiploc = originTiploc,
                destTiploc = destTiploc,
                originCrs = originCrs,
                destCrs = destCrs,
                scheduleStartDate = validFromStr,
                scheduleEndDate = validToStr,
                sequence = i,
                daysRun = scheduleDaysRun
            )
        }
        return stops
    }

    private fun parseDate(raw: String): LocalDate? = try {
        when {
            raw.isEmpty() -> null
            raw.contains('-') -> LocalDate.parse(raw.substringBefore('T'), DateTimeFormatter.ISO_LOCAL_DATE)
            raw.contains('/') -> LocalDate.parse(raw, DateTimeFormatter.ofPattern("dd/MM/yyyy"))
            raw.length == 6 -> LocalDate.parse(raw, DateTimeFormatter.ofPattern("yyMMdd"))
            else -> null
        }
    } catch (e: Exception) {
        null
    }

    private fun formatHHMM(raw: String): String {
        val s = raw.trimEnd('H', 'h').trim()
        return when {
            s.length >= 4 && s.all { it.isDigit() } -> "${s.substring(0, 2)}:${s.substring(2, 4)}"
            s.contains(':') && s.length >= 5 -> s.substring(0, 5)
            else -> ""
        }
    }
}

object CifDownloader {
    private val http = HttpClient(CIO) {
        install(HttpRedirect) { checkHttpMethod = false }
        engine { requestTimeout = 0L }
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
        val auth = Base64.getEncoder().encodeToString("${Config.scheduleUsername}:${Config.schedulePassword}".toByteArray())
        val response = http.get(Config.scheduleUrl) {
            header("Authorization", "Basic $auth")
        }
        if (!response.status.isSuccess()) error("CIF download failed: HTTP ${response.status}")
        val file = File("/opt/traintracker/cif_cache.gz")
        val channel = response.bodyAsChannel()
        runCatching {
            FileOutputStream(file).buffered().use { out ->
                val buf = ByteArray(65536)
                while (true) {
                    val n = channel.readAvailable(buf)
                    if (n == -1) break
                    if (n > 0) out.write(buf, 0, n)
                }
            }
        }.onFailure { error("CIF download failed while streaming to disk: ${it.message}") }
        log.info("CIF downloaded ${file.length() / 1024 / 1024}MB — parsing and inserting…")
        transaction { exec("DELETE FROM schedules WHERE stp_indicator != 'N'") }
        val count = CifParser.parseAndPersist(file)
        log.info("CIF import complete — $count stops inserted")
    }
}

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
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Base64
import java.util.zip.GZIPInputStream
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.json.JSONObject
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("CifParser")

/** One stop of a schedule, as stored in the schedules table. */
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
    val daysRun: String = "1111111",
    val wttArrival: String? = null,
    val wttDeparture: String? = null
)

/** Inserts stops into schedules; call inside a transaction. */
fun insertStops(stops: List<CifStop>, scheduleDate: (CifStop) -> String) {
    Schedules.batchInsert(stops, ignore = true) { s ->
        this[Schedules.uid] = s.uid
        this[Schedules.headcode] = s.headcode
        this[Schedules.tsc] = s.tsc
        this[Schedules.stpIndicator] = s.stpIndicator
        this[Schedules.tiploc] = s.tiploc
        this[Schedules.crs] = s.crs
        this[Schedules.scheduledTime] = s.scheduledTime
        this[Schedules.scheduledArrival] = s.scheduledArrival
        this[Schedules.scheduledDeparture] = s.scheduledDeparture
        this[Schedules.wttArrival] = s.wttArrival
        this[Schedules.wttDeparture] = s.wttDeparture
        this[Schedules.platform] = s.platform
        this[Schedules.isPass] = s.isPass
        this[Schedules.stopType] = s.stopType
        this[Schedules.originTiploc] = s.originTiploc
        this[Schedules.destTiploc] = s.destTiploc
        this[Schedules.originCrs] = s.originCrs
        this[Schedules.destCrs] = s.destCrs
        this[Schedules.scheduleStartDate] = s.scheduleStartDate
        this[Schedules.scheduleEndDate] = s.scheduleEndDate
        this[Schedules.scheduleDate] = scheduleDate(s)
        this[Schedules.stopSequence] = s.sequence
        this[Schedules.daysRun] = s.daysRun
    }
}

object CifParser {

    /** One schedule location, read from either the CIF or the VSTP JSON layout. */
    private class RawLocation(
        val type: String,   // LO, LI or LT; "" when the CIF leaves it blank
        val tiploc: String,
        val publicDep: String,
        val publicArr: String,
        val wttDep: String,
        val wttArr: String,
        val pass: String,
        val platform: String?,
    )

    private fun platformOf(loc: JSONObject): String? =
        loc.optString("CIF_platform", "").trim().ifEmpty { loc.optString("platform", "").trim() }.ifEmpty { null }

    private fun cifLocation(loc: JSONObject) = RawLocation(
        type = loc.optString("location_type", ""),
        tiploc = loc.optString("tiploc_code", "").trim(),
        publicDep = loc.optString("public_departure", "").trim(),
        publicArr = loc.optString("public_arrival", "").trim(),
        wttDep = loc.optString("departure", "").trim(),
        wttArr = loc.optString("arrival", "").trim(),
        pass = loc.optString("pass", "").trim(),
        platform = platformOf(loc),
    )

    private fun vstpLocation(loc: JSONObject): RawLocation {
        fun time(key: String) = loc.optString(key, "").trim().replace(" ", "")
        return RawLocation(
            type = when (loc.optString("CIF_activity", "").trim()) {
                "TB" -> "LO"
                "TF" -> "LT"
                else -> "LI"
            },
            tiploc = loc.optJSONObject("location")?.optJSONObject("tiploc")?.optString("tiploc_id", "")?.trim() ?: "",
            publicDep = time("public_departure_time"),
            publicArr = time("public_arrival_time"),
            wttDep = time("scheduled_departure_time"),
            wttArr = time("scheduled_arrival_time"),
            pass = time("scheduled_pass_time"),
            platform = platformOf(loc),
        )
    }

    fun parseVstpSchedule(sched: JSONObject): List<CifStop> = parseSchedule(sched, LocalDate.now(), vstp = true)

    /**
     * Stops of a schedule valid today. Cancellations ('C') and deletes give no stops. A VSTP schedule must also
     * run on today's weekday and keeps the default days_run; a CIF schedule stores its own days_run.
     */
    private fun parseSchedule(sched: JSONObject, today: LocalDate, vstp: Boolean): List<CifStop> {
        val stpIndicator = sched.optString("CIF_stp_indicator", "").firstOrNull() ?: return emptyList()
        if (stpIndicator == 'C') return emptyList()
        if (sched.optString("transaction_type", "") == "Delete") return emptyList()
        val validFromStr = sched.optString("schedule_start_date", "")
        val validToStr = sched.optString("schedule_end_date", "")
        val validFrom = parseDate(validFromStr) ?: return emptyList()
        val validTo = parseDate(validToStr) ?: return emptyList()
        if (today.isBefore(validFrom) || today.isAfter(validTo)) return emptyList()
        val daysRunRaw = sched.optString("schedule_days_runs", "")
        if (vstp && (daysRunRaw.length < 7 || daysRunRaw[today.dayOfWeek.value - 1] != '1')) return emptyList()
        val daysRun = if (vstp) "1111111" else daysRunRaw.ifEmpty { "1111111" }
        val uid = sched.optString("CIF_train_uid", "").trim()
        if (vstp && uid.isEmpty()) return emptyList()
        val segArray = sched.optJSONArray("schedule_segment")
        val seg = (if (segArray != null && segArray.length() > 0) segArray.optJSONObject(0) else sched.optJSONObject("schedule_segment"))
            ?: return emptyList()
        val headcode = seg.optString("signalling_id", "").trim()
        if (headcode.isEmpty()) return emptyList()
        val tsc = seg.optString("CIF_train_service_code", "").trim()
        val locs = seg.optJSONArray("schedule_location") ?: return emptyList()
        if (locs.length() == 0) return emptyList()

        val locations = List(locs.length()) { i -> locs.optJSONObject(i)?.let { if (vstp) vstpLocation(it) else cifLocation(it) } }
        val originTiploc = locations.lastOrNull { it?.type == "LO" }?.tiploc ?: ""
        val destTiploc = locations.lastOrNull { it?.type == "LT" }?.tiploc ?: ""
        val originCrs = CorpusLookup.crsFromTiploc(originTiploc)
        val destCrs = CorpusLookup.crsFromTiploc(destTiploc)

        return locations.mapIndexedNotNull { i, loc ->
            if (loc == null || loc.tiploc.isEmpty()) return@mapIndexedNotNull null
            val schedArr = formatHHMM(loc.publicArr.ifEmpty { loc.wttArr }).ifEmpty { null }
            val schedDep = formatHHMM(loc.publicDep.ifEmpty { loc.wttDep }).ifEmpty { null }
            val schedTime = when {
                loc.type == "LO" -> schedDep ?: ""
                loc.type == "LT" -> schedArr ?: ""
                loc.publicDep.isNotEmpty() -> formatHHMM(loc.publicDep)
                loc.publicArr.isNotEmpty() -> formatHHMM(loc.publicArr)
                loc.pass.isNotEmpty() -> formatHHMM(loc.pass)
                loc.wttDep.isNotEmpty() -> formatHHMM(loc.wttDep)
                loc.wttArr.isNotEmpty() -> formatHHMM(loc.wttArr)
                else -> ""
            }
            if (schedTime.isEmpty() && loc.type != "LT") return@mapIndexedNotNull null
            CifStop(
                uid = uid,
                headcode = headcode,
                tsc = tsc,
                stpIndicator = stpIndicator,
                tiploc = loc.tiploc,
                crs = CorpusLookup.crsFromTiploc(loc.tiploc),
                scheduledTime = schedTime,
                scheduledArrival = schedArr,
                scheduledDeparture = schedDep,
                platform = loc.platform,
                isPass = loc.type == "LI" && loc.publicDep.isEmpty() && loc.publicArr.isEmpty() && loc.pass.isNotEmpty(),
                stopType = loc.type.ifEmpty { "LI" },
                originTiploc = originTiploc,
                destTiploc = destTiploc,
                originCrs = originCrs,
                destCrs = destCrs,
                scheduleStartDate = validFromStr,
                scheduleEndDate = validToStr,
                sequence = i,
                daysRun = daysRun,
                wttArrival = formatHHMM(loc.wttArr).ifEmpty { null },
                wttDeparture = formatHHMM(loc.wttDep).ifEmpty { null },
            )
        }
    }

    private fun forEachLine(gzipFile: File, block: (String) -> Unit) {
        GZIPInputStream(FileInputStream(gzipFile)).bufferedReader(Charsets.UTF_8).useLines { lines ->
            lines.forEach { block(it.trim()) }
        }
    }

    /**
     * Loads the CIF file into schedules (valid today), associations and cancellation windows. A first pass
     * collects the current TIPLOCs, which LocationData needs before CRS codes are looked up for the stops.
     */
    fun parseAndPersist(gzipFile: File): Int {
        val tiplocs = HashSet<String>(16000)
        val associationLines = mutableListOf<String>()
        forEachLine(gzipFile) { line ->
            if (line.startsWith("{\"TiplocV1\"")) {
                try {
                    JSONObject(line).optJSONObject("TiplocV1")?.optString("tiploc_code", "")?.trim()
                        ?.takeIf { it.isNotEmpty() }?.let { tiplocs.add(it) }
                } catch (e: Exception) {
                }
            } else if (line.startsWith("{\"JsonAssociationV1\"")) {
                associationLines.add(line)
            }
        }
        ScheduleCancellations.begin()
        LocationData.onCif(tiplocs)

        val today = LocalDate.now()
        val buffer = mutableListOf<CifStop>()
        var total = 0
        var scheduleCount = 0
        var validCount = 0
        fun flush() {
            if (buffer.isEmpty()) return
            transaction { insertStops(buffer) { today.toString() } }
            total += buffer.size
            buffer.clear()
        }
        forEachLine(gzipFile) { line ->
            if (!line.startsWith("{\"JsonScheduleV1\"")) return@forEachLine
            scheduleCount++
            try {
                val sched = JSONObject(line).optJSONObject("JsonScheduleV1") ?: return@forEachLine
                if (sched.optString("CIF_stp_indicator", "") == "C") ScheduleCancellations.collect(sched)
                val stops = parseSchedule(sched, today, vstp = false)
                if (stops.isNotEmpty()) {
                    buffer.addAll(stops)
                    validCount++
                    if (buffer.size >= 5000) flush()
                }
            } catch (e: Exception) {
            }
        }
        flush()
        ScheduleCancellations.persist()
        log.info("CIF JSON: $scheduleCount schedules, $validCount run today → $total stops")

        val associations = associationLines.mapNotNull { line ->
            try {
                parseAssociation(JSONObject(line).optJSONObject("JsonAssociationV1") ?: return@mapNotNull null, today)
            } catch (e: Exception) {
                null
            }
        }
        AppDatabase.replaceAssociations(associations)
        log.info("CIF associations: ${associations.size} parsed and stored")
        return total
    }

    /** An association valid today, or null. */
    private fun parseAssociation(assoc: JSONObject, today: LocalDate): AssociationRecord? {
        val mainUid = assoc.optString("main_train_uid", "").trim()
        val assocUid = assoc.optString("assoc_train_uid", "").trim()
        val assocTiploc = assoc.optString("location", "").trim()
        val assocType = assoc.optString("category", "").trim()
        val stpInd = assoc.optString("CIF_stp_indicator", "").firstOrNull() ?: return null
        val validFrom = parseDate(assoc.optString("assoc_start_date", "")) ?: return null
        val validTo = parseDate(assoc.optString("assoc_end_date", "")) ?: return null
        val daysRun = assoc.optString("assoc_days", "")
        val dateInd = assoc.optString("date_indicator", "").firstOrNull()
        if (today.isBefore(validFrom) || today.isAfter(validTo)) return null
        if (daysRun.length >= 7 && daysRun[today.dayOfWeek.value - 1] != '1') return null
        if (mainUid.isEmpty() || assocUid.isEmpty() || assocTiploc.isEmpty()) return null
        return AssociationRecord(mainUid, assocUid, assocTiploc, assocType, stpInd, dateInd)
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

    /** "1234", "1234H" or "12:34..." -> "12:34"; anything else -> "". */
    private fun formatHHMM(raw: String): String {
        val s = raw.trimEnd('H', 'h').trim()
        return when {
            s.length >= 4 && s.all { it.isDigit() } -> "${s.substring(0, 2)}:${s.substring(2, 4)}"
            s.contains(':') && s.length >= 5 -> s.substring(0, 5)
            else -> ""
        }
    }
}

/** Downloads the full CIF once a day and reloads the timetable from it. */
object CifDownloader {
    private val http = HttpClient(CIO) {
        install(HttpRedirect) { checkHttpMethod = false }
        engine { requestTimeout = 0L }
    }

    suspend fun downloadIfNeeded() {
        val today = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
        if (AppDatabase.getCifMeta("last_download_date") == today) {
            log.info("CIF up to date (last download: $today)")
            return
        }
        log.info("Downloading CIF schedule for $today…")
        download()
        AppDatabase.setCifMeta("last_download_date", today)
    }

    private suspend fun download() {
        val auth = Base64.getEncoder().encodeToString("${Config.scheduleUsername}:${Config.schedulePassword}".toByteArray())
        val response = http.get(Config.scheduleUrl) { header("Authorization", "Basic $auth") }
        if (!response.status.isSuccess()) error("CIF download failed: HTTP ${response.status}")
        val file = Config.CIF_CACHE
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
        // VSTP ('N') schedules come from Kafka and survive the reload.
        transaction { exec("DELETE FROM schedules WHERE stp_indicator != 'N'") }
        val count = CifParser.parseAndPersist(file)
        log.info("CIF import complete — $count stops inserted")
    }
}

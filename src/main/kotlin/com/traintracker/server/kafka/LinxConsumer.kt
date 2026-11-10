package com.traintracker.server.kafka

import com.traintracker.server.Config
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.MovementBatch
import com.traintracker.server.database.execUpdate
import com.traintracker.server.database.query
import java.io.StringReader
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import javax.xml.parsers.DocumentBuilderFactory
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory
import org.w3c.dom.Element
import org.xml.sax.InputSource

private val log = LoggerFactory.getLogger("LinxConsumer")

/**
 * Network Rail LINX EI (TAF/TSI XML), the successor to the TRUST movements feed and the source of all train
 * running data. Each poll is stored in the linx_* tables (kept briefly, for the TD platform fill and for
 * debugging) and handed to [LinxMovementWriter], which writes trust_movements / train_activations.
 *
 * Flows (Kafka header "linx.flow"):
 *  - S509 Train Activation (PathDetailsMessage): uid, origin and its departure time.
 *  - S510 Train Running Information: TrainLocationStatus 01 = arrival at destination, 03 = arrival,
 *    04 = departure or pass (there is no pass flag; passes are told apart with the timetable).
 *  - S513 Train Journey Modification: 91 = cancellation, 92 = cancellation en route, 94 = change of origin,
 *    96 = reinstatement; the reason is the usual TRUST delay code.
 *  - S515 Change of Identity: modification code 07; OperationalTrainNumber is the new headcode and
 *    ReferenceOTN the old one. The train ID does not change.
 * Unknown flows and parse failures are sampled into linx_raw.
 *
 * Identifiers: the "TR" core is the TRUST train ID with a "--" prefix; the "PA" core is headcode + uid + 2
 * digits. LocationPrimaryCode is not a STANOX, so locations are keyed by TIPLOC and mapped to STANOX here.
 */
object LinxConsumer {
    const val TOPIC = "prod-1033-NWR-LINX-EI1_0"
    private const val RAW_SAMPLE_LIMIT = 200 // raw messages kept per poll window for unknown flows / failures

    private val pending = ConcurrentLinkedQueue<Pending>()
    private val counts = ConcurrentHashMap<String, AtomicLong>()
    private var lastReport = System.currentTimeMillis()
    private var lastPlatformFill = 0L
    private var rawThisMinute = 0

    /** TIPLOC -> STANOX from the RDM location files, refreshed hourly. */
    @Volatile private var tiplocStanox: Map<String, String> = emptyMap()
    @Volatile private var tiplocStanoxLoadedAt = 0L

    private sealed class Pending
    private class Movement(
        val trainId: String, val uid: String, val headcode: String, val startDate: String,
        val tiploc: String, val stanox: String?, val crs: String?, val locStatus: String, val eventType: String,
        val bookedTs: String, val actualTs: String, val delay: Int?, val referenceOtn: String?, val receivedMs: Long,
    ) : Pending()
    private class Activation(
        val trainId: String, val uid: String, val headcode: String, val startDate: String,
        val originTiploc: String?, val originStanox: String?, val originTime: String?, val receivedMs: Long,
    ) : Pending()
    private class Modification(
        val trainId: String, val uid: String, val headcode: String, val startDate: String,
        val code: String, val kind: String, val tiploc: String?, val stanox: String?, val time: String?,
        val reason: String?, val modifiedAt: String?, val receivedMs: Long, val previousHeadcode: String? = null,
    ) : Pending()
    private class Raw(val flow: String, val note: String, val xml: String, val receivedMs: Long) : Pending()

    fun start(scope: CoroutineScope): Job =
        launchConsumer(scope, "LINX", TOPIC, Config.kafkaGroupIdLinx, log) { _, records ->
            for (record in records) {
                val flow = record.headers().lastHeader("linx.flow")?.value()?.let { String(it) } ?: "?"
                record.value()?.let { handle(flow, it, record.timestamp()) }
            }
            flush()
        }

    private fun count(key: String) = counts.getOrPut(key) { AtomicLong() }.incrementAndGet()

    private fun stanoxFor(tiploc: String?): String? {
        if (tiploc.isNullOrEmpty()) return null
        val now = System.currentTimeMillis()
        if (now - tiplocStanoxLoadedAt > 3_600_000L) {
            tiplocStanoxLoadedAt = now
            try {
                val map = HashMap<String, String>()
                transaction {
                    // NLC first, PIF last so PIF wins where both have one.
                    for (table in listOf("nlc_loc", "pif_loc")) {
                        query("SELECT tiploc, stanox FROM $table WHERE stanox IS NOT NULL AND stanox != ''") { rs ->
                            while (rs.next()) map[rs.getString(1)] = rs.getString(2)
                        }
                    }
                }
                tiplocStanox = map
            } catch (e: Exception) {
                log.warn("LINX TIPLOC->STANOX load failed: ${e.message}")
            }
        }
        return tiplocStanox[tiploc]
    }

    // ---- XML helpers (default namespace, so element names are unprefixed) ----

    private val dbf: DocumentBuilderFactory = DocumentBuilderFactory.newInstance().apply {
        isNamespaceAware = false
        setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
        setFeature("http://xml.org/sax/features/external-general-entities", false)
        setFeature("http://xml.org/sax/features/external-parameter-entities", false)
        isXIncludeAware = false
        isExpandEntityReferences = false
    }

    private fun Element.first(tag: String): Element? = getElementsByTagName(tag).item(0) as? Element
    private fun Element.text(tag: String): String? = first(tag)?.textContent?.trim()?.takeIf { it.isNotEmpty() }

    /** (TR core, PA core) from the TransportOperationalIdentifiers / PlannedTransportIdentifiers blocks. */
    private fun identifiers(root: Element): Pair<String?, String?> {
        var tr: String? = null
        var pa: String? = null
        val types = root.getElementsByTagName("ObjectType")
        for (i in 0 until types.length) {
            val block = types.item(i).parentNode as? Element ?: continue
            when (types.item(i).textContent.trim()) {
                "TR" -> tr = tr ?: block.text("Core")
                "PA" -> pa = pa ?: block.text("Core")
            }
        }
        return tr to pa
    }

    private class Ids(val trainId: String, val uid: String, val headcode: String, val startDate: String)

    private fun ids(root: Element): Ids? {
        val (tr, pa) = identifiers(root)
        val trainId = tr?.trimStart('-')?.takeIf { it.length == 10 && it.all(Char::isLetterOrDigit) } ?: return null
        // Headcode + uid + 2 digits; a 5-character uid (VSTP and special workings) is space-padded: "1Z11 3224112".
        val uid = pa?.takeIf { it.length >= 10 }?.substring(4, 10)?.trim()?.takeIf { it.isNotEmpty() && it.all(Char::isLetterOrDigit) } ?: ""
        val headcode = root.text("OperationalTrainNumber")?.take(4) ?: trainId.substring(2, 6)
        val startDate = root.text("StartDate")?.take(10) ?: ""
        return Ids(trainId, uid, headcode, startDate)
    }

    /** LocationSubsidiaryCode (the TIPLOC) inside [el]. */
    private fun tiplocIn(el: Element?): String? = el?.text("LocationSubsidiaryCode")?.takeIf { it.length <= 7 && it.all(Char::isLetterOrDigit) }

    private fun handle(flow: String, xml: String, tsMs: Long) {
        count("flow:$flow")
        try {
            val root = dbf.newDocumentBuilder().parse(InputSource(StringReader(xml))).documentElement
            when (flow) {
                "S510" -> running(root, tsMs)
                "S509" -> activation(root, tsMs)
                "S513", "S515" -> modification(root, tsMs)   // same TrainJourneyModificationMessage
                else -> raw(flow, "unhandled flow", xml, tsMs)
            }
        } catch (e: Exception) {
            count("error")
            raw(flow, "parse error: ${e.message?.take(200)}", xml, tsMs)
        }
    }

    private fun raw(flow: String, note: String, xml: String, tsMs: Long) {
        if (rawThisMinute >= RAW_SAMPLE_LIMIT) return
        rawThisMinute++
        pending.add(Raw(flow, note, xml.take(20_000), tsMs))
    }

    private fun running(root: Element, tsMs: Long) {
        val ids = ids(root) ?: return run { count("S510:no-id") }
        val report = root.first("TrainLocationReport") ?: return run { count("S510:no-report") }
        val tiploc = tiplocIn(report.first("Location")) ?: return run { count("S510:no-tiploc") }
        val status = report.text("TrainLocationStatus") ?: ""
        val eventType = when (status) {
            "01" -> "DESTINATION"
            "03" -> "ARRIVAL"
            "04" -> "DEPARTURE"
            else -> "STATUS_$status"
        }
        val stanox = stanoxFor(tiploc)
        if (stanox == null) count("S510:no-stanox")
        pending.add(
            Movement(
                ids.trainId, ids.uid, ids.headcode, ids.startDate, tiploc, stanox, CorpusLookup.crsFromTiploc(tiploc),
                status, eventType,
                bookedTs = report.text("BookedLocationDateTime") ?: "",
                actualTs = report.text("LocationDateTime") ?: "",
                delay = report.first("TrainDelay")?.text("AgainstBooked")?.toIntOrNull(),
                referenceOtn = root.text("ReferenceOTN"),
                receivedMs = tsMs,
            )
        )
    }

    private fun activation(root: Element, tsMs: Long) {
        val ids = ids(root) ?: return run { count("S509:no-id") }
        // The first PlannedJourneyLocation is the origin; it also carries the UID parameter.
        val origin = root.first("PlannedJourneyLocation")
        var uid = ids.uid
        val params = root.getElementsByTagName("NetworkSpecificParameter")
        for (i in 0 until params.length) {
            val p = params.item(i) as? Element ?: continue
            if (p.text("Name") == "UID") p.text("Value")?.takeIf { it.all(Char::isLetterOrDigit) }?.let { uid = it }
        }
        val tiploc = tiplocIn(origin)
        pending.add(Activation(ids.trainId, uid, ids.headcode, ids.startDate, tiploc, stanoxFor(tiploc), origin?.text("Time"), tsMs))
    }

    private fun modification(root: Element, tsMs: Long) {
        val ids = ids(root) ?: return run { count("modification:no-id") }
        val mod = root.first("TrainJourneyModification")
        val code = mod?.text("TrainJourneyModificationIndicator") ?: ""
        val kind = when (code) {
            "91" -> "CANCELLATION"
            "92" -> "CANCELLATION_EN_ROUTE"
            "94" -> "CHANGE_OF_ORIGIN"
            "96" -> "REINSTATEMENT"
            "07" -> "CHANGE_OF_IDENTITY"   // S515: OperationalTrainNumber is the new headcode, ReferenceOTN the old
            else -> "CODE_$code"
        }
        val loc = mod?.first("LocationModified")
        val tiploc = tiplocIn(loc)
        val reasonEl = root.first("ModificationReason")
        val reason = reasonEl?.getAttribute("NationalDelayCode")?.takeIf { it.isNotEmpty() }
        pending.add(
            Modification(
                ids.trainId, ids.uid, ids.headcode, ids.startDate, code, kind, tiploc, stanoxFor(tiploc),
                loc?.text("Time"), reason, root.text("TrainJourneyModificationTime"), tsMs,
                previousHeadcode = root.first("ReferenceOTN")?.text("OperationalTrainNumber")?.take(4),
            )
        )
    }

    private fun flush() {
        val batch = ArrayList<Pending>()
        while (true) batch.add(pending.poll() ?: break)
        if (batch.isNotEmpty()) {
            try {
                transaction {
                    for (p in batch) when (p) {
                        is Movement -> execUpdate(
                            "INSERT OR REPLACE INTO linx_movements (train_id, uid, headcode, start_date, tiploc, stanox, crs, loc_status, event_type, booked_ts, actual_ts, booked_time, actual_time, delay, reference_otn, received_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            listOf(
                                p.trainId, p.uid, p.headcode, p.startDate, p.tiploc, p.stanox, p.crs, p.locStatus, p.eventType,
                                p.bookedTs, p.actualTs, hhmm(p.bookedTs), hhmm(p.actualTs), p.delay?.toString(), p.referenceOtn, p.receivedMs.toString(),
                            )
                        )
                        is Activation -> execUpdate(
                            "INSERT OR REPLACE INTO linx_activations (train_id, uid, headcode, start_date, origin_tiploc, origin_stanox, origin_time, received_ms) VALUES (?,?,?,?,?,?,?,?)",
                            listOf(p.trainId, p.uid, p.headcode, p.startDate, p.originTiploc, p.originStanox, p.originTime, p.receivedMs.toString())
                        )
                        is Modification -> execUpdate(
                            "INSERT INTO linx_modifications (train_id, uid, headcode, start_date, code, kind, tiploc, stanox, time, reason, modified_at, received_ms, previous_headcode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            listOf(p.trainId, p.uid, p.headcode, p.startDate, p.code, p.kind, p.tiploc, p.stanox, p.time, p.reason, p.modifiedAt, p.receivedMs.toString(), p.previousHeadcode)
                        )
                        is Raw -> execUpdate(
                            "INSERT INTO linx_raw (flow, note, xml, received_ms) VALUES (?,?,?,?)",
                            listOf(p.flow, p.note, p.xml, p.receivedMs.toString())
                        )
                    }
                }
                writeMovements(batch)
                counts.getOrPut("written") { AtomicLong() }.addAndGet(batch.size.toLong())
            } catch (e: Exception) {
                counts.getOrPut("write-failed") { AtomicLong() }.addAndGet(batch.size.toLong())
                log.warn("LINX write failed (${batch.size} rows): ${e.message}")
            }
        }
        val now = System.currentTimeMillis()
        if (now - lastPlatformFill >= 60_000L) {
            lastPlatformFill = now
            fillPlatformsFromTd()
        }
        if (now - lastReport >= 60_000L) {
            val summary = counts.entries.sortedBy { it.key }.joinToString(" ") { "${it.key}=${it.value.getAndSet(0)}" }
            log.info("LINX last minute: $summary")
            lastReport = now
            rawThisMinute = 0
        }
    }

    /**
     * LINX has no platforms. For arrivals and departures received 2-15 minutes ago, takes the platform of the
     * TD event (SMART berth platform) for the same headcode, STANOX and arrival/departure, nearest in time and
     * within 3 minutes. Rows with no match are marked so they are not retried.
     */
    private fun fillPlatformsFromTd() {
        val now = System.currentTimeMillis()
        try {
            transaction {
                class Row(val key: List<String>, val headcode: String, val stanox: String?, val eventType: String, val actualS: Long?) {
                    val tdType get() = if (eventType == "DEPARTURE") "D" else "A"
                }
                val rows = ArrayList<Row>()
                query(
                    "SELECT train_id, tiploc, loc_status, booked_ts, headcode, stanox, event_type, actual_ts FROM linx_movements WHERE platform_source IS NULL AND received_ms BETWEEN ? AND ?",
                    listOf((now - 15 * 60_000L).toString(), (now - 2 * 60_000L).toString())
                ) { rs ->
                    while (rs.next()) {
                        rows += Row(
                            listOf(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)),
                            rs.getString(5), rs.getString(6), rs.getString(7),
                            epochSeconds(rs.getString(8)),
                        )
                    }
                }
                for (r in rows) {
                    var best: Pair<Long, String>? = null
                    if (r.stanox != null && r.actualS != null) {
                        query(
                            "SELECT event_s, platform FROM td_events WHERE headcode = ? AND stanox = ? AND event_type = ? AND event_s BETWEEN ? AND ? AND platform IS NOT NULL AND platform != ''",
                            listOf(r.headcode, r.stanox, r.tdType, (r.actualS - 180).toString(), (r.actualS + 180).toString())
                        ) { rs ->
                            while (rs.next()) {
                                val gap = kotlin.math.abs(rs.getLong(1) - r.actualS)
                                if (best == null || gap < best!!.first) best = gap to rs.getString(2)
                            }
                        }
                    }
                    execUpdate(
                        "UPDATE linx_movements SET platform = ?, platform_source = ? WHERE train_id = ? AND tiploc = ? AND loc_status = ? AND booked_ts = ?",
                        listOf(best?.second, if (best != null) "TD" else "none") + r.key
                    )
                    best?.let { LinxMovementWriter.platform(r.key[0], r.stanox, r.eventType, it.second) }
                }
            }
        } catch (e: Exception) {
            log.warn("LINX platform fill failed: ${e.message}")
        }
    }

    /** Also writes TRUST-shaped rows (trust_movements, train_activations) so every reader uses LINX data. */
    private fun writeMovements(batch: List<Pending>) {
        try {
            val rows = ArrayList<MovementBatch>()
            for (p in batch) when (p) {
                is Movement -> LinxMovementWriter.movement(
                    p.trainId, p.uid, p.headcode, p.stanox, p.crs, p.eventType, hhmm(p.bookedTs), hhmm(p.actualTs), p.delay
                )?.let(rows::add)
                is Activation -> LinxMovementWriter.activation(p.trainId, p.uid, p.originStanox)
                is Modification -> when (p.kind) {
                    "CANCELLATION", "CANCELLATION_EN_ROUTE" ->
                        LinxMovementWriter.cancellation(p.trainId, p.uid, p.headcode, p.stanox, p.time?.take(5) ?: "", p.reason)
                    "CHANGE_OF_IDENTITY" -> p.previousHeadcode?.let { LinxMovementWriter.changeOfIdentity(p.trainId, p.uid, it, p.headcode) }
                    "REINSTATEMENT" -> LinxMovementWriter.reinstatement(p.trainId, p.uid, p.headcode, p.stanox, p.time?.take(5) ?: "")
                    "CHANGE_OF_ORIGIN" -> LinxMovementWriter.changeOfOrigin(
                        p.trainId, p.uid, p.headcode, p.startDate, p.tiploc, p.stanox, p.reason, ::stanoxFor
                    )
                    else -> {}
                }
                is Raw -> {}
            }
            AppDatabase.batchUpsertMovements(rows)
        } catch (e: Exception) {
            log.warn("LINX movement write failed: ${e.message}")
        }
    }

    /** ISO date-time with offset -> epoch seconds; null when unparseable. */
    private fun epochSeconds(ts: String?): Long? = try {
        ts?.let { java.time.OffsetDateTime.parse(it).toEpochSecond() }
    } catch (e: Exception) {
        null
    }

    /** "2026-11-10T13:31:00-00:00" -> "13:31"; "" when missing. */
    private fun hhmm(ts: String): String = if (ts.length >= 16) ts.substring(11, 16) else ""
}

package com.traintracker.server.kafka

import com.traintracker.server.Config
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.MovementBatch
import com.traintracker.server.database.execUpdate
import com.traintracker.server.database.query
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("LinxConsumer")

/**
 * Network Rail LINX EI (TAF/TSI XML): an alternative to the TRUST movements feed, which TrainTracker uses
 * instead of it as its only source of train running data (both feeds remain available). Messages are parsed by [LinxParser]; each poll is stored in the linx_* tables (kept briefly,
 * for the TD platform fill and for debugging) and handed to [LinxMovementWriter], which writes movements /
 * train_activations in UK clock time (see [LinxClock]).
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
 */
object LinxConsumer {
    const val TOPIC = "prod-1033-NWR-LINX-EI1_0"
    private const val RAW_SAMPLE_LIMIT = 200 // raw messages kept per minute for unknown flows / failures
    private const val CLOCK_VOTES_NEEDED = 20
    private val HANDLED_MODIFICATIONS = setOf("91", "92", "94", "96", "07", "06", "95")

    private val pending = ConcurrentLinkedQueue<LinxMessage>()
    private val counts = ConcurrentHashMap<String, AtomicLong>()
    private var lastReport = System.currentTimeMillis()
    private var lastPlatformFill = 0L
    private var rawThisMinute = 0

    /** True when LINX's "-00:00" times are really UK clock time; settled by [decideClock]. */
    @Volatile private var labelsUkTime = false
    private var clockDecided = false
    private var votesUkTime = 0
    private var votesUtc = 0

    /** TIPLOC -> STANOX from the RDM location files, refreshed hourly. */
    @Volatile private var tiplocStanox: Map<String, String> = emptyMap()
    @Volatile private var tiplocStanoxLoadedAt = 0L

    private val parser = LinxParser(::stanoxFor, CorpusLookup::crsFromTiploc)

    fun start(scope: CoroutineScope): Job =
        launchConsumer(scope, "LINX", TOPIC, Config.kafkaGroupIdLinx, log) { _, records ->
            for (record in records) {
                val flow = record.headers().lastHeader("linx.flow")?.value()?.let { String(it) } ?: "?"
                record.value()?.let { handle(flow, it, record.timestamp()) }
            }
            flush()
        }

    private fun count(key: String) = counts.getOrPut(key) { AtomicLong() }.incrementAndGet()

    private fun stanoxFor(tiploc: String): String? {
        if (tiploc.isEmpty()) return null
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

    private fun handle(flow: String, xml: String, kafkaMs: Long) {
        count("flow:$flow")
        val parsed = try {
            parser.parse(flow, xml, kafkaMs)
        } catch (e: Exception) {
            count("error")
            raw(LinxRaw(flow, "parse error: ${e.message?.take(200)}", xml, kafkaMs))
            return
        }
        when (LinxClock.vote(parsed.sentAt, kafkaMs)) {
            true -> votesUkTime++
            false -> votesUtc++
            null -> {}
        }
        parsed.problem?.let(::count)
        when (val m = parsed.message) {
            null -> {}
            is LinxRaw -> raw(m)
            else -> pending.add(m)
        }
        // Modification codes not yet acted on (e.g. 06, 93) are kept whole, to learn their layout.
        if (parsed.message is LinxModification && parsed.message.code !in HANDLED_MODIFICATIONS) {
            raw(LinxRaw(flow, "modification ${parsed.message.code}", xml, kafkaMs))
        }
    }

    private fun raw(m: LinxRaw) {
        if (rawThisMinute >= RAW_SAMPLE_LIMIT) return
        rawThisMinute++
        pending.add(m.copy(xml = m.xml.take(20_000)))
    }

    /** Settles [labelsUkTime] once enough messages have voted; runs before a poll's times are converted. */
    private fun decideClock() {
        if (votesUkTime + votesUtc < CLOCK_VOTES_NEEDED) return
        val ukTime = votesUkTime > votesUtc
        if (!clockDecided || ukTime != labelsUkTime) {
            log.info("LINX times are ${if (ukTime) "UK clock time labelled as UTC" else "true UTC"} ($votesUkTime vs $votesUtc messages)")
        }
        labelsUkTime = ukTime
        clockDecided = true
        votesUkTime = 0
        votesUtc = 0
    }

    private fun ukTime(ts: String) = LinxClock.ukTime(ts, labelsUkTime)

    private fun flush() {
        decideClock()
        val batch = ArrayList<LinxMessage>()
        while (true) batch.add(pending.poll() ?: break)
        if (batch.isNotEmpty()) {
            try {
                transaction {
                    for (p in batch) when (p) {
                        is LinxMovement -> execUpdate(
                            "INSERT OR REPLACE INTO linx_movements (train_id, uid, headcode, start_date, tiploc, stanox, crs, loc_status, event_type, booked_ts, actual_ts, booked_time, actual_time, delay, reference_otn, received_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            listOf(
                                p.trainId, p.uid, p.headcode, p.startDate, p.tiploc, p.stanox, p.crs, p.locStatus, p.eventType,
                                p.bookedTs, p.actualTs, ukTime(p.bookedTs), ukTime(p.actualTs), p.delay?.toString(), p.referenceOtn, p.receivedMs.toString(),
                            )
                        )
                        is LinxActivation -> execUpdate(
                            "INSERT OR REPLACE INTO linx_activations (train_id, uid, headcode, start_date, origin_tiploc, origin_stanox, origin_time, received_ms) VALUES (?,?,?,?,?,?,?,?)",
                            listOf(p.trainId, p.uid, p.headcode, p.startDate, p.originTiploc, p.originStanox, p.originTime, p.receivedMs.toString())
                        )
                        is LinxModification -> execUpdate(
                            "INSERT INTO linx_modifications (train_id, uid, headcode, start_date, code, kind, tiploc, stanox, time, reason, modified_at, received_ms, previous_headcode, locations) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            listOf(p.trainId, p.uid, p.headcode, p.startDate, p.code, p.kind, p.tiploc, p.stanox, p.time, p.reason, p.modifiedAt, p.receivedMs.toString(), p.previousHeadcode, p.locations)
                        )
                        is LinxRaw -> execUpdate(
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
                            LinxClock.epochSeconds(rs.getString(8), labelsUkTime),
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

    /** Also writes the movements / train_activations rows every reader uses. */
    private fun writeMovements(batch: List<LinxMessage>) {
        try {
            val rows = ArrayList<MovementBatch>()
            for (p in batch) when (p) {
                is LinxMovement -> LinxMovementWriter.movement(
                    p.trainId, p.uid, p.headcode, p.stanox, p.crs, p.eventType, ukTime(p.bookedTs), ukTime(p.actualTs), p.delay
                )?.let(rows::add)
                is LinxActivation -> LinxMovementWriter.activation(p.trainId, p.uid, p.originStanox)
                is LinxModification -> when (p.kind) {
                    "CANCELLATION", "CANCELLATION_EN_ROUTE" -> LinxMovementWriter.cancellation(
                        p.trainId, p.uid, p.headcode, p.stanox, LinxClock.ukClock(p.time, p.startDate, labelsUkTime), p.reason
                    )
                    "CHANGE_OF_IDENTITY" -> p.previousHeadcode?.let { LinxMovementWriter.changeOfIdentity(p.trainId, p.uid, it, p.headcode) }
                    "REINSTATEMENT" -> LinxMovementWriter.reinstatement(
                        p.trainId, p.uid, p.headcode, p.stanox, LinxClock.ukClock(p.time, p.startDate, labelsUkTime)
                    )
                    "CHANGE_OF_ORIGIN" -> LinxMovementWriter.changeOfOrigin(
                        p.trainId, p.uid, p.headcode, p.startDate, p.tiploc, p.stanox, p.reason, ::stanoxFor
                    )
                    else -> {}
                }
                is LinxRaw -> {}
            }
            AppDatabase.batchUpsertMovements(rows)
        } catch (e: Exception) {
            log.warn("LINX movement write failed: ${e.message}")
        }
    }
}

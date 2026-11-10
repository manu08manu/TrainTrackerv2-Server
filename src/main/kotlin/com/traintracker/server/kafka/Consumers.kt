package com.traintracker.server.kafka

import com.traintracker.server.Config
import com.traintracker.server.cif.CifParser
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.SmartLookup
import com.traintracker.server.cif.insertStops
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.MovementBatch
import com.traintracker.server.database.Schedules
import com.traintracker.server.database.TrustMovements
import com.traintracker.server.database.effectiveDateFor
import com.traintracker.server.database.scheduleDateScope
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.util.Properties
import java.util.concurrent.CancellationException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.less
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import org.json.JSONArray
import org.json.JSONObject
import org.slf4j.Logger
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("KafkaConsumers")

// Live state shared with the API routes.
val trainLocations = ConcurrentHashMap<String, TrainLocation>()
val trainIdToRid = ConcurrentHashMap<String, String>()   // TRUST train ID -> schedule uid
val ridToUid = ConcurrentHashMap<String, String>()        // Darwin RID -> schedule uid
val darwinPlatforms = ConcurrentHashMap<String, ConcurrentHashMap<String, String>>()   // uid -> TIPLOC -> platform

data class TrainLocation(
    val headcode: String,
    val rid: String,
    val stationName: String,
    val crs: String?,
    val actualTime: String,
    val eventType: String,
    val delayMinutes: Int,
    val updatedEpochMs: Long = System.currentTimeMillis()
)

private fun buildConsumer(groupId: String): KafkaConsumer<String, String> {
    val props = Properties().apply {
        put("bootstrap.servers", Config.kafkaBootstrap)
        put("group.id", groupId)
        put("auto.offset.reset", "latest")
        put("enable.auto.commit", "true")
        put("key.deserializer", StringDeserializer::class.java.name)
        put("value.deserializer", StringDeserializer::class.java.name)
        put("request.timeout.ms", "30000")
        put("session.timeout.ms", "45000")
        put("max.poll.records", "500")
        put("security.protocol", "SASL_SSL")
        put("sasl.mechanism", "PLAIN")
        put(
            "sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${Config.kafkaUsername}\" password=\"${Config.kafkaPassword}\";"
        )
        put("metric.reporters", "")
        put("auto.include.jmx.reporter", "false")
    }
    return KafkaConsumer(props)
}

/**
 * Polls [topic] every 5 seconds while [scope] is active, passing each batch to [onPoll].
 * Any error closes the consumer and reconnects after 10 seconds.
 */
fun launchConsumer(
    scope: CoroutineScope,
    name: String,
    topic: String,
    groupId: String,
    logger: Logger = log,
    onPoll: (KafkaConsumer<String, String>, ConsumerRecords<String, String>) -> Unit,
): Job = scope.launch(Dispatchers.IO) {
    while (isActive) {
        try {
            buildConsumer(groupId).use { consumer ->
                consumer.subscribe(listOf(topic))
                logger.info("$name subscribed to $topic (group=$groupId)")
                while (isActive) onPoll(consumer, consumer.poll(Duration.ofSeconds(5L)))
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logger.warn("$name error: ${e.message} — retrying in 10s")
            delay(10_000L)
        }
    }
}

/** Headcode from a TRUST train ID (characters 3-6), or "" when it does not look like one. */
private fun headcodeFromTrainId(trainId: String): String {
    if (trainId.length < 6) return ""
    val hc = trainId.substring(2, 6)
    if (!hc[0].isDigit() || !hc[1].isLetter()) return ""
    return hc
}

/** Epoch milliseconds -> "HH:mm" in UTC; "" when missing or zero. */
private fun formatTrustTime(epochMs: String): String {
    val ms = epochMs.toLongOrNull() ?: return ""
    if (ms == 0L) return ""
    val zdt = Instant.ofEpochMilli(ms).atZone(ZoneId.of("UTC"))
    return "%02d:%02d".format(zdt.hour, zdt.minute)
}

/** Minutes late (never negative); an actual time more than 2 hours "early" is taken as after midnight. */
fun minuteDelay(scheduled: String, actual: String): Int {
    val schParts = scheduled.split(":")
    val actParts = actual.split(":")
    if (schParts.size < 2 || actParts.size < 2) return 0
    val schMins = (schParts[0].toIntOrNull() ?: 0) * 60 + (schParts[1].toIntOrNull() ?: 0)
    val actMins = (actParts[0].toIntOrNull() ?: 0) * 60 + (actParts[1].toIntOrNull() ?: 0)
    var diff = actMins - schMins
    if (diff < -120) diff += 1440
    return if (diff > 0) diff else 0
}

/** TRUST train movements: activations, cancellations, movements, reinstatements, changes of origin and identity. */
object TrustConsumer {
    private val movementBuffer = ConcurrentLinkedQueue<MovementBatch>()

    fun start(scope: CoroutineScope): Job =
        launchConsumer(scope, "TRUST", "TRAIN_MVT_ALL_TOC", Config.kafkaGroupIdTrust) { _, records ->
            for (record in records) record.value()?.let { handleMessage(it) }
            val batch = mutableListOf<MovementBatch>()
            while (true) batch.add(movementBuffer.poll() ?: break)
            if (batch.isNotEmpty()) AppDatabase.batchUpsertMovements(batch)
        }

    private fun handleMessage(json: String) {
        try {
            val messages: JSONArray = if (json.trimStart().startsWith('[')) JSONArray(json) else JSONArray().put(JSONObject(json))
            for (i in 0 until messages.length()) {
                val msg = messages.getJSONObject(i)
                val header = msg.optJSONObject("header") ?: continue
                val body = msg.optJSONObject("body") ?: continue
                val trainId = body.optString("train_id", "").take(10)
                val headcode = headcodeFromTrainId(trainId)
                if (headcode.isEmpty()) continue
                when (header.optString("msg_type")) {
                    "0001" -> handleActivation(body, trainId, headcode)
                    "0002" -> handleCancellation(body, trainId, headcode)
                    "0003" -> handleMovement(body, trainId, headcode)
                    "0005" -> handleReinstatement(body, trainId, headcode)
                    "0006" -> handleChangeOfOrigin(body, trainId, headcode)
                    "0007" -> handleChangeOfIdentity(body, trainId, headcode)
                }
            }
        } catch (e: Exception) {
            log.debug("TRUST parse error: ${e.message}")
        }
    }

    private fun handleMovement(body: JSONObject, trainId: String, headcode: String) {
        val platform = body.optString("platform", "").trim().take(3).ifEmpty { null }
        val stanox = body.optString("loc_stanox", "")
        if (stanox.isBlank()) return
        CorpusLookup.checkTrustMovementForMissingName(stanox, headcode)
        CorpusLookup.checkTrustMovementForMissingAtoc(headcode)
        val crs = CorpusLookup.crsFromStanox(stanox)
        val isPassing = body.optString("movement_type", "").uppercase().contains("PASSING")
        val type = when {
            isPassing -> "PASSING"
            body.optString("event_type", "").uppercase() == "DEPARTURE" -> "DEPARTURE"
            else -> "ARRIVAL"
        }
        val scheduledTime = formatTrustTime(body.optString("planned_timestamp", "").ifEmpty { body.optString("gbtt_timestamp", "") })
        val actualTime = formatTrustTime(body.optString("actual_timestamp", ""))
        val reasonCode = body.optString("reason_code", "").trim().ifEmpty { null }
        val variationStatus = body.optString("variation_status", "").trim().uppercase()
        val variationMins = body.optString("timetable_variation", "").trim().toIntOrNull()
        val delayMins = when {
            variationMins != null && variationStatus.isNotEmpty() -> if (variationStatus == "LATE") variationMins else 0
            scheduledTime.isNotEmpty() && actualTime.isNotEmpty() -> minuteDelay(scheduledTime, actualTime)
            else -> 0
        }
        if (actualTime.isNotEmpty() && !isPassing) {
            TdCorrelator.onTrust(headcode, stanox, type, actualTime)
            if (SmartLookup.isLoaded() && !SmartLookup.hasCoverage(stanox, type)) {
                CorpusLookup.recordNoSmartCoverage(stanox, type, headcode)
            }
        }
        val uid = trainIdToRid[trainId] ?: ""
        if (actualTime.isNotEmpty() && type == "DEPARTURE") {
            val stationName = crs ?: stanox
            trainLocations["$headcode|$uid"] = TrainLocation(headcode, uid, stationName, crs, actualTime, type, delayMins)
            AppDatabase.upsertLocation(headcode, uid, stationName, crs, actualTime, type, delayMins)
        }
        movementBuffer.add(
            MovementBatch(
                headcode, trainId, stanox, crs, type,
                scheduledTime.ifEmpty { null }, actualTime.ifEmpty { null }, false, reasonCode, uid, platform
            )
        )
    }

    private fun handleActivation(body: JSONObject, trainId: String, headcode: String) {
        val rawUid = body.optString("train_uid", "").trim()
        val uid = if (rawUid.length in 2 until 7 && rawUid.all { it.isLetterOrDigit() }) rawUid else return
        val originStanox = body.optString("sched_origin_stanox", "").trim().ifEmpty { null }
        trainIdToRid[trainId] = uid
        AppDatabase.saveTrainActivation(trainId, uid, originStanox)
        log.debug("TRUST activation: $headcode ($trainId) uid=$uid originStanox=$originStanox")
    }

    private fun handleCancellation(body: JSONObject, trainId: String, headcode: String) {
        val reasonCode = body.optString("canx_reason_code", "")
        val stanox = body.optString("loc_stanox", "").ifBlank { return }
        val scheduledTime = formatTrustTime(body.optString("dep_timestamp", ""))
        movementBuffer.add(
            MovementBatch(
                headcode, trainId, stanox, CorpusLookup.crsFromStanox(stanox), "CANCELLATION",
                scheduledTime.ifEmpty { null }, null, true, reasonCode.ifEmpty { null }, trainIdToRid[trainId] ?: ""
            )
        )
    }

    /** Records the new origin as a departure and cancels every earlier stop of today's schedule. */
    private fun handleChangeOfOrigin(body: JSONObject, trainId: String, headcode: String) {
        val stanox = body.optString("loc_stanox", "").ifBlank { return }
        val scheduledTime = formatTrustTime(body.optString("dep_timestamp", ""))
        val reasonCode = body.optString("reason_code", "")
        val uid = trainIdToRid[trainId] ?: ""
        val newOriginCrs = CorpusLookup.crsFromStanox(stanox)
        movementBuffer.add(
            MovementBatch(headcode, trainId, stanox, newOriginCrs, "DEPARTURE", scheduledTime.ifEmpty { null }, null, false, uid = uid)
        )
        if (uid.isEmpty() || newOriginCrs == null) return
        try {
            val cancelledStops = transaction {
                val today = scheduleDateScope(effectiveDateFor(LocalDate.now().toString()))
                val newOriginTime = Schedules.selectAll()
                    .where { (Schedules.uid eq uid) and (Schedules.crs eq newOriginCrs) and today }
                    .limit(1)
                    .firstOrNull()
                    ?.get(Schedules.scheduledTime) ?: ""
                if (newOriginTime.isEmpty()) return@transaction emptyList()
                Schedules.selectAll()
                    .where {
                        (Schedules.uid eq uid) and (Schedules.scheduledTime less newOriginTime) and (Schedules.isPass eq false) and today
                    }
                    .mapNotNull { row ->
                        val crs = row[Schedules.crs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(row[Schedules.tiploc])
                        row[Schedules.scheduledTime].takeIf { it.isNotEmpty() }?.let { it to crs }
                    }
            }
            for ((scht, crs) in cancelledStops) {
                movementBuffer.add(MovementBatch(headcode, trainId, stanox, crs, "CANCELLATION", scht, null, true, reasonCode.ifEmpty { null }, uid))
            }
            if (cancelledStops.isNotEmpty()) {
                log.info("TRUST COO: $headcode cancelled ${cancelledStops.size} stops before $newOriginCrs")
            }
        } catch (e: Exception) {
            log.warn("COO cancellation lookup failed for $headcode: ${e.message}")
        }
    }

    /** A new headcode for the same train: keep an alias, move its location, and carry its uid to the new train ID. */
    private fun handleChangeOfIdentity(body: JSONObject, oldTrainId: String, oldHeadcode: String) {
        val revisedTrainId = body.optString("revised_train_id", "").ifBlank { return }
        val newHeadcode = headcodeFromTrainId(revisedTrainId)
        if (newHeadcode.isEmpty() || newHeadcode == oldHeadcode) return
        log.info("TRUST change of identity: $oldHeadcode -> $newHeadcode")
        val uid = trainIdToRid[oldTrainId]
        transaction {
            AppDatabase.saveHeadcodeAlias(oldHeadcode, newHeadcode, uid ?: "")
            AppDatabase.transferLocation(oldHeadcode, newHeadcode, uid ?: "")
        }
        if (!uid.isNullOrEmpty()) {
            trainIdToRid[revisedTrainId] = uid
            AppDatabase.saveTrainActivation(revisedTrainId, uid, null)
            log.debug("TRUST identity change carried uid=$uid from $oldTrainId to $revisedTrainId")
        }
    }

    private fun handleReinstatement(body: JSONObject, trainId: String, headcode: String) {
        val stanox = body.optString("loc_stanox", "").ifBlank { return }
        val scheduledTime = formatTrustTime(body.optString("dep_timestamp", ""))
        val uid = trainIdToRid[trainId] ?: ""
        if (uid.isNotEmpty() && scheduledTime.isNotEmpty()) {
            transaction {
                TrustMovements.update({
                    (TrustMovements.uid eq uid) and (TrustMovements.scheduledTime eq scheduledTime) and (TrustMovements.isCancelled eq true)
                }) {
                    it[isCancelled] = false
                    it[cancelReason] = null
                }
            }
        }
        movementBuffer.add(
            MovementBatch(
                headcode, trainId, stanox, CorpusLookup.crsFromStanox(stanox), "REINSTATEMENT",
                scheduledTime.ifEmpty { null }, null, false, uid = uid
            )
        )
        log.info("TRUST reinstatement: $headcode at ${scheduledTime.ifEmpty { "unknown time" }}")
    }
}

/** Darwin push port: RID -> uid mappings and live platforms. */
object DarwinConsumer {
    fun start(scope: CoroutineScope): Job =
        launchConsumer(scope, "Darwin", "prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON", Config.kafkaGroupIdDarwin) { _, records ->
            for (record in records) record.value()?.let { handleMessage(it) }
        }

    private fun handleMessage(json: String) {
        try {
            val payload = JSONObject(json).optString("bytes").ifEmpty { return }
            val ts = JSONObject(payload).optJSONObject("uR")?.optJSONObject("TS") ?: return
            val rid = ts.optString("rid").ifEmpty { return }
            val uid = ts.optString("uid").ifEmpty { return }
            ridToUid[rid] = uid
            AppDatabase.upsertDarwinRidUid(rid, uid)
            val locations: JSONArray = when (val raw = ts.opt("Location")) {
                is JSONArray -> raw
                is JSONObject -> JSONArray().put(raw)
                else -> return
            }
            val platforms = mutableMapOf<String, String>()
            for (i in 0 until locations.length()) {
                val loc = locations.optJSONObject(i) ?: continue
                val tiploc = loc.optString("tpl")
                if (tiploc.isEmpty()) continue
                platforms[tiploc] = platform(loc) ?: continue
            }
            if (platforms.isEmpty()) return
            darwinPlatforms.getOrPut(uid) { ConcurrentHashMap() }.putAll(platforms)
            AppDatabase.upsertDarwinPlatforms(uid, rid, platforms)
        } catch (e: Exception) {
            log.debug("Darwin parse error: ${e.message}")
        }
    }

    /** The platform of a location, unless it is suppressed. */
    private fun platform(loc: JSONObject): String? = when (val raw = loc.opt("plat")) {
        is String -> raw.trim().ifEmpty { null }
        is JSONObject -> if (raw.optString("platsup") == "true") null else raw.optString("").trim().ifEmpty { null }
        else -> null
    }
}

/** Very short term (VSTP) schedules. Re-reads the whole topic on every connect. */
object VstpConsumer {
    @Volatile
    private var seekedConsumer: KafkaConsumer<String, String>? = null

    fun start(scope: CoroutineScope): Job =
        launchConsumer(scope, "VSTP", "VSTP_ALL", Config.kafkaGroupIdVstp) { consumer, records ->
            if (seekedConsumer !== consumer && consumer.assignment().isNotEmpty()) {
                // The first batch after partitions are assigned is dropped and the topic re-read from the start.
                consumer.seekToBeginning(consumer.assignment())
                log.info("VSTP seeked to beginning on ${consumer.assignment().size} partitions")
                seekedConsumer = consumer
            } else {
                for (record in records) record.value()?.let { handleMessage(it) }
            }
        }

    private fun handleMessage(json: String) {
        try {
            val schedule = JSONObject(json).optJSONObject("VSTPCIFMsgV1")?.optJSONObject("schedule") ?: return
            val txType = schedule.optString("transaction_type", "")
            val uid = schedule.optString("CIF_train_uid", "").trim()
            val stp = schedule.optString("CIF_stp_indicator", "").firstOrNull() ?: return
            if (uid.isEmpty()) return
            val startDate = schedule.optString("schedule_start_date", "")
            val endDate = schedule.optString("schedule_end_date", "")
            when (txType) {
                "Delete" -> transaction {
                    val deleted = deleteSchedule(uid, stp, startDate, endDate)
                    if (deleted > 0) log.info("VSTP Delete: uid=$uid stp=$stp removed $deleted rows")
                }
                "Create" -> {
                    val stops = CifParser.parseVstpSchedule(schedule)
                    if (stops.isEmpty()) return
                    transaction {
                        deleteSchedule(uid, stp, startDate, endDate)
                        insertStops(stops) { it.scheduleStartDate }
                    }
                    log.info("VSTP Create: uid=$uid stp=$stp ${stops.size} stops upserted")
                }
                else -> log.debug("VSTP unknown txType=$txType uid=$uid")
            }
        } catch (e: Exception) {
            log.warn("VSTP parse error: ${e.message} — json=${json.take(200)}")
        }
    }

    private fun deleteSchedule(uid: String, stp: Char, startDate: String, endDate: String): Int = Schedules.deleteWhere {
        (Schedules.uid eq uid) and (Schedules.stpIndicator eq stp) and
            (Schedules.scheduleStartDate eq startDate) and (Schedules.scheduleEndDate eq endDate)
    }
}

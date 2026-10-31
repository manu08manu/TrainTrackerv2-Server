package com.traintracker.server.kafka

import com.traintracker.server.Config
import com.traintracker.server.cif.CifParser
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.SmartLookup
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
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.less
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import org.json.JSONArray
import org.json.JSONObject
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("KafkaConsumers")

val trainLocations = ConcurrentHashMap<String, TrainLocation>()
val trainIdToRid = ConcurrentHashMap<String, String>()
val ridToUid = ConcurrentHashMap<String, String>()
val darwinPlatforms = ConcurrentHashMap<String, ConcurrentHashMap<String, String>>()
private val movementBuffer = ConcurrentLinkedQueue<MovementBatch>()

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

fun buildConsumer(offsetReset: String = "latest", groupId: String): KafkaConsumer<String, String> {
    val props = Properties().apply {
        put("bootstrap.servers", Config.kafkaBootstrap)
        put("group.id", groupId)
        put("auto.offset.reset", offsetReset)
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

private fun headcodeFromTrainId(trainId: String): String {
    if (trainId.length < 6) return ""
    val hc = trainId.substring(2, 6)
    if (hc.length != 4 || !hc[0].isDigit() || !hc[1].isLetter()) return ""
    return hc
}

private fun formatTrustTime(epochMs: String): String {
    val ms = epochMs.toLongOrNull() ?: return ""
    if (ms == 0L) return ""
    val zdt = Instant.ofEpochMilli(ms).atZone(ZoneId.of("UTC"))
    return "%02d:%02d".format(zdt.hour, zdt.minute)
}

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

private fun stanoxToCrs(stanox: String): String? = CorpusLookup.crsFromStanox(stanox)

object TrustConsumer {
    private val loggedRawMovementSample = AtomicBoolean(false)

    fun start(scope: CoroutineScope): Job = scope.launch(Dispatchers.IO) {
        while (isActive) {
            try {
                buildConsumer(groupId = Config.kafkaGroupIdTrust).use { consumer ->
                    consumer.subscribe(listOf("TRAIN_MVT_ALL_TOC"))
                    log.info("TRUST subscribed (group=${Config.kafkaGroupIdTrust})")
                    while (isActive) {
                        val records = consumer.poll(Duration.ofSeconds(5L))
                        for (record in records) {
                            record.value()?.let { handleTrustMessage(it) }
                        }
                        val batch = mutableListOf<MovementBatch>()
                        while (true) batch.add(movementBuffer.poll() ?: break)
                        if (batch.isNotEmpty()) AppDatabase.batchUpsertMovements(batch)
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("TRUST error: ${e.message} — retrying in 10s")
                delay(10000L)
            }
        }
    }

    private fun handleTrustMessage(json: String) {
        try {
            val messages: JSONArray = if (json.trimStart().startsWith('[')) JSONArray(json) else JSONArray().put(JSONObject(json))
            for (i in 0 until messages.length()) {
                val msg = messages.getJSONObject(i)
                val header = msg.optJSONObject("header") ?: continue
                val body = msg.optJSONObject("body") ?: continue
                val msgType = header.optString("msg_type")
                val trainId = body.optString("train_id", "").take(10)
                val headcode = headcodeFromTrainId(trainId)
                when (msgType) {
                    "0001" -> handleActivation(header, body, trainId, headcode)
                    "0002" -> handleCancellation(body, trainId, headcode)
                    "0003" -> handleMovement(body, trainId, headcode)
                    "0005" -> handleReinstatement(body, trainId, headcode)
                    "0006" -> handleChangeOfOrigin(body, trainId, headcode)
                    "0007" -> handleChangeOfIdentity(body, trainId, headcode)
                    "0008" -> Unit
                }
            }
        } catch (e: Exception) {
            log.debug("TRUST parse error: ${e.message}")
        }
    }

    private fun handleMovement(body: JSONObject, trainId: String, headcode: String) {
        if (headcode.isEmpty()) return
        val platform = body.optString("platform", "").trim().take(3).ifEmpty { null }
        val stanox = body.optString("loc_stanox", "")
        if (stanox.isBlank()) return
        CorpusLookup.checkTrustMovementForMissingName(stanox, headcode)
        CorpusLookup.checkTrustMovementForMissingAtoc(headcode)
        val eventType = body.optString("event_type", "").uppercase()
        val movementType = body.optString("movement_type", "").uppercase()
        val isPassing = movementType.contains("PASSING")
        val type = if (isPassing) "PASSING" else if (eventType == "DEPARTURE") "DEPARTURE" else "ARRIVAL"
        val scheduledTime = formatTrustTime(body.optString("planned_timestamp", "").ifEmpty { body.optString("gbtt_timestamp", "") })
        val actualTime = formatTrustTime(body.optString("actual_timestamp", ""))
        val reasonCode = body.optString("reason_code", "").trim().ifEmpty { null }
        val variationStatus = body.optString("variation_status", "").trim().uppercase()
        val variationMins = body.optString("timetable_variation", "").trim().toIntOrNull()
        val delayMins = if (variationMins != null && variationStatus.isNotEmpty()) {
            if (variationStatus == "LATE") variationMins else 0
        } else if (scheduledTime.isNotEmpty() && actualTime.isNotEmpty()) {
            minuteDelay(scheduledTime, actualTime)
        } else {
            0
        }
        if (actualTime.isNotEmpty() && !isPassing) {
            TdCorrelator.onTrust(headcode, stanox, type, actualTime)
        }
        if (actualTime.isNotEmpty() && !isPassing && SmartLookup.isLoaded() && !SmartLookup.hasCoverage(stanox, type)) {
            CorpusLookup.recordNoSmartCoverage(stanox, type, headcode)
        }
        if (actualTime.isNotEmpty() && type == "DEPARTURE") {
            val stationName = stanoxToCrs(stanox) ?: stanox
            val crs = stanoxToCrs(stanox)
            val rid = trainIdToRid[trainId] ?: ""
            trainLocations["$headcode|$rid"] = TrainLocation(headcode, rid, stationName, crs, actualTime, type, delayMins)
            AppDatabase.upsertLocation(headcode, rid, stationName, crs, actualTime, type, delayMins)
        }
        val uid = trainIdToRid[trainId] ?: ""
        movementBuffer.add(
            MovementBatch(
                headcode, trainId, stanox, stanoxToCrs(stanox), type,
                scheduledTime.ifEmpty { null }, actualTime.ifEmpty { null }, false, reasonCode, uid, platform
            )
        )
    }

    private fun handleActivation(header: JSONObject, body: JSONObject, trainId: String, headcode: String) {
        if (headcode.isEmpty()) return
        val rawUid = body.optString("train_uid", "").trim()
        val uid = if (rawUid.length in 2 until 7 && rawUid.all { it.isLetterOrDigit() }) rawUid else ""
        val originStanox = body.optString("sched_origin_stanox", "").trim().ifEmpty { null }
        if (uid.isNotEmpty()) {
            trainIdToRid[trainId] = uid
            AppDatabase.saveTrainActivation(trainId, uid, originStanox)
            log.debug("TRUST activation: $headcode ($trainId) uid=$uid originStanox=$originStanox")
        }
    }

    private fun handleCancellation(body: JSONObject, trainId: String, headcode: String) {
        if (headcode.isEmpty()) return
        val reasonCode = body.optString("canx_reason_code", "")
        val stanox = body.optString("loc_stanox", "").ifBlank { return }
        val scheduledTime = formatTrustTime(body.optString("dep_timestamp", ""))
        movementBuffer.add(
            MovementBatch(
                headcode, trainId, stanox, stanoxToCrs(stanox), "CANCELLATION",
                scheduledTime.ifEmpty { null }, null, true, reasonCode.ifEmpty { null }, trainIdToRid[trainId] ?: ""
            )
        )
    }

    private fun handleChangeOfOrigin(body: JSONObject, trainId: String, headcode: String) {
        if (headcode.isEmpty()) return
        val stanox = body.optString("loc_stanox", "").ifBlank { return }
        val scheduledTime = formatTrustTime(body.optString("dep_timestamp", ""))
        val reasonCode = body.optString("reason_code", "")
        val uid = trainIdToRid[trainId] ?: ""
        val newOriginCrs = stanoxToCrs(stanox)
        movementBuffer.add(
            MovementBatch(
                headcode, trainId, stanox, newOriginCrs, "DEPARTURE",
                scheduledTime.ifEmpty { null }, null, false, uid = uid
            )
        )
        if (uid.isNotEmpty() && newOriginCrs != null) {
            try {
                val cancelledStops = transaction {
                    val newOriginTime = Schedules.selectAll()
                        .where { (Schedules.uid eq uid) and (Schedules.crs eq newOriginCrs) and scheduleDateScope(effectiveDateFor(LocalDate.now().toString())) }
                        .limit(1)
                        .firstOrNull()
                        ?.get(Schedules.scheduledTime) ?: ""
                    val result = mutableListOf<Pair<String, String?>>()
                    if (newOriginTime.isNotEmpty()) {
                        Schedules.selectAll()
                            .where {
                                (Schedules.uid eq uid) and (Schedules.scheduledTime less newOriginTime) and (Schedules.isPass eq false) and
                                    scheduleDateScope(effectiveDateFor(LocalDate.now().toString()))
                            }
                            .forEach { row ->
                                val crs = row[Schedules.crs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(row[Schedules.tiploc])
                                val scht = row[Schedules.scheduledTime]
                                if (scht.isNotEmpty()) result.add(Pair(scht, crs))
                            }
                    }
                    result
                }
                for ((scht, crs) in cancelledStops) {
                    movementBuffer.add(
                        MovementBatch(
                            headcode, trainId, stanox, crs, "CANCELLATION",
                            scht, null, true, reasonCode.ifEmpty { null }, uid
                        )
                    )
                }
                if (cancelledStops.isNotEmpty()) {
                    log.info("TRUST COO: $headcode cancelled ${cancelledStops.size} stops before $newOriginCrs")
                }
            } catch (e: Exception) {
                log.warn("COO cancellation lookup failed for $headcode: ${e.message}")
            }
        }
    }

    private fun handleChangeOfIdentity(body: JSONObject, oldTrainId: String, oldHeadcode: String) {
        if (oldHeadcode.isEmpty()) return
        val revisedTrainId = body.optString("revised_train_id", "").ifBlank { return }
        val newHeadcode = headcodeFromTrainId(revisedTrainId)
        if (newHeadcode.isEmpty() || newHeadcode == oldHeadcode) return
        log.info("TRUST change of identity: $oldHeadcode -> $newHeadcode")
        transaction {
            AppDatabase.saveHeadcodeAlias(oldHeadcode, newHeadcode, trainIdToRid[oldTrainId] ?: "")
            AppDatabase.transferLocation(oldHeadcode, newHeadcode, trainIdToRid[oldTrainId] ?: "")
        }
        val uid = trainIdToRid[oldTrainId]
        if (!uid.isNullOrEmpty()) {
            trainIdToRid[revisedTrainId] = uid
            AppDatabase.saveTrainActivation(revisedTrainId, uid, null)
            log.debug("TRUST identity change carried uid=$uid from $oldTrainId to $revisedTrainId")
        }
    }

    private fun handleReinstatement(body: JSONObject, trainId: String, headcode: String) {
        if (headcode.isEmpty()) return
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
                headcode, trainId, stanox, stanoxToCrs(stanox), "REINSTATEMENT",
                scheduledTime.ifEmpty { null }, null, false, uid = uid
            )
        )
        log.info("TRUST reinstatement: $headcode at ${scheduledTime.ifEmpty { "unknown time" }}")
    }
}

object DarwinConsumer {
    fun start(scope: CoroutineScope): Job = scope.launch(Dispatchers.IO) {
        while (isActive) {
            try {
                buildConsumer(groupId = Config.kafkaGroupIdDarwin).use { consumer ->
                    consumer.subscribe(listOf("prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON"))
                    log.info("Darwin subscribed (group=${Config.kafkaGroupIdDarwin})")
                    while (isActive) {
                        val records = consumer.poll(Duration.ofSeconds(5L))
                        for (record in records) {
                            record.value()?.let { handleDarwinMessage(it) }
                        }
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("Darwin error: ${e.message} — retrying in 10s")
                delay(10000L)
            }
        }
    }

    private fun handleDarwinMessage(json: String) {
        try {
            val outer = JSONObject(json)
            val payload = outer.optString("bytes").ifEmpty { return }
            val root = JSONObject(payload)
            val ts = root.optJSONObject("uR")?.optJSONObject("TS") ?: return
            val rid = ts.optString("rid").ifEmpty { return }
            val uid = ts.optString("uid").ifEmpty { return }
            if (uid.isNotEmpty()) {
                ridToUid[rid] = uid
                AppDatabase.upsertDarwinRidUid(rid, uid)
            }
            val locationRaw = ts.opt("Location") ?: return
            val locations: JSONArray = when (locationRaw) {
                is JSONArray -> locationRaw
                is JSONObject -> JSONArray().put(locationRaw)
                else -> return
            }
            val platformsForService = mutableMapOf<String, String>()
            for (i in 0 until locations.length()) {
                val loc = locations.optJSONObject(i) ?: continue
                val tiploc = loc.optString("tpl")
                if (tiploc.isEmpty()) continue
                val plat = parseDarwinPlatform(loc) ?: continue
                platformsForService[tiploc] = plat
            }
            if (platformsForService.isEmpty()) return
            darwinPlatforms.getOrPut(uid) { ConcurrentHashMap() }.putAll(platformsForService)
            AppDatabase.upsertDarwinPlatforms(uid, rid, platformsForService)
        } catch (e: Exception) {
            log.debug("Darwin parse error: ${e.message}")
        }
    }

    private fun parseDarwinPlatform(loc: JSONObject): String? {
        val platRaw = loc.opt("plat") ?: return null
        return when (platRaw) {
            is String -> platRaw.trim().ifEmpty { null }
            is JSONObject -> {
                if (platRaw.optString("platsup") == "true") return null
                platRaw.optString("").trim().ifEmpty { null }
            }
            else -> null
        }
    }
}

object VstpConsumer {
    fun start(scope: CoroutineScope): Job = scope.launch(Dispatchers.IO) {
        while (isActive) {
            try {
                buildConsumer(groupId = Config.kafkaGroupIdVstp).use { consumer ->
                    consumer.subscribe(listOf("VSTP_ALL"))
                    log.info("VSTP subscribed (group=${Config.kafkaGroupIdVstp})")
                    var seeked = false
                    while (isActive) {
                        val records = consumer.poll(Duration.ofSeconds(5L))
                        if (!seeked && consumer.assignment().isNotEmpty()) {
                            consumer.seekToBeginning(consumer.assignment())
                            log.info("VSTP seeked to beginning on ${consumer.assignment().size} partitions")
                            seeked = true
                            continue
                        }
                        for (record in records) {
                            record.value()?.let { handleVstpMessage(it) }
                        }
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("VSTP Kafka error: ${e.message} — retrying in 10s")
                delay(10000L)
            }
        }
    }

    private fun handleVstpMessage(json: String) {
        try {
            val root = JSONObject(json)
            val envelope = root.optJSONObject("VSTPCIFMsgV1") ?: return
            val schedule = envelope.optJSONObject("schedule") ?: return
            val txType = schedule.optString("transaction_type", "")
            val uid = schedule.optString("CIF_train_uid", "").trim()
            if (uid == "03479") log.info("VSTP_RAW_DEBUG uid=$uid: $json")
            val stp = schedule.optString("CIF_stp_indicator", "").firstOrNull() ?: return
            if (uid.isEmpty()) return
            val schedStartDate = schedule.optString("schedule_start_date", "")
            val schedEndDate = schedule.optString("schedule_end_date", "")
            when (txType) {
                "Delete" -> applyVstpDelete(uid, stp, schedStartDate, schedEndDate)
                "Create" -> applyVstpCreate(uid, stp, schedule, schedStartDate, schedEndDate)
                else -> log.debug("VSTP unknown txType=$txType uid=$uid")
            }
        } catch (e: Exception) {
            log.warn("VSTP parse error: ${e.message} — json=${json.take(200)}")
        }
    }

    private fun applyVstpDelete(uid: String, stp: Char, startDate: String, endDate: String) {
        transaction {
            val deleted = Schedules.deleteWhere {
                (Schedules.uid eq uid) and (Schedules.stpIndicator eq stp.toString().first()) and
                    (Schedules.scheduleStartDate eq startDate) and (Schedules.scheduleEndDate eq endDate)
            }
            if (deleted > 0) log.info("VSTP Delete: uid=$uid stp=$stp removed $deleted rows")
        }
    }

    private fun applyVstpCreate(uid: String, stp: Char, sched: JSONObject, startDate: String, endDate: String) {
        val stops = CifParser.parseScheduleForVstpKafka(sched)
        if (stops.isEmpty()) return
        transaction {
            Schedules.deleteWhere {
                (Schedules.uid eq uid) and (Schedules.stpIndicator eq stp.toString().first()) and
                    (Schedules.scheduleStartDate eq startDate) and (Schedules.scheduleEndDate eq endDate)
            }
            Schedules.batchInsert(stops, ignore = true) { s ->
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
                this[Schedules.scheduleDate] = s.scheduleStartDate
                this[Schedules.originCrs] = s.originCrs
                this[Schedules.destCrs] = s.destCrs
                this[Schedules.scheduleStartDate] = s.scheduleStartDate
                this[Schedules.scheduleEndDate] = s.scheduleEndDate
                this[Schedules.stopSequence] = s.sequence
            }
        }
        log.info("VSTP Create: uid=$uid stp=$stp ${stops.size} stops upserted")
    }
}

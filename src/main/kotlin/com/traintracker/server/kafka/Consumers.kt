package com.traintracker.server.kafka

import com.traintracker.server.Config
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.MovementBatch
import com.traintracker.server.database.Schedules
import com.traintracker.server.cif.CifParser
import com.traintracker.server.cif.CifStop
import com.traintracker.server.cif.CorpusLookup
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.transactions.transaction
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.json.JSONArray
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

private val log = LoggerFactory.getLogger("KafkaConsumers")

val trainLocations = ConcurrentHashMap<String, TrainLocation>()
val trainIdToRid = ConcurrentHashMap<String, String>()
val ridToUid = ConcurrentHashMap<String, String>()
val darwinPlatforms = ConcurrentHashMap<String, ConcurrentHashMap<String, String>>()
private val movementBuffer = java.util.concurrent.CopyOnWriteArrayList<MovementBatch>()

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

object TrustConsumer {
    fun start(scope: CoroutineScope): Job = scope.launch(Dispatchers.IO) {
        while (isActive) {
            try {
                buildConsumer().use { consumer ->
                    consumer.subscribe(listOf("TRAIN_MVT_ALL_TOC"))
                    log.info("TRUST subscribed (group=${Config.kafkaGroupId})")
                    while (isActive) {
                        val records = consumer.poll(Duration.ofSeconds(5))
                        for (record in records) record.value()?.let { handleTrustMessage(it) }
                        if (movementBuffer.isNotEmpty()) {
                            val batch = movementBuffer.toList()
                            movementBuffer.clear()
                            AppDatabase.batchUpsertMovements(batch)
                        }
                    }
                }
            } catch (e: CancellationException) { throw e
            } catch (e: Exception) { log.warn("TRUST error: ${e.message} — retrying in 10s"); delay(10_000) }
        }
    }

    private fun handleTrustMessage(json: String) {
        try {
            val messages: JSONArray = when {
                json.trimStart().startsWith('[') -> JSONArray(json)
                else -> JSONArray().put(JSONObject(json))
            }
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
                    "0007" -> handleChangeOfIdentity(body, headcode)
                    "0008" -> Unit
                }
            }
        } catch (e: Exception) { log.debug("TRUST parse error: ${e.message}") }
    }

    private fun handleMovement(body: JSONObject, trainId: String, headcode: String) {
        if (headcode.isEmpty()) return
        val stanox = body.optString("loc_stanox", "")
        if (stanox.isBlank()) return
        val eventType = body.optString("event_type", "").uppercase()
        val movementType = body.optString("movement_type", "").uppercase()
        val isPassing = movementType.contains("PASSING")
        val type = when {
            isPassing -> "PASSING"
            eventType == "DEPARTURE" -> "DEPARTURE"
            else -> "ARRIVAL"
        }
        val scheduledTime = formatTrustTime(body.optString("planned_timestamp", "").ifEmpty { body.optString("gbtt_timestamp", "") })
        val actualTime = formatTrustTime(body.optString("actual_timestamp", ""))
        val reasonCode = body.optString("reason_code", "").trim().ifEmpty { null }
        val delayMins = if (scheduledTime.isNotEmpty() && actualTime.isNotEmpty()) minuteDelay(scheduledTime, actualTime) else 0
        if (actualTime.isNotEmpty() && type == "DEPARTURE") {
            val stationName = stanoxToCrs(stanox) ?: stanox
            val crs = stanoxToCrs(stanox)
            val rid = trainIdToRid[trainId] ?: ""
            trainLocations[headcode] = TrainLocation(headcode, rid, stationName, crs, actualTime, type, delayMins)
            AppDatabase.upsertLocation(headcode, rid, stationName, crs, actualTime, type, delayMins)
        }
        val uid = trainIdToRid[trainId] ?: ""
        movementBuffer.add(MovementBatch(headcode, trainId, stanox, stanoxToCrs(stanox), type,
            scheduledTime.ifEmpty { null }, actualTime.ifEmpty { null },
            isCancelled = false, cancelReason = reasonCode, uid = uid))
    }

    private fun handleActivation(header: JSONObject, body: JSONObject, trainId: String, headcode: String) {
        if (headcode.isEmpty()) return
        val rawUid = body.optString("train_uid", "").trim()
        val uid = if (rawUid.length in 2..6 && rawUid[0].isLetter()) rawUid else ""
        if (uid.isNotEmpty()) {
            trainIdToRid[trainId] = uid
            AppDatabase.saveTrainActivation(trainId, uid)
            log.debug("TRUST activation: $headcode ($trainId) uid=$uid")
        }
    }

    private fun handleCancellation(body: JSONObject, trainId: String, headcode: String) {
        if (headcode.isEmpty()) return
        val reasonCode = body.optString("canx_reason_code", "")
        val stanox = body.optString("loc_stanox", "").ifBlank { return }
        val scheduledTime = formatTrustTime(body.optString("dep_timestamp", ""))
        movementBuffer.add(MovementBatch(headcode, trainId, stanox, stanoxToCrs(stanox),
            "CANCELLATION", scheduledTime.ifEmpty { null }, null,
            isCancelled = true, cancelReason = reasonCode.ifEmpty { null }, uid = trainIdToRid[trainId] ?: ""))
    }

    private fun handleChangeOfOrigin(body: JSONObject, trainId: String, headcode: String) {
        if (headcode.isEmpty()) return
        val stanox = body.optString("loc_stanox", "").ifBlank { return }
        val scheduledTime = formatTrustTime(body.optString("dep_timestamp", ""))
        val reasonCode = body.optString("reason_code", "")
        val uid = trainIdToRid[trainId] ?: ""
        val newOriginCrs = stanoxToCrs(stanox)
        movementBuffer.add(MovementBatch(headcode, trainId, stanox, newOriginCrs,
            "DEPARTURE", scheduledTime.ifEmpty { null }, null, isCancelled = false, uid = uid))
        if (uid.isNotEmpty() && newOriginCrs != null) {
            try {
                val cancelledStops = transaction {
                    var newOriginTime = ""
                    exec("SELECT scheduled_time FROM schedules WHERE uid='$uid' AND crs='$newOriginCrs' LIMIT 1") { rs ->
                        if (rs.next()) newOriginTime = rs.getString("scheduled_time") ?: ""
                    }
                    val result = mutableListOf<Pair<String, String?>>()
                    if (newOriginTime.isNotEmpty()) {
                        exec("SELECT tiploc, crs, scheduled_time FROM schedules WHERE uid='$uid' AND scheduled_time < '$newOriginTime' AND is_pass=0") { rs ->
                            while (rs.next()) {
                                val crs = rs.getString("crs")?.takeIf { it.isNotEmpty() }
                                    ?: CorpusLookup.crsFromTiploc(rs.getString("tiploc") ?: "")
                                val scht = rs.getString("scheduled_time") ?: ""
                                if (scht.isNotEmpty()) result.add(Pair(scht, crs))
                            }
                        }
                    }
                    result
                }
                for ((scht, crs) in cancelledStops) {
                    movementBuffer.add(MovementBatch(headcode, trainId, stanox, crs,
                        "CANCELLATION", scht, null,
                        isCancelled = true, cancelReason = reasonCode.ifEmpty { null }, uid = uid))
                }
                if (cancelledStops.isNotEmpty())
                    log.info("TRUST COO: $headcode cancelled ${cancelledStops.size} stops before $newOriginCrs")
            } catch (e: Exception) { log.warn("COO cancellation lookup failed for $headcode: ${e.message}") }
        }
    }

    private fun handleChangeOfIdentity(body: JSONObject, oldHeadcode: String) {
        if (oldHeadcode.isEmpty()) return
        val revisedTrainId = body.optString("revised_train_id", "").ifBlank { return }
        val newHeadcode = headcodeFromTrainId(revisedTrainId)
        if (newHeadcode.isEmpty() || newHeadcode == oldHeadcode) return
        log.info("TRUST change of identity: $oldHeadcode -> $newHeadcode")
        AppDatabase.saveHeadcodeAlias(oldHeadcode, newHeadcode)
        AppDatabase.transferLocation(oldHeadcode, newHeadcode)
    }

    private fun handleReinstatement(body: JSONObject, trainId: String, headcode: String) {
        if (headcode.isEmpty()) return
        val stanox = body.optString("loc_stanox", "").ifBlank { return }
        val scheduledTime = formatTrustTime(body.optString("dep_timestamp", ""))
        val uid = trainIdToRid[trainId] ?: ""
        if (uid.isNotEmpty() && scheduledTime.isNotEmpty()) {
            transaction { exec("UPDATE trust_movements SET is_cancelled = 0, cancel_reason = NULL WHERE uid = '$uid' AND scheduled_time = '$scheduledTime' AND is_cancelled = 1") }
        }
        movementBuffer.add(MovementBatch(headcode, trainId, stanox, stanoxToCrs(stanox),
            "REINSTATEMENT", scheduledTime.ifEmpty { null }, null, isCancelled = false, uid = uid))
        log.info("TRUST reinstatement: $headcode at ${scheduledTime.ifEmpty { "unknown time" }}")
    }
}

object VstpConsumer {
    fun start(scope: CoroutineScope): Job = scope.launch(Dispatchers.IO) {
        while (isActive) {
            try {
                buildConsumer().use { consumer ->
                    consumer.subscribe(listOf("VSTP_ALL"))
                    log.info("VSTP subscribed (group=${Config.kafkaGroupId})")
                    var seeked = false
                    while (isActive) {
                        val records = consumer.poll(Duration.ofSeconds(5))
                        if (!seeked && consumer.assignment().isNotEmpty()) {
                            consumer.seekToBeginning(consumer.assignment())
                            log.info("VSTP seeked to beginning on ${consumer.assignment().size} partitions")
                            seeked = true
                            continue
                        }
                        for (record in records) record.value()?.let { handleVstpMessage(it) }
                    }
                }
            } catch (e: CancellationException) { throw e
            } catch (e: Exception) { log.warn("VSTP Kafka error: ${e.message} — retrying in 10s"); delay(10_000) }
        }
    }

    private fun handleVstpMessage(json: String) {
        try {
            val root = JSONObject(json)
            val envelope = root.optJSONObject("VSTPCIFMsgV1") ?: return
            val schedule = envelope.optJSONObject("schedule") ?: return
            val txType = schedule.optString("transaction_type", "")
            val uid = schedule.optString("CIF_train_uid", "").trim()
            val stp = schedule.optString("CIF_stp_indicator", "").firstOrNull() ?: return
            if (uid.isEmpty()) return
            when (txType) {
                "Delete" -> applyVstpDelete(uid, stp)
                "Create" -> applyVstpCreate(uid, stp, schedule)
                else -> log.debug("VSTP unknown txType=$txType uid=$uid")
            }
        } catch (e: Exception) { log.warn("VSTP parse error: ${e.message} — json=${json.take(200)}") }
    }

    private fun applyVstpDelete(uid: String, stp: Char) {
        transaction {
            val deleted = Schedules.deleteWhere { (Schedules.uid eq uid) and (Schedules.stpIndicator eq stp.toString().first()) }
            if (deleted > 0) log.info("VSTP Delete: uid=$uid stp=$stp removed $deleted rows")
        }
    }

    private fun applyVstpCreate(uid: String, stp: Char, sched: JSONObject) {
        val stops: List<CifStop> = CifParser.parseScheduleForVstpKafka(sched)
        if (stops.isEmpty()) return
        transaction {
            Schedules.deleteWhere { (Schedules.uid eq uid) and (Schedules.stpIndicator eq stp.toString().first()) }
            Schedules.batchInsert(stops, ignore = true) { s ->
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
        log.info("VSTP Create: uid=$uid stp=$stp ${stops.size} stops upserted")
    }
}

object DarwinConsumer {
    fun start(scope: CoroutineScope): Job = scope.launch(Dispatchers.IO) {
        while (isActive) {
            try {
                buildConsumer().use { consumer ->
                    consumer.subscribe(listOf("prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON"))
                    log.info("Darwin subscribed (group=${Config.kafkaGroupId})")
                    while (isActive) {
                        val records = consumer.poll(Duration.ofSeconds(5))
                        for (record in records) record.value()?.let { handleDarwinMessage(it) }
                    }
                }
            } catch (e: CancellationException) { throw e
            } catch (e: Exception) { log.warn("Darwin error: ${e.message} — retrying in 10s"); delay(10_000) }
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
                is JSONArray  -> locationRaw
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
        } catch (e: Exception) { log.debug("Darwin parse error: ${e.message}") }
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

fun buildConsumer(offsetReset: String = "latest"): KafkaConsumer<String, String> {
    val props = Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        Config.kafkaBootstrap)
        put(ConsumerConfig.GROUP_ID_CONFIG,                 Config.kafkaGroupId)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        offsetReset)
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true")
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer::class.java.name)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,       "30000")
        put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,       "45000")
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         "500")
        put("security.protocol",  "SASL_SSL")
        put("sasl.mechanism",     "PLAIN")
        put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"${Config.kafkaUsername}\" password=\"${Config.kafkaPassword}\";")
        put("metric.reporters",          "")
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
    val zdt = java.time.Instant.ofEpochMilli(ms).atZone(java.time.ZoneId.of("UTC"))
    return "%02d:%02d".format(zdt.hour, zdt.minute)
}

fun minuteDelay(scheduled: String, actual: String): Int {
    val schParts = scheduled.split(":"); val actParts = actual.split(":")
    if (schParts.size < 2 || actParts.size < 2) return 0
    val schMins = (schParts[0].toIntOrNull() ?: 0) * 60 + (schParts[1].toIntOrNull() ?: 0)
    val actMins = (actParts[0].toIntOrNull() ?: 0) * 60 + (actParts[1].toIntOrNull() ?: 0)
    var diff = actMins - schMins
    if (diff < -120) diff += 1440
    return if (diff > 0) diff else 0
}

private fun stanoxToCrs(stanox: String): String? = CorpusLookup.crsFromStanox(stanox)

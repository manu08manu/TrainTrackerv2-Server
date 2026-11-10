package com.traintracker.server.kafka

import com.traintracker.server.Config
import com.traintracker.server.cif.CifParser
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.SmartLookup
import com.traintracker.server.cif.insertStops
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.MovementBatch
import com.traintracker.server.database.Schedules
import com.traintracker.server.database.Movements
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
val trainIdToRid = ConcurrentHashMap<String, String>()   // TRUST-format train ID -> schedule uid (from LINX activations)
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

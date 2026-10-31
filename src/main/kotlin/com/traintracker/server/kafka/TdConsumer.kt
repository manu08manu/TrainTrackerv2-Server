package com.traintracker.server.kafka

import com.traintracker.server.Config
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.SmartLookup
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.CancellationException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.json.JSONArray
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("TdConsumer")

object TdConsumer {
    private const val TOPIC = "TD_ALL_SIG_AREA"
    private const val WINDOW_MS = 60000L
    private const val MAX_SAMPLES = 6
    private val LONDON: ZoneId = ZoneId.of("Europe/London")
    private val HMS: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")
    private var records = 0L
    private var noCaRecords = 0L
    private var wrappers = 0L
    private var caCount = 0L
    private var nonHeadcode = 0L
    private var headcodeCa = 0L
    private var exact = 0L
    private var fromOnly = 0L
    private var toOnly = 0L
    private var noBerth = 0L
    private var noArea = 0L
    private var ccCount = 0L
    private var nonHeadcodeCc = 0L
    private var headcodeCc = 0L
    private var toOnlyCc = 0L
    private var noBerthCc = 0L
    private var noAreaCc = 0L
    private var ccPseudoMarker = 0L
    private var ccEntries1 = 0L
    private var ccEntries2 = 0L
    private var ccEntries3Plus = 0L
    private var parseErrors = 0L
    private var parseNanos = 0L
    private var windowStart = System.currentTimeMillis()
    private val resolvedSamples = ArrayList<String>()
    private val unresolvedSamples = ArrayList<String>()
    private val ccUnresolvedSamples = ArrayList<String>()
    private val msgTypeCounts = ConcurrentHashMap<String, Int>()
    private val diagCounter = AtomicLong(0L)

    fun start(scope: CoroutineScope): Job = scope.launch(Dispatchers.IO) {
        while (isActive) {
            try {
                buildTdConsumer().use { consumer ->
                    consumer.subscribe(listOf(TOPIC))
                    log.info("TD subscribed (group=${Config.kafkaGroupIdTd}), committed offsets")
                    while (isActive) {
                        val recs = consumer.poll(Duration.ofSeconds(5L))
                        for (r in recs) {
                            r.value()?.let { handle(it) }
                        }
                        TdStore.flush()
                        maybeLog()
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("TD error: ${e.message} -- retrying in 10s")
                delay(10000L)
            }
        }
    }

    private fun isHeadcode(d: String): Boolean =
        d.length == 4 && d[0].isDigit() && d[1].isLetter() && d[2].isDigit() && d[3].isDigit()

    private fun handle(json: String) {
        records++
        try {
            val diagArr = JSONArray(json)
            for (i in 0 until diagArr.length()) {
                val obj = diagArr.optJSONObject(i) ?: continue
                val key = obj.keys().asSequence().firstOrNull() ?: continue
                msgTypeCounts.merge(key, 1, Int::plus)
            }
            val n = diagCounter.incrementAndGet()
            if (n % 200L == 0L) log.info("TD msg_type distribution: $msgTypeCounts")
        } catch (_: Exception) {
        }
        if (!json.contains("\"CA_MSG\"")) {
            noCaRecords++
            return
        }
        val t0 = System.nanoTime()
        try {
            val arr = JSONArray(json)
            for (i in 0 until arr.length()) {
                wrappers++
                val ca = arr.optJSONObject(i)?.optJSONObject("CA_MSG") ?: continue
                caCount++
                val descr = ca.optString("descr", "")
                if (!isHeadcode(descr)) {
                    nonHeadcode++
                    continue
                }
                headcodeCa++
                val area = ca.optString("area_id", "")
                val from = ca.optString("from", "")
                val to = ca.optString("to", "")
                val m = SmartLookup.resolve(area, from, to)
                if (m == null) {
                    if (SmartLookup.hasArea(area)) noBerth++ else noArea++
                    if (unresolvedSamples.size < MAX_SAMPLES) {
                        unresolvedSamples.add("$area $from>$to $descr${if (SmartLookup.hasArea(area)) " (area in SMART)" else " (area NOT in SMART)"}")
                    }
                    continue
                }
                when (m.kind) {
                    SmartLookup.KIND_EXACT -> exact++
                    SmartLookup.KIND_FROM -> fromOnly++
                    else -> toOnly++
                }
                if (m.kind == SmartLookup.KIND_EXACT) {
                    val tdMs = ca.optString("time", "").toLongOrNull()
                    if (tdMs != null) {
                        for (se in m.entries) TdCorrelator.onTd(descr, se, tdMs)
                    }
                }
                if (resolvedSamples.size < MAX_SAMPLES) {
                    val e = m.entries[0]
                    val ms = ca.optString("time", "").toLongOrNull()
                    val t = if (ms != null) Instant.ofEpochMilli(ms).atZone(LONDON).format(HMS) else "?"
                    resolvedSamples.add(
                        "$area $from>$to $descr @$t London -> ${e.stanox} ev=${e.event} pl=${e.platform} step=${e.stepType} kind=${m.kind} n=${m.entries.size}"
                    )
                }
            }
        } catch (e: Exception) {
            parseErrors++
        }
        try {
            val ccArr = JSONArray(json)
            for (i in 0 until ccArr.length()) {
                val cc = ccArr.optJSONObject(i)?.optJSONObject("CC_MSG") ?: continue
                ccCount++
                val descr = cc.optString("descr", "")
                if (!isHeadcode(descr)) {
                    nonHeadcodeCc++
                    continue
                }
                headcodeCc++
                val area = cc.optString("area_id", "")
                val to = cc.optString("to", "")
                if (to == "COUT" || to == "STIN") {
                    ccPseudoMarker++
                    continue
                }
                val m = SmartLookup.resolve(area, "", to)
                if (m == null) {
                    if (SmartLookup.hasArea(area)) noBerthCc++ else noAreaCc++
                    if (ccUnresolvedSamples.size < MAX_SAMPLES) {
                        ccUnresolvedSamples.add("$area ->$to $descr${if (SmartLookup.hasArea(area)) " (area in SMART)" else " (area NOT in SMART)"}")
                    }
                    continue
                }
                toOnlyCc++
                if (m.entries.size == 1) ccEntries1++ else if (m.entries.size == 2) ccEntries2++ else ccEntries3Plus++
            }
        } catch (_: Exception) {
        }
        parseNanos += System.nanoTime() - t0
    }

    private fun maybeLog() {
        val now = System.currentTimeMillis()
        if (now - windowStart < WINDOW_MS) return
        val secs = (now - windowStart) / 1000
        val resolved = exact + fromOnly + toOnly
        val pct = if (headcodeCa > 0L) resolved * 100 / headcodeCa else 0L
        log.info(
            "TD ${secs}s: records=$records noCA=$noCaRecords wrappers=$wrappers CA=$caCount nonHeadcode=$nonHeadcode headcodeCA=$headcodeCa exact=$exact fromOnly=$fromOnly toOnly=$toOnly noBerth=$noBerth noArea=$noArea resolved=$pct% parseErr=$parseErrors parseMs=${parseNanos / 1000000} smartKeys=${SmartLookup.keyCount()}"
        )
        log.info(
            "TD-CC ${secs}s: ccCount=$ccCount nonHeadcodeCc=$nonHeadcodeCc headcodeCc=$headcodeCc toOnlyCc=$toOnlyCc noBerthCc=$noBerthCc noAreaCc=$noAreaCc pseudoMarker=$ccPseudoMarker entries1=$ccEntries1 entries2=$ccEntries2 entries3Plus=$ccEntries3Plus"
        )
        log.info(TdCorrelator.report())
        try {
            CorpusLookup.flushNoSmartCoverage()
        } catch (e: Exception) {
            log.warn("No-SMART-coverage flush error")
        }
        if (resolvedSamples.isNotEmpty()) log.info("TD resolved: ${resolvedSamples.joinToString(" | ")}")
        if (unresolvedSamples.isNotEmpty()) log.info("TD unresolved: ${unresolvedSamples.joinToString(" | ")}")
        if (ccUnresolvedSamples.isNotEmpty()) log.info("TD-CC unresolved: ${ccUnresolvedSamples.joinToString(" | ")}")
        records = 0L
        noCaRecords = 0L
        wrappers = 0L
        caCount = 0L
        nonHeadcode = 0L
        headcodeCa = 0L
        exact = 0L
        fromOnly = 0L
        toOnly = 0L
        noBerth = 0L
        noArea = 0L
        parseErrors = 0L
        parseNanos = 0L
        ccCount = 0L
        nonHeadcodeCc = 0L
        headcodeCc = 0L
        toOnlyCc = 0L
        noBerthCc = 0L
        noAreaCc = 0L
        ccPseudoMarker = 0L
        ccEntries1 = 0L
        ccEntries2 = 0L
        ccEntries3Plus = 0L
        resolvedSamples.clear()
        unresolvedSamples.clear()
        ccUnresolvedSamples.clear()
        windowStart = now
    }

    private fun buildTdConsumer(): KafkaConsumer<String, String> {
        val props = Properties().apply {
            put("bootstrap.servers", Config.kafkaBootstrap)
            put("group.id", Config.kafkaGroupIdTd)
            put("auto.offset.reset", "latest")
            put("enable.auto.commit", "true")
            put("key.deserializer", StringDeserializer::class.java.name)
            put("value.deserializer", StringDeserializer::class.java.name)
            put("request.timeout.ms", "30000")
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
}

package com.traintracker.server.kafka

import com.traintracker.server.Config
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.SmartLookup
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import org.json.JSONArray
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("TdConsumer")

/**
 * Train describer feed. Berth steps (CA) of headcode-described trains are resolved through SMART to STANOX
 * events, which are stored (TdStore) and compared with the reported movements (TdCorrelator). CC messages are only counted.
 * Resolution statistics are logged once a minute.
 */
object TdConsumer {
    private const val WINDOW_MS = 60_000L
    private const val MAX_SAMPLES = 6
    private val LONDON: ZoneId = ZoneId.of("Europe/London")
    private val HMS: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")

    /** Counters for one logging window. */
    private class Stats {
        var records = 0L; var noCa = 0L; var wrappers = 0L; var ca = 0L; var nonHeadcode = 0L; var headcodeCa = 0L
        var exact = 0L; var fromOnly = 0L; var toOnly = 0L; var noBerth = 0L; var noArea = 0L
        var cc = 0L; var nonHeadcodeCc = 0L; var headcodeCc = 0L; var toOnlyCc = 0L; var noBerthCc = 0L; var noAreaCc = 0L
        var ccPseudoMarker = 0L; var ccEntries1 = 0L; var ccEntries2 = 0L; var ccEntries3Plus = 0L
        var parseErrors = 0L; var parseNanos = 0L
        val windowStart = System.currentTimeMillis()
        val resolvedSamples = ArrayList<String>()
        val unresolvedSamples = ArrayList<String>()
        val ccUnresolvedSamples = ArrayList<String>()
    }

    private var stats = Stats()
    private val msgTypeCounts = ConcurrentHashMap<String, Int>()
    private var messagesSeen = 0L

    fun start(scope: CoroutineScope): Job =
        launchConsumer(scope, "TD", "TD_ALL_SIG_AREA", Config.kafkaGroupIdTd, log) { _, records ->
            for (r in records) r.value()?.let { handle(it) }
            TdStore.flush()
            maybeLog()
        }

    private fun isHeadcode(d: String): Boolean =
        d.length == 4 &&
        ((d[0].isDigit() && d[1].isLetter() && d[2].isDigit() && d[3].isDigit()) ||
         (d[0] in 'A'..'Z' && d[1] in '1'..'9' && d[2].isDigit() && d[3].isDigit()))

    private fun areaNote(area: String) = if (SmartLookup.hasArea(area)) " (area in SMART)" else " (area NOT in SMART)"

    private fun handle(json: String) {
        val s = stats
        s.records++
        val arr = try {
            JSONArray(json)
        } catch (_: Exception) {
            null
        }
        if (arr != null) {
            for (i in 0 until arr.length()) {
                val key = arr.optJSONObject(i)?.keys()?.asSequence()?.firstOrNull() ?: continue
                msgTypeCounts.merge(key, 1, Int::plus)
            }
            if (++messagesSeen % 200L == 0L) log.info("TD msg_type distribution: $msgTypeCounts")
        }
        if (!json.contains("\"CA_MSG\"")) {
            s.noCa++
            return
        }
        val t0 = System.nanoTime()
        if (arr == null) {
            s.parseErrors++
        } else {
            try {
                for (i in 0 until arr.length()) {
                    s.wrappers++
                    handleCa(s, arr.optJSONObject(i)?.optJSONObject("CA_MSG") ?: continue)
                }
            } catch (e: Exception) {
                s.parseErrors++
            }
            try {
                for (i in 0 until arr.length()) handleCc(s, arr.optJSONObject(i)?.optJSONObject("CC_MSG") ?: continue)
            } catch (_: Exception) {
            }
        }
        s.parseNanos += System.nanoTime() - t0
    }

    private fun handleCa(s: Stats, ca: org.json.JSONObject) {
        s.ca++
        val descr = ca.optString("descr", "")
        if (!isHeadcode(descr)) {
            s.nonHeadcode++
            return
        }
        s.headcodeCa++
        val area = ca.optString("area_id", "")
        val from = ca.optString("from", "")
        val to = ca.optString("to", "")
        val m = SmartLookup.resolve(area, from, to)
        if (m == null) {
            if (SmartLookup.hasArea(area)) s.noBerth++ else s.noArea++
            if (s.unresolvedSamples.size < MAX_SAMPLES) s.unresolvedSamples.add("$area $from>$to $descr${areaNote(area)}")
            return
        }
        when (m.kind) {
            SmartLookup.KIND_EXACT -> s.exact++
            SmartLookup.KIND_FROM -> s.fromOnly++
            else -> s.toOnly++
        }
        val tdMs = ca.optString("time", "").toLongOrNull()
        if (m.kind == SmartLookup.KIND_EXACT && tdMs != null) {
            for (entry in m.entries) TdCorrelator.onTd(descr, entry, tdMs)
        }
        if (s.resolvedSamples.size < MAX_SAMPLES) {
            val e = m.entries[0]
            val t = if (tdMs != null) Instant.ofEpochMilli(tdMs).atZone(LONDON).format(HMS) else "?"
            s.resolvedSamples.add(
                "$area $from>$to $descr @$t London -> ${e.stanox} ev=${e.event} pl=${e.platform} step=${e.stepType} kind=${m.kind} n=${m.entries.size}"
            )
        }
    }

    private fun handleCc(s: Stats, cc: org.json.JSONObject) {
        s.cc++
        val descr = cc.optString("descr", "")
        if (!isHeadcode(descr)) {
            s.nonHeadcodeCc++
            return
        }
        s.headcodeCc++
        val area = cc.optString("area_id", "")
        val to = cc.optString("to", "")
        if (to == "COUT" || to == "STIN") {
            s.ccPseudoMarker++
            return
        }
        val m = SmartLookup.resolve(area, "", to)
        if (m == null) {
            if (SmartLookup.hasArea(area)) s.noBerthCc++ else s.noAreaCc++
            if (s.ccUnresolvedSamples.size < MAX_SAMPLES) s.ccUnresolvedSamples.add("$area ->$to $descr${areaNote(area)}")
            return
        }
        s.toOnlyCc++
        when (m.entries.size) {
            1 -> s.ccEntries1++
            2 -> s.ccEntries2++
            else -> s.ccEntries3Plus++
        }
    }

    private fun maybeLog() {
        val s = stats
        val now = System.currentTimeMillis()
        if (now - s.windowStart < WINDOW_MS) return
        val secs = (now - s.windowStart) / 1000
        val resolved = s.exact + s.fromOnly + s.toOnly
        val pct = if (s.headcodeCa > 0L) resolved * 100 / s.headcodeCa else 0L
        log.info(
            "TD ${secs}s: records=${s.records} noCA=${s.noCa} wrappers=${s.wrappers} CA=${s.ca} nonHeadcode=${s.nonHeadcode} headcodeCA=${s.headcodeCa} exact=${s.exact} fromOnly=${s.fromOnly} toOnly=${s.toOnly} noBerth=${s.noBerth} noArea=${s.noArea} resolved=$pct% parseErr=${s.parseErrors} parseMs=${s.parseNanos / 1000000} smartKeys=${SmartLookup.keyCount()}"
        )
        log.info(
            "TD-CC ${secs}s: ccCount=${s.cc} nonHeadcodeCc=${s.nonHeadcodeCc} headcodeCc=${s.headcodeCc} toOnlyCc=${s.toOnlyCc} noBerthCc=${s.noBerthCc} noAreaCc=${s.noAreaCc} pseudoMarker=${s.ccPseudoMarker} entries1=${s.ccEntries1} entries2=${s.ccEntries2} entries3Plus=${s.ccEntries3Plus}"
        )
        log.info(TdCorrelator.report())
        try {
            CorpusLookup.flushNoSmartCoverage()
        } catch (e: Exception) {
            log.warn("No-SMART-coverage flush error")
        }
        if (s.resolvedSamples.isNotEmpty()) log.info("TD resolved: ${s.resolvedSamples.joinToString(" | ")}")
        if (s.unresolvedSamples.isNotEmpty()) log.info("TD unresolved: ${s.unresolvedSamples.joinToString(" | ")}")
        if (s.ccUnresolvedSamples.isNotEmpty()) log.info("TD-CC unresolved: ${s.ccUnresolvedSamples.joinToString(" | ")}")
        stats = Stats()
    }
}

package com.traintracker.server.kafka

import com.traintracker.server.cif.SmartEntry
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

object TdCorrelator {
    private const val TTL_MS = 180000L
    private const val MAX_SAMPLES = 6
    private val LONDON: ZoneId = ZoneId.of("Europe/London")
    private val HM: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm")
    private val HMS: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")
    private val pending = ConcurrentHashMap<String, Pending>()
    private val agree = AtomicLong()
    private val disagree = AtomicLong()
    private val tdOnly = AtomicLong()
    private val movementOnly = AtomicLong()
    private val disagreeSamples = ConcurrentLinkedQueue<String>()
    private val movementOnlySamples = ConcurrentLinkedQueue<String>()

    fun onTd(headcode: String, e: SmartEntry, stepMs: Long) {
        val off = e.offsetSec ?: return
        val type = when (e.event) {
            "A", "C" -> "ARRIVAL"
            "B", "D" -> "DEPARTURE"
            else -> return
        }
        val exact = stepMs + off * 1000L
        val minute = Instant.ofEpochMilli(exact).atZone(LONDON).format(HM)
        TdStore.enqueue(headcode, e.stanox, type, exact, e.platform)
        meet("$headcode|${e.stanox}|$type", true, minute, exact)
    }

    fun onMovement(headcode: String, stanox: String, type: String, minute: String) {
        meet("$headcode|$stanox|$type", false, minute, 0L)
    }

    private fun meet(key: String, fromTd: Boolean, minute: String, exactMs: Long) {
        val now = System.currentTimeMillis()
        val other = pending.remove(key)
        if (other != null) {
            if (other.fromTd != fromTd && now - other.at < TTL_MS) {
                val tdMinute = if (fromTd) minute else other.minute
                val movementMinute = if (fromTd) other.minute else minute
                if (tdMinute == movementMinute) {
                    agree.incrementAndGet()
                } else {
                    disagree.incrementAndGet()
                    if (disagreeSamples.size < MAX_SAMPLES) {
                        val tdExact = if (fromTd) exactMs else other.exactMs
                        disagreeSamples.add("$key td=${Instant.ofEpochMilli(tdExact).atZone(LONDON).format(HMS)} move=$movementMinute")
                    }
                }
                return
            }
            if (now - other.at >= TTL_MS) expire(key, other)
        }
        pending[key] = Pending(fromTd, minute, exactMs, now)
    }

    private fun expire(key: String, p: Pending) {
        if (p.fromTd) {
            tdOnly.incrementAndGet()
        } else {
            movementOnly.incrementAndGet()
            if (movementOnlySamples.size < MAX_SAMPLES) movementOnlySamples.add(key)
        }
    }

    private fun sweep() {
        val cutoff = System.currentTimeMillis() - TTL_MS
        for ((k, p) in pending) {
            if (p.at < cutoff && pending.remove(k, p)) expire(k, p)
        }
    }

    private fun drain(q: ConcurrentLinkedQueue<String>): String? {
        if (q.isEmpty()) return null
        val out = ArrayList<String>()
        while (true) out.add(q.poll() ?: break)
        return out.joinToString(" ; ")
    }

    fun report(): String {
        sweep()
        val a = agree.getAndSet(0L)
        val d = disagree.getAndSet(0L)
        val t = tdOnly.getAndSet(0L)
        val r = movementOnly.getAndSet(0L)
        val pct = if (a + d > 0L) a * 100 / (a + d) else 0L
        val sb = StringBuilder("TD/movement minute check: agree=$a disagree=$d ($pct% agree) tdOnlyExpired=$t movementOnlyExpired=$r pending=${pending.size}")
        drain(disagreeSamples)?.let { sb.append(" | disagree: ").append(it) }
        drain(movementOnlySamples)?.let { sb.append(" | movementOnly: ").append(it) }
        return sb.toString()
    }

    private class Pending(val fromTd: Boolean, val minute: String, val exactMs: Long, val at: Long)
}

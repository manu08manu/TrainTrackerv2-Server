package com.traintracker.server.kafka

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("TdStore")

/** Buffers TD-derived STANOX events and writes them to td_events in bulk; keeps 31 days. */
object TdStore {
    private const val CHUNK = 400
    private const val MAX_BATCH = 4000
    private const val MAX_QUEUE = 100_000
    private const val RETENTION_DAYS = 31L
    private val queue = ConcurrentLinkedQueue<Row>()
    private val queued = AtomicInteger()
    private var lastPrune = 0L
    private var lastReport = System.currentTimeMillis()
    private var written = 0L
    private var failed = 0L
    private var dropped = 0L

    private class Row(val headcode: String, val stanox: String, val type: Char, val eventS: Long)

    fun enqueue(headcode: String, stanox: String, type: String, exactMs: Long) {
        if (headcode.length != 4 || stanox.isEmpty() || stanox.length > 6) return
        if (!headcode.all { it.isLetterOrDigit() } || !stanox.all { it.isLetterOrDigit() }) return
        if (queued.get() >= MAX_QUEUE) {
            dropped++
            return
        }
        queue.add(Row(headcode, stanox, if (type == "ARRIVAL") 'A' else 'D', exactMs / 1000))
        queued.incrementAndGet()
    }

    /** Writes up to MAX_BATCH queued rows, prunes hourly and reports once a minute. */
    fun flush() {
        val now = System.currentTimeMillis()
        try {
            val batch = ArrayList<Row>()
            while (batch.size < MAX_BATCH) {
                batch.add(queue.poll() ?: break)
                queued.decrementAndGet()
            }
            for (chunk in batch.chunked(CHUNK)) {
                // Values are validated alphanumerics and numbers, so the INSERT is built as text.
                val values = chunk.joinToString(",") { "('${it.headcode}','${it.stanox}','${it.type}',${it.eventS})" }
                try {
                    transaction { exec("INSERT OR IGNORE INTO td_events (headcode, stanox, event_type, event_s) VALUES $values") }
                    written += chunk.size
                } catch (e: Exception) {
                    failed += chunk.size
                    log.warn("td_events write failed: ${e.message}")
                }
            }
            if (now - lastPrune > 3_600_000L) {
                lastPrune = now
                val cutoff = Instant.now().minus(RETENTION_DAYS, ChronoUnit.DAYS).epochSecond
                transaction { exec("DELETE FROM td_events WHERE event_s < $cutoff") }
            }
        } catch (e: Exception) {
            log.warn("TdStore error: ${e.message}")
        }
        if (now - lastReport >= 60_000L) {
            log.info("TD store: written=$written failed=$failed dropped=$dropped queued=${queued.get()}")
            written = 0L
            failed = 0L
            dropped = 0L
            lastReport = now
        }
    }
}

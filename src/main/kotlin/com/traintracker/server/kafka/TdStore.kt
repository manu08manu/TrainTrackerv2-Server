package com.traintracker.server.kafka

import com.traintracker.server.database.Retention
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("TdStore")

object TdStore {
    private const val CHUNK = 400
    private const val MAX_BATCH = 4000
    private const val MAX_QUEUE = 100000
    private val queue = ConcurrentLinkedQueue<Row>()
    private val queued = AtomicInteger()
    private var tableReady = false
    private var lastPrune = 0L
    private var lastReport = System.currentTimeMillis()
    private var written = 0L
    private var failed = 0L
    private var dropped = 0L

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

    fun flush() {
        val now = System.currentTimeMillis()
        try {
            if (!tableReady) createTable()
            val batch = ArrayList<Row>()
            while (batch.size < MAX_BATCH) {
                val r = queue.poll() ?: break
                queued.decrementAndGet()
                batch.add(r)
            }
            for (chunk in batch.chunked(CHUNK)) {
                val values = chunk.joinToString(",") { "('${it.headcode}','${it.stanox}','${it.type}',${it.eventS})" }
                try {
                    transaction { exec("INSERT OR IGNORE INTO td_events (headcode, stanox, event_type, event_s) VALUES $values") }
                    written += chunk.size
                } catch (e: Exception) {
                    failed += chunk.size
                    log.warn("td_events write failed: ${e.message}")
                }
            }
            if (now - lastPrune > 3600000L) {
                lastPrune = now
                prune(now)
            }
        } catch (e: Exception) {
            log.warn("TdStore error: ${e.message}")
        }
        if (now - lastReport >= 60000L) {
            log.info("TD store: written=$written failed=$failed dropped=$dropped queued=${queued.get()}")
            written = 0L
            failed = 0L
            dropped = 0L
            lastReport = now
        }
    }

    private fun createTable() {
        transaction {
            exec("CREATE TABLE IF NOT EXISTS td_events (headcode TEXT NOT NULL, stanox TEXT NOT NULL, event_type TEXT NOT NULL, event_s INTEGER NOT NULL, PRIMARY KEY (headcode, stanox, event_type, event_s)) WITHOUT ROWID")
            exec("CREATE INDEX IF NOT EXISTS idx_td_events_s ON td_events(event_s)")
        }
        tableReady = true
        log.info("td_events table ready")
    }

    private fun prune(nowMs: Long) {
        val cutoff = Retention.cutoffEpochSeconds()
        transaction { exec("DELETE FROM td_events WHERE event_s < $cutoff") }
    }

    private class Row(val headcode: String, val stanox: String, val type: Char, val eventS: Long)
}

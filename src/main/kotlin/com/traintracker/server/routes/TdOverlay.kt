package com.traintracker.server.routes

import com.traintracker.server.database.query
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import kotlin.math.abs

private val log = LoggerFactory.getLogger("TdOverlay")

/** TD times for one headcode's service day, used to give movements a time to the second. */
class TdOverlay private constructor(private val byKey: Map<String, List<Long>>) {
    class Result(val minute: String, val seconds: String)

    /** The TD time closest to when the event was recorded (within 15 minutes), or null. */
    fun resolve(stanox: String, eventType: String, eventTs: String?): Result? {
        if (byKey.isEmpty() || eventTs.isNullOrEmpty()) return null
        val list = byKey["$stanox|$eventType"] ?: return null
        val written = try {
            LocalDateTime.parse(eventTs.replace(' ', 'T')).atZone(LONDON).toEpochSecond()
        } catch (e: Exception) {
            return null
        }
        val best = list.filter { abs(it - written) <= MATCH_WINDOW_S }.minByOrNull { abs(it - written) } ?: return null
        return Result(
            Instant.ofEpochSecond(best).atZone(LONDON).format(HM),
            Instant.ofEpochSecond(best).atZone(LONDON).format(HMS)
        )
    }

    companion object {
        private val LONDON: ZoneId = ZoneId.of("Europe/London")
        private val HM: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm")
        private val HMS: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")
        private const val MATCH_WINDOW_S: Long = 900L
        val EMPTY = TdOverlay(emptyMap())

        fun load(headcode: String, svcDate: String): TdOverlay {
            val hc = headcode.filter { it.isLetterOrDigit() }
            if (hc.length != 4) return EMPTY
            return try {
                val day = LocalDate.parse(svcDate).atStartOfDay(LONDON)
                val from = day.minusHours(12).toEpochSecond()
                val to = day.plusHours(36).toEpochSecond()
                val map = HashMap<String, MutableList<Long>>()
                transaction {
                    query("SELECT stanox, event_type, event_s FROM td_events WHERE nr_headcode=? AND event_s >= $from AND event_s < $to", listOf(hc)) { rs ->
                        while (rs.next()) {
                            val t = if (rs.getString(2) == "A") "ARRIVAL" else "DEPARTURE"
                            map.getOrPut("${rs.getString(1)}|$t") { ArrayList() }.add(rs.getLong(3))
                        }
                    }
                }
                TdOverlay(map)
            } catch (e: Exception) {
                log.debug("TdOverlay load failed: ${e.message}")
                EMPTY
            }
        }
    }
}

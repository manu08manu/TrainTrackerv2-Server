package com.traintracker.server.kafka

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import kotlin.math.abs

/**
 * LINX labels its times "-00:00". Whether that is true UTC, or UK clock time labelled as UTC (as TRUST's
 * were), only shows in British Summer Time. [vote] tells them apart per message; the conversions take the
 * decision ([labelsUkTime]) and always return UK clock time, like the timetable.
 */
internal object LinxClock {
    private val LONDON: ZoneId = ZoneId.of("Europe/London")
    private val HHMM: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm")

    /**
     * Kafka's timestamp is true UTC and lands within ~1.5 seconds of LINX's MessageDateTime ([sentAt]).
     * Returns true when the label is about an hour ahead (UK summer time labelled as UTC), false when they
     * agree (true UTC; in GMT both readings agree), null when there is nothing to judge.
     */
    fun vote(sentAt: String?, kafkaMs: Long): Boolean? {
        val labelledS = try { OffsetDateTime.parse(sentAt ?: return null).toEpochSecond() } catch (e: Exception) { return null }
        val gap = labelledS - kafkaMs / 1000
        return when {
            gap in 1800..5400 -> true
            abs(gap) < 1800 -> false
            else -> null
        }
    }

    /** LINX date-time ("2026-11-10T13:31:00-00:00") -> UK clock time "HH:mm"; "" when missing. */
    fun ukTime(ts: String, labelsUkTime: Boolean): String {
        if (ts.length < 16) return ""
        if (labelsUkTime) return ts.substring(11, 16)
        return try {
            OffsetDateTime.parse(ts).atZoneSameInstant(LONDON).format(HHMM)
        } catch (e: Exception) {
            ts.substring(11, 16)
        }
    }

    /** A LINX time with no date or offset ("14:17:00", on [date]) -> UK clock time "HH:mm"; "" when missing. */
    fun ukClock(time: String?, date: String, labelsUkTime: Boolean): String {
        val t = time?.takeIf { it.length >= 5 } ?: return ""
        if (labelsUkTime) return t.take(5)
        return try {
            LocalDateTime.of(LocalDate.parse(date), LocalTime.parse(t)).atOffset(ZoneOffset.UTC).atZoneSameInstant(LONDON).format(HHMM)
        } catch (e: Exception) {
            t.take(5)
        }
    }

    /** LINX date-time -> epoch seconds (the true instant); null when unparseable. */
    fun epochSeconds(ts: String?, labelsUkTime: Boolean): Long? = try {
        ts?.let {
            if (labelsUkTime) LocalDateTime.parse(it.substring(0, 19)).atZone(LONDON).toEpochSecond()
            else OffsetDateTime.parse(it).toEpochSecond()
        }
    } catch (e: Exception) {
        null
    }
}

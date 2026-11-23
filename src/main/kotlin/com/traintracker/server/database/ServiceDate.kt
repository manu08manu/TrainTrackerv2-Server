package com.traintracker.server.database

import java.time.LocalDate
import java.time.LocalDateTime

/**
 * The service date of a TRUST train ID seen at [ts]: the date ending in the train ID's day of month
 * whose service day (12 hours before to 36 hours after its start) contains [ts]. Falls back to the
 * calendar date of [ts] when there is none. Three consecutive dates never share a day of month,
 * so the answer stays the same as [ts] moves through the service day.
 * The same rule is in scripts/migrate_movements.py (SQL): keep them in step.
 */
fun movementServiceDate(trainId: String, ts: LocalDateTime): LocalDate {
    val day = ts.toLocalDate()
    val dom = trainId.takeLast(2).toIntOrNull() ?: return day
    return listOf(day, day.minusDays(1), day.plusDays(1)).firstOrNull { d ->
        d.dayOfMonth == dom &&
            !ts.isBefore(d.atStartOfDay().minusHours(12)) &&
            ts.isBefore(d.atStartOfDay().plusHours(36))
    } ?: day
}

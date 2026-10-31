package com.traintracker.server.database

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

object Retention {
    const val MIN_DAYS: Long = 30L
    const val DELETE_AFTER_DAYS: Long = 31L
    const val SQL_MODIFIER: String = "-31 days"

    fun cutoffLocal(): LocalDateTime = LocalDateTime.now().minusDays(DELETE_AFTER_DAYS)

    fun cutoffUtc(): LocalDateTime = LocalDateTime.now(ZoneOffset.UTC).minusDays(DELETE_AFTER_DAYS)

    fun cutoffEpochSeconds(): Long = Instant.now().minus(DELETE_AFTER_DAYS, ChronoUnit.DAYS).epochSecond

    fun cutoffStamp(): String =
        Instant.now().minus(DELETE_AFTER_DAYS, ChronoUnit.DAYS).toString().replace('T', ' ').take(19)
}

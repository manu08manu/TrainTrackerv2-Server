package com.traintracker.server.kafka

import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class LinxClockTest {
    private fun ms(iso: String) = Instant.parse(iso).toEpochMilli()

    @Test
    fun `vote tells true UTC from UK summer time labelled as UTC`() {
        // Kafka's timestamp is the true instant; LINX's label is either the same or an hour ahead in summer.
        assertEquals(false, LinxClock.vote("2027-07-01T08:00:01-00:00", ms("2027-07-01T08:00:00Z")))
        assertEquals(true, LinxClock.vote("2027-07-01T09:00:01-00:00", ms("2027-07-01T08:00:00Z")))
        assertEquals(false, LinxClock.vote("2026-11-10T13:18:59-00:00", ms("2026-11-10T13:19:00Z")))
        assertNull(LinxClock.vote("2027-07-01T12:00:00-00:00", ms("2027-07-01T08:00:00Z")))   // far off: no vote
        assertNull(LinxClock.vote(null, 0))
        assertNull(LinxClock.vote("garbage", 0))
    }

    @Test
    fun `winter times are the same either way`() {
        for (uk in listOf(true, false)) {
            assertEquals("13:31", LinxClock.ukTime("2026-11-10T13:31:00-00:00", uk))
            assertEquals("14:17", LinxClock.ukClock("14:17:00", "2026-11-10", uk))
            assertEquals(Instant.parse("2026-11-10T13:31:00Z").epochSecond, LinxClock.epochSeconds("2026-11-10T13:31:00-00:00", uk))
        }
    }

    @Test
    fun `summer true UTC is moved to UK time`() {
        assertEquals("09:00", LinxClock.ukTime("2027-07-01T08:00:00-00:00", labelsUkTime = false))
        assertEquals("09:00", LinxClock.ukClock("08:00:00", "2027-07-01", labelsUkTime = false))
        assertEquals("00:30", LinxClock.ukTime("2027-06-30T23:30:00-00:00", labelsUkTime = false))   // crosses midnight
        assertEquals(Instant.parse("2027-07-01T08:00:00Z").epochSecond, LinxClock.epochSeconds("2027-07-01T08:00:00-00:00", false))
    }

    @Test
    fun `summer UK time labelled as UTC is left as it is`() {
        assertEquals("08:00", LinxClock.ukTime("2027-07-01T08:00:00-00:00", labelsUkTime = true))
        assertEquals("08:00", LinxClock.ukClock("08:00:00", "2027-07-01", labelsUkTime = true))
        // ...but its true instant is an hour earlier, which TD platform matching needs.
        assertEquals(Instant.parse("2027-07-01T07:00:00Z").epochSecond, LinxClock.epochSeconds("2027-07-01T08:00:00-00:00", true))
    }

    @Test
    fun `missing or malformed times`() {
        assertEquals("", LinxClock.ukTime("", false))
        assertEquals("", LinxClock.ukClock(null, "2026-11-10", false))
        assertEquals("14:17", LinxClock.ukClock("14:17:00", "not-a-date", false))
        assertNull(LinxClock.epochSeconds(null, false))
        assertNull(LinxClock.epochSeconds("garbage", true))
    }
}

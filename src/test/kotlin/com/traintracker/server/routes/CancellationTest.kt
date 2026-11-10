package com.traintracker.server.routes

import kotlin.test.Test
import kotlin.test.assertEquals

/** How cancellations show on the service page, including change of origin. */
class CancellationTest {
    private fun cp(
        crs: String, time: String, stopType: String = "LI", actual: String = "", cancelled: Boolean = false, reason: String? = null,
    ) = CallingPointResponse(
        tiploc = crs, crs = crs, name = crs, scheduledTime = time, platform = null, isPass = false, stopType = stopType,
        actualTime = actual, isCancelled = cancelled, cancelReason = reason,
    )

    /** "crs:C" for cancelled, "crs" for not, so a whole run reads as one string. */
    private fun shape(cps: List<CallingPointResponse>) = cps.joinToString(" ") { if (it.isCancelled) "${it.crs}:C" else it.crs!! }

    @Test
    fun `cancelled en route cancels the calls after it`() {
        val out = propagateCancellations(
            listOf(
                cp("NCL", "10:00", "LO", actual = "10:01"),
                cp("DHM", "10:15", cancelled = true, reason = "XY"),
                cp("DAR", "10:30"),
                cp("YRK", "11:00", "LT"),
            )
        )
        assertEquals("NCL DHM:C DAR:C YRK:C", shape(out))
        assertEquals("XY", out[2].cancelReason)
    }

    @Test
    fun `cancelled at origin before running is cancelled throughout`() {
        val out = propagateCancellations(listOf(cp("NCL", "10:00", "LO", cancelled = true, reason = "M8"), cp("DAR", "10:30"), cp("YRK", "11:00", "LT")))
        assertEquals("NCL:C DAR:C YRK:C", shape(out))
    }

    @Test
    fun `calls that already ran are not cancelled after the event`() {
        val out = propagateCancellations(
            listOf(cp("NCL", "10:00", "LO", actual = "10:00"), cp("DHM", "10:15", cancelled = true), cp("DAR", "10:30", actual = "10:31"), cp("YRK", "11:00", "LT"))
        )
        assertEquals("NCL DHM:C DAR YRK:C", shape(out))
    }

    // ---- Change of origin -------------------------------------------------------------------------------

    @Test
    fun `a change of origin cancels only the stops before the new start`() {
        // One stop cut off (like 2J57 from Crouch Hill): as a plain cancellation it would cancel the whole run.
        val cps = listOf(
            cp("GPO", "13:09", "LO", cancelled = true, reason = "YI"),
            cp("CRH", "13:15"),
            cp("HRY", "13:20"),
            cp("BKG", "13:40", "LT"),
        )
        assertEquals("GPO:C CRH:C HRY:C BKG:C", shape(propagateCancellations(cps)), "plain spreading, for contrast")
        assertEquals("GPO:C CRH HRY BKG", shape(applyCancellations(cps) { it.crs == "GPO" }))
    }

    @Test
    fun `a real cancellation later in the run still spreads after a change of origin`() {
        val cps = listOf(
            cp("NCL", "14:34", "LO", cancelled = true, reason = "I2"),
            cp("BHM", "17:34", actual = "17:35"),
            cp("BAN", "18:19", cancelled = true, reason = "XA"),
            cp("OXF", "18:41"),
            cp("RDG", "19:05", "LT"),
        )
        val out = applyCancellations(cps) { it.crs == "NCL" }
        assertEquals("NCL:C BHM BAN:C OXF:C RDG:C", shape(out))
        assertEquals("I2", out[0].cancelReason)
        assertEquals("XA", out[3].cancelReason)
    }

    @Test
    fun `stops before the activation origin are shown cancelled`() {
        // 1V91 on 2026-11-10: Newcastle - Reading, now starting at Birmingham New Street.
        val all = listOf(
            cp("NCL", "14:34", "LO", cancelled = true, reason = "AC/DC trip"),
            cp("YRK", "15:37"),
            cp("DBY", "16:53"),
            cp("BHM", "17:34"),
            cp("OXF", "18:41"),
            cp("RDG", "19:05", "LT"),
        )
        val out = cancelBeforeOrigin(all, "BHM")
        assertEquals("NCL:C YRK:C DBY:C BHM OXF RDG", shape(out))
        assertEquals("AC/DC trip", out[1].cancelReason)
    }

    @Test
    fun `cancelBeforeOrigin leaves the run alone when nothing moved`() {
        val all = listOf(cp("NCL", "14:34", "LO"), cp("YRK", "15:37"), cp("RDG", "19:05", "LT"))
        assertEquals(all, cancelBeforeOrigin(all, "NCL"))   // timetabled origin
        assertEquals(all, cancelBeforeOrigin(all, null))    // no activation yet
        assertEquals(all, cancelBeforeOrigin(all, "XXX"))   // not on this run
        assertEquals(all, cancelBeforeOrigin(all, "RDG"))   // the terminus: not a real start
    }

    @Test
    fun `a stop that reported running before the new origin is not cancelled`() {
        val all = listOf(cp("NCL", "14:34", "LO", actual = "14:35"), cp("YRK", "15:37"), cp("BHM", "17:34"), cp("RDG", "19:05", "LT"))
        assertEquals("NCL YRK:C BHM RDG", shape(cancelBeforeOrigin(all, "BHM")))
    }
}

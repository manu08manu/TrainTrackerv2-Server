package com.traintracker.server.routes

import kotlin.test.Test
import kotlin.test.assertEquals

class PunctualityTest {
    private fun stop(crs: String, stanox: String?, time: String, type: String = "LI", arr: String? = null, dep: String? = time) =
        TimetabledStop(crs, stanox, time, arr, dep, type)

    private fun dep(stanox: String?, crs: String?, sched: String, actual: String) =
        ReportedMovement(stanox, crs, sched, actual, "DEPARTURE", false, null)

    private fun arr(stanox: String?, crs: String?, sched: String, actual: String) =
        ReportedMovement(stanox, crs, sched, actual, "ARRIVAL", false, null)

    private fun canc(stanox: String?, crs: String?, sched: String, type: String = "CANCELLATION", reason: String = "XY") =
        ReportedMovement(stanox, crs, sched, null, type, true, reason)

    /** "crs status delay" per call. */
    private fun shape(o: List<CallOutcome>) = o.joinToString(", ") { "${it.crs} ${it.status}${it.delay?.let { d -> " $d" } ?: ""}" }

    @Test
    fun `bands by departure, and arrival at the terminus`() {
        val stops = listOf(
            stop("AAA", "1", "10:00", "LO"),
            stop("BBB", "2", "10:10"),
            stop("CCC", "3", "10:20"),
            stop("DDD", "4", "10:40", "LT", arr = "10:40", dep = null),
        )
        val moves = listOf(
            dep("1", "AAA", "10:00", "09:59"),
            dep("2", "BBB", "10:10", "10:10"),
            arr("3", "CCC", "10:20", "10:22"),   // arrival only: still counts
            arr("4", "DDD", "10:40", "10:57"),
        )
        assertEquals("AAA early -1, BBB ontime 0, CCC late 2, DDD late 17", shape(callOutcomes(stops, moves, null)))
    }

    @Test
    fun `working timetable times a few minutes off the public ones still match`() {
        // Leeds terminus on 2026-11-04: public arrival 09:21, movement booked 09:19 (WTT), arrived 09:25.
        val stops = listOf(stop("HUD", "1", "08:50", "LO"), stop("LDS", "17132", "09:21", "LT", arr = "09:21", dep = null))
        val moves = listOf(dep("1", "HUD", "08:50", "08:51"), arr("17132", "LDS", "09:19", "09:25"))
        assertEquals("HUD late 1, LDS late 4", shape(callOutcomes(stops, moves, null)))
    }

    @Test
    fun `a movement more than 10 minutes away is a different call`() {
        val stops = listOf(stop("AAA", "1", "10:00", "LO"), stop("BBB", "2", "10:30", "LT", arr = "10:30", dep = null))
        val moves = listOf(dep("1", "AAA", "10:00", "10:00"), arr("2", "BBB", "10:45", "10:46"))
        assertEquals("AAA ontime 0, BBB noreport", shape(callOutcomes(stops, moves, null)))
    }

    @Test
    fun `location is matched by STANOX when the CRS differs`() {
        // Clapham Junction movements carry CRS XCP for STANOX 87219.
        val stops = listOf(stop("CLJ", "87219", "10:00", "LO"), stop("WAT", "87001", "10:10", "LT", arr = "10:10", dep = null))
        val moves = listOf(dep("87219", "XCP", "10:00", "10:03"), arr("87001", "WAT", "10:10", "10:12"))
        assertEquals("CLJ late 3, WAT late 2", shape(callOutcomes(stops, moves, null)))
    }

    @Test
    fun `cancellation en route cancels the rest`() {
        val stops = listOf(stop("AAA", "1", "10:00", "LO"), stop("BBB", "2", "10:10"), stop("CCC", "3", "10:20", "LT", arr = "10:20", dep = null))
        val moves = listOf(dep("1", "AAA", "10:00", "10:05"), canc("2", "BBB", "10:10"))
        assertEquals("AAA late 5, BBB cancelled, CCC cancelled", shape(callOutcomes(stops, moves, null)))
    }

    @Test
    fun `change of origin cancels only the stops before the new start`() {
        val stops = listOf(stop("AAA", "1", "10:00", "LO"), stop("BBB", "2", "10:10"), stop("CCC", "3", "10:20", "LT", arr = "10:20", dep = null))
        val moves = listOf(canc("1", "AAA", "10:00", type = "COO_CANCELLED", reason = "YI"), dep("2", "BBB", "10:10", "10:11"))
        assertEquals("AAA cancelled, BBB late 1, CCC noreport", shape(callOutcomes(stops, moves, "BBB")))
    }

    @Test
    fun `counts add up into the site's bands`() {
        val c = PunctualityCounts()
        listOf(-2, 0, 1, 4, 5, 14, 15, 40).forEach { c.add(CallOutcome("X", "10:00", if (it < 0) "early" else if (it == 0) "ontime" else "late", it)) }
        c.add(CallOutcome("X", "10:00", "cancelled", null))
        c.add(CallOutcome("X", "10:00", "noreport", null))
        assertEquals(listOf(10, 1, 1, 2, 2, 2, 1, 1), listOf(c.total, c.early, c.onTime, c.lateMinor, c.lateModerate, c.lateSevere, c.cancelled, c.noReport))
        assertEquals(77L, c.delaySum)
    }
}

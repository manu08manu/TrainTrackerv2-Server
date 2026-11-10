package com.traintracker.server.routes

import com.traintracker.server.database.CANCELLED_ONLY_SQL
import com.traintracker.server.database.csvList
import com.traintracker.server.database.query
import com.traintracker.server.database.queryFirst
import com.traintracker.server.database.queryList
import com.traintracker.server.database.unitsCsv
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction

// Splits and joins inferred from the units allocated to trains, for when the CIF has no association.

/** True when the movements have a train ID for the uid, within a day of [date], that only has cancellations. */
fun JdbcTransaction.isUidWhollyCancelled(uid: String, date: String): Boolean = queryFirst(
    "SELECT train_id FROM movements WHERE uid=? AND date(event_ts) BETWEEN date(?, '-1 day') AND date(?, '+1 day') GROUP BY train_id HAVING $CANCELLED_ONLY_SQL LIMIT 1",
    listOf(uid, date, date)
) { true } != null

private fun JdbcTransaction.unitSet(uid: String, date: String): Set<String> = unitsCsv(uid, date).csvList().toSet()

/** (uid, headcode) of the train this one joins (CIF 'JJ'), preferring one that shares a unit. */
fun JdbcTransaction.lookupJoinsInto(uid: String, date: String): Pair<String?, String?> {
    val myUnits = unitSet(uid, date)
    val candidates = queryList("SELECT main_uid FROM cif_associations WHERE assoc_uid=? AND assoc_type='JJ'", listOf(uid)) { it.getString("main_uid") } +
        queryList("SELECT assoc_uid FROM cif_associations WHERE main_uid=? AND assoc_type='JJ'", listOf(uid)) { it.getString("assoc_uid") }
    val joinsUid = (if (myUnits.isNotEmpty()) candidates.firstOrNull { cand -> unitSet(cand, date).any { it in myUnits } } else null)
        ?: candidates.firstOrNull()
    val joinsHeadcode = joinsUid?.let {
        queryFirst("SELECT DISTINCT headcode FROM schedules WHERE uid=? LIMIT 1", listOf(it)) { rs -> rs.getString("headcode") }
    }
    return joinsUid to joinsHeadcode
}

/** Number of vehicles allocated, or null when none. */
fun JdbcTransaction.lookupCoachCount(uid: String, date: String): Int? =
    queryFirst("SELECT vehicles FROM allocation_consists WHERE uid=? AND service_date=? LIMIT 1", listOf(uid, date)) {
        it.getString("vehicles")
    }.csvList().size.takeIf { it > 0 }

/** Minutes from a to b ("HH:mm"), or -9999 when either is malformed. */
fun timeDiffMinutes(a: String, b: String): Int = try {
    val ap = a.split(":").map { it.toInt() }
    val bp = b.split(":").map { it.toInt() }
    (bp[0] * 60 + bp[1]) - (ap[0] * 60 + ap[1])
} catch (e: Exception) {
    -9999
}

/** The train this one split from: one carrying all of its units and more, which splits it off. */
fun JdbcTransaction.detectUnitBasedSplitParent(uid: String, date: String): SplitCandidate? {
    val myUnits = unitSet(uid, date)
    if (myUnits.isEmpty()) return null
    val candidates = queryList(
        "SELECT uid, units FROM allocation_consists WHERE service_date=? AND uid != ? AND (${myUnits.joinToString(" OR ") { "units LIKE ?" }})",
        listOf(date, uid) + myUnits.map { "%$it%" }
    ) { rs ->
        val cUid = rs.getString("uid") ?: return@queryList null
        val cUnits = rs.getString("units").csvList().toSet()
        cUid.takeIf { cUnits.isNotEmpty() && myUnits.all { it in cUnits } && myUnits.size < cUnits.size }
    }
    for (cUid in candidates) {
        val match = detectUnitBasedSplits(cUid, date).firstOrNull { it.uid == uid } ?: continue
        val parentHc = queryFirst("SELECT headcode FROM schedules WHERE uid=? LIMIT 1", listOf(cUid)) { it.getString("headcode") } ?: ""
        return SplitCandidate(cUid, parentHc, null, matchedTiploc = match.matchedTiploc)
    }
    return null
}

/**
 * Portions that split from this train: trains carrying a subset of its units that start at one of its
 * intermediate stops within 4 hours after it calls there. One per unit combination, the earliest.
 */
fun JdbcTransaction.detectUnitBasedSplits(uid: String, date: String): List<SplitCandidate> {
    val myUnits = unitSet(uid, date)
    if (myUnits.size < 2) return emptyList()
    data class Portion(val uid: String, val headcode: String, val unitsKey: String)
    val portions = queryList(
        "SELECT uid, headcode, units FROM allocation_consists WHERE service_date=? AND uid != ? AND (${myUnits.joinToString(" OR ") { "units LIKE ?" }})",
        listOf(date, uid) + myUnits.map { "%$it%" }
    ) { rs ->
        val cUid = rs.getString("uid") ?: return@queryList null
        val cHc = rs.getString("headcode") ?: return@queryList null
        val cUnits = rs.getString("units").csvList().toSet()
        if (cUnits.isNotEmpty() && cUnits.all { it in myUnits } && cUnits.size < myUnits.size) {
            Portion(cUid, cHc, cUnits.sorted().joinToString(","))
        } else {
            null
        }
    }
    if (portions.isEmpty()) return emptyList()
    val myStops = queryList("SELECT tiploc, scheduled_time, stop_type FROM schedules WHERE uid=? AND stop_type NOT IN ('LT', 'LO')", listOf(uid)) { rs ->
        (rs.getString("tiploc") ?: "") to (rs.getString("scheduled_time") ?: "")
    }
    data class Confirmed(val candidate: SplitCandidate, val unitsKey: String, val originTime: String)
    val confirmed = portions.mapNotNull { p ->
        val origin = queryFirst("SELECT tiploc, scheduled_time FROM schedules WHERE uid=? AND stop_type='LO' LIMIT 1", listOf(p.uid)) { rs ->
            rs.getString("tiploc") to rs.getString("scheduled_time")
        }
        val originTiploc = origin?.first ?: return@mapNotNull null
        val originTime = origin.second ?: return@mapNotNull null
        val matched = myStops.firstOrNull { (tiploc, time) ->
            tiploc == originTiploc && time.isNotEmpty() && timeDiffMinutes(time, originTime) in 0..240
        } ?: return@mapNotNull null
        Confirmed(SplitCandidate(p.uid, p.headcode, null, matchedTiploc = matched.first), p.unitsKey, originTime)
    }
    return confirmed
        .groupBy { it.unitsKey }
        .values
        .map { group -> group.minBy { it.originTime } }
        .sortedBy { it.originTime }
        .map { it.candidate }
}

/** Units (resource groups) whose allocation leg ends at [tiploc] without another starting there; null without leg data. */
fun JdbcTransaction.unitsDividingAt(uid: String, date: String, tiploc: String): Set<String>? {
    val ends = mutableSetOf<String>()
    val starts = mutableSetOf<String>()
    var rows = 0
    query("SELECT resource_group_id, origin_tiploc, dest_tiploc FROM allocation_legs WHERE uid=? AND service_date=?", listOf(uid, date)) { rs ->
        while (rs.next()) {
            rows++
            val group = rs.getString("resource_group_id") ?: continue
            if (rs.getString("dest_tiploc") == tiploc) ends.add(group)
            if (rs.getString("origin_tiploc") == tiploc) starts.add(group)
        }
    }
    return if (rows == 0) null else ends - starts
}

package com.traintracker.server.routes

import org.jetbrains.exposed.sql.IColumnType
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.VarCharColumnType

fun Transaction.isUidWhollyCancelled(uid: String, date: String): Boolean {
    val colType: IColumnType<*> = VarCharColumnType()
    fun bind(v: String): Pair<IColumnType<*>, Any?> = colType to (v as Any?)
    var found = false
    exec(
        "SELECT train_id FROM trust_movements WHERE uid=? AND date(event_ts) BETWEEN date(?, '-1 day') AND date(?, '+1 day') GROUP BY train_id HAVING SUM(CASE WHEN is_cancelled=0 THEN 1 ELSE 0 END)=0 AND SUM(CASE WHEN is_cancelled=1 THEN 1 ELSE 0 END)>0 LIMIT 1",
        listOf(bind(uid), bind(date), bind(date))
    ) { rs ->
        if (rs.next()) found = true
    }
    return found
}

fun Transaction.lookupJoinsInto(uid: String, date: String): Pair<String?, String?> {
    val colType: IColumnType<*> = VarCharColumnType()
    fun bind(v: String): Pair<IColumnType<*>, Any?> = colType to (v as Any?)
    fun unitsOf(u: String): Set<String> =
        lookupUnits(u, date)?.split(",")?.map { it.trim() }?.filter { it.isNotEmpty() }?.toSet() ?: emptySet()
    val myUnits = unitsOf(uid)
    val candidates = mutableListOf<String>()
    exec("SELECT main_uid FROM cif_associations WHERE assoc_uid=? AND assoc_type='JJ'", listOf(bind(uid))) { rs ->
        while (rs.next()) candidates.add(rs.getString("main_uid"))
    }
    exec("SELECT assoc_uid FROM cif_associations WHERE main_uid=? AND assoc_type='JJ'", listOf(bind(uid))) { rs ->
        while (rs.next()) candidates.add(rs.getString("assoc_uid"))
    }
    var joinsUid: String? = null
    if (myUnits.isNotEmpty()) {
        joinsUid = candidates.firstOrNull { cand -> unitsOf(cand).any { it in myUnits } }
    }
    if (joinsUid == null) joinsUid = candidates.firstOrNull()
    var joinsHeadcode: String? = null
    if (joinsUid != null) {
        exec("SELECT DISTINCT headcode FROM schedules WHERE uid=? LIMIT 1", listOf(bind(joinsUid))) { rs ->
            if (rs.next()) joinsHeadcode = rs.getString("headcode")
        }
    }
    return joinsUid to joinsHeadcode
}

fun Transaction.lookupUnits(uid: String, date: String): String? {
    val colType: IColumnType<*> = VarCharColumnType()
    fun bind(v: String): Pair<IColumnType<*>, Any?> = colType to (v as Any?)
    var units: String? = null
    exec("SELECT units FROM allocation_consists WHERE uid=? AND service_date=? LIMIT 1", listOf(bind(uid), bind(date))) { rs ->
        if (rs.next()) units = rs.getString("units")
    }
    return units?.takeIf { it.isNotBlank() }
}

fun Transaction.lookupCoachCount(uid: String, date: String): Int? {
    val colType: IColumnType<*> = VarCharColumnType()
    fun bind(v: String): Pair<IColumnType<*>, Any?> = colType to (v as Any?)
    var vehicles: String? = null
    exec("SELECT vehicles FROM allocation_consists WHERE uid=? AND service_date=? LIMIT 1", listOf(bind(uid), bind(date))) { rs ->
        if (rs.next()) vehicles = rs.getString("vehicles")
    }
    return vehicles?.split(",")?.map { it.trim() }?.filter { it.isNotEmpty() }?.size?.takeIf { it > 0 }
}

fun timeDiffMinutes(a: String, b: String): Int {
    return try {
        val ap = a.split(":").map { it.toInt() }
        val bp = b.split(":").map { it.toInt() }
        (bp[0] * 60 + bp[1]) - (ap[0] * 60 + ap[1])
    } catch (e: Exception) {
        -9999
    }
}

fun Transaction.detectUnitBasedSplitParent(uid: String, date: String): SplitCandidate? {
    val myUnits = lookupUnits(uid, date)?.split(",")?.map { it.trim() }?.filter { it.isNotEmpty() }?.toSet() ?: return null
    if (myUnits.isEmpty()) return null
    val colType: IColumnType<*> = VarCharColumnType()
    fun bind(v: String): Pair<IColumnType<*>, Any?> = colType to (v as Any?)
    val candidates = mutableListOf<String>()
    val likeClauses = myUnits.joinToString(" OR ") { "units LIKE ?" }
    val likeArgs = myUnits.map { bind("%$it%") }
    exec(
        "SELECT uid, units FROM allocation_consists WHERE service_date=? AND uid != ? AND ($likeClauses)",
        listOf(bind(date), bind(uid)) + likeArgs
    ) { rs ->
        while (rs.next()) {
            val cUid = rs.getString("uid") ?: continue
            val cUnits = (rs.getString("units") ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() }.toSet()
            if (cUnits.isNotEmpty() && myUnits.all { it in cUnits } && myUnits.size < cUnits.size) candidates.add(cUid)
        }
    }
    for (cUid in candidates) {
        val children = detectUnitBasedSplits(cUid, date)
        val match = children.firstOrNull { it.uid == uid }
        if (match != null) {
            var pHc = ""
            exec("SELECT headcode FROM schedules WHERE uid=? LIMIT 1", listOf(bind(cUid))) { rs ->
                if (rs.next()) pHc = rs.getString("headcode") ?: ""
            }
            return SplitCandidate(cUid, pHc, null, matchedTiploc = match.matchedTiploc)
        }
    }
    return null
}

fun Transaction.detectUnitBasedSplits(uid: String, date: String): List<SplitCandidate> {
    val myUnits = lookupUnits(uid, date)?.split(",")?.map { it.trim() }?.filter { it.isNotEmpty() }?.toSet() ?: return emptyList()
    if (myUnits.size < 2) return emptyList()
    val colType: IColumnType<*> = VarCharColumnType()
    fun bind(v: String): Pair<IColumnType<*>, Any?> = colType to (v as Any?)
    val candidates = mutableListOf<Triple<String, String, String>>()
    val likeClauses = myUnits.joinToString(" OR ") { "units LIKE ?" }
    val likeArgs = myUnits.map { bind("%$it%") }
    exec(
        "SELECT uid, headcode, units FROM allocation_consists WHERE service_date=? AND uid != ? AND ($likeClauses)",
        listOf(bind(date), bind(uid)) + likeArgs
    ) { rs ->
        while (rs.next()) {
            val cUid = rs.getString("uid") ?: continue
            val cHc = rs.getString("headcode") ?: continue
            val cUnits = (rs.getString("units") ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() }.toSet()
            if (cUnits.isNotEmpty() && cUnits.all { it in myUnits } && cUnits.size < myUnits.size) {
                candidates.add(Triple(cUid, cHc, cUnits.sorted().joinToString(",")))
            }
        }
    }
    if (candidates.isEmpty()) return emptyList()
    val myStops = mutableListOf<Triple<String, String, String>>()
    exec("SELECT tiploc, scheduled_time, stop_type FROM schedules WHERE uid=? AND stop_type NOT IN ('LT', 'LO')", listOf(bind(uid))) { rs ->
        while (rs.next()) {
            myStops.add(Triple(rs.getString("tiploc") ?: "", rs.getString("scheduled_time") ?: "", rs.getString("stop_type") ?: ""))
        }
    }
    data class ConfirmedMatch(val candidate: SplitCandidate, val unitsSig: String, val originTime: String)
    val allConfirmed = mutableListOf<ConfirmedMatch>()
    for ((cUid, cHc, cUnitsSig) in candidates) {
        var cOriginTiploc: String? = null
        var cOriginTime: String? = null
        exec("SELECT tiploc, scheduled_time FROM schedules WHERE uid=? AND stop_type='LO' LIMIT 1", listOf(bind(cUid))) { rs ->
            if (rs.next()) {
                cOriginTiploc = rs.getString("tiploc")
                cOriginTime = rs.getString("scheduled_time")
            }
        }
        val ot = cOriginTiploc
        val otm = cOriginTime
        if (ot == null || otm == null) continue
        val matched = myStops.firstOrNull { (t, time, _) ->
            t == ot && time.isNotEmpty() && timeDiffMinutes(time, otm) in 0..240
        }
        if (matched != null) {
            allConfirmed.add(ConfirmedMatch(SplitCandidate(cUid, cHc, null, matchedTiploc = matched.first), cUnitsSig, otm))
        }
    }
    return allConfirmed
        .groupBy { it.unitsSig }
        .values
        .map { group -> group.minByOrNull { it.originTime }!! }
        .sortedBy { it.originTime }
        .map { it.candidate }
}

fun Transaction.unitsDividingAt(uid: String, date: String, tiploc: String): Set<String>? {
    val colType: IColumnType<*> = VarCharColumnType()
    fun bind(v: String): Pair<IColumnType<*>, Any?> = colType to (v as Any?)
    val ends = mutableSetOf<String>()
    val starts = mutableSetOf<String>()
    var rows = 0
    exec(
        "SELECT resource_group_id, origin_tiploc, dest_tiploc FROM allocation_legs WHERE uid=? AND service_date=?",
        listOf(bind(uid), bind(date))
    ) { rs ->
        while (rs.next()) {
            rows++
            val g = rs.getString("resource_group_id") ?: continue
            if (rs.getString("dest_tiploc") == tiploc) ends.add(g)
            if (rs.getString("origin_tiploc") == tiploc) starts.add(g)
        }
    }
    return if (rows == 0) null else ends - starts
}

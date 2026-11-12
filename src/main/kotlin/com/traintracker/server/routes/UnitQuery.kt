package com.traintracker.server.routes

import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.Schedules
import com.traintracker.server.database.csvList
import com.traintracker.server.database.effectiveDateFor
import com.traintracker.server.database.placeholders
import com.traintracker.server.database.query
import com.traintracker.server.database.scheduleDateScope
import java.time.LocalDate
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction

/** A train the unit is allocated to, with its first schedule row. */
private class UnitWorking(val headcode: String, val loRow: ResultRow, val units: List<String>, val vehicles: List<String>)

/** Another train sharing a unit with one of the searched unit's multi-unit workings. */
private class PartnerCandidate(
    val parentTiplocs: Set<String>,
    val parentOriginTiploc: String,
    val parentDestTiploc: String,
    val headcode: String,
    val units: List<String>,
    val vehicles: List<String>,
    val uid: String?,
)

/** The origin row (or else first row) of the strongest STP variant among non-cancelled rows. */
private fun firstRowOfBestVariant(rows: List<ResultRow>): ResultRow? {
    val best = rows.minOfOrNull { stpRank(it) }
    val bestRows = rows.filter { stpRank(it) == best }
    return bestRows.firstOrNull { it[Schedules.stopType] == "LO" } ?: bestRows.firstOrNull()
}

/** Where the unit joins this train: the row's own TIPLOC when it is not the origin. */
private fun joinTiploc(row: ResultRow): String = if (row[Schedules.stopType] == "LI") row[Schedules.tiploc] else row[Schedules.originTiploc]

/** uid -> every TIPLOC of its non-cancelled schedules. */
private fun JdbcTransaction.tiplocsByUid(uids: Set<String>): Map<String, Set<String>> {
    if (uids.isEmpty()) return emptyMap()
    val result = mutableMapOf<String, MutableSet<String>>()
    try {
        query("SELECT uid, tiploc FROM schedules WHERE uid IN (${placeholders(uids.size)}) AND stp_indicator != 'C'", uids.toList()) { rs ->
            while (rs.next()) {
                val uid = rs.getString("uid") ?: continue
                val tiploc = rs.getString("tiploc") ?: continue
                result.getOrPut(uid) { mutableSetOf() }.add(tiploc)
            }
        }
    } catch (_: Exception) {
    }
    return result
}

/**
 * The services a unit works on [date], plus the portions that split from them (a train it runs coupled to that
 * starts part way along), each annotated with where it splits and which service the unit forms next.
 */
internal fun queryByUnit(unit: String, date: String = LocalDate.now().toString()): List<ServiceResponse> = transaction {
    val workings = unitWorkings(unit, date)
    val seenUids = workings.map { it.loRow[Schedules.uid] }.toMutableSet()
    val workingTiplocs = tiplocsByUid(seenUids.toSet())

    // Other trains carrying a partner unit of a multi-unit working.
    val candidates = mutableListOf<PartnerCandidate>()
    for (w in workings.filter { it.units.size > 1 && unit in it.units }) {
        val parentTiplocs = workingTiplocs[w.loRow[Schedules.uid]] ?: continue
        for (partner in w.units.filter { it != unit }) {
            try {
                val excluded = seenUids.map { it.take(6) }.toSet()
                val notIn = if (excluded.isEmpty()) "''" else placeholders(excluded.size)
                query(
                    "SELECT ac.headcode, ac.units, ac.vehicles, ac.operator, ac.core_id FROM allocation_consists ac WHERE (',' || ac.units || ',') LIKE '%,' || ? || ',%' AND ac.service_date = ? AND ac.core_id NOT IN ($notIn)",
                    listOf(partner, date) + excluded
                ) { rs ->
                    while (rs.next()) {
                        val hc = rs.getString("headcode") ?: continue
                        val coreId = rs.getString("core_id") ?: continue
                        val uidLength = coreId.length - hc.length - 2
                        candidates.add(
                            PartnerCandidate(
                                parentTiplocs, w.loRow[Schedules.originTiploc], w.loRow[Schedules.destTiploc], hc,
                                rs.getString("units").csvList(), rs.getString("vehicles").csvList(),
                                if (uidLength in 4..8) coreId.substring(hc.length, hc.length + uidLength) else null
                            )
                        )
                    }
                }
            } catch (_: Exception) {
            }
        }
    }
    // Keep candidates whose uid really is a schedule of their headcode.
    val headcodeUids = mutableMapOf<String, MutableSet<String>>()
    val candidateHeadcodes = candidates.map { it.headcode }.toSet()
    if (candidateHeadcodes.isNotEmpty()) {
        try {
            query("SELECT DISTINCT uid, headcode FROM schedules WHERE headcode IN (${placeholders(candidateHeadcodes.size)})", candidateHeadcodes.toList()) { rs ->
                while (rs.next()) {
                    val u = rs.getString("uid") ?: continue
                    headcodeUids.getOrPut(rs.getString("headcode") ?: continue) { mutableSetOf() }.add(u)
                }
            }
        } catch (_: Exception) {
        }
    }
    val validCandidates = candidates.filter { c -> c.uid != null && headcodeUids[c.headcode]?.contains(c.uid) == true }
    val partnerUids = validCandidates.mapNotNull { it.uid }.toSet()
    val partnerOrigins = mutableMapOf<String, String>()
    if (partnerUids.isNotEmpty()) {
        try {
            query("SELECT uid, origin_tiploc FROM schedules WHERE uid IN (${placeholders(partnerUids.size)}) AND stop_type = 'LO'", partnerUids.toList()) { rs ->
                while (rs.next()) partnerOrigins.putIfAbsent(rs.getString("uid") ?: continue, rs.getString("origin_tiploc") ?: continue)
            }
        } catch (_: Exception) {
        }
    }
    val partnerSchedules = if (partnerUids.isNotEmpty()) {
        Schedules.selectAll().where { Schedules.uid inList partnerUids }
            .orderBy(Schedules.stopSequence, SortOrder.ASC).toList()
            .filter { it[Schedules.stpIndicator] != 'C' }
            .groupBy { it[Schedules.uid] }
    } else {
        emptyMap()
    }

    val allUids = (workings.map { it.loRow[Schedules.uid] } + partnerUids).toSet()
    val darwinDb = AppDatabase.darwinPlatforms(allUids, date)
    val movementPlatforms = AppDatabase.movementPlatforms(allUids, date)
    fun service(loRow: ResultRow, headcode: String, units: List<String>, vehicles: List<String>): ServiceResponse {
        val uid = loRow[Schedules.uid]
        val (platform, confirmed) = resolvePlatform(
            uid, loRow[Schedules.tiploc], loRow[Schedules.crs], loRow[Schedules.platform], darwinDb[uid to loRow[Schedules.tiploc]], movementPlatforms
        )
        return ServiceResponse(
            uid = uid,
            headcode = headcode,
            atocCode = CorpusLookup.operatorForTsc(loRow[Schedules.tsc]),
            scheduledTime = loRow[Schedules.scheduledTime],
            scheduledArrival = loRow[Schedules.scheduledArrival],
            scheduledDeparture = loRow[Schedules.scheduledDeparture],
            platform = platform,
            platformConfirmed = confirmed,
            isPass = loRow[Schedules.isPass],
            stopType = loRow[Schedules.stopType],
            originTiploc = loRow[Schedules.originTiploc],
            destTiploc = loRow[Schedules.destTiploc],
            originCrs = loRow[Schedules.originCrs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(loRow[Schedules.originTiploc]),
            destCrs = loRow[Schedules.destCrs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(loRow[Schedules.destTiploc]),
            units = units,
            vehicles = vehicles,
            unitJoinTiploc = joinTiploc(loRow),
        )
    }
    val rows = workings.map { service(it.loRow, it.headcode, it.units, it.vehicles) }

    // A partner that starts at a stop of the parent (not its origin or terminus) is a portion splitting off.
    val portions = mutableListOf<ServiceResponse>()
    for (c in validCandidates) {
        val uid = c.uid ?: continue
        if (uid in seenUids) continue
        val origin = partnerOrigins[uid] ?: ""
        if (origin.isEmpty() || origin !in c.parentTiplocs || origin == c.parentOriginTiploc) continue
        if (c.parentDestTiploc == origin) continue
        val loRow = firstRowOfBestVariant(partnerSchedules[uid] ?: continue) ?: continue
        seenUids.add(uid)
        portions.add(service(loRow, c.headcode, c.units, c.vehicles))
    }
    // Drop a portion when one of its units is in a working that starts later.
    val workingTimesByUnit = rows.flatMap { svc -> svc.units.map { it to svc.scheduledTime } }.groupBy({ it.first }, { it.second })
    val keptPortions = portions.filter { p ->
        p.units.all { u -> workingTimesByUnit[u]?.none { it > p.scheduledTime } ?: true }
    }

    val services = (rows + keptPortions).sortedBy { it.scheduledTime }.withResolvedNames()
    val tiplocs = tiplocsByUid(services.map { it.uid }.toSet())
    services.map { svc -> annotateUnitDay(svc, services, tiplocs[svc.uid] ?: emptySet(), unit) }
        .filter { it.units.isEmpty() || unit in it.units }
}

/** The unit's allocations on the date, each with the first row of its timetable. */
private fun JdbcTransaction.unitWorkings(unit: String, date: String): List<UnitWorking> {
    val workings = mutableListOf<UnitWorking>()
    try {
        query(
            "SELECT ac.headcode, ac.units, ac.vehicles, ac.operator, ac.core_id, ac.uid FROM allocation_consists ac WHERE (',' || ac.units || ',') LIKE '%,' || ? || ',%' AND ac.service_date = ? AND SUBSTR(ac.units, 1, 3) = SUBSTR(?, 1, 3)",
            listOf(unit, date, unit)
        ) { rs ->
            while (rs.next()) {
                val hc = rs.getString("headcode") ?: continue
                val coreId = rs.getString("core_id") ?: continue
                val uid = rs.getString("uid")?.trim()?.takeIf { it.isNotEmpty() }
                    ?: if (coreId.length > hc.length + 2) coreId.substring(hc.length, coreId.length - 2).trim().takeIf { it.isNotEmpty() } else null
                val schedule = Schedules.selectAll().where {
                    (if (uid != null) Schedules.uid eq uid else Schedules.headcode eq hc.filter { it.isLetterOrDigit() }) and
                        scheduleDateScope(effectiveDateFor(date))
                }.orderBy(Schedules.stopSequence, SortOrder.ASC).toList().filter { it[Schedules.stpIndicator] != 'C' }
                val loRow = firstRowOfBestVariant(schedule) ?: continue
                workings.add(UnitWorking(hc, loRow, rs.getString("units").csvList(), rs.getString("vehicles").csvList()))
            }
        }
    } catch (_: Exception) {
    }
    return workings
}

/** Marks where a multi-unit service splits off a smaller portion, and the next service the unit forms. */
private fun annotateUnitDay(svc: ServiceResponse, day: List<ServiceResponse>, svcTiplocs: Set<String>, unit: String): ServiceResponse {
    var result = svc
    val portion = day.filter { other ->
        other.uid != svc.uid &&
            other.originTiploc != svc.originTiploc &&
            other.originTiploc != svc.destTiploc &&
            svcTiplocs.contains(other.originTiploc) &&
            other.units.isNotEmpty() &&
            svc.units.size > 1 &&
            other.units.all { svc.units.contains(it) } &&
            other.units.size < svc.units.size &&
            svc.units.contains(unit) &&
            other.scheduledTime >= svc.scheduledTime &&
            other.scheduledTime.toMinutes() - svc.scheduledTime.toMinutes() <= 90 &&
            (other.units.contains(unit) || svc.units.filter { it != unit }.any { other.units.contains(it) })
    }.minByOrNull { it.scheduledTime }
    if (portion != null) {
        result = result.copy(
            association = result.association.copy(
                splitTiploc = portion.originTiploc,
                splitTiplocName = portion.originName,
                splitToUid = portion.uid,
                splitToHeadcode = portion.headcode
            )
        )
    }
    val next = if (unit in svc.units) {
        day.filter { it.uid != svc.uid && it.scheduledTime > svc.scheduledTime && unit in it.units }.minByOrNull { it.scheduledTime }
    } else {
        null
    }
    if (next != null) {
        result = result.copy(association = result.association.copy(formsUid = next.uid, formsHeadcode = next.headcode))
    }
    return result
}

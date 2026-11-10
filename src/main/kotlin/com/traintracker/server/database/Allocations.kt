package com.traintracker.server.database

import com.traintracker.server.kafka.ridToUid
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.insertIgnore
import org.jetbrains.exposed.v1.jdbc.replace
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

/** The units string of a uid's current consist on a date, or null when there is none. */
fun JdbcTransaction.unitsCsv(uid: String, date: String): String? =
    queryFirst("SELECT units FROM allocation_consists WHERE uid=? AND service_date=? LIMIT 1", listOf(uid, date)) {
        it.getString("units")
    }?.takeIf { it.isNotBlank() }

/** Unit allocations from the allocation feed: current consist, daily history, change log and per-leg detail. */
object Allocations {
    private val log = LoggerFactory.getLogger("Database")
    private val writeLock = ReentrantLock()

    data class Allocation(
        val coreId: String,
        val headcode: String,
        val serviceDate: String,
        val operator: String,
        val vehicles: List<String>,
        val units: List<String>,
        val unitCount: Int,
        val updatedAt: String = "",
    )

    data class Change(
        val changedAt: String,
        val headcode: String,
        val oldUnits: List<String>,
        val newUnits: List<String>,
        val oldCoaches: Int,
        val newCoaches: Int,
    )

    /** One resource group on one leg, as parsed from the feed. */
    data class Leg(
        val allocationSeq: Int,
        val resourceGroupId: String,
        val vehicles: List<String>,
        val originStanox: String,
        val originTiploc: String?,
        val originTime: String?,
        val destStanox: String,
        val destTiploc: String?,
        val destTime: String?,
    )

    data class LegSpan(val originTiploc: String?, val destTiploc: String?, val vehicleCount: Int)

    /** Units running together between two points. */
    data class LegGroup(
        val fromTiploc: String?,
        val toTiploc: String?,
        val fromTime: String?,
        val toTime: String?,
        val units: List<String>,
        val coachCount: Int,
        val vehicles: List<String> = emptyList(),
    )

    data class LegData(val spans: List<LegSpan>, val groups: List<LegGroup>)

    /** core_id is headcode + optional space + uid + 2 character suffix. */
    private fun uidFromCoreId(coreId: String, headcode: String): String? {
        val rest = coreId.drop(headcode.length).removePrefix(" ")
        return if (rest.length > 2) rest.dropLast(2) else null
    }

    // ---- Writes --------------------------------------------------------------------------------------

    /** Stores the current consist, logs a change when units or vehicles differ, and keeps the first report of the day. */
    fun upsert(
        coreId: String,
        headcode: String,
        serviceDate: String,
        operator: String,
        vehicles: List<String>,
        unitCount: Int,
        units: List<String> = emptyList(),
    ) {
        writeLock.withLock { upsertLocked(coreId, headcode, serviceDate, operator, vehicles, unitCount, units) }
    }

    private fun upsertLocked(
        coreId: String,
        headcode: String,
        serviceDate: String,
        operator: String,
        vehicles: List<String>,
        unitCount: Int,
        units: List<String>,
    ) {
        if (coreId.isEmpty()) return
        val uid = uidFromCoreId(coreId, headcode)
        val vehiclesCsv = vehicles.joinToString(",")
        val unitsCsv = units.joinToString(",")
        transaction {
            val old = AllocationConsists.selectAll().where { AllocationConsists.coreId eq coreId }.singleOrNull()
            val oldVehicles = old?.get(AllocationConsists.vehicles) ?: ""
            val oldUnits = if (oldVehicles.isEmpty()) "" else old?.get(AllocationConsists.units) ?: ""
            val newUnits = if (vehicles.isEmpty()) "" else unitsCsv
            if (oldVehicles != vehiclesCsv || oldUnits != newUnits) {
                AllocationChanges.insert {
                    it[AllocationChanges.coreId] = coreId
                    it[AllocationChanges.headcode] = headcode
                    it[AllocationChanges.serviceDate] = serviceDate
                    it[AllocationChanges.uid] = uid
                    it[AllocationChanges.changedAt] = LocalDateTime.now(ZoneOffset.UTC)
                    it[AllocationChanges.oldUnits] = oldUnits
                    it[AllocationChanges.newUnits] = newUnits
                    it[AllocationChanges.oldCoaches] = if (oldVehicles.isEmpty()) 0 else oldVehicles.split(",").size
                    it[AllocationChanges.newCoaches] = vehicles.size
                }
            }
            AllocationConsists.replace {
                it[AllocationConsists.coreId] = coreId
                it[AllocationConsists.headcode] = headcode
                it[AllocationConsists.serviceDate] = serviceDate
                it[AllocationConsists.operator] = operator
                it[AllocationConsists.vehicles] = vehiclesCsv
                it[AllocationConsists.units] = unitsCsv
                it[AllocationConsists.unitCount] = unitCount
                it[AllocationConsists.updatedAt] = LocalDateTime.now(ZoneOffset.UTC)
                it[AllocationConsists.uid] = uid
            }
            AllocationHistory.insertIgnore {
                it[AllocationHistory.coreId] = coreId
                it[AllocationHistory.headcode] = headcode
                it[AllocationHistory.serviceDate] = serviceDate
                it[AllocationHistory.operator] = operator
                it[AllocationHistory.vehicles] = vehiclesCsv
                it[AllocationHistory.units] = unitsCsv
                it[AllocationHistory.unitCount] = unitCount
                it[AllocationHistory.recordedAt] = LocalDateTime.now()
                it[AllocationHistory.uid] = uid
            }
        }
    }

    fun replaceLegs(coreId: String, headcode: String, serviceDate: String, legs: List<Leg>) {
        if (coreId.isEmpty()) return
        val uid = uidFromCoreId(coreId, headcode)
        transaction {
            AllocationLegs.deleteWhere { AllocationLegs.coreId eq coreId }
            for (leg in legs) {
                AllocationLegs.insert {
                    it[AllocationLegs.coreId] = coreId
                    it[AllocationLegs.allocationSeq] = leg.allocationSeq
                    it[AllocationLegs.headcode] = headcode
                    it[AllocationLegs.serviceDate] = serviceDate
                    it[AllocationLegs.uid] = uid
                    it[AllocationLegs.resourceGroupId] = leg.resourceGroupId
                    it[AllocationLegs.vehicles] = leg.vehicles.joinToString(",")
                    it[AllocationLegs.vehicleCount] = leg.vehicles.size
                    it[AllocationLegs.originStanox] = leg.originStanox
                    it[AllocationLegs.originTiploc] = leg.originTiploc
                    it[AllocationLegs.originTime] = leg.originTime
                    it[AllocationLegs.destStanox] = leg.destStanox
                    it[AllocationLegs.destTiploc] = leg.destTiploc
                    it[AllocationLegs.destTime] = leg.destTime
                    it[AllocationLegs.updatedAt] = LocalDateTime.now()
                }
            }
        }
    }

    // ---- Reads ---------------------------------------------------------------------------------------

    private fun allocation(
        coreId: String,
        headcode: String,
        serviceDate: String,
        operator: String,
        vehiclesCsv: String?,
        unitsCsv: String?,
        declaredCount: Int,
        updatedAt: String,
    ): Allocation {
        val units = splitCsv(unitsCsv)
        val vehicles = splitCsv(vehiclesCsv)
        // An even formation of 4+ units that are all the same class is treated as reported twice, and halved.
        val doubled = declaredCount >= 4 && declaredCount % 2 == 0 && units.size == declaredCount &&
            units.all { it.take(3) == units.first().take(3) }
        return if (doubled) {
            val half = declaredCount / 2
            Allocation(coreId, headcode, serviceDate, operator, vehicles.take(vehicles.size / 2), units.take(half), half, updatedAt)
        } else {
            Allocation(coreId, headcode, serviceDate, operator, vehicles, units, declaredCount, updatedAt)
        }
    }

    private fun splitCsv(csv: String?): List<String> = csv.orEmpty().split(",").filter { it.isNotEmpty() }

    private fun ResultRow.toConsist() = allocation(
        this[AllocationConsists.coreId], this[AllocationConsists.headcode], this[AllocationConsists.serviceDate],
        this[AllocationConsists.operator], this[AllocationConsists.vehicles], this[AllocationConsists.units],
        this[AllocationConsists.unitCount], this[AllocationConsists.updatedAt].toString()
    )

    private fun ResultRow.toHistory() = allocation(
        this[AllocationHistory.coreId], this[AllocationHistory.headcode], this[AllocationHistory.serviceDate],
        this[AllocationHistory.operator], this[AllocationHistory.vehicles], this[AllocationHistory.units],
        this[AllocationHistory.unitCount], this[AllocationHistory.recordedAt].toString()
    )

    /** Reads the 8 columns core_id, headcode, service_date, operator, vehicles, units, unit_count, <timestamp>. */
    private fun allocationFromColumns(rs: java.sql.ResultSet, tsColumn: Int) = allocation(
        rs.getString(1) ?: "", rs.getString(2) ?: "", rs.getString(3) ?: "", rs.getString(4) ?: "",
        rs.getString(5), rs.getString(6), rs.getInt(7), (rs.getString(tsColumn) ?: "").replace(' ', 'T')
    )

    private fun safeDate(date: String?): String = date?.filter { it.isDigit() || it == '-' } ?: LocalDate.now().toString()

    /**
     * Allocation of a uid on a date. The headcode comes from the timetable; without a schedule, a single distinct
     * headcode among the allocations is used, and an ambiguous uid returns null rather than a guess.
     */
    fun byUid(uid: String, date: String? = null): Allocation? = transaction {
        val safeUid = uid.filter { it.isLetterOrDigit() }
        val day = safeDate(date)
        var hc = queryFirst(
            "SELECT headcode FROM schedules WHERE uid = ? AND $SCHEDULE_DATE_SCOPE_SQL LIMIT 1",
            listOf(safeUid) + scheduleDateScopeArgs(effectiveDateFor(day))
        ) { (it.getString(1) ?: "").filter { c -> c.isLetterOrDigit() } } ?: ""
        if (hc.isEmpty()) {
            val headcodes = queryList(
                "SELECT DISTINCT headcode FROM allocation_consists WHERE uid = ? AND service_date = ? UNION SELECT DISTINCT headcode FROM allocation_history WHERE uid = ? AND service_date = ?",
                listOf(safeUid, day, safeUid, day)
            ) { it.getString(1) }.toSet()
            when (headcodes.size) {
                0 -> return@transaction null
                1 -> hc = headcodes.first()
                else -> {
                    log.warn(
                        "getAllocationByUid: ambiguous uid=$safeUid date=$day matches ${headcodes.size} headcodes ($headcodes) with no schedule to anchor on — returning null rather than guessing"
                    )
                    return@transaction null
                }
            }
        }
        queryFirst(
            "SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count, updated_at FROM allocation_consists WHERE uid = ? AND headcode = ? AND service_date = ? UNION ALL SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count, recorded_at FROM allocation_history WHERE uid = ? AND headcode = ? AND service_date = ? AND NOT EXISTS (SELECT 1 FROM allocation_consists WHERE uid = ? AND headcode = ? AND service_date = ?) ORDER BY service_date DESC LIMIT 1",
            List(3) { listOf(safeUid, hc, day) }.flatten()
        ) { allocationFromColumns(it, 8) }
    }

    /** Allocations for many (uid -> headcode) pairs on one date: current consists first, then history. */
    fun byUids(uidHeadcodes: Map<String, String>, date: String? = null): Map<String, Allocation> {
        if (uidHeadcodes.isEmpty()) return emptyMap()
        val day = safeDate(date)
        val wanted = uidHeadcodes.entries.associate { (uid, hc) ->
            uid.filter { it.isLetterOrDigit() } to hc.filter { it.isLetterOrDigit() }
        }.filterKeys { it.isNotEmpty() }
        if (wanted.isEmpty()) return emptyMap()
        val results = mutableMapOf<String, Allocation>()
        transaction {
            fun fetch(table: String, tsColumn: String, suffix: String, pairs: Map<String, String>) {
                pairs.entries.chunked(200).forEach { chunk ->
                    query(
                        "SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count, $tsColumn, uid FROM $table WHERE (${chunk.joinToString(" OR ") { "(uid = ? AND headcode = ?)" }}) AND service_date = ?$suffix",
                        chunk.flatMap { listOf(it.key, it.value) } + day
                    ) { rs ->
                        while (rs.next()) {
                            val uid = rs.getString(9) ?: continue
                            if (uid !in pairs || uid in results) continue
                            results[uid] = allocationFromColumns(rs, 8)
                        }
                    }
                }
            }
            fetch("allocation_consists", "updated_at", "", wanted)
            val missing = wanted.filterKeys { it !in results }
            if (missing.isNotEmpty()) fetch("allocation_history", "recorded_at", " ORDER BY service_date DESC", missing)
        }
        return results
    }

    fun byCoreId(coreId: String): Allocation? = transaction {
        AllocationConsists.selectAll().where { AllocationConsists.coreId eq coreId }.singleOrNull()?.toConsist()
    }

    /** Current consists for a headcode (newest first), falling back to history. */
    fun byHeadcode(headcode: String, serviceDate: String? = null): List<Allocation> = transaction {
        val current = AllocationConsists.selectAll()
            .where {
                if (serviceDate != null) {
                    (AllocationConsists.headcode eq headcode) and (AllocationConsists.serviceDate eq serviceDate)
                } else {
                    AllocationConsists.headcode eq headcode
                }
            }
            .orderBy(AllocationConsists.updatedAt, SortOrder.DESC)
            .map { it.toConsist() }
        if (current.isNotEmpty()) return@transaction current
        AllocationHistory.selectAll()
            .where {
                if (serviceDate != null) {
                    (AllocationHistory.headcode eq headcode) and (AllocationHistory.serviceDate eq serviceDate)
                } else {
                    AllocationHistory.headcode eq headcode
                }
            }
            .orderBy(AllocationHistory.recordedAt, SortOrder.DESC)
            .map { it.toHistory() }
    }

    /** Allocation for a Darwin RID: by uid when the RID is known, else by core_id suffix in the history. */
    fun byRid(rid: String): Allocation? {
        if (rid.length < 9) return null
        val date = "${rid.substring(0, 4)}-${rid.substring(4, 6)}-${rid.substring(6, 8)}"
        val uid = ridToUid[rid] ?: AppDatabase.darwinUidForRid(rid)
        if (uid != null) byUid(uid, date)?.let { return it }
        return transaction {
            queryFirst(
                "SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count FROM allocation_history WHERE (core_id LIKE '%' || ? || '%' OR core_id LIKE '%' || ? || '%') AND service_date = ? LIMIT 1",
                listOf(rid.takeLast(5), rid.takeLast(7), date)
            ) { rs ->
                allocation(rs.getString(1) ?: "", "", date, "", rs.getString("vehicles"), rs.getString("units"), rs.getInt("unit_count"), "")
            }
        }
    }

    fun changes(uid: String, serviceDate: String): List<Change> = transaction {
        AllocationChanges.selectAll()
            .where { (AllocationChanges.uid eq uid) and (AllocationChanges.serviceDate eq serviceDate) }
            .orderBy(AllocationChanges.changedAt, SortOrder.ASC)
            .map { row ->
                Change(
                    "${row[AllocationChanges.changedAt]}Z",
                    row[AllocationChanges.headcode],
                    splitCsv(row[AllocationChanges.oldUnits]),
                    splitCsv(row[AllocationChanges.newUnits]),
                    row[AllocationChanges.oldCoaches],
                    row[AllocationChanges.newCoaches]
                )
            }
    }

    /** Leg spans (for train length per stop) and unit groups merged along the route. */
    fun legData(uid: String, date: String): LegData = transaction {
        val rows = AllocationLegs.selectAll().where {
            (AllocationLegs.uid eq uid) and (AllocationLegs.serviceDate eq date)
        }.toList()
        val spans = rows.map {
            LegSpan(it[AllocationLegs.originTiploc], it[AllocationLegs.destTiploc], it[AllocationLegs.vehicleCount])
        }
        val groups = rows.groupBy {
            listOf(it[AllocationLegs.originTiploc], it[AllocationLegs.originTime], it[AllocationLegs.destTiploc], it[AllocationLegs.destTime])
        }.map { (_, g) ->
            val f = g.first()
            LegGroup(
                f[AllocationLegs.originTiploc],
                f[AllocationLegs.destTiploc],
                f[AllocationLegs.originTime],
                f[AllocationLegs.destTime],
                g.map { it[AllocationLegs.resourceGroupId] }.distinct().sorted(),
                g.sumOf { it[AllocationLegs.vehicleCount] },
                g.sortedBy { it[AllocationLegs.resourceGroupId] }.flatMap { it[AllocationLegs.vehicles].csvList() }
            )
        }.sortedBy { it.fromTime ?: "" }
        // Join consecutive groups of the same units into one span.
        val merged = mutableListOf<LegGroup>()
        for (g in groups) {
            val last = merged.lastOrNull()
            if (last != null && last.units == g.units && last.toTiploc == g.fromTiploc) {
                merged[merged.lastIndex] = last.copy(toTiploc = g.toTiploc, toTime = g.toTime)
            } else {
                merged.add(g)
            }
        }
        LegData(spans, merged)
    }

    fun unitsForUids(uids: Collection<String>, date: String): Map<String, Set<String>> {
        if (uids.isEmpty()) return emptyMap()
        val safeDate = date.filter { it.isDigit() || it == '-' }
        return try {
            transaction {
                val result = mutableMapOf<String, MutableSet<String>>()
                query(
                    "SELECT uid, units FROM allocation_consists WHERE uid IN (${placeholders(uids.size)}) AND service_date = ?",
                    uids.toList() + safeDate
                ) { rs ->
                    while (rs.next()) {
                        val uid = rs.getString("uid") ?: continue
                        val units = rs.getString("units").csvList()
                        if (units.isNotEmpty()) result.getOrPut(uid) { mutableSetOf() }.addAll(units)
                    }
                }
                result.mapValues { it.value.toSet() }
            }
        } catch (e: Exception) {
            emptyMap()
        }
    }

    // ---- Stock neighbours: the services a uid's units work next / worked before -----------------------

    /** For each unit, the earliest service starting from this service's terminus after it arrives (today or tomorrow). */
    fun nextWorkings(uid: String, date: String): List<Triple<String, String, String>> {
        val safeUid = uid.filter { it.isLetterOrDigit() }
        val safeDate = date.filter { it.isDigit() || it == '-' }
        return try {
            transaction {
                val myUnits = unitsCsv(safeUid, safeDate).csvList()
                if (myUnits.isEmpty()) return@transaction emptyList()
                val scope = scheduleDateScopeArgs(effectiveDateFor(safeDate))
                val terminus = queryFirst(
                    "SELECT s.tiploc AS tp, s.scheduled_time AS tm FROM schedules s WHERE s.uid = ? AND s.stop_type = 'LT' AND $SCHEDULE_DATE_SCOPE_SQL LIMIT 1",
                    listOf(safeUid) + scope
                ) { it.getString("tp") to it.getString("tm") }
                val terminusTiploc = terminus?.first ?: return@transaction emptyList()
                val minTime = terminus.second
                val myOriginTime = queryFirst(
                    "SELECT s.scheduled_time AS tm FROM schedules s WHERE s.uid = ? AND s.stop_type = 'LO' AND $SCHEDULE_DATE_SCOPE_SQL LIMIT 1",
                    listOf(safeUid) + scope
                ) { it.getString("tm") }
                val terminusRolledOver = myOriginTime != null && minTime != null && minTime < myOriginTime
                val nextDate = plusDays(safeDate, 1)
                val nextScope = scheduleDateScopeArgs(effectiveDateFor(nextDate))
                val seen = LinkedHashMap<String, Pair<String, String>>()
                for (unit in myUnits) {
                    var best: Triple<String, String, String>? = null   // uid, headcode, service date
                    var bestKey: String? = null
                    query(
                        "SELECT ac.uid AS u, ac.headcode AS hc, ac.service_date AS sd, MIN(s.scheduled_time) AS lo FROM allocation_consists ac JOIN schedules s ON s.uid = ac.uid AND s.stop_type = 'LO' AND s.tiploc = ? WHERE ac.uid != ? AND (',' || ac.units || ',') LIKE ? AND ((ac.service_date = ? AND $SCHEDULE_DATE_SCOPE_SQL) OR (ac.service_date = ? AND $SCHEDULE_DATE_SCOPE_SQL)) GROUP BY ac.uid, ac.headcode, ac.service_date",
                        listOf(terminusTiploc, safeUid, "%,$unit,%", safeDate) + scope + nextDate + nextScope
                    ) { rs ->
                        while (rs.next()) {
                            val u = rs.getString("u") ?: continue
                            val hc = rs.getString("hc") ?: ""
                            val sd = rs.getString("sd") ?: continue
                            val lo = rs.getString("lo") ?: continue
                            if (lo.isEmpty()) continue
                            if (sd == safeDate && terminusRolledOver) continue
                            if (sd == safeDate && !terminusRolledOver && minTime != null && lo < minTime) continue
                            if (sd == nextDate && terminusRolledOver && lo < minTime) continue
                            val key = "$sd $lo"
                            if (bestKey == null || key < bestKey!!) {
                                best = Triple(u, hc, sd)
                                bestKey = key
                            }
                        }
                    }
                    best?.let { (u, hc, sd) -> seen.putIfAbsent(u, hc to sd) }
                }
                seen.entries.map { Triple(it.key, it.value.first, it.value.second) }
            }
        } catch (e: Exception) {
            emptyList()
        }
    }

    /** For each unit, the latest service terminating at this service's origin before it departs (yesterday or today). */
    fun previousWorkings(uid: String, date: String): List<Triple<String, String, String>> {
        val safeUid = uid.filter { it.isLetterOrDigit() }
        val safeDate = date.filter { it.isDigit() || it == '-' }
        return try {
            transaction {
                val myUnits = unitsCsv(safeUid, safeDate).csvList()
                if (myUnits.isEmpty()) return@transaction emptyList()
                val origin = queryFirst(
                    "SELECT s.tiploc AS tp, s.scheduled_time AS tm FROM schedules s WHERE s.uid = ? AND s.stop_type = 'LO' AND $SCHEDULE_DATE_SCOPE_SQL LIMIT 1",
                    listOf(safeUid) + scheduleDateScopeArgs(effectiveDateFor(safeDate))
                ) { it.getString("tp") to it.getString("tm") }
                val originTiploc = origin?.first ?: return@transaction emptyList()
                val maxTime = origin.second
                val prevDate = plusDays(safeDate, -1)
                val buckets = listOf(prevDate to effectiveDateFor(prevDate), safeDate to effectiveDateFor(safeDate))
                val seen = LinkedHashMap<String, Pair<String, String>>()
                for (unit in myUnits) {
                    var best: Triple<String, String, String>? = null   // uid, headcode, bucket date
                    var bestKey: String? = null
                    for ((bucketDate, bucketEffectiveDate) in buckets) {
                        val scope = scheduleDateScopeArgs(bucketEffectiveDate)
                        query(
                            "SELECT ac.uid AS u, ac.headcode AS hc, MAX(s.scheduled_time) AS lt, (SELECT MIN(s3.scheduled_time) FROM schedules s3 WHERE s3.uid = ac.uid AND s3.stop_type = 'LO' AND $SCHEDULE_DATE_SCOPE_SQL) AS ownLo FROM allocation_consists ac JOIN schedules s ON s.uid = ac.uid AND s.stop_type = 'LT' AND s.tiploc = ? WHERE ac.service_date = ? AND ac.uid != ? AND (',' || ac.units || ',') LIKE ? AND $SCHEDULE_DATE_SCOPE_SQL GROUP BY ac.uid, ac.headcode",
                            scope + listOf(originTiploc, bucketDate, safeUid, "%,$unit,%") + scope
                        ) { rs ->
                            while (rs.next()) {
                                val u = rs.getString("u") ?: continue
                                val hc = rs.getString("hc") ?: ""
                                val lt = rs.getString("lt") ?: continue
                                if (lt.isEmpty()) continue
                                val ownLo = rs.getString("ownLo")
                                // A service whose terminus time is before its own origin time arrives the next day.
                                val trueDate = if (ownLo != null && lt < ownLo) plusDays(bucketDate, 1) else bucketDate
                                if (trueDate > safeDate) continue
                                if (trueDate == safeDate && maxTime != null && lt > maxTime) continue
                                val key = "$trueDate $lt"
                                if (bestKey == null || key > bestKey!!) {
                                    best = Triple(u, hc, bucketDate)
                                    bestKey = key
                                }
                            }
                        }
                    }
                    best?.let { (u, hc, sd) -> seen.putIfAbsent(u, hc to sd) }
                }
                seen.entries.map { Triple(it.key, it.value.first, it.value.second) }
            }
        } catch (e: Exception) {
            emptyList()
        }
    }

    private fun plusDays(date: String, days: Long): String = try {
        LocalDate.parse(date).plusDays(days).toString()
    } catch (e: Exception) {
        date
    }
}

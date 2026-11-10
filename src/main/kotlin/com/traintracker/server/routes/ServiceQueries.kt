package com.traintracker.server.routes

import com.traintracker.server.ReasonCodes
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.ScheduleCancellations
import com.traintracker.server.database.Allocations
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.Schedules
import com.traintracker.server.database.activationsFor
import com.traintracker.server.database.effectiveDateFor
import com.traintracker.server.database.placeholders
import com.traintracker.server.database.query
import com.traintracker.server.database.scheduleDateScope
import com.traintracker.server.database.serviceDayArgs
import com.traintracker.server.database.serviceDaySql
import com.traintracker.server.kafka.LinxMovementWriter
import com.traintracker.server.kafka.darwinPlatforms
import java.time.LocalDate
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction

// ---- STP precedence: N (VSTP) > O (overlay) > P (permanent) > C (cancelled) ---------------------------

internal val STP_PRIORITY: Map<Char, Int> = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)

/** Lower is stronger; unknown indicators rank last. */
internal fun stpRank(row: ResultRow): Int = STP_PRIORITY[row[Schedules.stpIndicator]] ?: 99

/** Per uid, the origin row (or else any row) of its strongest STP variant; uids whose strongest variant is a cancellation are dropped. */
private fun bestOriginRows(rows: List<ResultRow>): List<ResultRow> = rows.groupBy { it[Schedules.uid] }.values.mapNotNull { group ->
    val best = group.minOf { stpRank(it) }
    if (best == STP_PRIORITY['C']) return@mapNotNull null
    group.filter { stpRank(it) == best }.minByOrNull { if (it[Schedules.stopType] == "LO") 0 else 1 }
}

// ---- Platforms ----------------------------------------------------------------------------------------

/**
 * The platform to show and whether it is confirmed: TRUST (by CRS), then Darwin in memory, then Darwin from
 * the database [darwinDb]; otherwise the booked platform, unconfirmed.
 */
internal fun resolvePlatform(
    uid: String,
    tiploc: String,
    crs: String?,
    booked: String?,
    darwinDb: String?,
    trustPlatforms: Map<Pair<String, String>, String>,
): Pair<String?, Boolean> {
    crs?.let { trustPlatforms[uid to it] }?.let { return it to true }
    darwinPlatforms[uid]?.get(tiploc)?.let { return it to true }
    darwinDb?.let { return it to true }
    return booked to false
}

// ---- Origin and destination --------------------------------------------------------------------------

/**
 * Corrections to a service's origin and destination: an overlay ('O') schedule's own LO/LT, and the origin
 * TRUST activated the train from (which differs after a change of origin).
 */
internal class OriginDestOverrides(
    private val overlayDest: Map<String, Pair<String, String?>>,
    private val overlayOrigin: Map<String, Pair<String, String?>>,
    private val activationOrigin: Map<String, String>,
) {
    /** (TIPLOC, CRS) of the origin for a schedule row. */
    fun origin(row: ResultRow): Pair<String, String?> {
        val uid = row[Schedules.uid]
        val overlay = overlayOrigin[uid]
        val activated = activationOrigin[uid]
        val tiploc = overlay?.first ?: activated ?: row[Schedules.originTiploc]
        val crs = when {
            overlay != null -> overlay.second ?: CorpusLookup.crsFromTiploc(overlay.first)
            activated != null -> CorpusLookup.crsFromTiploc(activated)
            else -> row[Schedules.originCrs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(row[Schedules.originTiploc])
        }
        return tiploc to crs
    }

    /** (TIPLOC, CRS) of the destination for a schedule row. */
    fun dest(row: ResultRow): Pair<String, String?> {
        val overlay = overlayDest[row[Schedules.uid]]
        val tiploc = overlay?.first ?: row[Schedules.destTiploc]
        val crs = overlay?.second
            ?: row[Schedules.destCrs]?.takeIf { it.isNotEmpty() }
            ?: CorpusLookup.crsFromTiploc(row[Schedules.destTiploc])
        return tiploc to crs
    }
}

internal fun JdbcTransaction.originDestOverrides(uids: Collection<String>, date: String): OriginDestOverrides {
    if (uids.isEmpty()) return OriginDestOverrides(emptyMap(), emptyMap(), emptyMap())
    val overlayDest = mutableMapOf<String, Pair<String, String?>>()
    val overlayOrigin = mutableMapOf<String, Pair<String, String?>>()
    Schedules.selectAll().where {
        (Schedules.uid inList uids) and (Schedules.stpIndicator eq 'O') and (Schedules.stopType inList listOf("LT", "LO"))
    }.forEach { row ->
        val point = row[Schedules.tiploc] to row[Schedules.crs]?.takeIf { it.isNotEmpty() }
        when (row[Schedules.stopType]) {
            "LT" -> overlayDest[row[Schedules.uid]] = point
            "LO" -> overlayOrigin[row[Schedules.uid]] = point
        }
    }
    val activationOrigin = mutableMapOf<String, String>()
    activationsFor(uids, date).forEach { (uid, activation) ->
        val stanox = activation.originStanox ?: return@forEach
        val tiploc = CorpusLookup.tiplocFromStanox(stanox) ?: run {
            val crs = CorpusLookup.crsFromStanox(stanox) ?: return@forEach
            Schedules.selectAll().where { (Schedules.crs eq crs) and (Schedules.stopType eq "LO") }
                .limit(1).firstOrNull()?.get(Schedules.tiploc)
        } ?: return@forEach
        activationOrigin[uid] = tiploc
    }
    return OriginDestOverrides(overlayDest, overlayOrigin, activationOrigin)
}

// ---- Enrichment ----------------------------------------------------------------------------------------

/** Adds allocated units and vehicles to services that have none yet. */
internal fun enrichWithUnits(services: List<ServiceResponse>, date: String = LocalDate.now().toString()): List<ServiceResponse> {
    val missing = services.filter { it.units.isEmpty() }.associate { it.uid to it.headcode }
    if (missing.isEmpty()) return services
    val allocations = Allocations.byUids(missing, date)
    return services.map { svc ->
        val alloc = if (svc.units.isEmpty()) allocations[svc.uid] else null
        if (alloc != null) svc.copy(units = alloc.units, vehicles = alloc.vehicles) else svc
    }
}

/** Adds CIF association details (splits, joins, next working) for today. */
internal fun annotateSplits(services: List<ServiceResponse>): List<ServiceResponse> {
    if (services.isEmpty()) return services
    return transaction {
        val ctx = buildAssociationContext(services.map { it.uid }.toSet())
        services.map { applyAssociations(it, ctx) }
    }
}

// ---- Operator and headcode searches --------------------------------------------------------------------

/** Every service of an operator on a date. */
internal fun queryByToc(toc: String, date: String = LocalDate.now().toString()): List<ServiceResponse> {
    val tscs = CorpusLookup.tscsForOperator(toc)
    if (tscs.isEmpty()) return emptyList()
    return transaction {
        val scope = scheduleDateScope(effectiveDateFor(date))
        val uids = Schedules.selectAll().where {
            (Schedules.tsc inList tscs) and (Schedules.stopType eq "LO") and scope
        }.map { it[Schedules.uid] }.toSet()
        if (uids.isEmpty()) return@transaction emptyList()
        val rows = Schedules.selectAll().where {
            (Schedules.uid inList uids) and ((Schedules.stopType eq "LO") or (Schedules.stpIndicator eq 'C')) and
                scheduleDateScope(effectiveDateFor(date))
        }.toList()
        val best = bestOriginRows(rows).filter { CorpusLookup.operatorForTsc(it[Schedules.tsc]).equals(toc, ignoreCase = true) }
        if (best.isEmpty()) return@transaction emptyList()
        searchResults(best, date)
    }
}

/** Every service with a headcode on a date. */
internal fun queryByHeadcode(headcode: String, date: String = LocalDate.now().toString()): List<ServiceResponse> = transaction {
    val rows = Schedules.selectAll().where {
        (Schedules.headcode eq headcode) and scheduleDateScope(effectiveDateFor(date))
    }.orderBy(Schedules.stopSequence, SortOrder.ASC).toList()
    if (rows.isEmpty()) return@transaction emptyList()
    searchResults(bestOriginRows(rows), date)
}

/** Services (one row each) with TRUST times and cancellations, platforms, origin/destination and associations. */
private fun searchResults(bestRows: List<ResultRow>, date: String): List<ServiceResponse> = transaction {
    val uids = bestRows.map { it[Schedules.uid] }.toSet()
    // "uid|scheduled time" -> latest (actual time, cancelled, reason); "uid" -> latest cancellation
    val trust = mutableMapOf<String, Triple<String, Boolean, String?>>()
    if (uids.isNotEmpty()) {
        try {
            query(
                "SELECT uid, scheduled_time, actual_time, is_cancelled, cancel_reason, event_type FROM trust_movements WHERE uid IN (${placeholders(uids.size)}) AND ${serviceDaySql("event_ts")} ORDER BY event_ts DESC",
                uids.toList() + serviceDayArgs(date)
            ) { rs ->
                while (rs.next()) {
                    val uid = rs.getString("uid")?.takeIf { it.isNotEmpty() } ?: continue
                    val scht = rs.getString("scheduled_time") ?: continue
                    val cancelled = rs.getBoolean("is_cancelled")
                    val entry = Triple(rs.getString("actual_time") ?: "", cancelled, ReasonCodes.resolve(rs.getString("cancel_reason")))
                    trust.putIfAbsent("$uid|$scht", entry)
                    if (cancelled && rs.getString("event_type") != LinxMovementWriter.COO_CANCELLATION) trust.putIfAbsent(uid, entry)
                }
            }
        } catch (e: Exception) {
        }
    }
    val overrides = originDestOverrides(uids, date)
    val ctx = buildAssociationContext(uids)
    val darwinDb = AppDatabase.darwinPlatforms(uids)
    val trustPlatforms = AppDatabase.trustPlatforms(uids)
    bestRows.sortedBy { it[Schedules.scheduledTime] }.map { row ->
        val scht = row[Schedules.scheduledTime]
        val uid = row[Schedules.uid]
        val trustEntry = trust["$uid|$scht"] ?: trust[uid]?.takeIf { it.second }
        val hasAnyActualTime = trust.entries.any { (k, v) -> k.startsWith("$uid|") && v.first.isNotEmpty() }
        val (platform, confirmed) = resolvePlatform(
            uid, row[Schedules.tiploc], row[Schedules.crs], row[Schedules.platform], darwinDb[uid to row[Schedules.tiploc]], trustPlatforms
        )
        val (originTiploc, originCrs) = overrides.origin(row)
        val (destTiploc, destCrs) = overrides.dest(row)
        val svc = ServiceResponse(
            uid = uid,
            headcode = row[Schedules.headcode],
            atocCode = CorpusLookup.operatorForTsc(row[Schedules.tsc]),
            scheduledTime = scht,
            scheduledArrival = row[Schedules.scheduledArrival],
            scheduledDeparture = row[Schedules.scheduledDeparture],
            platform = platform,
            platformConfirmed = confirmed,
            isPass = row[Schedules.isPass],
            stopType = row[Schedules.stopType],
            originTiploc = originTiploc,
            destTiploc = destTiploc,
            originCrs = originCrs,
            destCrs = destCrs,
            actualTime = trustEntry?.first ?: "",
            isCancelled = ScheduleCancellations.effectiveCancelled(uid, date, trustEntry?.second ?: false, hasAnyActualTime),
            isRunning = hasAnyActualTime,
            cancelReason = trustEntry?.third
                ?: if (trustEntry?.second != true && ScheduleCancellations.isCancelled(uid, date)) TIMETABLE_CANCELLED else null
        )
        applyAssociations(svc, ctx)
    }.withResolvedNames()
}

internal const val TIMETABLE_CANCELLED = "Cancelled in the timetable"

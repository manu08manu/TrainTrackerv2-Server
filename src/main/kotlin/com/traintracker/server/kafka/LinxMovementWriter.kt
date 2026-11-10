package com.traintracker.server.kafka

import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.SmartLookup
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.MovementBatch
import com.traintracker.server.database.Schedules
import com.traintracker.server.database.Movements
import com.traintracker.server.database.effectiveDateFor
import com.traintracker.server.database.scheduleDateScope
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("LinxMovementWriter")

/**
 * TrainTracker uses LINX EI instead of the TRUST movements feed (both remain available): it writes movements and
 * train_activations in TRUST's shape (so every reader keeps working), and feeds live locations, the
 * train ID -> uid map, the TD correlation and the missing-location checks.
 *
 * Change of origin differs from TRUST's handling on purpose: the activation origin moves to the new start
 * (the service page already starts the journey there), and each stop before it gets its own cancellation
 * under its own STANOX, typed COO_CANCELLATION so readers cancel only that stop and not the rest of the run.
 */
internal object LinxMovementWriter {
    /** Event type of a stop cancelled because the train now starts later (change of origin). */
    const val COO_CANCELLATION = "COO_CANCELLED"

    private val STP_RANK = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)

    /** Side effects of a running report (live location, TD correlation, checks); returns the row to write. */
    fun movement(
        trainId: String, uid: String, headcode: String, stanox: String?, crs: String?, eventType: String,
        bookedTime: String, actualTime: String, delay: Int?,
    ): MovementBatch? {
        if (stanox == null) return null
        val resolvedUid = uid.ifEmpty { trainIdToRid[trainId] ?: "" }
        val type = if (eventType == "DEPARTURE") "DEPARTURE" else "ARRIVAL"
        CorpusLookup.checkMovementForMissingName(stanox, headcode)
        CorpusLookup.checkMovementForMissingAtoc(headcode)
        if (actualTime.isNotEmpty()) {
            TdCorrelator.onMovement(headcode, stanox, type, actualTime)
            if (SmartLookup.isLoaded() && !SmartLookup.hasCoverage(stanox, type)) CorpusLookup.recordNoSmartCoverage(stanox, type, headcode)
        }
        if (actualTime.isNotEmpty() && type == "DEPARTURE") {
            val late = maxOf(0, delay ?: 0)
            val stationName = crs ?: stanox
            trainLocations["$headcode|$resolvedUid"] = TrainLocation(headcode, resolvedUid, stationName, crs, actualTime, type, late)
            AppDatabase.upsertLocation(headcode, resolvedUid, stationName, crs, actualTime, type, late)
        }
        return MovementBatch(headcode, trainId, stanox, crs, type, bookedTime.ifEmpty { null }, actualTime.ifEmpty { null }, false, null, resolvedUid)
    }

    fun activation(trainId: String, uid: String, originStanox: String?) {
        if (uid.isEmpty()) return
        trainIdToRid[trainId] = uid
        AppDatabase.saveTrainActivation(trainId, uid, originStanox)
    }

    /** S513 91/92: a cancellation at [stanox]; readers carry it on to later stops, as with TRUST. */
    fun cancellation(trainId: String, uid: String, headcode: String, stanox: String?, time: String, reason: String?) {
        if (stanox == null) return
        AppDatabase.batchUpsertMovements(
            listOf(
                MovementBatch(
                    headcode, trainId, stanox, CorpusLookup.crsFromStanox(stanox), "CANCELLATION",
                    time.ifEmpty { null }, null, true, reason, uid.ifEmpty { trainIdToRid[trainId] ?: "" }
                )
            )
        )
    }

    /** S513 96: undoes cancellations at that scheduled time and records the reinstatement. */
    fun reinstatement(trainId: String, uid: String, headcode: String, stanox: String?, time: String) {
        if (stanox == null) return
        val resolvedUid = uid.ifEmpty { trainIdToRid[trainId] ?: "" }
        if (resolvedUid.isNotEmpty() && time.isNotEmpty()) {
            transaction {
                Movements.update({
                    (Movements.uid eq resolvedUid) and (Movements.scheduledTime eq time) and (Movements.isCancelled eq true)
                }) {
                    it[isCancelled] = false
                    it[cancelReason] = null
                }
            }
        }
        AppDatabase.batchUpsertMovements(
            listOf(MovementBatch(headcode, trainId, stanox, CorpusLookup.crsFromStanox(stanox), "REINSTATEMENT", time.ifEmpty { null }, null, false, uid = resolvedUid))
        )
        log.info("LINX reinstatement: $headcode at ${time.ifEmpty { "unknown time" }}")
    }

    /**
     * S513 94: the train now starts at [tiploc]. Moves the activation origin there and cancels each earlier
     * stop of the day's timetable individually.
     */
    fun changeOfOrigin(
        trainId: String, uid: String, headcode: String, startDate: String, tiploc: String?, stanox: String?, reason: String?,
        stanoxOf: (String) -> String?,
    ) {
        val resolvedUid = uid.ifEmpty { trainIdToRid[trainId] ?: "" }
        if (resolvedUid.isEmpty() || tiploc == null || stanox == null) return
        activation(trainId, resolvedUid, stanox)
        try {
            // A later change of origin replaces an earlier one (it can even move the start back).
            transaction {
                Movements.deleteWhere { (Movements.trainId eq trainId) and (Movements.eventType eq COO_CANCELLATION) }
            }
            val date = startDate.takeIf { it.length == 10 } ?: java.time.LocalDate.now().toString()
            val earlier = transaction {
                val rows = Schedules.selectAll()
                    .where { (Schedules.uid eq resolvedUid) and scheduleDateScope(effectiveDateFor(date)) }
                    .toList()
                val best = rows.minOfOrNull { STP_RANK[it[Schedules.stpIndicator]] ?: 99 } ?: return@transaction emptyList()
                if (best == STP_RANK['C']) return@transaction emptyList()
                val run = rows.filter { (STP_RANK[it[Schedules.stpIndicator]] ?: 99) == best }.sortedBy { it[Schedules.stopSequence] }
                val newOriginIdx = run.indexOfFirst { it[Schedules.tiploc] == tiploc }
                if (newOriginIdx <= 0) return@transaction emptyList()
                run.take(newOriginIdx).filter { !it[Schedules.isPass] }
            }
            val cancels = earlier.mapNotNull { row ->
                val stopStanox = stanoxOf(row[Schedules.tiploc]) ?: return@mapNotNull null
                val crs = row[Schedules.crs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(row[Schedules.tiploc])
                MovementBatch(headcode, trainId, stopStanox, crs, COO_CANCELLATION, row[Schedules.scheduledTime], null, true, reason, resolvedUid)
            }
            AppDatabase.batchUpsertMovements(cancels)
            log.info("LINX change of origin: $headcode ($resolvedUid) now starts at $tiploc; ${cancels.size} earlier stops cancelled")
        } catch (e: Exception) {
            log.warn("LINX change of origin failed for $headcode: ${e.message}")
        }
    }

    /**
     * S515: the train keeps its train ID but runs under [newHeadcode] from now on (later LINX messages carry
     * it). Keeps an alias so searches for either headcode find it, and moves its live location across.
     */
    fun changeOfIdentity(trainId: String, uid: String, oldHeadcode: String, newHeadcode: String) {
        if (oldHeadcode.length != 4 || newHeadcode.length != 4 || oldHeadcode == newHeadcode) return
        val resolvedUid = uid.ifEmpty { trainIdToRid[trainId] ?: "" }
        transaction {
            AppDatabase.saveHeadcodeAlias(oldHeadcode, newHeadcode, resolvedUid)
            AppDatabase.transferLocation(oldHeadcode, newHeadcode, resolvedUid)
        }
        trainLocations.remove("$oldHeadcode|$resolvedUid")?.let { trainLocations["$newHeadcode|$resolvedUid"] = it.copy(headcode = newHeadcode) }
        log.info("LINX change of identity: $oldHeadcode -> $newHeadcode ($resolvedUid)")
    }

    /** Copies a TD platform onto the matching movement row. Call inside a transaction. */
    fun platform(trainId: String, stanox: String?, eventType: String, platform: String) {
        if (stanox == null) return
        val type = if (eventType == "DEPARTURE") "DEPARTURE" else "ARRIVAL"
        Movements.update({
            (Movements.trainId eq trainId) and (Movements.stanox eq stanox) and (Movements.eventType eq type)
        }) { it[Movements.platform] = platform }
    }
}

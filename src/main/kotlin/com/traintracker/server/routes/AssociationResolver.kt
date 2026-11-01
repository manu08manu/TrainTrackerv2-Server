package com.traintracker.server.routes

import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.AssociationRecord
import com.traintracker.server.database.effectiveDateFor
import com.traintracker.server.database.scheduleDateScopeArgs
import kotlinx.serialization.Serializable
import org.jetbrains.exposed.v1.core.IColumnType
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.core.VarCharColumnType
import java.time.LocalDate

@Serializable
data class SplitCandidate(
    val uid: String,
    val headcode: String,
    val destName: String?,
    val joinsUid: String? = null,
    val joinsHeadcode: String? = null,
    val units: String? = null,
    val matchedTiploc: String? = null,
    val coachCount: Int? = null
)

@Serializable
data class StockNeighbourCandidate(
    val uid: String,
    val headcode: String,
    val date: String? = null
)

data class AssociationContext(
    val assocMap: Map<String, List<AssociationRecord>>,
    val reverseAssocMap: Map<String, List<AssociationRecord>>,
    val assocHeadcodes: Map<String, String>,
    val assocEffectiveLos: Map<String, String>,
    val assocOriginTiplocs: Map<String, String>,
    val assocDestTiplocs: Map<String, String>,
    val assocAllStops: Map<String, Set<String>>,
    val svcTermini: Map<String, String>,
    val assocOriginTimes: Map<String, String>,
    val assocUnits: Map<String, Set<String>>
)

fun JdbcTransaction.buildAssociationContext(uids: Set<String>, date: String = LocalDate.now().toString()): AssociationContext {
    val effDate = effectiveDateFor(date)
    val assocMap = AppDatabase.getAssociationsForUids(uids)
    val reverseAssocMap = AppDatabase.getReverseAssociationsForUids(uids)
    val allAssocUids = (assocMap.values.flatten().map { it.assocUid } + reverseAssocMap.values.flatten().map { it.mainUid }).toSet()
    val assocHeadcodes = mutableMapOf<String, String>()
    val assocEffectiveLos = mutableMapOf<String, String>()
    val assocOriginTiplocs = mutableMapOf<String, String>()
    val assocDestTiplocs = mutableMapOf<String, String>()
    val assocAllStops = mutableMapOf<String, MutableSet<String>>()
    val svcTermini = mutableMapOf<String, String>()
    val assocOriginTimes = mutableMapOf<String, String>()
    val assocUnits = AppDatabase.getUnitsForUids(allAssocUids + uids, date)
    if (allAssocUids.isNotEmpty()) {
        val allAssocUidsList = allAssocUids.toList()
        val placeholders1 = allAssocUidsList.joinToString(",") { "?" }
        val args1 = allAssocUidsList.map { (VarCharColumnType() as IColumnType<*>) to (it as Any?) }
        exec(
            "SELECT uid, headcode, tiploc, stop_type, stp_indicator, scheduled_time FROM schedules WHERE uid IN ($placeholders1) AND stp_indicator != 'C' AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '') ORDER BY CASE stp_indicator WHEN 'N' THEN 0 WHEN 'O' THEN 1 WHEN 'P' THEN 2 ELSE 3 END",
            args1 + scheduleDateScopeArgs(effDate)
        ) { rs ->
            while (rs.next()) {
                val rUid = rs.getString("uid") ?: continue
                val rTiploc = rs.getString("tiploc") ?: continue
                val rStop = rs.getString("stop_type") ?: continue
                val rHc = rs.getString("headcode") ?: ""
                val rTime = rs.getString("scheduled_time") ?: ""
                assocAllStops.getOrPut(rUid) { mutableSetOf() }.add(rTiploc)
                if (rStop == "LO") {
                    assocHeadcodes.putIfAbsent(rUid, rHc)
                    assocEffectiveLos.putIfAbsent(rUid, rTiploc)
                    assocOriginTiplocs.putIfAbsent(rUid, rTiploc)
                    if (rTime.isNotEmpty()) assocOriginTimes.putIfAbsent(rUid, rTime)
                }
                if (rStop == "LT") assocDestTiplocs.putIfAbsent(rUid, rTiploc)
            }
        }
    }
    if (uids.isNotEmpty()) {
        val uidsList = uids.toList()
        val placeholders2 = uidsList.joinToString(",") { "?" }
        val args2 = uidsList.map { (VarCharColumnType() as IColumnType<*>) to (it as Any?) }
        exec(
            "SELECT uid, tiploc, stp_indicator FROM schedules WHERE uid IN ($placeholders2) AND stop_type='LT' AND stp_indicator IN ('N','O','P') AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '') ORDER BY CASE stp_indicator WHEN 'N' THEN 0 WHEN 'O' THEN 1 ELSE 2 END",
            args2 + scheduleDateScopeArgs(effDate)
        ) { rs ->
            while (rs.next()) {
                val rUid = rs.getString("uid") ?: continue
                val rTiploc = rs.getString("tiploc") ?: continue
                svcTermini.putIfAbsent(rUid, rTiploc)
            }
        }
    }
    return AssociationContext(
        assocMap, reverseAssocMap, assocHeadcodes, assocEffectiveLos, assocOriginTiplocs, assocDestTiplocs,
        assocAllStops, svcTermini, assocOriginTimes, assocUnits
    )
}

fun resolveConnectionDate(requestedDate: String, dateIndicator: Char?, fallbackNextDay: Boolean = false): String {
    val base = LocalDate.parse(requestedDate)
    return when (dateIndicator) {
        'N' -> base.plusDays(1).toString()
        'P' -> base.minusDays(1).toString()
        'S' -> requestedDate
        else -> if (fallbackNextDay) base.plusDays(1).toString() else requestedDate
    }
}

data class FormsCandidate(
    val uid: String,
    val headcode: String,
    val dateIndicator: Char?,
    val stpInd: Char
)

private fun pickFormsCandidate(formsCands: List<FormsCandidate>, myUnits: Set<String>, ctx: AssociationContext): FormsCandidate? {
    val stpPriority = mapOf('N' to 0, 'O' to 1, 'P' to 2)
    val bestStp = formsCands.mapNotNull { stpPriority[it.stpInd] }.minOrNull()
    val stpFiltered = if (bestStp != null) formsCands.filter { stpPriority[it.stpInd] == bestStp } else formsCands
    val fShare = stpFiltered.filter { c -> (ctx.assocUnits[c.uid] ?: emptySet()).any { it in myUnits } }
    return if (stpFiltered.size == 1) stpFiltered.first() else if (fShare.size == 1) fShare.first() else null
}

fun resolveNextForms(uid: String, ctx: AssociationContext, date: String): FormsCandidate? {
    val assocs = ctx.assocMap[uid] ?: return null
    val formsCands = mutableListOf<FormsCandidate>()
    for (assoc in assocs) {
        if (assoc.assocType in listOf("JJ", "NP")) {
            val assocHeadcode = ctx.assocHeadcodes[assoc.assocUid] ?: ""
            val assocTiplocIsTerminus = ctx.svcTermini[uid] == assoc.assocTiploc
            val assocOriginTiploc = ctx.assocOriginTiplocs[assoc.assocUid] ?: ""
            if (assocTiplocIsTerminus && assocHeadcode.isNotEmpty() && assocOriginTiploc == assoc.assocTiploc) {
                formsCands.add(FormsCandidate(assoc.assocUid, assocHeadcode, assoc.dateIndicator, assoc.stpInd))
            }
        }
    }
    val myUnits = ctx.assocUnits[uid] ?: emptySet()
    return pickFormsCandidate(formsCands, myUnits, ctx)
}

private data class ParentCand(
    val uid: String,
    val headcode: String,
    val tiploc: String,
    val tiplocName: String,
    val assocType: String,
    val dateIndicator: Char? = null
)

private fun unitsCompatible(mine: Set<String>, theirs: Set<String>): Boolean {
    if (mine.isEmpty() || theirs.isEmpty()) return true
    return mine.any { it in theirs }
}

fun applyAssociations(svc: ServiceResponse, ctx: AssociationContext, date: String = LocalDate.now().toString()): ServiceResponse {
    val uid = svc.uid
    var formsUid2: String? = null
    var formsHeadcode2: String? = null
    var formsDate2: String? = null
    val formsCands = mutableListOf<FormsCandidate>()
    val splitCands = mutableListOf<SplitCandidate>()
    var earlySplitFromUid: String? = null
    var earlySplitFromHeadcode: String? = null
    var earlySplitFromDate: String? = null
    run {
        val earlyReverseAssocs = ctx.reverseAssocMap[uid]
        if (!earlyReverseAssocs.isNullOrEmpty()) {
            val vvParentCands = earlyReverseAssocs
                .filter { it.assocType == "VV" }
                .mapNotNull { assoc ->
                    val parentHeadcode = ctx.assocHeadcodes[assoc.mainUid] ?: ""
                    val mainCallsAtAssocTiploc = ctx.assocAllStops[assoc.mainUid]
                        ?.contains(assoc.assocTiploc) ?: false
                    if (parentHeadcode.isNotEmpty() && mainCallsAtAssocTiploc) {
                        ParentCand(assoc.mainUid, parentHeadcode, assoc.assocTiploc, "", "VV", assoc.dateIndicator)
                    } else null
                }
            if (vvParentCands.isNotEmpty()) {
                val myUnitsEarly = ctx.assocUnits[uid] ?: emptySet()
                val vvShared = vvParentCands.filter { p -> (ctx.assocUnits[p.uid] ?: emptySet()).any { it in myUnitsEarly } }
                val vvPick = if (vvParentCands.size == 1) vvParentCands.first() else vvShared.singleOrNull() ?: vvParentCands.first()
                if (unitsCompatible(myUnitsEarly, ctx.assocUnits[vvPick.uid] ?: emptySet())) {
                    earlySplitFromUid = vvPick.uid
                    earlySplitFromHeadcode = vvPick.headcode
                    earlySplitFromDate = resolveConnectionDate(date, vvPick.dateIndicator)
                }
            }
        }
    }

    val assocs = ctx.assocMap[uid]
    if (!assocs.isNullOrEmpty()) {
        var splitTiploc: String? = null
        var splitTiplocName: String? = null
        var splitToHeadcode: String? = null
        var splitToUid2: String? = null
        var splitToDestName2: String? = null
        var splitToDate2: String? = null
        var couplingTiploc2: String? = null
        var couplingTiplocName2: String? = null
        var coupledFromHeadcode2: String? = null
        val couplingCands = mutableListOf<CoupledParent>()
        for (assoc in assocs) {
            val assocHeadcode = ctx.assocHeadcodes[assoc.assocUid] ?: ""
            val tiplocCrs = CorpusLookup.nameFromTiploc(assoc.assocTiploc) ?: assoc.assocTiploc
            val assocTiplocIsTerminus = ctx.svcTermini[uid] == assoc.assocTiploc
            val assocEffectiveLo = ctx.assocEffectiveLos[assoc.assocUid] ?: ""
            val assocOriginTiploc = ctx.assocOriginTiplocs[assoc.assocUid] ?: ""
            val assocDestTiploc = ctx.assocDestTiplocs[assoc.assocUid] ?: ""
            when (assoc.assocType) {
                "VV" -> {
                    val isPassengerSplit = isPassengerHeadcode(assocHeadcode)
                    val isValidSplit = !assocTiplocIsTerminus &&
                        (assocEffectiveLo == assoc.assocTiploc || assocEffectiveLo.isEmpty()) &&
                        unitsCompatible(ctx.assocUnits[uid] ?: emptySet(), ctx.assocUnits[assoc.assocUid] ?: emptySet())
                    if (isValidSplit) {
                        splitCands.add(
                            SplitCandidate(
                                assoc.assocUid,
                                assocHeadcode,
                                assocDestTiploc.takeIf { it.isNotEmpty() }?.let { CorpusLookup.nameFromTiploc(it) }
                            )
                        )
                        splitTiploc = assoc.assocTiploc
                        splitTiplocName = tiplocCrs
                    }
                    if (isPassengerSplit && isValidSplit) {
                        splitToHeadcode = assocHeadcode
                        splitToUid2 = assoc.assocUid
                        splitToDestName2 = assocDestTiploc.takeIf { it.isNotEmpty() }
                            ?.let { CorpusLookup.nameFromTiploc(it) }
                        val splitOriginTime = ctx.assocOriginTimes[assoc.assocUid] ?: ""
                        val splitFallbackNextDay = splitOriginTime.isNotEmpty() && splitOriginTime < svc.scheduledTime
                        splitToDate2 = resolveConnectionDate(date, assoc.dateIndicator, splitFallbackNextDay)
                    }
                }
                "JJ", "NP" -> {
                    val validJoin = !assocTiplocIsTerminus && assocHeadcode.isNotEmpty() &&
                        (assocOriginTiploc == assoc.assocTiploc || assocDestTiploc == assoc.assocTiploc)
                    if (validJoin) {
                        couplingTiploc2 = assoc.assocTiploc
                        couplingTiplocName2 = tiplocCrs
                        coupledFromHeadcode2 = assocHeadcode
                        couplingCands.add(CoupledParent(assoc.assocUid, assocHeadcode, assoc.assocType))
                    } else if (assocTiplocIsTerminus && assocHeadcode.isNotEmpty() && assocOriginTiploc == assoc.assocTiploc) {
                        formsCands.add(FormsCandidate(assoc.assocUid, assocHeadcode, assoc.dateIndicator, assoc.stpInd))
                    }
                }
            }
        }
        val myUnits = ctx.assocUnits[uid] ?: emptySet()
        val fPick = pickFormsCandidate(formsCands, myUnits, ctx)
        formsUid2 = fPick?.uid
        formsHeadcode2 = fPick?.headcode
        formsDate2 = fPick?.let { resolveConnectionDate(date, it.dateIndicator) }
        val couplingPrimary = couplingCands.firstOrNull { isPassengerHeadcode(it.headcode) } ?: couplingCands.firstOrNull()
        val matchedCoupling = assocs.firstOrNull { it.assocTiploc == couplingTiploc2 }
        if (splitTiploc != null) {
            return svc.copy(
                association = svc.association.copy(
                    formsUid = formsUid2,
                    formsHeadcode = formsHeadcode2,
                    formsDate = formsDate2,
                    splitTiploc = splitTiploc,
                    splitTiplocName = splitTiplocName,
                    splitToUid = splitToUid2,
                    splitToHeadcode = splitToHeadcode,
                    splitToDestName = splitToDestName2,
                    splitToDate = splitToDate2,
                    splitToAll = splitCands,
                    splitFromUid = earlySplitFromUid,
                    splitFromHeadcode = earlySplitFromHeadcode,
                    splitFromDate = earlySplitFromDate
                )
            )
        }
        if (couplingTiploc2 != null) {
            return svc.copy(
                association = svc.association.copy(
                    couplingTiploc = couplingTiploc2,
                    couplingTiplocName = couplingTiplocName2,
                    coupledFromUid = couplingPrimary?.uid,
                    coupledFromHeadcode = couplingPrimary?.headcode,
                    couplingAssocType = couplingPrimary?.assocType ?: matchedCoupling?.assocType ?: "NP",
                    coupledFromAll = couplingCands,
                    splitFromUid = earlySplitFromUid,
                    splitFromHeadcode = earlySplitFromHeadcode,
                    splitFromDate = earlySplitFromDate,
                    formsUid = formsUid2,
                    formsHeadcode = formsHeadcode2,
                    formsDate = formsDate2,
                    splitToAll = splitCands
                )
            )
        }
    }

    val parentCands = mutableListOf<ParentCand>()
    val reverseAssocs = ctx.reverseAssocMap[uid]
    if (!reverseAssocs.isNullOrEmpty() && svc.association.couplingTiploc == null) {
        for (assoc in reverseAssocs) {
            if (assoc.assocType in listOf("VV", "JJ", "NP")) {
                val parentHeadcode = ctx.assocHeadcodes[assoc.mainUid] ?: ""
                val tiplocCrs = CorpusLookup.nameFromTiploc(assoc.assocTiploc) ?: assoc.assocTiploc
                val mainCallsAtAssocTiploc = ctx.assocAllStops[assoc.mainUid]
                    ?.contains(assoc.assocTiploc) ?: false
                if (parentHeadcode.isNotEmpty() && mainCallsAtAssocTiploc) {
                    parentCands.add(ParentCand(assoc.mainUid, parentHeadcode, assoc.assocTiploc, tiplocCrs, assoc.assocType, assoc.dateIndicator))
                }
            }
        }
    }
    if (parentCands.isNotEmpty()) {
        val myUnitsRev = ctx.assocUnits[uid] ?: emptySet()
        val nonVvParentCands = parentCands.filter { it.assocType != "VV" }
        val shared = nonVvParentCands.filter { p -> (ctx.assocUnits[p.uid] ?: emptySet()).any { it in myUnitsRev } }
        val primary = if (nonVvParentCands.isEmpty()) null
        else if (nonVvParentCands.size == 1) nonVvParentCands.first()
        else shared.singleOrNull() ?: nonVvParentCands.first()
        val vvCands = parentCands.filter { it.assocType == "VV" }
        val vvShared = vvCands.filter { p -> (ctx.assocUnits[p.uid] ?: emptySet()).any { it in myUnitsRev } }
        val vvPrimary = (
            if (vvCands.isEmpty()) null
            else if (vvCands.size == 1) vvCands.first()
            else vvShared.singleOrNull() ?: vvCands.first()
            )?.takeIf { unitsCompatible(myUnitsRev, ctx.assocUnits[it.uid] ?: emptySet()) }
        return svc.copy(
            association = svc.association.copy(
                couplingTiploc = primary?.tiploc,
                couplingTiplocName = primary?.tiplocName,
                coupledFromUid = primary?.uid,
                coupledFromHeadcode = primary?.headcode,
                couplingAssocType = primary?.assocType,
                coupledFromAll = parentCands.map { CoupledParent(it.uid, it.headcode, it.assocType) },
                splitFromUid = vvPrimary?.uid,
                splitFromHeadcode = vvPrimary?.headcode,
                splitFromDate = vvPrimary?.let { resolveConnectionDate(date, it.dateIndicator) },
                formsUid = formsUid2,
                formsHeadcode = formsHeadcode2,
                formsDate = formsDate2,
                splitToAll = splitCands
            )
        )
    }
    return if (formsUid2 != null || splitCands.isNotEmpty()) {
        svc.copy(
            association = svc.association.copy(
                formsUid = formsUid2,
                formsHeadcode = formsHeadcode2,
                formsDate = formsDate2,
                splitToAll = splitCands
            )
        )
    } else svc
}

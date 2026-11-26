package com.traintracker.server.routes

import com.traintracker.server.reference.CorpusLookup
import com.traintracker.server.database.Allocations
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.AssociationRecord
import com.traintracker.server.database.SCHEDULE_DATE_SCOPE_SQL
import com.traintracker.server.database.effectiveDateFor
import com.traintracker.server.database.placeholders
import com.traintracker.server.database.query
import com.traintracker.server.database.scheduleDateScopeArgs
import java.time.LocalDate
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.jdbc.transactions.transaction

// Schedule associations: VV = divide (split), JJ = join, NP = next working (the train this one forms).

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

/** Associations of a set of services, with the schedule facts needed to judge them. */
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
    val assocUnits: Map<String, Set<String>>,
    val cancelledTiplocs: Map<String, Set<String>> = emptyMap()
)

fun JdbcTransaction.buildAssociationContext(uids: Set<String>, date: String = LocalDate.now().toString()): AssociationContext {
    val effDate = effectiveDateFor(date)
    val assocMap = AppDatabase.associationsFrom(uids)
    val reverseAssocMap = AppDatabase.associationsTo(uids)
    val allAssocUids = (assocMap.values.flatten().map { it.assocUid } + reverseAssocMap.values.flatten().map { it.mainUid }).toSet()
    val assocHeadcodes = mutableMapOf<String, String>()
    val assocEffectiveLos = mutableMapOf<String, String>()
    val assocOriginTiplocs = mutableMapOf<String, String>()
    val assocDestTiplocs = mutableMapOf<String, String>()
    val assocAllStops = mutableMapOf<String, MutableSet<String>>()
    val svcTermini = mutableMapOf<String, String>()
    val assocOriginTimes = mutableMapOf<String, String>()
    val assocUnits = Allocations.unitsForUids(allAssocUids + uids, date)
    val cancelledTiplocs = loadCancelledTiplocs(allAssocUids + uids, date)
    if (allAssocUids.isNotEmpty()) {
        query(
            "SELECT uid, headcode, tiploc, stop_type, stp_indicator, scheduled_time FROM schedules WHERE uid IN (${placeholders(allAssocUids.size)}) AND stp_indicator != 'C' AND $SCHEDULE_DATE_SCOPE_SQL ORDER BY CASE stp_indicator WHEN 'N' THEN 0 WHEN 'O' THEN 1 WHEN 'P' THEN 2 ELSE 3 END",
            allAssocUids.toList() + scheduleDateScopeArgs(effDate)
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
        query(
            "SELECT uid, tiploc, stp_indicator FROM schedules WHERE uid IN (${placeholders(uids.size)}) AND stop_type='LT' AND stp_indicator IN ('N','O','P') AND $SCHEDULE_DATE_SCOPE_SQL ORDER BY CASE stp_indicator WHEN 'N' THEN 0 WHEN 'O' THEN 1 ELSE 2 END",
            uids.toList() + scheduleDateScopeArgs(effDate)
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
        assocAllStops, svcTermini, assocOriginTimes, assocUnits, cancelledTiplocs
    )
}

/** The date of a connecting service from a schedule date indicator: N = next day, P = previous day, S = same day. */
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

/** The service this one forms at its terminus (JJ/NP starting where it terminates), when unambiguous. */
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

/**
 * Adds association details to a service: the portions it divides into (VV), the train it joins or is joined by
 * (JJ/NP mid-journey), what it forms at its terminus, and the train it split from.
 */
fun applyAssociations(svc: ServiceResponse, ctx: AssociationContext, date: String = LocalDate.now().toString()): ServiceResponse =
    ctx.pruneCancelled(applyAssociationsRaw(svc, ctx, date))

/** The train this service divided from (VV). */
private class SplitFrom(val uid: String, val headcode: String, val date: String)

/** One valid portion this service divides into. */
private class DividePortion(val assoc: AssociationRecord, val headcode: String, val destName: String?)

/** What a service divides into: every valid portion, the divide point, and the last passenger portion. */
private class DividesInto(
    val all: List<SplitCandidate>,
    val tiploc: String,
    val tiplocName: String,
    val toUid: String?,
    val toHeadcode: String?,
    val toDestName: String?,
    val toDate: String?,
)

/** The mid-journey join (JJ/NP) of a service with another train, and every train it joins. */
private class JoinsWith(val tiploc: String, val tiplocName: String, val parents: List<CoupledParent>)

/** Picks one of several candidate parents: the only one, else the only one sharing a unit with this service, else the first. */
private fun pickParent(cands: List<ParentCand>, myUnits: Set<String>, ctx: AssociationContext): ParentCand? {
    if (cands.size <= 1) return cands.firstOrNull()
    val sharing = cands.filter { p -> (ctx.assocUnits[p.uid] ?: emptySet()).any { it in myUnits } }
    return sharing.singleOrNull() ?: cands.first()
}

/** Trains that divide into (VV) or join (JJ/NP) this one at a stop the parent really calls at. */
private fun parentCandidates(uid: String, ctx: AssociationContext): List<ParentCand> =
    ctx.reverseAssocMap[uid].orEmpty()
        .filter { it.assocType in listOf("VV", "JJ", "NP") }
        .mapNotNull { assoc ->
            val parentHeadcode = ctx.assocHeadcodes[assoc.mainUid] ?: ""
            val callsAtTiploc = ctx.assocAllStops[assoc.mainUid]?.contains(assoc.assocTiploc) ?: false
            if (parentHeadcode.isNotEmpty() && callsAtTiploc) {
                val tiplocName = CorpusLookup.nameFromTiploc(assoc.assocTiploc) ?: assoc.assocTiploc
                ParentCand(assoc.mainUid, parentHeadcode, assoc.assocTiploc, tiplocName, assoc.assocType, assoc.dateIndicator)
            } else null
        }

/** The train this one divided from (VV), when its units fit. */
private fun findSplitFrom(parents: List<ParentCand>, myUnits: Set<String>, ctx: AssociationContext, date: String): SplitFrom? {
    val pick = pickParent(parents.filter { it.assocType == "VV" }, myUnits, ctx) ?: return null
    if (!unitsCompatible(myUnits, ctx.assocUnits[pick.uid] ?: emptySet())) return null
    return SplitFrom(pick.uid, pick.headcode, resolveConnectionDate(date, pick.dateIndicator))
}

/** The portions this service divides into (VV): not at its terminus, starting at the divide point, with units that fit. */
private fun findDivides(svc: ServiceResponse, ctx: AssociationContext, date: String): DividesInto? {
    val uid = svc.uid
    val myUnits = ctx.assocUnits[uid] ?: emptySet()
    val portions = ctx.assocMap[uid].orEmpty().filter { it.assocType == "VV" }.mapNotNull { a ->
        val headcode = ctx.assocHeadcodes[a.assocUid] ?: ""
        val effectiveLo = ctx.assocEffectiveLos[a.assocUid] ?: ""
        val valid = ctx.svcTermini[uid] != a.assocTiploc &&
            (effectiveLo == a.assocTiploc || effectiveLo.isEmpty()) &&
            unitsCompatible(myUnits, ctx.assocUnits[a.assocUid] ?: emptySet())
        if (!valid) return@mapNotNull null
        val destName = (ctx.assocDestTiplocs[a.assocUid] ?: "").takeIf { it.isNotEmpty() }
            ?.let { CorpusLookup.nameFromTiploc(it) }
        DividePortion(a, headcode, destName)
    }
    val last = portions.lastOrNull() ?: return null
    val toPassenger = portions.lastOrNull { isPassengerHeadcode(it.headcode) }
    val toDate = toPassenger?.let { p ->
        val originTime = ctx.assocOriginTimes[p.assoc.assocUid] ?: ""
        resolveConnectionDate(date, p.assoc.dateIndicator, originTime.isNotEmpty() && originTime < svc.scheduledTime)
    }
    return DividesInto(
        all = portions.map { SplitCandidate(it.assoc.assocUid, it.headcode, it.destName) },
        tiploc = last.assoc.assocTiploc,
        tiplocName = CorpusLookup.nameFromTiploc(last.assoc.assocTiploc) ?: last.assoc.assocTiploc,
        toUid = toPassenger?.assoc?.assocUid,
        toHeadcode = toPassenger?.headcode,
        toDestName = toPassenger?.destName,
        toDate = toDate,
    )
}

/** The trains this service joins or is joined by mid-journey (JJ/NP): not at its terminus, and one end of the other train is the join point. */
private fun findJoins(uid: String, ctx: AssociationContext): JoinsWith? {
    val joins = ctx.assocMap[uid].orEmpty().filter { it.assocType == "JJ" || it.assocType == "NP" }.filter { a ->
        val headcode = ctx.assocHeadcodes[a.assocUid] ?: ""
        val origin = ctx.assocOriginTiplocs[a.assocUid] ?: ""
        val dest = ctx.assocDestTiplocs[a.assocUid] ?: ""
        ctx.svcTermini[uid] != a.assocTiploc && headcode.isNotEmpty() && (origin == a.assocTiploc || dest == a.assocTiploc)
    }
    val last = joins.lastOrNull() ?: return null
    return JoinsWith(
        tiploc = last.assocTiploc,
        tiplocName = CorpusLookup.nameFromTiploc(last.assocTiploc) ?: last.assocTiploc,
        parents = joins.map { CoupledParent(it.assocUid, ctx.assocHeadcodes[it.assocUid] ?: "", it.assocType) },
    )
}

private fun AssociationInfo.withForms(forms: FormsCandidate?, formsDate: String?) =
    copy(formsUid = forms?.uid, formsHeadcode = forms?.headcode, formsDate = formsDate)

private fun AssociationInfo.withSplitFrom(from: SplitFrom?) =
    copy(splitFromUid = from?.uid, splitFromHeadcode = from?.headcode, splitFromDate = from?.date)

private fun applyAssociationsRaw(svc: ServiceResponse, ctx: AssociationContext, date: String): ServiceResponse {
    val uid = svc.uid
    val myUnits = ctx.assocUnits[uid] ?: emptySet()
    val assoc = svc.association
    val parents = parentCandidates(uid, ctx)
    val splitFrom = findSplitFrom(parents, myUnits, ctx, date)
    val forms = resolveNextForms(uid, ctx, date)
    val formsDate = forms?.let { resolveConnectionDate(date, it.dateIndicator) }

    // 1. It divides into portions here.
    findDivides(svc, ctx, date)?.let { d ->
        return svc.copy(
            association = assoc.withForms(forms, formsDate).withSplitFrom(splitFrom).copy(
                splitTiploc = d.tiploc,
                splitTiplocName = d.tiplocName,
                splitToUid = d.toUid,
                splitToHeadcode = d.toHeadcode,
                splitToDestName = d.toDestName,
                splitToDate = d.toDate,
                splitToAll = d.all,
            )
        )
    }
    // 2. It joins another train mid-journey.
    findJoins(uid, ctx)?.let { j ->
        val primary = j.parents.firstOrNull { isPassengerHeadcode(it.headcode) } ?: j.parents.first()
        return svc.copy(
            association = assoc.withForms(forms, formsDate).withSplitFrom(splitFrom).copy(
                couplingTiploc = j.tiploc,
                couplingTiplocName = j.tiplocName,
                coupledFromUid = primary.uid,
                coupledFromHeadcode = primary.headcode,
                couplingAssocType = primary.assocType,
                coupledFromAll = j.parents,
                splitToAll = emptyList(),
            )
        )
    }
    // 3. It was divided from, or joined by, a parent train.
    if (assoc.couplingTiploc == null && parents.isNotEmpty()) {
        val primary = pickParent(parents.filter { it.assocType != "VV" }, myUnits, ctx)
        return svc.copy(
            association = assoc.withForms(forms, formsDate).withSplitFrom(splitFrom).copy(
                couplingTiploc = primary?.tiploc,
                couplingTiplocName = primary?.tiplocName,
                coupledFromUid = primary?.uid,
                coupledFromHeadcode = primary?.headcode,
                couplingAssocType = primary?.assocType,
                coupledFromAll = parents.map { CoupledParent(it.uid, it.headcode, it.assocType) },
                splitToAll = emptyList(),
            )
        )
    }
    // 4. Only what it forms at its terminus.
    return if (forms != null) svc.copy(association = assoc.withForms(forms, formsDate).copy(splitToAll = emptyList())) else svc
}

data class ChainHop(val uid: String, val headcode: String, val date: String)

/** Follows "forms" associations from a service, up to [maxDepth] hops, stopping at a loop or a cancelled service. */
suspend fun walkFormsChain(startUid: String, startDate: String, maxDepth: Int = 5): List<ChainHop> {
    val hops = mutableListOf<ChainHop>()
    val seen = mutableSetOf(startUid)
    var curUid = startUid
    var curDate = startDate
    repeat(maxDepth) {
        val next = withContext(Dispatchers.IO) {
            transaction { resolveNextForms(curUid, buildAssociationContext(setOf(curUid), curDate), curDate) }
        } ?: return hops
        if (next.uid in seen) return hops
        val nextDate = resolveConnectionDate(curDate, next.dateIndicator)
        if (withContext(Dispatchers.IO) { transaction { isUidWhollyCancelled(next.uid, nextDate) } }) return hops
        hops.add(ChainHop(next.uid, next.headcode, nextDate))
        seen.add(next.uid)
        curUid = next.uid
        curDate = nextDate
    }
    return hops
}

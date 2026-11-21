package com.traintracker.server.routes

import com.traintracker.server.database.SCHEDULE_DATE_SCOPE_SQL
import com.traintracker.server.database.effectiveDateFor
import com.traintracker.server.database.placeholders
import com.traintracker.server.database.query
import com.traintracker.server.database.scheduleDateScopeArgs
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction

/*
 * Association clean-up for partly cancelled trains (S513 codes 92, 93, 94, 95).
 * loadCancelledTiplocs reads linx_modifications for a service date; pruneCancelled drops associations
 * that touch a cancelled stop. Used by buildAssociationContext and applyAssociations.
 */

internal fun JdbcTransaction.loadCancelledTiplocs(uids: Set<String>, date: String): Map<String, Set<String>> {
    if (uids.isEmpty()) return emptyMap()
    val uidList = uids.toList()
    val effDate = effectiveDateFor(date)
    val runStops = mutableMapOf<String, MutableList<String>>()
    query(
        "SELECT uid, tiploc, stop_sequence, stp_indicator FROM schedules s " +
            "WHERE uid IN (${placeholders(uidList.size)}) AND stp_indicator != 'C' AND $SCHEDULE_DATE_SCOPE_SQL " +
            "AND stp_indicator = (SELECT stp_indicator FROM schedules s2 WHERE s2.uid = s.uid AND s2.stp_indicator != 'C' AND $SCHEDULE_DATE_SCOPE_SQL " +
            "ORDER BY CASE s2.stp_indicator WHEN 'N' THEN 0 WHEN 'O' THEN 1 WHEN 'P' THEN 2 ELSE 3 END LIMIT 1) " +
            "ORDER BY uid, stop_sequence",
        uidList + scheduleDateScopeArgs(effDate) + scheduleDateScopeArgs(effDate)
    ) { rs ->
        while (rs.next()) {
            val u = rs.getString("uid") ?: continue
            val t = rs.getString("tiploc") ?: continue
            runStops.getOrPut(u) { mutableListOf() }.add(t)
        }
    }
    val cancelled = mutableMapOf<String, MutableSet<String>>()
    val latest94 = mutableMapOf<String, List<String>>()   // a later 94 replaces an earlier one
    query(
        "SELECT uid, code, tiploc, locations FROM linx_modifications " +
            "WHERE uid IN (${placeholders(uidList.size)}) AND code IN ('92','93','94','95') " +
            "AND start_date = ? ORDER BY id",
        uidList + listOf(date)
    ) { rs ->
        while (rs.next()) {
            val u = rs.getString("uid") ?: continue
            val code = rs.getString("code") ?: continue
            val tip = rs.getString("tiploc")
            val locs = rs.getString("locations")
            val stops = runStops[u] ?: continue
            // 95: the 04 entry is the location the train starts from; it is often off-route and not in the schedule.
            val usedLoc = if (code == "95") locs?.split(';')?.mapNotNull { entry ->
                val parts = entry.split('|')
                if (parts.size >= 2 && parts[1] == "04") parts[0].takeIf { it.isNotEmpty() } else null
            }?.firstOrNull() else null
            val usedIdx = usedLoc?.let { stops.indexOf(it) } ?: -1
            val tipIdx = tip?.let { stops.indexOf(it) } ?: -1
            val affected = when (code) {
                "92" -> if (tipIdx >= 0) stops.drop(tipIdx + 1) else emptyList()
                "93" -> if (tipIdx >= 0) stops.drop(tipIdx) else emptyList()
                "94" -> if (tipIdx >= 0) stops.take(tipIdx) else emptyList()
                "95" -> when {
                    usedIdx >= 0 -> stops.take(usedIdx)
                    tipIdx >= 0 -> stops.take(tipIdx + 1)
                    else -> emptyList()
                }
                else -> emptyList()
            }
            if (code == "94") {
                if (tipIdx >= 0) latest94[u] = affected
            } else if (affected.isNotEmpty()) cancelled.getOrPut(u) { mutableSetOf() }.addAll(affected)
        }
    }
    latest94.forEach { (u, a) -> if (a.isNotEmpty()) cancelled.getOrPut(u) { mutableSetOf() }.addAll(a) }
    return cancelled
}

private fun AssociationContext.isCancelled(uid: String?, tiploc: String): Boolean =
    uid != null && cancelledTiplocs[uid]?.contains(tiploc) == true

internal fun AssociationContext.pruneCancelled(svc: ServiceResponse): ServiceResponse {
    if (cancelledTiplocs.isEmpty()) return svc
    val uid = svc.uid
    var a = svc.association
    a.couplingTiploc?.let { ct ->
        if (isCancelled(uid, ct) || (a.coupledFromUid != null && isCancelled(a.coupledFromUid, ct))) {
            a = a.copy(couplingTiploc = null, couplingTiplocName = null, couplingAssocType = null,
                coupledFromUid = null, coupledFromHeadcode = null, coupledFromAll = emptyList())
        }
    }
    a.splitTiploc?.let { st ->
        if (isCancelled(uid, st)) {
            a = a.copy(splitTiploc = null, splitTiplocName = null, splitToUid = null, splitToHeadcode = null,
                splitToDestName = null, splitToDate = null, splitToAll = emptyList())
        }
    }
    if (a.splitToAll.isNotEmpty()) {
        val filtered = a.splitToAll.filter { cand ->
            val mt = cand.matchedTiploc ?: a.splitTiploc
            mt == null || (!isCancelled(uid, mt) && !isCancelled(cand.uid, mt))
        }
        if (filtered.size != a.splitToAll.size) a = a.copy(splitToAll = filtered)
    }
    if (a.coupledFromAll.isNotEmpty()) {
        val ct = a.couplingTiploc
        val filtered = a.coupledFromAll.filter { cand ->
            ct == null || (!isCancelled(uid, ct) && !isCancelled(cand.uid, ct))
        }
        if (filtered.size != a.coupledFromAll.size) a = a.copy(coupledFromAll = filtered)
    }
    a.formsUid?.let { forms ->
        val terminus = svcTermini[uid]
        if (terminus != null && (isCancelled(uid, terminus) || isCancelled(forms, terminus))) {
            a = a.copy(formsUid = null, formsHeadcode = null, formsDate = null)
        }
    }
    return svc.copy(association = a)
}

package com.traintracker.server.routes

import com.traintracker.server.cif.ScheduleCancellations
import com.traintracker.server.ReasonCodes
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.AppDatabase.AllocationLegSpan
import com.traintracker.server.database.Schedules
import com.traintracker.server.database.scheduleDateScope
import io.ktor.server.routing.Route
import com.traintracker.server.cif.CorpusLookup
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.SortOrder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.LocalDateTime

fun Route.serviceDetailRoutes() {
    get("/service/{uid}") {
        val uid = call.parameters["uid"]?.trim()
            ?: return@get call.respond(HttpStatusCode.BadRequest, "uid required")
        val atCrs = call.parameters["crs"]?.uppercase()?.trim() ?: ""
        val svcDate = call.parameters["date"]
            ?.filter { it.isDigit() || it == '-' }
            ?.takeIf { it.length == 10 }
            ?: defaultServiceDate(uid)
        val svcDayOfMonth = svcDate.takeLast(2)
        val trustMapForService = mutableMapOf<String, Triple<String, Boolean, String?>>()
        val trustArrivalMap = mutableMapOf<String, String>()
        val trustDepartureMap = mutableMapOf<String, String>()
        val trustArrivalSecMap = mutableMapOf<String, String>()
        val trustDepartureSecMap = mutableMapOf<String, String>()
        var tdOverlay = TdOverlay.EMPTY
        var timetableCancelled = false
        var serviceHeadcode = ""
        var serviceTocCode = ""
        var tiplocNames: Map<String, String?> = emptyMap()
        val callingPoints = withContext(Dispatchers.IO) {
            transaction {
                Schedules.selectAll().where { (Schedules.uid eq uid) and scheduleDateScope(svcDate) }
                    .orderBy(Schedules.stopSequence, SortOrder.ASC).toList()
                    .let { rows ->
                        val priority = STP_PRIORITY
                        val bestStp = rows.minOfOrNull { priority[it[Schedules.stpIndicator]] ?: 99 }
                        var bestRows = rows.filter { (priority[it[Schedules.stpIndicator]] ?: 99) == bestStp }
                        if (atCrs.isNotEmpty()) {
                            val bestHasAnyLo = bestRows.any { it[Schedules.stopType] == "LO" }
                            val bestHasMatchingLo = bestRows.any { it[Schedules.stopType] == "LO" && it[Schedules.crs] == atCrs }
                            if (bestHasAnyLo && !bestHasMatchingLo) {
                                val altStp = priority.entries.sortedBy { it.value }.firstOrNull { e ->
                                    val k = e.key
                                    k != priority.entries.find { it.value == bestStp }?.key &&
                                        rows.any { it[Schedules.stpIndicator] == k && it[Schedules.stopType] == "LO" && it[Schedules.crs] == atCrs }
                                }
                                if (altStp != null) {
                                    bestRows = rows.filter { it[Schedules.stpIndicator] == altStp.key }
                                }
                            }
                        }
                        val bestSttpChar = priority.entries.find { it.value == (priority[bestRows.firstOrNull()?.get(Schedules.stpIndicator)] ?: 99) }?.key
                        if (bestSttpChar != null && bestSttpChar != 'P') {
                            val hasOwnLo = bestRows.any { it[Schedules.stopType] == "LO" }
                            val hasOwnLt = bestRows.any { it[Schedules.stopType] == "LT" }
                            val hasOwnLi = bestRows.any { it[Schedules.stopType] == "LI" }
                            if (!hasOwnLo && !hasOwnLi && hasOwnLt) {
                                val oLt = bestRows.first { it[Schedules.stopType] == "LT" }
                                val oLtTiploc = oLt[Schedules.tiploc]
                                val pRows = rows.filter { it[Schedules.stpIndicator] == 'P' }.sortedBy { it[Schedules.scheduledTime] }
                                val cutIdx = pRows.indexOfFirst { it[Schedules.tiploc] == oLtTiploc }
                                if (cutIdx >= 0) {
                                    pRows.subList(0, cutIdx) + oLt
                                } else {
                                    pRows.filter { it[Schedules.stopType] != "LT" } + oLt
                                }
                            } else if (!hasOwnLo && !hasOwnLt) {
                                val overriddenTiplocs = bestRows.map { it[Schedules.tiploc] }.toSet()
                                val pRows = rows.filter { it[Schedules.stpIndicator] == 'P' }.sortedBy { it[Schedules.scheduledTime] }
                                val hasOverlap = pRows.any { overriddenTiplocs.contains(it[Schedules.tiploc]) }
                                val allOArePassingPoints = bestRows.all { it[Schedules.crs].isNullOrEmpty() && it[Schedules.isPass] }
                                if (allOArePassingPoints) {
                                    val filteredP = pRows.filter { !overriddenTiplocs.contains(it[Schedules.tiploc]) }
                                    (bestRows + filteredP).sortedBy { it[Schedules.scheduledTime] }
                                } else if (hasOverlap) {
                                    val filteredP = pRows.filter { !overriddenTiplocs.contains(it[Schedules.tiploc]) }
                                    (bestRows + filteredP).sortedBy { it[Schedules.scheduledTime] }
                                } else {
                                    val oLast = bestRows.maxOf { it[Schedules.scheduledTime] }
                                    val pAfterByTime = pRows.filter { it[Schedules.scheduledTime] > oLast }
                                    val pAfterTiplocs = pAfterByTime.map { it[Schedules.tiploc] }.toSet()
                                    val firstPAfterIdx = pRows.indexOfFirst { pAfterTiplocs.contains(it[Schedules.tiploc]) }
                                    val pBeforeCandidate = if (firstPAfterIdx > 0) pRows.subList(0, firstPAfterIdx) else emptyList()
                                    val pBeforeHasStations = pBeforeCandidate.any {
                                        it[Schedules.stopType] == "LI" && !it[Schedules.crs].isNullOrEmpty()
                                    }
                                    val pBefore = if (pBeforeHasStations) {
                                        pBeforeCandidate.filter { it[Schedules.stopType] == "LO" }
                                    } else {
                                        pBeforeCandidate
                                    }
                                    val pAfter = if (firstPAfterIdx >= 0) pRows.subList(firstPAfterIdx, pRows.size) else pAfterByTime
                                    val oCrsCovered = bestRows.mapNotNull { it[Schedules.crs].takeIf { c -> !c.isNullOrEmpty() } }.toSet()
                                    val pBeforeDeduped = pBefore.filter {
                                        it[Schedules.crs].isNullOrEmpty() || !oCrsCovered.contains(it[Schedules.crs])
                                    }
                                    val pAfterDeduped = pAfter.filter {
                                        it[Schedules.crs].isNullOrEmpty() || !oCrsCovered.contains(it[Schedules.crs])
                                    }
                                    pBeforeDeduped + bestRows + pAfterDeduped
                                }
                            } else if (hasOwnLo && !hasOwnLi) {
                                val oLoTime = bestRows.first { it[Schedules.stopType] == "LO" }[Schedules.scheduledTime]
                                val oLtTime = bestRows.firstOrNull { it[Schedules.stopType] == "LT" }?.get(Schedules.scheduledTime)
                                val oOriginTiploc = bestRows.first { it[Schedules.stopType] == "LO" }[Schedules.tiploc]
                                val pSorted = rows.filter { it[Schedules.stpIndicator] == 'P' }.sortedBy { it[Schedules.scheduledTime] }
                                val oOriginIdxInP = pSorted.indexOfFirst { it[Schedules.tiploc] == oOriginTiploc }
                                val pLi = pSorted.filter { row ->
                                    row[Schedules.stopType] == "LI" && row[Schedules.tiploc] != oOriginTiploc &&
                                        (
                                            if (oOriginIdxInP >= 0) {
                                                pSorted.indexOfFirst { it[Schedules.tiploc] == row[Schedules.tiploc] } > oOriginIdxInP
                                            } else {
                                                row[Schedules.scheduledTime] > oLoTime
                                            }
                                            ) &&
                                        (oLtTime == null || row[Schedules.scheduledTime] < oLtTime)
                                }
                                val pLt = if (!hasOwnLt) pSorted.filter { it[Schedules.stopType] == "LT" } else emptyList()
                                (bestRows + pLi + pLt).sortedBy { row ->
                                    if (row[Schedules.tiploc] == oOriginTiploc) {
                                        oOriginIdxInP
                                    } else {
                                        pSorted.indexOfFirst { it[Schedules.tiploc] == row[Schedules.tiploc] }.takeIf { it >= 0 } ?: Int.MAX_VALUE
                                    }
                                }
                            } else {
                                bestRows.sortedBy { it[Schedules.scheduledTime] }
                            }
                        } else {
                            bestRows
                        }
                    }
                    .let { rows ->
                        val destTiploc = rows.firstOrNull { it[Schedules.stopType] == "LT" }?.get(Schedules.tiploc)
                        if (destTiploc != null) {
                            val loT = rows.firstOrNull { it[Schedules.stopType] == "LO" }?.get(Schedules.scheduledTime) ?: "00:00"
                            val ltT = rows.firstOrNull { it[Schedules.stopType] == "LT" }?.get(Schedules.scheduledTime) ?: "23:59"
                            val sorted = if (loT > ltT) {
                                val loMins = loT.replace(":", "").toIntOrNull() ?: 0
                                rows.sortedBy { r ->
                                    val mins = r[Schedules.scheduledTime].replace(":", "").toIntOrNull() ?: 0
                                    if (mins < loMins) mins + 2400 else mins
                                }
                            } else {
                                rows
                            }
                            val ltIndex = sorted.indexOfFirst { it[Schedules.tiploc] == destTiploc && it[Schedules.stopType] == "LT" }
                                .let { if (it < 0) sorted.indexOfFirst { r -> r[Schedules.tiploc] == destTiploc } else it }
                            val deduped = sorted.filterIndexed { i, r ->
                                r[Schedules.tiploc] != destTiploc || r[Schedules.stopType] == "LT" || r[Schedules.stopType] == "LO" || i == ltIndex
                            }
                            val finalIdx = deduped.indexOfFirst { it[Schedules.tiploc] == destTiploc && it[Schedules.stopType] == "LT" }
                                .let { if (it < 0) deduped.indexOfFirst { r -> r[Schedules.tiploc] == destTiploc } else it }
                            if (finalIdx >= 0) deduped.subList(0, finalIdx + 1) else deduped
                        } else {
                            rows.sortedBy { it[Schedules.scheduledTime] }
                        }
                    }
                    .also { rows ->
                        val headcode = rows.firstOrNull()?.get(Schedules.headcode) ?: ""
                        serviceHeadcode = headcode
                        tdOverlay = TdOverlay.load(headcode, svcDate)
                        timetableCancelled = ScheduleCancellations.isCancelled(uid, svcDate)
                        val atocRow = rows.firstOrNull()
                        serviceTocCode = atocRow?.let { CorpusLookup.operatorForTsc(it[Schedules.tsc]) } ?: ""
                        atocRow?.let { CorpusLookup.recordMissingTscIfNoOverride(it[Schedules.tsc], headcode, uid) }
                        fun bindSvc(v: String) = bindStr(v)
                        val safeHc = headcode.filter { it.isLetterOrDigit() }
                        if (safeHc.isNotEmpty()) {
                            val trustCrsAlias = mapOf("PDX" to "PAD", "FDX" to "FOG", "LSX" to "LST", "ABX" to "ABW", "WHX" to "WWC")
                            val cancelOnlyTrainId = run {
                                var tid: String? = null
                                exec(
                                    "SELECT train_id FROM trust_movements WHERE uid=? AND event_ts >= datetime(?, '-12 hours') AND event_ts < datetime(?, '+36 hours') AND train_id LIKE '%' || ? GROUP BY train_id HAVING SUM(CASE WHEN is_cancelled=0 THEN 1 ELSE 0 END)=0 AND SUM(CASE WHEN is_cancelled=1 THEN 1 ELSE 0 END)>0 LIMIT 1",
                                    listOf(bindSvc(uid), bindSvc(svcDate), bindSvc(svcDate), bindSvc(svcDayOfMonth))
                                ) { rs -> if (rs.next()) tid = rs.getString(1) }
                                tid
                            }
                            val trainIdFilter = if (cancelOnlyTrainId != null) "AND train_id=?" else ""
                            val trustArgs = if (cancelOnlyTrainId != null) {
                                listOf(bindSvc(uid), bindSvc(cancelOnlyTrainId), bindSvc(svcDate), bindSvc(svcDate), bindSvc(svcDayOfMonth))
                            } else {
                                listOf(bindSvc(uid), bindSvc(svcDate), bindSvc(svcDate), bindSvc(svcDayOfMonth))
                            }
                            exec("SELECT crs, stanox, scheduled_time, actual_time, event_type, is_cancelled, cancel_reason, event_ts FROM trust_movements WHERE uid=? $trainIdFilter AND event_ts >= datetime(?, '-12 hours') AND event_ts < datetime(?, '+36 hours') AND train_id LIKE '%' || ? ORDER BY event_ts DESC", trustArgs) { rs ->
                                while (rs.next()) {
                                    val scht = rs.getString("scheduled_time") ?: continue
                                    val isCancelled = rs.getBoolean("is_cancelled")
                                    val cancelReason = ReasonCodes.resolve(rs.getString("cancel_reason"))
                                    val eventType = rs.getString("event_type") ?: ""
                                    val trustActualT = rs.getString("actual_time") ?: ""
                                    val tdResult = tdOverlay.resolve(rs.getString("stanox") ?: "", eventType, rs.getString("event_ts"))
                                    val actualT = tdResult?.minute ?: trustActualT
                                    val actualSec = tdResult?.seconds ?: ""
                                    val utcScht = scht
                                    val rawCrs = rs.getString("crs")?.takeIf { it.isNotEmpty() }
                                        ?: CorpusLookup.crsFromStanox(rs.getString("stanox") ?: "")
                                    val crs = rawCrs?.let { trustCrsAlias[it] ?: it }
                                    for (key in listOfNotNull(utcScht, scht.takeIf { it != utcScht }, crs?.let { "$it|$utcScht" }, crs?.let { "$it|$scht" })) {
                                        val existing = trustMapForService[key]
                                        if (existing == null) {
                                            trustMapForService[key] = Triple(actualT, isCancelled, cancelReason)
                                        } else {
                                            val mergedActual = if (actualT.isNotEmpty()) actualT else existing.first
                                            val mergedCancelled = existing.second || isCancelled
                                            val mergedReason = cancelReason ?: existing.third
                                            trustMapForService[key] = Triple(mergedActual, mergedCancelled, mergedReason)
                                        }
                                        if (actualT.isNotEmpty()) {
                                            when (eventType) {
                                                "ARRIVAL" -> if (trustArrivalMap.putIfAbsent(key, actualT) == null && actualSec.isNotEmpty()) {
                                                    trustArrivalSecMap[key] = actualSec
                                                }
                                                "DEPARTURE" -> if (trustDepartureMap.putIfAbsent(key, actualT) == null && actualSec.isNotEmpty()) {
                                                    trustDepartureSecMap[key] = actualSec
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    .also { rows ->
                        val rowTiplocs = rows.map { it[Schedules.tiploc] }.toSet()
                        tiplocNames = CorpusLookup.namesFromTiplocs(rowTiplocs)
                        CorpusLookup.recordMissingFromNames(rowTiplocs, serviceHeadcode, uid)
                    }
                    .let { rows ->
                        val dbPlatforms = AppDatabase.getDarwinPlatformsForUid(uid, rows.map { it[Schedules.tiploc] }.toSet())
                        val trustPlatforms = AppDatabase.getTrustPlatforms(listOf(uid))
                        rows.map { row ->
                            val tiploc = row[Schedules.tiploc]
                            val crs = row[Schedules.crs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(tiploc)
                            val scht = row[Schedules.scheduledTime]
                            val trustEntry = trustMapForService[scht] ?: if (crs != null) trustMapForService["$crs|$scht"] else null
                            val schedArrKey = row[Schedules.scheduledArrival]
                            val schedDepKey = row[Schedules.scheduledDeparture]
                            val actArr = schedArrKey?.let { a -> trustArrivalMap[a] ?: crs?.let { trustArrivalMap["$it|$a"] } } ?: ""
                            val actDep = schedDepKey?.let { d -> trustDepartureMap[d] ?: crs?.let { trustDepartureMap["$it|$d"] } }
                                ?: trustDepartureMap[scht] ?: crs?.let { trustDepartureMap["$crs|$scht"] } ?: ""
                            val actArrSec = schedArrKey?.let { a -> trustArrivalSecMap[a] ?: crs?.let { trustArrivalSecMap["$it|$a"] } } ?: ""
                            val actDepSec = schedDepKey?.let { d -> trustDepartureSecMap[d] ?: crs?.let { trustDepartureSecMap["$it|$d"] } }
                                ?: trustDepartureSecMap[scht] ?: crs?.let { trustDepartureSecMap["$crs|$scht"] } ?: ""
                            val ttCancelled = timetableCancelled && trustArrivalMap.isEmpty() && trustDepartureMap.isEmpty()
                            val (plt_, pltC_) = resolvePlatformWithConfirmed(uid, tiploc, row[Schedules.platform], dbPlatforms, trustPlatforms, crs)
                            CallingPointResponse(
                                tiploc,
                                crs,
                                tiplocNames[tiploc],
                                row[Schedules.scheduledTime],
                                row[Schedules.scheduledArrival],
                                row[Schedules.scheduledDeparture],
                                plt_,
                                pltC_,
                                row[Schedules.platform],
                                row[Schedules.isPass],
                                row[Schedules.stopType],
                                trustEntry?.first ?: "",
                                actArr,
                                actDep,
                                actArrSec,
                                actDepSec,
                                (trustEntry?.second ?: false) || ttCancelled,
                                trustEntry?.third ?: if (ttCancelled) "Cancelled in the timetable" else null
                            )
                        }
                    }
                    .let { rawCps ->
                        val loCancel = rawCps.firstOrNull { it.stopType == "LO" }
                            ?.takeIf { it.isCancelled }
                        val serviceNeverRan = loCancel != null && rawCps.none { it.actualTime.isNotEmpty() } &&
                            rawCps.none { !it.isCancelled && it.stopType != "LO" }
                        val propagatedCps = if (serviceNeverRan) {
                            rawCps.map { cp ->
                                if (!cp.isCancelled) cp.copy(isCancelled = true, cancelReason = loCancel!!.cancelReason) else cp
                            }
                        } else {
                            rawCps
                        }
                        val firstCancelIdx = propagatedCps.indexOfFirst { it.isCancelled && it.stopType != "LT" }
                        val hasTrustCancelAfterFirst = firstCancelIdx >= 0 && propagatedCps.drop(firstCancelIdx + 1).any { it.isCancelled }
                        val hasActualBeforeFirstCancel = firstCancelIdx > 0 && propagatedCps.take(firstCancelIdx).any { it.actualTime.isNotEmpty() }
                        val backPropCps = if (hasTrustCancelAfterFirst && firstCancelIdx > 0 && !hasActualBeforeFirstCancel) {
                            val cancelReason = propagatedCps[firstCancelIdx].cancelReason
                            propagatedCps.mapIndexed { idx, cp ->
                                if (idx < firstCancelIdx && !cp.isCancelled) cp.copy(isCancelled = true, cancelReason = cancelReason) else cp
                            }
                        } else {
                            propagatedCps
                        }
                        data class SwState(val cps: List<CallingPointResponse>, val cancelled: Boolean, val reason: String?)
                        val propagatedCps2 = if (hasTrustCancelAfterFirst) {
                            backPropCps
                        } else {
                            propagatedCps.fold(SwState(emptyList(), false, null)) { state, cp ->
                                if (cp.isCancelled && cp.stopType != "LT") {
                                    state.copy(state.cps + cp, true, cp.cancelReason)
                                } else if (state.cancelled && !cp.isCancelled && cp.actualTime.isEmpty()) {
                                    state.copy(cps = state.cps + cp.copy(isCancelled = true, cancelReason = state.reason))
                                } else {
                                    state.copy(cps = state.cps + cp)
                                }
                            }.cps
                        }
                        val trustConfirmedTimes = trustMapForService.keys.toSet()
                        var lastDelayMins = 0
                        var lastReasonCode: String? = null
                        propagatedCps2.map { cp ->
                            val isTrustConfirmed = cp.actualTime.isNotEmpty() && trustConfirmedTimes.contains(cp.effectiveTime())
                            if (isTrustConfirmed) {
                                val sch = cp.effectiveTime().toMinutes()
                                val act = cp.actualTime.toMinutes()
                                var diff = act - sch
                                if (diff < -720) {
                                    diff += 1440
                                } else if (diff > 720) {
                                    diff -= 1440
                                }
                                if (diff >= 0) {
                                    lastDelayMins = diff
                                    if (cp.cancelReason != null) lastReasonCode = cp.cancelReason
                                }
                                if (diff == 0 && lastDelayMins > 0 && cp.crs == null) {
                                    val projMins = (sch + lastDelayMins + 1440) % 1440
                                    cp.copy(
                                        actualTime = "%02d:%02d".format(projMins / 60, projMins % 60),
                                        cancelReason = lastReasonCode,
                                        isPredicted = true
                                    )
                                } else {
                                    cp
                                }
                            } else if (cp.actualTime.isEmpty() && lastDelayMins > 0 && !cp.isCancelled) {
                                val projMins = (cp.effectiveTime().toMinutes() + lastDelayMins + 1440) % 1440
                                cp.copy(
                                    actualTime = "%02d:%02d".format(projMins / 60, projMins % 60),
                                    cancelReason = lastReasonCode,
                                    isPredicted = true
                                )
                            } else {
                                cp
                            }
                        }
                    }
                    .let { cps ->
                        val loTime = cps.firstOrNull { it.stopType == "LO" }?.effectiveTime() ?: "00:00"
                        val ltTime = cps.firstOrNull { it.stopType == "LT" }?.effectiveTime() ?: "23:59"
                        if (loTime > ltTime) {
                            cps.sortedBy { cp ->
                                val mins = cp.effectiveTime().replace(":", "").toIntOrNull() ?: 0
                                val loMins = loTime.replace(":", "").toIntOrNull() ?: 0
                                if (mins < loMins) mins + 2400 else mins
                            }
                        } else {
                            cps
                        }
                    }
                    .let { cps -> RoutePoints.merge(cps, uid, svcDate, serviceHeadcode) }
            }
        }
        val activationOriginCrs = withContext(Dispatchers.IO) {
            transaction {
                activationsFor(listOf(uid), svcDate)[uid]?.originStanox?.let { CorpusLookup.crsFromStanox(it) }
            }
        }
        val finalCps = if (activationOriginCrs != null) {
            val trimIdx = callingPoints.indexOfFirst { it.crs == activationOriginCrs }
            val terminusCrs = callingPoints.lastOrNull { it.stopType == "LT" }?.crs
            if (trimIdx > 0 && activationOriginCrs != terminusCrs) callingPoints.subList(trimIdx, callingPoints.size) else callingPoints
        } else {
            callingPoints
        }
        if (finalCps.isEmpty()) {
            call.respond(HttpStatusCode.NotFound, "Service $uid not found")
            return@get
        }
        val annotatedCps = finalCps
        val atIndex = annotatedCps.indexOfFirst { it.crs == atCrs }.let {
            if (it < 0 && atCrs.isNotEmpty()) {
                annotatedCps.indexOfFirst { cp -> cp.tiploc.startsWith(atCrs) }
            } else if (it < 0) {
                0
            } else {
                it
            }
        }
        val previous = if (atCrs.isNotEmpty() && atIndex > 0) annotatedCps.subList(0, atIndex) else emptyList()
        val subsequent = if (atCrs.isEmpty()) {
            annotatedCps
        } else if (atIndex >= 0) {
            annotatedCps.subList(atIndex, annotatedCps.size)
        } else {
            annotatedCps
        }
        val headcode = serviceHeadcode
        val serviceType = when {
            headcode.startsWith("0B") -> "BUS_REPLACEMENT"
            headcode.startsWith("0C") -> "COACH_REPLACEMENT"
            else -> "NORMAL"
        }
        val allCpsForStatus = previous + subsequent
        val isCancelled = allCpsForStatus.isNotEmpty() && allCpsForStatus.all { it.isCancelled }
        val isPartiallyCancelled = allCpsForStatus.any { it.isCancelled } && allCpsForStatus.any { !it.isCancelled }
        val topCancelReason = allCpsForStatus.firstOrNull { it.isCancelled && it.cancelReason != null }?.cancelReason
        val assocCtx = withContext(Dispatchers.IO) {
            transaction { buildAssociationContext(setOf(uid), svcDate) }
        }
        val svcStub = ServiceResponse(
            uid = uid,
            headcode = headcode,
            atocCode = serviceTocCode,
            scheduledTime = annotatedCps.firstOrNull { it.stopType == "LO" }?.effectiveTime() ?: "",
            platform = null,
            isPass = false,
            stopType = "",
            originTiploc = "",
            destTiploc = "",
            originCrs = null,
            destCrs = null
        )
        var annotatedSvc = applyAssociations(svcStub, assocCtx, svcDate)
        if (annotatedSvc.association.formsUid != null &&
            withContext(Dispatchers.IO) { transaction { isUidWhollyCancelled(annotatedSvc.association.formsUid!!, svcDate) } }
        ) {
            annotatedSvc = annotatedSvc.copy(
                association = annotatedSvc.association.copy(formsUid = null, formsHeadcode = null, formsDate = null)
            )
        }
        if (annotatedSvc.association.coupledFromUid != null &&
            withContext(Dispatchers.IO) { transaction { isUidWhollyCancelled(annotatedSvc.association.coupledFromUid!!, svcDate) } }
        ) {
            annotatedSvc = annotatedSvc.copy(
                association = annotatedSvc.association.copy(
                    couplingTiploc = null,
                    couplingTiplocName = null,
                    couplingAssocType = null,
                    coupledFromUid = null,
                    coupledFromHeadcode = null
                )
            )
        }
        if (annotatedSvc.association.formsUid != null) {
            val chain = walkFormsChain(annotatedSvc.association.formsUid!!, annotatedSvc.association.formsDate ?: svcDate)
            chain.firstOrNull()?.let { h ->
                annotatedSvc = annotatedSvc.copy(
                    association = annotatedSvc.association.copy(formsNextUid = h.uid, formsNextHeadcode = h.headcode, formsNextDate = h.date)
                )
            }
            if (!isPassengerHeadcode(annotatedSvc.association.formsHeadcode ?: "")) {
                chain.firstOrNull { isPassengerHeadcode(it.headcode) }?.let { h ->
                    annotatedSvc = annotatedSvc.copy(
                        association = annotatedSvc.association.copy(
                            formsNextPassengerUid = h.uid,
                            formsNextPassengerHeadcode = h.headcode,
                            formsNextPassengerDate = h.date
                        )
                    )
                }
            }
        }
        val inferredCands = withContext(Dispatchers.IO) {
            transaction {
                detectUnitBasedSplits(uid, svcDate).filter { ic ->
                    val leaving = ic.matchedTiploc?.let { unitsDividingAt(uid, svcDate, it) }
                    if (leaving == null) true else (lookupUnits(ic.uid, svcDate) ?: "").split(",").map { it.trim() }.any { leaving.contains(it) }
                }
            }
        }
        val mergedCands = annotatedSvc.association.splitToAll +
            inferredCands.filter { ic -> annotatedSvc.association.splitToAll.none { it.uid == ic.uid } }
        if (mergedCands.isNotEmpty()) {
            val enriched = withContext(Dispatchers.IO) {
                transaction {
                    mergedCands.map { sc ->
                        val (ju, jh) = lookupJoinsInto(sc.uid, svcDate)
                        val u = lookupUnits(sc.uid, svcDate)
                        val cc = lookupCoachCount(sc.uid, svcDate)
                        sc.copy(joinsUid = ju, joinsHeadcode = jh, units = u, coachCount = cc)
                    }
                }
            }
            val matchedTiploc = inferredCands.firstOrNull()?.matchedTiploc
            annotatedSvc = annotatedSvc.copy(
                association = annotatedSvc.association.copy(
                    splitToAll = enriched,
                    splitTiploc = annotatedSvc.association.splitTiploc ?: matchedTiploc,
                    splitTiplocName = annotatedSvc.association.splitTiplocName ?: matchedTiploc?.let { tiplocNames[it] }
                )
            )
        }
        if (annotatedSvc.association.splitFromUid == null) {
            val inferredParent = withContext(Dispatchers.IO) {
                transaction { detectUnitBasedSplitParent(uid, svcDate) }
            }
            if (inferredParent != null) {
                annotatedSvc = annotatedSvc.copy(
                    association = annotatedSvc.association.copy(splitFromUid = inferredParent.uid, splitFromHeadcode = inferredParent.headcode)
                )
            }
        }
        val svcOriginTiploc = annotatedCps.firstOrNull { it.stopType == "LO" }?.tiploc
        val svcDestTiploc = annotatedCps.firstOrNull { it.stopType == "LT" }?.tiploc
        val backwardExplained = annotatedSvc.association.splitFromUid != null ||
            (annotatedSvc.association.couplingTiploc != null && annotatedSvc.association.couplingTiploc == svcOriginTiploc)
        val forwardExplained = annotatedSvc.association.formsUid != null ||
            (annotatedSvc.association.couplingTiploc != null && annotatedSvc.association.couplingTiploc == svcDestTiploc)
        val prevWorkingAllRaw = if (!backwardExplained) {
            withContext(Dispatchers.IO) { AppDatabase.getStockNeighboursBackwardAll(uid, svcDate) }
        } else {
            emptyList()
        }
        val nextWorkingAllRaw = if (!forwardExplained) {
            withContext(Dispatchers.IO) { AppDatabase.getStockNeighboursForwardAll(uid, svcDate) }
        } else {
            emptyList()
        }
        val legData = withContext(Dispatchers.IO) {
            transaction { AppDatabase.getLegData(uid, svcDate) }
        }
        val allocationLegs = legData.spans
        val allTiplocsInOrder = (previous + subsequent).map { it.tiploc }
        val activation = withContext(Dispatchers.IO) {
            transaction { activationsFor(listOf(uid), svcDate)[uid] }
        }
        val activatedAt = activation?.updatedAt?.let { toIsoUtc(it) }
        val activationOriginStanox = activation?.originStanox
        val activationOrigin = activationOriginStanox?.let { CorpusLookup.tiplocFromStanox(it) }
        val previousWithLength = computeLegLength(previous, allocationLegs, allTiplocsInOrder)
        val subsequentWithLength = computeLegLength(subsequent, allocationLegs, allTiplocsInOrder)
        val allCpsForNames = previous + subsequent
        val unitLegs = legData.groups.map { g ->
            UnitLegResponse(
                g.fromTiploc,
                g.toTiploc,
                allCpsForNames.firstOrNull { it.tiploc == g.fromTiploc }?.name,
                allCpsForNames.firstOrNull { it.tiploc == g.toTiploc }?.name,
                g.fromTime,
                g.toTime,
                g.units,
                g.coachCount,
                g.vehicles
            )
        }
        call.respond(
            ServiceDetailResponse(
                uid = uid,
                atCrs = atCrs,
                headcode = headcode,
                atocCode = serviceTocCode,
                previous = previousWithLength,
                subsequent = subsequentWithLength,
                serviceType = serviceType,
                isCancelled = isCancelled,
                isPartiallyCancelled = isPartiallyCancelled,
                cancelReason = topCancelReason,
                association = annotatedSvc.association,
                prevWorkingUid = if (!backwardExplained) prevWorkingAllRaw.firstOrNull()?.first else null,
                prevWorkingHeadcode = if (!backwardExplained) prevWorkingAllRaw.firstOrNull()?.second else null,
                prevWorkingDate = if (!backwardExplained) prevWorkingAllRaw.firstOrNull()?.third else null,
                nextWorkingUid = if (!forwardExplained) nextWorkingAllRaw.firstOrNull()?.first else null,
                nextWorkingHeadcode = if (!forwardExplained) nextWorkingAllRaw.firstOrNull()?.second else null,
                nextWorkingDate = if (!forwardExplained) nextWorkingAllRaw.firstOrNull()?.third else null,
                nextWorkingAll = nextWorkingAllRaw.map { StockNeighbourCandidate(it.first, it.second, it.third) },
                activatedAt = activatedAt,
                activationOrigin = activationOrigin,
                legs = unitLegs,
                serviceDate = svcDate
            )
        )
    }
}

private fun computeLegLength(
    cps: List<CallingPointResponse>,
    allocationLegs: List<AllocationLegSpan>,
    allTiplocsInOrder: List<String>
): List<CallingPointResponse> {
    val minOriginIdx = allocationLegs.mapNotNull { leg ->
        leg.originTiploc?.let { allTiplocsInOrder.indexOf(it) }?.takeIf { it >= 0 }
    }.minOrNull()
    if (allocationLegs.isEmpty()) return cps.map { it.copy(length = null) }
    return cps.map { cp ->
        val idx = allTiplocsInOrder.indexOf(cp.tiploc)
        if (idx < 0) {
            cp.copy(length = null)
        } else {
            var total = 0
            var matchedAny = false
            for (leg in allocationLegs) {
                val originIdx = leg.originTiploc?.let { allTiplocsInOrder.indexOf(it) } ?: -1
                val destIdx = leg.destTiploc?.let { allTiplocsInOrder.indexOf(it) } ?: -1
                if (originIdx < 0 || destIdx < 0) continue
                val originMatches = originIdx < idx || (originIdx == idx && idx == minOriginIdx)
                if (originMatches && idx <= destIdx) {
                    total += leg.vehicleCount
                    matchedAny = true
                }
            }
            cp.copy(length = if (matchedAny) total else null)
        }
    }
}

private suspend fun defaultServiceDate(uid: String): String = withContext(Dispatchers.IO) {
    val now = LocalDateTime.now()
    val today = now.toLocalDate().toString()
    val yesterday = now.toLocalDate().minusDays(1).toString()
    val nowMins = now.hour * 60 + now.minute
    transaction {
        val rows = Schedules.selectAll().where { (Schedules.uid eq uid) and scheduleDateScope(yesterday) }.toList()
        val loMax = rows.filter { it[Schedules.stopType] == "LO" }.maxOfOrNull { it[Schedules.scheduledTime] }
        val ltMax = rows.filter { it[Schedules.stopType] == "LT" }.maxOfOrNull { it[Schedules.scheduledTime] }
        if (loMax != null && ltMax != null && ltMax < loMax && nowMins <= ltMax.toMinutes() + 120) yesterday else today
    }
}

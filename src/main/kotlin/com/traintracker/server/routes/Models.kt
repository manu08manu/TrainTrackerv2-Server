package com.traintracker.server.routes

import com.traintracker.server.cif.CorpusLookup
import kotlinx.serialization.Serializable

@Serializable
data class StatusResponse(
    val cifLastDownload: String?,
    val trustConnected: Boolean,
    val trainLocationsCount: Int,
    val scheduleCount: Int = 0,
    val uniqueServiceCount: Int = 0,
    val trustMovementCount: Int = 0,
    val trustMovementCount24h: Int = 0,
    val allocationsToday: Int = 0,
    val hspUnitCount: Int = 0,
    val tiplocNameCount: Int = 0,
    val cifChangePendingCount: Int = 0,
    val missingTiplocCount: Int = 0,
    val missingTscCount: Int = 0,
    val feedbackCount: Int = 0
)

@Serializable
data class TrainLocationResponse(
    val headcode: String,
    val rid: String,
    val stationName: String,
    val crs: String?,
    val actualTime: String,
    val eventType: String,
    val delayMinutes: Int,
    val ageSeconds: Long
)

@Serializable
data class MovementResponse(
    val crs: String,
    val eventType: String,
    val scheduledTime: String,
    val actualTime: String,
    val platform: String,
    val isCancelled: Boolean
)

@Serializable
data class MovementsResponse(
    val headcode: String,
    val movements: List<MovementResponse>
)

@Serializable
data class ServiceResponse(
    val uid: String,
    val headcode: String,
    val atocCode: String,
    val scheduledTime: String,
    val scheduledArrival: String? = null,
    val scheduledDeparture: String? = null,
    val platform: String?,
    val platformConfirmed: Boolean = false,
    val bookedPlatform: String? = null,
    val isPass: Boolean,
    val stopType: String,
    val originTiploc: String,
    val destTiploc: String,
    val originCrs: String?,
    val destCrs: String?,
    val actualTime: String = "",
    val isCancelled: Boolean = false,
    val cancelReason: String? = null,
    val units: List<String> = emptyList(),
    val vehicles: List<String> = emptyList(),
    val unitJoinTiploc: String? = null,
    val association: AssociationInfo = AssociationInfo(),
    val destName: String? = null,
    val originName: String? = null,
    val isPredicted: Boolean = false,
    val link: String? = null,
    val serviceDate: String = ""
)

@Serializable
data class BoardResponse(
    val crs: String,
    val boardType: String,
    val windowStart: String,
    val windowEnd: String,
    val services: List<ServiceResponse>
)

@Serializable
data class BoardDateRangeResponse(
    val minDate: String,
    val maxDate: String
)

@Serializable
data class CallingPointResponse(
    val tiploc: String,
    val crs: String?,
    val name: String?,
    val scheduledTime: String,
    val scheduledArrival: String? = null,
    val scheduledDeparture: String? = null,
    val platform: String?,
    val platformConfirmed: Boolean = false,
    val bookedPlatform: String? = null,
    val isPass: Boolean,
    val stopType: String,
    val actualTime: String = "",
    val actualArrival: String = "",
    val actualDeparture: String = "",
    val actualArrivalSec: String = "",
    val actualDepartureSec: String = "",
    val isCancelled: Boolean = false,
    val cancelReason: String? = null,
    val isPredicted: Boolean = false,
    val length: Int? = null,
    val isTdPoint: Boolean = false
)

@Serializable
data class CoupledParent(
    val uid: String,
    val headcode: String,
    val assocType: String? = null
)

@Serializable
data class AssociationInfo(
    val couplingTiploc: String? = null,
    val couplingTiplocName: String? = null,
    val couplingAssocType: String? = null,
    val coupledFromUid: String? = null,
    val coupledFromHeadcode: String? = null,
    val coupledFromAll: List<CoupledParent> = emptyList(),
    val formsUid: String? = null,
    val formsHeadcode: String? = null,
    val formsDate: String? = null,
    val formsNextUid: String? = null,
    val formsNextHeadcode: String? = null,
    val formsNextDate: String? = null,
    val formsNextPassengerUid: String? = null,
    val formsNextPassengerHeadcode: String? = null,
    val formsNextPassengerDate: String? = null,
    val splitTiploc: String? = null,
    val splitTiplocName: String? = null,
    val splitToUid: String? = null,
    val splitToHeadcode: String? = null,
    val splitToDestName: String? = null,
    val splitToDate: String? = null,
    val splitToAll: List<SplitCandidate> = emptyList(),
    val splitFromUid: String? = null,
    val splitFromHeadcode: String? = null,
    val splitFromDate: String? = null
)

@Serializable
data class ServiceDetailResponse(
    val uid: String,
    val headcode: String? = null,
    val atocCode: String = "",
    val atCrs: String,
    val previous: List<CallingPointResponse>,
    val subsequent: List<CallingPointResponse>,
    val serviceType: String = "NORMAL",
    val isCancelled: Boolean = false,
    val isPartiallyCancelled: Boolean = false,
    val cancelReason: String? = null,
    val association: AssociationInfo = AssociationInfo(),
    val prevWorkingUid: String? = null,
    val prevWorkingHeadcode: String? = null,
    val prevWorkingDate: String? = null,
    val nextWorkingUid: String? = null,
    val nextWorkingHeadcode: String? = null,
    val nextWorkingDate: String? = null,
    val nextWorkingAll: List<StockNeighbourCandidate> = emptyList(),
    val activatedAt: String? = null,
    val activationOrigin: String? = null,
    val legs: List<UnitLegResponse> = emptyList(),
    val serviceDate: String? = null
)

@Serializable
data class AllocationResponse(
    val coreId: String,
    val headcode: String,
    val serviceDate: String,
    val operator: String,
    val units: List<String>,
    val vehicles: List<String>,
    val unitCount: Int,
    val coachCount: Int,
    val reportedAt: String = ""
)

@Serializable
data class AllocationChangeResponse(
    val changedAt: String,
    val headcode: String,
    val oldUnits: List<String>,
    val newUnits: List<String>,
    val oldCoaches: Int,
    val newCoaches: Int
)

@Serializable
data class UnitLegResponse(
    val fromTiploc: String? = null,
    val toTiploc: String? = null,
    val fromName: String? = null,
    val toName: String? = null,
    val fromTime: String? = null,
    val toTime: String? = null,
    val units: List<String>,
    val coachCount: Int,
    val vehicles: List<String> = emptyList()
)

fun List<ServiceResponse>.withResolvedNames(): List<ServiceResponse> {
    val tiplocs = flatMap { listOf(it.originTiploc, it.destTiploc) }.toSet()
    val names = CorpusLookup.namesFromTiplocs(tiplocs)
    return map { it.copy(originName = names[it.originTiploc], destName = names[it.destTiploc]) }
}

fun CallingPointResponse.effectiveTime(): String = scheduledDeparture ?: scheduledArrival ?: scheduledTime

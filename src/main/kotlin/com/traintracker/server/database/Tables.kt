package com.traintracker.server.database

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.datetime

// Tables managed through Exposed. Tables only used with raw SQL are created in Schema.kt.

object Schedules : Table("schedules") {
    val uid = varchar("uid", 6)
    val headcode = varchar("headcode", 4)
    val tsc = varchar("tsc", 8).default("")
    val stpIndicator = char("stp_indicator")
    val tiploc = varchar("tiploc", 7)
    val crs = varchar("crs", 3).nullable()
    val scheduledTime = varchar("scheduled_time", 5)
    val scheduledArrival = varchar("scheduled_arrival", 5).nullable()
    val scheduledDeparture = varchar("scheduled_departure", 5).nullable()
    /** Working-timetable arrival minute, for matching LINX/TD only; the API keeps showing public times. */
    val wttArrival = varchar("wtt_arrival", 5).nullable()
    val wttDeparture = varchar("wtt_departure", 5).nullable()
    val platform = varchar("platform", 3).nullable()
    val isPass = bool("is_pass")
    val stopType = varchar("stop_type", 2)
    val originTiploc = varchar("origin_tiploc", 7)
    val destTiploc = varchar("dest_tiploc", 7)
    val originCrs = varchar("origin_crs", 3).nullable()
    val destCrs = varchar("dest_crs", 3).nullable()
    val scheduleStartDate = varchar("schedule_start_date", 10).default("")
    val scheduleEndDate = varchar("schedule_end_date", 10).default("")
    val scheduleDate = varchar("schedule_date", 10).default("")
    val daysRun = varchar("days_run", 7).default("1111111")
    val stopSequence = integer("stop_sequence").default(0)

    override val primaryKey = PrimaryKey(uid, tiploc, stopType, scheduleStartDate, name = "pk_schedules")

    init {
        index("idx_sched_uid_stp", false, uid, stpIndicator)
        index("idx_sched_crs_time", false, crs, scheduledTime)
        index("idx_sched_headcode", false, headcode, stopType)
    }
}

object Movements : Table("movements") {
    val headcode = varchar("headcode", 4)
    val trainId = varchar("train_id", 10)
    val uid = varchar("uid", 6).default("")
    val stanox = varchar("stanox", 6)
    val crs = varchar("crs", 3).nullable()
    val eventType = varchar("event_type", 15)
    val scheduledTime = varchar("scheduled_time", 5).nullable()
    val actualTime = varchar("actual_time", 5).nullable()
    val platform = varchar("platform", 3).nullable()
    val isCancelled = bool("is_cancelled")
    val cancelReason = varchar("cancel_reason", 4).nullable()
    val eventTs = datetime("event_ts")

    override val primaryKey = PrimaryKey(trainId, stanox, eventType, name = "pk_trust_movements")

    init {
        index("idx_trust_hc", false, headcode)
        index("idx_trust_ts", false, eventTs)
        index("idx_trust_crs_time", false, crs, scheduledTime)
        index("idx_trust_uid", false, uid, eventTs)
    }
}

object TrainLocations : Table("train_locations") {
    val headcode = varchar("headcode", 4)
    val rid = varchar("rid", 50).default("")
    val stationName = varchar("station_name", 100)
    val crs = varchar("crs", 3).nullable()
    val actualTime = varchar("actual_time", 5)
    val eventType = varchar("event_type", 12)
    val delayMinutes = integer("delay_minutes").default(0)
    val updatedAt = datetime("updated_at")

    override val primaryKey = PrimaryKey(headcode, rid, name = "pk_train_locations")
}

object CifMeta : Table("cif_meta") {
    val key = varchar("key", 64)
    val value = varchar("value", 256)

    override val primaryKey = PrimaryKey(key, name = "pk_cif_meta")
}

object HeadcodeAliases : Table("headcode_aliases") {
    val oldHeadcode = varchar("old_headcode", 4)
    val newHeadcode = varchar("new_headcode", 4)
    val uid = varchar("uid", 6).default("")
    val updatedAt = datetime("updated_at")

    override val primaryKey = PrimaryKey(oldHeadcode, name = "pk_headcode_aliases")
}

/** The STANOX maps learnt from the feeds, kept across restarts (keys "stanox_map" and "stanox_tiploc_map"). */
object SavedMaps : Table("saved_maps") {
    val key = varchar("cache_key", 200)
    val json = text("response_json")
    val savedAt = text("cached_at")

    override val primaryKey = PrimaryKey(key, name = "pk_hsp_cache")
}

object TrainActivations : Table("train_activations") {
    val trainId = varchar("train_id", 10)
    val uid = varchar("uid", 10)
    val updatedAt = varchar("updated_at", 20)
    val originStanox = varchar("origin_stanox", 10).nullable()

    override val primaryKey = PrimaryKey(trainId)

    init {
        index("idx_activations_uid", false, uid)
    }
}

object DarwinPlatforms : Table("darwin_platforms") {
    val uid = varchar("uid", 10)
    val rid = varchar("rid", 20)
    val tiploc = varchar("tiploc", 10)
    val platform = varchar("platform", 10)
    val updatedAt = varchar("updated_at", 20)

    override val primaryKey = PrimaryKey(uid, tiploc)

    init {
        index("idx_darwin_platforms_updated", false, updatedAt)
    }
}

object CifAssociations : Table("cif_associations") {
    val mainUid = varchar("main_uid", 6)
    val assocUid = varchar("assoc_uid", 6)
    val assocTiploc = varchar("assoc_tiploc", 7)
    val assocType = varchar("assoc_type", 2)
    val stpInd = char("stp_ind")
    val dateIndicator = char("date_indicator").nullable()

    override val primaryKey = PrimaryKey(mainUid, assocUid, assocTiploc, name = "pk_cif_associations")
}

object AllocationConsists : Table("allocation_consists") {
    val coreId = varchar("core_id", 20)
    val headcode = varchar("headcode", 6)
    val serviceDate = varchar("service_date", 10)
    val operator = varchar("operator", 10)
    val vehicles = varchar("vehicles", 2000)
    val units = varchar("units", 500).default("")
    val unitCount = integer("unit_count").default(0)
    val updatedAt = datetime("updated_at")
    val uid = varchar("uid", 10).nullable()

    override val primaryKey = PrimaryKey(coreId, name = "pk_allocation_consists")

    init {
        index("idx_alloc_hc_date", false, headcode, serviceDate)
        index("idx_alloc_consists_uid_date", false, uid, serviceDate)
    }
}

object AllocationHistory : Table("allocation_history") {
    val coreId = varchar("core_id", 20)
    val headcode = varchar("headcode", 6)
    val serviceDate = varchar("service_date", 10)
    val operator = varchar("operator", 10)
    val vehicles = varchar("vehicles", 2000)
    val units = varchar("units", 500).default("")
    val unitCount = integer("unit_count").default(0)
    val recordedAt = datetime("recorded_at")
    val uid = varchar("uid", 10).nullable()

    override val primaryKey = PrimaryKey(coreId, serviceDate, name = "pk_allocation_history")

    init {
        index("idx_alloc_hist_core_date", false, coreId, serviceDate)
        index("idx_alloc_hist_date", false, serviceDate)
        index("idx_alloc_hist_uid_date", false, uid, serviceDate)
    }
}

object AllocationChanges : Table("allocation_changes") {
    val id = integer("id").autoIncrement()
    val coreId = varchar("core_id", 20)
    val headcode = varchar("headcode", 6)
    val serviceDate = varchar("service_date", 10)
    val uid = varchar("uid", 10).nullable()
    val changedAt = datetime("changed_at")
    val oldUnits = varchar("old_units", 500).default("")
    val newUnits = varchar("new_units", 500).default("")
    val oldCoaches = integer("old_coaches").default(0)
    val newCoaches = integer("new_coaches").default(0)

    override val primaryKey = PrimaryKey(id, name = "pk_allocation_changes")

    init {
        index("idx_alloc_changes_uid_date", false, uid, serviceDate)
        index("idx_alloc_changes_changed_at", false, changedAt)
    }
}

object AllocationLegs : Table("allocation_legs") {
    val coreId = varchar("core_id", 20)
    val allocationSeq = integer("allocation_seq")
    val headcode = varchar("headcode", 6)
    val serviceDate = varchar("service_date", 10)
    val uid = varchar("uid", 10).nullable()
    val resourceGroupId = varchar("resource_group_id", 20)
    val vehicles = varchar("vehicles", 2000)
    val vehicleCount = integer("vehicle_count").default(0)
    val originStanox = varchar("origin_stanox", 10)
    val originTiploc = varchar("origin_tiploc", 10).nullable()
    val originTime = varchar("origin_time", 5).nullable()
    val destStanox = varchar("dest_stanox", 10)
    val destTiploc = varchar("dest_tiploc", 10).nullable()
    val destTime = varchar("dest_time", 5).nullable()
    val updatedAt = datetime("updated_at")

    override val primaryKey = PrimaryKey(coreId, allocationSeq, name = "pk_allocation_legs")

    init {
        index("idx_alloc_legs_uid_date", false, uid, serviceDate)
    }
}

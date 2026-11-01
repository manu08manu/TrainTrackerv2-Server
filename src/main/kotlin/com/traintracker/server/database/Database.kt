package com.traintracker.server.database

import com.traintracker.server.Config
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.kafka.ridToUid
import com.traintracker.server.routes.StatusResponse
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.ExpressionWithColumnType
import org.jetbrains.exposed.v1.core.IColumnType
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.less
import org.jetbrains.exposed.v1.core.lessEq
import org.jetbrains.exposed.v1.core.greaterEq
import org.jetbrains.exposed.v1.core.like
import org.jetbrains.exposed.v1.core.neq
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.migration.jdbc.MigrationUtils
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.core.VarCharColumnType
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.batchReplace
import org.jetbrains.exposed.v1.jdbc.deleteAll
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.insertIgnore
import org.jetbrains.exposed.v1.javatime.datetime
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.jdbc.replace
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.core.statements.BatchInsertStatement
import org.jetbrains.exposed.v1.core.statements.BatchReplaceStatement
import org.jetbrains.exposed.v1.core.statements.DeleteStatement
import org.jetbrains.exposed.v1.core.statements.InsertStatement
import org.jetbrains.exposed.v1.core.statements.ReplaceStatement
import org.jetbrains.exposed.v1.core.statements.UpdateBuilder
import org.jetbrains.exposed.v1.core.statements.UpsertStatement
import org.jetbrains.exposed.v1.jdbc.statements.api.ExposedConnection
import org.jetbrains.exposed.v1.jdbc.statements.jdbc.JdbcConnectionImpl
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.upsert
import org.json.JSONObject
import org.slf4j.LoggerFactory

object Schedules : Table("schedules") {
    val uid: Column<String> = varchar("uid", 6)
    val headcode: Column<String> = varchar("headcode", 4)
    val tsc: Column<String> = varchar("tsc", 8).default("")
    val stpIndicator: Column<Char> = char("stp_indicator")
    val tiploc: Column<String> = varchar("tiploc", 7)
    val crs: Column<String?> = varchar("crs", 3).nullable()
    val scheduledTime: Column<String> = varchar("scheduled_time", 5)
    val scheduledArrival: Column<String?> = varchar("scheduled_arrival", 5).nullable()
    val scheduledDeparture: Column<String?> = varchar("scheduled_departure", 5).nullable()
    val platform: Column<String?> = varchar("platform", 3).nullable()
    val isPass: Column<Boolean> = bool("is_pass")
    val stopType: Column<String> = varchar("stop_type", 2)
    val originTiploc: Column<String> = varchar("origin_tiploc", 7)
    val destTiploc: Column<String> = varchar("dest_tiploc", 7)
    val originCrs: Column<String?> = varchar("origin_crs", 3).nullable()
    val destCrs: Column<String?> = varchar("dest_crs", 3).nullable()
    val scheduleStartDate: Column<String> = varchar("schedule_start_date", 10).default("")
    val scheduleEndDate: Column<String> = varchar("schedule_end_date", 10).default("")
    val scheduleDate: Column<String> = varchar("schedule_date", 10).default("")
    val daysRun: Column<String> = varchar("days_run", 7).default("1111111")
    val stopSequence: Column<Int> = integer("stop_sequence").default(0)

    override val primaryKey = PrimaryKey(uid, tiploc, stopType, scheduleStartDate, name = "pk_schedules")

    val idxUidStp = index("idx_sched_uid_stp", false, uid, stpIndicator)
    val idxCrsTime = index("idx_sched_crs_time", false, crs, scheduledTime)
    val idxHeadcode = index("idx_sched_headcode", false, headcode, stopType)
}

object TrustMovements : Table("trust_movements") {
    val headcode: Column<String> = varchar("headcode", 4)
    val trainId: Column<String> = varchar("train_id", 10)
    val uid: Column<String> = varchar("uid", 6).default("")
    val stanox: Column<String> = varchar("stanox", 6)
    val crs: Column<String?> = varchar("crs", 3).nullable()
    val eventType: Column<String> = varchar("event_type", 15)
    val scheduledTime: Column<String?> = varchar("scheduled_time", 5).nullable()
    val actualTime: Column<String?> = varchar("actual_time", 5).nullable()
    val platform: Column<String?> = varchar("platform", 3).nullable()
    val isCancelled: Column<Boolean> = bool("is_cancelled")
    val cancelReason: Column<String?> = varchar("cancel_reason", 4).nullable()
    val eventTs: Column<LocalDateTime> = datetime("event_ts")

    override val primaryKey = PrimaryKey(trainId, stanox, eventType, name = "pk_trust_movements")

    val idxHc = index("idx_trust_hc", false, headcode)
    val idxTs = index("idx_trust_ts", false, eventTs)
    val idxCrsTime = index("idx_trust_crs_time", false, crs, scheduledTime)
    val idxUidTs = index("idx_trust_uid", false, uid, eventTs)
}

object TrainLocations : Table("train_locations") {
    val headcode: Column<String> = varchar("headcode", 4)
    val rid: Column<String> = varchar("rid", 50).default("")
    val stationName: Column<String> = varchar("station_name", 100)
    val crs: Column<String?> = varchar("crs", 3).nullable()
    val actualTime: Column<String> = varchar("actual_time", 5)
    val eventType: Column<String> = varchar("event_type", 12)
    val delayMinutes: Column<Int> = integer("delay_minutes").default(0)
    val updatedAt: Column<LocalDateTime> = datetime("updated_at")

    override val primaryKey = PrimaryKey(headcode, rid, name = "pk_train_locations")
}

object CifMeta : Table("cif_meta") {
    val key: Column<String> = varchar("key", 64)
    val value: Column<String> = varchar("value", 256)

    override val primaryKey = PrimaryKey(key, name = "pk_cif_meta")
}

object HeadcodeAliases : Table("headcode_aliases") {
    val oldHeadcode: Column<String> = varchar("old_headcode", 4)
    val newHeadcode: Column<String> = varchar("new_headcode", 4)
    val uid: Column<String> = varchar("uid", 6).default("")
    val updatedAt: Column<LocalDateTime> = datetime("updated_at")

    override val primaryKey = PrimaryKey(oldHeadcode, name = "pk_headcode_aliases")
}

object DarwinRidUid : Table("darwin_rid_uid") {
    val rid: Column<String> = varchar("rid", 20)
    val uid: Column<String> = varchar("uid", 10)
    val updatedAt: Column<String> = varchar("updated_at", 20)

    override val primaryKey = PrimaryKey(rid)
}

object HspCache : Table("hsp_cache") {
    val cacheKey: Column<String> = varchar("cache_key", 200)
    val responseJson: Column<String> = text("response_json")
    val cachedAt: Column<String> = text("cached_at")

    override val primaryKey = PrimaryKey(cacheKey, name = "pk_hsp_cache")
}

object TrainActivations : Table("train_activations") {
    val trainId: Column<String> = varchar("train_id", 10)
    val uid: Column<String> = varchar("uid", 10)
    val updatedAt: Column<String> = varchar("updated_at", 20)
    val originStanox: Column<String?> = varchar("origin_stanox", 10).nullable()

    override val primaryKey = PrimaryKey(trainId)

    val idxUid = index("idx_activations_uid", false, uid)
}

object DarwinPlatforms : Table("darwin_platforms") {
    val uid: Column<String> = varchar("uid", 10)
    val rid: Column<String> = varchar("rid", 20)
    val tiploc: Column<String> = varchar("tiploc", 10)
    val platform: Column<String> = varchar("platform", 10)
    val updatedAt: Column<String> = varchar("updated_at", 20)

    override val primaryKey = PrimaryKey(uid, tiploc)

    val idxUpdated = index("idx_darwin_platforms_updated", false, updatedAt)
}

object CifAssociations : Table("cif_associations") {
    val mainUid: Column<String> = varchar("main_uid", 6)
    val assocUid: Column<String> = varchar("assoc_uid", 6)
    val assocTiploc: Column<String> = varchar("assoc_tiploc", 7)
    val assocType: Column<String> = varchar("assoc_type", 2)
    val stpInd: Column<Char> = char("stp_ind")
    val dateIndicator: Column<Char?> = char("date_indicator").nullable()

    override val primaryKey = PrimaryKey(mainUid, assocUid, assocTiploc, name = "pk_cif_associations")
}

data class AssociationRecord(
    val mainUid: String,
    val assocUid: String,
    val assocTiploc: String,
    val assocType: String,
    val stpInd: Char,
    val dateIndicator: Char? = null,
)

object AllocationConsists : Table("allocation_consists") {
    val coreId: Column<String> = varchar("core_id", 20)
    val headcode: Column<String> = varchar("headcode", 6)
    val serviceDate: Column<String> = varchar("service_date", 10)
    val operator: Column<String> = varchar("operator", 10)
    val vehicles: Column<String> = varchar("vehicles", 2000)
    val units: Column<String> = varchar("units", 500).default("")
    val unitCount: Column<Int> = integer("unit_count").default(0)
    val updatedAt: Column<LocalDateTime> = datetime("updated_at")
    val uid: Column<String?> = varchar("uid", 10).nullable()

    override val primaryKey = PrimaryKey(coreId, name = "pk_allocation_consists")

    val idxHcDate = index("idx_alloc_hc_date", false, headcode, serviceDate)
    val idxUidDate = index("idx_alloc_consists_uid_date", false, uid, serviceDate)
}

object AllocationHistory : Table("allocation_history") {
    val coreId: Column<String> = varchar("core_id", 20)
    val headcode: Column<String> = varchar("headcode", 6)
    val serviceDate: Column<String> = varchar("service_date", 10)
    val operator: Column<String> = varchar("operator", 10)
    val vehicles: Column<String> = varchar("vehicles", 2000)
    val units: Column<String> = varchar("units", 500).default("")
    val unitCount: Column<Int> = integer("unit_count").default(0)
    val recordedAt: Column<LocalDateTime> = datetime("recorded_at")
    val uid: Column<String?> = varchar("uid", 10).nullable()

    override val primaryKey = PrimaryKey(coreId, serviceDate, name = "pk_allocation_history")

    val idxCoreDate = index("idx_alloc_hist_core_date", false, coreId, serviceDate)
    val idxDate = index("idx_alloc_hist_date", false, serviceDate)
    val idxUidDate = index("idx_alloc_hist_uid_date", false, uid, serviceDate)
}

object AllocationChanges : Table("allocation_changes") {
    val id: Column<Int> = integer("id").autoIncrement()
    val coreId: Column<String> = varchar("core_id", 20)
    val headcode: Column<String> = varchar("headcode", 6)
    val serviceDate: Column<String> = varchar("service_date", 10)
    val uid: Column<String?> = varchar("uid", 10).nullable()
    val changedAt: Column<LocalDateTime> = datetime("changed_at")
    val oldUnits: Column<String> = varchar("old_units", 500).default("")
    val newUnits: Column<String> = varchar("new_units", 500).default("")
    val oldCoaches: Column<Int> = integer("old_coaches").default(0)
    val newCoaches: Column<Int> = integer("new_coaches").default(0)

    override val primaryKey = PrimaryKey(id, name = "pk_allocation_changes")

    val idxUidDate = index("idx_alloc_changes_uid_date", false, uid, serviceDate)
    val idxChangedAt = index("idx_alloc_changes_changed_at", false, changedAt)
}

object AllocationLegs : Table("allocation_legs") {
    val coreId: Column<String> = varchar("core_id", 20)
    val allocationSeq: Column<Int> = integer("allocation_seq")
    val headcode: Column<String> = varchar("headcode", 6)
    val serviceDate: Column<String> = varchar("service_date", 10)
    val uid: Column<String?> = varchar("uid", 10).nullable()
    val resourceGroupId: Column<String> = varchar("resource_group_id", 20)
    val vehicles: Column<String> = varchar("vehicles", 2000)
    val vehicleCount: Column<Int> = integer("vehicle_count").default(0)
    val originStanox: Column<String> = varchar("origin_stanox", 10)
    val originTiploc: Column<String?> = varchar("origin_tiploc", 10).nullable()
    val originTime: Column<String?> = varchar("origin_time", 5).nullable()
    val destStanox: Column<String> = varchar("dest_stanox", 10)
    val destTiploc: Column<String?> = varchar("dest_tiploc", 10).nullable()
    val destTime: Column<String?> = varchar("dest_time", 5).nullable()
    val updatedAt: Column<LocalDateTime> = datetime("updated_at")

    override val primaryKey = PrimaryKey(coreId, allocationSeq, name = "pk_allocation_legs")

    val idxUidDate = index("idx_alloc_legs_uid_date", false, uid, serviceDate)
}

data class MovementBatch(
    val headcode: String,
    val trainId: String,
    val stanox: String,
    val crs: String?,
    val eventType: String,
    val scheduledTime: String?,
    val actualTime: String?,
    val isCancelled: Boolean,
    val cancelReason: String? = null,
    val uid: String = "",
    val platform: String? = null,
)

// Used only by AppDatabase (via the synthetic DatabaseKt.access$getLog$p, so AppDatabase must live in this file).
private val log = LoggerFactory.getLogger("Database")

const val SCHEDULE_DATE_SCOPE_SQL: String =
    "((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '')"

private val effectiveDateCache: ConcurrentHashMap<String, Pair<Long, String>> = ConcurrentHashMap()
private const val EFFECTIVE_DATE_TTL_MS: Long = 30000L

/** LIKE pattern matching the days_run string for [date]: '1' at its weekday (Mon=1..Sun=7), '_' elsewhere. */
fun dayRunLikePattern(date: String): String {
    // RECONSTRUCTED: the decompiler dropped the line computing the day of week (verify with jshell)
    val dow = LocalDate.parse(date).dayOfWeek.value
    return (1..7).joinToString("") { if (it == dow) "1" else "_" }
}

fun scheduleDateScope(date: String): Op<Boolean> = run {
    ((Schedules.scheduleStartDate lessEq date) and
        (Schedules.scheduleEndDate greaterEq date) and
        (Schedules.daysRun like dayRunLikePattern(date))) or
        (Schedules.scheduleDate eq "")
}

fun scheduleDateScopeArgs(date: String): List<Pair<IColumnType<*>, Any?>> {
    return listOf(
        VarCharColumnType() as IColumnType<*> to date as Any?,
        VarCharColumnType() as IColumnType<*> to date as Any?,
        VarCharColumnType() as IColumnType<*> to dayRunLikePattern(date) as Any?,
    )
}

fun effectiveDateFor(requested: String): String {
    val now = System.currentTimeMillis()
    effectiveDateCache[requested]?.let { (at, cached) ->
        if (now - at < EFFECTIVE_DATE_TTL_MS) return cached
    }
    if (requested != LocalDate.now().toString()) return requested

    val resolved: String = try {
        transaction {
            var exists = false
            exec(
                "SELECT 1 FROM schedules WHERE schedule_date = ? AND stp_indicator != 'N' LIMIT 1",
                listOf(VarCharColumnType() as IColumnType<*> to requested as Any?),
            ) { rs -> exists = rs.next() }
            if (exists) {
                requested
            } else {
                var loaded: String? = null
                exec("SELECT MAX(schedule_date) AS d FROM schedules WHERE schedule_date != '' AND stp_indicator != 'N'") { rs ->
                    if (rs.next()) loaded = rs.getString("d")
                }
                loaded?.takeIf { it.isNotBlank() } ?: requested
            }
        }
    } catch (e: Exception) {
        requested
    }

    if (effectiveDateCache.size > 8) effectiveDateCache.clear()
    effectiveDateCache[requested] = now to resolved
    return resolved
}

fun crsfromTiplocFallback(tiploc: String): String? =
    try {
        transaction {
            Schedules.select(Schedules.crs)
                .where { (Schedules.tiploc eq tiploc) and (Schedules.crs neq "") }
                .limit(1)
                .firstOrNull()
                ?.get(Schedules.crs)
                ?.takeIf { it.isNotEmpty() }
        }
    } catch (e: Exception) {
        null
    }



object AppDatabase {
    private lateinit var dataSource: HikariDataSource
    private const val RETENTION_DAYS: Long = 30L
    private val allocWriteLock: ReentrantLock = ReentrantLock()
    // RECONSTRUCTED: decompiler shows no initializer; a nullable var must be initialised and null is the only
    // value consistent with the `if (statusCache != null)` check at pristine line 636.
    // RECONSTRUCTED: @Volatile dropped by the decompiler; javap on the live jar shows `private static volatile`.
    @Volatile
    private var statusCache: Pair<Long, StatusResponse>? = null

    data class AllocationChangeResult(
        val changedAt: String,
        val headcode: String,
        val oldUnits: List<String>,
        val newUnits: List<String>,
        val oldCoaches: Int,
        val newCoaches: Int
    )

    data class AllocationLegData(
        val allocationSeq: Int,
        val resourceGroupId: String,
        val vehicles: List<String>,
        val originStanox: String,
        val originTiploc: String?,
        val originTime: String?,
        val destStanox: String,
        val destTiploc: String?,
        val destTime: String?
    )

    data class AllocationLegSpan(val originTiploc: String?, val destTiploc: String?, val vehicleCount: Int)

    data class AllocationResult(
        val coreId: String,
        val headcode: String,
        val serviceDate: String,
        val operator: String,
        val vehicles: List<String>,
        val units: List<String>,
        val unitCount: Int,
        val updatedAt: String = ""
    )

    data class LegData(
        val spans: List<AllocationLegSpan>,
        val groups: List<UnitLegGroup>
    )

    data class UnitLegGroup(
        val fromTiploc: String?,
        val toTiploc: String?,
        val fromTime: String?,
        val toTime: String?,
        val units: List<String>,
        val coachCount: Int,
        val vehicles: List<String> = emptyList()
    )


    fun init() {
        dataSource = if (Config.useOracle) buildOraclePool() else buildSqlitePool()
        Database.connect(dataSource)
        createTables()
        log.info("Database ready (${if (Config.useOracle) "Oracle" else "SQLite"})")
    }

    private fun buildOraclePool(): HikariDataSource {
        Config.oracleWalletDir?.let {
            System.setProperty("oracle.net.wallet_location", it)
            System.setProperty("oracle.net.tns_admin", it)
        }
        val config = HikariConfig().apply {
            jdbcUrl = Config.oracleDbUrl!!
            username = Config.oracleDbUser!!
            password = Config.oracleDbPassword!!
            driverClassName = "oracle.jdbc.OracleDriver"
            maximumPoolSize = 5
            minimumIdle = 2
            connectionTimeout = 30000L
            idleTimeout = 600000L
            maxLifetime = 1800000L
            connectionTestQuery = "SELECT 1 FROM DUAL"
        }
        return HikariDataSource(config)
    }

    private fun buildSqlitePool(): HikariDataSource {
        val config = HikariConfig().apply {
            jdbcUrl = "jdbc:sqlite:${Config.sqlitePath}"
            driverClassName = "org.sqlite.JDBC"
            maximumPoolSize = 10
            connectionInitSql =
                "PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-32768; PRAGMA temp_store=MEMORY; PRAGMA mmap_size=268435456; PRAGMA journal_size_limit=67108864;"
        }
        return HikariDataSource(config)
    }

    private fun createTables() {
        transaction {
            val tables = arrayOf<Table>(
                HspCache,
                Schedules,
                TrustMovements,
                TrainLocations,
                CifMeta,
                AllocationConsists,
                AllocationLegs,
                HeadcodeAliases,
                AllocationHistory,
                AllocationChanges,
                CifAssociations,
                DarwinPlatforms,
                DarwinRidUid,
                TrainActivations,
            )
            MigrationUtils.statementsRequiredForDatabaseMigration(*tables).forEach { sql ->
                if (sql.uppercase().contains("DROP ")) {
                    log.warn("Schema drop skipped: $sql")
                } else {
                    log.info("Schema change: $sql")
                    exec(sql)
                }
            }
            try {
                exec("CREATE INDEX IF NOT EXISTS idx_sched_crs_time ON schedules(crs, scheduled_time)")
                exec("CREATE INDEX IF NOT EXISTS idx_trust_hc ON trust_movements(headcode)")
                exec("CREATE INDEX IF NOT EXISTS idx_trust_ts ON trust_movements(event_ts)")
                exec("CREATE INDEX IF NOT EXISTS idx_trust_crs_time ON trust_movements(crs, scheduled_time)")
                exec("CREATE INDEX IF NOT EXISTS idx_trust_uid ON trust_movements(uid, event_ts)")
                exec("CREATE INDEX IF NOT EXISTS idx_activations_uid ON train_activations(uid)")
                exec("CREATE INDEX IF NOT EXISTS idx_alloc_hc_date ON allocation_consists(headcode, service_date)")
                exec("CREATE INDEX IF NOT EXISTS idx_alloc_hist_core_date ON allocation_history(core_id, service_date)")
                exec("CREATE INDEX IF NOT EXISTS idx_alloc_hist_date ON allocation_history(service_date)")
                exec("CREATE INDEX IF NOT EXISTS idx_alloc_consists_uid_date ON allocation_consists(uid, service_date)")
                exec("CREATE INDEX IF NOT EXISTS idx_alloc_hist_uid_date ON allocation_history(uid, service_date)")
                exec("CREATE INDEX IF NOT EXISTS idx_darwin_platforms_updated ON darwin_platforms(updated_at)")
            } catch (e: Exception) {
            }
        }
        backfillAllocationUids()
    }

    private fun backfillAllocationUids() {
        // RECONSTRUCTED: the decompiler dropped this local entirely; string copied from the live jar's constant pool.
        val caseExpr =
            "CASE WHEN substr(core_id, length(headcode)+1, 1) = ' ' THEN substr(core_id, length(headcode)+2, length(core_id)-length(headcode)-1-2) ELSE substr(core_id, length(headcode)+1, length(core_id)-length(headcode)-2) END"
        transaction {
            try {
                val consists = exec("UPDATE allocation_consists SET uid = $caseExpr WHERE length(core_id) > length(headcode)+2")
                val history = exec("UPDATE allocation_history SET uid = $caseExpr WHERE length(core_id) > length(headcode)+2")
                log.info("Allocation uid backfill (unified formula): consists=$consists history=$history rows updated")
            } catch (e: Exception) {
                log.warn("Allocation uid backfill failed: ${e.message}")
            }
        }
    }


    fun saveHeadcodeAlias(oldHeadcode: String, newHeadcode: String, uid: String = "") {
        transaction {
            HeadcodeAliases.upsert {
                it[HeadcodeAliases.oldHeadcode] = oldHeadcode
                it[HeadcodeAliases.newHeadcode] = newHeadcode
                it[HeadcodeAliases.uid] = uid
                it[HeadcodeAliases.updatedAt] = LocalDateTime.now()
            }
        }
    }

    fun transferLocation(oldHeadcode: String, newHeadcode: String, rid: String) {
        transaction {
            val existing = TrainLocations.selectAll()
                .where { (TrainLocations.headcode eq oldHeadcode) and (TrainLocations.rid eq rid) }
                .singleOrNull() ?: return@transaction
            TrainLocations.upsert {
                it[TrainLocations.headcode] = newHeadcode
                it[TrainLocations.rid] = rid
                it[TrainLocations.stationName] = existing[TrainLocations.stationName]
                it[TrainLocations.crs] = existing[TrainLocations.crs]
                it[TrainLocations.actualTime] = existing[TrainLocations.actualTime]
                it[TrainLocations.eventType] = existing[TrainLocations.eventType]
                it[TrainLocations.updatedAt] = existing[TrainLocations.updatedAt]
            }
            TrainLocations.deleteWhere { (TrainLocations.headcode eq oldHeadcode) and (TrainLocations.rid eq rid) }
        }
        log.info("transferLocation: $oldHeadcode -> $newHeadcode (rid=$rid)")
    }

    fun upsertLocation(
        headcode: String,
        rid: String,
        stationName: String,
        crs: String?,
        actualTime: String,
        eventType: String,
        delayMinutes: Int
    ) {
        transaction {
            TrainLocations.replace {
                it[TrainLocations.headcode] = headcode
                it[TrainLocations.rid] = rid
                it[TrainLocations.stationName] = stationName
                it[TrainLocations.crs] = crs
                it[TrainLocations.actualTime] = actualTime
                it[TrainLocations.eventType] = eventType
                it[TrainLocations.delayMinutes] = delayMinutes
                it[TrainLocations.updatedAt] = LocalDateTime.now()
            }
        }
    }

    fun batchUpsertMovements(movements: List<MovementBatch>) {
        if (movements.isEmpty()) return
        transaction {
            TrustMovements.batchReplace(movements) { m ->
                this[TrustMovements.headcode] = m.headcode
                this[TrustMovements.trainId] = m.trainId
                this[TrustMovements.uid] = m.uid
                this[TrustMovements.stanox] = m.stanox
                this[TrustMovements.crs] = m.crs
                this[TrustMovements.eventType] = m.eventType
                this[TrustMovements.scheduledTime] = m.scheduledTime
                this[TrustMovements.actualTime] = m.actualTime
                this[TrustMovements.platform] = m.platform
                this[TrustMovements.isCancelled] = m.isCancelled
                this[TrustMovements.cancelReason] = m.cancelReason
                this[TrustMovements.eventTs] = LocalDateTime.now()
            }
        }
    }

    fun pruneTrustMovements() {
        transaction {
            TrustMovements.deleteWhere { TrustMovements.eventTs less LocalDateTime.now().minusDays(30L) }
        }
    }

    fun pruneTrainLocations() {
        transaction {
            TrainLocations.deleteWhere { TrainLocations.updatedAt less LocalDateTime.now().minusHours(24L) }
        }
    }

    private fun uidFromCoreId(coreId: String, headcode: String): String? {
        val rest = if (coreId.length > headcode.length) coreId.substring(headcode.length) else ""
        val trimmed = if (rest.startsWith(" ")) rest.substring(1) else rest
        return if (trimmed.length > 2) trimmed.substring(0, trimmed.length - 2) else null
    }

    fun upsertAllocation(
        coreId: String,
        headcode: String,
        serviceDate: String,
        operator: String,
        vehicles: List<String>,
        unitCount: Int,
        units: List<String> = emptyList()
    ) {
        allocWriteLock.lock()
        try {
            upsertAllocationUnlocked(coreId, headcode, serviceDate, operator, vehicles, unitCount, units)
        } finally {
            allocWriteLock.unlock()
        }
    }

    private fun upsertAllocationUnlocked(
        coreId: String,
        headcode: String,
        serviceDate: String,
        operator: String,
        vehicles: List<String>,
        unitCount: Int,
        units: List<String> = emptyList()
    ) {
        if (coreId.isEmpty()) return
        // RECONSTRUCTED: the decompiler dropped this local; recovered from live-jar bytecode (astore 8 = uidFromCoreId(...)).
        val extractedUid = uidFromCoreId(coreId, headcode)
        transaction {
            val oldRow = AllocationConsists.selectAll().where { AllocationConsists.coreId eq coreId }.singleOrNull()
            // RECONSTRUCTED: the decompiler merged oldVehicles and oldUnits into one variable; split per live-jar
            // bytecode (locals 10 and 11). oldCoaches is counted from oldVehicles, not oldUnits.
            val oldVehicles = oldRow?.get(AllocationConsists.vehicles) ?: ""
            val oldUnits = if (oldVehicles.isEmpty()) "" else oldRow?.get(AllocationConsists.units) ?: ""
            val newVehicles = vehicles.joinToString(",")
            val newUnits = if (vehicles.isEmpty()) "" else units.joinToString(",")
            if (oldVehicles != newVehicles || oldUnits != newUnits) {
                AllocationChanges.insert {
                    it[AllocationChanges.coreId] = coreId
                    it[AllocationChanges.headcode] = headcode
                    it[AllocationChanges.serviceDate] = serviceDate
                    it[AllocationChanges.uid] = extractedUid
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
                it[AllocationConsists.vehicles] = vehicles.joinToString(",")
                it[AllocationConsists.units] = units.joinToString(",")
                it[AllocationConsists.unitCount] = unitCount
                it[AllocationConsists.updatedAt] = LocalDateTime.now(ZoneOffset.UTC)
                it[AllocationConsists.uid] = extractedUid
            }
            AllocationHistory.insertIgnore {
                it[AllocationHistory.coreId] = coreId
                it[AllocationHistory.headcode] = headcode
                it[AllocationHistory.serviceDate] = serviceDate
                it[AllocationHistory.operator] = operator
                it[AllocationHistory.vehicles] = vehicles.joinToString(",")
                it[AllocationHistory.units] = units.joinToString(",")
                it[AllocationHistory.unitCount] = unitCount
                it[AllocationHistory.recordedAt] = LocalDateTime.now()
                it[AllocationHistory.uid] = extractedUid
            }
        }
    }

    fun replaceAllocationLegs(
        coreId: String,
        headcode: String,
        serviceDate: String,
        legs: List<AllocationLegData>
    ) {
        if (coreId.isEmpty()) return
        // RECONSTRUCTED: the decompiler dropped this local; recovered from live-jar bytecode (astore 5 = uidFromCoreId(...)).
        val extractedUid = uidFromCoreId(coreId, headcode)
        transaction {
            AllocationLegs.deleteWhere { AllocationLegs.coreId eq coreId }
            for (leg in legs) {
                AllocationLegs.insert {
                    it[AllocationLegs.coreId] = coreId
                    it[AllocationLegs.allocationSeq] = leg.allocationSeq
                    it[AllocationLegs.headcode] = headcode
                    it[AllocationLegs.serviceDate] = serviceDate
                    it[AllocationLegs.uid] = extractedUid
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


    fun getLegData(uid: String, date: String): LegData = transaction {
        val rows = AllocationLegs.selectAll().where {
            (AllocationLegs.uid eq uid) and (AllocationLegs.serviceDate eq date)
        }.toList()
        val spans = rows.map {
            AllocationLegSpan(
                it[AllocationLegs.originTiploc],
                it[AllocationLegs.destTiploc],
                it[AllocationLegs.vehicleCount]
            )
        }

        // RECONSTRUCTED: the decompiler dropped `?: ""` in the final sortedBy comparator (separate class
        // AppDatabase$getLegData$lambda$38$$inlined$sortedBy$1); recovered from its bytecode.
        val grouped = rows.groupBy {
            listOf(it[AllocationLegs.originTiploc], it[AllocationLegs.originTime], it[AllocationLegs.destTiploc], it[AllocationLegs.destTime])
        }.map { (_, g) ->
            val f = g.first()
            UnitLegGroup(
                f[AllocationLegs.originTiploc],
                f[AllocationLegs.destTiploc],
                f[AllocationLegs.originTime],
                f[AllocationLegs.destTime],
                g.map { it[AllocationLegs.resourceGroupId] }.distinct().sorted(),
                g.sumOf { it[AllocationLegs.vehicleCount] },
                g.sortedBy { it[AllocationLegs.resourceGroupId] }.flatMap { r -> r[AllocationLegs.vehicles].split(",").map { it.trim() }.filter { it.isNotEmpty() } }
            )
        }.sortedBy { it.fromTime ?: "" }
        val merged = mutableListOf<UnitLegGroup>()
        for (g in grouped) {
            val last = merged.lastOrNull()
            if (last != null && last.units == g.units && last.toTiploc == g.fromTiploc) {
                merged[merged.lastIndex] = last.copy(toTiploc = g.toTiploc, toTime = g.toTime)
            } else {
                merged.add(g)
            }
        }

        LegData(spans, merged)
    }

    private fun correctAllocation(
        allUnits: List<String>,
        allVehicles: List<String>,
        declaredCount: Int
    ): Triple<List<String>, List<String>, Int> {
        return if (declaredCount >= 4 && declaredCount % 2 == 0 && allUnits.size == declaredCount && allUnits.isNotEmpty() &&
            allUnits.all { it.take(3) == allUnits.first().take(3) }
        ) {
            val half = declaredCount / 2
            Triple(allUnits.take(half), allVehicles.take(allVehicles.size / 2), half)
        } else {
            Triple(allUnits, allVehicles, declaredCount)
        }
    }

    private fun allocationFromRaw(
        coreId: String,
        headcode: String,
        serviceDate: String,
        operator: String,
        vehiclesCsv: String?,
        unitsCsv: String?,
        declaredCount: Int,
        updatedAt: String
    ): AllocationResult {
        val allUnits = (unitsCsv ?: "").split(",").filter { it.isNotEmpty() }
        val allVehicles = (vehiclesCsv ?: "").split(",").filter { it.isNotEmpty() }
        val (units, vehicles, count) = correctAllocation(allUnits, allVehicles, declaredCount)
        return AllocationResult(coreId, headcode, serviceDate, operator, vehicles, units, count, updatedAt)
    }

    private fun rowToAllocationResult(row: ResultRow): AllocationResult =
        allocationFromRaw(
            row[AllocationConsists.coreId],
            row[AllocationConsists.headcode],
            row[AllocationConsists.serviceDate],
            row[AllocationConsists.operator],
            row[AllocationConsists.vehicles],
            row[AllocationConsists.units],
            row[AllocationConsists.unitCount],
            row[AllocationConsists.updatedAt].toString()
        )

    private fun rowToAllocationHistoryResult(row: ResultRow): AllocationResult =
        allocationFromRaw(
            row[AllocationHistory.coreId],
            row[AllocationHistory.headcode],
            row[AllocationHistory.serviceDate],
            row[AllocationHistory.operator],
            row[AllocationHistory.vehicles],
            row[AllocationHistory.units],
            row[AllocationHistory.unitCount],
            row[AllocationHistory.recordedAt].toString()
        )

    private fun extractAllocationFromRs(rs: ResultSet, coreId: String, date: String): AllocationResult =
        allocationFromRaw(coreId, "", date, "", rs.getString("vehicles"), rs.getString("units"), rs.getInt("unit_count"), "")

    fun getAllocationByUid(uid: String, date: String? = null): AllocationResult? = transaction {
        fun bind(colType: IColumnType<*>, v: String): Pair<IColumnType<*>, Any?> = colType to (v as Any?)

        val safeUid = uid.filter { it.isLetterOrDigit() }
        val effectiveDate = date?.filter { it.isDigit() || it == '-' } ?: LocalDate.now().toString()
        val vc: IColumnType<*> = VarCharColumnType()
        var hc = ""
        exec(
            "SELECT headcode FROM schedules WHERE uid = ? AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '') LIMIT 1",
            listOf(bind(vc, safeUid)) + scheduleDateScopeArgs(effectiveDateFor(effectiveDate))
        ) { rs ->
            if (rs.next()) hc = (rs.getString(1) ?: "").filter { it.isLetterOrDigit() }
        }
        if (hc.isEmpty()) {
            val distinctHeadcodes = mutableSetOf<String>()
            exec(
                "SELECT DISTINCT headcode FROM allocation_consists WHERE uid = ? AND service_date = ? UNION SELECT DISTINCT headcode FROM allocation_history WHERE uid = ? AND service_date = ?",
                listOf(bind(vc, safeUid), bind(vc, effectiveDate), bind(vc, safeUid), bind(vc, effectiveDate))
            ) { rs ->
                while (rs.next()) {
                    rs.getString(1)?.let { distinctHeadcodes.add(it) }
                }
            }
            when (distinctHeadcodes.size) {
                0 -> return@transaction null
                1 -> hc = distinctHeadcodes.first()
                else -> {
                    log.warn(
                        "getAllocationByUid: ambiguous uid=$safeUid date=$effectiveDate matches ${distinctHeadcodes.size} headcodes ($distinctHeadcodes) with no schedule to anchor on — returning null rather than guessing"
                    )
                    return@transaction null
                }
            }
        }
        var result: AllocationResult? = null
        exec(
            "SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count, updated_at FROM allocation_consists WHERE uid = ? AND headcode = ? AND service_date = ? UNION ALL SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count, recorded_at FROM allocation_history WHERE uid = ? AND headcode = ? AND service_date = ? AND NOT EXISTS (SELECT 1 FROM allocation_consists WHERE uid = ? AND headcode = ? AND service_date = ?) ORDER BY service_date DESC LIMIT 1",
            listOf(
                bind(vc, safeUid), bind(vc, hc), bind(vc, effectiveDate),
                bind(vc, safeUid), bind(vc, hc), bind(vc, effectiveDate),
                bind(vc, safeUid), bind(vc, hc), bind(vc, effectiveDate)
            )
        ) { rs ->
            if (rs.next()) {
                result = allocationFromRaw(
                    rs.getString(1) ?: "",
                    rs.getString(2) ?: "",
                    rs.getString(3) ?: "",
                    rs.getString(4) ?: "",
                    rs.getString(5),
                    rs.getString(6),
                    rs.getInt(7),
                    (rs.getString(8) ?: "").replace(' ', 'T')
                )
            }
        }
        result
    }


    fun getAllocationsByUids(uids: Map<String, String>, date: String? = null): Map<String, AllocationResult> {
        if (uids.isEmpty()) return emptyMap()
        val safeDate = date?.filter { it.isDigit() || it == '-' } ?: LocalDate.now().toString()
        // RECONSTRUCTED: the decompiler dropped the name of this local and the filterKeys step that builds it;
        // recovered from live-jar bytecode (associate -> filter on non-empty key -> astore 4).
        val safeUids = uids.entries.associate { (uid, hc) ->
            uid.filter { it.isLetterOrDigit() } to hc.filter { it.isLetterOrDigit() }
        }.filter { (k, _) -> k.isNotEmpty() }
        if (safeUids.isEmpty()) return emptyMap()
        val results = mutableMapOf<String, AllocationResult>()
        // RECONSTRUCTED: dropped by the decompiler; live bytecode: sipush 200, istore 6.
        val batchSize = 200
        transaction {
            val colType: IColumnType<*> = VarCharColumnType()
            fun bind(v: String): Pair<IColumnType<*>, Any?> = colType to (v as Any?)

            fun fetch(table: String, tsCol: String, suffix: String, wanted: Map<String, String>) {
                // RECONSTRUCTED: the decompiler dropped the receiver of this forEach; live bytecode:
                // wanted.entrySet() -> CollectionsKt.chunked(batchSize).
                wanted.entries.chunked(batchSize).forEach { chunk ->
                    val pairClause = chunk.joinToString(" OR ") { "(uid = ? AND headcode = ?)" }
                    val pairArgs = chunk.flatMap { (k, v) -> listOf(bind(k), bind(v)) }
                    exec(
                        "SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count, uid, $tsCol FROM $table WHERE ($pairClause) AND service_date = ?$suffix",
                        pairArgs + listOf(bind(safeDate))
                    ) { rs ->
                        while (rs.next()) {
                            val uid = rs.getString(8) ?: continue
                            if (!wanted.containsKey(uid) || results.containsKey(uid)) continue
                            results[uid] = allocationFromRaw(
                                rs.getString(1) ?: "",
                                rs.getString(2) ?: "",
                                rs.getString(3) ?: "",
                                rs.getString(4) ?: "",
                                rs.getString(5),
                                rs.getString(6),
                                rs.getInt(7),
                                (rs.getString(9) ?: "").replace(' ', 'T')
                            )
                        }
                    }
                }
            }

            fetch("allocation_consists", "updated_at", "", safeUids)
            val missing = safeUids.filter { (k, _) -> !results.containsKey(k) }
            if (missing.isNotEmpty()) {
                fetch("allocation_history", "recorded_at", " ORDER BY service_date DESC", missing)
            }
        }
        return results
    }

    fun getAllocationByCoreId(coreId: String): AllocationResult? = transaction {
        AllocationConsists.selectAll().where { AllocationConsists.coreId eq coreId }.singleOrNull()
            ?.let { rowToAllocationResult(it) }
    }

    fun getAllocationsByHeadcode(headcode: String, serviceDate: String? = null): List<AllocationResult> = transaction {
        val primary = AllocationConsists.selectAll()
            .where {
                if (serviceDate != null) {
                    (AllocationConsists.headcode eq headcode) and (AllocationConsists.serviceDate eq serviceDate)
                } else {
                    AllocationConsists.headcode eq headcode
                }
            }
            .orderBy(AllocationConsists.updatedAt, SortOrder.DESC)
            .map { rowToAllocationResult(it) }
        if (primary.isNotEmpty()) return@transaction primary
        AllocationHistory.selectAll()
            .where {
                if (serviceDate != null) {
                    (AllocationHistory.headcode eq headcode) and (AllocationHistory.serviceDate eq serviceDate)
                } else {
                    AllocationHistory.headcode eq headcode
                }
            }
            .orderBy(AllocationHistory.recordedAt, SortOrder.DESC)
            .map { rowToAllocationHistoryResult(it) }
    }

    fun getAllocationChanges(uid: String, serviceDate: String): List<AllocationChangeResult> = transaction {
        AllocationChanges.selectAll()
            .where { (AllocationChanges.uid eq uid) and (AllocationChanges.serviceDate eq serviceDate) }
            .orderBy(AllocationChanges.changedAt, SortOrder.ASC)
            .map { row ->
                AllocationChangeResult(
                    "${row[AllocationChanges.changedAt]}Z",
                    row[AllocationChanges.headcode],
                    row[AllocationChanges.oldUnits].split(",").filter { it.isNotEmpty() },
                    row[AllocationChanges.newUnits].split(",").filter { it.isNotEmpty() },
                    row[AllocationChanges.oldCoaches],
                    row[AllocationChanges.newCoaches]
                )
            }
    }

    fun pruneAllocationChanges() {
        transaction {
            AllocationChanges.deleteWhere { AllocationChanges.changedAt less LocalDateTime.now(ZoneOffset.UTC).minusDays(30L) }
        }
    }

    fun pruneHspCache() {
        try {
            transaction {
                exec("DELETE FROM hsp_cache WHERE cached_at < datetime('now', '-30 days')")
                exec("DELETE FROM hsp_cache WHERE cache_key LIKE 'act:%'")
            }
        } catch (e: Exception) {
            log.debug("HSP cache prune error: ${e.message}")
        }
    }

    private fun toIsoUtc(raw: String?): String {
        if (raw.isNullOrEmpty()) return ""
        val s = raw.replace(' ', 'T')
        return if (s.endsWith("Z")) s else "${s}Z"
    }

    private fun getMissingItems(table: String, keyColumn: String): List<Map<String, String>> {
        val rows = mutableListOf<Map<String, String>>()
        transaction {
            exec("SELECT $keyColumn, seen_count, first_seen, last_seen, headcode, uid FROM $table ORDER BY $keyColumn ASC LIMIT 200") { rs ->
                while (rs.next()) {
                    rows.add(
                        mapOf(
                            keyColumn to (rs.getString(keyColumn) ?: ""),
                            "seenCount" to (rs.getString("seen_count") ?: "0"),
                            "firstSeen" to toIsoUtc(rs.getString("first_seen")),
                            "lastSeen" to toIsoUtc(rs.getString("last_seen")),
                            "headcode" to (rs.getString("headcode") ?: ""),
                            "uid" to (rs.getString("uid") ?: "")
                        )
                    )
                }
            }
        }
        return rows
    }

    private fun deleteMissingItem(table: String, keyColumn: String, key: String) {
        transaction {
            val colType: IColumnType<*> = VarCharColumnType()
            exec("DELETE FROM $table WHERE $keyColumn = ?", listOf(colType to key as Any?))
        }
    }

    fun uidForHeadcode(headcode: String): String? = transaction {
        Schedules.selectAll()
            .where { (Schedules.headcode eq headcode) and scheduleDateScope(effectiveDateFor(LocalDate.now().toString())) }
            .limit(1)
            .firstOrNull()
            ?.get(Schedules.uid)
    }

    fun tscAndUidForHeadcode(headcode: String): Pair<String, String>? = transaction {
        Schedules.selectAll()
            .where { (Schedules.headcode eq headcode) and scheduleDateScope(effectiveDateFor(LocalDate.now().toString())) }
            .limit(1)
            .firstOrNull()
            ?.let { it[Schedules.tsc] to it[Schedules.uid] }
    }

    fun getMissingTiplocs(): List<Map<String, String>> = getMissingItems("missing_tiplocs", "tiploc")

    fun deleteMissingTiploc(tiploc: String) = deleteMissingItem("missing_tiplocs", "tiploc", tiploc)

    fun getMissingTscs(): List<Map<String, String>> = getMissingItems("missing_tscs", "tsc")

    fun deleteMissingTsc(tsc: String) = deleteMissingItem("missing_tscs", "tsc", tsc)

    fun getCifChangeAlerts(): List<Map<String, String>> {
        val rows = mutableListOf<Map<String, String>>()
        transaction {
            exec("SELECT tiploc, old_desc, old_crs, new_desc, new_crs, first_seen, last_seen\n                    FROM cif_change_alerts WHERE dismissed_at IS NULL\n                    ORDER BY last_seen DESC LIMIT 200") { rs ->
                while (rs.next()) {
                    rows.add(
                        mapOf(
                            "tiploc" to (rs.getString("tiploc") ?: ""),
                            "oldDesc" to (rs.getString("old_desc") ?: ""),
                            "oldCrs" to (rs.getString("old_crs") ?: ""),
                            "newDesc" to (rs.getString("new_desc") ?: ""),
                            "newCrs" to (rs.getString("new_crs") ?: ""),
                            "firstSeen" to toIsoUtc(rs.getString("first_seen")),
                            "lastSeen" to toIsoUtc(rs.getString("last_seen"))
                        )
                    )
                }
            }
        }
        return rows
    }


    fun dismissCifChangeAlert(tiploc: String, oldDesc: String, oldCrs: String, newDesc: String, newCrs: String) {
        transaction {
            val s: IColumnType<*> = VarCharColumnType()
            exec(
                "UPDATE cif_change_alerts SET dismissed_at = datetime('now')\n                    WHERE tiploc = ? AND old_desc = ? AND old_crs = ? AND new_desc = ? AND new_crs = ? AND dismissed_at IS NULL",
                listOf(s to tiploc as Any?, s to oldDesc as Any?, s to oldCrs as Any?, s to newDesc as Any?, s to newCrs as Any?)
            )
        }
    }

    fun getFeedback(): List<Map<String, String>> {
        val rows = mutableListOf<Map<String, String>>()
        transaction {
            exec("SELECT id, message, page, submitted_at\n                    FROM feedback WHERE dismissed_at IS NULL\n                    ORDER BY submitted_at DESC LIMIT 200") { rs ->
                while (rs.next()) {
                    rows.add(
                        mapOf(
                            "id" to (rs.getString("id") ?: ""),
                            "message" to (rs.getString("message") ?: ""),
                            "page" to (rs.getString("page") ?: ""),
                            "submittedAt" to toIsoUtc(rs.getString("submitted_at"))
                        )
                    )
                }
            }
        }
        return rows
    }

    fun submitFeedback(message: String, page: String) {
        transaction {
            val s: IColumnType<*> = VarCharColumnType()
            exec("INSERT INTO feedback (message, page) VALUES (?, ?)", listOf(s to message as Any?, s to page as Any?))
        }
    }

    fun dismissFeedback(id: String) {
        transaction {
            val s: IColumnType<*> = VarCharColumnType()
            exec("UPDATE feedback SET dismissed_at = datetime('now') WHERE id = ? AND dismissed_at IS NULL", listOf(s to id as Any?))
        }
    }

    fun pruneFeedback() {
        try {
            transaction {
                exec("DELETE FROM feedback WHERE dismissed_at IS NOT NULL AND dismissed_at < datetime('now', '-30 days')")
            }
        } catch (e: Exception) {
            log.debug("Feedback prune error: ${e.message}")
        }
    }

    fun pruneCifChangeAlerts() {
        try {
            transaction {
                exec("DELETE FROM cif_change_alerts WHERE dismissed_at IS NOT NULL AND dismissed_at < datetime('now', '-7 days')")
            }
        } catch (e: Exception) {
            log.debug("CIF change alert prune error: ${e.message}")
        }
    }

    fun pruneVstpAmendments() {
        try {
            transaction {
                exec("DELETE FROM schedules WHERE stp_indicator = 'N' AND schedule_end_date < date('now', '-30 days')")
            }
        } catch (e: Exception) {
            log.debug("VSTP prune error: ${e.message}")
        }
    }

    fun pruneAllocations() {
        transaction {
            AllocationConsists.deleteWhere { AllocationConsists.updatedAt less LocalDateTime.now(ZoneOffset.UTC).minusDays(30L) }
        }
    }

    fun pruneAllocationLegs() {
        transaction {
            AllocationLegs.deleteWhere { AllocationLegs.updatedAt less LocalDateTime.now().minusDays(30L) }
        }
    }

    fun pruneAllocationHistory() {
        transaction {
            AllocationHistory.deleteWhere { AllocationHistory.recordedAt less LocalDateTime.now().minusDays(30L) }
        }
    }

    fun getHspUnit(
        rid: String,
        scheduledDep: String = "",
        originCrs: String = "",
        scheduledArr: String = "",
        destTiploc: String = ""
    ): AllocationResult? {
        if (rid.length < 9) return null
        val date = rid.substring(0, 8).let { "${it.substring(0, 4)}-${it.substring(4, 6)}-${it.substring(6, 8)}" }
        val uid = ridToUid[rid] ?: getDarwinUidForRid(rid)
        if (uid != null) {
            val byUid = getAllocationByUid(uid, date)
            if (byUid != null) return byUid
        }
        val ridSuffix5 = rid.takeLast(5)
        val ridSuffix7 = rid.takeLast(7)
        var result1: AllocationResult? = null
        transaction {
            val args = listOf(
                VarCharColumnType() as IColumnType<*> to ridSuffix5 as Any?,
                VarCharColumnType() as IColumnType<*> to ridSuffix7 as Any?,
                VarCharColumnType() as IColumnType<*> to date as Any?
            )
            exec(
                "SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count FROM allocation_history WHERE (core_id LIKE '%' || ? || '%' OR core_id LIKE '%' || ? || '%') AND service_date = ? LIMIT 1",
                args
            ) { rs ->
                if (rs.next()) result1 = extractAllocationFromRs(rs, rs.getString(1) ?: "", date)
            }
        }
        if (result1 != null) return result1
        return null
    }

    fun getHspCache(key: String): String? = transaction {
        HspCache.selectAll().where { HspCache.cacheKey eq key }.singleOrNull()
            ?.get(HspCache.responseJson)
    }

    fun setHspCache(key: String, json: String): UpsertStatement<Long> = transaction {
        HspCache.upsert {
            it[cacheKey] = key
            it[responseJson] = json
            it[cachedAt] = Instant.now().toString()
        }
    }

    fun saveStanoxMap(map: Map<String, String>) {
        transaction {
            if (map.isEmpty()) return@transaction
            val json = JSONObject()
            map.forEach { (k, v) -> json.put(k, v) }
            HspCache.replace {
                it[HspCache.cacheKey] = "stanox_map"
                it[HspCache.responseJson] = json.toString()
                it[HspCache.cachedAt] = Instant.now().toString()
            }
        }
    }

    fun loadStanoxMap(): Map<String, String> = transaction {
        val raw = HspCache.selectAll().where { HspCache.cacheKey eq "stanox_map" }.singleOrNull()
            ?.get(HspCache.responseJson) ?: return@transaction emptyMap()
        try {
            val json = JSONObject(raw)
            json.keySet().associateWith { json.getString(it) }
        } catch (e: Exception) {
            emptyMap()
        }
    }

    fun saveStanoxTiplocMap(map: Map<String, String>) {
        transaction {
            if (map.isEmpty()) return@transaction
            val json = JSONObject()
            map.forEach { (k, v) -> json.put(k, v) }
            HspCache.replace {
                it[HspCache.cacheKey] = "stanox_tiploc_map"
                it[HspCache.responseJson] = json.toString()
                it[HspCache.cachedAt] = Instant.now().toString()
            }
        }
    }

    fun loadStanoxTiplocMap(): Map<String, String> = transaction {
        val raw = HspCache.selectAll().where { HspCache.cacheKey eq "stanox_tiploc_map" }.singleOrNull()
            ?.get(HspCache.responseJson) ?: return@transaction emptyMap()
        try {
            val json = JSONObject(raw)
            json.keySet().associateWith { json.getString(it) }
        } catch (e: Exception) {
            emptyMap()
        }
    }

    fun saveTrainActivation(trainId: String, uid: String, originStanox: String? = null) {
        val now = Instant.now().toString().replace('T', ' ').take(19)
        val safeOrigin = originStanox?.replace("'", "") ?: ""
        val colType: IColumnType<*> = VarCharColumnType()
        fun bind(v: String?): Pair<IColumnType<*>, Any?> = colType to (v as Any?)
        try {
            transaction {
                exec(
                    "INSERT INTO train_activations(train_id, uid, updated_at, origin_stanox) VALUES(?, ?, ?, ?) ON CONFLICT(train_id) DO UPDATE SET uid=excluded.uid, updated_at=excluded.updated_at, origin_stanox=excluded.origin_stanox",
                    // RECONSTRUCTED: the decompiler mangled the 4th argument's control flow; live bytecode = inlined ifEmpty { null }.
                    listOf(bind(trainId), bind(uid), bind(now), bind(safeOrigin.ifEmpty { null }))
                )
            }
        } catch (e: Exception) {
            log.debug("saveTrainActivation error: ${e.message}")
        }
    }

    fun loadTrainActivations(): Map<String, String> {
        val result = mutableMapOf<String, String>()
        try {
            transaction {
                exec("SELECT train_id, uid FROM train_activations WHERE updated_at >= datetime('now', '-1 day')") { rs ->
                    while (rs.next()) {
                        val trainId = rs.getString("train_id") ?: return@exec
                        val uid = rs.getString("uid") ?: return@exec
                        // RECONSTRUCTED: the decompiler merged both locals and showed put(x, x); live bytecode puts trainId -> uid.
                        result[trainId] = uid
                    }
                }
            }
        } catch (e: Exception) {
        }
        return result
    }

    fun pruneTrainActivations() {
        try {
            transaction {
                exec("DELETE FROM train_activations WHERE updated_at < datetime('now', '-30 days')")
            }
        } catch (e: Exception) {
            log.debug("pruneTrainActivations error: ${e.message}")
        }
    }


    fun getEnrichedStatus(trustConnected: Boolean, trainLocCount: Int): StatusResponse {
        statusCache?.let { (ts, cached) ->
            if (System.currentTimeMillis() - ts < 30000L) {
                return cached.copy(trustConnected = trustConnected, trainLocationsCount = trainLocCount)
            }
        }
        val cifDate = getCifMeta("last_download_date")
        var scheduleCount = 0
        var uniqueServiceCount = 0
        var trustCount = 0
        var trustCount24h = 0
        var allocToday = 0
        var cifChangePendingCount = 0
        var missingTiplocCount = 0
        var missingTscCount = 0
        var feedbackCount = 0
        val today = LocalDate.now().toString()
        transaction {
            exec("SELECT COUNT(*) FROM schedules") { rs -> if (rs.next()) scheduleCount = rs.getInt(1) }
            exec("SELECT COUNT(DISTINCT uid) FROM schedules") { rs -> if (rs.next()) uniqueServiceCount = rs.getInt(1) }
            exec("SELECT COUNT(*) FROM trust_movements") { rs -> if (rs.next()) trustCount = rs.getInt(1) }
            trustCount24h = TrustMovements.selectAll().where { TrustMovements.eventTs greaterEq LocalDateTime.now().minusHours(24L) }.count().toInt()
            exec("SELECT COUNT(DISTINCT headcode) FROM allocation_history WHERE service_date='$today'") { rs -> if (rs.next()) allocToday = rs.getInt(1) }
            exec("SELECT COUNT(*) FROM cif_change_alerts WHERE dismissed_at IS NULL") { rs -> if (rs.next()) cifChangePendingCount = rs.getInt(1) }
            exec("SELECT COUNT(*) FROM missing_tiplocs") { rs -> if (rs.next()) missingTiplocCount = rs.getInt(1) }
            exec("SELECT COUNT(*) FROM missing_tscs") { rs -> if (rs.next()) missingTscCount = rs.getInt(1) }
            exec("SELECT COUNT(*) FROM feedback WHERE dismissed_at IS NULL") { rs -> if (rs.next()) feedbackCount = rs.getInt(1) }
        }
        val resp = StatusResponse(
            cifDate,
            trustConnected,
            trainLocCount,
            scheduleCount,
            uniqueServiceCount,
            trustCount,
            trustCount24h,
            allocToday,
            transaction { AllocationConsists.selectAll().count().toInt() },
            CorpusLookup.tiplocNameCount(),
            cifChangePendingCount,
            missingTiplocCount,
            missingTscCount,
            feedbackCount
        )
        statusCache = System.currentTimeMillis() to resp
        return resp
    }

    fun getCifMeta(key: String): String? = transaction {
        CifMeta.selectAll().where { CifMeta.key eq key }.singleOrNull()?.get(CifMeta.value)
    }

    fun setCifMeta(key: String, value: String): ReplaceStatement<Long> = transaction {
        CifMeta.replace {
            it[CifMeta.key] = key
            it[CifMeta.value] = value
        }
    }

    fun getStockNeighbours(uid: String, date: String): Pair<Pair<String, String>?, Pair<String, String>?> {
        val safeUid = uid.filter { it.isLetterOrDigit() }
        val safeDate = date.filter { it.isDigit() || it == '-' }
        val vc: IColumnType<*> = VarCharColumnType()
        return try {
            transaction {
                var myUnits: List<String> = emptyList()
                exec(
                    "SELECT units FROM allocation_consists WHERE uid = ? AND service_date = ? LIMIT 1",
                    listOf(vc to safeUid as Any?, vc to safeDate as Any?)
                ) { rs ->
                    if (rs.next()) {
                        myUnits = (rs.getString("units") ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() }
                    }
                }
                if (myUnits.isEmpty()) return@transaction null to null
                val likeClause = myUnits.joinToString(" OR ") { "(',' || ac.units || ',') LIKE ?" }
                val likeArgs = myUnits.map { vc to "%,$it,%" as Any? }
                val rows = mutableListOf<Triple<String, String, String>>()
                exec(
                    "SELECT ac.uid AS u, ac.headcode AS hc, MIN(s.scheduled_time) AS lo FROM allocation_consists ac JOIN schedules s ON s.uid = ac.uid AND s.stop_type = 'LO' WHERE ac.service_date = ? AND ($likeClause) AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '') GROUP BY ac.uid, ac.headcode",
                    listOf(vc to safeDate as Any?) + likeArgs + scheduleDateScopeArgs(effectiveDateFor(safeDate))
                ) { rs ->
                    while (rs.next()) {
                        val u = rs.getString("u") ?: continue
                        val hc = rs.getString("hc") ?: ""
                        val lo = rs.getString("lo") ?: continue
                        if (lo.isNotEmpty()) rows.add(Triple(u, hc, lo))
                    }
                }
                // RECONSTRUCTED: the decompiler merged/garbled the locals below (and the u/hc/lo locals above);
                // names and structure from the live LocalVariableTable and bytecode.
                val mine = rows.firstOrNull { it.first == safeUid } ?: return@transaction null to null
                val ordered = rows.filter { it.first != safeUid }.sortedBy { it.third }
                val prev = ordered.lastOrNull { it.third < mine.third }?.let { it.first to it.second }
                val next = ordered.firstOrNull { it.third > mine.third }?.let { it.first to it.second }
                prev to next
            }
        } catch (e: Exception) {
            null to null
        }
    }


    fun getStockNeighboursForwardAll(uid: String, date: String): List<Triple<String, String, String>> {
        val safeUid = uid.filter { it.isLetterOrDigit() }
        val safeDate = date.filter { it.isDigit() || it == '-' }
        val vc: IColumnType<*> = VarCharColumnType()
        return try {
            transaction {
                var myUnits: List<String> = emptyList()
                exec(
                    "SELECT units FROM allocation_consists WHERE uid = ? AND service_date = ? LIMIT 1",
                    listOf(vc to safeUid as Any?, vc to safeDate as Any?)
                ) { rs ->
                    if (rs.next()) {
                        myUnits = (rs.getString("units") ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() }
                    }
                }
                if (myUnits.isEmpty()) return@transaction emptyList()
                var myTerminusTiploc: String? = null
                var myTerminusTime: String? = null
                exec(
                    "SELECT s.tiploc AS tp, s.scheduled_time AS tm FROM schedules s WHERE s.uid = ? AND s.stop_type = 'LT' AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '') LIMIT 1",
                    listOf(vc to safeUid as Any?) + scheduleDateScopeArgs(effectiveDateFor(safeDate))
                ) { rs ->
                    if (rs.next()) {
                        myTerminusTiploc = rs.getString("tp")
                        myTerminusTime = rs.getString("tm")
                    }
                }
                val terminusTiploc = myTerminusTiploc ?: return@transaction emptyList()
                var myOriginTime: String? = null
                exec(
                    "SELECT s.scheduled_time AS tm FROM schedules s WHERE s.uid = ? AND s.stop_type = 'LO' AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '') LIMIT 1",
                    listOf(vc to safeUid as Any?) + scheduleDateScopeArgs(effectiveDateFor(safeDate))
                ) { rs ->
                    if (rs.next()) myOriginTime = rs.getString("tm")
                }
                val minTime = myTerminusTime
                val terminusRolledOver = myOriginTime != null && minTime != null && minTime < myOriginTime
                val nextDateStr = try {
                    LocalDate.parse(safeDate).plusDays(1).toString()
                } catch (e: Exception) {
                    safeDate
                }
                val nextEffectiveDate = effectiveDateFor(nextDateStr)
                val seen = LinkedHashMap<String, Pair<String, String>>()
                for (unit in myUnits) {
                    var bestUid: String? = null
                    var bestHc = ""
                    var bestKey: String? = null
                    var bestSd: String? = null
                    exec(
                        "SELECT ac.uid AS u, ac.headcode AS hc, ac.service_date AS sd, MIN(s.scheduled_time) AS lo FROM allocation_consists ac JOIN schedules s ON s.uid = ac.uid AND s.stop_type = 'LO' AND s.tiploc = ? WHERE ac.uid != ? AND (',' || ac.units || ',') LIKE ? AND ((ac.service_date = ? AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '')) OR (ac.service_date = ? AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = ''))) GROUP BY ac.uid, ac.headcode, ac.service_date",
                        listOf(vc to terminusTiploc as Any?, vc to safeUid as Any?, vc to "%,$unit,%" as Any?, vc to safeDate as Any?) +
                            scheduleDateScopeArgs(effectiveDateFor(safeDate)) +
                            listOf(vc to nextDateStr as Any?) +
                            scheduleDateScopeArgs(nextEffectiveDate)
                    ) { rs ->
                        // RECONSTRUCTED: the decompiler merged u/hc/sd/lo into one variable and folded the guards into one
                        // condition; names from the live LocalVariableTable, guards from bytecode. Guard 4 relies on the
                        // K2 smart cast of minTime through terminusRolledOver (no null check in the live bytecode).
                        while (rs.next()) {
                            val u = rs.getString("u") ?: continue
                            val hc = rs.getString("hc") ?: ""
                            val sd = rs.getString("sd") ?: continue
                            val lo = rs.getString("lo") ?: continue
                            if (lo.isEmpty()) continue
                            if (sd == safeDate && terminusRolledOver) continue
                            if (sd == safeDate && !terminusRolledOver && minTime != null && lo < minTime) continue
                            if (sd == nextDateStr && terminusRolledOver && lo < minTime) continue
                            val key = "$sd $lo"
                            if (bestKey == null || key < bestKey!!) {
                                bestUid = u
                                bestHc = hc
                                bestSd = sd
                                bestKey = key
                            }
                        }
                    }
                    if (bestUid != null) seen.putIfAbsent(bestUid, bestHc to (bestSd ?: safeDate))
                }
                seen.entries.map { Triple(it.key, it.value.first, it.value.second) }
            }
        } catch (e: Exception) {
            emptyList()
        }
    }

    fun getStockNeighboursBackwardAll(uid: String, date: String): List<Triple<String, String, String>> {
        val safeUid = uid.filter { it.isLetterOrDigit() }
        val safeDate = date.filter { it.isDigit() || it == '-' }
        val vc: IColumnType<*> = VarCharColumnType()
        return try {
            transaction {
                var myUnits: List<String> = emptyList()
                exec(
                    "SELECT units FROM allocation_consists WHERE uid = ? AND service_date = ? LIMIT 1",
                    listOf(vc to safeUid as Any?, vc to safeDate as Any?)
                ) { rs ->
                    if (rs.next()) {
                        myUnits = (rs.getString("units") ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() }
                    }
                }
                if (myUnits.isEmpty()) return@transaction emptyList()
                var myOriginTiploc: String? = null
                var myOriginTime: String? = null
                exec(
                    "SELECT s.tiploc AS tp, s.scheduled_time AS tm FROM schedules s WHERE s.uid = ? AND s.stop_type = 'LO' AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '') LIMIT 1",
                    listOf(vc to safeUid as Any?) + scheduleDateScopeArgs(effectiveDateFor(safeDate))
                ) { rs ->
                    if (rs.next()) {
                        myOriginTiploc = rs.getString("tp")
                        myOriginTime = rs.getString("tm")
                    }
                }
                val originTiploc = myOriginTiploc ?: return@transaction emptyList()
                val maxTime = myOriginTime
                val prevDateStr = try {
                    LocalDate.parse(safeDate).minusDays(1).toString()
                } catch (e: Exception) {
                    safeDate
                }
                val prevEffectiveDate = effectiveDateFor(prevDateStr)
                val safeEffectiveDate = effectiveDateFor(safeDate)
                val seen = LinkedHashMap<String, Pair<String, String>>()
                for (unit in myUnits) {
                    var bestUid: String? = null
                    var bestHc = ""
                    var bestKey: String? = null
                    var bestBucketDate: String? = null
                    for ((bucketDate, bucketEffectiveDate) in listOf(prevDateStr to prevEffectiveDate, safeDate to safeEffectiveDate)) {
                        exec(
                            "SELECT ac.uid AS u, ac.headcode AS hc, MAX(s.scheduled_time) AS lt, (SELECT MIN(s3.scheduled_time) FROM schedules s3 WHERE s3.uid = ac.uid AND s3.stop_type = 'LO' AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '')) AS ownLo FROM allocation_consists ac JOIN schedules s ON s.uid = ac.uid AND s.stop_type = 'LT' AND s.tiploc = ? WHERE ac.service_date = ? AND ac.uid != ? AND (',' || ac.units || ',') LIKE ? AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '') GROUP BY ac.uid, ac.headcode",
                            scheduleDateScopeArgs(bucketEffectiveDate) +
                                listOf(vc to originTiploc as Any?, vc to bucketDate as Any?, vc to safeUid as Any?, vc to "%,$unit,%" as Any?) +
                                scheduleDateScopeArgs(bucketEffectiveDate)
                        ) { rs ->
                            // RECONSTRUCTED: locals merged by the decompiler; names from the live LocalVariableTable,
                            // control flow (rolledOver / trueDate / guards) from bytecode.
                            while (rs.next()) {
                                val u = rs.getString("u") ?: continue
                                val hc = rs.getString("hc") ?: ""
                                val lt = rs.getString("lt") ?: continue
                                if (lt.isEmpty()) continue
                                val ownLo = rs.getString("ownLo")
                                val rolledOver = ownLo != null && lt < ownLo
                                val trueDate = if (rolledOver) {
                                    try {
                                        LocalDate.parse(bucketDate).plusDays(1).toString()
                                    } catch (e: Exception) {
                                        bucketDate
                                    }
                                } else {
                                    bucketDate
                                }
                                if (trueDate > safeDate) continue
                                if (trueDate == safeDate && maxTime != null && lt > maxTime) continue
                                val key = "$trueDate $lt"
                                if (bestKey == null || key > bestKey!!) {
                                    bestUid = u
                                    bestHc = hc
                                    bestBucketDate = bucketDate
                                    bestKey = key
                                }
                            }
                        }
                    }
                    if (bestUid != null) seen.putIfAbsent(bestUid, bestHc to (bestBucketDate ?: safeDate))
                }
                seen.entries.map { Triple(it.key, it.value.first, it.value.second) }
            }
        } catch (e: Exception) {
            emptyList()
        }
    }


    fun getUnitsForUids(uids: Collection<String>, date: String): Map<String, Set<String>> {
        if (uids.isEmpty()) return emptyMap()
        val safeDate = date.filter { it.isDigit() || it == '-' }
        return try {
            transaction {
                val result = mutableMapOf<String, MutableSet<String>>()
                val list = uids.toList()
                val placeholders = list.joinToString(",") { "?" }
                val colType: IColumnType<*> = VarCharColumnType()
                val args = list.map { colType to it as Any? } + (colType to safeDate as Any?)
                exec(
                    "SELECT uid, units FROM allocation_consists WHERE uid IN ($placeholders) AND service_date = ?",
                    args
                ) { rs ->
                    while (rs.next()) {
                        val u = rs.getString("uid") ?: continue
                        val units = (rs.getString("units") ?: "").split(",")
                            .map { it.trim() }
                            .filter { it.isNotEmpty() }
                        if (units.isNotEmpty()) result.getOrPut(u) { mutableSetOf() }.addAll(units)
                    }
                }
                result.mapValues { it.value.toSet() }
            }
        } catch (e: Exception) {
            emptyMap()
        }
    }

    private fun replaceAssociationsRaw(records: List<AssociationRecord>) {
        if (records.isEmpty()) return
        transaction {
            CifAssociations.deleteAll()
            CifAssociations.batchInsert(records, ignore = true) { r ->
                this[CifAssociations.mainUid] = r.mainUid
                this[CifAssociations.assocUid] = r.assocUid
                this[CifAssociations.assocTiploc] = r.assocTiploc
                this[CifAssociations.assocType] = r.assocType
                this[CifAssociations.stpInd] = r.stpInd
                this[CifAssociations.dateIndicator] = r.dateIndicator
            }
        }
        log.info("replaceAssociations: ${records.size} records stored")
    }

    private fun queryAssociationsBy(uids: Set<String>, byMain: Boolean): Map<String, List<AssociationRecord>> {
        if (uids.isEmpty()) return emptyMap()
        val result = mutableMapOf<String, MutableList<AssociationRecord>>()
        // RECONSTRUCTED: the decompiler dropped this local and the receiver of the forEach below; live bytecode.
        val col = if (byMain) CifAssociations.mainUid else CifAssociations.assocUid
        transaction {
            CifAssociations.selectAll().where { (col inList uids) and (CifAssociations.stpInd neq 'C') }
                .forEach { row ->
                val rec = AssociationRecord(
                    row[CifAssociations.mainUid],
                    row[CifAssociations.assocUid],
                    row[CifAssociations.assocTiploc],
                    row[CifAssociations.assocType],
                    row[CifAssociations.stpInd],
                    row[CifAssociations.dateIndicator]
                )
                result.getOrPut(if (byMain) rec.mainUid else rec.assocUid) { mutableListOf() }.add(rec)
            }
        }
        return result
    }

    fun getTrustPlatforms(uids: Collection<String>): Map<Pair<String, String>, String> {
        if (uids.isEmpty()) return emptyMap()
        return try {
            transaction {
                val result = mutableMapOf<Pair<String, String>, String>()
                val distinctUids = uids.toList()
                val placeholders = distinctUids.joinToString(",") { "?" }
                val args = distinctUids.map { VarCharColumnType() as IColumnType<*> to it as Any? }
                exec("SELECT uid, crs, stanox, platform FROM trust_movements WHERE uid IN ($placeholders) AND platform IS NOT NULL AND platform != '' AND event_ts >= datetime('now', '-12 hours') ORDER BY event_ts ASC", args) { rs ->
                    while (rs.next()) {
                        // RECONSTRUCTED: locals merged by the decompiler; names/flow from live LocalVariableTable + bytecode.
                        val u = rs.getString("uid") ?: continue
                        val rawCrs = rs.getString("crs")
                        val stanox = rs.getString("stanox") ?: continue
                        val crs = rawCrs?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromStanox(stanox) ?: continue
                        val p = rs.getString("platform") ?: continue
                        result[u to crs] = p
                    }
                }
                result
            }
        } catch (e: Exception) {
            emptyMap()
        }
    }

    fun getActivatedUids(uids: Collection<String>, date: String): Set<String> {
        if (uids.isEmpty()) return emptySet()
        return try {
            transaction {
                val result = mutableSetOf<String>()
                val distinctUids = uids.toList()
                val placeholders = distinctUids.joinToString(",") { "?" }
                val dayOfMonth = date.takeLast(2)
                val args = distinctUids.map { VarCharColumnType() as IColumnType<*> to it as Any? } +
                    listOf(
                        VarCharColumnType() as IColumnType<*> to date as Any?,
                        VarCharColumnType() as IColumnType<*> to date as Any?,
                        VarCharColumnType() as IColumnType<*> to dayOfMonth as Any?
                    )
                exec("SELECT DISTINCT uid FROM train_activations WHERE uid IN ($placeholders) AND updated_at >= datetime(?, '-12 hours') AND updated_at < datetime(?, '+36 hours') AND train_id LIKE '%' || ?", args) { rs ->
                    while (rs.next()) {
                        rs.getString("uid")?.let { result.add(it) }
                    }
                }
                result
            }
        } catch (e: Exception) {
            emptySet()
        }
    }

    fun upsertDarwinPlatforms(uid: String, rid: String, platforms: Map<String, String>) {
        if (platforms.isEmpty()) return
        val now = Instant.now().toString().replace('T', ' ').take(19)
        try {
            transaction {
                val conn = (connection as JdbcConnectionImpl).connection
                val sql = "INSERT INTO darwin_platforms(uid, rid, tiploc, platform, updated_at) VALUES(?, ?, ?, ?, ?) ON CONFLICT(uid, tiploc) DO UPDATE SET platform=excluded.platform, rid=excluded.rid, updated_at=excluded.updated_at"
                conn.prepareStatement(sql).use { stmt ->
                    for ((tiploc, platform) in platforms) {
                        stmt.setString(1, uid)
                        stmt.setString(2, rid)
                        stmt.setString(3, tiploc)
                        stmt.setString(4, platform)
                        stmt.setString(5, now)
                        stmt.addBatch()
                    }
                    stmt.executeBatch()
                }
            }
        } catch (e: Exception) {
            log.debug("Darwin DB upsert error: ${e.message}")
        }
    }

    fun getDarwinPlatformsForUids(uids: Collection<String>): Map<Pair<String, String>, String> {
        if (uids.isEmpty()) return emptyMap()
        return try {
            transaction {
                val result = mutableMapOf<Pair<String, String>, String>()
                val distinctUids = uids.toList()
                val placeholders = distinctUids.joinToString(",") { "?" }
                val args = distinctUids.map { VarCharColumnType() as IColumnType<*> to it as Any? }
                exec("SELECT uid, tiploc, platform FROM darwin_platforms WHERE uid IN ($placeholders) AND updated_at >= datetime('now', '-24 hours')", args) { rs ->
                    while (rs.next()) {
                        // RECONSTRUCTED: decompiler merged u/t/p and showed put(Pair(x, x), x); live bytecode.
                        val u = rs.getString("uid") ?: continue
                        val t = rs.getString("tiploc") ?: continue
                        val p = rs.getString("platform") ?: continue
                        result[u to t] = p
                    }
                }
                result
            }
        } catch (e: Exception) {
            emptyMap()
        }
    }

    fun getDarwinPlatformsForUid(uid: String, tiplocs: Collection<String>): Map<String, String> {
        if (tiplocs.isEmpty()) return emptyMap()
        return try {
            transaction {
                val result = mutableMapOf<String, String>()
                // RECONSTRUCTED: the decompiler dropped this local (the forEach receiver below); live bytecode.
                val distinctTiplocs = tiplocs.toList()
                val placeholders = distinctTiplocs.joinToString(",") { "?" }
                val args = mutableListOf<Pair<IColumnType<*>, Any?>>()
                args.add(VarCharColumnType() to uid)
                distinctTiplocs.forEach { args.add(VarCharColumnType() to it) }
                exec("SELECT tiploc, platform FROM darwin_platforms WHERE uid=? AND tiploc IN ($placeholders) AND updated_at >= datetime('now', '-24 hours')", args) { rs ->
                    while (rs.next()) {
                        val t = rs.getString("tiploc") ?: continue
                        val p = rs.getString("platform") ?: continue
                        result[t] = p
                    }
                }
                result
            }
        } catch (e: Exception) {
            emptyMap()
        }
    }

    fun upsertDarwinRidUid(rid: String, uid: String) {
        val now = Instant.now().toString().replace('T', ' ').take(19)
        val colType: IColumnType<*> = VarCharColumnType()
        fun bind(v: String): Pair<IColumnType<*>, Any?> = colType to (v as Any?)
        try {
            transaction {
                exec("INSERT INTO darwin_rid_uid(rid, uid, updated_at) VALUES(?, ?, ?) ON CONFLICT(rid) DO UPDATE SET uid=excluded.uid, updated_at=excluded.updated_at", listOf(bind(rid), bind(uid), bind(now)))
            }
        } catch (e: Exception) {
            log.debug("DarwinRidUid upsert error: ${e.message}")
        }
    }

    fun loadDarwinRidUid(): Map<String, String> {
        val result = mutableMapOf<String, String>()
        try {
            transaction {
                exec("SELECT rid, uid FROM darwin_rid_uid") { rs ->
                    while (rs.next()) {
                        // RECONSTRUCTED: decompiler merged rid/uid and showed put(x, x); live bytecode puts rid -> uid.
                        val rid = rs.getString("rid") ?: return@exec
                        val uid = rs.getString("uid") ?: return@exec
                        result[rid] = uid
                    }
                }
            }
        } catch (e: Exception) {
        }
        return result
    }

    fun getDarwinUidForRid(rid: String): String? =
        try {
            transaction {
                val colType: IColumnType<*> = VarCharColumnType()
                var result: String? = null
                exec("SELECT uid FROM darwin_rid_uid WHERE rid=?", listOf(colType to rid as Any?)) { rs ->
                    if (rs.next()) result = rs.getString("uid")
                }
                result
            }
        } catch (e: Exception) {
            null
        }

    fun cleanupDarwinRidUid() {
        try {
            transaction {
                exec("DELETE FROM darwin_rid_uid WHERE updated_at < datetime('now', '-30 days')")
            }
        } catch (e: Exception) {
            log.debug("DarwinRidUid cleanup error: ${e.message}")
        }
    }


    fun cleanupDarwinPlatforms() {
        try {
            transaction {
                exec("DELETE FROM darwin_platforms WHERE updated_at < datetime('now', '-24 hours')")
            }
        } catch (e: Exception) {
            log.debug("Darwin cleanup error: ${e.message}")
        }
    }

    fun getAssociationsForUids(uids: Set<String>): Map<String, List<AssociationRecord>> = queryAssociationsBy(uids, true)

    fun replaceAssociations(all: List<AssociationRecord>) {
        val prio = mapOf('C' to 4, 'N' to 3, 'O' to 2, 'P' to 1)
        val best = HashMap<String, AssociationRecord>()
        for (r in all) {
            val key = "${r.mainUid}|${r.assocUid}|${r.assocTiploc}"
            val existing = best[key]
            if (existing != null && (prio[r.stpInd] ?: 0) <= (prio[existing.stpInd] ?: 0)) continue
            best[key] = r
        }
        val filtered = best.values.filter { it.stpInd != 'C' }
        log.info("replaceAssociations: ${all.size} valid today, ${filtered.size} after STP precedence")
        replaceAssociationsRaw(filtered)
    }

    fun getReverseAssociationsForUids(uids: Set<String>): Map<String, List<AssociationRecord>> = queryAssociationsBy(uids, false)

}

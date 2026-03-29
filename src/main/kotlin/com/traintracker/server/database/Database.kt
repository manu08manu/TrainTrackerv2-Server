package com.traintracker.server.database

import com.traintracker.server.Config
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.sql.SqlExpressionBuilder.less
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.javatime.datetime
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

private val log = LoggerFactory.getLogger("Database")

// ─── Table definitions ────────────────────────────────────────────────────────

object Schedules : Table("schedules") {
    val uid            = varchar("uid", 6)
    val headcode       = varchar("headcode", 4)
    val atocCode       = varchar("atoc_code", 2)
    val stpIndicator   = char("stp_indicator")
    val tiploc         = varchar("tiploc", 7)
    val crs            = varchar("crs", 3).nullable()
    val scheduledTime  = varchar("scheduled_time", 5)
    val platform       = varchar("platform", 3).nullable()
    val isPass         = bool("is_pass")
    val stopType       = varchar("stop_type", 2)
    val originTiploc   = varchar("origin_tiploc", 7)
    val destTiploc     = varchar("dest_tiploc", 7)
    val originCrs      = varchar("origin_crs", 3).nullable()
    val destCrs        = varchar("dest_crs", 3).nullable()

    override val primaryKey = PrimaryKey(uid, tiploc, stopType, name = "pk_schedules")
}

object TrustMovements : Table("trust_movements") {
    val headcode      = varchar("headcode", 4)
    val trainId       = varchar("train_id", 10)
    val stanox        = varchar("stanox", 6)
    val crs           = varchar("crs", 3).nullable()
    val eventType     = varchar("event_type", 12)
    val scheduledTime = varchar("scheduled_time", 5).nullable()
    val actualTime    = varchar("actual_time", 5).nullable()
    val platform      = varchar("platform", 3).nullable()
    val isCancelled    = bool("is_cancelled")
    val cancelReason   = varchar("cancel_reason", 4).nullable()
    val eventTs        = datetime("event_ts")

    override val primaryKey = PrimaryKey(trainId, stanox, eventType, name = "pk_trust_movements")
}

object TrainLocations : Table("train_locations") {
    val headcode     = varchar("headcode", 4)
    val stationName  = varchar("station_name", 100)
    val crs          = varchar("crs", 3).nullable()
    val actualTime   = varchar("actual_time", 5)
    val eventType    = varchar("event_type", 12)
    val delayMinutes = integer("delay_minutes").default(0)
    val updatedAt    = datetime("updated_at")

    override val primaryKey = PrimaryKey(headcode, name = "pk_train_locations")
}

/**
 * Allocation consists — one row per unique train (keyed by coreId).
 * coreId is the globally unique train identifier from the TAFTSI XML <Core> field.
 * Multiple trains with the same headcode on the same day are stored as separate rows.
 */
object AllocationConsists : Table("allocation_consists") {
    val coreId      = varchar("core_id", 20)        // e.g. 1J23L6453116 — primary key
    val headcode    = varchar("headcode", 6)         // e.g. 1J23
    val serviceDate = varchar("service_date", 10)    // e.g. 2026-03-27
    val operator    = varchar("operator", 10)        // e.g. 9971
    val vehicles    = varchar("vehicles", 2000)      // comma-separated vehicle numbers
    val units       = varchar("units", 500).default("")  // comma-separated unit/ResourceGroupIds
    val unitCount   = integer("unit_count").default(0)
    val updatedAt   = datetime("updated_at")

    override val primaryKey = PrimaryKey(coreId, name = "pk_allocation_consists")
}

// Insert-only history table — one row per allocation event, never overwritten
object AllocationHistory : Table("allocation_history") {
    val coreId      = varchar("core_id", 20)
    val headcode    = varchar("headcode", 6)
    val serviceDate = varchar("service_date", 10)
    val operator    = varchar("operator", 10)
    val vehicles    = varchar("vehicles", 2000)
    val units       = varchar("units", 500).default("")
    val unitCount   = integer("unit_count").default(0)
    val recordedAt  = datetime("recorded_at")
    override val primaryKey = PrimaryKey(coreId, name = "pk_allocation_history")
}

object HeadcodeAliases : Table("headcode_aliases") {
    val oldHeadcode = varchar("old_headcode", 4)
    val newHeadcode = varchar("new_headcode", 4)
    val updatedAt   = datetime("updated_at")
    override val primaryKey = PrimaryKey(oldHeadcode, name = "pk_headcode_aliases")
}

object HspUnitHistory : Table("hsp_unit_history") {
    val headcode      = varchar("headcode", 6)
    val serviceDate   = varchar("service_date", 10)
    val scheduledDep  = varchar("scheduled_dep", 5).default("")
    val units         = varchar("units", 500)
    val vehicles      = varchar("vehicles", 2000)
    val unitCount     = integer("unit_count").default(0)
    val snapshottedAt = datetime("snapshotted_at")

    override val primaryKey = PrimaryKey(headcode, serviceDate, name = "pk_hsp_unit_history")
}

object CifMeta : Table("cif_meta") {
    val key   = varchar("key", 64)
    val value = varchar("value", 256)

    override val primaryKey = PrimaryKey(key, name = "pk_cif_meta")
}

// ─── Database initialisation ──────────────────────────────────────────────────

data class MovementBatch(
    val headcode: String, val trainId: String, val stanox: String, val crs: String?,
    val eventType: String, val scheduledTime: String?, val actualTime: String?,
    val platform: String?, val isCancelled: Boolean, val cancelReason: String? = null
)

object AppDatabase {

    private lateinit var dataSource: HikariDataSource

    fun init() {
        dataSource = if (Config.useOracle) buildOraclePool() else buildSqlitePool()
        Database.connect(dataSource)
        createTables()
        log.info("Database ready (${if (Config.useOracle) "Oracle" else "SQLite"})")
    }

    private fun buildOraclePool(): HikariDataSource {
        Config.oracleWalletDir?.let { walletDir ->
            System.setProperty("oracle.net.wallet_location", walletDir)
            System.setProperty("oracle.net.tns_admin", walletDir)
        }
        val cfg = HikariConfig().apply {
            jdbcUrl          = Config.oracleDbUrl!!
            username         = Config.oracleDbUser!!
            password         = Config.oracleDbPassword!!
            driverClassName  = "oracle.jdbc.OracleDriver"
            maximumPoolSize  = 5
            minimumIdle      = 2
            connectionTimeout = 30_000
            idleTimeout       = 600_000
            maxLifetime       = 1_800_000
            connectionTestQuery = "SELECT 1 FROM DUAL"
        }
        return HikariDataSource(cfg)
    }

    private fun buildSqlitePool(): HikariDataSource {
        val cfg = HikariConfig().apply {
            jdbcUrl          = "jdbc:sqlite:${Config.sqlitePath}"
            driverClassName  = "org.sqlite.JDBC"
            maximumPoolSize  = 1
            connectionInitSql = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-131072; PRAGMA temp_store=MEMORY; PRAGMA mmap_size=268435456;"
        }
        return HikariDataSource(cfg)
    }

    private fun createTables() {
        transaction {
            SchemaUtils.createMissingTablesAndColumns(
                Schedules, TrustMovements, TrainLocations, CifMeta, AllocationConsists, HeadcodeAliases, HspUnitHistory, AllocationHistory
            )
            try {
                exec("CREATE INDEX IF NOT EXISTS idx_sched_crs_time ON schedules(crs, scheduled_time)")
                exec("CREATE INDEX IF NOT EXISTS idx_trust_hc ON trust_movements(headcode)")
                exec("CREATE INDEX IF NOT EXISTS idx_trust_ts ON trust_movements(event_ts)")
                exec("CREATE INDEX IF NOT EXISTS idx_trust_crs_time ON trust_movements(crs, scheduled_time)")
                exec("CREATE INDEX IF NOT EXISTS idx_alloc_hc_date ON allocation_consists(headcode, service_date)")
            } catch (_: Exception) {}
        }
    }

    /** Upsert a TRUST movement row. */
    fun upsertMovement(
        headcode: String, trainId: String, stanox: String, crs: String?,
        eventType: String, scheduledTime: String?, actualTime: String?,
        platform: String?, isCancelled: Boolean, cancelReason: String? = null
    ) {
        transaction {
            TrustMovements.replace {
                it[TrustMovements.headcode]      = headcode
                it[TrustMovements.trainId]       = trainId
                it[TrustMovements.stanox]        = stanox
                it[TrustMovements.crs]           = crs
                it[TrustMovements.eventType]     = eventType
                it[TrustMovements.scheduledTime] = scheduledTime
                it[TrustMovements.actualTime]    = actualTime
                it[TrustMovements.platform]      = platform
                it[TrustMovements.isCancelled]   = isCancelled
                it[TrustMovements.cancelReason]  = cancelReason
                it[TrustMovements.eventTs]       = LocalDateTime.now()
            }
        }
    }

    /** Upsert the last known location for a headcode. */


    fun saveHeadcodeAlias(oldHeadcode: String, newHeadcode: String) {
        transaction {
            HeadcodeAliases.upsert {
                it[HeadcodeAliases.oldHeadcode] = oldHeadcode
                it[HeadcodeAliases.newHeadcode] = newHeadcode
                it[HeadcodeAliases.updatedAt]   = LocalDateTime.now()
            }
        }
    }

    fun resolveHeadcode(headcode: String): String {
        return transaction {
            HeadcodeAliases.selectAll()
                .where { HeadcodeAliases.oldHeadcode eq headcode }
                .singleOrNull()
                ?.get(HeadcodeAliases.newHeadcode) ?: headcode
        }
    }

    fun transferLocation(oldHeadcode: String, newHeadcode: String) {
        transaction {
            val existing = TrainLocations.selectAll()
                .where { TrainLocations.headcode eq oldHeadcode }
                .singleOrNull() ?: return@transaction
            TrainLocations.upsert {
                it[TrainLocations.headcode]    = newHeadcode
                it[TrainLocations.stationName] = existing[TrainLocations.stationName]
                it[TrainLocations.crs]         = existing[TrainLocations.crs]
                it[TrainLocations.actualTime]  = existing[TrainLocations.actualTime]
                it[TrainLocations.eventType]   = existing[TrainLocations.eventType]
                it[TrainLocations.updatedAt]   = existing[TrainLocations.updatedAt]
            }
            TrainLocations.deleteWhere { TrainLocations.headcode eq oldHeadcode }
        }
        log.info("transferLocation: $oldHeadcode -> $newHeadcode")
    }

    fun upsertLocation(
        headcode: String, stationName: String, crs: String?,
        actualTime: String, eventType: String, delayMinutes: Int
    ) {
        transaction {
            TrainLocations.replace {
                it[TrainLocations.headcode]     = headcode
                it[TrainLocations.stationName]  = stationName
                it[TrainLocations.crs]          = crs
                it[TrainLocations.actualTime]   = actualTime
                it[TrainLocations.eventType]    = eventType
                it[TrainLocations.delayMinutes] = delayMinutes
                it[TrainLocations.updatedAt]    = LocalDateTime.now()
            }
        }
    }

    /** Batch-insert a list of TRUST movement rows in a single transaction. */
    fun batchUpsertMovements(movements: List<MovementBatch>) {
        if (movements.isEmpty()) return
        transaction {
            movements.forEach { m ->
                TrustMovements.replace {
                    it[TrustMovements.headcode]      = m.headcode
                    it[TrustMovements.trainId]       = m.trainId
                    it[TrustMovements.stanox]        = m.stanox
                    it[TrustMovements.crs]           = m.crs
                    it[TrustMovements.eventType]     = m.eventType
                    it[TrustMovements.scheduledTime] = m.scheduledTime
                    it[TrustMovements.actualTime]    = m.actualTime
                    it[TrustMovements.platform]      = m.platform
                    it[TrustMovements.isCancelled]   = m.isCancelled
                    it[TrustMovements.cancelReason]  = m.cancelReason
                    it[TrustMovements.eventTs]       = LocalDateTime.now()
                }
            }
        }
    }

    /** Prune TRUST movements older than 24 hours. */
    fun pruneTrustMovements() {
        transaction {
            TrustMovements.deleteWhere {
                TrustMovements.eventTs less LocalDateTime.now().minusHours(24)
            }
        }
    }

    /** Upsert an allocation consist keyed by coreId. */
    fun upsertAllocation(
        coreId:      String,
        headcode:    String,
        serviceDate: String,
        operator:    String,
        vehicles:    List<String>,
        unitCount:   Int,
        units:       List<String> = emptyList()
    ) {
        if (coreId.isEmpty()) return
        transaction {
            AllocationConsists.replace {
                it[AllocationConsists.coreId]      = coreId
                it[AllocationConsists.headcode]    = headcode
                it[AllocationConsists.serviceDate] = serviceDate
                it[AllocationConsists.operator]    = operator
                it[AllocationConsists.vehicles]    = vehicles.joinToString(",")
                it[AllocationConsists.units]       = units.joinToString(",")
                it[AllocationConsists.unitCount]   = unitCount
                it[AllocationConsists.updatedAt]   = LocalDateTime.now()
            }
            // Insert into history — ignore if already recorded for this coreId
            val alreadyRecorded = AllocationHistory.select {
                AllocationHistory.coreId eq coreId
            }.count() > 0
            if (!alreadyRecorded) {
                AllocationHistory.insert {
                    it[AllocationHistory.coreId]      = coreId
                    it[AllocationHistory.headcode]    = headcode
                    it[AllocationHistory.serviceDate] = serviceDate
                    it[AllocationHistory.operator]    = operator
                    it[AllocationHistory.vehicles]    = vehicles.joinToString(",")
                    it[AllocationHistory.units]       = units.joinToString(",")
                    it[AllocationHistory.unitCount]   = unitCount
                    it[AllocationHistory.recordedAt]  = LocalDateTime.now()
                }
            }
        }
    }

    data class AllocationResult(
        val coreId:      String,
        val headcode:    String,
        val serviceDate: String,
        val operator:    String,
        val vehicles:    List<String>,
        val units:       List<String>,
        val unitCount:   Int
    )

    private fun rowToAllocationResult(row: ResultRow) = AllocationResult(
        coreId      = row[AllocationConsists.coreId],
        headcode    = row[AllocationConsists.headcode],
        serviceDate = row[AllocationConsists.serviceDate],
        operator    = row[AllocationConsists.operator],
        vehicles    = row[AllocationConsists.vehicles].split(",").filter { it.isNotEmpty() },
        units       = row[AllocationConsists.units].split(",").filter { it.isNotEmpty() },
        unitCount   = row[AllocationConsists.unitCount]
    )

    /** Look up by coreId directly. */
    fun getAllocationByCoreId(coreId: String): AllocationResult? = transaction {
        AllocationConsists.select { AllocationConsists.coreId eq coreId }
            .singleOrNull()?.let { rowToAllocationResult(it) }
    }

    /** Look up all allocations for a headcode, optionally filtered by date. */
    fun getAllocationsByHeadcode(headcode: String, serviceDate: String? = null): List<AllocationResult> = transaction {
        AllocationConsists.select {
            if (serviceDate != null)
                (AllocationConsists.headcode eq headcode) and (AllocationConsists.serviceDate eq serviceDate)
            else
                AllocationConsists.headcode eq headcode
        }
        .orderBy(AllocationConsists.updatedAt, SortOrder.DESC)
        .map { rowToAllocationResult(it) }
    }

    /** Prune allocation consists older than 48 hours. */
    fun pruneAllocations() {
        transaction {
            AllocationConsists.deleteWhere {
                AllocationConsists.updatedAt less LocalDateTime.now().minusHours(48)
            }
        }
    }

    fun snapshotUnitAllocations(date: String) {
        val dbPath = "/opt/traintracker/traintracker.db"
        val sql = """
            INSERT OR REPLACE INTO hsp_unit_history
                (headcode, service_date, scheduled_dep, units, vehicles, unit_count, snapshotted_at)
            SELECT
                a.headcode,
                a.service_date,
                COALESCE((
                    SELECT s.scheduled_time FROM schedules s
                    WHERE s.headcode = a.headcode AND s.stop_type = 'LO'
                    LIMIT 1
                ), '') AS scheduled_dep,
                a.units,
                a.vehicles,
                a.unit_count,
                datetime('now')
            FROM allocation_consists a
            WHERE a.service_date = '$date';
        """.trimIndent()
        val result = ProcessBuilder("sqlite3", dbPath, sql)
            .redirectErrorStream(true)
            .start()
        val output = result.inputStream.bufferedReader().readText()
        val exit = result.waitFor()
        if (exit != 0) {
            log.error("snapshotUnitAllocations failed (exit=$exit): $output")
        } else {
            val count = transaction {
                HspUnitHistory.select { HspUnitHistory.serviceDate eq date }.count()
            }
            log.info("snapshotUnitAllocations: wrote $count rows for $date")
        }
    }

        fun getHspUnit(rid: String, scheduledDep: String = ""): AllocationResult? {
        if (rid.length < 8) return null
        val date = rid.substring(0, 8).let {
            "${it.substring(0, 4)}-${it.substring(4, 6)}-${it.substring(6, 8)}"
        }
        val ridSuffix = rid.substring(8)
        val matchKey = if (ridSuffix.length >= 7) ridSuffix.substring(2, 7) else ""
        if (matchKey.isEmpty()) return null

        fun queryTable(table: String): AllocationResult? {
            var result: AllocationResult? = null
            transaction {
                exec("SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count " +
                    "FROM $table WHERE service_date = '$date' AND core_id LIKE '%$matchKey%' " +
                    "AND (headcode LIKE '1%' OR headcode LIKE '2%') LIMIT 1") { rs ->
                    if (rs.next()) {
                        result = AllocationResult(
                            coreId      = rs.getString(1) ?: "",
                            headcode    = rs.getString(2) ?: "",
                            serviceDate = rs.getString(3) ?: "",
                            operator    = rs.getString(4) ?: "",
                            vehicles    = (rs.getString(5) ?: "").split(",").filter { it.isNotEmpty() },
                            units       = (rs.getString(6) ?: "").split(",").filter { it.isNotEmpty() },
                            unitCount   = rs.getInt(7)
                        )
                    }
                }
            }
            return result
        }

        return queryTable("allocation_history") ?: queryTable("allocation_consists")
    }

    /** Read/write CIF metadata. */
    fun getCifMeta(key: String): String? = transaction {
        CifMeta.select { CifMeta.key eq key }.singleOrNull()?.get(CifMeta.value)
    }

    fun setCifMeta(key: String, value: String) = transaction {
        CifMeta.replace {
            it[CifMeta.key]   = key
            it[CifMeta.value] = value
        }
    }
}

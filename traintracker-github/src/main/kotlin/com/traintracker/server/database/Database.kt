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
    val destCrs           = varchar("dest_crs", 3).nullable()
    val scheduleStartDate = varchar("schedule_start_date", 10).default("")

    override val primaryKey = PrimaryKey(uid, tiploc, stopType, scheduleStartDate, name = "pk_schedules")
    val idxUidStp   = index("idx_sched_uid_stp",   false, uid, stpIndicator)
    val idxCrsTime  = index("idx_sched_crs_time",  false, crs, scheduledTime)
    val idxHeadcode = index("idx_sched_headcode",  false, headcode, stopType)
}



object TrustMovements : Table("trust_movements") {
    val headcode      = varchar("headcode", 4)
    val trainId       = varchar("train_id", 10)
    val uid           = varchar("uid", 6).default("")
    val stanox        = varchar("stanox", 6)
    val crs           = varchar("crs", 3).nullable()
    val eventType     = varchar("event_type", 15)
    val scheduledTime = varchar("scheduled_time", 5).nullable()
    val actualTime    = varchar("actual_time", 5).nullable()
    val platform      = varchar("platform", 3).nullable()
    val isCancelled    = bool("is_cancelled")
    val cancelReason   = varchar("cancel_reason", 4).nullable()
    val eventTs        = datetime("event_ts")

    override val primaryKey = PrimaryKey(trainId, stanox, eventType, name = "pk_trust_movements")
    val idxHc      = index("idx_trust_hc",       false, headcode)
    val idxTs      = index("idx_trust_ts",        false, eventTs)
    val idxCrsTime = index("idx_trust_crs_time",  false, crs, scheduledTime)
}

object TrainLocations : Table("train_locations") {
    val headcode     = varchar("headcode", 4)
    val rid          = varchar("rid", 50).default("")
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
    val idxHcDate = index("idx_alloc_hc_date", false, headcode, serviceDate)
}

// Insert-only history table — one row per allocation event, never overwritten
object HspCache : Table("hsp_cache") {
    val cacheKey  = varchar("cache_key", 200)
    val responseJson = text("response_json")
    val cachedAt  = text("cached_at")
    override val primaryKey = PrimaryKey(cacheKey, name = "pk_hsp_cache")
}

object AllocationHistory : Table("allocation_history") {
    val coreId      = varchar("core_id", 20)
    val headcode    = varchar("headcode", 6)
    val serviceDate = varchar("service_date", 10)
    val operator    = varchar("operator", 10)
    val vehicles    = varchar("vehicles", 2000)
    val units       = varchar("units", 500).default("")
    val unitCount   = integer("unit_count").default(0)
    val recordedAt  = datetime("recorded_at")
    override val primaryKey = PrimaryKey(coreId, serviceDate, name = "pk_allocation_history")
    val idxCoreDate = index("idx_alloc_hist_core_date", false, coreId, serviceDate)
    val idxDate = index("idx_alloc_hist_date", false, serviceDate)
}

object HeadcodeAliases : Table("headcode_aliases") {
    val oldHeadcode = varchar("old_headcode", 4)
    val newHeadcode = varchar("new_headcode", 4)
    val updatedAt   = datetime("updated_at")
    override val primaryKey = PrimaryKey(oldHeadcode, name = "pk_headcode_aliases")
}

object CifMeta : Table("cif_meta") {
    val key   = varchar("key", 64)
    val value = varchar("value", 256)

    override val primaryKey = PrimaryKey(key, name = "pk_cif_meta")
}

object CifAssociations : Table("cif_associations") {
    val mainUid     = varchar("main_uid", 6)      // UID of the main train
    val assocUid    = varchar("assoc_uid", 6)     // UID of the associated (split/join) train
    val assocTiploc = varchar("assoc_tiploc", 7) // tiploc where split/join occurs
    val assocType   = varchar("assoc_type", 2)   // VV=divide, NP=join
    val stpInd      = char("stp_ind")             // N/O/P/C

    override val primaryKey = PrimaryKey(mainUid, assocUid, assocTiploc, name = "pk_cif_associations")
}

// ─── Database initialisation ──────────────────────────────────────────────────

data class MovementBatch(
    val headcode: String, val trainId: String, val stanox: String, val crs: String?,
    val eventType: String, val scheduledTime: String?, val actualTime: String?,
    val isCancelled: Boolean, val cancelReason: String? = null,
    val uid: String = ""
)

data class AssociationRecord(
    val mainUid:     String,
    val assocUid:    String,
    val assocTiploc: String,
    val assocType:   String,  // VV=divide, NP=join
    val stpInd:      Char
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

    @Suppress("DEPRECATION")
    private fun createTables() {
        transaction {
            SchemaUtils.createMissingTablesAndColumns(HspCache,
                Schedules, TrustMovements, TrainLocations, CifMeta, AllocationConsists, HeadcodeAliases, AllocationHistory, CifAssociations, DarwinPlatforms, DarwinRidUid, TrainActivations
            )
            try {
                exec("CREATE INDEX IF NOT EXISTS idx_sched_crs_time ON schedules(crs, scheduled_time)")
                exec("CREATE INDEX IF NOT EXISTS idx_trust_hc ON trust_movements(headcode)")
                exec("CREATE INDEX IF NOT EXISTS idx_trust_ts ON trust_movements(event_ts)")
                exec("CREATE INDEX IF NOT EXISTS idx_trust_crs_time ON trust_movements(crs, scheduled_time)")
                exec("CREATE INDEX IF NOT EXISTS idx_alloc_hc_date ON allocation_consists(headcode, service_date)")
                exec("CREATE INDEX IF NOT EXISTS idx_alloc_hist_core_date ON allocation_history(core_id, service_date)")
                exec("CREATE INDEX IF NOT EXISTS idx_alloc_hist_date ON allocation_history(service_date)")
            } catch (_: Exception) {}
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
        headcode: String, rid: String, stationName: String, crs: String?,
        actualTime: String, eventType: String, delayMinutes: Int
    ) {
        transaction {
            TrainLocations.replace {
                it[TrainLocations.headcode]     = headcode
                it[TrainLocations.rid]          = rid
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
                    it[TrustMovements.uid]           = m.uid
                    it[TrustMovements.stanox]        = m.stanox
                    it[TrustMovements.crs]           = m.crs
                    it[TrustMovements.eventType]     = m.eventType
                    it[TrustMovements.scheduledTime] = m.scheduledTime
                    it[TrustMovements.actualTime]    = m.actualTime
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
            // Insert into history — ignore if already recorded for this coreId+date
            AllocationHistory.insertIgnore {
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

    data class AllocationResult(
        val coreId:      String,
        val headcode:    String,
        val serviceDate: String,
        val operator:    String,
        val vehicles:    List<String>,
        val units:       List<String>,
        val unitCount:   Int
    )

    /**
     * If an allocation row looks like two back-to-back identical consists
     * (e.g. ECS + service merged by the feed), take only the first half.
     * Condition: unit_count >= 4, even, all units share the same 3-char prefix.
     */
    private fun correctAllocation(
        allUnits: List<String>,
        allVehicles: List<String>,
        declaredCount: Int
    ): Triple<List<String>, List<String>, Int> =
        if (declaredCount >= 4 &&
            declaredCount % 2 == 0 &&
            allUnits.size == declaredCount &&
            allUnits.isNotEmpty() &&
            allUnits.all { it.take(3) == allUnits.first().take(3) }
        ) {
            val half = declaredCount / 2
            Triple(allUnits.take(half), allVehicles.take(allVehicles.size / 2), half)
        } else {
            Triple(allUnits, allVehicles, declaredCount)
        }

    private fun rowToAllocationResult(row: ResultRow) = AllocationResult(
        coreId      = row[AllocationConsists.coreId],
        headcode    = row[AllocationConsists.headcode],
        serviceDate = row[AllocationConsists.serviceDate],
        operator    = row[AllocationConsists.operator],
        vehicles    = row[AllocationConsists.vehicles].split(",").filter { it.isNotEmpty() },
        units       = row[AllocationConsists.units].split(",").filter { it.isNotEmpty() },
        unitCount   = row[AllocationConsists.unitCount]
    )

    private fun rowToAllocationHistoryResult(row: ResultRow) = AllocationResult(
        coreId      = row[AllocationHistory.coreId],
        headcode    = row[AllocationHistory.headcode],
        serviceDate = row[AllocationHistory.serviceDate],
        operator    = row[AllocationHistory.operator],
        vehicles    = row[AllocationHistory.vehicles].split(",").filter { it.isNotEmpty() },
        units       = row[AllocationHistory.units].split(",").filter { it.isNotEmpty() },
        unitCount   = row[AllocationHistory.unitCount]
    )

    private fun extractAllocationFromRs(rs: java.sql.ResultSet, coreId: String, date: String): AllocationResult {
        val rawUnits    = (rs.getString("units") ?: "").split(",").filter { it.isNotEmpty() }
        val rawVehicles = (rs.getString("vehicles") ?: "").split(",").filter { it.isNotEmpty() }
        val rawCount    = rs.getInt("unit_count")
        val (units, vehicles, unitCount) = correctAllocation(rawUnits, rawVehicles, rawCount)
        return AllocationResult(coreId, "", date, "", vehicles, units, unitCount)
    }

    /** Look up by coreId directly. */
    fun getAllocationByUid(uid: String, date: String? = null): AllocationResult? = transaction {
        val safeUid = uid.filter { it.isLetterOrDigit() }
        val safeDate = date?.filter { it.isDigit() || it == '-' } ?: java.time.LocalDate.now().toString()
        // Look up headcode from schedules to anchor the allocation match and avoid UID substring collisions
        var headcodeFilter = ""
        exec("SELECT headcode FROM schedules WHERE uid = '$safeUid' LIMIT 1") { rs ->
            if (rs.next()) {
                val hc = (rs.getString(1) ?: "").filter { it.isLetterOrDigit() }
                if (hc.isNotEmpty()) headcodeFilter = " AND headcode = '$hc'"
            }
        }
        var result: AllocationResult? = null
        exec("SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count FROM allocation_consists WHERE substr(core_id, length(headcode)+1, length('$safeUid')) = '$safeUid'$headcodeFilter AND service_date = '$safeDate' UNION ALL SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count FROM allocation_history WHERE substr(core_id, length(headcode)+1, length('$safeUid')) = '$safeUid'$headcodeFilter AND service_date = '$safeDate' AND NOT EXISTS (SELECT 1 FROM allocation_consists WHERE substr(core_id, length(headcode)+1, length('$safeUid')) = '$safeUid'$headcodeFilter AND service_date = '$safeDate') ORDER BY service_date DESC LIMIT 1") { rs ->
            if (rs.next()) {
                val allUnits    = (rs.getString(6) ?: "").split(",").filter { it.isNotEmpty() }
                val allVehicles = (rs.getString(5) ?: "").split(",").filter { it.isNotEmpty() }
                val declaredCount = rs.getInt(7)
                // If unit count >= 4, even, and all units share the same traction-type prefix,
                // the allocation feed has merged multiple consists (e.g. ECS + service).
                // Take only the first half, which corresponds to the service itself.
                val (units, vehicles, unitCount) = correctAllocation(allUnits, allVehicles, declaredCount)
                result = AllocationResult(
                    coreId      = rs.getString(1) ?: "",
                    headcode    = rs.getString(2) ?: "",
                    serviceDate = rs.getString(3) ?: "",
                    operator    = rs.getString(4) ?: "",
                    vehicles    = vehicles,
                    units       = units,
                    unitCount   = unitCount
                )
            }
        }

        result
    }

    fun getAllocationsByUids(uids: Map<String, String>, date: String? = null): Map<String, AllocationResult> {
        if (uids.isEmpty()) return emptyMap()
        val safeDate = date?.filter { it.isDigit() || it == '-' } ?: java.time.LocalDate.now().toString()
        val safeUids = uids.entries.associate { (uid, hc) -> uid.filter { c -> c.isLetterOrDigit() } to hc.filter { c -> c.isLetterOrDigit() } }.filter { (uid, _) -> uid.isNotEmpty() }
        if (safeUids.isEmpty()) return emptyMap()
        val results = mutableMapOf<String, AllocationResult>()
        transaction {
            // Stage 1: allocation_consists — fetch all rows whose core_id contains any uid
            val likeClause = safeUids.entries.joinToString(" OR ") { (uid, hc) -> "(headcode = '$hc' AND (substr(core_id, length(headcode)+1, ${uid.length}) = '$uid' OR substr(core_id, length(headcode)+2, ${uid.length}) = '$uid'))" }
            exec("SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count FROM allocation_consists WHERE ($likeClause) AND service_date = '$safeDate'") { rs ->
                while (rs.next()) {
                    val coreId = rs.getString(1) ?: continue
                    val headcode = rs.getString(2) ?: ""
                    val uidPart = coreId.drop(headcode.length).trimStart()
                    val matchedUid = safeUids.keys.firstOrNull { uidPart.startsWith(it) } ?: continue
                    if (results.containsKey(matchedUid)) continue
                    val allUnits    = (rs.getString(6) ?: "").split(",").filter { it.isNotEmpty() }
                    val allVehicles = (rs.getString(5) ?: "").split(",").filter { it.isNotEmpty() }
                    val (units, vehicles, unitCount) = correctAllocation(allUnits, allVehicles, rs.getInt(7))
                    results[matchedUid] = AllocationResult(
                        coreId      = coreId,
                        headcode    = rs.getString(2) ?: "",
                        serviceDate = rs.getString(3) ?: "",
                        operator    = rs.getString(4) ?: "",
                        vehicles    = vehicles,
                        units       = units,
                        unitCount   = unitCount
                    )
                }
            }
            // Stage 2: allocation_history fallback for any uid still missing
            val missingAfterStage1 = safeUids.filter { (uid, _) -> !results.containsKey(uid) }
            if (missingAfterStage1.isNotEmpty()) {
                val likeFallback = missingAfterStage1.entries.joinToString(" OR ") { (uid, hc) -> "(headcode = '$hc' AND (substr(core_id, length(headcode)+1, ${uid.length}) = '$uid' OR substr(core_id, length(headcode)+2, ${uid.length}) = '$uid'))" }
                exec("SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count FROM allocation_history WHERE ($likeFallback) AND service_date = '$safeDate' ORDER BY service_date DESC") { rs ->
                    while (rs.next()) {
                        val coreId = rs.getString(1) ?: continue
                        val headcode = rs.getString(2) ?: ""
                        val uidPart = coreId.drop(headcode.length).trimStart()
                        val matchedUid = missingAfterStage1.keys.firstOrNull { uidPart.startsWith(it) } ?: continue
                        if (results.containsKey(matchedUid)) continue
                        val allUnits    = (rs.getString(6) ?: "").split(",").filter { it.isNotEmpty() }
                        val allVehicles = (rs.getString(5) ?: "").split(",").filter { it.isNotEmpty() }
                        val (units, vehicles, unitCount) = correctAllocation(allUnits, allVehicles, rs.getInt(7))
                        results[matchedUid] = AllocationResult(
                            coreId      = coreId,
                            headcode    = rs.getString(2) ?: "",
                            serviceDate = rs.getString(3) ?: "",
                            operator    = rs.getString(4) ?: "",
                            vehicles    = vehicles,
                            units       = units,
                            unitCount   = unitCount
                        )
                    }
                }
            }
        }
        return results
    }

    fun getAllocationByCoreId(coreId: String): AllocationResult? = transaction {
        AllocationConsists.selectAll().where { AllocationConsists.coreId eq coreId }
            .singleOrNull()?.let { rowToAllocationResult(it) }
    }

    /** Look up all allocations for a headcode, optionally filtered by date. Falls back to allocation_history for past dates. */
    fun getAllocationsByHeadcode(headcode: String, serviceDate: String? = null): List<AllocationResult> = transaction {
        val primary = AllocationConsists.selectAll().where {
            if (serviceDate != null)
                (AllocationConsists.headcode eq headcode) and (AllocationConsists.serviceDate eq serviceDate)
            else
                AllocationConsists.headcode eq headcode
        }
        .orderBy(AllocationConsists.updatedAt, SortOrder.DESC)
        .map { rowToAllocationResult(it) }

        if (primary.isNotEmpty()) return@transaction primary

        // Fallback to history for past dates pruned from allocation_consists
        AllocationHistory.selectAll().where {
            if (serviceDate != null)
                (AllocationHistory.headcode eq headcode) and (AllocationHistory.serviceDate eq serviceDate)
            else
                AllocationHistory.headcode eq headcode
        }
        .orderBy(AllocationHistory.recordedAt, SortOrder.DESC)
        .map { rowToAllocationHistoryResult(it) }
    }

    /** Prune allocation consists older than 48 hours. */
    fun pruneHspCache() {
        try {
            transaction {
                exec("DELETE FROM hsp_cache WHERE cached_at < datetime('now', '-30 days')")
                exec("DELETE FROM hsp_cache WHERE cache_key LIKE 'act:%'")
                exec("DELETE FROM hsp_cache WHERE cache_key LIKE 'act:%'")
                exec("DELETE FROM hsp_cache WHERE cache_key LIKE 'act:%'")
            }
        } catch (e: Exception) { log.debug("HSP cache prune error: ${e.message}") }
    }

    fun pruneVstpAmendments() {
        try {
            transaction { exec("DROP TABLE IF EXISTS vstp_amendments") }
        } catch (e: Exception) { log.debug("vstp_amendments drop error: ${e.message}") }
    }

    fun pruneAllocations() {
        transaction {
            AllocationConsists.deleteWhere {
                AllocationConsists.updatedAt less LocalDateTime.now().minusHours(48)
            }
        }
    }

    /** Prune allocation history older than 30 days. */
    fun pruneAllocationHistory() {
        transaction {
            AllocationHistory.deleteWhere {
                AllocationHistory.recordedAt less LocalDateTime.now().minusDays(30)
            }
        }
    }




    fun getHspUnit(rid: String, scheduledDep: String = "", originCrs: String = "", scheduledArr: String = "", destTiploc: String = ""): AllocationResult? {
        if (rid.length < 9) return null
        val date = rid.substring(0, 8).let {
            "${it.substring(0, 4)}-${it.substring(4, 6)}-${it.substring(6, 8)}"
        }
        // Strategy 0: Direct RID → UID via Darwin ridToUid map → allocation lookup.
        // Checks in-memory map first (live trains), then DB (historical, survives restarts).
        val directUid = com.traintracker.server.kafka.ridToUid[rid]
            ?: getDarwinUidForRid(rid)
        if (directUid != null) {
            val directAlloc = getAllocationByUid(directUid, date)
            if (directAlloc != null) return directAlloc
        }
        // Strategy 1: allocation_history suffix match — fallback for services Darwin didn't cover
        // RID last 5 digits match the numeric portion of the UID (e.g. RID ...21687 → UID C21687)
        val ridSuffix5 = rid.takeLast(5)
        val ridSuffix7 = rid.takeLast(7)
        var result1: AllocationResult? = null
        transaction {
            exec("SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count FROM allocation_history WHERE (core_id LIKE '%$ridSuffix5%' OR core_id LIKE '%$ridSuffix7%') AND service_date = '$date' LIMIT 1") { rs ->
                if (rs.next()) result1 = extractAllocationFromRs(rs, rs.getString(1) ?: "", date)
            }
        }
        if (result1 != null) return result1

        return null
    }

    /** HSP persistent cache — keyed by route+date+time params. */
    fun getHspCache(key: String): String? = transaction {
        HspCache.selectAll().where { HspCache.cacheKey eq key }
            .singleOrNull()?.get(HspCache.responseJson)
    }

    fun setHspCache(key: String, json: String) = transaction {
        HspCache.upsert {
            it[cacheKey]     = key
            it[responseJson] = json
            it[cachedAt]     = java.time.Instant.now().toString()
        }
    }

    /** Persist and reload stanox→CRS map so it survives restarts.
     *  Stored in hsp_cache table (text column) to avoid varchar(256) limit of cif_meta. */
    fun saveStanoxMap(map: Map<String, String>) = transaction {
        if (map.isEmpty()) return@transaction
        val json = org.json.JSONObject()
        map.forEach { (k, v) -> json.put(k, v) }
        HspCache.replace {
            it[HspCache.cacheKey]     = "stanox_map"
            it[HspCache.responseJson] = json.toString()
            it[HspCache.cachedAt]     = java.time.Instant.now().toString()
        }
    }

    fun loadStanoxMap(): Map<String, String> = transaction {
        val raw = HspCache.selectAll().where { HspCache.cacheKey eq "stanox_map" }
            .singleOrNull()?.get(HspCache.responseJson) ?: return@transaction emptyMap()
        try {
            val json = org.json.JSONObject(raw)
            json.keySet().associateWith { json.getString(it) }
        } catch (_: Exception) { emptyMap() }
    }

    /** Persist and reload trainId→uid activation map so it survives restarts. */
    fun saveTrainActivation(trainId: String, uid: String) {
        val now = java.time.Instant.now().toString().take(19)
        try {
            transaction {
                exec("INSERT INTO train_activations(train_id, uid, updated_at) VALUES('$trainId', '$uid', '$now') ON CONFLICT(train_id) DO UPDATE SET uid=excluded.uid, updated_at=excluded.updated_at")
            }
        } catch (e: Exception) { log.debug("saveTrainActivation error: ${e.message}") }
    }

    fun loadTrainActivations(): Map<String, String> {
        val result = mutableMapOf<String, String>()
        try {
            transaction {
                exec("SELECT train_id, uid FROM train_activations WHERE updated_at >= datetime('now', '-1 day')") { rs ->
                    while (rs.next()) {
                        val trainId = rs.getString("train_id") ?: return@exec
                        val uid     = rs.getString("uid")      ?: return@exec
                        result[trainId] = uid
                    }
                }
            }
        } catch (_: Exception) {}
        return result
    }

    fun pruneTrainActivations() {
        try {
            transaction { exec("DELETE FROM train_activations WHERE updated_at < datetime('now', '-1 day')") }
        } catch (e: Exception) { log.debug("pruneTrainActivations error: ${e.message}") }
    }


    /** Read/write CIF metadata. */
    fun getEnrichedStatus(trustConnected: Boolean, trainLocCount: Int): com.traintracker.server.routes.StatusResponse {
        val cifDate = getCifMeta("last_download_date")
        var scheduleCount = 0; var uniqueServiceCount = 0; var trustCount = 0
        var allocToday = 0
        val today = java.time.LocalDate.now().toString()
        transaction {
            exec("SELECT COUNT(*) FROM schedules") { rs -> if (rs.next()) scheduleCount = rs.getInt(1) }
            exec("SELECT COUNT(DISTINCT uid) FROM schedules") { rs -> if (rs.next()) uniqueServiceCount = rs.getInt(1) }
            exec("SELECT COUNT(*) FROM trust_movements") { rs -> if (rs.next()) trustCount = rs.getInt(1) }
            exec("SELECT COUNT(DISTINCT headcode) FROM allocation_history WHERE service_date='$today'") { rs -> if (rs.next()) allocToday = rs.getInt(1) }
        }
        return com.traintracker.server.routes.StatusResponse(
            cifLastDownload     = cifDate,
            trustConnected      = trustConnected,
            trainLocationsCount = trainLocCount,
            scheduleCount       = scheduleCount,
            uniqueServiceCount  = uniqueServiceCount,
            trustMovementCount  = trustCount,
            allocationsToday    = allocToday,
            hspUnitCount        = 0,
            tiplocNameCount     = com.traintracker.server.cif.CorpusLookup.tiplocNameCount()
        )
    }

    fun getCifMeta(key: String): String? = transaction {
        CifMeta.selectAll().where { CifMeta.key eq key }.singleOrNull()?.get(CifMeta.value)
    }

    fun setCifMeta(key: String, value: String) = transaction {
        CifMeta.replace {
            it[CifMeta.key]   = key
            it[CifMeta.value] = value
        }
    }

    /** Replace all CIF associations (called after each full CIF download). */
    fun replaceAssociations(records: List<AssociationRecord>) {
        if (records.isEmpty()) return
        transaction {
            CifAssociations.deleteAll()
            CifAssociations.batchInsert(records, ignore = true) { r ->
                this[CifAssociations.mainUid]     = r.mainUid
                this[CifAssociations.assocUid]    = r.assocUid
                this[CifAssociations.assocTiploc] = r.assocTiploc
                this[CifAssociations.assocType]   = r.assocType
                this[CifAssociations.stpInd]      = r.stpInd
            }
        }
        log.info("replaceAssociations: ${records.size} records stored")
    }

    private fun queryAssociationsBy(uids: Set<String>, byMain: Boolean): Map<String, List<AssociationRecord>> {
        if (uids.isEmpty()) return emptyMap()
        val result = mutableMapOf<String, MutableList<AssociationRecord>>()
        val col = if (byMain) CifAssociations.mainUid else CifAssociations.assocUid
        transaction {
            CifAssociations.selectAll().where {
                (col inList uids) and (CifAssociations.stpInd neq 'C')
            }.forEach { row ->
                val rec = AssociationRecord(
                    mainUid     = row[CifAssociations.mainUid],
                    assocUid    = row[CifAssociations.assocUid],
                    assocTiploc = row[CifAssociations.assocTiploc],
                    assocType   = row[CifAssociations.assocType],
                    stpInd      = row[CifAssociations.stpInd]
                )
                result.getOrPut(if (byMain) rec.mainUid else rec.assocUid) { mutableListOf() }.add(rec)
            }
        }
        return result
    }

    fun upsertDarwinPlatforms(uid: String, rid: String, platforms: Map<String, String>) {
        if (platforms.isEmpty()) return
        val now = java.time.Instant.now().toString().take(19)
        try {
            transaction {
                for ((tiploc, platform) in platforms) {
                    val safePlat = platform.replace("'", "''")
                    exec("INSERT INTO darwin_platforms(uid, rid, tiploc, platform, updated_at) VALUES('$uid', '$rid', '$tiploc', '$safePlat', '$now') ON CONFLICT(uid, tiploc) DO UPDATE SET platform=excluded.platform, rid=excluded.rid, updated_at=excluded.updated_at")
                }
            }
        } catch (e: Exception) { log.debug("Darwin DB upsert error: ${e.message}") }
    }

    fun getDarwinPlatform(uid: String, tiploc: String): String? {
        return try {
            transaction {
                var result: String? = null
                exec("SELECT platform FROM darwin_platforms WHERE uid='$uid' AND tiploc='$tiploc' AND updated_at >= datetime('now', '-24 hours')") { rs ->
                    if (rs.next()) result = rs.getString("platform")
                }
                result
            }
        } catch (e: Exception) { null }
    }

    fun upsertDarwinRidUid(rid: String, uid: String) {
        val now = java.time.Instant.now().toString().take(19)
        try {
            transaction {
                exec("INSERT INTO darwin_rid_uid(rid, uid, updated_at) VALUES('$rid', '$uid', '$now') ON CONFLICT(rid) DO UPDATE SET uid=excluded.uid, updated_at=excluded.updated_at")
            }
        } catch (e: Exception) { log.debug("DarwinRidUid upsert error: ${e.message}") }
    }

    fun loadDarwinRidUid(): Map<String, String> {
        val result = mutableMapOf<String, String>()
        try {
            transaction {
                exec("SELECT rid, uid FROM darwin_rid_uid") { rs ->
                    while (rs.next()) {
                        val rid = rs.getString("rid") ?: return@exec
                        val uid = rs.getString("uid") ?: return@exec
                        result[rid] = uid
                    }
                }
            }
        } catch (_: Exception) {}
        return result
    }

    fun getDarwinUidForRid(rid: String): String? {
        return try {
            transaction {
                var result: String? = null
                exec("SELECT uid FROM darwin_rid_uid WHERE rid='$rid'") { rs ->
                    if (rs.next()) result = rs.getString("uid")
                }
                result
            }
        } catch (e: Exception) { null }
    }

    fun cleanupDarwinRidUid() {
        try {
            transaction { exec("DELETE FROM darwin_rid_uid WHERE updated_at < datetime('now', '-30 days')") }
        } catch (e: Exception) { log.debug("DarwinRidUid cleanup error: ${e.message}") }
    }

    fun cleanupDarwinPlatforms() {
        try {
            transaction { exec("DELETE FROM darwin_platforms WHERE updated_at < datetime('now', '-24 hours')") }
        } catch (e: Exception) { log.debug("Darwin cleanup error: ${e.message}") }
    }

    fun getAssociationsForUids(uids: Set<String>)        = queryAssociationsBy(uids, byMain = true)
    fun getReverseAssociationsForUids(uids: Set<String>) = queryAssociationsBy(uids, byMain = false)

}

fun crsfromTiplocFallback(tiploc: String): String? {
    return try {
        transaction {
            Schedules.select(Schedules.crs)
                .where { (Schedules.tiploc eq tiploc) and (Schedules.crs neq "") }
                .limit(1)
                .firstOrNull()
                ?.get(Schedules.crs)
                ?.takeIf { it.isNotEmpty() }
        }
    } catch (e: Exception) { null }
}

// ─── Train activations table ─────────────────────────────────────────────────

object TrainActivations : org.jetbrains.exposed.sql.Table("train_activations") {
    val trainId   = varchar("train_id",   10)
    val uid       = varchar("uid",        10)
    val updatedAt = varchar("updated_at", 20)
    override val primaryKey = PrimaryKey(trainId)
}

// ─── Darwin RID → UID mapping table ─────────────────────────────────────────────

object DarwinRidUid : org.jetbrains.exposed.sql.Table("darwin_rid_uid") {
    val rid       = varchar("rid", 20)
    val uid       = varchar("uid", 10)
    val updatedAt = varchar("updated_at", 20)
    override val primaryKey = PrimaryKey(rid)
}

// ─── Darwin platform table ────────────────────────────────────────────────────

object DarwinPlatforms : org.jetbrains.exposed.sql.Table("darwin_platforms") {
    val uid       = varchar("uid",     10)
    val rid       = varchar("rid",     20)
    val tiploc    = varchar("tiploc",  10)
    val platform  = varchar("platform", 10)
    val updatedAt = varchar("updated_at", 20)
    override val primaryKey = PrimaryKey(uid, tiploc)
}

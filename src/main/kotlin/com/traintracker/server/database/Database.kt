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
}

object HeadcodeAliases : Table("headcode_aliases") {
    val oldHeadcode = varchar("old_headcode", 4)
    val newHeadcode = varchar("new_headcode", 4)
    val updatedAt   = datetime("updated_at")
    override val primaryKey = PrimaryKey(oldHeadcode, name = "pk_headcode_aliases")
}

object HspUnitHistory : Table("hsp_unit_history") {
    val coreId        = varchar("core_id", 20)
    val headcode      = varchar("headcode", 6)
    val serviceDate   = varchar("service_date", 10)
    val scheduledDep  = varchar("scheduled_dep", 5).default("")
    val uid           = varchar("uid", 10).default("")
    val units         = varchar("units", 500)
    val vehicles      = varchar("vehicles", 2000)
    val unitCount     = integer("unit_count").default(0)
    val snapshottedAt = datetime("snapshotted_at")
    val originCrs     = varchar("origin_crs", 3).default("")
    val destCrs       = varchar("dest_crs", 3).default("")

    override val primaryKey = PrimaryKey(coreId, name = "pk_hsp_unit_history")
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
    val platform: String?, val isCancelled: Boolean, val cancelReason: String? = null,
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

    private fun createTables() {
        transaction {
            SchemaUtils.createMissingTablesAndColumns(HspCache,
                Schedules, TrustMovements, TrainLocations, CifMeta, AllocationConsists, HeadcodeAliases, HspUnitHistory, AllocationHistory, CifAssociations
            )
            try {
                exec("CREATE INDEX IF NOT EXISTS idx_sched_crs_time ON schedules(crs, scheduled_time)")
                exec("CREATE INDEX IF NOT EXISTS idx_trust_hc ON trust_movements(headcode)")
                exec("CREATE INDEX IF NOT EXISTS idx_trust_ts ON trust_movements(event_ts)")
                exec("CREATE INDEX IF NOT EXISTS idx_trust_crs_time ON trust_movements(crs, scheduled_time)")
                exec("CREATE INDEX IF NOT EXISTS idx_alloc_hc_date ON allocation_consists(headcode, service_date)")
                // Migration: add uid column to hsp_unit_history if not present
                try { exec("ALTER TABLE hsp_unit_history ADD COLUMN uid TEXT NOT NULL DEFAULT ''") } catch (_: Exception) {}
                try { exec("ALTER TABLE hsp_unit_history ADD COLUMN origin_crs TEXT NOT NULL DEFAULT ''") } catch (_: Exception) {}
                try { exec("ALTER TABLE hsp_unit_history ADD COLUMN dest_crs TEXT NOT NULL DEFAULT ''") } catch (_: Exception) {}
                exec("CREATE INDEX IF NOT EXISTS idx_hsp_unit_date_dep ON hsp_unit_history(service_date, scheduled_dep)")
                exec("CREATE INDEX IF NOT EXISTS idx_hsp_unit_date_uid ON hsp_unit_history(service_date, uid)")
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

    /** Purge trust_movements rows from before today's 02:00 — called daily at 02:00. */
    fun purgeStaleMovements() {
        val now = java.time.LocalDateTime.now(java.time.ZoneId.of("Europe/London"))
        val cutoff = now.toLocalDate().atTime(2, 0)
            .let { if (now.hour < 2) it.minusDays(1) else it }
        transaction {
            TrustMovements.deleteWhere {
                TrustMovements.eventTs less cutoff
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
            val alreadyRecorded = AllocationHistory.selectAll().where {
                (AllocationHistory.coreId eq coreId) and (AllocationHistory.serviceDate eq serviceDate)
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

    /** Look up by coreId directly. */
    fun getAllocationByUid(uid: String, date: String? = null): AllocationResult? = transaction {
        val safeUid = uid.filter { it.isLetterOrDigit() }
        val safeDate = date?.filter { it.isDigit() || it == '-' } ?: java.time.LocalDate.now().toString()
        var result: AllocationResult? = null
        exec("SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count FROM allocation_consists WHERE core_id LIKE '%$safeUid%' AND service_date = '$safeDate' UNION ALL SELECT core_id, headcode, service_date, operator, vehicles, units, unit_count FROM allocation_history WHERE core_id LIKE '%$safeUid%' AND service_date = '$safeDate' AND NOT EXISTS (SELECT 1 FROM allocation_consists WHERE core_id LIKE '%$safeUid%' AND service_date = '$safeDate') ORDER BY service_date DESC LIMIT 1") { rs ->
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
        // Fallback: check hsp_unit_history by uid + date
        if (result == null) {
            exec("SELECT uid, units, vehicles, unit_count FROM hsp_unit_history WHERE uid = '$safeUid' AND service_date = '$safeDate' LIMIT 1") { rs ->
                if (rs.next()) {
                    val allUnits    = (rs.getString("units") ?: "").split(",").filter { it.isNotEmpty() }
                    val allVehicles = (rs.getString("vehicles") ?: "").split(",").filter { it.isNotEmpty() }
                    val declaredCount = rs.getInt("unit_count")
                    val (units, vehicles, unitCount) = correctAllocation(allUnits, allVehicles, declaredCount)
                    result = AllocationResult(
                        coreId      = rs.getString("uid") ?: "",
                        headcode    = "",
                        serviceDate = safeDate,
                        operator    = "",
                        vehicles    = vehicles,
                        units       = units,
                        unitCount   = unitCount
                    )
                }
            }
        }
        result
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
        .map { row ->
            AllocationResult(
                coreId      = row[AllocationHistory.coreId],
                headcode    = row[AllocationHistory.headcode],
                serviceDate = row[AllocationHistory.serviceDate],
                operator    = row[AllocationHistory.operator],
                vehicles    = row[AllocationHistory.vehicles].split(",").filter { it.isNotEmpty() },
                units       = row[AllocationHistory.units].split(",").filter { it.isNotEmpty() },
                unitCount   = row[AllocationHistory.unitCount]
            )
        }
    }

    /** Prune allocation consists older than 48 hours. */
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


    fun snapshotUnitAllocations(date: String) {
        val dbPath = "/opt/traintracker/traintracker.db"
        val sql = """
            INSERT OR REPLACE INTO hsp_unit_history
                (core_id, headcode, service_date, scheduled_dep, uid, units, vehicles, unit_count, snapshotted_at, origin_crs, dest_crs)
            SELECT
                a.core_id,
                a.headcode,
                a.service_date,
                COALESCE((
                    SELECT s.scheduled_time FROM schedules s
                    WHERE s.uid = SUBSTR(a.core_id, LENGTH(a.headcode) + 1, 6) AND s.stop_type = 'LO'
                    AND s.stp_indicator != 'C'
                    ORDER BY CASE s.stp_indicator WHEN 'N' THEN 0 WHEN 'O' THEN 1 WHEN 'P' THEN 2 ELSE 3 END
                    LIMIT 1
                ), '') AS scheduled_dep,
                SUBSTR(a.core_id, LENGTH(a.headcode) + 1, 6) AS uid,
                a.units,
                a.vehicles,
                a.unit_count,
                datetime('now'),
                COALESCE((
                    SELECT s.crs FROM schedules s
                    WHERE s.uid = SUBSTR(a.core_id, LENGTH(a.headcode) + 1, 6) AND s.stop_type = 'LO'
                    AND s.stp_indicator != 'C'
                    ORDER BY CASE s.stp_indicator WHEN 'N' THEN 0 WHEN 'O' THEN 1 WHEN 'P' THEN 2 ELSE 3 END
                    LIMIT 1
                ), '') AS origin_crs,
                COALESCE((
                    SELECT s.crs FROM schedules s
                    WHERE s.uid = SUBSTR(a.core_id, LENGTH(a.headcode) + 1, 6) AND s.stop_type = 'LT'
                    AND s.stp_indicator != 'C'
                    ORDER BY CASE s.stp_indicator WHEN 'N' THEN 0 WHEN 'O' THEN 1 WHEN 'P' THEN 2 ELSE 3 END
                    LIMIT 1
                ), '') AS dest_crs
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
                HspUnitHistory.selectAll().where { HspUnitHistory.serviceDate eq date }.count()
            }
            log.info("snapshotUnitAllocations: wrote $count rows for $date")
        }
    }

        /**
         * Resolve an origin CRS for a RID by scanning hsp_cache metrics entries.
         * Falls back to CorpusLookup to convert tiploc -> CRS.
         */
        /**
         * Resolve origin CRS for a RID.
         * Strategy A: look up the UID (suffix after date in RID) in the schedules table LO stop.
         * Strategy B: scan hsp_cache metrics JSON for a matching rid entry.
         * HSP metrics cache stores originTiploc which for EMR/NR services is already a CRS code.
         */
        private fun resolveOriginCrs(rid: String): String {
            // Use SQLite JSON functions to extract originTiploc directly for the matching RID
            val safeRid = rid.filter { it.isLetterOrDigit() }
            val sql = "SELECT json_extract(value, '$.originCrs') as ocrs, " +
                      "json_extract(value, '$.originTiploc') as otiploc " +
                      "FROM hsp_cache, json_each(hsp_cache.response_json) " +
                      "WHERE cache_key NOT LIKE 'act:%' AND cache_key NOT LIKE 'stanox%' " +
                      "AND json_extract(value, '$.rid') = '$safeRid' LIMIT 1"
            return try {
                var crs = ""
                transaction {
                    exec(sql) { rs ->
                        if (rs.next()) {
                            val rawCrs    = rs.getString("ocrs")?.trim() ?: ""
                            val rawTiploc = rs.getString("otiploc")?.trim() ?: ""
                            crs = rawCrs.ifEmpty { rawTiploc }
                        }
                    }
                }
                crs
            } catch (_: Exception) { "" }
        }

        fun getHspUnit(rid: String, scheduledDep: String = "", originCrs: String = "", scheduledArr: String = "", destTiploc: String = ""): AllocationResult? {
        // RID format: YYYYMMDD + numeric suffix (e.g. 202603306751542)
        if (rid.length < 9) return null
        val date = rid.substring(0, 8).let {
            "${it.substring(0, 4)}-${it.substring(4, 6)}-${it.substring(6, 8)}"
        }
        val resolvedOriginCrs = originCrs.ifEmpty { resolveOriginCrs(rid) }
        val ridSuffix = rid.takeLast(5)

        // Strategy 1: RID suffix match against hsp_unit_history.
        // Fastest path — uses snapshotted data, no schedules dependency.
        var result1: AllocationResult? = null
        transaction {
            exec("SELECT uid, units, vehicles, unit_count FROM hsp_unit_history " +
                 "WHERE service_date = '$date' AND uid LIKE '%$ridSuffix' LIMIT 1") { rs ->
                if (rs.next()) {
                    val rawUnits    = (rs.getString("units") ?: "").split(",").filter { it.isNotEmpty() }
                    val rawVehicles = (rs.getString("vehicles") ?: "").split(",").filter { it.isNotEmpty() }
                    val rawCount    = rs.getInt("unit_count")
                    val (units, vehicles, unitCount) = correctAllocation(rawUnits, rawVehicles, rawCount)
                    result1 = AllocationResult(rs.getString("uid") ?: "", "", date, "", vehicles, units, unitCount)
                }
            }
        }
        if (result1 != null) return result1

        // Strategy 2: RID suffix match against allocation_history.
        // Catches services where allocation arrived after the nightly snapshot ran,
        // or where the snapshot schedule join failed (e.g. altered WTT services).
        var result2: AllocationResult? = null
        transaction {
            exec("SELECT units, vehicles, unit_count FROM allocation_history " +
                 "WHERE service_date = '$date' AND core_id LIKE '%$ridSuffix%' LIMIT 1") { rs ->
                if (rs.next()) {
                    val rawUnits    = (rs.getString("units") ?: "").split(",").filter { it.isNotEmpty() }
                    val rawVehicles = (rs.getString("vehicles") ?: "").split(",").filter { it.isNotEmpty() }
                    val rawCount    = rs.getInt("unit_count")
                    val (units, vehicles, unitCount) = correctAllocation(rawUnits, rawVehicles, rawCount)
                    result2 = AllocationResult(rid, "", date, "", vehicles, units, unitCount)
                }
            }
        }
        if (result2 != null) return result2

        // Strategy 3: scheduled departure + origin CRS match against hsp_unit_history.
        // Fallback for cases where the RID suffix doesn't match directly.
        if (scheduledDep.isNotEmpty() && resolvedOriginCrs.isNotEmpty()) {
            var result3: AllocationResult? = null
            transaction {
                exec("SELECT uid, units, vehicles, unit_count FROM hsp_unit_history " +
                     "WHERE service_date = '$date' AND scheduled_dep = '$scheduledDep' " +
                     "AND origin_crs = '$resolvedOriginCrs' LIMIT 1") { rs ->
                    if (rs.next()) {
                        val rawUnits    = (rs.getString("units") ?: "").split(",").filter { it.isNotEmpty() }
                        val rawVehicles = (rs.getString("vehicles") ?: "").split(",").filter { it.isNotEmpty() }
                        val rawCount    = rs.getInt("unit_count")
                        val (units, vehicles, unitCount) = correctAllocation(rawUnits, rawVehicles, rawCount)
                        result3 = AllocationResult(rs.getString("uid") ?: "", "", date, "", vehicles, units, unitCount)
                    }
                }
            }
            if (result3 != null) return result3
        }

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
    fun saveTrainActivation(trainId: String, uid: String) = transaction {
        HspCache.replace {
            it[HspCache.cacheKey]     = "act:$trainId"
            it[HspCache.responseJson] = uid
            it[HspCache.cachedAt]     = java.time.Instant.now().toString()
        }
    }

    fun loadTrainActivations(): Map<String, String> = transaction {
        val result = mutableMapOf<String, String>()
        try {
            exec("SELECT cache_key, response_json FROM hsp_cache WHERE cache_key LIKE 'act:%' AND cached_at >= datetime('now', '-1 day')") { rs ->
                while (rs.next()) {
                    val key = rs.getString("cache_key")?.removePrefix("act:") ?: return@exec
                    val uid = rs.getString("response_json") ?: return@exec
                    result[key] = uid
                }
            }
        } catch (_: Exception) {}
        result
    }

    fun pruneTrainActivations() = transaction {
        try {
            exec("DELETE FROM hsp_cache WHERE cache_key LIKE 'act:%' AND cached_at < datetime('now', '-1 day')")
        } catch (_: Exception) {}
    }

    /** Read/write CIF metadata. */
    fun getEnrichedStatus(trustConnected: Boolean, trainLocCount: Int): com.traintracker.server.routes.StatusResponse {
        val cifDate = getCifMeta("last_download_date")
        var scheduleCount = 0; var uniqueServiceCount = 0; var trustCount = 0
        var allocToday = 0; var hspCount = 0
        val today = java.time.LocalDate.now().toString()
        transaction {
            exec("SELECT COUNT(*) FROM schedules") { rs -> if (rs.next()) scheduleCount = rs.getInt(1) }
            exec("SELECT COUNT(DISTINCT uid) FROM schedules") { rs -> if (rs.next()) uniqueServiceCount = rs.getInt(1) }
            exec("SELECT COUNT(*) FROM trust_movements") { rs -> if (rs.next()) trustCount = rs.getInt(1) }
            exec("SELECT COUNT(DISTINCT headcode) FROM allocation_history WHERE service_date='$today'") { rs -> if (rs.next()) allocToday = rs.getInt(1) }
            exec("SELECT COUNT(*) FROM hsp_unit_history") { rs -> if (rs.next()) hspCount = rs.getInt(1) }
        }
        return com.traintracker.server.routes.StatusResponse(
            cifLastDownload     = cifDate,
            trustConnected      = trustConnected,
            trainLocationsCount = trainLocCount,
            scheduleCount       = scheduleCount,
            uniqueServiceCount  = uniqueServiceCount,
            trustMovementCount  = trustCount,
            allocationsToday    = allocToday,
            hspUnitCount        = hspCount,
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

    /**
     * Return associations keyed by mainUid for a set of UIDs.
     * Excludes cancelled (STP=C) associations.
     */
    fun getReverseAssociationsForUids(uids: Set<String>): Map<String, List<AssociationRecord>> {
        if (uids.isEmpty()) return emptyMap()
        val result = mutableMapOf<String, MutableList<AssociationRecord>>()
        transaction {
            CifAssociations.selectAll().where {
                (CifAssociations.assocUid inList uids) and
                (CifAssociations.stpInd neq 'C')
            }.forEach { row ->
                val rec = AssociationRecord(
                    mainUid     = row[CifAssociations.mainUid],
                    assocUid    = row[CifAssociations.assocUid],
                    assocTiploc = row[CifAssociations.assocTiploc],
                    assocType   = row[CifAssociations.assocType],
                    stpInd      = row[CifAssociations.stpInd]
                )
                result.getOrPut(rec.assocUid) { mutableListOf() }.add(rec)
            }
        }
        return result
    }

    fun getAssociationsForUids(uids: Set<String>): Map<String, List<AssociationRecord>> {
        if (uids.isEmpty()) return emptyMap()
        val result = mutableMapOf<String, MutableList<AssociationRecord>>()
        transaction {
            CifAssociations.selectAll().where {
                (CifAssociations.mainUid inList uids) and
                (CifAssociations.stpInd neq 'C')
            }.forEach { row ->
                val rec = AssociationRecord(
                    mainUid     = row[CifAssociations.mainUid],
                    assocUid    = row[CifAssociations.assocUid],
                    assocTiploc = row[CifAssociations.assocTiploc],
                    assocType   = row[CifAssociations.assocType],
                    stpInd      = row[CifAssociations.stpInd]
                )
                result.getOrPut(rec.mainUid) { mutableListOf() }.add(rec)
            }
        }
        return result
    }

}

fun crsfromTiplocFallback(tiploc: String): String? {
    return try {
        transaction {
            Schedules.slice(Schedules.crs)
                .select { (Schedules.tiploc eq tiploc) and (Schedules.crs neq "") }
                .limit(1)
                .firstOrNull()
                ?.get(Schedules.crs)
                ?.takeIf { it.isNotEmpty() }
        }
    } catch (e: Exception) { null }
}

package com.traintracker.server.database

import com.traintracker.server.Config
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.routes.StatusResponse
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.greaterEq
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.core.less
import org.jetbrains.exposed.v1.core.neq
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.batchReplace
import org.jetbrains.exposed.v1.jdbc.deleteAll
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.replace
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.statements.jdbc.JdbcConnectionImpl
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.upsert
import org.json.JSONObject
import org.slf4j.LoggerFactory

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

data class AssociationRecord(
    val mainUid: String,
    val assocUid: String,
    val assocTiploc: String,
    val assocType: String,
    val stpInd: Char,
    val dateIndicator: Char? = null,
)

data class ActivationInfo(val updatedAt: String, val originStanox: String?)

/** Latest TRUST activation of each uid on the service day [date]. */
fun JdbcTransaction.activationsFor(uids: Collection<String>, date: String): Map<String, ActivationInfo> {
    val result = mutableMapOf<String, ActivationInfo>()
    uids.chunked(500).forEach { chunk ->
        query(
            "SELECT uid, updated_at, origin_stanox FROM train_activations WHERE uid IN (${placeholders(chunk.size)}) AND ${serviceDaySql("updated_at")} ORDER BY updated_at DESC",
            chunk + serviceDayArgs(date)
        ) { rs ->
            while (rs.next()) {
                result.putIfAbsent(rs.getString("uid"), ActivationInfo(rs.getString("updated_at"), rs.getString("origin_stanox")))
            }
        }
    }
    return result
}

object AppDatabase {
    private val log = LoggerFactory.getLogger("Database")

    @Volatile
    private var statusCache: Pair<Long, StatusResponse>? = null

    fun init() {
        val pool = HikariDataSource(HikariConfig().apply {
            jdbcUrl = "jdbc:sqlite:${Config.sqlitePath}?busy_timeout=5000&journal_mode=WAL&synchronous=FULL&cache_size=-32768" +
                "&temp_store=MEMORY&mmap_size=268435456&journal_size_limit=67108864"
            driverClassName = "org.sqlite.JDBC"
            maximumPoolSize = 10
        })
        Database.connect(pool)
        Schema.create()
        log.info("Database ready (SQLite)")
    }

    // ---- Retention -----------------------------------------------------------------------------------

    /** Hourly clean-up. The first group shares one failure path (an error stops the rest); the second does not. */
    fun pruneExpired() {
        transaction { TrustMovements.deleteWhere { TrustMovements.eventTs less LocalDateTime.now().minusDays(30L) } }
        transaction { TrainLocations.deleteWhere { TrainLocations.updatedAt less LocalDateTime.now().minusHours(24L) } }
        transaction { AllocationConsists.deleteWhere { AllocationConsists.updatedAt less LocalDateTime.now(ZoneOffset.UTC).minusDays(30L) } }
        transaction { AllocationLegs.deleteWhere { AllocationLegs.updatedAt less LocalDateTime.now().minusDays(30L) } }
        transaction { AllocationHistory.deleteWhere { AllocationHistory.recordedAt less LocalDateTime.now().minusDays(30L) } }
        transaction { AllocationChanges.deleteWhere { AllocationChanges.changedAt less LocalDateTime.now(ZoneOffset.UTC).minusDays(30L) } }
        listOf(
            "DELETE FROM darwin_platforms WHERE updated_at < datetime('now', '-24 hours')",
            "DELETE FROM darwin_rid_uid WHERE updated_at < datetime('now', '-30 days')",
            "DELETE FROM hsp_cache WHERE cached_at < datetime('now', '-30 days') OR cache_key LIKE 'act:%'",
            "DELETE FROM train_activations WHERE updated_at < datetime('now', '-30 days')",
            "DELETE FROM schedules WHERE stp_indicator = 'N' AND schedule_end_date < date('now', '-30 days')",
            "DELETE FROM cif_change_alerts WHERE dismissed_at IS NOT NULL AND dismissed_at < datetime('now', '-7 days')",
            "DELETE FROM feedback WHERE dismissed_at IS NOT NULL AND dismissed_at < datetime('now', '-30 days')",
        ).forEach { sql ->
            try {
                transaction { exec(sql) }
            } catch (e: Exception) {
                log.debug("Prune failed ($sql): ${e.message}")
            }
        }
    }

    // ---- TRUST ---------------------------------------------------------------------------------------

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

    fun upsertLocation(headcode: String, rid: String, stationName: String, crs: String?, actualTime: String, eventType: String, delayMinutes: Int) {
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

    /** Moves a train's last known location to its new headcode after a TRUST change of identity. */
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

    fun saveTrainActivation(trainId: String, uid: String, originStanox: String? = null) {
        val origin = originStanox?.replace("'", "")?.ifEmpty { null }
        try {
            transaction {
                execUpdate(
                    "INSERT INTO train_activations(train_id, uid, updated_at, origin_stanox) VALUES(?, ?, ?, ?) ON CONFLICT(train_id) DO UPDATE SET uid=excluded.uid, updated_at=excluded.updated_at, origin_stanox=excluded.origin_stanox",
                    listOf(trainId, uid, utcStamp(), origin)
                )
            }
        } catch (e: Exception) {
            log.debug("saveTrainActivation error: ${e.message}")
        }
    }

    /** Train ID -> uid for activations in the last day. */
    fun loadTrainActivations(): Map<String, String> = try {
        transaction {
            queryList("SELECT train_id, uid FROM train_activations WHERE updated_at >= datetime('now', '-1 day')") {
                it.getString("train_id") to it.getString("uid")
            }.toMap()
        }
    } catch (e: Exception) {
        emptyMap()
    }

    /** uids that TRUST activated on the service day [date]. */
    fun activatedUids(uids: Collection<String>, date: String): Set<String> {
        if (uids.isEmpty()) return emptySet()
        return try {
            transaction {
                queryList(
                    "SELECT DISTINCT uid FROM train_activations WHERE uid IN (${placeholders(uids.size)}) AND ${serviceDaySql("updated_at")}",
                    uids.toList() + serviceDayArgs(date)
                ) { it.getString("uid") }.toSet()
            }
        } catch (e: Exception) {
            emptySet()
        }
    }

    /** (uid, CRS) -> platform from TRUST movements of the last 12 hours; the latest report wins. */
    fun trustPlatforms(uids: Collection<String>): Map<Pair<String, String>, String> {
        if (uids.isEmpty()) return emptyMap()
        return try {
            transaction {
                val result = mutableMapOf<Pair<String, String>, String>()
                query(
                    "SELECT uid, crs, stanox, platform FROM trust_movements WHERE uid IN (${placeholders(uids.size)}) AND platform IS NOT NULL AND platform != '' AND event_ts >= datetime('now', '-12 hours') ORDER BY event_ts ASC",
                    uids.toList()
                ) { rs ->
                    while (rs.next()) {
                        val uid = rs.getString("uid") ?: continue
                        val rawCrs = rs.getString("crs")
                        val stanox = rs.getString("stanox") ?: continue
                        val crs = rawCrs?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromStanox(stanox) ?: continue
                        result[uid to crs] = rs.getString("platform") ?: continue
                    }
                }
                result
            }
        } catch (e: Exception) {
            emptyMap()
        }
    }

    // ---- Darwin --------------------------------------------------------------------------------------

    fun upsertDarwinPlatforms(uid: String, rid: String, platforms: Map<String, String>) {
        if (platforms.isEmpty()) return
        val now = utcStamp()
        try {
            transaction {
                // A JDBC batch: one Darwin message can carry dozens of platforms.
                val conn = (connection as JdbcConnectionImpl).connection
                conn.prepareStatement(
                    "INSERT INTO darwin_platforms(uid, rid, tiploc, platform, updated_at) VALUES(?, ?, ?, ?, ?) ON CONFLICT(uid, tiploc) DO UPDATE SET platform=excluded.platform, rid=excluded.rid, updated_at=excluded.updated_at"
                ).use { stmt ->
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

    /** (uid, TIPLOC) -> Darwin platform, from the last 24 hours. */
    fun darwinPlatforms(uids: Collection<String>): Map<Pair<String, String>, String> {
        if (uids.isEmpty()) return emptyMap()
        return try {
            transaction {
                queryList(
                    "SELECT uid, tiploc, platform FROM darwin_platforms WHERE uid IN (${placeholders(uids.size)}) AND updated_at >= datetime('now', '-24 hours')",
                    uids.toList()
                ) { rs ->
                    val uid = rs.getString("uid") ?: return@queryList null
                    val tiploc = rs.getString("tiploc") ?: return@queryList null
                    val platform = rs.getString("platform") ?: return@queryList null
                    (uid to tiploc) to platform
                }.toMap()
            }
        } catch (e: Exception) {
            emptyMap()
        }
    }

    /** TIPLOC -> Darwin platform for one uid, from the last 24 hours. */
    fun darwinPlatforms(uid: String, tiplocs: Collection<String>): Map<String, String> {
        if (tiplocs.isEmpty()) return emptyMap()
        return try {
            transaction {
                queryList(
                    "SELECT tiploc, platform FROM darwin_platforms WHERE uid=? AND tiploc IN (${placeholders(tiplocs.size)}) AND updated_at >= datetime('now', '-24 hours')",
                    listOf(uid) + tiplocs
                ) { rs ->
                    val tiploc = rs.getString("tiploc") ?: return@queryList null
                    val platform = rs.getString("platform") ?: return@queryList null
                    tiploc to platform
                }.toMap()
            }
        } catch (e: Exception) {
            emptyMap()
        }
    }

    fun upsertDarwinRidUid(rid: String, uid: String) {
        try {
            transaction {
                execUpdate(
                    "INSERT INTO darwin_rid_uid(rid, uid, updated_at) VALUES(?, ?, ?) ON CONFLICT(rid) DO UPDATE SET uid=excluded.uid, updated_at=excluded.updated_at",
                    listOf(rid, uid, utcStamp())
                )
            }
        } catch (e: Exception) {
            log.debug("DarwinRidUid upsert error: ${e.message}")
        }
    }

    fun loadDarwinRidUid(): Map<String, String> = try {
        transaction { queryList("SELECT rid, uid FROM darwin_rid_uid") { it.getString("rid") to it.getString("uid") }.toMap() }
    } catch (e: Exception) {
        emptyMap()
    }

    fun darwinUidForRid(rid: String): String? = try {
        transaction { queryFirst("SELECT uid FROM darwin_rid_uid WHERE rid=?", listOf(rid)) { it.getString("uid") } }
    } catch (e: Exception) {
        null
    }

    // ---- Schedules and associations ------------------------------------------------------------------

    /** (TSC, uid) of a schedule running today with this headcode. */
    fun scheduleForHeadcode(headcode: String): Pair<String, String>? = transaction {
        Schedules.selectAll()
            .where { (Schedules.headcode eq headcode) and scheduleDateScope(effectiveDateFor(LocalDate.now().toString())) }
            .limit(1)
            .firstOrNull()
            ?.let { it[Schedules.tsc] to it[Schedules.uid] }
    }

    /** Replaces all associations, keeping one record per (main, assoc, TIPLOC) by STP precedence and dropping cancellations. */
    fun replaceAssociations(all: List<AssociationRecord>) {
        val rank = mapOf('C' to 4, 'N' to 3, 'O' to 2, 'P' to 1)
        val best = HashMap<String, AssociationRecord>()
        for (r in all) {
            val key = "${r.mainUid}|${r.assocUid}|${r.assocTiploc}"
            val existing = best[key]
            if (existing == null || (rank[r.stpInd] ?: 0) > (rank[existing.stpInd] ?: 0)) best[key] = r
        }
        val kept = best.values.filter { it.stpInd != 'C' }
        log.info("replaceAssociations: ${all.size} valid today, ${kept.size} after STP precedence")
        if (kept.isEmpty()) return
        transaction {
            CifAssociations.deleteAll()
            CifAssociations.batchInsert(kept, ignore = true) { r ->
                this[CifAssociations.mainUid] = r.mainUid
                this[CifAssociations.assocUid] = r.assocUid
                this[CifAssociations.assocTiploc] = r.assocTiploc
                this[CifAssociations.assocType] = r.assocType
                this[CifAssociations.stpInd] = r.stpInd
                this[CifAssociations.dateIndicator] = r.dateIndicator
            }
        }
        log.info("replaceAssociations: ${kept.size} records stored")
    }

    /** Associations keyed by main uid. */
    fun associationsFrom(uids: Set<String>): Map<String, List<AssociationRecord>> = associations(uids, byMain = true)

    /** Associations keyed by associated uid. */
    fun associationsTo(uids: Set<String>): Map<String, List<AssociationRecord>> = associations(uids, byMain = false)

    private fun associations(uids: Set<String>, byMain: Boolean): Map<String, List<AssociationRecord>> {
        if (uids.isEmpty()) return emptyMap()
        val column = if (byMain) CifAssociations.mainUid else CifAssociations.assocUid
        return transaction {
            CifAssociations.selectAll().where { (column inList uids) and (CifAssociations.stpInd neq 'C') }
                .map { row ->
                    AssociationRecord(
                        row[CifAssociations.mainUid],
                        row[CifAssociations.assocUid],
                        row[CifAssociations.assocTiploc],
                        row[CifAssociations.assocType],
                        row[CifAssociations.stpInd],
                        row[CifAssociations.dateIndicator]
                    )
                }
                .groupBy { if (byMain) it.mainUid else it.assocUid }
        }
    }

    // ---- Key/value stores ----------------------------------------------------------------------------

    fun getCifMeta(key: String): String? = transaction {
        CifMeta.selectAll().where { CifMeta.key eq key }.singleOrNull()?.get(CifMeta.value)
    }

    fun setCifMeta(key: String, value: String) {
        transaction {
            CifMeta.replace {
                it[CifMeta.key] = key
                it[CifMeta.value] = value
            }
        }
    }

    fun getHspCache(key: String): String? = transaction {
        HspCache.selectAll().where { HspCache.cacheKey eq key }.singleOrNull()?.get(HspCache.responseJson)
    }

    fun setHspCache(key: String, json: String) {
        transaction {
            HspCache.upsert {
                it[cacheKey] = key
                it[responseJson] = json
                it[cachedAt] = Instant.now().toString()
            }
        }
    }

    fun saveStanoxMap(map: Map<String, String>) = saveMap("stanox_map", map)
    fun loadStanoxMap(): Map<String, String> = loadMap("stanox_map")
    fun saveStanoxTiplocMap(map: Map<String, String>) = saveMap("stanox_tiploc_map", map)
    fun loadStanoxTiplocMap(): Map<String, String> = loadMap("stanox_tiploc_map")

    private fun saveMap(key: String, map: Map<String, String>) {
        if (map.isEmpty()) return
        val json = JSONObject()
        map.forEach { (k, v) -> json.put(k, v) }
        transaction {
            HspCache.replace {
                it[HspCache.cacheKey] = key
                it[HspCache.responseJson] = json.toString()
                it[HspCache.cachedAt] = Instant.now().toString()
            }
        }
    }

    private fun loadMap(key: String): Map<String, String> {
        val raw = getHspCache(key) ?: return emptyMap()
        return try {
            val json = JSONObject(raw)
            json.keySet().associateWith { json.getString(it) }
        } catch (e: Exception) {
            emptyMap()
        }
    }

    // ---- Status --------------------------------------------------------------------------------------

    fun status(trustConnected: Boolean, trainLocCount: Int): StatusResponse {
        statusCache?.let { (at, cached) ->
            if (System.currentTimeMillis() - at < 30_000L) {
                return cached.copy(trustConnected = trustConnected, trainLocationsCount = trainLocCount)
            }
        }
        val cifDate = getCifMeta("last_download_date")
        val today = LocalDate.now().toString()
        val resp = transaction {
            StatusResponse(
                cifLastDownload = cifDate,
                trustConnected = trustConnected,
                trainLocationsCount = trainLocCount,
                scheduleCount = count("SELECT COUNT(*) FROM schedules"),
                uniqueServiceCount = count("SELECT COUNT(DISTINCT uid) FROM schedules"),
                trustMovementCount = count("SELECT COUNT(*) FROM trust_movements"),
                trustMovementCount24h = TrustMovements.selectAll()
                    .where { TrustMovements.eventTs greaterEq LocalDateTime.now().minusHours(24L) }.count().toInt(),
                allocationsToday = count("SELECT COUNT(DISTINCT headcode) FROM allocation_history WHERE service_date=?", listOf(today)),
                hspUnitCount = AllocationConsists.selectAll().count().toInt(),
                tiplocNameCount = CorpusLookup.tiplocNameCount(),
                cifChangePendingCount = count("SELECT COUNT(*) FROM cif_change_alerts WHERE dismissed_at IS NULL"),
                missingTiplocCount = count("SELECT COUNT(*) FROM missing_tiplocs"),
                missingTscCount = count("SELECT COUNT(*) FROM missing_tscs"),
                feedbackCount = count("SELECT COUNT(*) FROM feedback WHERE dismissed_at IS NULL"),
            )
        }
        statusCache = System.currentTimeMillis() to resp
        return resp
    }
}

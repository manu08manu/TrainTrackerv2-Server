package com.traintracker.server.database

import org.jetbrains.exposed.v1.migration.jdbc.MigrationUtils
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

/** Creates or migrates every table the application uses. Runs once at startup. */
object Schema {
    private val log = LoggerFactory.getLogger("Database")

    const val MASTER_ADMIN_BOOTSTRAP_EMAIL = "tannajaisal@gmail.com"

    private const val NR_HEADCODE_EXPRESSION =
        "CASE WHEN headcode GLOB '[A-Z][1-9][0-9][0-9]' " +
        "THEN substr(headcode,2,1) || substr(headcode,1,1) || substr(headcode,3) " +
        "ELSE headcode END"

    private val exposedTables = arrayOf(
        SavedMaps, Schedules, Movements, TrainLocations, CifMeta, AllocationConsists, AllocationLegs,
        HeadcodeAliases, AllocationHistory, AllocationChanges, CifAssociations, DarwinPlatforms,
        TrainActivations,
    )

    private val rawTables = listOf(
        // Admin users and sessions
        "CREATE TABLE IF NOT EXISTS admin_users (email TEXT PRIMARY KEY, password_hash TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS admin_sessions (token TEXT PRIMARY KEY, email TEXT NOT NULL, expires_at TEXT NOT NULL)",
        // Audit trail of admin edits
        "CREATE TABLE IF NOT EXISTS tiploc_audit (id INTEGER PRIMARY KEY AUTOINCREMENT, tiploc TEXT NOT NULL, old_name TEXT, new_name TEXT NOT NULL, changed_by TEXT NOT NULL, changed_at TEXT DEFAULT (datetime('now')))",
        "CREATE TABLE IF NOT EXISTS tiploc_crs_audit (id INTEGER PRIMARY KEY AUTOINCREMENT, tiploc TEXT NOT NULL, old_crs TEXT, new_crs TEXT NOT NULL, changed_by TEXT NOT NULL, changed_at TEXT DEFAULT (datetime('now')))",
        // Manual overrides
        "CREATE TABLE IF NOT EXISTS location_overrides (name TEXT, crs TEXT, tiploc TEXT PRIMARY KEY, stanox TEXT, changed_by TEXT, changed_at TEXT)",
        "CREATE INDEX IF NOT EXISTS idx_location_overrides_stanox ON location_overrides(stanox)",
        // A STANOX shorter than five digits is zero-padded, on insert and when it is changed.
        "CREATE TRIGGER IF NOT EXISTS lo_stanox_pad_i AFTER INSERT ON location_overrides WHEN NEW.stanox GLOB '[0-9]*' AND length(NEW.stanox)<5 BEGIN UPDATE location_overrides SET stanox=printf('%05d',NEW.stanox) WHERE rowid=NEW.rowid; END",
        "CREATE TRIGGER IF NOT EXISTS lo_stanox_pad_u AFTER UPDATE OF stanox ON location_overrides WHEN NEW.stanox GLOB '[0-9]*' AND length(NEW.stanox)<5 BEGIN UPDATE location_overrides SET stanox=printf('%05d',NEW.stanox) WHERE rowid=NEW.rowid; END",
        "CREATE TABLE IF NOT EXISTS tsc_operator_overrides (tsc TEXT PRIMARY KEY, operator TEXT NOT NULL, changed_by TEXT NOT NULL, changed_at TEXT DEFAULT (datetime('now')))",
        "CREATE TABLE IF NOT EXISTS operators (code TEXT PRIMARY KEY, name TEXT NOT NULL, colour TEXT DEFAULT NULL)",
        // Admin work queues
        "CREATE TABLE IF NOT EXISTS missing_tscs (tsc TEXT PRIMARY KEY, first_seen TEXT DEFAULT (datetime('now')), last_seen TEXT DEFAULT (datetime('now')), seen_count INTEGER DEFAULT 1, headcode TEXT DEFAULT '', uid TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS missing_tiplocs (tiploc TEXT PRIMARY KEY, first_seen TEXT DEFAULT (datetime('now')), last_seen TEXT DEFAULT (datetime('now')), seen_count INTEGER DEFAULT 1, headcode TEXT DEFAULT '', uid TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS missing_smart_stanox (stanox TEXT PRIMARY KEY, first_seen TEXT DEFAULT (datetime('now')), last_seen TEXT DEFAULT (datetime('now')), seen_count INTEGER DEFAULT 1, headcode TEXT DEFAULT '', uid TEXT DEFAULT '')",
        "CREATE TABLE IF NOT EXISTS feedback (id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT NOT NULL, page TEXT DEFAULT '', submitted_at TEXT DEFAULT (datetime('now')), dismissed_at TEXT)",
        // Train describer events
        "CREATE TABLE IF NOT EXISTS td_events (headcode TEXT NOT NULL, stanox TEXT NOT NULL, event_type TEXT NOT NULL, event_s INTEGER NOT NULL, nr_headcode TEXT GENERATED ALWAYS AS ($NR_HEADCODE_EXPRESSION) VIRTUAL, PRIMARY KEY (headcode, stanox, event_type, event_s)) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS idx_td_events_s ON td_events(event_s)",
        "CREATE INDEX IF NOT EXISTS idx_td_events_stanox ON td_events(stanox)",
        // CIF cancellation windows (STP 'C' schedules)
        "CREATE TABLE IF NOT EXISTS cif_cancellations (uid TEXT NOT NULL, start_date TEXT NOT NULL, end_date TEXT NOT NULL, days TEXT NOT NULL, PRIMARY KEY (uid, start_date, end_date, days)) WITHOUT ROWID",
        // RDM location reference files
        "CREATE TABLE IF NOT EXISTS pif_loc (tiploc TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', start_date TEXT NOT NULL, end_date TEXT, os_easting INTEGER, os_northing INTEGER, timing_point_type TEXT NOT NULL DEFAULT '', zone TEXT NOT NULL DEFAULT '', stanox TEXT, off_network INTEGER NOT NULL DEFAULT 0, force_lpb TEXT NOT NULL DEFAULT '', action TEXT NOT NULL DEFAULT '', source_file TEXT NOT NULL DEFAULT '', loaded_at TEXT DEFAULT (datetime('now')))",
        "CREATE INDEX IF NOT EXISTS idx_pif_loc_stanox ON pif_loc(stanox)",
        "CREATE TABLE IF NOT EXISTS nlc_loc (tiploc TEXT PRIMARY KEY, nlc TEXT NOT NULL DEFAULT '', capts_ident TEXT NOT NULL DEFAULT '', check_char TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '', capri_desc TEXT NOT NULL DEFAULT '', crs TEXT NOT NULL DEFAULT '', tiploc_owner TEXT NOT NULL DEFAULT '', uic TEXT NOT NULL DEFAULT '', stanox TEXT NOT NULL DEFAULT '', po_mcp TEXT NOT NULL DEFAULT '', source_file TEXT NOT NULL DEFAULT '', loaded_at TEXT DEFAULT (datetime('now')))",
        "CREATE INDEX IF NOT EXISTS idx_nlc_loc_crs ON nlc_loc(crs)",
        "CREATE INDEX IF NOT EXISTS idx_nlc_loc_stanox ON nlc_loc(stanox)",
        // SMART berth steps: the RDM file plus manual edits
        "CREATE TABLE IF NOT EXISTS smart_berths (id INTEGER PRIMARY KEY AUTOINCREMENT, TD TEXT NOT NULL DEFAULT '', StepType TEXT NOT NULL DEFAULT '', FromBerth TEXT NOT NULL DEFAULT '', ToBerth TEXT NOT NULL DEFAULT '', Stanox TEXT NOT NULL DEFAULT '', Stanme TEXT NOT NULL DEFAULT '', Event TEXT NOT NULL DEFAULT '', Platform TEXT NOT NULL DEFAULT '', ToLine TEXT NOT NULL DEFAULT '', BerthOffset INTEGER, Route TEXT NOT NULL DEFAULT '', FromLine TEXT NOT NULL DEFAULT '', Comment TEXT NOT NULL DEFAULT '', Source TEXT NOT NULL DEFAULT '', ChangedBy TEXT, ChangedAt TEXT DEFAULT (datetime('now')))",
        "CREATE INDEX IF NOT EXISTS idx_smart_berths_stanox ON smart_berths(stanox)",
        "CREATE INDEX IF NOT EXISTS idx_smart_berths_key ON smart_berths(TD, FromBerth, ToBerth)",
        // LINX EI (standalone for now; see LinxConsumer). Times are kept as sent, plus "HH:mm".
        "CREATE TABLE IF NOT EXISTS linx_movements (train_id TEXT NOT NULL, uid TEXT NOT NULL DEFAULT '', headcode TEXT NOT NULL DEFAULT '', start_date TEXT NOT NULL DEFAULT '', tiploc TEXT NOT NULL, stanox TEXT, crs TEXT, loc_status TEXT NOT NULL DEFAULT '', event_type TEXT NOT NULL, booked_ts TEXT NOT NULL DEFAULT '', actual_ts TEXT NOT NULL DEFAULT '', booked_time TEXT NOT NULL DEFAULT '', actual_time TEXT NOT NULL DEFAULT '', delay INTEGER, reference_otn TEXT, received_ms INTEGER NOT NULL, platform TEXT, platform_source TEXT, PRIMARY KEY (train_id, tiploc, loc_status, booked_ts)) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS idx_linx_mvt_uid ON linx_movements(uid, start_date)",
        "CREATE INDEX IF NOT EXISTS idx_linx_mvt_received ON linx_movements(received_ms)",
        "CREATE TABLE IF NOT EXISTS linx_activations (train_id TEXT PRIMARY KEY, uid TEXT NOT NULL DEFAULT '', headcode TEXT NOT NULL DEFAULT '', start_date TEXT NOT NULL DEFAULT '', origin_tiploc TEXT, origin_stanox TEXT, origin_time TEXT, received_ms INTEGER NOT NULL)",
        "CREATE INDEX IF NOT EXISTS idx_linx_act_uid ON linx_activations(uid, start_date)",
        "CREATE TABLE IF NOT EXISTS linx_modifications (id INTEGER PRIMARY KEY AUTOINCREMENT, train_id TEXT NOT NULL, uid TEXT NOT NULL DEFAULT '', headcode TEXT NOT NULL DEFAULT '', start_date TEXT NOT NULL DEFAULT '', code TEXT NOT NULL, kind TEXT NOT NULL, tiploc TEXT, stanox TEXT, time TEXT, reason TEXT, modified_at TEXT, received_ms INTEGER NOT NULL, previous_headcode TEXT, locations TEXT)",
        "CREATE INDEX IF NOT EXISTS idx_linx_mod_train ON linx_modifications(train_id)",
        "CREATE INDEX IF NOT EXISTS idx_linx_mod_received ON linx_modifications(received_ms)",
        // Punctuality: calls per station, operator and half-hour of each finished day (routes/Punctuality.kt)
        "CREATE TABLE IF NOT EXISTS punctuality_daily (date TEXT NOT NULL, crs TEXT NOT NULL, toc TEXT NOT NULL, slot INTEGER NOT NULL, total INTEGER NOT NULL, early INTEGER NOT NULL, on_time INTEGER NOT NULL, late_minor INTEGER NOT NULL, late_moderate INTEGER NOT NULL, late_severe INTEGER NOT NULL, cancelled INTEGER NOT NULL, no_report INTEGER NOT NULL, delay_sum INTEGER NOT NULL, PRIMARY KEY (crs, date, toc, slot)) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS idx_punctuality_toc ON punctuality_daily(toc, date)",
        "CREATE INDEX IF NOT EXISTS idx_punctuality_date ON punctuality_daily(date)",
        "CREATE TABLE IF NOT EXISTS punctuality_days (date TEXT PRIMARY KEY, built_at TEXT NOT NULL, calls INTEGER NOT NULL)",
        "CREATE TABLE IF NOT EXISTS linx_raw (id INTEGER PRIMARY KEY AUTOINCREMENT, flow TEXT NOT NULL, note TEXT NOT NULL DEFAULT '', xml TEXT NOT NULL, received_ms INTEGER NOT NULL)",
    )

    fun create() {
        renameTrustMovements()
        retireHsp()
        transaction {
            MigrationUtils.statementsRequiredForDatabaseMigration(*exposedTables).forEach { sql ->
                if (sql.uppercase().contains("DROP ")) {
                    log.warn("Schema drop skipped: $sql")
                } else {
                    log.info("Schema change: $sql")
                    exec(sql)
                }
            }
        }
        backfillAllocationUids()
        transaction {
            rawTables.forEach { exec(it) }
            addMasterAdminColumn()
            addTdPlatformColumn()
            addScheduleWttColumns()
            addTdNrHeadcodeColumn()
            addLinxModificationColumns()
        }
    }

    /** td_events gained the SMART platform of the berth step; older tables lack the column. */
    private fun JdbcTransaction.addTdPlatformColumn() {
        val columns = queryList("PRAGMA table_info(td_events)") { it.getString(2) }
        if ("platform" in columns) return
        exec("ALTER TABLE td_events ADD COLUMN platform TEXT")
        log.info("Migrated td_events.platform column")
    }

    /** schedules gained wtt_arrival and wtt_departure (working-timetable minutes, for LINX/TD matching only). */
    private fun JdbcTransaction.addScheduleWttColumns() {
        val columns = queryList("PRAGMA table_info(schedules)") { it.getString(2) }
        if (columns.isEmpty()) return
        for (column in listOf("wtt_arrival", "wtt_departure")) {
            if (column in columns) continue
            exec("ALTER TABLE schedules ADD COLUMN $column VARCHAR(5)")
            log.info("Migrated schedules.$column column")
        }
    }

    /** td_events gains a normalised NR-style headcode for matching LU descriptors. */
    private fun JdbcTransaction.addTdNrHeadcodeColumn() {
        // table_info omits generated columns; table_xinfo includes them.
        val columns = queryList("PRAGMA table_xinfo(td_events)") { it.getString(2) }

        if ("nr_headcode" !in columns) {
            exec("""ALTER TABLE td_events ADD COLUMN nr_headcode TEXT
            GENERATED ALWAYS AS ($NR_HEADCODE_EXPRESSION) VIRTUAL""")
            log.info("Migrated td_events.nr_headcode column")
        }

        exec("CREATE INDEX IF NOT EXISTS idx_td_events_nrhc ON td_events(nr_headcode, stanox, event_s)")
    }

    /**
     * trust_movements was renamed movements when TrainTracker switched from TRUST to LINX (2026-11). Runs before the Exposed
     * migration so it never creates an empty movements table. The pk_trust_movements constraint and the
     * idx_trust_* indexes keep their names: renaming an index means rebuilding it.
     */
    private fun renameTrustMovements() = transaction {
        val tables = queryList("SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('trust_movements', 'movements')") { it.getString(1) }
        if ("trust_movements" in tables && "movements" !in tables) {
            exec("ALTER TABLE trust_movements RENAME TO movements")
            log.info("Renamed trust_movements to movements")
        }
    }

    /**
     * HSP is gone: its cache table only still holds the saved STANOX maps, so it becomes saved_maps, and the
     * Darwin RID-to-uid table, which only HSP's unit lookup read, is dropped.
     */
    private fun retireHsp() = transaction {
        val tables = queryList("SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('hsp_cache', 'saved_maps', 'darwin_rid_uid')") { it.getString(1) }
        if ("hsp_cache" in tables && "saved_maps" !in tables) {
            exec("DELETE FROM hsp_cache WHERE cache_key NOT IN ('stanox_map', 'stanox_tiploc_map')")
            exec("ALTER TABLE hsp_cache RENAME TO saved_maps")
            log.info("Renamed hsp_cache to saved_maps")
        }
        if ("darwin_rid_uid" in tables) {
            exec("DROP TABLE darwin_rid_uid")
            log.info("Dropped darwin_rid_uid")
        }
    }

    /** linx_modifications gained the old headcode of a change of identity, then every modified location. */
    private fun JdbcTransaction.addLinxModificationColumns() {
        val columns = queryList("PRAGMA table_info(linx_modifications)") { it.getString(2) }
        for (column in listOf("previous_headcode", "locations")) {
            if (column in columns) continue
            exec("ALTER TABLE linx_modifications ADD COLUMN $column TEXT")
            log.info("Migrated linx_modifications.$column column")
        }
    }

    /** Older admin_users tables lack is_master_admin; add it and promote the bootstrap account. */
    private fun JdbcTransaction.addMasterAdminColumn() {
        val columns = queryList("PRAGMA table_info(admin_users)") { it.getString(2) }
        if ("is_master_admin" in columns) return
        exec("ALTER TABLE admin_users ADD COLUMN is_master_admin INTEGER NOT NULL DEFAULT 0")
        execUpdate("UPDATE admin_users SET is_master_admin = 1 WHERE email = ?", listOf(MASTER_ADMIN_BOOTSTRAP_EMAIL))
        log.info("Migrated is_master_admin column; bootstrap master admin: $MASTER_ADMIN_BOOTSTRAP_EMAIL")
    }

    /** Fills allocation uid from core_id (headcode + optional space + uid + 2 digit suffix). */
    private fun backfillAllocationUids() {
        val uidFromCoreId =
            "CASE WHEN substr(core_id, length(headcode)+1, 1) = ' ' THEN substr(core_id, length(headcode)+2, length(core_id)-length(headcode)-1-2) ELSE substr(core_id, length(headcode)+1, length(core_id)-length(headcode)-2) END"
        transaction {
            try {
                exec("UPDATE allocation_consists SET uid = $uidFromCoreId WHERE length(core_id) > length(headcode)+2")
                exec("UPDATE allocation_history SET uid = $uidFromCoreId WHERE length(core_id) > length(headcode)+2")
                log.info("Allocation uid backfill complete")
            } catch (e: Exception) {
                log.warn("Allocation uid backfill failed: ${e.message}")
            }
        }
    }
}

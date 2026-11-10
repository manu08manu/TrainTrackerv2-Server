package com.traintracker.server.database

import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

/** Manual location (TIPLOC name/CRS/STANOX) and TSC operator overrides, and the audit trail of admin edits. */
object Overrides {
    private val log = LoggerFactory.getLogger("Overrides")

    // ---- Loading -------------------------------------------------------------------------------------

    private fun load(sql: String): Map<String, String> = transaction {
        queryList(sql) { it.getString(1) to it.getString(2) }.toMap()
    }

    fun tiplocCrs(): Map<String, String> = load("SELECT tiploc, crs FROM location_overrides WHERE LENGTH(crs) != 0")

    fun tiplocNames(): Map<String, String> = load("SELECT tiploc, name FROM location_overrides WHERE LENGTH(name) != 0")

    fun tscOperators(): Map<String, String> = load("SELECT tsc, operator FROM tsc_operator_overrides")

    /** STANOX -> CRS where every overridden TIPLOC with that STANOX agrees on the CRS. */
    fun stanoxCrs(): Map<String, String> = load(
        "SELECT stanox, MIN(crs) AS crs FROM location_overrides WHERE LENGTH(stanox) != 0 AND LENGTH(crs) != 0 GROUP BY stanox HAVING COUNT(DISTINCT crs) = 1"
    )

    /** STANOX -> TIPLOC where exactly one overridden TIPLOC has that STANOX. */
    fun stanoxTiplocs(): Map<String, String> = load(
        "SELECT stanox, MIN(tiploc) AS tiploc FROM location_overrides WHERE LENGTH(stanox) != 0 GROUP BY stanox HAVING COUNT(DISTINCT tiploc) = 1"
    )

    // ---- Saving --------------------------------------------------------------------------------------

    fun setCrs(tiploc: String, crs: String, changedBy: String) = setLocation("crs", tiploc, crs, changedBy)

    fun setName(tiploc: String, name: String, changedBy: String) = setLocation("name", tiploc, name, changedBy)

    private fun setLocation(column: String, tiploc: String, value: String, changedBy: String) {
        transaction {
            execUpdate(
                "INSERT INTO location_overrides (tiploc, $column) VALUES (?, ?) ON CONFLICT(tiploc) DO UPDATE SET $column = excluded.$column",
                listOf(tiploc, value)
            )
            execUpdate(
                "UPDATE location_overrides SET changed_by = ?, changed_at = datetime('now') WHERE tiploc = ?",
                listOf(changedBy, tiploc)
            )
        }
    }

    fun setOperator(tsc: String, operator: String) {
        transaction {
            execUpdate(
                "INSERT INTO tsc_operator_overrides (tsc, operator) VALUES (?, ?) ON CONFLICT(tsc) DO UPDATE SET operator = excluded.operator",
                listOf(tsc, operator)
            )
        }
    }

    /** Sets the STANOX only when the TIPLOC has none. Returns the resulting (existing or new) value. */
    fun setStanoxIfEmpty(tiploc: String, stanox: String, changedBy: String): String? = transaction {
        val current = queryFirst("SELECT stanox FROM location_overrides WHERE tiploc = ?", listOf(tiploc)) {
            it.getString(1)?.takeIf { s -> s.isNotEmpty() }
        }
        if (current != null) return@transaction current
        execUpdate(
            "UPDATE location_overrides SET stanox = ?, changed_by = ?, changed_at = datetime('now') WHERE tiploc = ?",
            listOf(stanox, changedBy, tiploc)
        )
        stanox
    }

    /** Applies a CRS override to the already loaded timetable. */
    fun applyCrsToSchedules(tiploc: String, crs: String) {
        val safeTiploc = tiploc.filter { it.isLetterOrDigit() }
        val safeCrs = crs.filter { it.isLetterOrDigit() }
        if (safeTiploc.isEmpty() || safeCrs.isEmpty()) return
        transaction { execUpdate("UPDATE schedules SET crs = ? WHERE tiploc = ?", listOf(safeCrs, safeTiploc)) }
    }

    // ---- Audit trail ---------------------------------------------------------------------------------

    /** One audited field: its table and columns, and the JSON names used when listing it. */
    enum class Audit(
        val table: String,
        val keyColumn: String,
        val oldColumn: String,
        val newColumn: String,
        val keyName: String,
        val oldName: String,
        val newName: String,
    ) {
        NAME("tiploc_audit", "tiploc", "old_name", "new_name", "tiploc", "oldName", "newName"),
        CRS("tiploc_crs_audit", "tiploc", "old_crs", "new_crs", "tiploc", "oldCrs", "newCrs"),
        OPERATOR("tsc_operator_audit", "tsc", "old_operator", "new_operator", "tsc", "oldOperator", "newOperator"),
    }

    fun logNameChange(tiploc: String, oldName: String?, newName: String, changedBy: String) {
        if ((oldName ?: "") == newName) return
        record(Audit.NAME, tiploc, oldName, newName, changedBy)
    }

    fun logCrsChange(tiploc: String, oldCrs: String?, newCrs: String, changedBy: String) {
        if (oldCrs == newCrs) return
        record(Audit.CRS, tiploc, oldCrs, newCrs, changedBy)
    }

    fun logOperatorChange(tsc: String, oldOperator: String?, newOperator: String, changedBy: String) {
        if ((oldOperator ?: "").equals(newOperator, ignoreCase = true)) return
        record(Audit.OPERATOR, tsc, oldOperator, newOperator, changedBy)
    }

    private fun record(audit: Audit, key: String, old: String?, new: String, changedBy: String) {
        try {
            transaction {
                execUpdate(
                    "INSERT INTO ${audit.table} (${audit.keyColumn}, ${audit.oldColumn}, ${audit.newColumn}, changed_by) VALUES (?, ?, ?, ?)",
                    listOf(key, old ?: "", new, changedBy)
                )
            }
        } catch (e: Exception) {
            log.warn("${audit.table} insert failed: ${e.message}")
        }
    }

    fun auditLog(audit: Audit, limit: Int): List<Map<String, String?>> = transaction {
        queryList(
            "SELECT ${audit.keyColumn}, ${audit.oldColumn}, ${audit.newColumn}, changed_by, changed_at FROM ${audit.table} ORDER BY id DESC LIMIT $limit"
        ) { rs ->
            mapOf(
                audit.keyName to rs.getString(1),
                audit.oldName to rs.getString(2),
                audit.newName to rs.getString(3),
                "changedBy" to rs.getString(4),
                "changedAt" to rs.getString(5)?.let { toIsoUtc(it) },
            )
        }
    }

    /** All three audit trails merged, newest first. */
    fun combinedAuditLog(limit: Int): List<Map<String, String?>> = transaction {
        queryList(
            Audit.entries.joinToString(" UNION ALL ") {
                "SELECT '${it.name.lowercase()}' AS type, ${it.keyColumn} AS key, ${it.oldColumn} AS old_val, ${it.newColumn} AS new_val, changed_by, changed_at FROM ${it.table}"
            } + " ORDER BY changed_at DESC LIMIT $limit"
        ) { rs ->
            mapOf(
                "type" to rs.getString(1),
                "key" to rs.getString(2),
                "oldVal" to rs.getString(3),
                "newVal" to rs.getString(4),
                "changedBy" to rs.getString(5),
                "changedAt" to rs.getString(6)?.let { toIsoUtc(it) },
            )
        }
    }
}

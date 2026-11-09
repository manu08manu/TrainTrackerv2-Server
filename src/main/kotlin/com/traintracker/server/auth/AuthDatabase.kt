package com.traintracker.server.auth

import java.sql.ResultSet
import java.util.UUID
import kotlinx.serialization.Serializable
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.jdbc.statements.jdbc.JdbcConnectionImpl
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

@Serializable
data class UserInfo(
    val email: String,
    val isMasterAdmin: Boolean
)

object AuthDatabase {
    private val log = LoggerFactory.getLogger("AuthDatabase")
    const val MASTER_ADMIN_BOOTSTRAP_EMAIL = "tannajaisal@gmail.com"

    fun init() {
        transaction {
            exec("CREATE TABLE IF NOT EXISTS admin_users (\n    email         TEXT PRIMARY KEY,\n    password_hash TEXT NOT NULL\n)")
            val existingCols = mutableListOf<String>()
            exec("PRAGMA table_info(admin_users)") { rs ->
                while (rs.next()) existingCols += rs.getString(2)
            }
            if (!existingCols.contains("is_master_admin")) {
                exec("ALTER TABLE admin_users ADD COLUMN is_master_admin INTEGER NOT NULL DEFAULT 0")
                execPrepared("UPDATE admin_users SET is_master_admin = 1 WHERE email = ?", MASTER_ADMIN_BOOTSTRAP_EMAIL)
                log.info("Migrated is_master_admin column; bootstrap master admin: $MASTER_ADMIN_BOOTSTRAP_EMAIL")
            }
            exec("CREATE TABLE IF NOT EXISTS admin_sessions (\n    token      TEXT PRIMARY KEY,\n    email      TEXT NOT NULL,\n    expires_at TEXT NOT NULL\n)")
            exec("CREATE TABLE IF NOT EXISTS tiploc_audit (\n    id         INTEGER PRIMARY KEY AUTOINCREMENT,\n    tiploc     TEXT NOT NULL,\n    old_name   TEXT,\n    new_name   TEXT NOT NULL,\n    changed_by TEXT NOT NULL,\n    changed_at TEXT DEFAULT (datetime('now'))\n)")
            exec("CREATE TABLE IF NOT EXISTS tiploc_crs_audit (\n    id         INTEGER PRIMARY KEY AUTOINCREMENT,\n    tiploc     TEXT NOT NULL,\n    old_crs    TEXT,\n    new_crs    TEXT NOT NULL,\n    changed_by TEXT NOT NULL,\n    changed_at TEXT DEFAULT (datetime('now'))\n)")
            exec("CREATE TABLE IF NOT EXISTS tiploc_crs_overrides (\n    tiploc     TEXT PRIMARY KEY,\n    crs        TEXT NOT NULL,\n    changed_by TEXT NOT NULL,\n    changed_at TEXT DEFAULT (datetime('now'))\n)")
            exec("CREATE TABLE IF NOT EXISTS tiploc_name_overrides (\n    tiploc     TEXT PRIMARY KEY,\n    name       TEXT NOT NULL,\n    changed_by TEXT NOT NULL,\n    changed_at TEXT DEFAULT (datetime('now'))\n)")
            exec("                CREATE TABLE IF NOT EXISTS location_overrides (\nname TEXT,\ncrs TEXT,\ntiploc TEXT PRIMARY KEY,\nstanox TEXT,\nchanged_by TEXT,\nchanged_at TEXT\n)")
            exec("CREATE TABLE IF NOT EXISTS tsc_operator_overrides (\n                    tsc        TEXT PRIMARY KEY,\n                    operator   TEXT NOT NULL,\n                    changed_by TEXT NOT NULL,\n                    changed_at TEXT DEFAULT (datetime('now'))\n                )")
            exec("CREATE TABLE IF NOT EXISTS operators (\n    code   TEXT PRIMARY KEY,\n    name   TEXT NOT NULL,\n    colour TEXT DEFAULT NULL\n)")
            exec("CREATE TABLE IF NOT EXISTS missing_tscs (\n    tsc        TEXT PRIMARY KEY,\n    first_seen TEXT DEFAULT (datetime('now')),\n    last_seen  TEXT DEFAULT (datetime('now')),\n    seen_count INTEGER DEFAULT 1,\n    headcode   TEXT DEFAULT '',\n    uid        TEXT DEFAULT ''\n)")
            exec("CREATE TABLE IF NOT EXISTS missing_tiplocs (\n    tiploc     TEXT PRIMARY KEY,\n    first_seen TEXT DEFAULT (datetime('now')),\n    last_seen  TEXT DEFAULT (datetime('now')),\n    seen_count INTEGER DEFAULT 1,\n    headcode   TEXT DEFAULT '',\n    uid        TEXT DEFAULT ''\n)")
            exec("CREATE TABLE IF NOT EXISTS feedback (\n    id           INTEGER PRIMARY KEY AUTOINCREMENT,\n    message      TEXT NOT NULL,\n    page         TEXT DEFAULT '',\n    submitted_at TEXT DEFAULT (datetime('now')),\n    dismissed_at TEXT\n)")
        }
        log.info("AuthDatabase initialised")
    }

    fun setUser(email: String, password: String): Boolean {
        val hash = PasswordHasher.hash(password)
        val wasMaster = isMasterAdmin(email)
        return try {
            transaction {
                execPrepared("DELETE FROM admin_users WHERE email = ?", email)
                execPrepared("INSERT INTO admin_users (email, password_hash, is_master_admin) VALUES (?, ?, ?)", email, hash, if (wasMaster) "1" else "0")
            }
            log.info("User set: $email")
            true
        } catch (e: Exception) {
            log.error("setUser failed: ${e.message}")
            false
        }
    }

    fun isMasterAdmin(email: String): Boolean {
        var result = false
        transaction {
            execQuery("SELECT is_master_admin FROM admin_users WHERE email = ?", email) { rs ->
                if (rs.next()) result = rs.getInt(1) == 1
            }
        }
        return result
    }

    fun countMasterAdmins(): Int {
        var count = 0
        transaction {
            exec("SELECT COUNT(*) FROM admin_users WHERE is_master_admin = 1") { rs ->
                if (rs.next()) count = rs.getInt(1)
            }
        }
        return count
    }

    fun setMasterAdmin(email: String, isMaster: Boolean): Boolean {
        if (!isMaster && isMasterAdmin(email) && countMasterAdmins() <= 1) return false
        transaction {
            execPrepared("UPDATE admin_users SET is_master_admin = ? WHERE email = ?", if (isMaster) "1" else "0", email)
        }
        return true
    }

    fun deleteUser(email: String) {
        transaction {
            execPrepared("DELETE FROM admin_users WHERE email = ?", email)
            execPrepared("DELETE FROM admin_sessions WHERE email = ?", email)
        }
    }

    fun listUsers(): List<UserInfo> {
        val users = mutableListOf<UserInfo>()
        transaction {
            exec("SELECT email, is_master_admin FROM admin_users ORDER BY email") { rs ->
                while (rs.next()) users += UserInfo(rs.getString(1), rs.getInt(2) == 1)
            }
        }
        return users
    }

    fun login(email: String, password: String): String? {
        var hash: String? = null
        transaction {
            execQuery("SELECT password_hash FROM admin_users WHERE email = ?", email) { rs ->
                if (rs.next()) hash = rs.getString(1)
            }
        }
        val h = hash ?: return null
        val result = PasswordHasher.verify(password, h)
        if (!result.ok) return null
        if (result.needsRehash) {
            val upgraded = PasswordHasher.hash(password)
            transaction {
                execPrepared("UPDATE admin_users SET password_hash = ? WHERE email = ?", upgraded, email)
            }
            log.info("Password hash upgraded")
        }
        val token = UUID.randomUUID().toString()
        transaction {
            execPrepared("INSERT INTO admin_sessions (token, email, expires_at) VALUES (?, ?, datetime('now', '+7 days'))", token, email)
        }
        return token
    }

    fun validateSession(token: String): String? {
        var email: String? = null
        transaction {
            execQuery("SELECT email FROM admin_sessions WHERE token = ? AND expires_at > datetime('now')", token) { rs ->
                if (rs.next()) email = rs.getString(1)
            }
        }
        return email
    }

    fun logout(token: String) {
        transaction { execPrepared("DELETE FROM admin_sessions WHERE token = ?", token) }
    }

    fun logTiplocChange(tiploc: String, oldName: String?, newName: String, changedBy: String) {
        if ((oldName ?: "") == newName) return
        try {
            transaction {
                execPrepared("INSERT INTO tiploc_audit (tiploc, old_name, new_name, changed_by) VALUES (?, ?, ?, ?)", tiploc, oldName ?: "", newName, changedBy)
            }
        } catch (e: Exception) {
            log.warn("tiploc_audit insert failed: ${e.message}")
        }
    }

    private fun toIsoUtc(raw: String?): String? {
        if (raw.isNullOrEmpty()) return raw
        val s = raw.replace(' ', 'T')
        return if (s.endsWith("Z")) s else "${s}Z"
    }

    fun getCombinedAuditLog(limit: Int = 100): List<Map<String, String?>> {
        val rows = mutableListOf<Map<String, String?>>()
        transaction {
            exec(
                "\n                SELECT 'name' AS type, tiploc AS key, old_name AS old_val, new_name AS new_val, changed_by, changed_at\n                  FROM tiploc_audit\n                UNION ALL\n                SELECT 'crs' AS type, tiploc AS key, old_crs AS old_val, new_crs AS new_val, changed_by, changed_at\n                  FROM tiploc_crs_audit\n                UNION ALL\n                SELECT 'operator' AS type, tsc AS key, old_operator AS old_val, new_operator AS new_val, changed_by, changed_at\n                  FROM tsc_operator_audit\n                ORDER BY changed_at DESC\n                LIMIT $limit\n            ".trimIndent()
            ) { rs ->
                while (rs.next()) {
                    rows += mapOf(
                        "type" to rs.getString(1),
                        "key" to rs.getString(2),
                        "oldVal" to rs.getString(3),
                        "newVal" to rs.getString(4),
                        "changedBy" to rs.getString(5),
                        "changedAt" to toIsoUtc(rs.getString(6))
                    )
                }
            }
        }
        return rows
    }

    fun getTiplocAuditLog(limit: Int = 50): List<Map<String, String?>> {
        val rows = mutableListOf<Map<String, String?>>()
        transaction {
            exec("SELECT tiploc, old_name, new_name, changed_by, changed_at FROM tiploc_audit ORDER BY id DESC LIMIT $limit") { rs ->
                while (rs.next()) {
                    rows += mapOf(
                        "tiploc" to rs.getString(1),
                        "oldName" to rs.getString(2),
                        "newName" to rs.getString(3),
                        "changedBy" to rs.getString(4),
                        "changedAt" to toIsoUtc(rs.getString(5))
                    )
                }
            }
        }
        return rows
    }

    fun logTiplocCrsChange(tiploc: String, oldCrs: String?, newCrs: String, changedBy: String) {
        if (oldCrs == newCrs) return
        try {
            transaction {
                execPrepared("INSERT INTO tiploc_crs_audit (tiploc, old_crs, new_crs, changed_by) VALUES (?, ?, ?, ?)", tiploc, oldCrs ?: "", newCrs, changedBy)
            }
        } catch (e: Exception) {
            log.warn("tiploc_crs_audit insert failed: ${e.message}")
        }
    }

    fun getTiplocCrsAuditLog(limit: Int = 50): List<Map<String, String?>> {
        val rows = mutableListOf<Map<String, String?>>()
        transaction {
            exec("SELECT tiploc, old_crs, new_crs, changed_by, changed_at FROM tiploc_crs_audit ORDER BY id DESC LIMIT $limit") { rs ->
                while (rs.next()) {
                    rows += mapOf(
                        "tiploc" to rs.getString(1),
                        "oldCrs" to rs.getString(2),
                        "newCrs" to rs.getString(3),
                        "changedBy" to rs.getString(4),
                        "changedAt" to toIsoUtc(rs.getString(5))
                    )
                }
            }
        }
        return rows
    }

    fun logOperatorChange(tsc: String, oldOperator: String?, newOperator: String, changedBy: String) {
        if ((oldOperator ?: "").equals(newOperator, ignoreCase = true)) return
        try {
            transaction {
                execPrepared(
                    "INSERT INTO tsc_operator_audit (tsc, old_operator, new_operator, changed_by) VALUES (?, ?, ?, ?)",
                    tsc, oldOperator ?: "", newOperator, changedBy
                )
            }
        } catch (e: Exception) {
            log.warn("tsc_operator_audit insert failed: ${e.message}")
        }
    }

    fun getTscOperatorAuditLog(limit: Int = 50): List<Map<String, String?>> {
        val rows = mutableListOf<Map<String, String?>>()
        transaction {
            exec("SELECT tsc, old_operator, new_operator, changed_by, changed_at FROM tsc_operator_audit ORDER BY id DESC LIMIT $limit") { rs ->
                while (rs.next()) {
                    rows += mapOf(
                        "tsc" to rs.getString(1),
                        "oldOperator" to rs.getString(2),
                        "newOperator" to rs.getString(3),
                        "changedBy" to rs.getString(4),
                        "changedAt" to toIsoUtc(rs.getString(5))
                    )
                }
            }
        }
        return rows
    }

    fun getOperators(): List<Map<String, String?>> {
        val rows = mutableListOf<Map<String, String?>>()
        transaction {
            exec("SELECT code, name, colour FROM operators ORDER BY name COLLATE NOCASE") { rs ->
                while (rs.next()) {
                    rows += mapOf(
                        "code" to rs.getString(1),
                        "name" to rs.getString(2),
                        "colour" to rs.getString(3)?.takeIf { it.isNotEmpty() }
                    )
                }
            }
        }
        return rows
    }

    fun upsertOperator(code: String, name: String, colour: String) {
        transaction {
            execPrepared(
                "INSERT INTO operators (code, name, colour) VALUES (?, ?, ?) ON CONFLICT(code) DO UPDATE SET name = excluded.name, colour = excluded.colour",
                code, name, colour
            )
        }
    }

    fun deleteOperator(code: String) {
        transaction { execPrepared("DELETE FROM operators WHERE code = ?", code) }
    }

    private fun loadOverrides(table: String, keyCol: String, valCol: String): Map<String, String> {
        val result = mutableMapOf<String, String>()
        transaction {
            exec("SELECT $keyCol, $valCol FROM $table") { rs ->
                while (rs.next()) result[rs.getString(1)] = rs.getString(2)
            }
        }
        return result
    }

    private fun setOverride(table: String, keyCol: String, valCol: String, key: String, value: String, changedBy: String) {
        transaction {
            execPrepared("INSERT INTO $table ($keyCol, $valCol) VALUES (?, ?) ON CONFLICT($keyCol) DO UPDATE SET $valCol = excluded.$valCol", key, value)
        }
    }

    fun loadCrsOverrides(): Map<String, String> =
        loadOverrides("(SELECT tiploc, crs FROM location_overrides WHERE LENGTH(crs) != 0)", "tiploc", "crs")

    fun setCrsOverride(tiploc: String, crs: String, changedBy: String) {
        setLocationOverride("crs", tiploc, crs, changedBy)
    }

    private fun setLocationOverride(col: String, tiploc: String, value: String, changedBy: String) {
        setOverride("location_overrides", "tiploc", col, tiploc, value, changedBy)
        transaction {
            execPrepared("UPDATE location_overrides SET changed_by = ?, changed_at = datetime('now') WHERE tiploc = ?", changedBy, tiploc)
        }
    }

    fun loadOperatorOverrides(): Map<String, String> = loadOverrides("tsc_operator_overrides", "tsc", "operator")

    fun setOperatorOverride(tsc: String, operator: String, changedBy: String) {
        setOverride("tsc_operator_overrides", "tsc", "operator", tsc, operator, changedBy)
    }

    fun loadStanoxCrsOverrides(): Map<String, String> = loadOverrides(
        "(SELECT stanox, MIN(crs) AS crs FROM location_overrides WHERE LENGTH(stanox) != 0 AND LENGTH(crs) != 0 GROUP BY stanox HAVING COUNT(DISTINCT crs) = 1)",
        "stanox",
        "crs"
    )

    fun loadStanoxTiplocOverrides(): Map<String, String> = loadOverrides(
        "(SELECT stanox, MIN(tiploc) AS tiploc FROM location_overrides WHERE LENGTH(stanox) != 0 GROUP BY stanox HAVING COUNT(DISTINCT tiploc) = 1)",
        "stanox",
        "tiploc"
    )

    fun loadNameOverrides(): Map<String, String> =
        loadOverrides("(SELECT tiploc, name FROM location_overrides WHERE LENGTH(name) != 0)", "tiploc", "name")

    fun setNameOverride(tiploc: String, name: String, changedBy: String) {
        setLocationOverride("name", tiploc, name, changedBy)
    }

    fun updateSchedulesCrs(tiploc: String, crs: String) {
        val safeTiploc = tiploc.filter { it.isLetterOrDigit() }
        val safeCrs = crs.filter { it.isLetterOrDigit() }
        if (safeTiploc.isEmpty() || safeCrs.isEmpty()) return
        transaction { execPrepared("UPDATE schedules SET crs = ? WHERE tiploc = ?", safeCrs, safeTiploc) }
    }

    private fun JdbcTransaction.execPrepared(sql: String, vararg params: String) {
        val conn = (connection as JdbcConnectionImpl).connection
        conn.prepareStatement(sql).use { stmt ->
            params.forEachIndexed { i, v -> stmt.setString(i + 1, v) }
            stmt.executeUpdate()
        }
    }

    private fun JdbcTransaction.execQuery(sql: String, vararg params: String, block: (ResultSet) -> Unit) {
        val conn = (connection as JdbcConnectionImpl).connection
        conn.prepareStatement(sql).use { stmt ->
            params.forEachIndexed { i, v -> stmt.setString(i + 1, v) }
            block(stmt.executeQuery())
        }
    }
}

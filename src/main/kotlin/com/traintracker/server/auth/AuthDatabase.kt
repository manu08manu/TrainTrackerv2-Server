package com.traintracker.server.auth

import org.jetbrains.exposed.sql.transactions.transaction
import org.mindrot.jbcrypt.BCrypt
import org.slf4j.LoggerFactory
import java.util.UUID

object AuthDatabase {
    private val log = LoggerFactory.getLogger("AuthDatabase")

    fun init() {
        transaction {
            exec("""
                CREATE TABLE IF NOT EXISTS admin_users (
                    email         TEXT PRIMARY KEY,
                    password_hash TEXT NOT NULL
                )
            """.trimIndent())
            exec("""
                CREATE TABLE IF NOT EXISTS admin_sessions (
                    token      TEXT PRIMARY KEY,
                    email      TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                )
            """.trimIndent())
            exec("""
                CREATE TABLE IF NOT EXISTS tiploc_audit (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    tiploc     TEXT NOT NULL,
                    old_name   TEXT,
                    new_name   TEXT NOT NULL,
                    changed_by TEXT NOT NULL,
                    changed_at TEXT DEFAULT (datetime('now'))
                )
            """.trimIndent())
            exec("""
                CREATE TABLE IF NOT EXISTS tiploc_crs_overrides (
                    tiploc     TEXT PRIMARY KEY,
                    crs        TEXT NOT NULL,
                    changed_by TEXT NOT NULL,
                    changed_at TEXT DEFAULT (datetime('now'))
                )
            """.trimIndent())
            exec("""
                CREATE TABLE IF NOT EXISTS tiploc_name_overrides (
                    tiploc     TEXT PRIMARY KEY,
                    name       TEXT NOT NULL,
                    changed_by TEXT NOT NULL,
                    changed_at TEXT DEFAULT (datetime('now'))
                )
            """.trimIndent())
        }
        log.info("AuthDatabase initialised")
    }

    /** Create or overwrite a user. Safe to call for initial setup. */
    fun setUser(email: String, password: String): Boolean {
        val hash = BCrypt.hashpw(password, BCrypt.gensalt())
        return try {
            transaction {
                execPrepared("DELETE FROM admin_users WHERE email = ?", email)
                execPrepared(
                    "INSERT INTO admin_users (email, password_hash) VALUES (?, ?)",
                    email, hash
                )
            }
            log.info("User set: $email")
            true
        } catch (ex: Exception) {
            log.error("setUser failed: ${ex.message}")
            false
        }
    }

    fun deleteUser(email: String) {
        transaction {
            execPrepared("DELETE FROM admin_users WHERE email = ?", email)
            execPrepared("DELETE FROM admin_sessions WHERE email = ?", email)
        }
    }

    fun listUsers(): List<String> {
        val users = mutableListOf<String>()
        transaction {
            exec("SELECT email FROM admin_users ORDER BY email") { rs ->
                while (rs.next()) users += rs.getString(1)
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
        val stored = hash ?: return null
        if (!BCrypt.checkpw(password, stored)) return null
        val token = UUID.randomUUID().toString()
        transaction {
            execPrepared(
                "INSERT INTO admin_sessions (token, email, expires_at) VALUES (?, ?, datetime('now', '+7 days'))",
                token, email
            )
        }
        return token
    }

    fun validateSession(token: String): String? {
        var email: String? = null
        transaction {
            execQuery(
                "SELECT email FROM admin_sessions WHERE token = ? AND expires_at > datetime('now')",
                token
            ) { rs ->
                if (rs.next()) email = rs.getString(1)
            }
        }
        return email
    }

    fun logout(token: String) {
        transaction {
            execPrepared("DELETE FROM admin_sessions WHERE token = ?", token)
        }
    }

    fun logTiplocChange(tiploc: String, oldName: String?, newName: String, changedBy: String) {
        try {
            transaction {
                execPrepared(
                    "INSERT INTO tiploc_audit (tiploc, old_name, new_name, changed_by) VALUES (?, ?, ?, ?)",
                    tiploc, oldName ?: "", newName, changedBy
                )
            }
        } catch (ex: Exception) {
            log.warn("tiploc_audit insert failed: ${ex.message}")
        }
    }

    fun getTiplocAuditLog(limit: Int = 50): List<Map<String, String?>> {
        val rows = mutableListOf<Map<String, String?>>()
        transaction {
            exec("SELECT tiploc, old_name, new_name, changed_by, changed_at FROM tiploc_audit ORDER BY id DESC LIMIT $limit") { rs ->
                while (rs.next()) rows += mapOf(
                    "tiploc"    to rs.getString(1),
                    "oldName"   to rs.getString(2),
                    "newName"   to rs.getString(3),
                    "changedBy" to rs.getString(4),
                    "changedAt" to rs.getString(5)
                )
            }
        }
        return rows
    }


    fun loadCrsOverrides(): Map<String, String> {
        val result = mutableMapOf<String, String>()
        transaction {
            exec("SELECT tiploc, crs FROM tiploc_crs_overrides") { rs ->
                while (rs.next()) result[rs.getString(1)] = rs.getString(2)
            }
        }
        return result
    }

    fun setCrsOverride(tiploc: String, crs: String, changedBy: String) {
        transaction {
            execPrepared(
                "INSERT INTO tiploc_crs_overrides (tiploc, crs, changed_by) VALUES (?, ?, ?) " +
                "ON CONFLICT(tiploc) DO UPDATE SET crs = excluded.crs, changed_by = excluded.changed_by, changed_at = datetime('now')",
                tiploc, crs, changedBy
            )
        }
    }

    fun getCrsOverrides(): List<Map<String, String?>> {
        val rows = mutableListOf<Map<String, String?>>()
        transaction {
            exec("SELECT tiploc, crs, changed_by, changed_at FROM tiploc_crs_overrides ORDER BY tiploc") { rs ->
                while (rs.next()) rows += mapOf(
                    "tiploc"    to rs.getString(1),
                    "crs"       to rs.getString(2),
                    "changedBy" to rs.getString(3),
                    "changedAt" to rs.getString(4)
                )
            }
        }
        return rows
    }


    fun loadNameOverrides(): Map<String, String> {
        val result = mutableMapOf<String, String>()
        transaction {
            exec("SELECT tiploc, name FROM tiploc_name_overrides") { rs ->
                while (rs.next()) result[rs.getString(1)] = rs.getString(2)
            }
        }
        return result
    }

    fun setNameOverride(tiploc: String, name: String, changedBy: String) {
        transaction {
            execPrepared(
                "INSERT INTO tiploc_name_overrides (tiploc, name, changed_by) VALUES (?, ?, ?) " +
                "ON CONFLICT(tiploc) DO UPDATE SET name = excluded.name, changed_by = excluded.changed_by, changed_at = datetime('now')",
                tiploc, name, changedBy
            )
        }
    }

    
    fun updateSchedulesCrs(tiploc: String, crs: String) {
        val safeTiploc = tiploc.filter { it.isLetterOrDigit() }
        val safeCrs    = crs.filter { it.isLetterOrDigit() }
        if (safeTiploc.isEmpty() || safeCrs.isEmpty()) return
        transaction {
            exec("UPDATE schedules SET crs = '$safeCrs' WHERE tiploc = '$safeTiploc'")
        }
    }

            // ── Prepared statement helpers ────────────────────────────────────────────

    /** Execute a DML statement (INSERT/UPDATE/DELETE) with parameters. */
    private fun org.jetbrains.exposed.sql.Transaction.execPrepared(sql: String, vararg params: String) {
        val conn = (this.connection as org.jetbrains.exposed.sql.statements.jdbc.JdbcConnectionImpl).connection
        conn.prepareStatement(sql).use { stmt ->
            params.forEachIndexed { i, v -> stmt.setString(i + 1, v) }
            stmt.executeUpdate()
        }
    }

    /** Execute a SELECT with parameters and process the ResultSet. */
    private fun org.jetbrains.exposed.sql.Transaction.execQuery(
        sql: String,
        vararg params: String,
        block: (java.sql.ResultSet) -> Unit
    ) {
        val conn = (this.connection as org.jetbrains.exposed.sql.statements.jdbc.JdbcConnectionImpl).connection
        conn.prepareStatement(sql).use { stmt ->
            params.forEachIndexed { i, v -> stmt.setString(i + 1, v) }
            block(stmt.executeQuery())
        }
    }
}

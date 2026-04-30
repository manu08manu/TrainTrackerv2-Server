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
        }
        transaction {
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
        }
        log.info("AuthDatabase initialised")
    }

    /** Create or overwrite a user. Safe to call for initial setup. */
    fun setUser(email: String, password: String): Boolean {
        val hash = BCrypt.hashpw(password, BCrypt.gensalt())
        return try {
            transaction {
                exec("DELETE FROM admin_users WHERE email = '${e(email)}'")
                exec("INSERT INTO admin_users (email, password_hash) VALUES ('${e(email)}', '${e(hash)}')")
            }
            log.info("User set: $email")
            true
        } catch (ex: Exception) {
            log.error("setUser failed: \${ex.message}")
            false
        }
    }

    fun deleteUser(email: String) {
        transaction {
            exec("DELETE FROM admin_users WHERE email = '${e(email)}'")
            exec("DELETE FROM admin_sessions WHERE email = '${e(email)}'")
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
            exec("SELECT password_hash FROM admin_users WHERE email = '${e(email)}'") { rs ->
                if (rs.next()) hash = rs.getString(1)
            }
        }
        val stored = hash ?: return null
        if (!BCrypt.checkpw(password, stored)) return null
        val token = UUID.randomUUID().toString()
        transaction {
            exec("INSERT INTO admin_sessions (token, email, expires_at) VALUES ('$token', '${e(email)}', datetime('now', '+7 days'))")
        }
        return token
    }

    fun validateSession(token: String): String? {
        var email: String? = null
        transaction {
            exec("SELECT email FROM admin_sessions WHERE token = '${e(token)}' AND expires_at > datetime('now')") { rs ->
                if (rs.next()) email = rs.getString(1)
            }
        }
        return email
    }

    fun logout(token: String) {
        transaction { exec("DELETE FROM admin_sessions WHERE token = '${e(token)}'") }
    }

    fun logTiplocChange(tiploc: String, oldName: String?, newName: String, changedBy: String) {
        try {
            transaction {
                exec("INSERT INTO tiploc_audit (tiploc, old_name, new_name, changed_by) VALUES ('${e(tiploc)}', ${if (oldName != null) "'${e(oldName)}'" else "NULL"}, '${e(newName)}', '${e(changedBy)}')")
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

    private fun e(s: String) = s.replace("'", "''")
}

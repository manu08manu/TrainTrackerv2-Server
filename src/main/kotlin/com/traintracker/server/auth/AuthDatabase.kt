package com.traintracker.server.auth

import com.traintracker.server.database.count
import com.traintracker.server.database.execUpdate
import com.traintracker.server.database.queryFirst
import com.traintracker.server.database.queryList
import java.util.UUID
import kotlinx.serialization.Serializable
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

@Serializable
data class UserInfo(
    val email: String,
    val isMasterAdmin: Boolean
)

/** Admin users and login sessions. */
object AuthDatabase {
    private val log = LoggerFactory.getLogger("AuthDatabase")

    /** Creates or replaces a user's password, keeping their master admin flag. */
    fun setUser(email: String, password: String): Boolean {
        val hash = PasswordHasher.hash(password)
        val wasMaster = isMasterAdmin(email)
        return try {
            transaction {
                execUpdate("DELETE FROM admin_users WHERE email = ?", listOf(email))
                execUpdate(
                    "INSERT INTO admin_users (email, password_hash, is_master_admin) VALUES (?, ?, ?)",
                    listOf(email, hash, if (wasMaster) "1" else "0")
                )
            }
            log.info("User set: $email")
            true
        } catch (e: Exception) {
            log.error("setUser failed: ${e.message}")
            false
        }
    }

    fun isMasterAdmin(email: String): Boolean = transaction {
        queryFirst("SELECT is_master_admin FROM admin_users WHERE email = ?", listOf(email)) { it.getInt(1) == 1 } ?: false
    }

    fun countMasterAdmins(): Int = transaction { count("SELECT COUNT(*) FROM admin_users WHERE is_master_admin = 1") }

    /** Returns false (and changes nothing) when this would demote the last master admin. */
    fun setMasterAdmin(email: String, isMaster: Boolean): Boolean {
        if (!isMaster && isMasterAdmin(email) && countMasterAdmins() <= 1) return false
        transaction {
            execUpdate("UPDATE admin_users SET is_master_admin = ? WHERE email = ?", listOf(if (isMaster) "1" else "0", email))
        }
        return true
    }

    fun deleteUser(email: String) {
        transaction {
            execUpdate("DELETE FROM admin_users WHERE email = ?", listOf(email))
            execUpdate("DELETE FROM admin_sessions WHERE email = ?", listOf(email))
        }
    }

    fun listUsers(): List<UserInfo> = transaction {
        queryList("SELECT email, is_master_admin FROM admin_users ORDER BY email") { UserInfo(it.getString(1), it.getInt(2) == 1) }
    }

    /** Returns a new 7-day session token, or null for a wrong email or password. */
    fun login(email: String, password: String): String? {
        val hash = transaction {
            queryFirst("SELECT password_hash FROM admin_users WHERE email = ?", listOf(email)) { it.getString(1) }
        } ?: return null
        val result = PasswordHasher.verify(password, hash)
        if (!result.ok) return null
        if (result.needsRehash) {
            val upgraded = PasswordHasher.hash(password)
            transaction { execUpdate("UPDATE admin_users SET password_hash = ? WHERE email = ?", listOf(upgraded, email)) }
            log.info("Password hash upgraded")
        }
        val token = UUID.randomUUID().toString()
        transaction {
            execUpdate(
                "INSERT INTO admin_sessions (token, email, expires_at) VALUES (?, ?, datetime('now', '+7 days'))",
                listOf(token, email)
            )
        }
        return token
    }

    /** The email of a valid session, or null. */
    fun validateSession(token: String): String? = transaction {
        queryFirst("SELECT email FROM admin_sessions WHERE token = ? AND expires_at > datetime('now')", listOf(token)) { it.getString(1) }
    }

    fun logout(token: String) {
        transaction { execUpdate("DELETE FROM admin_sessions WHERE token = ?", listOf(token)) }
    }
}

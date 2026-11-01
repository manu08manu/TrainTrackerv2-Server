package com.traintracker.server.routes

import com.traintracker.server.auth.AuthDatabase
import com.traintracker.server.cif.CorpusLookup
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

private val srLog = LoggerFactory.getLogger("StanoxReview")
private val TIPLOC_RE = Regex("[A-Z0-9]{1,8}")
private val STANOX_RE = Regex("\\d{5}")

private fun srIso(raw: String?): String {
    if (raw.isNullOrEmpty()) return ""
    val s = raw.replace(' ', 'T')
    return if (s.endsWith("Z")) s else "${s}Z"
}

object StanoxReview {
    @Volatile
    private var columnReady = false

    private fun ensureColumn() {
        if (columnReady) return
        var has = false
        transaction {
            exec("SELECT COUNT(*) FROM pragma_table_info('location_overrides') WHERE name = 'no_stanox'") { rs ->
                if (rs.next()) has = rs.getInt(1) > 0
            }
            if (!has) exec("ALTER TABLE location_overrides ADD COLUMN no_stanox INTEGER NOT NULL DEFAULT 0")
        }
        columnReady = true
    }

    fun currentStanox(tiploc: String): String? {
        if (!TIPLOC_RE.matches(tiploc)) return null
        var s: String? = null
        try {
            transaction {
                exec("SELECT stanox FROM location_overrides WHERE tiploc = '$tiploc'") { rs ->
                    if (rs.next()) s = rs.getString(1)
                }
            }
        } catch (e: Exception) {
            srLog.warn("currentStanox failed for $tiploc: ${e.message}")
        }
        return s?.takeIf { it.isNotEmpty() }
    }

    fun fillOnNaming(tiploc: String, changedBy: String, stanoxBefore: String?): String {
        try {
            val now = currentStanox(tiploc)
            if (now != null) return now
            val target = stanoxBefore ?: CorpusLookup.cifStanoxForTiploc(tiploc) ?: return ""
            if (!STANOX_RE.matches(target)) return ""
            if (stanoxBefore == null && AuthDatabase.stanoxConflicts(target, tiploc).toList().isNotEmpty()) return ""
            AuthDatabase.setStanoxOverride(tiploc, target, changedBy)
            CorpusLookup.reloadStanoxOverrides()
            return target
        } catch (e: Exception) {
            srLog.warn("fillOnNaming failed for $tiploc: ${e.message}")
            return ""
        }
    }

    fun pending(): List<Map<String, String>> {
        val out = mutableListOf<Map<String, String>>()
        try {
            ensureColumn()
            transaction {
                exec("SELECT tiploc, name, crs, changed_at FROM location_overrides WHERE (stanox IS NULL OR stanox = '') AND no_stanox = 0 ORDER BY changed_at DESC LIMIT 500") { rs ->
                    while (rs.next()) {
                        val tiploc = rs.getString("tiploc") ?: continue
                        val at = srIso(rs.getString("changed_at"))
                        out.add(
                            mapOf(
                                "tiploc" to tiploc,
                                "seenCount" to "No unique CIF STANOX",
                                "firstSeen" to at,
                                "lastSeen" to at,
                                "headcode" to (rs.getString("crs") ?: ""),
                                "uid" to (rs.getString("name") ?: "")
                            )
                        )
                    }
                }
            }
        } catch (e: Exception) {
            srLog.warn("pending failed: ${e.message}")
        }
        return out
    }

    fun markNoStanox(tiploc: String) {
        if (!TIPLOC_RE.matches(tiploc)) return
        ensureColumn()
        transaction {
            exec("UPDATE location_overrides SET no_stanox = 1 WHERE tiploc = '$tiploc'")
        }
    }
}

fun Route.stanoxReviewRoutes() {
    adminGet("/admin/missing-stanox-review") {
        respond(mapOf("missing" to StanoxReview.pending()))
    }
    adminDelete("/admin/missing-stanox-review") {
        val tiploc = param("tiploc", true)
        if (tiploc.isEmpty()) {
            badRequest("tiploc required")
            return@adminDelete
        }
        StanoxReview.markNoStanox(tiploc)
        respondOk("tiploc" to tiploc)
    }
}

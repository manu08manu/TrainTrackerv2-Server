package com.traintracker.server.routes

import com.traintracker.server.auth.AuthDatabase
import com.traintracker.server.cif.CorpusLookup
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

private val srLog = LoggerFactory.getLogger("StanoxReview")
private val TIPLOC_RE = Regex("[A-Z0-9]{1,8}")
private val STANOX_RE = Regex("\\d{5}")

object StanoxReview {
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
}

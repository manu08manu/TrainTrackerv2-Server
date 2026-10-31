package com.traintracker.server.routes

import com.traintracker.server.cif.CorpusLookup
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import org.jetbrains.exposed.sql.transactions.transaction

fun Route.smartCoverageRoutes() {
    adminGet("/admin/missing-smart-stanox") {
        respond(mapOf("missing" to loadSmartGaps()))
    }
    adminDelete("/admin/missing-smart-stanox") {
        val key = param("stanox")
        if (key.isEmpty()) {
            badRequest("stanox required")
            return@adminDelete
        }
        deleteSmartGap(key)
        respondOk("stanox" to key)
    }
}

private fun isoUtc(raw: String?): String {
    if (raw.isNullOrEmpty()) return ""
    val s = raw.replace(' ', 'T')
    return if (s.endsWith("Z")) s else "${s}Z"
}

private fun loadSmartGaps(): List<Map<String, String>> {
    val rows = mutableListOf<Map<String, String>>()
    try {
        transaction {
            exec("SELECT stanox, seen_count, first_seen, last_seen, headcode FROM missing_smart_stanox ORDER BY seen_count DESC, last_seen DESC LIMIT 500") { rs ->
                while (rs.next()) {
                    val key = rs.getString("stanox") ?: ""
                    val sx = key.substringBefore('|')
                    val byTiploc = CorpusLookup.tiplocFromStanox(sx)?.let { CorpusLookup.nameFromTiploc(it) }
                    val crs = CorpusLookup.crsFromStanox(sx)
                    val byCrs = if (byTiploc == null && crs != null) {
                        CorpusLookup.tiplocFromCrs(crs).firstNotNullOfOrNull { CorpusLookup.nameFromTiploc(it) }
                    } else null
                    val label = byTiploc ?: byCrs
                    val name = if (label != null && crs != null) "$label ($crs)" else label ?: crs ?: ""
                    rows.add(
                        mapOf(
                            "stanox" to key,
                            "seenCount" to (rs.getString("seen_count") ?: "0"),
                            "firstSeen" to isoUtc(rs.getString("first_seen")),
                            "lastSeen" to isoUtc(rs.getString("last_seen")),
                            "headcode" to (rs.getString("headcode") ?: ""),
                            "uid" to name
                        )
                    )
                }
            }
        }
    } catch (e: Exception) {
    }
    return rows
}

private fun deleteSmartGap(key: String) {
    val id = key.substringBefore('|')
    if (id.isEmpty() || id.length > 6 || !id.all { it.isLetterOrDigit() }) return
    if (key != "$id|ARR" && key != "$id|DEP") return
    transaction {
        exec("DELETE FROM missing_smart_stanox WHERE stanox = '$key'")
    }
}

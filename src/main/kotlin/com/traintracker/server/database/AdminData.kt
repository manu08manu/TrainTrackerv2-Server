package com.traintracker.server.database

import org.jetbrains.exposed.v1.jdbc.transactions.transaction

/** Data behind the admin dashboard: operators, the "missing" work queues, CIF change alerts and user feedback. */
object AdminData {

    // ---- Operators -----------------------------------------------------------------------------------

    fun operators(): List<Map<String, String?>> = transaction {
        queryList("SELECT code, name, colour FROM operators ORDER BY name COLLATE NOCASE") { rs ->
            mapOf("code" to rs.getString(1), "name" to rs.getString(2), "colour" to rs.getString(3)?.takeIf { it.isNotEmpty() })
        }
    }

    fun upsertOperator(code: String, name: String, colour: String) {
        transaction {
            execUpdate(
                "INSERT INTO operators (code, name, colour) VALUES (?, ?, ?) ON CONFLICT(code) DO UPDATE SET name = excluded.name, colour = excluded.colour",
                listOf(code, name, colour)
            )
        }
    }

    fun deleteOperator(code: String) {
        transaction { execUpdate("DELETE FROM operators WHERE code = ?", listOf(code)) }
    }

    // ---- Missing TIPLOC names and TSC operators ------------------------------------------------------

    fun missingTiplocs(): List<Map<String, String>> = missingItems("missing_tiplocs", "tiploc")

    fun deleteMissingTiploc(tiploc: String) = deleteMissingItem("missing_tiplocs", "tiploc", tiploc)

    fun missingTscs(): List<Map<String, String>> = missingItems("missing_tscs", "tsc")

    fun deleteMissingTsc(tsc: String) = deleteMissingItem("missing_tscs", "tsc", tsc)

    private fun missingItems(table: String, keyColumn: String): List<Map<String, String>> = transaction {
        queryList("SELECT $keyColumn, seen_count, first_seen, last_seen, headcode, uid FROM $table ORDER BY $keyColumn ASC LIMIT 200") { rs ->
            mapOf(
                keyColumn to (rs.getString(keyColumn) ?: ""),
                "seenCount" to (rs.getString("seen_count") ?: "0"),
                "firstSeen" to toIsoUtc(rs.getString("first_seen")),
                "lastSeen" to toIsoUtc(rs.getString("last_seen")),
                "headcode" to (rs.getString("headcode") ?: ""),
                "uid" to (rs.getString("uid") ?: ""),
            )
        }
    }

    private fun deleteMissingItem(table: String, keyColumn: String, key: String) {
        transaction { execUpdate("DELETE FROM $table WHERE $keyColumn = ?", listOf(key)) }
    }

    // ---- STANOX events with no SMART berth coverage --------------------------------------------------

    class SmartGap(val key: String, val seenCount: String, val firstSeen: String, val lastSeen: String, val headcode: String)

    fun smartGaps(): List<SmartGap> = try {
        transaction {
            queryList("SELECT stanox, seen_count, first_seen, last_seen, headcode FROM missing_smart_stanox ORDER BY seen_count DESC, last_seen DESC LIMIT 500") { rs ->
                SmartGap(
                    rs.getString("stanox") ?: "",
                    rs.getString("seen_count") ?: "0",
                    toIsoUtc(rs.getString("first_seen")),
                    toIsoUtc(rs.getString("last_seen")),
                    rs.getString("headcode") ?: "",
                )
            }
        }
    } catch (e: Exception) {
        emptyList()
    }

    /** Deletes one "STANOX|ARR" or "STANOX|DEP" entry; anything else is ignored. */
    fun deleteSmartGap(key: String) {
        val stanox = key.substringBefore('|')
        if (stanox.isEmpty() || stanox.length > 6 || !stanox.all { it.isLetterOrDigit() }) return
        if (key != "$stanox|ARR" && key != "$stanox|DEP") return
        transaction { execUpdate("DELETE FROM missing_smart_stanox WHERE stanox = ?", listOf(key)) }
    }

    // ---- CIF name/CRS change alerts (written by scripts/cif_name_watch.sh) ---------------------------

    fun cifChangeAlerts(): List<Map<String, String>> = transaction {
        queryList(
            "SELECT tiploc, old_desc, old_crs, new_desc, new_crs, first_seen, last_seen FROM cif_change_alerts WHERE dismissed_at IS NULL ORDER BY last_seen DESC LIMIT 200"
        ) { rs ->
            mapOf(
                "tiploc" to (rs.getString("tiploc") ?: ""),
                "oldDesc" to (rs.getString("old_desc") ?: ""),
                "oldCrs" to (rs.getString("old_crs") ?: ""),
                "newDesc" to (rs.getString("new_desc") ?: ""),
                "newCrs" to (rs.getString("new_crs") ?: ""),
                "firstSeen" to toIsoUtc(rs.getString("first_seen")),
                "lastSeen" to toIsoUtc(rs.getString("last_seen")),
            )
        }
    }

    fun dismissCifChangeAlert(tiploc: String, oldDesc: String, oldCrs: String, newDesc: String, newCrs: String) {
        transaction {
            execUpdate(
                "UPDATE cif_change_alerts SET dismissed_at = datetime('now') WHERE tiploc = ? AND old_desc = ? AND old_crs = ? AND new_desc = ? AND new_crs = ? AND dismissed_at IS NULL",
                listOf(tiploc, oldDesc, oldCrs, newDesc, newCrs)
            )
        }
    }

    // ---- Feedback ------------------------------------------------------------------------------------

    fun feedback(): List<Map<String, String>> = transaction {
        queryList("SELECT id, message, page, submitted_at FROM feedback WHERE dismissed_at IS NULL ORDER BY submitted_at DESC LIMIT 200") { rs ->
            mapOf(
                "id" to (rs.getString("id") ?: ""),
                "message" to (rs.getString("message") ?: ""),
                "page" to (rs.getString("page") ?: ""),
                "submittedAt" to toIsoUtc(rs.getString("submitted_at")),
            )
        }
    }

    fun submitFeedback(message: String, page: String) {
        transaction { execUpdate("INSERT INTO feedback (message, page) VALUES (?, ?)", listOf(message, page)) }
    }

    fun dismissFeedback(id: String) {
        transaction {
            execUpdate("UPDATE feedback SET dismissed_at = datetime('now') WHERE id = ? AND dismissed_at IS NULL", listOf(id))
        }
    }
}

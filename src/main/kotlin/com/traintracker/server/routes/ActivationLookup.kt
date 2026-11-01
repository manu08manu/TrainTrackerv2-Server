package com.traintracker.server.routes

import org.jetbrains.exposed.v1.jdbc.JdbcTransaction

internal data class ActivationInfo(
    val updatedAt: String,
    val originStanox: String?
)

internal fun JdbcTransaction.activationsFor(uids: Collection<String>, date: String): Map<String, ActivationInfo> {
    if (uids.isEmpty()) return emptyMap()
    val dayOfMonth = date.takeLast(2)
    val result = mutableMapOf<String, ActivationInfo>()
    uids.chunked(500).forEach { chunk ->
        exec(
            "SELECT uid, updated_at, origin_stanox FROM train_activations WHERE uid IN (${chunk.joinToString(",") { "?" }}) AND updated_at >= datetime(?, '-12 hours') AND updated_at < datetime(?, '+36 hours') AND train_id LIKE '%' || ? ORDER BY updated_at DESC",
            chunk.map { bindStr(it) } + listOf(bindStr(date), bindStr(date), bindStr(dayOfMonth))
        ) { rs ->
            while (rs.next()) {
                val uid = rs.getString("uid")
                if (!result.containsKey(uid)) {
                    result[uid] = ActivationInfo(rs.getString("updated_at"), rs.getString("origin_stanox"))
                }
            }
        }
    }
    return result
}

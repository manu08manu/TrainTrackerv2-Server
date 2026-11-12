package com.traintracker.server.routes

import com.traintracker.server.ReasonCodes
import com.traintracker.server.database.placeholders
import com.traintracker.server.database.queryList
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction

/**
 * LINX location changes for one service:
 * - code 06: replace a booked location with the location used instead;
 * - code 95: the train starts from a changed location;
 * - code 93: the train is cut short to a changed location.
 */
internal data class LocationChanges(
    val replaced: Map<String, String>,
    val movedOrigin: String?,
    val movedOriginReason: String?,
    val cutShort: CutShort?,
)

/** Code 93: the train ends at [to] instead of continuing from [from]. */
internal data class CutShort(
    val from: String,
    val to: String,
    val time: String,
    val reason: String?,
)

private data class LinxLocationMessage(
    val uid: String,
    val locations: String,
    val reason: String?,
)

private class LocationChangesBuilder {
    private val replaced = mutableMapOf<String, String>()
    private var startMovedFrom: String? = null
    private var startMovedTo: String? = null
    private var startMovedReason: String? = null
    private var cutShort: CutShort? = null

    fun add(message: String, reason: String?) {
        val entries = message.split(';')
            .map { it.split('|') }
            .filter { it.size >= 2 && it[0].isNotEmpty() }

        fun marked(indicator: String) =
            entries.filter { it[1] == indicator }.map { it[0] }

        marked("03").zip(marked("04")).forEach { (from, to) ->
            // A later message with the same replacement supersedes the earlier one.
            // 1K70: Elmers End -> Orpington, then Ladywell -> Orpington.
            replaced.entries.removeAll { it.value == to && it.key != from }
            replaced[from] = to
        }

        marked("95").firstOrNull()?.let { from ->
            startMovedFrom = from
            startMovedTo = marked("04").firstOrNull()
            startMovedReason = ReasonCodes.resolve(reason)
        }

        val cutFrom = marked("93").firstOrNull()
        val end = entries.firstOrNull { it[1] == "04" }
        if (cutFrom != null && end != null) {
            cutShort = CutShort(
                cutFrom,
                end[0],
                end.getOrNull(3)?.take(5) ?: "",
                ReasonCodes.resolve(reason)
            )
        }
    }

    fun build(): LocationChanges {
        val movedOrigin = startMovedFrom?.let { from -> startMovedTo ?: replaced[from] }
        return LocationChanges(
            replaced.toMap(),
            movedOrigin,
            startMovedReason.takeIf { movedOrigin != null },
            cutShort
        )
    }
}

/** Single-service convenience wrapper used by the service-detail route. */
internal fun JdbcTransaction.locationChanges(uid: String, svcDate: String): LocationChanges =
    locationChanges(listOf(uid), svcDate)[uid]
        ?: LocationChanges(emptyMap(), null, null, null)

/**
 * Loads LINX changes for multiple UIDs in batches, so search results and boards
 * do not need to issue one query per service.
 */
internal fun JdbcTransaction.locationChanges(
    uids: Collection<String>,
    svcDate: String
): Map<String, LocationChanges> {
    if (uids.isEmpty()) return emptyMap()

    val builders = mutableMapOf<String, LocationChangesBuilder>()

    uids.distinct().chunked(500).forEach { chunk ->
        val messages = queryList(
            "SELECT uid, locations, reason FROM linx_modifications " +
                "WHERE uid IN (${placeholders(chunk.size)}) " +
                "AND start_date = ? AND code IN ('06', '93', '95') " +
                "AND locations IS NOT NULL ORDER BY uid, id",
            chunk + svcDate
        ) { rs ->
            val uid = rs.getString("uid") ?: return@queryList null
            val locations = rs.getString("locations") ?: return@queryList null
            LinxLocationMessage(uid, locations, rs.getString("reason"))
        }

        messages.forEach { message ->
            builders.getOrPut(message.uid) { LocationChangesBuilder() }
                .add(message.locations, message.reason)
        }
    }

    return builders.mapValues { (_, builder) -> builder.build() }
}

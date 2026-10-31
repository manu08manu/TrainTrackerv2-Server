package com.traintracker.server.routes

import com.traintracker.server.auth.AuthDatabase
import com.traintracker.server.kafka.darwinPlatforms
import com.traintracker.server.kb.KbIncident
import io.ktor.http.HttpStatusCode
import com.traintracker.server.auth.authRoutes
import com.traintracker.server.kafka.trainLocations
import io.ktor.http.ContentType
import io.ktor.server.request.receiveText
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import com.traintracker.server.hsp.HspClient
import com.traintracker.server.hsp.HspMetricsRequest
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.http.content.staticFiles
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import java.io.File
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.header
import io.ktor.server.response.respond
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.put
import org.jetbrains.exposed.sql.IColumnType
import org.jetbrains.exposed.sql.VarCharColumnType
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.LocalDate
import com.traintracker.server.ReasonCodes
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.ScheduleCancellations
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.Schedules
import com.traintracker.server.database.HeadcodeAliases
import com.traintracker.server.database.TrainLocations
import com.traintracker.server.database.TrustMovements
import com.traintracker.server.database.effectiveDateFor
import com.traintracker.server.database.scheduleDateScope
import com.traintracker.server.database.scheduleDateScopeArgs
import com.traintracker.server.kafka.TrainLocation
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Op
import org.jetbrains.exposed.sql.min
import org.jetbrains.exposed.sql.max
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.SqlExpressionBuilder.greaterEq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.lessEq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.neq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.inList
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.or
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.selectAll
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter

private enum class BoardMode { DEPARTURES, ARRIVALS, ALL }

private enum class RolledFilter { ANY, ONLY, EXCLUDE }

data class ChainHop(val uid: String, val headcode: String, val date: String)

private data class OriginDestOverrides(
    val oLtOverrides: Map<String, Pair<String, String?>>,
    val oLoOverrides: Map<String, Pair<String, String?>>,
    val activationOrigins: Map<String, String>
)

internal val STP_PRIORITY: Map<Char, Int> = mapOf('N' to 0, 'O' to 1, 'P' to 2, 'C' to 3)

private val bindStrColType: IColumnType<*> = VarCharColumnType()

internal suspend fun ApplicationCall.badRequest(msg: String) {
    respond(HttpStatusCode.BadRequest, mapOf("error" to msg))
}

internal fun List<KbIncident>.toJson(): JsonArray = buildJsonArray {
    for (inc in this@toJson) {
        add(buildJsonObject {
            put("id", inc.id)
            put("summary", inc.summary)
            put("description", inc.description)
            put("isPlanned", inc.isPlanned)
            put("startTime", inc.startTime)
            put("endTime", inc.endTime)
            put("operators", Json.encodeToJsonElement(inc.operators))
            put("link", inc.link)
        })
    }
}

private fun ApplicationCall.dateParam(): String =
    parameters["date"]?.trim()?.takeIf { it.length == 10 } ?: LocalDate.now().toString()

internal suspend fun ApplicationCall.validCrs(): String? {
    val crs = parameters["crs"]?.uppercase()?.trim()
    if (crs == null) {
        respond(HttpStatusCode.BadRequest, "crs required")
        return null
    }
    if (!isValidCrs(crs)) {
        respond(HttpStatusCode.BadRequest, "crs must be exactly 3 letters")
        return null
    }
    return crs
}

internal fun ApplicationCall.changedBy(): String =
    AuthDatabase.validateSession(request.header("Authorization")?.removePrefix("Bearer ")?.trim() ?: "") ?: "unknown"

internal fun bindStr(v: String): Pair<IColumnType<*>, Any?> = bindStrColType to (v as Any?)

internal fun toIsoUtc(raw: String?): String {
    if (raw.isNullOrEmpty()) return ""
    val s = raw.replace(' ', 'T')
    return if (s.endsWith("Z")) s else "${s}Z"
}

suspend fun walkFormsChain(startUid: String, startDate: String, maxDepth: Int = 5): List<ChainHop> {
    val hops: MutableList<ChainHop> = ArrayList()
    val seen = mutableSetOf(startUid)
    var curUid = startUid
    var curDate = startDate
    repeat(maxDepth) {
        val next = withContext(Dispatchers.IO) {
            transaction {
                val ctx = buildAssociationContext(setOf(curUid), curDate)
                resolveNextForms(curUid, ctx, curDate)
            }
        } ?: return hops
        if (seen.contains(next.uid)) return hops
        val nextDate = resolveConnectionDate(curDate, next.dateIndicator)
        val cancelled = withContext(Dispatchers.IO) {
            transaction { isUidWhollyCancelled(next.uid, nextDate) }
        }
        if (cancelled) return hops
        hops.add(ChainHop(next.uid, next.headcode, nextDate))
        seen.add(next.uid)
        curUid = next.uid
        curDate = nextDate
    }
    return hops
}

internal fun isPassengerHeadcode(headcode: String): Boolean = headcode.firstOrNull()?.let { "1239".contains(it) } == true

internal fun isValidCrs(crs: String): Boolean = crs.length == 3 && crs.all { it.isLetter() }

private fun sqlPlaceholders(values: Collection<String>): Pair<String, List<Pair<IColumnType<*>, Any?>>> {
    val list = values.toList()
    val placeholders = list.joinToString(",") { "?" }
    val binds = list.map { bindStr(it) }
    return placeholders to binds
}

internal fun String.toMinutes(): Int {
    return split(":").let { p -> (p.getOrNull(0)?.toIntOrNull() ?: 0) * 60 + (p.getOrNull(1)?.toIntOrNull() ?: 0) }
}

internal fun wrapDelta(actual: String, sched: String): Int = (actual.toMinutes() - sched.toMinutes() + 720).mod(1440) - 720

private fun wrapAwareMinutes(t: String, anchorMinutes: Int): Int {
    val m = t.toMinutes()
    return if (m < anchorMinutes - 720) m + 1440 else m
}

internal fun resolvePlatformWithConfirmed(
    uid: String,
    tiploc: String,
    cifPlatform: String?,
    dbPlatforms: Map<String, String>,
    trustPlatforms: Map<Pair<String, String>, String> = emptyMap(),
    crs: String? = null
): Pair<String?, Boolean> {
    if (crs != null) {
        val live = trustPlatforms[uid to crs]
        if (live != null) return Pair(live, true)
    }
    val darwin = darwinPlatforms[uid]?.get(tiploc)
    if (darwin != null) return Pair(darwin, true)
    val db = dbPlatforms[tiploc]
    if (db != null) return Pair(db, true)
    return Pair(cifPlatform, false)
}

private fun resolvePlatformForBoard(
    uid: String,
    tiploc: String,
    cifPlatform: String?,
    dbPlatforms: Map<Pair<String, String>, String>,
    trustPlatforms: Map<Pair<String, String>, String> = emptyMap(),
    crs: String? = null
): Pair<String?, Boolean> {
    if (crs != null) {
        val live = trustPlatforms[uid to crs]
        if (live != null) return Pair(live, true)
    }
    val darwin = darwinPlatforms[uid]?.get(tiploc)
    if (darwin != null) return Pair(darwin, true)
    val db = dbPlatforms[uid to tiploc]
    if (db != null) return Pair(db, true)
    return Pair(cifPlatform, false)
}

private suspend fun ApplicationCall.handleBoardRequest(mode: BoardMode) {
    val crs = parameters["crs"]?.uppercase()?.trim()
    if (crs == null) {
        respond(HttpStatusCode.BadRequest, "crs required")
        return
    }
    val window = parameters["window"]?.toIntOrNull() ?: 120
    val offsetRaw = parameters["offset"]?.toIntOrNull() ?: 0
    val date = dateParam()
    val isToday = date == LocalDate.now().toString()
    val (start, end) = if (isToday) {
        val nowMins = LocalTime.now().let { it.hour * 60 + it.minute }
        val offset = maxOf(offsetRaw, -nowMins)
        if (offset >= 1440 - nowMins) {
            respond(BoardResponse(crs, mode.name, "23:59", "23:59", emptyList()))
            return
        }
        timeWindow(window, offset)
    } else {
        timeWindow(window, offsetRaw)
    }
    val early = end >= start && start.toMinutes() < 780
    val prevDate = try {
        LocalDate.parse(date).minusDays(1L).toString()
    } catch (e: Exception) {
        date
    }
    val services = withContext(Dispatchers.IO) {
        val own = annotateSplits(enrichWithUnits(queryBoard(crs, start, end, mode, true, true, date, if (early) RolledFilter.EXCLUDE else RolledFilter.ANY), date))
        if (!early) {
            own
        } else {
            val overnight = annotateSplits(enrichWithUnits(queryBoard(crs, start, end, mode, true, true, prevDate, RolledFilter.ONLY), prevDate))
            (own + overnight).sortedBy { s ->
                s.scheduledTime.toMinutes().let { sm ->
                    if (s.isCancelled || s.actualTime.isEmpty()) sm else sm + wrapDelta(s.actualTime, s.scheduledTime)
                }
            }
        }
    }
    respond(BoardResponse(crs, mode.name, start, end, services))
}

private fun AppDatabase.AllocationResult.toResponse(): AllocationResponse = AllocationResponse(
    coreId, headcode, serviceDate, operator, units, vehicles, unitCount, vehicles.size, updatedAt
)

private fun TrainLocation.toResponse(): TrainLocationResponse = TrainLocationResponse(
    headcode, rid, stationName, crs, actualTime, eventType, delayMinutes,
    (System.currentTimeMillis() - updatedEpochMs) / 1000
)

private fun ResultRow.toLocationResponse(): TrainLocationResponse = TrainLocationResponse(
    this[TrainLocations.headcode],
    this[TrainLocations.rid],
    this[TrainLocations.stationName],
    this[TrainLocations.crs],
    this[TrainLocations.actualTime],
    this[TrainLocations.eventType],
    this[TrainLocations.delayMinutes],
    -1L
)

fun Application.configureRoutes() {
    routing {
        pageRoutes()
        staticFiles("/css", File("/opt/traintracker/static/css"))
        staticFiles("/js", File("/opt/traintracker/static/js"))
        staticFiles("/icons", File("/opt/traintracker/static/icons"))
        route("/api") {
            authRoutes()
            operatorRoutes()
            smartCoverageRoutes()
            stanoxReviewRoutes()
            smartBerthRoutes()
            get("/cif-changes") {
                val file = File("/opt/traintracker/cif_changes.json")
                if (file.exists()) {
                    call.respondText(file.readText(), ContentType.Application.Json)
                } else {
                    call.respondText("{\"last_run\":null,\"checked\":0,\"change_count\":0,\"changes\":[]}", ContentType.Application.Json)
                }
            }
            prideRoutes()
            get("/status") {
                call.respond(AppDatabase.getEnrichedStatus(trainLocations.isNotEmpty(), trainLocations.size))
            }
            get("/departures") {
                call.handleBoardRequest(BoardMode.DEPARTURES)
            }
            get("/arrivals") {
                call.handleBoardRequest(BoardMode.ARRIVALS)
            }
            serviceDetailRoutes()
            get("/allservices") {
                val crs = call.parameters["crs"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "crs required")
                val window = call.parameters["window"]?.toIntOrNull() ?: 120
                val offset = call.parameters["offset"]?.toIntOrNull() ?: 0
                val date = call.dateParam()
                val (start, end) = timeWindow(window, offset)
                val services = withContext(Dispatchers.IO) {
                    enrichWithUnits(queryBoard(crs, start, end, BoardMode.ALL, false, false, date), date)
                }
                call.respond(BoardResponse(crs, "ALL", start, end, services))
            }
            get("/trust/{headcode}") {
                val headcode = call.parameters["headcode"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "headcode required")
                val loc = trainLocations.values.filter { it.headcode == headcode }.maxByOrNull { it.updatedEpochMs }
                if (loc != null) {
                    call.respond(loc.toResponse())
                } else {
                    val dbLoc = withContext(Dispatchers.IO) {
                        transaction {
                            TrainLocations.selectAll().where {
                                (TrainLocations.headcode eq headcode) and (TrainLocations.updatedAt greaterEq LocalDateTime.now().minusHours(24))
                            }.orderBy(TrainLocations.updatedAt, SortOrder.DESC).limit(1).firstOrNull()
                        }
                    }
                    if (dbLoc != null) {
                        call.respond(dbLoc.toLocationResponse())
                    } else {
                        call.respond(HttpStatusCode.NotFound, "No location data for $headcode")
                    }
                }
            }
            get("/trust/locate/{rid}") {
                val rid = call.parameters["rid"]?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "rid required")
                val loc = trainLocations.values.firstOrNull { it.rid == rid }
                if (loc != null) {
                    call.respond(loc.toResponse())
                } else {
                    val dbLoc = withContext(Dispatchers.IO) {
                        transaction {
                            TrainLocations.selectAll().where {
                                (TrainLocations.rid eq rid) and (TrainLocations.updatedAt greaterEq LocalDateTime.now().minusHours(24))
                            }.singleOrNull()
                        }
                    }
                    if (dbLoc != null) {
                        call.respond(dbLoc.toLocationResponse())
                    } else {
                        call.respond(HttpStatusCode.NotFound, "No location data for rid=$rid")
                    }
                }
            }
            get("/trust/movements") {
                val headcode = call.parameters["headcode"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "headcode required")
                val uid = call.parameters["uid"]?.uppercase()?.trim() ?: ""
                val safeHc = headcode.filter { it.isLetterOrDigit() }
                val safeUid = uid.filter { it.isLetterOrDigit() }
                val movements = withContext(Dispatchers.IO) {
                    transaction {
                        val cutoff = LocalDateTime.now().minusHours(3)
                        TrustMovements.selectAll().where {
                            (TrustMovements.headcode eq safeHc) and
                                (if (safeUid.isNotEmpty()) TrustMovements.uid eq safeUid else Op.TRUE) and
                                (TrustMovements.eventTs greaterEq cutoff)
                        }.orderBy(TrustMovements.eventTs, SortOrder.DESC).limit(50).map { row ->
                            MovementResponse(
                                crs = row[TrustMovements.crs] ?: row[TrustMovements.stanox],
                                eventType = row[TrustMovements.eventType],
                                scheduledTime = row[TrustMovements.scheduledTime] ?: "",
                                actualTime = row[TrustMovements.actualTime] ?: "",
                                platform = row[TrustMovements.platform] ?: "",
                                isCancelled = row[TrustMovements.isCancelled]
                            )
                        }
                    }
                }
                call.respond(MovementsResponse(headcode, movements))
            }
            get("/toc/{code}") {
                val code = call.parameters["code"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "toc required")
                val safeToc = code.filter { it.isLetterOrDigit() }
                val date = call.dateParam()
                val services = withContext(Dispatchers.IO) {
                    annotateSplits(enrichWithUnits(queryByToc(safeToc, date), date))
                }
                if (services.isEmpty()) {
                    call.respond(HttpStatusCode.NotFound, "No services found for operator $safeToc")
                } else {
                    call.respond(
                        BoardResponse(safeToc, "TOC", "00:00", "23:59", services.map { if (it.serviceDate.isEmpty()) it.copy(serviceDate = date) else it })
                    )
                }
            }
            get("/headcode/{headcode}") {
                val headcode = call.parameters["headcode"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "headcode required")
                val safeHc = headcode.filter { it.isLetterOrDigit() }
                val date = call.dateParam()
                var services = withContext(Dispatchers.IO) {
                    annotateSplits(enrichWithUnits(queryByHeadcode(safeHc, date), date))
                }
                if (services.isEmpty()) {
                    val aliases = HeadcodeAliases
                    val aliasHc = withContext(Dispatchers.IO) {
                        transaction {
                            aliases.selectAll().where { aliases.newHeadcode eq safeHc }.firstOrNull()
                                ?.get(aliases.oldHeadcode)
                                ?: aliases.selectAll().where { aliases.oldHeadcode eq safeHc }.firstOrNull()
                                    ?.get(aliases.newHeadcode)
                        }
                    }
                    if (aliasHc != null && aliasHc != safeHc) {
                        services = withContext(Dispatchers.IO) {
                            annotateSplits(enrichWithUnits(queryByHeadcode(aliasHc, date), date))
                        }.map { it.copy(headcode = safeHc) }
                    }
                }
                if (services.isNotEmpty()) {
                    val aliases = HeadcodeAliases
                    val fwd = withContext(Dispatchers.IO) {
                        transaction {
                            aliases.selectAll().where { aliases.oldHeadcode eq safeHc }.firstOrNull()
                                ?.takeIf { it[aliases.updatedAt].toLocalDate() == LocalDate.now() }
                                ?.let { it[aliases.newHeadcode] to it[aliases.uid] }
                        }
                    }
                    if (fwd != null) {
                        services = services.map { svc ->
                            if (fwd.second.isEmpty() || fwd.second == svc.uid) svc.copy(headcode = fwd.first) else svc
                        }
                    }
                }
                if (services.isEmpty()) {
                    call.respond(HttpStatusCode.NotFound, "No schedule found for headcode $headcode")
                } else {
                    call.respond(
                        BoardResponse(headcode, "HEADCODE", "00:00", "23:59", services.map { if (it.serviceDate.isEmpty()) it.copy(serviceDate = date) else it })
                    )
                }
            }
            get("/unit/{unit}") {
                val unit = call.parameters["unit"]?.uppercase()?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "unit required")
                val safeUnit = unit.filter { it.isLetterOrDigit() }
                val date = call.dateParam()
                val services = withContext(Dispatchers.IO) {
                    queryByUnit(safeUnit, date)
                }
                if (services.isEmpty()) {
                    call.respond(HttpStatusCode.NotFound, "No services found for unit $unit")
                } else {
                    call.respond(
                        BoardResponse(unit, "UNIT", "00:00", "23:59", services.map { if (it.serviceDate.isEmpty()) it.copy(serviceDate = date) else it })
                    )
                }
            }
            post("/hsp/metrics") {
                if (!HspClient.isAvailable) {
                    call.respond(HttpStatusCode.ServiceUnavailable, mapOf("error" to "HSP not configured on server"))
                    return@post
                }
                val body = call.receiveText()
                val json = try {
                    Json.parseToJsonElement(body).jsonObject
                } catch (e: Exception) {
                    call.badRequest("Invalid JSON body")
                    return@post
                }
                val fromLoc = json["from_loc"]?.jsonPrimitive?.content?.uppercase()
                val fromDate = json["from_date"]?.jsonPrimitive?.content
                if (fromLoc == null || fromDate == null) {
                    call.badRequest("from_loc and from_date required")
                    return@post
                }
                val req = HspMetricsRequest(
                    from_loc = fromLoc,
                    to_loc = json["to_loc"]?.jsonPrimitive?.content?.uppercase() ?: fromLoc,
                    from_time = json["from_time"]?.jsonPrimitive?.content ?: "0000",
                    to_time = json["to_time"]?.jsonPrimitive?.content ?: "2359",
                    from_date = fromDate,
                    to_date = json["to_date"]?.jsonPrimitive?.content ?: fromDate,
                    days = json["days"]?.jsonPrimitive?.content ?: HspClient.daysParam(fromDate)
                )
                val result = withContext(Dispatchers.IO) { HspClient.getMetrics(req) }
                if (result != null) {
                    call.respond(result)
                } else {
                    call.respond(HttpStatusCode.BadGateway, mapOf("error" to "HSP API request failed"))
                }
            }
            get("/hsp/metrics/stream") {
                if (!HspClient.isAvailable) {
                    call.respond(HttpStatusCode.ServiceUnavailable, mapOf("error" to "HSP not configured"))
                    return@get
                }
                val from = call.parameters["from"]?.uppercase()?.trim()
                val to = call.parameters["to"]?.uppercase()?.trim()
                val date = call.parameters["date"]?.trim()
                val fromTime = call.parameters["from_time"]?.trim() ?: "0000"
                val toTime = call.parameters["to_time"]?.trim() ?: "2359"
                if (from == null || to == null || date == null) {
                    call.badRequest("from, to, date required")
                    return@get
                }
                val req = HspMetricsRequest(from, to, fromTime, toTime, date, date, HspClient.daysParam(date))
                val result = withContext(Dispatchers.IO) { HspClient.getMetrics(req) }
                if (result != null) {
                    call.respond(result)
                } else {
                    call.respond(HttpStatusCode.BadGateway, mapOf("error" to "HSP API request failed"))
                }
            }
            post("/hsp/details") {
                if (!HspClient.isAvailable) {
                    call.respond(HttpStatusCode.ServiceUnavailable, mapOf("error" to "HSP not configured on server"))
                    return@post
                }
                val body = call.receiveText()
                val json = try {
                    Json.parseToJsonElement(body).jsonObject
                } catch (e: Exception) {
                    call.badRequest("Invalid JSON body")
                    return@post
                }
                val rid = json["rid"]?.jsonPrimitive?.content?.trim()
                if (rid.isNullOrEmpty()) {
                    call.badRequest("rid required")
                    return@post
                }
                val scheduledDep = json["scheduled_dep"]?.jsonPrimitive?.content?.trim() ?: ""
                val originCrsRaw = json["origin_crs"]?.jsonPrimitive?.content?.trim() ?: ""
                val originTiploc = json["origin_tiploc"]?.jsonPrimitive?.content?.trim() ?: ""
                val scheduledArr = json["scheduled_arr"]?.jsonPrimitive?.content?.trim() ?: ""
                val destTiploc = json["dest_tiploc"]?.jsonPrimitive?.content?.trim() ?: ""
                val originCrs = originCrsRaw.ifEmpty { CorpusLookup.crsFromTiploc(originTiploc) ?: "" }
                val result = withContext(Dispatchers.IO) { HspClient.getDetails(rid) }
                if (result != null) {
                    val unitInfo = AppDatabase.getHspUnit(rid, scheduledDep, originCrs, scheduledArr, destTiploc)
                    val enriched = buildJsonObject {
                        put("rid", result.rid)
                        put("date", result.date)
                        put("tocCode", result.tocCode)
                        put("unit", unitInfo?.units?.firstOrNull() ?: "")
                        put("units", Json.encodeToJsonElement(unitInfo?.units ?: emptyList()))
                        put("vehicles", Json.encodeToJsonElement(unitInfo?.vehicles ?: emptyList()))
                        put("unitCount", unitInfo?.unitCount ?: 0)
                        put("locations", buildJsonArray {
                            for (loc in result.locations) {
                                add(buildJsonObject {
                                    val crs = loc.tiploc.uppercase().trim()
                                    val name = ""
                                    put("tiploc", crs)
                                    put("crs", crs)
                                    put("name", name)
                                    put("scheduledDep", loc.scheduledDep)
                                    put("scheduledArr", loc.scheduledArr)
                                    put("actualDep", loc.actualDep)
                                    put("actualArr", loc.actualArr)
                                    put("cancelReason", loc.cancelReason)
                                })
                            }
                        })
                    }
                    call.respond(enriched)
                } else {
                    call.respond(HttpStatusCode.NotFound, mapOf("error" to "No HSP detail found for rid=$rid"))
                }
            }
            get("/allocation/{param}") {
                val param = call.parameters["param"]?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "coreId or headcode required")
                val date = call.request.queryParameters["date"]
                val results = AppDatabase.getAllocationByCoreId(param)
                    ?.let { listOf(it) }
                    ?: AppDatabase.getAllocationsByHeadcode(param.uppercase(), date)
                if (results.isEmpty()) {
                    call.respond(HttpStatusCode.NotFound, "No allocation data for $param")
                } else if (results.size == 1) {
                    call.respond(results.first().toResponse())
                } else {
                    call.respond(results.map { it.toResponse() })
                }
            }
            get("/allocation/uid/{uid}") {
                val uid = call.parameters["uid"]?.trim()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "uid required")
                val date = call.request.queryParameters["date"]
                val result = AppDatabase.getAllocationByUid(uid, date)
                if (result == null) {
                    call.respond(HttpStatusCode.NotFound, "No allocation data for uid $uid")
                } else {
                    call.respond(result.toResponse())
                }
            }
            get("/allocation/history/{uid}") {
                val uid = call.parameters["uid"]?.trim()?.filter { it.isLetterOrDigit() }
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "uid required")
                val date = call.request.queryParameters["date"]
                    ?.filter { it.isDigit() || it == '-' }
                    ?.takeIf { it.isNotEmpty() }
                    ?: LocalDate.now().toString()
                val changes = AppDatabase.getAllocationChanges(uid, date).map {
                    AllocationChangeResponse(it.changedAt, it.headcode, it.oldUnits, it.newUnits, it.oldCoaches, it.newCoaches)
                }
                call.respond(changes)
            }
            adminRoutes()
            get("/stations") {
                val q = call.parameters["q"]?.trim() ?: ""
                if (q.length < 2) {
                    call.respond(emptyList<Map<String, String>>())
                    return@get
                }
                val matches = CorpusLookup.searchStations(q, 20)
                call.respond(matches)
            }
            get("/board-date-range") {
                val range = withContext(Dispatchers.IO) {
                    transaction {
                        val minEventTs = TrustMovements.eventTs.min()
                        val minDate = TrustMovements.select(minEventTs).singleOrNull()
                            ?.get(minEventTs)
                            ?.toLocalDate()
                            ?.toString()
                        val maxScheduleEnd = Schedules.scheduleEndDate.max()
                        val maxDate = Schedules.select(maxScheduleEnd).where { Schedules.stpIndicator eq 'P' }.singleOrNull()
                            ?.get(maxScheduleEnd)
                        BoardDateRangeResponse(minDate ?: "2026-01-01", maxDate ?: "2026-12-31")
                    }
                }
                call.respond(range)
            }
            kbRoutes()
        }
    }
}

private fun resolveOriginDestOverrides(uids: Collection<String>, date: String): OriginDestOverrides {
    if (uids.isEmpty()) return OriginDestOverrides(emptyMap(), emptyMap(), emptyMap())
    val oLtOverrides = mutableMapOf<String, Pair<String, String?>>()
    val oLoOverrides = mutableMapOf<String, Pair<String, String?>>()
    val activationOrigins = mutableMapOf<String, String>()
    transaction {
        Schedules.selectAll().where {
            (Schedules.uid inList uids) and (Schedules.stpIndicator eq 'O') and (Schedules.stopType inList listOf("LT", "LO"))
        }.forEach { row ->
            val uid = row[Schedules.uid]
            val tiploc = row[Schedules.tiploc]
            val crs = row[Schedules.crs]?.takeIf { it.isNotEmpty() }
            when (row[Schedules.stopType]) {
                "LT" -> oLtOverrides[uid] = Pair(tiploc, crs)
                "LO" -> oLoOverrides[uid] = Pair(tiploc, crs)
            }
        }
        val latestStanoxByUid = mutableMapOf<String, String>()
        activationsFor(uids, date).forEach { (rUid, a) ->
            a.originStanox?.let { latestStanoxByUid[rUid] = it }
        }
        latestStanoxByUid.forEach { (rUid, rStanox) ->
            val rTiploc = CorpusLookup.tiplocFromStanox(rStanox) ?: run {
                val rCrs = CorpusLookup.crsFromStanox(rStanox) ?: return@forEach
                Schedules.selectAll().where { (Schedules.crs eq rCrs) and (Schedules.stopType eq "LO") }
                    .limit(1).firstOrNull()?.get(Schedules.tiploc)
            } ?: return@forEach
            activationOrigins[rUid] = rTiploc
        }
    }
    return OriginDestOverrides(oLtOverrides, oLoOverrides, activationOrigins)
}

private fun OriginDestOverrides.effectiveOrigin(row: ResultRow): Pair<String, String?> {
    val uid = row[Schedules.uid]
    val override = oLoOverrides[uid]
    val actTiploc = activationOrigins[uid]
    val tiploc = override?.first ?: actTiploc ?: row[Schedules.originTiploc]
    val crs = when {
        override != null -> override.second ?: CorpusLookup.crsFromTiploc(override.first)
        actTiploc != null -> CorpusLookup.crsFromTiploc(actTiploc)
        else -> row[Schedules.originCrs]?.takeIf { it.isNotEmpty() } ?: CorpusLookup.crsFromTiploc(row[Schedules.originTiploc])
    }
    return tiploc to crs
}

private fun OriginDestOverrides.effectiveDest(row: ResultRow): Pair<String, String?> {
    val override = oLtOverrides[row[Schedules.uid]]
    val tiploc = override?.first ?: row[Schedules.destTiploc]
    val crs = override?.second
        ?: row[Schedules.destCrs]?.takeIf { it.isNotEmpty() }
        ?: CorpusLookup.crsFromTiploc(row[Schedules.destTiploc])
    return tiploc to crs
}

private fun queryByToc(toc: String, date: String = LocalDate.now().toString()): List<ServiceResponse> {
    val tscs = CorpusLookup.tscsForOperator(toc)
    if (tscs.isEmpty()) return emptyList()
    return transaction {
        val uids = Schedules.selectAll().where {
            (Schedules.tsc inList tscs) and (Schedules.stopType eq "LO") and scheduleDateScope(effectiveDateFor(date))
        }.map { it[Schedules.uid] }.toSet()
        if (uids.isEmpty()) return@transaction emptyList()
        val allRows = Schedules.selectAll().where {
            (Schedules.uid inList uids) and ((Schedules.stopType eq "LO") or (Schedules.stpIndicator eq 'C')) and
                scheduleDateScope(effectiveDateFor(date))
        }.toList()
        val priority = STP_PRIORITY
        val bestRows = allRows.groupBy { it[Schedules.uid] }.values.mapNotNull { rows ->
            val bestStp = rows.minOfOrNull { priority[it[Schedules.stpIndicator]] ?: 99 } ?: return@mapNotNull null
            if (priority.entries.find { it.value == bestStp }?.key == 'C') return@mapNotNull null
            rows.filter { (priority[it[Schedules.stpIndicator]] ?: 99) == bestStp }
                .minByOrNull { if (it[Schedules.stopType] == "LO") 0 else 1 }
        }.filter { CorpusLookup.operatorForTsc(it[Schedules.tsc]).equals(toc, ignoreCase = true) }
        if (bestRows.isEmpty()) return@transaction emptyList()
        buildServicesFromBestRows(bestRows, date)
    }
}

private fun queryByHeadcode(headcode: String, date: String = LocalDate.now().toString()): List<ServiceResponse> = transaction {
    val allRows = Schedules.selectAll().where {
        (Schedules.headcode eq headcode) and scheduleDateScope(effectiveDateFor(date))
    }.orderBy(Schedules.stopSequence, SortOrder.ASC).toList()
    if (allRows.isEmpty()) return@transaction emptyList()
    val priority = STP_PRIORITY
    val byUid = allRows.groupBy { it[Schedules.uid] }
    val bestRows = byUid.values.mapNotNull { rows ->
        val bestStp = rows.minOfOrNull { priority[it[Schedules.stpIndicator]] ?: 99 } ?: return@mapNotNull null
        if (priority.entries.find { it.value == bestStp }?.key == 'C') return@mapNotNull null
        rows.filter { (priority[it[Schedules.stpIndicator]] ?: 99) == bestStp }
            .minByOrNull { if (it[Schedules.stopType] == "LO") 0 else 1 }
    }
    buildServicesFromBestRows(bestRows, date)
}

private fun buildServicesFromBestRows(bestRows: List<ResultRow>, date: String): List<ServiceResponse> = transaction {
    val trustMap = mutableMapOf<String, Triple<String, Boolean, String?>>()
    val distinctUids = bestRows.map { it[Schedules.uid] }.toSet().toList()
    if (distinctUids.isNotEmpty()) {
        try {
            val placeholders = distinctUids.joinToString(",") { "?" }
            val colType = bindStrColType
            val dayOfMonth = date.takeLast(2)
            val args = distinctUids.map { colType to (it as Any?) } +
                listOf(colType to (date as Any?), colType to (date as Any?), colType to (dayOfMonth as Any?))
            exec(
                "SELECT uid, scheduled_time, actual_time, is_cancelled, cancel_reason FROM trust_movements WHERE uid IN ($placeholders) AND event_ts >= datetime(?, '-12 hours') AND event_ts < datetime(?, '+36 hours') AND train_id LIKE '%' || ? ORDER BY event_ts DESC",
                args
            ) { rs ->
                while (rs.next()) {
                    val uid = rs.getString("uid")?.takeIf { it.isNotEmpty() } ?: continue
                    val scht = rs.getString("scheduled_time") ?: continue
                    val key = "$uid|$scht"
                    if (!trustMap.containsKey(key)) {
                        trustMap[key] = Triple(rs.getString("actual_time") ?: "", rs.getBoolean("is_cancelled"), ReasonCodes.resolve(rs.getString("cancel_reason")))
                    }
                    val isCancelled = rs.getBoolean("is_cancelled")
                    if (isCancelled && !trustMap.containsKey(uid)) {
                        trustMap[uid] = Triple(rs.getString("actual_time") ?: "", true, ReasonCodes.resolve(rs.getString("cancel_reason")))
                    }
                }
            }
        } catch (e: Exception) {
        }
    }
    val uids = bestRows.map { it[Schedules.uid] }.toSet()
    val overrides = resolveOriginDestOverrides(uids, date)
    val ctx = buildAssociationContext(uids)
    val darwinPlatformMap = AppDatabase.getDarwinPlatformsForUids(uids)
    val trustPlatformMap = AppDatabase.getTrustPlatforms(uids)
    bestRows.sortedBy { it[Schedules.scheduledTime] }.map { row ->
        val scht = row[Schedules.scheduledTime]
        val uid = row[Schedules.uid]
        val trustEntry = trustMap["$uid|$scht"] ?: if (trustMap[uid]?.second == true) trustMap[uid] else null
        val hasAnyActualTime = trustMap.entries.any { (k, v) -> k.startsWith("$uid|") && v.first.isNotEmpty() }
        val (plt_, pltC_) = resolvePlatformForBoard(
            row[Schedules.uid], row[Schedules.tiploc], row[Schedules.platform], darwinPlatformMap, trustPlatformMap, row[Schedules.crs]
        )
        val (effOriginTiploc, effOriginCrs) = overrides.effectiveOrigin(row)
        val (effDestTiploc, effDestCrs) = overrides.effectiveDest(row)
        val svc = ServiceResponse(
            uid = row[Schedules.uid],
            headcode = row[Schedules.headcode],
            atocCode = CorpusLookup.operatorForTsc(row[Schedules.tsc]),
            scheduledTime = scht,
            scheduledArrival = row[Schedules.scheduledArrival],
            scheduledDeparture = row[Schedules.scheduledDeparture],
            platform = plt_,
            platformConfirmed = pltC_,
            isPass = row[Schedules.isPass],
            stopType = row[Schedules.stopType],
            originTiploc = effOriginTiploc,
            destTiploc = effDestTiploc,
            originCrs = effOriginCrs,
            destCrs = effDestCrs,
            actualTime = trustEntry?.first ?: "",
            isCancelled = ScheduleCancellations.effectiveCancelled(uid, date, trustEntry?.second ?: false, hasAnyActualTime),
            cancelReason = trustEntry?.third
                ?: if (trustEntry?.second != true && ScheduleCancellations.isCancelled(uid, date)) "Cancelled in the timetable" else null
        )
        applyAssociations(svc, ctx)
    }.withResolvedNames()
}

private fun queryByUnit(unit: String, date: String = LocalDate.now().toString()): List<ServiceResponse> = transaction {
    data class PrimaryUnitRow(
        val hc: String,
        val loRow: ResultRow,
        val unitList: List<String>,
        val vehicleList: List<String>,
        val joinTiploc: String
    )

    data class PartnerCandidate(
        val svcUid: String,
        val myTiplocs2: Set<String>,
        val svcOriginTiploc: String,
        val svcDestTiploc: String,
        val hc2: String,
        val coreId2: String,
        val units2: List<String>,
        val vehicles2: List<String>,
        val candidateUid2: String?
    )

    val today = date
    val primaryData: MutableList<PrimaryUnitRow> = ArrayList()
    try {
        val strCol = bindStrColType
        exec(
            "SELECT ac.headcode, ac.units, ac.vehicles, ac.operator, ac.core_id, ac.uid FROM allocation_consists ac WHERE (',' || ac.units || ',') LIKE '%,' || ? || ',%' AND ac.service_date = ? AND SUBSTR(ac.units, 1, 3) = SUBSTR(?, 1, 3)",
            listOf(strCol to (unit as Any?), strCol to (today as Any?), strCol to (unit as Any?))
        ) { rs ->
            while (rs.next()) {
                val hc = rs.getString("headcode") ?: continue
                val coreId = rs.getString("core_id") ?: continue
                val safeHc = hc.filter { it.isLetterOrDigit() }
                val uid = rs.getString("uid")?.trim()?.takeIf { it.isNotEmpty() }
                    ?: if (coreId.length > hc.length + 2) coreId.substring(hc.length, coreId.length - 2).trim().takeIf { it.isNotEmpty() } else null
                val sched = if (uid != null) {
                    Schedules.selectAll().where { (Schedules.uid eq uid) and scheduleDateScope(effectiveDateFor(date)) }
                        .orderBy(Schedules.stopSequence, SortOrder.ASC).toList()
                        .filter { it[Schedules.stpIndicator] != 'C' }
                } else {
                    Schedules.selectAll().where { (Schedules.headcode eq safeHc) and scheduleDateScope(effectiveDateFor(date)) }
                        .orderBy(Schedules.stopSequence, SortOrder.ASC).toList()
                        .filter { it[Schedules.stpIndicator] != 'C' }
                }
                val priority = STP_PRIORITY
                val bestStp = sched.minOfOrNull { priority[it[Schedules.stpIndicator]] ?: 99 }
                val best = sched.filter { (priority[it[Schedules.stpIndicator]] ?: 99) == bestStp }
                val loRow = best.firstOrNull { it[Schedules.stopType] == "LO" } ?: best.firstOrNull() ?: continue
                val unitList = (rs.getString("units") ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() }
                val vehicleList = (rs.getString("vehicles") ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() }
                val joinTiploc = if (loRow[Schedules.stopType] == "LI") loRow[Schedules.tiploc] else loRow[Schedules.originTiploc]
                primaryData.add(PrimaryUnitRow(hc, loRow, unitList, vehicleList, joinTiploc))
            }
        }
    } catch (_: Exception) {
    }
    val seenUids = primaryData.map { it.loRow[Schedules.uid] }.toMutableSet()
    val primaryTiplocs = mutableMapOf<String, MutableSet<String>>()
    if (primaryData.isNotEmpty()) {
        try {
            val (inClause, inArgs) = sqlPlaceholders(primaryData.map { it.loRow[Schedules.uid] }.toSet())
            exec("SELECT uid, tiploc FROM schedules WHERE uid IN ($inClause) AND stp_indicator != 'C'", inArgs) { rs ->
                while (rs.next()) {
                    val u = rs.getString("uid") ?: continue
                    val t = rs.getString("tiploc") ?: continue
                    primaryTiplocs.getOrPut(u) { mutableSetOf() }.add(t)
                }
            }
        } catch (_: Exception) {
        }
    }
    val candidates: MutableList<PartnerCandidate> = ArrayList()
    for (row in primaryData.filter { it.unitList.size > 1 && it.unitList.contains(unit) }) {
        val svcUid = row.loRow[Schedules.uid]
        val myTiplocs2 = primaryTiplocs[svcUid] ?: continue
        for (u in row.unitList.filter { it != unit }) {
            try {
                val seenUidPrefixes = seenUids.map { it.take(6) }.toSet()
                val (notInClause, notInArgs) = if (seenUidPrefixes.isEmpty()) "''" to emptyList() else sqlPlaceholders(seenUidPrefixes)
                val queryArgs = listOf(bindStr(u), bindStr(today)) + notInArgs
                exec("SELECT ac.headcode, ac.units, ac.vehicles, ac.operator, ac.core_id FROM allocation_consists ac WHERE (',' || ac.units || ',') LIKE '%,' || ? || ',%' AND ac.service_date = ? AND ac.core_id NOT IN ($notInClause)", queryArgs) { rs ->
                    while (rs.next()) {
                        val hc2 = rs.getString("headcode") ?: continue
                        val coreId2 = rs.getString("core_id") ?: continue
                        val units2 = (rs.getString("units") ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() }
                        val vehicles2 = (rs.getString("vehicles") ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() }
                        val hcLen = hc2.length
                        val uidLen = coreId2.length - hcLen - 2
                        val candidateUid2 = if (uidLen in 4..8) coreId2.substring(hcLen, hcLen + uidLen) else null
                        candidates.add(
                            PartnerCandidate(
                                svcUid, myTiplocs2, row.loRow[Schedules.originTiploc], row.loRow[Schedules.destTiploc],
                                hc2, coreId2, units2, vehicles2, candidateUid2
                            )
                        )
                    }
                }
            } catch (_: Exception) {
            }
        }
    }
    val candidateHeadcodes = candidates.mapNotNull { it.candidateUid2 }.let { _ -> candidates.map { it.hc2 }.toSet() }
    val headcodeToUids = mutableMapOf<String, MutableSet<String>>()
    if (candidateHeadcodes.isNotEmpty()) {
        try {
            val (inClause, inArgs) = sqlPlaceholders(candidateHeadcodes)
            exec("SELECT DISTINCT uid, headcode FROM schedules WHERE headcode IN ($inClause)", inArgs) { rs ->
                while (rs.next()) {
                    val u = rs.getString("uid") ?: continue
                    val h = rs.getString("headcode") ?: continue
                    headcodeToUids.getOrPut(h) { mutableSetOf() }.add(u)
                }
            }
        } catch (_: Exception) {
        }
    }
    val validatedCandidates = candidates.filter { c ->
        c.candidateUid2 != null && headcodeToUids[c.hc2]?.contains(c.candidateUid2) == true
    }
    val uid2Set = validatedCandidates.mapNotNull { it.candidateUid2 }.toSet()
    val originTiplocByUid2 = mutableMapOf<String, String>()
    if (uid2Set.isNotEmpty()) {
        try {
            val (inClause, inArgs) = sqlPlaceholders(uid2Set)
            exec("SELECT uid, origin_tiploc FROM schedules WHERE uid IN ($inClause) AND stop_type = 'LO'", inArgs) { rs ->
                while (rs.next()) {
                    val u = rs.getString("uid") ?: continue
                    val t = rs.getString("origin_tiploc") ?: continue
                    originTiplocByUid2.putIfAbsent(u, t)
                }
            }
        } catch (_: Exception) {
        }
    }
    val schedByUid2 = if (uid2Set.isNotEmpty()) {
        Schedules.selectAll().where { Schedules.uid inList uid2Set }
            .orderBy(Schedules.stopSequence, SortOrder.ASC).toList()
            .filter { it[Schedules.stpIndicator] != 'C' }
            .groupBy { it[Schedules.uid] }
    } else {
        emptyMap()
    }
    val darwinBoardUids = (primaryData.map { it.loRow[Schedules.uid] } + uid2Set).toSet()
    val darwinPlatformMap = AppDatabase.getDarwinPlatformsForUids(darwinBoardUids)
    val trustPlatformMap = AppDatabase.getTrustPlatforms(darwinBoardUids)
    val rows: MutableList<ServiceResponse> = ArrayList()
    primaryData.forEach { row ->
        val (plt_, pltC_) = resolvePlatformForBoard(
            row.loRow[Schedules.uid], row.loRow[Schedules.tiploc], row.loRow[Schedules.platform],
            darwinPlatformMap, trustPlatformMap, row.loRow[Schedules.crs]
        )
        rows.add(
            ServiceResponse(
                uid = row.loRow[Schedules.uid],
                headcode = row.hc,
                atocCode = CorpusLookup.operatorForTsc(row.loRow[Schedules.tsc]),
                scheduledTime = row.loRow[Schedules.scheduledTime],
                scheduledArrival = row.loRow[Schedules.scheduledArrival],
                scheduledDeparture = row.loRow[Schedules.scheduledDeparture],
                platform = plt_,
                platformConfirmed = pltC_,
                isPass = row.loRow[Schedules.isPass],
                stopType = row.loRow[Schedules.stopType],
                originTiploc = row.loRow[Schedules.originTiploc],
                destTiploc = row.loRow[Schedules.destTiploc],
                originCrs = row.loRow[Schedules.originCrs]?.takeIf { it.isNotEmpty() }
                    ?: CorpusLookup.crsFromTiploc(row.loRow[Schedules.originTiploc]),
                destCrs = row.loRow[Schedules.destCrs]?.takeIf { it.isNotEmpty() }
                    ?: CorpusLookup.crsFromTiploc(row.loRow[Schedules.destTiploc]),
                actualTime = "",
                isCancelled = false,
                cancelReason = null,
                units = row.unitList,
                vehicles = row.vehicleList,
                unitJoinTiploc = row.joinTiploc,
                destName = null,
                originName = null
            )
        )
    }
    val extraRows: MutableList<ServiceResponse> = ArrayList()
    for (c in validatedCandidates) {
        val uid2 = c.candidateUid2 ?: continue
        if (seenUids.contains(uid2)) continue
        val originTiploc2 = originTiplocByUid2[uid2] ?: ""
        if (originTiploc2.isEmpty() || !c.myTiplocs2.contains(originTiploc2) || originTiploc2 == c.svcOriginTiploc) continue
        if (c.svcDestTiploc == originTiploc2) continue
        val sched2 = schedByUid2[uid2] ?: continue
        val priority2 = STP_PRIORITY
        val bestStp2 = sched2.minOfOrNull { priority2[it[Schedules.stpIndicator]] ?: 99 }
        val best2 = sched2.filter { (priority2[it[Schedules.stpIndicator]] ?: 99) == bestStp2 }
        val loRow2 = best2.firstOrNull { it[Schedules.stopType] == "LO" } ?: best2.firstOrNull() ?: continue
        val joinTiploc2 = if (loRow2[Schedules.stopType] == "LI") loRow2[Schedules.tiploc] else loRow2[Schedules.originTiploc]
        seenUids.add(uid2)
        val (plt_, pltC_) = resolvePlatformForBoard(
            loRow2[Schedules.uid], loRow2[Schedules.tiploc], loRow2[Schedules.platform],
            darwinPlatformMap, trustPlatformMap, loRow2[Schedules.crs]
        )
        extraRows.add(
            ServiceResponse(
                uid = loRow2[Schedules.uid],
                headcode = c.hc2,
                atocCode = CorpusLookup.operatorForTsc(loRow2[Schedules.tsc]),
                scheduledTime = loRow2[Schedules.scheduledTime],
                scheduledArrival = loRow2[Schedules.scheduledArrival],
                scheduledDeparture = loRow2[Schedules.scheduledDeparture],
                platform = plt_,
                platformConfirmed = pltC_,
                isPass = loRow2[Schedules.isPass],
                stopType = loRow2[Schedules.stopType],
                originTiploc = loRow2[Schedules.originTiploc],
                destTiploc = loRow2[Schedules.destTiploc],
                originCrs = loRow2[Schedules.originCrs]?.takeIf { it.isNotEmpty() }
                    ?: CorpusLookup.crsFromTiploc(loRow2[Schedules.originTiploc]),
                destCrs = loRow2[Schedules.destCrs]?.takeIf { it.isNotEmpty() }
                    ?: CorpusLookup.crsFromTiploc(loRow2[Schedules.destTiploc]),
                actualTime = "",
                isCancelled = false,
                cancelReason = null,
                units = c.units2,
                vehicles = c.vehicles2,
                unitJoinTiploc = joinTiploc2,
                destName = null,
                originName = null
            )
        )
    }
    val primaryUnitsByTime = rows.flatMap { svc -> svc.units.map { u -> u to svc.scheduledTime } }
        .groupBy({ it.first }, { it.second })
    val filteredExtra = extraRows.filter { extra ->
        extra.units.all { u ->
            val primaryTimes = primaryUnitsByTime[u] ?: return@all true
            primaryTimes.none { t -> t > extra.scheduledTime }
        }
    }
    val allRows = rows + filteredExtra
    val sorted = allRows.sortedBy { it.scheduledTime }
    val namedSorted = sorted.withResolvedNames()
    val tiplocMap = mutableMapOf<String, MutableSet<String>>()
    if (namedSorted.isNotEmpty()) {
        try {
            val (inClause, inArgs) = sqlPlaceholders(namedSorted.map { it.uid }.toSet())
            exec("SELECT uid, tiploc FROM schedules WHERE uid IN ($inClause) AND stp_indicator != 'C' ORDER BY stop_sequence", inArgs) { rs ->
                while (rs.next()) {
                    val u = rs.getString("uid") ?: continue
                    val t = rs.getString("tiploc") ?: continue
                    tiplocMap.getOrPut(u) { mutableSetOf() }.add(t)
                }
            }
        } catch (_: Exception) {
        }
    }
    namedSorted.map { svc ->
        val myTiplocs = tiplocMap[svc.uid] ?: emptySet()
        val splitPortion = namedSorted.filter { other ->
            other.uid != svc.uid &&
                other.originTiploc != svc.originTiploc &&
                other.originTiploc != svc.destTiploc &&
                myTiplocs.contains(other.originTiploc) &&
                other.units.isNotEmpty() &&
                svc.units.size > 1 &&
                other.units.all { svc.units.contains(it) } &&
                other.units.size < svc.units.size &&
                svc.units.contains(unit) &&
                other.scheduledTime >= svc.scheduledTime &&
                other.scheduledTime.toMinutes() - svc.scheduledTime.toMinutes() <= 90 &&
                (other.units.contains(unit) || svc.units.filter { it != unit }.any { other.units.contains(it) })
        }.minByOrNull { it.scheduledTime }
        var result = svc
        if (splitPortion != null) {
            result = result.copy(
                association = result.association.copy(
                    splitTiploc = splitPortion.originTiploc,
                    splitTiplocName = splitPortion.originName,
                    splitToUid = splitPortion.uid,
                    splitToHeadcode = splitPortion.headcode
                )
            )
        }
        val formsSearchedUnit = if (svc.units.contains(unit)) {
            namedSorted.filter { other ->
                other.uid != svc.uid && other.scheduledTime > svc.scheduledTime && other.units.contains(unit)
            }.minByOrNull { it.scheduledTime }
        } else {
            null
        }
        if (formsSearchedUnit != null) {
            result = result.copy(
                association = result.association.copy(
                    formsUid = formsSearchedUnit.uid,
                    formsHeadcode = formsSearchedUnit.headcode
                )
            )
        }
        result
    }.filter { it.units.isEmpty() || it.units.contains(unit) }
}

private fun annotateSplits(services: List<ServiceResponse>): List<ServiceResponse> {
    if (services.isEmpty()) return services
    return transaction {
        val ctx = buildAssociationContext(services.map { it.uid }.toSet())
        services.map { applyAssociations(it, ctx) }
    }
}

private fun timeWindow(windowMinutes: Int, offsetMinutes: Int = 0): Pair<String, String> {
    val fmt = DateTimeFormatter.ofPattern("HH:mm")
    val start = LocalTime.now().plusMinutes(offsetMinutes.toLong())
    val end = start.plusMinutes(windowMinutes.toLong())
    return start.format(fmt) to end.format(fmt)
}

private fun enrichWithUnits(services: List<ServiceResponse>, date: String = LocalDate.now().toString()): List<ServiceResponse> {
    val uidHeadcodes = services.filter { it.units.isEmpty() }.associate { it.uid to it.headcode }
    if (uidHeadcodes.isEmpty()) return services
    val allocMap = AppDatabase.getAllocationsByUids(uidHeadcodes, date)
    return services.map { svc ->
        if (svc.units.isNotEmpty()) {
            svc
        } else {
            val alloc = allocMap[svc.uid]
            if (alloc != null) svc.copy(units = alloc.units, vehicles = alloc.vehicles) else svc
        }
    }
}

private fun queryBoard(
    crs: String,
    windowStart: String,
    windowEnd: String,
    mode: BoardMode,
    passengerOnly: Boolean = false,
    excludePassing: Boolean = false,
    date: String = LocalDate.now().toString(),
    rolled: RolledFilter = RolledFilter.ANY
): List<ServiceResponse> {
    val crsGroup = listOf(crs)
    val effectiveDate = effectiveDateFor(date)
    val isToday = date == LocalDate.now().toString()
    return transaction {
        val needed = listOf(
            Schedules.uid,
            Schedules.headcode,
            Schedules.scheduledTime,
            Schedules.platform,
            Schedules.isPass,
            Schedules.stopType,
            Schedules.stpIndicator,
            Schedules.crs,
            Schedules.tiploc,
            Schedules.originTiploc,
            Schedules.destTiploc,
            Schedules.originCrs,
            Schedules.destCrs,
            Schedules.scheduleStartDate,
            Schedules.tsc,
            Schedules.scheduledArrival,
            Schedules.scheduledDeparture
        )
        Schedules.select(needed).where {
            val timeCondition = if (windowEnd < windowStart) {
                (Schedules.scheduledTime greaterEq windowStart) or (Schedules.scheduledTime lessEq windowEnd)
            } else {
                (Schedules.scheduledTime greaterEq windowStart) and (Schedules.scheduledTime lessEq windowEnd)
            }
            scheduleDateScope(effectiveDate) and (Schedules.crs inList crsGroup) and timeCondition and
                when (mode) {
                    BoardMode.ARRIVALS -> Schedules.stopType neq "LO"
                    BoardMode.DEPARTURES -> Schedules.stopType neq "LT"
                    BoardMode.ALL -> Op.TRUE
                }
        }.orderBy(Schedules.scheduledTime, SortOrder.ASC).toList().let { rows ->
            if (windowEnd >= windowStart) {
                rows
            } else {
                val wrappedUids = rows.filter { it[Schedules.scheduledTime] < windowStart }.map { it[Schedules.uid] }.toSet()
                if (wrappedUids.isEmpty()) {
                    rows
                } else {
                    val loTimes = mutableMapOf<String, String>()
                    val (inClause, inArgs) = sqlPlaceholders(wrappedUids)
                    exec("SELECT uid, MAX(scheduled_time) AS lo FROM schedules WHERE uid IN ($inClause) AND stop_type='LO' AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '') GROUP BY uid", inArgs + scheduleDateScopeArgs(effectiveDate)) { rs ->
                        while (rs.next()) {
                            val u = rs.getString("uid") ?: continue
                            loTimes[u] = rs.getString("lo") ?: ""
                        }
                    }
                    rows.filter { r ->
                        val t = r[Schedules.scheduledTime]
                        !(t < windowStart && (loTimes[r[Schedules.uid]] ?: "") <= t)
                    }
                }
            }
        }.let { rows ->
            if (rolled == RolledFilter.ANY || rows.isEmpty()) {
                rows
            } else {
                val rolledUids = rows.map { it[Schedules.uid] }.toSet()
                val originTimes = mutableMapOf<String, String>()
                val (rIn, rArgs) = sqlPlaceholders(rolledUids)
                exec("SELECT uid, MAX(scheduled_time) AS lo FROM schedules WHERE uid IN ($rIn) AND stop_type='LO' AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '') GROUP BY uid", rArgs + scheduleDateScopeArgs(effectiveDate)) { rs ->
                    while (rs.next()) {
                        val u = rs.getString("uid") ?: continue
                        originTimes[u] = rs.getString("lo") ?: ""
                    }
                }
                rows.filter { r ->
                    val lo = originTimes[r[Schedules.uid]] ?: ""
                    val isRolled = lo.isNotEmpty() && r[Schedules.scheduledTime] < lo
                    if (rolled == RolledFilter.ONLY) isRolled else !isRolled
                }
            }
        }.filter { row ->
            (!excludePassing || !row[Schedules.isPass]) &&
                (!passengerOnly || row[Schedules.headcode].firstOrNull()?.let { "1239".contains(it) } == true)
        }.groupBy { it[Schedules.uid] }.values.map { rows ->
            val pVariants = rows.filter { it[Schedules.stpIndicator] == 'P' }.groupBy { it[Schedules.scheduleStartDate] }
            if (pVariants.size <= 1) {
                rows
            } else {
                val oRows = rows.filter { it[Schedules.stpIndicator] == 'O' }
                if (oRows.isEmpty()) {
                    rows
                } else {
                    val oFirstTime = oRows.minOf { it[Schedules.scheduledTime] }
                    val bestStartDate = pVariants.entries.mapNotNull { (startDate, pRows) ->
                        val loTime = pRows.firstOrNull { it[Schedules.stopType] == "LO" }
                            ?.get(Schedules.scheduledTime)
                            ?: return@mapNotNull null
                        startDate to loTime
                    }.filter { (_, loTime) -> loTime <= oFirstTime }
                        .maxByOrNull { (_, loTime) -> loTime }?.first ?: pVariants.keys.minOrNull()
                    val nonPRows = rows.filter { it[Schedules.stpIndicator] != 'P' }
                    val bestPRows = pVariants[bestStartDate] ?: rows.filter { it[Schedules.stpIndicator] == 'P' }
                    bestPRows + nonPRows
                }
            }
        }.let { groups ->
            val uidsWithOverride = mutableSetOf<String>()
            val pUids = groups.mapNotNull { rows ->
                val priority = STP_PRIORITY
                val best = rows.minByOrNull { priority[it[Schedules.stpIndicator]] ?: 99 } ?: return@mapNotNull null
                if (best[Schedules.stpIndicator] == 'P') best[Schedules.uid] else null
            }
            val uidsCancelled = mutableSetOf<String>()
            if (pUids.isNotEmpty()) {
                val (inClause, inArgs) = sqlPlaceholders(pUids)
                exec("SELECT DISTINCT uid, stp_indicator FROM schedules WHERE uid IN ($inClause) AND stp_indicator IN ('O','N','C') AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '')", inArgs + scheduleDateScopeArgs(effectiveDate)) { rs ->
                    while (rs.next()) {
                        val uid = rs.getString("uid") ?: continue
                        when (rs.getString("stp_indicator")) {
                            "O", "N" -> uidsWithOverride.add(uid)
                            "C" -> uidsCancelled.add(uid)
                        }
                    }
                }
            }
            val winnerRows: MutableList<ResultRow> = ArrayList()
            val pFallbackUids = mutableSetOf<String>()
            groups.forEach { rows ->
                val priority = STP_PRIORITY
                val best = rows.minByOrNull { priority[it[Schedules.stpIndicator]] ?: 99 } ?: return@forEach
                when {
                    best[Schedules.stpIndicator] == 'C' -> {}
                    best[Schedules.stpIndicator] == 'P' && uidsCancelled.contains(best[Schedules.uid]) -> {}
                    best[Schedules.stpIndicator] == 'P' && uidsWithOverride.contains(best[Schedules.uid]) ->
                        pFallbackUids.add(best[Schedules.uid])
                    else -> winnerRows.add(best)
                }
            }
            val validUids = mutableSetOf<String>()
            if (winnerRows.isNotEmpty()) {
                val (inClause, inArgs) = sqlPlaceholders(winnerRows.map { it[Schedules.uid] }.toSet())
                val safeCrsVal = crs.filter { it.isLetterOrDigit() }
                val crsArg = bindStr(safeCrsVal)
                exec("SELECT DISTINCT uid, stp_indicator FROM schedules WHERE uid IN ($inClause) AND crs=? AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '')", inArgs + crsArg + scheduleDateScopeArgs(effectiveDate)) { rs ->
                    while (rs.next()) {
                        val uid = rs.getString("uid") ?: continue
                        val stp = rs.getString("stp_indicator")?.firstOrNull() ?: continue
                        val winnerStp = winnerRows.find { it[Schedules.uid] == uid }?.get(Schedules.stpIndicator)
                        if (stp == winnerStp) validUids.add(uid)
                    }
                }
            }
            if (validUids.isNotEmpty()) {
                val (inClause, inArgs) = sqlPlaceholders(validUids)
                val safeCrsVal = crs.filter { it.isLetterOrDigit() }
                val crsArg = bindStr(safeCrsVal)
                val pTimeAtCrs = mutableMapOf<String, String>()
                val oLtTime = mutableMapOf<String, String>()
                val oLoTime = mutableMapOf<String, String>()
                exec("SELECT uid, stp_indicator, stop_type, scheduled_time FROM schedules WHERE uid IN ($inClause) AND ((stp_indicator='P' AND crs=?) OR (stp_indicator='O' AND stop_type IN ('LT','LO'))) AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '')", inArgs + crsArg + scheduleDateScopeArgs(effectiveDate)) { rs ->
                    while (rs.next()) {
                        val uid = rs.getString("uid") ?: continue
                        val t = rs.getString("scheduled_time") ?: continue
                        val stp = rs.getString("stp_indicator") ?: continue
                        val stop = rs.getString("stop_type") ?: continue
                        if (stp == "P") {
                            pTimeAtCrs[uid] = t
                        } else {
                            when (stop) {
                                "LT" -> oLtTime[uid] = t
                                "LO" -> oLoTime[uid] = t
                            }
                        }
                    }
                }
                val toRemove = mutableSetOf<String>()
                for (uid in validUids) {
                    val pTime = pTimeAtCrs[uid] ?: continue
                    val pTimeMins = pTime.toMinutes()
                    val oLt = oLtTime[uid]
                    if (oLt != null && wrapAwareMinutes(oLt, pTimeMins) <= pTimeMins) {
                        toRemove.add(uid)
                        continue
                    }
                    val oLo = oLoTime[uid]
                    if (oLo != null && wrapAwareMinutes(oLo, pTimeMins) > pTimeMins) toRemove.add(uid)
                }
                validUids -= toRemove
            }
            val pFallbackRows = if (pFallbackUids.isNotEmpty()) {
                val safeCrsVal = crs.filter { it.isLetterOrDigit() }
                val overrideCallsHere = mutableSetOf<String>()
                val overrideMovedOrigin = mutableSetOf<String>()
                val (inClause2, inArgs2) = sqlPlaceholders(pFallbackUids)
                val crsArg2 = bindStr(safeCrsVal)
                exec("SELECT DISTINCT uid, stop_type, crs FROM schedules WHERE uid IN ($inClause2) AND stp_indicator IN ('O','N') AND (crs=? OR stop_type='LO') AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '')", inArgs2 + crsArg2 + scheduleDateScopeArgs(effectiveDate)) { rs ->
                    while (rs.next()) {
                        val uid = rs.getString("uid") ?: continue
                        val stop = rs.getString("stop_type") ?: continue
                        val c = rs.getString("crs") ?: ""
                        if (c == safeCrsVal) overrideCallsHere.add(uid)
                        if (stop == "LO" && c != safeCrsVal) overrideMovedOrigin.add(uid)
                    }
                }
                val overrideNoLongerCallsHere = mutableSetOf<String>()
                val candidatesForTimeCheck = pFallbackUids - overrideCallsHere - overrideMovedOrigin
                if (candidatesForTimeCheck.isNotEmpty()) {
                    val (inClause3, inArgs3) = sqlPlaceholders(candidatesForTimeCheck)
                    val crsArg3 = bindStr(safeCrsVal)
                    val pTimeFb = mutableMapOf<String, String>()
                    val oLtFb = mutableMapOf<String, String>()
                    val oFirstFb = mutableMapOf<String, String>()
                    exec("SELECT uid, stp_indicator, stop_type, scheduled_time FROM schedules WHERE uid IN ($inClause3) AND ((stp_indicator='P' AND crs=?) OR stp_indicator='O') AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '')", inArgs3 + crsArg3 + scheduleDateScopeArgs(effectiveDate)) { rs ->
                        while (rs.next()) {
                            val uid = rs.getString("uid") ?: continue
                            val t = rs.getString("scheduled_time") ?: continue
                            val stp = rs.getString("stp_indicator") ?: continue
                            val stop = rs.getString("stop_type") ?: continue
                            if (stp == "P") {
                                pTimeFb[uid] = t
                            } else {
                                if (stop == "LT") oLtFb[uid] = t
                                if (oFirstFb[uid] == null || t < oFirstFb[uid]!!) oFirstFb[uid] = t
                            }
                        }
                    }
                    for (uid in candidatesForTimeCheck) {
                        val pTime = pTimeFb[uid] ?: continue
                        val pTimeMins = pTime.toMinutes()
                        val oLt = oLtFb[uid]
                        if (oLt != null && wrapAwareMinutes(oLt, pTimeMins) <= pTimeMins) {
                            overrideNoLongerCallsHere.add(uid)
                            continue
                        }
                        val oFirst = oFirstFb[uid]
                        if (oFirst != null && wrapAwareMinutes(oFirst, pTimeMins) > pTimeMins) overrideNoLongerCallsHere.add(uid)
                    }
                }
                val genuineFallbacks = pFallbackUids - overrideCallsHere - overrideMovedOrigin - overrideNoLongerCallsHere
                val pRows: MutableList<ResultRow> = ArrayList()
                for (uid in genuineFallbacks) {
                    Schedules.select(needed).where {
                        (Schedules.uid eq uid) and (Schedules.stpIndicator eq 'P') and (Schedules.crs eq safeCrsVal)
                    }.firstOrNull()?.let { pRows.add(it) }
                }
                pRows
            } else {
                emptyList()
            }
            val combinedRows = (winnerRows.filter { validUids.contains(it[Schedules.uid]) } + pFallbackRows)
                .sortedBy { it[Schedules.scheduledTime] }
            val safeCrsForN = crs.filter { it.isLetterOrDigit() }
            val nLoTimes = mutableSetOf<String>()
            val nLoArgs = listOf(bindStr(safeCrsForN), bindStr(windowStart), bindStr(windowEnd))
            exec("SELECT scheduled_time FROM schedules WHERE stp_indicator='N' AND stop_type='LO' AND crs=? AND scheduled_time >= ? AND scheduled_time <= ? AND ((schedule_start_date <= ? AND schedule_end_date >= ? AND days_run LIKE ?) OR schedule_date = '')", nLoArgs + scheduleDateScopeArgs(effectiveDate)) { rs ->
                while (rs.next()) {
                    val t = rs.getString("scheduled_time") ?: return@exec
                    val tMins = t.replace(":", "").toIntOrNull() ?: return@exec
                    val hh = tMins / 100
                    val mm = tMins % 100
                    val base = hh * 60 + mm
                    for (delta in -1..1) {
                        val adj = (base + delta + 1440) % 1440
                        nLoTimes.add("$safeCrsForN|%02d:%02d".format(adj / 60, adj % 60))
                    }
                }
            }
            combinedRows.filter { row ->
                if (row[Schedules.stpIndicator] != 'P' || row[Schedules.stopType] != "LI") return@filter true
                val timeKey = "$safeCrsForN|${row[Schedules.scheduledTime]}"
                !nLoTimes.contains(timeKey)
            }.sortedBy { it[Schedules.scheduledTime] }
        }.let { rows ->
            if (windowEnd < windowStart) {
                val windowStartMins = windowStart.replace(":", "").toIntOrNull() ?: 0
                rows.sortedBy {
                    val t = it[Schedules.scheduledTime].replace(":", "").toIntOrNull() ?: 0
                    if (t < windowStartMins) t + 2400 else t
                }
            } else {
                rows.sortedBy { it[Schedules.scheduledTime] }
            }
        }.let { sortedRows ->
            val safeCrsQ = crs.filter { it.isLetterOrDigit() }
            val trustMap = mutableMapOf<String, Triple<String, Boolean, String?>>()
            val trustRawReason = mutableMapOf<String, String?>()
            val uidLastDelay = mutableMapOf<String, Int>()
            val (uidInClause, uidInArgs) = sqlPlaceholders(sortedRows.map { it[Schedules.uid] }.toSet())
            val boardDayOfMonth = date.takeLast(2)
            fun bindBoard(v: String) = bindStr(v)
            val trustDateArgs = listOf(bindBoard(date), bindBoard(date), bindBoard(boardDayOfMonth))
            if (uidInClause.isNotEmpty()) {
                try {
                    val cancelOnlyTrainIds = mutableMapOf<String, String>()
                    exec("SELECT uid, train_id FROM trust_movements WHERE uid IN ($uidInClause) AND event_ts >= datetime(?, '-12 hours') AND event_ts < datetime(?, '+36 hours') AND train_id LIKE '%' || ? GROUP BY uid, train_id HAVING SUM(CASE WHEN is_cancelled=0 THEN 1 ELSE 0 END)=0 AND SUM(CASE WHEN is_cancelled=1 THEN 1 ELSE 0 END)>0", uidInArgs + trustDateArgs) { rs ->
                        while (rs.next()) {
                            val u = rs.getString("uid") ?: continue
                            val t = rs.getString("train_id") ?: continue
                            cancelOnlyTrainIds[u] = t
                        }
                    }
                    exec("SELECT uid, train_id, scheduled_time, actual_time, is_cancelled, cancel_reason FROM trust_movements WHERE uid IN ($uidInClause) AND event_ts >= datetime(?, '-12 hours') AND event_ts < datetime(?, '+36 hours') AND train_id LIKE '%' || ? ORDER BY event_ts DESC", uidInArgs + trustDateArgs) { rs ->
                        while (rs.next()) {
                            val uid = rs.getString("uid")?.takeIf { it.isNotEmpty() } ?: continue
                            val rowTrainId = rs.getString("train_id") ?: ""
                            val expectedTrainId = cancelOnlyTrainIds[uid]
                            if (expectedTrainId != null && rowTrainId != expectedTrainId) continue
                            val scht = rs.getString("scheduled_time") ?: continue
                            val actualTime = rs.getString("actual_time") ?: ""
                            val isCancelled = rs.getBoolean("is_cancelled")
                            val rawReasonCode = rs.getString("cancel_reason")
                            val reason = ReasonCodes.resolve(rawReasonCode)
                            val triple = Triple(actualTime, isCancelled, reason)
                            val stopKey = "$uid|$scht"
                            if (!trustMap.containsKey(stopKey)) trustMap[stopKey] = triple
                            if (!trustRawReason.containsKey(stopKey)) trustRawReason[stopKey] = rawReasonCode
                            if (isCancelled && !trustMap.containsKey(uid)) trustMap[uid] = triple
                            if (isCancelled && !trustRawReason.containsKey(uid)) trustRawReason[uid] = rawReasonCode
                            if (!isCancelled && actualTime.isNotEmpty() && !uidLastDelay.containsKey(uid)) {
                                var diff = actualTime.toMinutes() - scht.toMinutes()
                                if (diff < -720) diff += 1440 else if (diff > 720) diff -= 1440
                                if (diff >= 0) uidLastDelay[uid] = diff
                            }
                        }
                    }
                } catch (e: Exception) {
                }
            }
            val boardUids = sortedRows.map { it[Schedules.uid] }.toSet()
            val overrides = resolveOriginDestOverrides(boardUids, date)
            val darwinPlatformMap = AppDatabase.getDarwinPlatformsForUids(boardUids)
            val trustPlatformMap = AppDatabase.getTrustPlatforms(boardUids)
            val activatedUids = AppDatabase.getActivatedUids(boardUids, date)
            sortedRows.mapNotNull { row ->
                val scht = row[Schedules.scheduledTime]
                val boardStopKey = "${row[Schedules.uid]}|$scht"
                val stopEntry = trustMap[boardStopKey]
                val uidEntry = trustMap[row[Schedules.uid]]
                val trustEntry = stopEntry
                val cancelEntry = stopEntry ?: if (uidEntry?.second == true) uidEntry else null
                val cancelRawReason = if (stopEntry != null) {
                    trustRawReason[boardStopKey]
                } else if (uidEntry?.second == true) {
                    trustRawReason[row[Schedules.uid]]
                } else {
                    null
                }
                val actualTime = trustEntry?.first?.takeIf { it.isNotEmpty() } ?: ""
                val isCancelledByTrust = trustEntry?.second ?: false
                val cancelReasonCode = trustEntry?.third
                val isCancelledFinal = (cancelEntry?.second ?: false) || row[Schedules.stpIndicator] == 'C'
                val isCancelledFinal2 = isCancelledFinal ||
                    (
                        actualTime.isEmpty() && !uidLastDelay.containsKey(row[Schedules.uid]) &&
                            ScheduleCancellations.isCancelled(row[Schedules.uid], date.toString())
                        )
                val ttBoardCancelled = isCancelledFinal2 && !isCancelledFinal
                val uidDelay = uidLastDelay[row[Schedules.uid]]
                val (effectiveActualTime, isPredictedTime) =
                    if (actualTime.isEmpty() && !isCancelledFinal2 && uidDelay != null && uidDelay > 0) {
                        val projMins = (scht.toMinutes() + uidDelay + 1440) % 1440
                        "%02d:%02d".format(projMins / 60, projMins % 60) to true
                    } else {
                        actualTime to false
                    }
                val (effectiveOriginTiploc, effectiveOriginCrs) = overrides.effectiveOrigin(row)
                val (effectiveDestTiploc, effectiveDestCrs) = overrides.effectiveDest(row)
                val (plt_, pltC_) = resolvePlatformForBoard(
                    row[Schedules.uid], row[Schedules.tiploc], row[Schedules.platform],
                    darwinPlatformMap, trustPlatformMap, row[Schedules.crs]
                )
                if (mode != BoardMode.ALL &&
                    ((isCancelledFinal2 && cancelRawReason in setOf("PG", "PD")) || !activatedUids.contains(row[Schedules.uid]))
                ) {
                    null
                } else {
                    ServiceResponse(
                        uid = row[Schedules.uid],
                        headcode = row[Schedules.headcode],
                        atocCode = CorpusLookup.operatorForTsc(row[Schedules.tsc]),
                        scheduledTime = row[Schedules.scheduledTime],
                        scheduledArrival = row[Schedules.scheduledArrival],
                        scheduledDeparture = row[Schedules.scheduledDeparture],
                        platform = plt_,
                        platformConfirmed = pltC_,
                        isPass = row[Schedules.isPass],
                        stopType = row[Schedules.stopType],
                        originTiploc = effectiveOriginTiploc,
                        destTiploc = effectiveDestTiploc,
                        originCrs = effectiveOriginCrs,
                        destCrs = effectiveDestCrs,
                        actualTime = effectiveActualTime,
                        isCancelled = isCancelledFinal2,
                        cancelReason = cancelEntry?.third ?: if (ttBoardCancelled) "Cancelled in the timetable" else null,
                        isPredicted = isPredictedTime,
                        link = "/?service=${row[Schedules.uid]}&crs=$safeCrsQ&date=$date",
                        serviceDate = date
                    )
                }
            }.withResolvedNames().let { services ->
                val wrapsMidnight = windowEnd < windowStart
                val windowStartMins = windowStart.toMinutes()
                services.sortedBy { service ->
                    val displayTime = if (service.isCancelled) service.scheduledTime else service.actualTime.ifEmpty { service.scheduledTime }
                    val mins = displayTime.toMinutes()
                    if (wrapsMidnight && mins < windowStartMins) mins + 1440 else mins
                }
            }
        }
    }
}

private val prettyJson = Json { prettyPrint = true }

package com.traintracker.server.hsp

import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.database.AppDatabase
import java.net.HttpURLConnection
import java.net.URI
import java.time.DayOfWeek
import java.time.LocalDate
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.json.JSONArray
import org.json.JSONObject
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("HspClient")

@Serializable
data class HspMetricsRequest(
    val from_loc: String,
    val to_loc: String,
    val from_time: String,
    val to_time: String,
    val from_date: String,
    val to_date: String,
    val days: String
)

@Serializable
data class HspServiceMetrics(
    val rid: String,
    val originTiploc: String,
    val destTiploc: String,
    val scheduledDep: String,
    val scheduledArr: String,
    val tocCode: String,
    val matchedServices: Int,
    val onTime: Int,
    val total: Int,
    val punctualityPct: Int,
    val originCrs: String = ""
)

@Serializable
data class HspMetricsResponse(
    val services: List<HspServiceMetrics>
)

@Serializable
data class HspDetailsRequest(
    val rid: String
)

@Serializable
data class HspLocation(
    val tiploc: String,
    val scheduledDep: String,
    val scheduledArr: String,
    val actualDep: String,
    val actualArr: String,
    val cancelReason: String
)

@Serializable
data class HspDetailsResponse(
    val rid: String,
    val date: String,
    val tocCode: String,
    val locations: List<HspLocation>
)

object HspClient {
    private val httpSemaphore = Semaphore(2)
    private const val BASE_URL = "https://api1.raildata.org.uk/1010-historical-service-performance-_hsp_v1/api/v1"
    private val apiKey: String? by lazy { System.getenv("HSP_API_KEY")?.takeIf { it.isNotBlank() } }
    val isAvailable: Boolean
        get() = apiKey != null
    private val metricsCache = ConcurrentHashMap<String, HspMetricsResponse>()
    private val detailsCache = ConcurrentHashMap<String, Pair<HspDetailsResponse, Long>>()
    private val inFlight = ConcurrentHashMap<String, CompletableFuture<HspMetricsResponse?>>()
    private val DAY_CHUNKS = listOf(
        "0000" to "0259",
        "0300" to "0559",
        "0600" to "0859",
        "0900" to "1159",
        "1200" to "1459",
        "1500" to "1759",
        "1800" to "2059",
        "2100" to "2359"
    )

    fun getMetrics(req: HspMetricsRequest): HspMetricsResponse? {
        val key = apiKey ?: run {
            log.warn("HSP_API_KEY not set — HSP unavailable")
            return null
        }
        val fromLoc = req.from_loc.uppercase().filter { it.isLetter() }.take(3)
        val toLoc = req.to_loc.uppercase().filter { it.isLetter() }.take(3)
        if (fromLoc.length != 3 || toLoc.length != 3) {
            log.warn("HSP invalid CRS: from='${req.from_loc}' to='${req.to_loc}'")
            return null
        }
        val cacheKey = "$fromLoc|$toLoc|${req.from_date}|${req.to_date}|${req.from_time}|${req.to_time}|${req.days}"
        AppDatabase.getHspCache(cacheKey)?.let { cached ->
            log.info("HSP metrics DB cache hit: $cacheKey")
            try {
                val arr = JSONArray(cached)
                val services = (0 until arr.length()).mapNotNull { i -> parseServiceFromJson(arr.optJSONObject(i) ?: return@mapNotNull null) }
                return HspMetricsResponse(services).also { metricsCache[cacheKey] = it }
            } catch (_: Exception) {
            }
        }
        metricsCache[cacheKey]?.let {
            log.info("HSP metrics cache hit: $cacheKey")
            return it
        }
        inFlight[cacheKey]?.let {
            log.info("HSP waiting for in-flight request: $cacheKey")
            return it.get()
        }
        if (req.from_time == "0000" && req.to_time == "2359") {
            val future = CompletableFuture<HspMetricsResponse?>()
            inFlight[cacheKey] = future
            try {
                log.info("HSP full-day query — splitting into ${DAY_CHUNKS.size} chunks: $fromLoc→$toLoc ${req.from_date}")
                val allServices = mutableListOf<HspServiceMetrics>()
                var allChunksSucceeded = true
                for ((chunkFrom, chunkTo) in DAY_CHUNKS) {
                    Thread.sleep(8000L)
                    val result = getMetricsChunk(fromLoc, toLoc, req.copy(from_time = chunkFrom, to_time = chunkTo), key)
                    if (result != null) allServices.addAll(result.services) else allChunksSucceeded = false
                    if (allServices.isNotEmpty()) Thread.sleep(1000L)
                }
                val merged = HspMetricsResponse(allServices)
                if (allServices.isNotEmpty() && allChunksSucceeded) {
                    metricsCache[cacheKey] = merged
                    try {
                        AppDatabase.setHspCache(cacheKey, servicesToJson(allServices))
                    } catch (_: Exception) {
                    }
                }
                log.info("HSP full-day merge: ${allServices.size} services for $fromLoc→$toLoc ${req.from_date}")
                future.complete(merged)
                return merged
            } finally {
                inFlight.remove(cacheKey)
            }
        }
        return getMetricsChunk(fromLoc, toLoc, req, key)?.also {
            if (it.services.isNotEmpty()) metricsCache[cacheKey] = it
        }
    }

    private fun getMetricsChunk(fromLoc: String, toLoc: String, req: HspMetricsRequest, key: String): HspMetricsResponse? {
        val chunkCacheKey = "$fromLoc|$toLoc|${req.from_date}|${req.to_date}|${req.from_time}|${req.to_time}|${req.days}"
        metricsCache[chunkCacheKey]?.let { return it }
        val bodyJson = buildJsonObject {
            put("from_loc", fromLoc)
            put("to_loc", toLoc)
            put("from_time", req.from_time)
            put("to_time", req.to_time)
            put("from_date", req.from_date)
            put("to_date", req.to_date)
            put("days", req.days)
        }.toString()
        val raw = postWithRetry("$BASE_URL/serviceMetrics", bodyJson, key) ?: return null
        return try {
            val json = Json.parseToJsonElement(raw).jsonObject
            val body = json["body"]?.jsonObject ?: json
            val svcs = body["Services"]?.jsonArray ?: return HspMetricsResponse(emptyList())
            val services = mutableListOf<HspServiceMetrics>()
            for (svcEl in svcs) {
                val svc = svcEl.jsonObject
                val attrs = svc["serviceAttributesMetrics"]?.jsonObject ?: continue
                val rids = attrs["rids"]?.jsonArray ?: continue
                var onTime = 0
                var total = 0
                for (mEl in svc["Metrics"]?.jsonArray ?: JsonArray(emptyList())) {
                    val m = mEl.jsonObject
                    if (m["global_tolerance"]?.jsonPrimitive?.booleanOrNull == true) {
                        val notTol = m["num_not_tolerance"]?.jsonPrimitive?.content?.toIntOrNull() ?: 0
                        val tol = m["num_tolerance"]?.jsonPrimitive?.content?.toIntOrNull() ?: 0
                        onTime += tol
                        total += tol + notTol
                    }
                }
                val pct = if (total > 0) onTime * 100 / total else -1
                val matched = attrs["matched_services"]?.jsonPrimitive?.content?.toIntOrNull() ?: 0
                val originLoc = attrs["origin_location"]?.jsonPrimitive?.content ?: ""
                for (ridEl in rids) {
                    val rid = ridEl.jsonPrimitive.content.trim()
                    if (rid.isEmpty()) continue
                    services.add(
                        HspServiceMetrics(
                            rid,
                            originLoc,
                            attrs["destination_location"]?.jsonPrimitive?.content ?: "",
                            hhmm(attrs["gbtt_ptd"]?.jsonPrimitive?.content),
                            hhmm(attrs["gbtt_pta"]?.jsonPrimitive?.content),
                            attrs["toc_code"]?.jsonPrimitive?.content ?: "",
                            matched,
                            onTime,
                            total,
                            pct,
                            originLoc.uppercase().trim().ifEmpty { CorpusLookup.crsFromTiploc(originLoc) ?: "" }
                        )
                    )
                }
            }
            val result = HspMetricsResponse(services)
            if (services.isNotEmpty()) {
                metricsCache[chunkCacheKey] = result
                log.info("HSP chunk cached: ${req.from_time}-${req.to_time} (${services.size} services)")
            }
            result
        } catch (e: Exception) {
            log.error("HSP metrics parse error: ${e.message}", e)
            null
        }
    }

    fun getDetails(rid: String): HspDetailsResponse? {
        val key = apiKey ?: return null
        detailsCache[rid]?.let { cached ->
            val (detail, fetchedAt) = cached
            val ageMs = System.currentTimeMillis() - fetchedAt
            if (ageMs < 82800000L) {
                log.info("HSP details cache hit: rid=$rid age=${ageMs / 1000}s")
                return detail
            }
            detailsCache.remove(rid)
        }
        val dbCacheKey = "details:$rid"
        AppDatabase.getHspCache(dbCacheKey)?.let { cached ->
            try {
                val parsed = Json.decodeFromString<HspDetailsResponse>(cached)
                log.info("HSP details DB cache hit: rid=$rid")
                detailsCache[rid] = Pair(parsed, System.currentTimeMillis())
                return parsed
            } catch (_: Exception) {
            }
        }
        val raw = postWithRetry("$BASE_URL/serviceDetails", buildJsonObject { put("rid", rid) }.toString(), key) ?: return null
        return try {
            val json = Json.parseToJsonElement(raw).jsonObject
            val body = json["body"]?.jsonObject ?: json
            val detail = body["serviceAttributesDetails"]?.jsonObject ?: return null
            val locations = (detail["locations"]?.jsonArray ?: JsonArray(emptyList())).mapNotNull { locEl ->
                val l = locEl.jsonObject
                val dep = hhmm(l["actual_td"]?.jsonPrimitive?.content)
                val arr = hhmm(l["actual_ta"]?.jsonPrimitive?.content)
                HspLocation(
                    l["location"]?.jsonPrimitive?.content ?: return@mapNotNull null,
                    hhmm(l["gbtt_ptd"]?.jsonPrimitive?.content),
                    hhmm(l["gbtt_pta"]?.jsonPrimitive?.content),
                    dep,
                    arr,
                    if (dep.isEmpty() && arr.isEmpty()) l["late_canc_reason"]?.jsonPrimitive?.content ?: "" else ""
                )
            }
            HspDetailsResponse(
                detail["rid"]?.jsonPrimitive?.content ?: rid,
                detail["date_of_service"]?.jsonPrimitive?.content ?: "",
                detail["toc_code"]?.jsonPrimitive?.content ?: "",
                locations
            ).also {
                detailsCache[rid] = Pair(it, System.currentTimeMillis())
                try {
                    AppDatabase.setHspCache(dbCacheKey, Json.encodeToString(it))
                } catch (_: Exception) {
                }
            }
        } catch (e: Exception) {
            log.error("HSP details parse error for rid=$rid: ${e.message}", e)
            null
        }
    }

    private fun postWithRetry(url: String, bodyJson: String, apiKey: String): String? {
        val delays = listOf(3000L, 10000L, 30000L)
        repeat(4) { attempt ->
            if (attempt > 0) {
                val delay = delays.getOrElse(attempt - 1) { 30000L }
                log.info("HSP retrying $url (attempt ${attempt + 1}, waiting ${delay / 1000}s)")
                Thread.sleep(delay)
            }
            post(url, bodyJson, apiKey)?.let { return it }
        }
        log.warn("HSP $url failed after 4 attempts")
        return null
    }

    private fun post(url: String, bodyJson: String, apiKey: String): String? {
        httpSemaphore.acquire()
        return try {
            val conn = (URI.create(url).toURL().openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                connectTimeout = 15000
                readTimeout = 70000
                doOutput = true
                setRequestProperty("x-apikey", apiKey)
                setRequestProperty("Content-Type", "application/json")
                setRequestProperty("Accept", "application/json")
            }
            conn.outputStream.use { it.write(bodyJson.toByteArray()) }
            if (conn.responseCode in 200 until 300) {
                conn.inputStream.bufferedReader().use { it.readText() }
            } else {
                val err = try {
                    conn.errorStream?.bufferedReader()?.use { it.readText() }
                } catch (_: Exception) {
                    ""
                }
                log.warn("HSP POST $url → HTTP ${conn.responseCode}: $err")
                null
            }
        } catch (e: Exception) {
            log.error("HSP POST $url failed: ${e.message}")
            null
        } finally {
            httpSemaphore.release()
        }
    }

    private fun parseServiceFromJson(s: JSONObject): HspServiceMetrics = HspServiceMetrics(
        s.optString("rid"),
        s.optString("originTiploc"),
        s.optString("destTiploc"),
        s.optString("scheduledDep"),
        s.optString("scheduledArr"),
        s.optString("tocCode"),
        s.optInt("matchedServices"),
        s.optInt("onTime"),
        s.optInt("total"),
        s.optInt("punctualityPct", -1),
        s.optString("originCrs")
    )

    private fun servicesToJson(services: List<HspServiceMetrics>): String {
        val arr = JSONArray()
        for (s in services) {
            arr.put(
                JSONObject().apply {
                    put("rid", s.rid)
                    put("originTiploc", s.originTiploc)
                    put("destTiploc", s.destTiploc)
                    put("scheduledDep", s.scheduledDep)
                    put("scheduledArr", s.scheduledArr)
                    put("tocCode", s.tocCode)
                    put("matchedServices", s.matchedServices)
                    put("onTime", s.onTime)
                    put("total", s.total)
                    put("punctualityPct", s.punctualityPct)
                    put("originCrs", s.originCrs)
                }
            )
        }
        return arr.toString()
    }

    private fun hhmm(raw: String?): String {
        if (raw.isNullOrBlank()) return ""
        val s = raw.trim().padStart(4, '0')
        return if (s.length >= 4) "${s.substring(0, 2)}:${s.substring(2, 4)}" else ""
    }

    fun daysParam(date: String): String = try {
        when (LocalDate.parse(date).dayOfWeek) {
            DayOfWeek.SATURDAY -> "SATURDAY"
            DayOfWeek.SUNDAY -> "SUNDAY"
            else -> "WEEKDAY"
        }
    } catch (e: Exception) {
        "WEEKDAY"
    }
}

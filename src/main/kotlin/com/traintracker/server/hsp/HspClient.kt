package com.traintracker.server.hsp

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import org.slf4j.LoggerFactory
import java.net.HttpURLConnection
import java.net.URL
import java.time.DayOfWeek
import java.time.LocalDate
import java.util.concurrent.ConcurrentHashMap
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.cif.CorpusLookup

private val log = LoggerFactory.getLogger("HspClient")

// ─── Request / response models ────────────────────────────────────────────────

@Serializable
data class HspMetricsRequest(
    val from_loc:   String,
    val to_loc:     String,
    val from_time:  String,
    val to_time:    String,
    val from_date:  String,
    val to_date:    String,
    val days:       String
)

@Serializable
data class HspDetailsRequest(val rid: String)

@Serializable
data class HspServiceMetrics(
    val rid:                  String,
    val originTiploc:         String,
    val destTiploc:           String,
    val scheduledDep:         String,
    val scheduledArr:         String,
    val tocCode:              String,
    val matchedServices:      Int,
    val onTime:               Int,
    val total:                Int,
    val punctualityPct:       Int,
    val originCrs:            String = ""
)

@Serializable
data class HspMetricsResponse(val services: List<HspServiceMetrics>)

@Serializable
data class HspLocation(
    val tiploc:       String,
    val scheduledDep: String,
    val scheduledArr: String,
    val actualDep:    String,
    val actualArr:    String,
    val cancelReason: String
)

@Serializable
data class HspDetailsResponse(
    val rid:       String,
    val date:      String,
    val tocCode:   String,
    val locations: List<HspLocation>
)

// ─── Client ───────────────────────────────────────────────────────────────────

object HspClient {
    private val httpSemaphore = java.util.concurrent.Semaphore(2)

    private const val BASE_URL =
        "https://api1.raildata.org.uk/1010-historical-service-performance-_hsp_v1/api/v1"

    private val apiKey: String? by lazy {
        System.getenv("HSP_API_KEY")?.takeIf { it.isNotBlank() }
    }

    val isAvailable: Boolean get() = apiKey != null

    // ── Caches ────────────────────────────────────────────────────────────────
    private val metricsCache = ConcurrentHashMap<String, HspMetricsResponse>()
    // detailsCache TTL: 23 hours — historic data is stable but shouldn't persist across day boundaries
    private val detailsCache = ConcurrentHashMap<String, Pair<HspDetailsResponse, Long>>()
    // In-flight deduplication — prevents concurrent fetches for the same key
    private val inFlight = ConcurrentHashMap<String, java.util.concurrent.CompletableFuture<HspMetricsResponse?>>()

    // ── /serviceMetrics ───────────────────────────────────────────────────────

    // Split full-day queries into 3-hour chunks to avoid API timeouts
    private val DAY_CHUNKS = listOf(
        "0000" to "0259", "0300" to "0559", "0600" to "0859",
        "0900" to "1159", "1200" to "1459", "1500" to "1759",
        "1800" to "2059", "2100" to "2359"
    )

    fun getMetrics(req: HspMetricsRequest): HspMetricsResponse? {
        val key = apiKey ?: run { log.warn("HSP_API_KEY not set — HSP unavailable"); return null }

        // HSP requires exactly 3 uppercase letters
        val fromLoc = req.from_loc.uppercase().filter { it.isLetter() }.take(3)
        val toLoc   = req.to_loc.uppercase().filter { it.isLetter() }.take(3)
        if (fromLoc.length != 3 || toLoc.length != 3) {
            log.warn("HSP invalid CRS: from='${req.from_loc}' to='${req.to_loc}'")
            return null
        }

        val cacheKey = "$fromLoc|$toLoc|${req.from_date}|${req.to_date}|${req.from_time}|${req.to_time}|${req.days}"

        // Check persistent DB cache first (survives restarts)
        AppDatabase.getHspCache(cacheKey)?.let { cached ->
            log.info("HSP metrics DB cache hit: $cacheKey")
            try {
                val arr = org.json.JSONArray(cached)
                val services = (0 until arr.length()).mapNotNull { i ->
                    parseServiceFromJson(arr.optJSONObject(i) ?: return@mapNotNull null)
                }
                return HspMetricsResponse(services).also { metricsCache[cacheKey] = it }
            } catch (_: Exception) { }
        }
        metricsCache[cacheKey]?.let { log.info("HSP metrics cache hit: $cacheKey"); return it }
        inFlight[cacheKey]?.let { log.info("HSP waiting for in-flight request: $cacheKey"); return it.get() }

        // Full-day query — split into chunks
        if (req.from_time == "0000" && req.to_time == "2359") {
            val future = java.util.concurrent.CompletableFuture<HspMetricsResponse?>()
            inFlight[cacheKey] = future
            try {
                log.info("HSP full-day query — splitting into ${DAY_CHUNKS.size} chunks: $fromLoc→$toLoc ${req.from_date}")
                val allServices = mutableListOf<HspServiceMetrics>()
                var allChunksSucceeded = true
                for ((chunkFrom, chunkTo) in DAY_CHUNKS) {
                    Thread.sleep(8000)
                    val result = getMetricsChunk(fromLoc, toLoc, req.copy(from_time = chunkFrom, to_time = chunkTo), key)
                    if (result != null) allServices.addAll(result.services) else allChunksSucceeded = false
                    if (allServices.isNotEmpty()) Thread.sleep(1_000)
                }
                val merged = HspMetricsResponse(allServices)
                // Only cache if ALL chunks succeeded — never cache partial results
                if (allServices.isNotEmpty() && allChunksSucceeded) {
                    metricsCache[cacheKey] = merged
                    try { AppDatabase.setHspCache(cacheKey, servicesToJson(allServices)) } catch (_: Exception) {}
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
            put("from_loc",  fromLoc); put("to_loc",    toLoc)
            put("from_time", req.from_time); put("to_time",   req.to_time)
            put("from_date", req.from_date); put("to_date",   req.to_date)
            put("days",      req.days)
        }.toString()

        val raw = postWithRetry("$BASE_URL/serviceMetrics", bodyJson, key) ?: return null

        return try {
            val json = Json.parseToJsonElement(raw).jsonObject
            val body = json["body"]?.jsonObject ?: json
            val svcs = body["Services"]?.jsonArray ?: return HspMetricsResponse(emptyList())

            val services = mutableListOf<HspServiceMetrics>()
            for (svcEl in svcs) {
                val svc   = svcEl.jsonObject
                val attrs = svc["serviceAttributesMetrics"]?.jsonObject ?: continue
                val rids  = attrs["rids"]?.jsonArray ?: continue

                var onTime = 0; var total = 0
                for (mEl in svc["Metrics"]?.jsonArray ?: JsonArray(emptyList())) {
                    val m = mEl.jsonObject
                    if (m["global_tolerance"]?.jsonPrimitive?.booleanOrNull == true) {
                        val notTol = m["num_not_tolerance"]?.jsonPrimitive?.content?.toIntOrNull() ?: 0
                        val tol    = m["num_tolerance"]?.jsonPrimitive?.content?.toIntOrNull() ?: 0
                        onTime += tol; total += tol + notTol
                    }
                }
                val pct       = if (total > 0) (onTime * 100 / total) else -1
                val matched   = attrs["matched_services"]?.jsonPrimitive?.content?.toIntOrNull() ?: 0
                val originLoc = attrs["origin_location"]?.jsonPrimitive?.content ?: ""

                for (ridEl in rids) {
                    val rid = ridEl.jsonPrimitive.content.trim()
                    if (rid.isEmpty()) continue
                    services.add(HspServiceMetrics(
                        rid             = rid,
                        originTiploc    = originLoc,
                        destTiploc      = attrs["destination_location"]?.jsonPrimitive?.content ?: "",
                        scheduledDep    = hhmm(attrs["gbtt_ptd"]?.jsonPrimitive?.content),
                        scheduledArr    = hhmm(attrs["gbtt_pta"]?.jsonPrimitive?.content),
                        tocCode         = attrs["toc_code"]?.jsonPrimitive?.content ?: "",
                        matchedServices = matched,
                        onTime          = onTime,
                        total           = total,
                        punctualityPct  = pct,
                        originCrs       = originLoc.uppercase().trim().ifEmpty {
                            CorpusLookup.crsFromTiploc(originLoc) ?: ""
                        }
                    ))
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

    // ── /serviceDetails ───────────────────────────────────────────────────────

    fun getDetails(rid: String): HspDetailsResponse? {
        val key = apiKey ?: return null

        detailsCache[rid]?.let { (cached, fetchedAt) ->
            val ageMs = System.currentTimeMillis() - fetchedAt
            if (ageMs < 23 * 3600 * 1000L) {
                log.info("HSP details cache hit: rid=$rid age=${ageMs/1000}s")
                return cached
            }
            detailsCache.remove(rid)
        }

        val raw = postWithRetry(
            "$BASE_URL/serviceDetails",
            buildJsonObject { put("rid", rid) }.toString(),
            key
        ) ?: return null

        return try {
            val json   = Json.parseToJsonElement(raw).jsonObject
            val body   = json["body"]?.jsonObject ?: json
            val detail = body["serviceAttributesDetails"]?.jsonObject ?: return null

            val locations = (detail["locations"]?.jsonArray ?: JsonArray(emptyList())).mapNotNull { locEl ->
                val l   = locEl.jsonObject
                val dep = hhmm(l["actual_td"]?.jsonPrimitive?.content)
                val arr = hhmm(l["actual_ta"]?.jsonPrimitive?.content)
                HspLocation(
                    tiploc       = l["location"]?.jsonPrimitive?.content ?: return@mapNotNull null,
                    scheduledDep = hhmm(l["gbtt_ptd"]?.jsonPrimitive?.content),
                    scheduledArr = hhmm(l["gbtt_pta"]?.jsonPrimitive?.content),
                    actualDep    = dep,
                    actualArr    = arr,
                    // Only a real cancellation if no actual times — late_canc_reason is also set for delayed-but-ran services
                    cancelReason = if (dep.isEmpty() && arr.isEmpty())
                        l["late_canc_reason"]?.jsonPrimitive?.content ?: "" else ""
                )
            }

            HspDetailsResponse(
                rid       = detail["rid"]?.jsonPrimitive?.content ?: rid,
                date      = detail["date_of_service"]?.jsonPrimitive?.content ?: "",
                tocCode   = detail["toc_code"]?.jsonPrimitive?.content ?: "",
                locations = locations
            ).also { detailsCache[rid] = Pair(it, System.currentTimeMillis()) }
        } catch (e: Exception) {
            log.error("HSP details parse error for rid=$rid: ${e.message}", e)
            null
        }
    }

    // ── HTTP — with retries on timeout or 5xx ─────────────────────────────────

    private fun postWithRetry(url: String, bodyJson: String, apiKey: String): String? {
        val delays = listOf(3_000L, 10_000L, 30_000L)
        repeat(4) { attempt ->
            if (attempt > 0) {
                val delay = delays.getOrElse(attempt - 1) { 30_000L }
                log.info("HSP retrying $url (attempt ${attempt + 1}, waiting ${delay/1000}s)")
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
            val conn = (URL(url).openConnection() as HttpURLConnection).apply {
                requestMethod  = "POST"
                connectTimeout = 15_000
                readTimeout    = 70_000
                doOutput       = true
                setRequestProperty("x-apikey", apiKey)
                setRequestProperty("Content-Type", "application/json")
                setRequestProperty("Accept", "application/json")
            }
            conn.outputStream.use { it.write(bodyJson.toByteArray()) }
            if (conn.responseCode in 200..299) {
                conn.inputStream.bufferedReader().readText()
            } else {
                val err = try { conn.errorStream?.bufferedReader()?.readText() } catch (_: Exception) { "" }
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

    // ── JSON helpers for persistent cache ────────────────────────────────────

    private fun parseServiceFromJson(s: org.json.JSONObject): HspServiceMetrics = HspServiceMetrics(
        rid             = s.optString("rid"),
        originTiploc    = s.optString("originTiploc"),
        destTiploc      = s.optString("destTiploc"),
        scheduledDep    = s.optString("scheduledDep"),
        scheduledArr    = s.optString("scheduledArr"),
        tocCode         = s.optString("tocCode"),
        matchedServices = s.optInt("matchedServices"),
        onTime          = s.optInt("onTime"),
        total           = s.optInt("total"),
        punctualityPct  = s.optInt("punctualityPct", -1),
        originCrs       = s.optString("originCrs")
    )

    private fun servicesToJson(services: List<HspServiceMetrics>): String {
        val arr = org.json.JSONArray()
        for (s in services) arr.put(org.json.JSONObject().apply {
            put("rid",             s.rid)
            put("originTiploc",    s.originTiploc)
            put("destTiploc",      s.destTiploc)
            put("scheduledDep",    s.scheduledDep)
            put("scheduledArr",    s.scheduledArr)
            put("tocCode",         s.tocCode)
            put("matchedServices", s.matchedServices)
            put("onTime",          s.onTime)
            put("total",           s.total)
            put("punctualityPct",  s.punctualityPct)
            put("originCrs",       s.originCrs)
        })
        return arr.toString()
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun hhmm(raw: String?): String {
        if (raw.isNullOrBlank()) return ""
        val s = raw.trim().padStart(4, '0')
        return if (s.length >= 4) "${s.substring(0, 2)}:${s.substring(2, 4)}" else ""
    }

    /** Returns WEEKDAY / SATURDAY / SUNDAY for a given ISO date string (yyyy-MM-dd). */
    fun daysParam(date: String): String = try {
        when (LocalDate.parse(date).dayOfWeek) {
            DayOfWeek.SATURDAY -> "SATURDAY"
            DayOfWeek.SUNDAY   -> "SUNDAY"
            else               -> "WEEKDAY"
        }
    } catch (_: Exception) { "WEEKDAY" }
}

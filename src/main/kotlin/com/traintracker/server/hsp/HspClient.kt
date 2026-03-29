package com.traintracker.server.hsp

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import org.slf4j.LoggerFactory
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.ConcurrentHashMap

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
data class HspDetailsRequest(
    val rid: String
)

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
    val punctualityPct:       Int
)

@Serializable
data class HspMetricsResponse(
    val services: List<HspServiceMetrics>
)

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

    private const val BASE_URL =
        "https://api1.raildata.org.uk/1010-historical-service-performance-_hsp_v1/api/v1"

    private val apiKey: String? by lazy {
        System.getenv("HSP_API_KEY")?.takeIf { it.isNotBlank() }
    }

    val isAvailable: Boolean get() = apiKey != null

    // ── Cache — historic data never changes, so cache forever ─────────────────
    // Key: "from_loc|to_loc|from_date|to_date|from_time|to_time|days"
    private val metricsCache = ConcurrentHashMap<String, HspMetricsResponse>()
    // Key: rid
    private val detailsCache = ConcurrentHashMap<String, HspDetailsResponse>()

    // ── /serviceMetrics ───────────────────────────────────────────────────────

    // Time chunks for splitting full-day queries — each window is light enough for the API
    private val DAY_CHUNKS = listOf("0000" to "0559", "0600" to "1159", "1200" to "1759", "1800" to "2359")

    fun getMetrics(req: HspMetricsRequest): HspMetricsResponse? {
        val key = apiKey ?: run {
            log.warn("HSP_API_KEY not set — HSP unavailable")
            return null
        }

        // HSP requires exactly 3 uppercase letters — strip anything else
        val fromLoc = req.from_loc.uppercase().filter { it.isLetter() }.take(3)
        val toLoc   = req.to_loc.uppercase().filter { it.isLetter() }.take(3)
        if (fromLoc.length != 3 || toLoc.length != 3) {
            log.warn("HSP invalid CRS: from='${req.from_loc}' to='${req.to_loc}'")
            return null
        }

        // Check full-day cache first
        val cacheKey = "$fromLoc|$toLoc|${req.from_date}|${req.to_date}|${req.from_time}|${req.to_time}|${req.days}"
        metricsCache[cacheKey]?.let {
            log.info("HSP metrics cache hit: $cacheKey")
            return it
        }

        // If querying a full day, split into 6-hour chunks to avoid API timeouts
        if (req.from_time == "0000" && req.to_time == "2359") {
            log.info("HSP full-day query — splitting into ${DAY_CHUNKS.size} chunks: $fromLoc→$toLoc ${req.from_date}")
            val allServices = mutableListOf<HspServiceMetrics>()
            for ((chunkFrom, chunkTo) in DAY_CHUNKS) {
                val chunkReq = req.copy(from_time = chunkFrom, to_time = chunkTo)
                val chunkResult = getMetricsChunk(fromLoc, toLoc, chunkReq, key)
                if (chunkResult != null) allServices.addAll(chunkResult.services)
            }
            val merged = HspMetricsResponse(allServices)
            if (allServices.isNotEmpty()) metricsCache[cacheKey] = merged
            log.info("HSP full-day merge: ${allServices.size} services for $fromLoc→$toLoc ${req.from_date}")
            return merged
        }

        return getMetricsChunk(fromLoc, toLoc, req, key)?.also {
            if (it.services.isNotEmpty()) metricsCache[cacheKey] = it
        }
    }

    private fun getMetricsChunk(fromLoc: String, toLoc: String, req: HspMetricsRequest, key: String): HspMetricsResponse? {
        val chunkCacheKey = "$fromLoc|$toLoc|${req.from_date}|${req.to_date}|${req.from_time}|${req.to_time}|${req.days}"
        metricsCache[chunkCacheKey]?.let { return it }

        val bodyJson = buildJsonObject {
            put("from_loc",  fromLoc)
            put("to_loc",    toLoc)
            put("from_time", req.from_time)
            put("to_time",   req.to_time)
            put("from_date", req.from_date)
            put("to_date",   req.to_date)
            put("days",      req.days)
        }.toString()

        // Try up to 2 times on timeout/5xx
        val raw = postWithRetry("$BASE_URL/serviceMetrics", bodyJson, key) ?: return null

        return try {
            val json = Json.parseToJsonElement(raw).jsonObject
            val svcs = json["Services"]?.jsonArray ?: return HspMetricsResponse(emptyList())

            val services = mutableListOf<HspServiceMetrics>()
            for (svcEl in svcs) {
                val svc   = svcEl.jsonObject
                val attrs = svc["serviceAttributesMetrics"]?.jsonObject ?: continue
                val rids  = attrs["rids"]?.jsonArray ?: continue

                var onTime = 0; var total = 0
                val metricsArr = svc["Metrics"]?.jsonArray ?: JsonArray(emptyList())
                for (mEl in metricsArr) {
                    val m = mEl.jsonObject
                    if (m["global_tolerance"]?.jsonPrimitive?.booleanOrNull == true) {
                        val notTol = m["num_not_tolerance"]?.jsonPrimitive?.content?.toIntOrNull() ?: 0
                        val tol    = m["num_tolerance"]?.jsonPrimitive?.content?.toIntOrNull() ?: 0
                        onTime += tol
                        total  += tol + notTol
                    }
                }
                val pct     = if (total > 0) (onTime * 100 / total) else -1
                val matched = attrs["matched_services"]?.jsonPrimitive?.content?.toIntOrNull() ?: 0

                for (ridEl in rids) {
                    val rid = ridEl.jsonPrimitive.content.trim()
                    if (rid.isEmpty()) continue
                    services.add(HspServiceMetrics(
                        rid             = rid,
                        originTiploc    = attrs["origin_location"]?.jsonPrimitive?.content ?: "",
                        destTiploc      = attrs["destination_location"]?.jsonPrimitive?.content ?: "",
                        scheduledDep    = hhmm(attrs["gbtt_ptd"]?.jsonPrimitive?.content),
                        scheduledArr    = hhmm(attrs["gbtt_pta"]?.jsonPrimitive?.content),
                        tocCode         = attrs["toc_code"]?.jsonPrimitive?.content ?: "",
                        matchedServices = matched,
                        onTime          = onTime,
                        total           = total,
                        punctualityPct  = pct
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

        // Check cache first
        detailsCache[rid]?.let {
            log.info("HSP details cache hit: rid=$rid")
            return it
        }

        val bodyJson = buildJsonObject { put("rid", rid) }.toString()
        val raw = postWithRetry("$BASE_URL/serviceDetails", bodyJson, key) ?: return null

        return try {
            val json   = Json.parseToJsonElement(raw).jsonObject
            val detail = json["serviceAttributesDetails"]?.jsonObject ?: return null
            val locArr = detail["locations"]?.jsonArray ?: JsonArray(emptyList())

            val locations = locArr.mapNotNull { locEl ->
                val l = locEl.jsonObject
                HspLocation(
                    tiploc       = l["location"]?.jsonPrimitive?.content ?: return@mapNotNull null,
                    scheduledDep = hhmm(l["gbtt_ptd"]?.jsonPrimitive?.content),
                    scheduledArr = hhmm(l["gbtt_pta"]?.jsonPrimitive?.content),
                    actualDep    = hhmm(l["actual_td"]?.jsonPrimitive?.content),
                    actualArr    = hhmm(l["actual_ta"]?.jsonPrimitive?.content),
                    cancelReason = l["late_canc_reason"]?.jsonPrimitive?.content ?: ""
                )
            }

            val result = HspDetailsResponse(
                rid       = detail["rid"]?.jsonPrimitive?.content ?: rid,
                date      = detail["date_of_service"]?.jsonPrimitive?.content ?: "",
                tocCode   = detail["toc_code"]?.jsonPrimitive?.content ?: "",
                locations = locations
            )
            detailsCache[rid] = result   // Cache on success
            result
        } catch (e: Exception) {
            log.error("HSP details parse error for rid=$rid: ${e.message}", e)
            null
        }
    }

    // ── HTTP — with one retry on timeout or 5xx ───────────────────────────────

    private fun postWithRetry(url: String, bodyJson: String, apiKey: String): String? {
        var lastError: String? = null
        repeat(2) { attempt ->
            if (attempt > 0) {
                log.info("HSP retrying $url (attempt ${attempt + 1})")
                Thread.sleep(2_000)
            }
            val result = post(url, bodyJson, apiKey)
            if (result != null) return result
            lastError = "attempt ${attempt + 1} failed"
        }
        log.warn("HSP $url failed after 2 attempts: $lastError")
        return null
    }

    private fun post(url: String, bodyJson: String, apiKey: String): String? {
        return try {
            val conn = (URL(url).openConnection() as HttpURLConnection).apply {
                requestMethod    = "POST"
                connectTimeout   = 15_000
                readTimeout      = 60_000
                doOutput         = true
                setRequestProperty("x-apikey", apiKey)
                setRequestProperty("Content-Type", "application/json")
                setRequestProperty("Accept", "application/json")
            }
            conn.outputStream.use { it.write(bodyJson.toByteArray()) }
            val code = conn.responseCode
            if (code in 200..299) {
                conn.inputStream.bufferedReader().readText()
            } else {
                val err = try { conn.errorStream?.bufferedReader()?.readText() } catch (_: Exception) { "" }
                log.warn("HSP POST $url → HTTP $code: $err")
                null  // Triggers retry for 5xx
            }
        } catch (e: Exception) {
            log.error("HSP POST $url failed: ${e.message}")
            null  // Triggers retry on timeout
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun hhmm(raw: String?): String {
        if (raw.isNullOrBlank()) return ""
        val s = raw.trim().padStart(4, '0')
        return if (s.length >= 4) "${s.substring(0, 2)}:${s.substring(2, 4)}" else ""
    }

    fun daysParam(date: String): String {
        return try {
            val parts = date.split("-")
            val cal   = java.util.Calendar.getInstance().apply {
                set(parts[0].toInt(), parts[1].toInt() - 1, parts[2].toInt())
            }
            when (cal.get(java.util.Calendar.DAY_OF_WEEK)) {
                java.util.Calendar.SATURDAY -> "SATURDAY"
                java.util.Calendar.SUNDAY   -> "SUNDAY"
                else                        -> "WEEKDAY"
            }
        } catch (_: Exception) { "WEEKDAY" }
    }
}

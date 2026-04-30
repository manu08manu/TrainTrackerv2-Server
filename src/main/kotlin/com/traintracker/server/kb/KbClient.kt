package com.traintracker.server.kb

import org.slf4j.LoggerFactory
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.ConcurrentHashMap
import javax.xml.parsers.DocumentBuilderFactory
import org.w3c.dom.Element
import org.xml.sax.InputSource
import java.io.StringReader

private val log = LoggerFactory.getLogger("KbClient")

object KbClient {

    // ── Credentials (injected from Config) ────────────────────────────────
    var nsiKey:       String = ""
    var nsiSecret:    String = ""
    var nsiTokenUrl:  String = ""
    var nsiUrl:       String = ""

    var incidentsKey:      String = ""
    var incidentsSecret:   String = ""
    var incidentsTokenUrl: String = ""
    var incidentsUrl:      String = ""

    var stationsKey:    String = ""
    var stationsSecret: String = ""
    var stationsUrl:    String = ""

    var tocKey:      String = ""
    var tocSecret:   String = ""
    var tocTokenUrl: String = ""
    var tocUrl:      String = ""

    // ── HTML tag stripper ─────────────────────────────────────────────────────
    private fun stripHtml(html: String): String =
        html.replace(Regex("<[^>]+>"), "")
            .replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
            .replace("&nbsp;", " ").replace("&#39;", "'").replace("&quot;", "\"").trim()

        // ── TTL caches ────────────────────────────────────────────────────────
    private const val INCIDENTS_TTL_MS = 5 * 60 * 1000L   // 5 min
    private const val NSI_TTL_MS       = 5 * 60 * 1000L   // 5 min
    private const val TOC_TTL_MS       = 60 * 60 * 1000L  // 1 hour
    private const val STATION_TTL_MS   = 60 * 60 * 1000L  // 1 hour

    private var incidentsCache: List<KbIncident> = emptyList()
    private var incidentsFetchedAt = 0L

    private var nsiCache: List<KbNsiEntry> = emptyList()
    private var nsiFetchedAt = 0L

    private var tocCache: List<KbTocEntry> = emptyList()
    private var tocFetchedAt = 0L

    private val stationCache = ConcurrentHashMap<String, Pair<KbStation, Long>>()

    // ── OAuth2 token cache ────────────────────────────────────────────────
    private val tokenCache = ConcurrentHashMap<String, Pair<String, Long>>()

    // ── Public API ────────────────────────────────────────────────────────

    fun getIncidents(): List<KbIncident> {
        val now = System.currentTimeMillis()
        if (now - incidentsFetchedAt < INCIDENTS_TTL_MS && incidentsCache.isNotEmpty())
            return incidentsCache
        return try {
            // Incidents uses x-apikey header directly, not OAuth2
            val xml = get(incidentsUrl, incidentsKey, "", "")
            val parsed = parseIncidentsXml(xml)
            incidentsCache = parsed
            incidentsFetchedAt = System.currentTimeMillis()
            parsed
        } catch (e: Exception) {
            log.warn("getIncidents failed: ${e.message}")
            incidentsCache
        }
    }

    fun getNsi(): List<KbNsiEntry> {
        val now = System.currentTimeMillis()
        if (now - nsiFetchedAt < NSI_TTL_MS && nsiCache.isNotEmpty())
            return nsiCache
        return try {
            // NSI uses x-apikey header directly, not OAuth2
            val xml = get(nsiUrl, nsiKey, "", "")
            val parsed = parseNsiXml(xml)
            nsiCache = parsed
            nsiFetchedAt = System.currentTimeMillis()
            parsed
        } catch (e: Exception) {
            log.warn("getNsi failed: ${e.message}")
            nsiCache
        }
    }

    fun getToc(): List<KbTocEntry> {
        val now = System.currentTimeMillis()
        if (now - tocFetchedAt < TOC_TTL_MS && tocCache.isNotEmpty())
            return tocCache
        return try {
            // TOC uses x-apikey header directly, not OAuth2
            val xml = get(tocUrl, tocKey, "", "")
            val parsed = parseTocXml(xml)
            tocCache = parsed
            tocFetchedAt = System.currentTimeMillis()
            parsed
        } catch (e: Exception) {
            log.warn("getToc failed: ${e.message}")
            tocCache
        }
    }

    fun getStation(crs: String): KbStation? {
        val key = crs.uppercase()
        stationCache[key]?.let { (station, fetchedAt) ->
            if (System.currentTimeMillis() - fetchedAt < STATION_TTL_MS) return station
        }
        return try {
            val url = stationsUrl.replace("{CRS}", key)
            val xml = get(url, stationsKey, stationsSecret, "")
            val parsed = parseStationXml(xml)
            if (parsed != null) stationCache[key] = Pair(parsed, System.currentTimeMillis())
            parsed
        } catch (e: Exception) {
            log.warn("getStation($key) failed: ${e.message}")
            stationCache[key]?.first
        }
    }

    // ── OAuth2 ────────────────────────────────────────────────────────────

    private fun getBearerToken(key: String, secret: String, tokenUrl: String): String {
        val cacheKey = "$key:$tokenUrl"
        tokenCache[cacheKey]?.let { (token, expiry) ->
            if (System.currentTimeMillis() < expiry) return token
        }
        val conn = URL(tokenUrl).openConnection() as HttpURLConnection
        conn.requestMethod = "POST"
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
        val credentials = java.util.Base64.getEncoder().encodeToString("$key:$secret".toByteArray())
        conn.setRequestProperty("Authorization", "Basic $credentials")
        conn.doOutput = true
        conn.connectTimeout = 10_000
        conn.readTimeout = 15_000
        val body = "grant_type=client_credentials"
        conn.outputStream.use { it.write(body.toByteArray()) }
        if (conn.responseCode != 200) throw Exception("Token fetch failed: HTTP ${conn.responseCode}")
        val response = conn.inputStream.bufferedReader().readText()
        val doc = parseXmlOrJson(response)
        val token = doc.first
        val expiresIn = doc.second
        val expiryMs = System.currentTimeMillis() + (expiresIn - 60) * 1000
        tokenCache[cacheKey] = Pair(token, expiryMs)
        return token
    }

    private fun parseXmlOrJson(response: String): Pair<String, Long> {
        // Simple JSON parsing for token response
        val tokenMatch = Regex("\"access_token\"\\s*:\\s*\"([^\"]+)\"").find(response)
        val expiryMatch = Regex("\"expires_in\"\\s*:\\s*(\\d+)").find(response)
        val token = tokenMatch?.groupValues?.get(1) ?: throw Exception("No access_token in response")
        val expiry = expiryMatch?.groupValues?.get(1)?.toLongOrNull() ?: 3600L
        return Pair(token, expiry)
    }

    private fun encode(s: String) = java.net.URLEncoder.encode(s, "UTF-8")

    // ── HTTP ──────────────────────────────────────────────────────────────

    private fun get(url: String, key: String, secret: String, tokenUrl: String): String {
        val conn = URL(url).openConnection() as HttpURLConnection
        conn.requestMethod = "GET"
        conn.connectTimeout = 10_000
        conn.readTimeout = 15_000
        if (secret.isNotEmpty() && tokenUrl.isNotEmpty()) {
            val token = getBearerToken(key, secret, tokenUrl)
            conn.setRequestProperty("Authorization", "Bearer $token")
        } else {
            conn.setRequestProperty("x-apikey", key)
        }
        conn.setRequestProperty("Accept-Encoding", "gzip")
        if (conn.responseCode != 200) throw Exception("HTTP ${conn.responseCode} from $url")
        val encoding = conn.contentEncoding ?: ""
        val stream = if (encoding.equals("gzip", ignoreCase = true))
            java.util.zip.GZIPInputStream(conn.inputStream)
        else conn.inputStream
        return stream.bufferedReader(Charsets.UTF_8).readText()
    }

    // ── XML parsers ───────────────────────────────────────────────────────

    private fun parseIncidentsXml(xml: String): List<KbIncident> {
        val result = mutableListOf<KbIncident>()
        try {
            val doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
                .parse(InputSource(StringReader(xml)))
            val incidents = doc.getElementsByTagName("PtIncident")
            for (i in 0 until incidents.length) {
                val node = incidents.item(i) as? Element ?: continue
                val id          = node.text("IncidentNumber")
                val summary     = node.text("Summary")
                val description = stripHtml(node.text("Description"))
                val isPlanned   = node.text("Planned").equals("true", ignoreCase = true)
                val validity    = node.getElementsByTagName("ValidityPeriod")
                val startTime   = if (validity.length > 0) (validity.item(0) as? Element)?.text("StartTime") ?: "" else ""
                val endTime     = if (validity.length > 0) (validity.item(0) as? Element)?.text("EndTime") ?: "" else ""
                val opRefs      = node.getElementsByTagName("OperatorRef")
                val operators   = (0 until opRefs.length).mapNotNull { j ->
                    opRefs.item(j)?.textContent?.trim()?.uppercase()?.takeIf { it.length in 2..4 }
                }
                val isCleared = endTime.isNotEmpty() && try {
                    java.time.OffsetDateTime.parse(endTime).isBefore(java.time.OffsetDateTime.now())
                } catch (_: Exception) { false }
                if (!isCleared && summary.isNotEmpty()) {
                    result.add(KbIncident(id, summary, description, isPlanned, startTime, endTime, operators))
                }
            }
        } catch (e: Exception) {
            log.error("parseIncidentsXml error: ${e.message}")
        }
        return result
    }

    private fun parseNsiXml(xml: String): List<KbNsiEntry> {
        val result = mutableListOf<KbNsiEntry>()
        try {
            val doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
                .parse(InputSource(StringReader(xml)))
            val tocs = doc.getElementsByTagName("TOC")
            for (i in 0 until tocs.length) {
                val node = tocs.item(i) as? Element ?: continue
                val tocCode           = node.text("TocCode")
                val tocName           = node.text("TocName")
                val statusText        = node.text("Status")
                val statusImage       = node.text("StatusImage")
                val statusDescription = stripHtml(node.text("StatusDescription"))
                val twitterAccount    = node.text("TwitterAccount")
                val additionalInfo    = node.text("AdditionalInfo")

                val level = when {
                    statusText.equals("Good service", ignoreCase = true) -> 1
                    statusImage.contains("tick", ignoreCase = true)       -> 1
                    statusText.contains("minor", ignoreCase = true)       -> 2
                    statusImage.contains("minor", ignoreCase = true)      -> 2
                    statusText.contains("severe", ignoreCase = true)      -> 4
                    statusImage.contains("severe", ignoreCase = true)     -> 4
                    statusImage.contains("note", ignoreCase = true)       -> 2
                    statusText.contains("major", ignoreCase = true) ||
                    statusText.equals("Custom", ignoreCase = true) ||
                    statusImage.contains("disruption", ignoreCase = true) -> 3
                    else -> 1
                }

                val groups = node.getElementsByTagName("ServiceGroup")
                val disruptions = (0 until groups.length).mapNotNull { j ->
                    val g = groups.item(j) as? Element ?: return@mapNotNull null
                    val url = g.text("CustomURL")
                    if (url.isEmpty()) null else NsiDisruption(g.text("CustomDetail"), url)
                }

                val code = tocCode.ifEmpty { "" }
                if (code.isNotEmpty()) {
                    result.add(KbNsiEntry(code, tocName, level.toString(),
                        statusDescription, disruptions, twitterAccount, additionalInfo))
                }
            }
        } catch (e: Exception) {
            log.error("parseNsiXml error: ${e.message}")
        }
        return result
    }

    private fun parseTocXml(xml: String): List<KbTocEntry> {
        val result = mutableListOf<KbTocEntry>()
        try {
            val doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
                .parse(InputSource(StringReader(xml)))
            val tocs = doc.getElementsByTagName("TrainOperatingCompany")
            for (i in 0 until tocs.length) {
                val node = tocs.item(i) as? Element ?: continue
                val code    = node.text("AtocCode")
                val name    = node.text("Name")
                val website = node.text("CompanyWebsite")
                val cs      = node.getElementsByTagName("CustomerService")
                val at      = node.getElementsByTagName("AssistedTravel")
                val lp      = node.getElementsByTagName("LostProperty")
                val csPhone = if (cs.length > 0) (cs.item(0) as? Element)?.text("com:PrimaryTelephoneNumber") ?: "" else ""
                val atPhone = if (at.length > 0) (at.item(0) as? Element)?.text("com:PrimaryTelephoneNumber") ?: "" else ""
                val atUrl   = if (at.length > 0) (at.item(0) as? Element)?.text("com:Url") ?: "" else ""
                val lpUrl   = if (lp.length > 0) (lp.item(0) as? Element)?.text("com:Url") ?: "" else ""
                if (code.isNotEmpty()) {
                    result.add(KbTocEntry(code, name, website, csPhone, atPhone, atUrl, lpUrl))
                }
            }
        } catch (e: Exception) {
            log.error("parseTocXml error: ${e.message}")
        }
        return result
    }

    private fun parseStationXml(xml: String): KbStation? {
        return try {
            val doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
                .parse(InputSource(StringReader(xml)))
            val stations = doc.getElementsByTagName("StationV4.0")
            if (stations.length == 0) return null
            val node = stations.item(0) as? Element ?: return null
            val crs  = node.text("CrsCode").uppercase()
            if (crs.length != 3) return null
            val addressLines = mutableListOf<String>()
            val lines = node.getElementsByTagName("Line")
            for (i in 0 until lines.length) {
                val t = lines.item(i)?.textContent?.trim() ?: ""
                if (t.isNotEmpty() && t != "-") addressLines.add(t)
            }
            val postCode = node.text("PostCode")
            val address  = (addressLines + listOf(postCode)).filter { it.isNotEmpty() }.joinToString(", ")
            KbStation(
                crs               = crs,
                name              = node.text("Name"),
                address           = address,
                telephone         = node.text("TelNationalNumber"),
                staffingNote      = node.text("StaffingLevel"),
                ticketOfficeHours = if (node.getElementsByTagName("Open").length > 0) "Open" else "",
                sstmAvailability  = if (node.textOf("Available", "TicketMachine") == "true") "Available" else "",
                stepFreeAccess    = when (node.text("Coverage")) {
                    "wholeStation"   -> "Whole station"
                    "partialStation" -> "Partial"
                    else             -> ""
                },
                assistanceAvail   = if (node.textOf("Available", "StaffHelpAvailable") == "true") "Available" else "",
                wifi              = if (node.textOf("Available", "WiFi") == "true") "Yes" else "",
                toilets           = if (node.textOf("Available", "Toilets") == "true") "Yes" else "",
                waitingRoom       = if (node.getElementsByTagName("WaitingRoom").length > 0) "Yes" else "",
                cctv              = if (node.textOf("Available", "ClosedCircuitTelevision") == "true") "Yes" else "",
                taxi              = "",
                busInterchange    = "",
                carParking        = node.text("NumberAccessibleSpaces").let {
                    if ((it.toIntOrNull() ?: 0) > 0) "$it accessible spaces" else ""
                }
            )
        } catch (e: Exception) {
            log.warn("parseStationXml error: ${e.message}")
            null
        }
    }

    // ── DOM helpers ───────────────────────────────────────────────────────

    private fun Element.text(tag: String): String =
        getElementsByTagName(tag).item(0)?.textContent?.trim() ?: ""

    private fun Element.textOf(tag: String, parent: String): String {
        val parents = getElementsByTagName(parent)
        for (i in 0 until parents.length) {
            val p = parents.item(i) as? Element ?: continue
            val t = p.getElementsByTagName(tag).item(0)?.textContent?.trim()
            if (!t.isNullOrEmpty()) return t
        }
        return ""
    }
}

// ── Data models ───────────────────────────────────────────────────────────────

data class KbIncident(
    val id: String,
    val summary: String,
    val description: String,
    val isPlanned: Boolean,
    val startTime: String,
    val endTime: String,
    val operators: List<String>
)

data class NsiDisruption(
    val detail: String,
    val url: String
)

data class KbNsiEntry(
    val tocCode: String,
    val tocName: String,
    val status: String,
    val statusDescription: String = "",
    val disruptions: List<NsiDisruption> = emptyList(),
    val twitterHandle: String = "",
    val additionalInfo: String = ""
)

data class KbStation(
    val crs: String,
    val name: String,
    val address: String,
    val telephone: String,
    val staffingNote: String,
    val ticketOfficeHours: String,
    val sstmAvailability: String,
    val stepFreeAccess: String,
    val assistanceAvail: String,
    val wifi: String,
    val toilets: String,
    val waitingRoom: String,
    val cctv: String,
    val taxi: String,
    val busInterchange: String,
    val carParking: String
)

data class KbTocEntry(
    val code: String,
    val name: String,
    val website: String = "",
    val customerServicePhone: String = "",
    val assistedTravelPhone: String = "",
    val assistedTravelUrl: String = "",
    val lostPropertyUrl: String = ""
)

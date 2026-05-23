package com.traintracker.server.kb
import kotlinx.serialization.Serializable

import org.slf4j.LoggerFactory
import org.json.JSONArray
import org.json.JSONObject
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
    // Disruptions Experience API (new JSON feed — single x-apikey for all endpoints)
    var disruptionsKey: String = ""
    var disruptionsUrl: String = ""

    // Stations XML feed (unchanged)
    var stationsKey:    String = ""
    var stationsSecret: String = ""
    var stationsUrl:    String = ""

    // TOC XML feed (unchanged)
    var tocKey:      String = ""
    var tocSecret:   String = ""
    var tocTokenUrl: String = ""
    var tocUrl:      String = ""

    // ── HTML tag stripper ─────────────────────────────────────────────────
    private fun stripHtml(html: String): String =
        html.replace(Regex("<[^>]+>"), "")

    private fun stripUrls(text: String): String =
        text.lines()
            .filter { !it.trim().startsWith("http://") && !it.trim().startsWith("https://") && !it.trim().startsWith("www.") }
            .joinToString(" ")
            .replace(Regex("\\s{2,}"), " ")
            .trim()

    // ── TTL caches ────────────────────────────────────────────────────────
    private const val INCIDENTS_TTL_MS = 5 * 60 * 1000L
    private const val NSI_TTL_MS       = 5 * 60 * 1000L
    private const val TOC_TTL_MS       = 60 * 60 * 1000L
    private const val STATION_TTL_MS   = 30 * 60 * 1000L

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

    fun getIncidents(crs: String = ""): List<KbIncident> {
        val now = System.currentTimeMillis()
        if (now - incidentsFetchedAt < INCIDENTS_TTL_MS && incidentsCache.isNotEmpty())
            return incidentsCache
        return try {
            val url = if (crs.isNotEmpty())
                "$disruptionsUrl/stations/disruptions/incidents?crsCode=$crs"
            else
                "$disruptionsUrl/stations/disruptions/incidents"
            val json = get(url, disruptionsKey, "", "")
            val parsed = parseIncidentsJson(json)
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
            val json = get("$disruptionsUrl/tocs/serviceIndicators", disruptionsKey, "", "")
            val parsed = parseNsiJson(json)
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
            val xml = get(tocUrl, tocKey, tocSecret, tocTokenUrl)
            val parsed = parseTocXml(xml)
            tocCache = parsed
            tocFetchedAt = System.currentTimeMillis()
            parsed
        } catch (e: Exception) {
            log.warn("getToc failed: ${e.message}")
            tocCache
        }
    }

    fun clearStationCache() { stationCache.clear() }

    fun getRawStation(crs: String): String? = try {
        val url = stationsUrl.replace("{CRS}", crs.uppercase())
        get(url, stationsKey, stationsSecret, "")
    } catch (e: Exception) {
        log.warn("getRawStation($crs) failed: ${e.message}")
        null
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

    private val stationMessagesCache = ConcurrentHashMap<String, Pair<KbStationMessages, Long>>()

    fun getStationMessages(crs: String): KbStationMessages? {
        val key = crs.uppercase()
        stationMessagesCache[key]?.let { (msgs, fetchedAt) ->
            if (System.currentTimeMillis() - fetchedAt < STATION_TTL_MS) return msgs
        }
        return try {
            val url = "$disruptionsUrl/stations/disruptions/stationMessages?crsCode=$key"
            val json = get(url, disruptionsKey, "", "")
            val parsed = parseStationMessagesJson(json, key)
            if (parsed != null) stationMessagesCache[key] = Pair(parsed, System.currentTimeMillis())
            parsed
        } catch (e: Exception) {
            log.warn("getStationMessages($key) failed: ${e.message}")
            stationMessagesCache[key]?.first
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
        conn.outputStream.use { it.write("grant_type=client_credentials".toByteArray()) }
        if (conn.responseCode != 200) throw Exception("Token fetch failed: HTTP ${conn.responseCode}")
        val response = conn.inputStream.bufferedReader().readText()
        val tokenMatch = Regex("\"access_token\"\\s*:\\s*\"([^\"]+)\"").find(response)
        val expiryMatch = Regex("\"expires_in\"\\s*:\\s*(\\d+)").find(response)
        val token = tokenMatch?.groupValues?.get(1) ?: throw Exception("No access_token in response")
        val expiresIn = expiryMatch?.groupValues?.get(1)?.toLongOrNull() ?: 3600L
        tokenCache[cacheKey] = Pair(token, System.currentTimeMillis() + (expiresIn - 60) * 1000)
        return token
    }

    // ── HTTP ──────────────────────────────────────────────────────────────

    private fun get(url: String, key: String, secret: String, tokenUrl: String): String {
        val conn = URL(url).openConnection() as HttpURLConnection
        conn.requestMethod = "GET"
        conn.connectTimeout = 10_000
        conn.readTimeout = 15_000
        if (secret.isNotEmpty() && tokenUrl.isNotEmpty()) {
            conn.setRequestProperty("Authorization", "Bearer ${getBearerToken(key, secret, tokenUrl)}")
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

    // ── JSON parsers (new disruptions API) ───────────────────────────────

    private fun parseIncidentsJson(json: String): List<KbIncident> {
        val result = mutableListOf<KbIncident>()
        try {
            val arr = JSONArray(json)
            for (i in 0 until arr.length()) {
                val stationObj = arr.getJSONObject(i)
                val disruptions = stationObj.optJSONArray("disruptions") ?: continue
                for (j in 0 until disruptions.length()) {
                    val d       = disruptions.getJSONObject(j)
                    val id      = d.optString("id")
                    val summary = d.optString("summary")
                    val desc    = stripHtml(d.optString("description"))
                    val planned = d.optBoolean("isPlanned", false)
                    val start   = d.optString("startDateTime")
                    val status  = d.optString("status")
                    if (status.equals("resolved", ignoreCase = true)) continue
                    if (summary.isEmpty()) continue
                    val ops = d.optJSONArray("affectedOperators") ?: JSONArray()
                    val operators = (0 until ops.length()).map {
                        ops.getJSONObject(it).optString("tocCode")
                    }.filter { it.isNotEmpty() }
                    result.add(KbIncident(id, summary, desc, planned, start, "", operators))
                }
            }
        } catch (e: Exception) {
            log.error("parseIncidentsJson error: ${e.message}")
        }
        // Deduplicate by incident ID
        return result.distinctBy { it.id }
    }

    private fun parseNsiJson(json: String): List<KbNsiEntry> {
        val result = mutableListOf<KbNsiEntry>()
        try {
            val arr = JSONArray(json)
            for (i in 0 until arr.length()) {
                val obj               = arr.getJSONObject(i)
                val tocCode           = obj.optString("tocCode")
                val tocName           = obj.optString("tocName")
                val statusText        = obj.optString("tocStatus")
                val statusImage       = obj.optString("tocStatusImage")
                val statusDescription = stripHtml(obj.optString("tocStatusDescription"))
                val twitterHandle     = obj.optString("tocTwitterAccount")
                val additionalInfo    = obj.optString("tocAdditionalInfo")

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

                val groups = obj.optJSONArray("tocServiceGroup") ?: JSONArray()
                val disruptions = (0 until groups.length()).mapNotNull { j ->
                    val g = groups.getJSONObject(j)
                    val url = g.optString("customURL")
                    if (url.isEmpty()) null else NsiDisruption(g.optString("customDetail"), url)
                }

                if (tocCode.isNotEmpty()) {
                    result.add(KbNsiEntry(tocCode, tocName, level.toString(),
                        statusDescription, disruptions, twitterHandle, additionalInfo))
                }
            }
        } catch (e: Exception) {
            log.error("parseNsiJson error: ${e.message}")
        }
        return result
    }

    private fun parseStationMessagesJson(json: String, crs: String): KbStationMessages? {
        return try {
            val arr = JSONArray(json)
            if (arr.length() == 0) return null
            val obj = arr.getJSONObject(0)
            val disruptions = obj.optJSONArray("stationDisruptions") ?: JSONArray()
            val msgs = (0 until disruptions.length()).map { i ->
                val d = disruptions.getJSONObject(i)
                KbStationDisruption(
                    category = d.optString("category"),
                    severity = d.optString("severity"),
                    summary  = stripHtml(d.optString("summary")),
                    description = d.optString("description")
                )
            }
            KbStationMessages(
                crs          = crs,
                disruptions  = msgs,
                stationAlerts = stripHtml(obj.optString("stationAlerts"))
            )
        } catch (e: Exception) {
            log.error("parseStationMessagesJson error: ${e.message}")
            null
        }
    }

    // ── XML parsers (unchanged feeds) ────────────────────────────────────

    private fun parseTocXml(xml: String): List<KbTocEntry> {
        val result = mutableListOf<KbTocEntry>()
        try {
            val cleanXml = xml
            .replace(Regex("<([a-zA-Z0-9_]+):"), "<")
            .replace(Regex("</([a-zA-Z0-9_]+):"), "</")
            .replace(Regex("""xmlns:[a-zA-Z0-9_]+\s*=\s*"[^"]*""""), "")
            .replace(Regex("""xmlns\s*=\s*"[^"]*""""), "")
        val doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
                .parse(InputSource(StringReader(cleanXml)))
            val tocs = doc.getElementsByTagName("TrainOperatingCompany")
            for (i in 0 until tocs.length) {
                val node = tocs.item(i) as? Element ?: continue
                val code    = node.text("AtocCode")
                val name    = node.text("Name")
                val website = node.text("CompanyWebsite")
                val cs      = node.getElementsByTagName("CustomerService")
                val at      = node.getElementsByTagName("AssistedTravel")
                val lp      = node.getElementsByTagName("LostProperty")
                val csPhone = if (cs.length > 0) (cs.item(0) as? Element)?.text("PrimaryTelephoneNumber") ?: "" else ""
                val atPhone = if (at.length > 0) (at.item(0) as? Element)?.text("PrimaryTelephoneNumber") ?: "" else ""
                val atUrl   = if (at.length > 0) (at.item(0) as? Element)?.text("Url") ?: "" else ""
                val lpUrl   = if (lp.length > 0) (lp.item(0) as? Element)?.text("Url") ?: "" else ""
                if (code.isNotEmpty()) result.add(KbTocEntry(code, name, website, csPhone, atPhone, atUrl, lpUrl))
            }
        } catch (e: Exception) {
            log.error("parseTocXml error: ${e.message}")
        }
        return result
    }

    private fun parseStationXml(xml: String): KbStation? {
        return try {
            val cleanXml = xml
                .replace(Regex("<([a-zA-Z0-9_]+):"), "<")
                .replace(Regex("</([a-zA-Z0-9_]+):"), "</")
                .replace(Regex("""xmlns:[a-zA-Z0-9_]+\s*=\s*"[^"]*""""), "")
                .replace(Regex("""\bxmlns\s*=\s*"[^"]*""""), "")
        val doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
                .parse(InputSource(StringReader(cleanXml)))
            val stations = doc.getElementsByTagName("StationV4.0")
            if (stations.length == 0) return null
            val node = stations.item(0) as? Element ?: return null
            val crs  = node.text("CrsCode").uppercase()
            if (crs.length != 3) return null

            // Address — manually walk Address > com:PostalAddress > add:A_5LineAddress
            // to avoid getElementsByTagName leaking into CarPark contact addresses
            val addressLines = mutableListOf<String>()
            var postCode = ""
            val topChildren = node.childNodes
            for (i in 0 until topChildren.length) {
                val addrEl = topChildren.item(i) as? Element ?: continue
                if (addrEl.tagName != "Address") continue
                val postalChildren = addrEl.childNodes
                for (j in 0 until postalChildren.length) {
                    val postalEl = postalChildren.item(j) as? Element ?: continue
                    val fiveLineChildren = postalEl.childNodes
                    for (k in 0 until fiveLineChildren.length) {
                        val fiveLineEl = fiveLineChildren.item(k) as? Element ?: continue
                        val lineChildren = fiveLineEl.childNodes
                        for (l in 0 until lineChildren.length) {
                            val lineEl = lineChildren.item(l) as? Element ?: continue
                            val t = lineEl.textContent?.trim() ?: ""
                            if (t.isEmpty() || t == "-") continue
                            when (lineEl.tagName) {
                                "add:Line", "Line"     -> addressLines.add(t)
                                "add:PostCode", "PostCode" -> postCode = t
                            }
                        }
                    }
                }
                break
            }
            val address = (addressLines + listOf(postCode)).filter { it.isNotEmpty() }.joinToString(", ")

            // Telephone — com: namespace prefix

            // Ticket office — TicketOffice element present with com:Open children
            val ticketOfficeEl = node.getElementsByTagName("TicketOffice").item(0) as? Element
            val ticketOfficeHours = run {
                val el = ticketOfficeEl ?: return@run ""
                // Prefer the human note inside com:Open/com:Annotation/com:Note
                val openEls = el.getElementsByTagName("Open")
                val annot = (if (openEls.length > 0) openEls.item(0) as? Element else null)
                    ?.getElementsByTagName("Note")?.item(0)?.textContent?.trim()
                    ?.replace(Regex("<[^>]+>"), " ")?.replace(Regex("\\s+"), " ")?.trim()
                if (!annot.isNullOrEmpty()) return@run annot
                // Build from DayAndTimeAvailability blocks
                val dayMap = mapOf(
                    "MondayToFriday" to "Mon–Fri", "Monday" to "Mon", "Tuesday" to "Tue",
                    "Wednesday" to "Wed", "Thursday" to "Thu", "Friday" to "Fri",
                    "Saturday" to "Sat", "Sunday" to "Sun",
                    "MondayToSaturday" to "Mon–Sat", "MondayToSunday" to "Mon–Sun"
                )
                // Individual day order for collapsing runs (Mon-Sat, Mon-Fri etc.)
                val dayOrder = listOf("Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday")
                val parts = mutableListOf<String>()
                val avails = el.getElementsByTagName("DayAndTimeAvailability")
                for (j in 0 until avails.length) {
                    val av = avails.item(j) as? Element ?: continue
                    val dayTypes = av.getElementsByTagName("DayTypes").item(0) as? Element ?: continue
                    val period = av.getElementsByTagName("OpenPeriod").item(0) as? Element
                    val twentyFour = av.getElementsByTagName("TwentyFourHours").item(0)?.textContent?.trim() == "true"
                    val start2 = period?.getElementsByTagName("StartTime")?.item(0)?.textContent?.trim()?.take(5) ?: ""
                    val end2   = period?.getElementsByTagName("EndTime")?.item(0)?.textContent?.trim()?.take(5) ?: ""
                    val timeStr = when {
                        twentyFour -> "24hrs"
                        start2.isNotEmpty() && end2.isNotEmpty() -> "$start2–$end2"
                        else -> continue
                    }
                    // Check for combined tags first (MondayToFriday, MondayToSunday etc.)
                    val combinedLabel = dayMap.entries.firstOrNull { (k, _) ->
                        k.contains("To") && dayTypes.getElementsByTagName(k).item(0)?.textContent?.trim() == "true"
                    }?.value
                    if (combinedLabel != null) {
                        parts.add("$combinedLabel $timeStr")
                        continue
                    }
                    // Collect individual days and collapse into ranges
                    val activeDays = dayOrder.filter {
                        dayTypes.getElementsByTagName(it).item(0)?.textContent?.trim() == "true"
                    }
                    if (activeDays.isEmpty()) continue
                    // Collapse consecutive days into ranges e.g. Mon,Tue,Wed,Thu,Fri,Sat -> Mon-Sat
                    val indices = activeDays.map { dayOrder.indexOf(it) }
                    val isConsecutive = indices == (indices.first()..indices.last()).toList()
                    val dayLabel = if (isConsecutive && activeDays.size > 1) {
                        "${dayMap[activeDays.first()]}–${dayMap[activeDays.last()]}"
                    } else {
                        activeDays.mapNotNull { dayMap[it] }.joinToString("/")
                    }
                    parts.add("$dayLabel $timeStr")
                }
                parts.joinToString(", ").ifEmpty { if (openEls.length > 0) "Yes" else "" }
            }

            // SSTM — TicketMachine > Available (no namespace prefix)
            val sstmAvailability = if (node.textOf("Available", "TicketMachine") == "true") "Available" else ""

            // Step-free — Coverage (no prefix)
            val stepFreeAccess = when (node.text("Coverage")) {
                "wholeStation"   -> "Whole station"
                "partialStation" -> "Partial"
                else             -> ""
            }

            // Assistance — StaffHelpAvailable has com:Open children, no Available tag
            val assistanceEl = node.getElementsByTagName("StaffHelpAvailable").item(0) as? Element
            val assistanceAvail = if (assistanceEl != null &&
                assistanceEl.getElementsByTagName("Open").length > 0) "Yes" else ""

            // Helper: get note text from a named element, stripping HTML
            fun Element.fieldNote(tag: String): String {
                val el = getElementsByTagName(tag).item(0) as? Element ?: return ""
                val noteEl = el.getElementsByTagName("Note").item(0) ?: return ""
                val raw = noteEl.textContent?.trim() ?: return ""
                // Extract href URLs from anchor tags before stripping HTML
                val hrefRegex = Regex("""href=["\']([^"\'>]+)["\']""")
                val extractedUrls = hrefRegex.findAll(raw)
                    .map { it.groupValues[1] }
                    .filter { it.startsWith("http") }
                    .distinct().toList()
                // Remove anchor tags but replace with their href text only if meaningful
                val linkPlaceholders = setOf("here", "click here", "this", "this link", "this page", "more", "more information", "further information", "find out more", "visit", "please", "details")
                val withLinksRemoved = raw.replace(Regex("<a\\s[^>]*>([^<]*)</a>")) { m ->
                    val text = m.groupValues[1].trim()
                    if (text.lowercase() in linkPlaceholders || (text.length < 20 && text.all { it.isLetter() || it.isWhitespace() })) "" else " $text "
                }
                val cleaned = withLinksRemoved
                    .replace(Regex("<[^>]+>"), " ")
                    .replace(Regex("\\s+"), " ")
                    .replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
                    .replace("&nbsp;", " ").replace("\u00a0", " ").replace("&#39;", "'").replace("&quot;", "\"")
                    .replace(Regex("https?://\\S+"), "").replace(Regex("(?i)\\s*(More details|More information|Find out more|Further information|Accessibility on|All Transport|click here|visit us|located in the .Maps. section).*$"), "").replace(Regex("[,.]\\s*$"), "").replace(Regex("(?i)^Onward travel map\\s*"), "").replace(Regex("(?i)\\s*(can be found here|BUS JOURNEYS FROM).*$"), "").replace(Regex("\\b(\\w+) \\1\\b"), "$1").replace(Regex("(?i)\\b(to access|access is):?\\s*(Yes|No)\\.?"), "$1").replace(Regex("\\s+"), " ")
                    .trim()
                return cleaned
            }
            // Helper: available flag from a named element
            fun Element.fieldAvail(tag: String): Boolean =
                (getElementsByTagName(tag).item(0) as? Element)
                    ?.getElementsByTagName("Available")?.item(0)
                    ?.textContent?.trim() == "true"
            // Helper: note if present, else "Yes" if available, else ""
            fun Element.noteOrAvail(tag: String): String {
                val n = fieldNote(tag)
                if (n.isNotEmpty()) return n
                return if (fieldAvail(tag)) "Yes" else ""
            }

            val wifi = node.noteOrAvail("WiFi")
            val toilets = node.noteOrAvail("Toilets")
            val waitingRoom = node.fieldNote("WaitingRoom").ifEmpty {
                if (node.getElementsByTagName("WaitingRoom").length > 0) "Yes" else ""
            }
            val cctv = node.noteOrAvail("ClosedCircuitTelevision")
            val taxi = node.fieldNote("TaxiRank").ifEmpty {
                if (node.getElementsByTagName("TaxiRank").length > 0) "Yes" else ""
            }
            val busInterchange = node.fieldNote("OnwardTravel").let { note ->
                if (note.isBlank() || note.equals("Onward travel map", ignoreCase = true) || note.contains("Onward Travel Information Map", ignoreCase = true)) {
                    if (node.getElementsByTagName("OnwardTravel").length > 0) "Yes" else ""
                } else note
            }
            val airport = node.fieldNote("Airport")



            // Cycle storage
            val cycleEl        = node.getElementsByTagName("CycleStorage").item(0) as? Element
            val cycleSpaces    = cycleEl?.getElementsByTagName("Spaces")?.item(0)?.textContent?.trim() ?: ""
            val cycleSheltered = cycleEl?.getElementsByTagName("Sheltered")?.item(0)?.textContent?.trim() ?: ""

            // Accessibility extras
            val inductionLoop = node.fieldNote("InductionLoop").ifEmpty {
                if (node.getElementsByTagName("InductionLoop").item(0)?.textContent?.trim() == "true") "Yes" else ""
            }
            val accessibleTicketMachines = node.noteOrAvail("AccessibleTicketMachines")
            val rampForTrainAccess       = node.noteOrAvail("RampForTrainAccess")
            val wheelchairsAvailable     = node.noteOrAvail("WheelchairsAvailable")
            val nationalKeyToilets       = node.noteOrAvail("NationalKeyToilets")
            val ticketGates              = node.noteOrAvail("TicketGates")
            val stepFreeNote             = node.fieldNote("StepFreeAccess")
            val stationBuffet            = node.noteOrAvail("StationBuffet")
            val showers                  = node.noteOrAvail("Showers")
            val atmMachine               = node.noteOrAvail("AtmMachine").replace(Regex("(?<=[a-z])\\s+(?=Located|On |In |At )"), ", ")
            val firstClassLounge = node.fieldNote("FirstClassLounge")
            val customerHelpPoints       = node.noteOrAvail("CustomerHelpPoints")
            val impairedMobilitySetDown  = node.noteOrAvail("ImpairedMobilitySetDown")

            // Facilities extras
            val babyChange              = node.noteOrAvail("BabyChange")
            val seatedArea              = node.noteOrAvail("SeatedArea")
            val trolleys                = node.noteOrAvail("Trolleys")
            val telephones              = if (node.textOf("Exists", "Telephones") == "true" ||
                node.textOf("com:Exists", "Telephones") == "true") "Yes" else ""
            val postBox                 = node.noteOrAvail("PostBox")
            val shops                   = node.noteOrAvail("Shops")
            val leftLuggage = run {
                val el = node.getElementsByTagName("LeftLuggage").item(0) as? Element ?: return@run ""
                // Explicit false = not available
                val avail = el.getElementsByTagName("com:Available").item(0)?.textContent?.trim()
                if (avail == "false") return@run ""
                // Has note
                val note = node.noteOrAvail("LeftLuggage")
                if (note.isNotEmpty()) return@run note
                // Has com:Open or com:OperatorName = available but no note
                if (el.getElementsByTagName("com:Open").length > 0 ||
                    el.getElementsByTagName("com:OperatorName").length > 0 ||
                    el.getElementsByTagName("OperatorName").length > 0) return@run "Yes"
                ""
            }
            val heightAdjustedCounter   = node.noteOrAvail("HeightAdjustedTicketOfficeCounter")
            val accessibleTaxis         = node.noteOrAvail("AccessibleTaxis")
            val cycleHire               = node.fieldNote("CycleHire")
            // Car parks - aggregate all CarPark elements
            val carParkEls   = node.getElementsByTagName("CarPark")
            var cpAccessible = 0
            var cpTotal      = 0
            val cpNames      = mutableListOf<String>()
            val cpOperators  = mutableSetOf<String>()
            for (ci in 0 until carParkEls.length) {
                val cp   = carParkEls.item(ci) as? Element ?: continue
                cpAccessible += cp.getElementsByTagName("NumberAccessibleSpaces").item(0)?.textContent?.trim()?.toIntOrNull() ?: 0
                cpTotal      += cp.getElementsByTagName("Spaces").item(0)?.textContent?.trim()?.toIntOrNull() ?: 0
                val n    = cp.getElementsByTagName("Name").item(0)?.textContent?.trim() ?: ""
                if (n.isNotEmpty()) cpNames.add(n)
                val op   = cp.getElementsByTagName("OperatorName").item(0)?.textContent?.trim()?.ifEmpty { null }
                           ?: cp.getElementsByTagName("com:OperatorName").item(0)?.textContent?.trim() ?: ""
                if (op.isNotEmpty()) cpOperators.add(op)
            }
            val carParking      = listOf(
                if (cpAccessible > 0) "$cpAccessible accessible spaces" else "",
                if (cpTotal > 0)      "$cpTotal total spaces"           else ""
            ).filter { it.isNotEmpty() }.joinToString(", ")
            val carParkName     = cpNames.firstOrNull() ?: ""
            val carParkOperator = cpOperators.firstOrNull() ?: ""
            val carParkTotal    = if (cpTotal > 0) cpTotal.toString() else ""

            KbStation(
                crs                      = crs,
                name                     = node.text("Name"),
                address                  = address,
                latitude                 = node.text("Latitude"),
                longitude                = node.text("Longitude"),
                stationOperator          = node.text("StationOperator"),
                staffingNote             = when (node.text("StaffingLevel")) {
                    "fullTime"  -> "Full time"
                    "partTime"  -> "Part time"
                    "nightTime" -> "Night time only"
                    "noPermanentStaff" -> "No permanent staff"
                    else        -> node.text("StaffingLevel")
                },
                ticketOfficeHours        = ticketOfficeHours,
                sstmAvailability         = sstmAvailability,
                stepFreeAccess           = stepFreeNote.ifEmpty { stepFreeAccess },
                assistanceAvail          = assistanceAvail,
                inductionLoop            = inductionLoop,
                accessibleTicketMachines = accessibleTicketMachines,
                rampForTrainAccess       = rampForTrainAccess,
                wheelchairsAvailable     = wheelchairsAvailable,
                nationalKeyToilets       = nationalKeyToilets,
                ticketGates              = ticketGates,
                wifi                     = wifi,
                toilets                  = if (toilets == babyChange) "" else toilets,
                waitingRoom              = waitingRoom,
                cctv                     = cctv,
                babyChange               = babyChange,
                seatedArea               = seatedArea,
                trolleys                 = trolleys,
                leftLuggage              = leftLuggage,
                stationBuffet            = stationBuffet,
                showers                  = showers,
                atmMachine               = atmMachine,
                firstClassLounge         = firstClassLounge,
                customerHelpPoints       = customerHelpPoints,
                impairedMobilitySetDown  = impairedMobilitySetDown,
                telephones               = telephones,
                postBox                  = postBox,
                shops                    = shops,
                heightAdjustedCounter    = heightAdjustedCounter,
                accessibleTaxis          = accessibleTaxis,
                cycleHire                = cycleHire,
                taxi                     = taxi,
                busInterchange           = busInterchange,
                airport                  = airport,
                carParking               = carParking,
                carParkName              = carParkName,
                carParkOperator          = carParkOperator,
                carParkTotal             = carParkTotal,
                cycleSpaces              = cycleSpaces,
                cycleSheltered           = cycleSheltered
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
    val latitude: String,
    val longitude: String,
    val stationOperator: String,
    val staffingNote: String,
    val ticketOfficeHours: String,
    val sstmAvailability: String,
    val stepFreeAccess: String,
    val assistanceAvail: String,
    val inductionLoop: String,
    val accessibleTicketMachines: String,
    val rampForTrainAccess: String,
    val wheelchairsAvailable: String,
    val nationalKeyToilets: String,
    val ticketGates: String,
    val wifi: String,
    val toilets: String,
    val waitingRoom: String,
    val cctv: String,
    val babyChange: String,
    val seatedArea: String,
    val trolleys: String,
    val leftLuggage: String,
    val stationBuffet: String,
    val showers: String,
    val atmMachine: String,
    val firstClassLounge: String,
    val customerHelpPoints: String,
    val impairedMobilitySetDown: String,
    val telephones: String,
    val postBox: String,
    val shops: String,
    val heightAdjustedCounter: String,
    val accessibleTaxis: String,
    val cycleHire: String,
    val taxi: String,
    val busInterchange: String,
    val airport: String,
    val carParking: String,
    val carParkName: String,
    val carParkOperator: String,
    val carParkTotal: String,
    val cycleSpaces: String,
    val cycleSheltered: String
)

@Serializable
data class KbStationDisruption(
    val category: String,
    val severity: String,
    val summary: String,
    val description: String
)

@Serializable
data class KbStationMessages(
    val crs: String,
    val disruptions: List<KbStationDisruption>,
    val stationAlerts: String
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

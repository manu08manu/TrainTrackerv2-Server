package com.traintracker.server.routes

import com.traintracker.server.database.placeholders
import com.traintracker.server.database.query
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap

private val rpLog = LoggerFactory.getLogger("RoutePoints")

/**
 * Adds timing points the timetable does not list but TD sees this train pass ("PT" calls, isTdPoint): learned
 * from the last 35 days of td_events for the headcode, on days the uid ran, and placed between the calls the
 * train passes them between. On a day the train ran, each point shows the time TD saw it. They are passes,
 * so the service page lists them only in its detailed view.
 */
object RoutePoints {
    private val LONDON: ZoneId = ZoneId.of("Europe/London")
    private val HM: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm")
    private val HMS: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")
    private const val HISTORY_DAYS: Long = 35L
    private const val MIN_SHARE: Double = 0.6
    private const val MIN_VALID_DAYS: Int = 3
    private const val CACHE_MS: Long = 1800000L
    private const val LEAD_S: Long = 1200L
    private const val TAIL_S: Long = 1800L
    private const val MAX_GAP_S: Long = 1200L
    private val cache = ConcurrentHashMap<String, Pair<Long, List<Learned>>>()

    private class Anchor(val tiploc: String, val absMin: Int, val stanox: String?)
    private class Ev(val stanox: String, val s: Long)
    private class Info(val tiploc: String?, val name: String?)
    private class Learned(val stanox: String, val prevIdx: Int, val medianSec: Long)

    private fun clean(s: String): String = s.filter { it.isLetterOrDigit() }

    /** Cleaned, distinct, non-empty values for an IN (...) list. */
    private fun cleanList(values: Collection<String>): List<String> = values.map { clean(it) }.filter { it.isNotEmpty() }.distinct()

    private fun parseMin(t: String?): Int? {
        if (t == null || t.length < 5 || t[2] != ':') return null
        val h = t.substring(0, 2).toIntOrNull() ?: return null
        val m = t.substring(3, 5).toIntOrNull() ?: return null
        return h * 60 + m
    }

    fun merge(cps: List<CallingPointResponse>, uid: String, svcDate: String, headcode: String): List<CallingPointResponse> {
        if (cps.size < 2) return cps
        val hc = clean(headcode)
        if (hc.length != 4) return cps
        return try {
            val mins = ArrayList<Int>()
            var offset = 0
            var prev = -1
            for (cp in cps) {
                val m = parseMin(cp.effectiveTime())
                if (m == null) {
                    mins.add(if (prev < 0) 0 else prev)
                    continue
                }
                var a = m + offset
                if (prev >= 0 && a < prev) {
                    offset += 1440
                    a = m + offset
                }
                mins.add(a)
                prev = a
            }
            val stanoxByTiploc = stanoxesFor(cps.map { it.tiploc })
            val anchors = cps.mapIndexed { i, cp -> Anchor(cp.tiploc, mins[i], stanoxByTiploc[cp.tiploc]) }
            val key = "$uid|${cps.joinToString(",") { it.tiploc }}"
            val now = System.currentTimeMillis()
            val cached = cache[key]
            val learned = if (cached != null && now - cached.first < CACHE_MS) {
                cached.second
            } else {
                val l = learn(hc, uid, anchors)
                if (cache.size > 2000) cache.clear()
                cache[key] = Pair(now, l)
                l
            }
            if (learned.isEmpty()) return cps
            val stanoxes = learned.map { it.stanox }
            val info = infoFor(stanoxes)
            val dayStart = LocalDate.parse(svcDate).atStartOfDay(LONDON).toEpochSecond()
            val lo = dayStart + mins.first() * 60L - LEAD_S
            val hi = dayStart + mins.last() * 60L + TAIL_S
            val observed = HashMap<String, Long>()
            val reqDate = LocalDate.parse(svcDate)
            val ranOnDate = activationDays(hc, setOf(reqDate))[reqDate]?.contains(uid) == true
            if (ranOnDate) {
                transaction {
                    val sx = cleanList(stanoxes)
                    query(
                        "SELECT stanox, MIN(event_s) FROM td_events WHERE headcode=? AND event_s >= $lo AND event_s <= $hi AND stanox IN (${placeholders(sx.size)}) GROUP BY stanox",
                        listOf(hc) + sx
                    ) { rs ->
                        while (rs.next()) {
                            val sx = rs.getString(1)
                            if (sx != null) observed[sx] = rs.getLong(2)
                        }
                    }
                }
            }
            val byPrev = learned.groupBy { it.prevIdx }
            val out = ArrayList<CallingPointResponse>(cps.size + learned.size)
            for ((i, cp) in cps.withIndex()) {
                out.add(cp)
                if (i == cps.size - 1) break
                val here = byPrev[i] ?: emptyList()
                for (l in here) {
                    val inf = info[l.stanox]
                    val t = observed[l.stanox]
                    val hm = if (t != null) Instant.ofEpochSecond(t).atZone(LONDON).format(HM) else ""
                    val hms = if (t != null) Instant.ofEpochSecond(t).atZone(LONDON).format(HMS) else ""
                    out.add(
                        CallingPointResponse(
                            tiploc = inf?.tiploc?.takeIf { it.isNotEmpty() } ?: l.stanox,
                            crs = null,
                            name = inf?.name?.takeIf { it.isNotEmpty() },
                            scheduledTime = "",
                            platform = null,
                            isPass = true,
                            stopType = "PT",
                            actualTime = hm,
                            actualDeparture = hm,
                            actualDepartureSec = hms,
                            isCancelled = cp.isCancelled,
                            cancelReason = cp.cancelReason,
                            isTdPoint = true
                        )
                    )
                }
            }
            out
        } catch (e: Exception) {
            rpLog.debug("RoutePoints merge failed: ${e.message}")
            cps
        }
    }

    /** TIPLOC -> STANOX (location overrides first, then NLC, then PIF). */
    private fun stanoxesFor(tiplocList: Collection<String>): Map<String, String> {
        val out = HashMap<String, String>()
        val tiplocs = cleanList(tiplocList)
        if (tiplocs.isEmpty()) return out
        transaction {
            val il = placeholders(tiplocs.size)
            query(
                "SELECT tiploc, stanox, 0 AS p FROM location_overrides WHERE tiploc IN ($il) AND stanox IS NOT NULL AND stanox != '' UNION ALL SELECT tiploc, stanox, 2 FROM pif_loc WHERE tiploc IN ($il) AND stanox IS NOT NULL AND stanox != '' UNION ALL SELECT tiploc, stanox, 1 FROM nlc_loc WHERE tiploc IN ($il) AND stanox IS NOT NULL AND stanox != '' ORDER BY p",
                tiplocs + tiplocs + tiplocs
            ) { rs ->
                while (rs.next()) {
                    val tl = rs.getString(1)
                    val sx = rs.getString(2)
                    if (tl != null && sx != null && !out.containsKey(tl)) out[tl] = sx
                }
            }
        }
        return out
    }

    /** STANOX -> its TIPLOC and the best name we have for it. */
    private fun infoFor(stanoxList: Collection<String>): Map<String, Info> {
        val info = HashMap<String, Info>()
        val sx = cleanList(stanoxList)
        if (sx.isEmpty()) return info
        transaction {
            val ils = placeholders(sx.size)
            query(
                "SELECT stanox, tiploc, name FROM (SELECT stanox, tiploc, name, 0 AS p FROM location_overrides WHERE stanox IN ($ils) UNION ALL SELECT stanox, tiploc, name, 1 FROM pif_loc WHERE stanox IN ($ils) UNION ALL SELECT stanox, tiploc, description, 2 FROM nlc_loc WHERE stanox IN ($ils)) ORDER BY p, LENGTH(name), tiploc",
                sx + sx + sx
            ) { rs ->
                while (rs.next()) {
                    val x = rs.getString(1)
                    val tl = rs.getString(2)
                    val nm = rs.getString(3)
                    if (x != null) {
                        val cur = info[x]
                        if (cur == null || cur.name.isNullOrEmpty() && !nm.isNullOrEmpty()) info[x] = Info(tl, nm)
                    }
                }
            }
        }
        return info
    }

    /**
     * Around a call LINX moved to another location (code 06, [CallingPointResponse.replaces]) the timetabled
     * passing points describe the booked route, not the one taken. Once the train has been seen at the new
     * location, the stretch between the calls either side of it shows only where TD or LINX saw the train:
     * timetabled points it was not seen at are dropped (9H08: Heathrow Tunnel Jn), those it was seen at get
     * their own times, and points seen that the timetable lacks are added (9H08: Stockley Jn), in time order.
     */
    fun followChangedLocations(cps: List<CallingPointResponse>, uid: String, svcDate: String, headcode: String): List<CallingPointResponse> {
        val hc = clean(headcode)
        if (hc.length != 4 || cps.none { it.replaces != null }) return cps
        return try {
            var out = cps
            for (tiploc in cps.filter { it.replaces != null }.map { it.tiploc }) {
                out = followChangedLocation(out, out.indexOfFirst { it.tiploc == tiploc }, uid, svcDate, hc)
            }
            out
        } catch (e: Exception) {
            rpLog.debug("RoutePoints followChangedLocations failed: ${e.message}")
            cps
        }
    }

    private fun followChangedLocation(cps: List<CallingPointResponse>, r: Int, uid: String, svcDate: String, hc: String): List<CallingPointResponse> {
        val changed = cps.getOrNull(r) ?: return cps
        val dayStart = LocalDate.parse(svcDate).atStartOfDay(LONDON).toEpochSecond()
        fun secs(t: String?): Long? = parseMin(t)?.let { dayStart + it * 60L }
        val at = secs(changed.actualTime) ?: return cps   // not seen there yet
        val a = (r - 1 downTo 0).firstOrNull { !cps[it].isPass } ?: -1
        val b = (r + 1 until cps.size).firstOrNull { !cps[it].isPass } ?: cps.size
        val lo = cps.getOrNull(a)?.actualTime?.let(::secs) ?: (at - 600)
        val hi = (cps.getOrNull(b)?.actualTime?.let(::secs) ?: (at + 600)) + 59
        if (lo > at || hi < at) return cps   // across midnight: leave as booked

        val stanoxOf = stanoxesFor(cps.map { it.tiploc })
        // Areas this schedule runs through, every booked stop (pass points included). region() is the first two
        // STANOX digits, so signal boxes within an area share a region and a multi-area route has all of them.
        val allowedRegions = cps.mapNotNull { stanoxOf[it.tiploc] }.mapNotNull { region(it) }.toSet()

        // td_events is keyed by headcode only, and headcodes are reused (a second 1K70 ran in Norfolk). As the
        // older merge() did, drop STANOXes that belong to another uid activated under this headcode on this date.
        val reqDate = LocalDate.parse(svcDate)
        val onDay = activationDays(hc, setOf(reqDate))[reqDate] ?: emptySet()
        val otherUidStanoxes = onDay.filter { it != clean(uid) }.flatMap { stanoxesOfUid(it) }.toSet()

        // Where the train was seen in the stretch: TD berth steps and LINX reports, earliest per STANOX.
        val seen = HashMap<String, Long>()
        fun see(stanox: String?, s: Long?) {
            if (stanox.isNullOrEmpty() || s == null || s < lo || s > hi) return
            if (s < (seen[stanox] ?: Long.MAX_VALUE)) seen[stanox] = s
        }
        val tdPairs = ArrayList<Pair<String, Long>>()
        transaction {
            query("SELECT stanox, MIN(event_s) FROM td_events WHERE headcode = ? AND event_s BETWEEN $lo AND $hi GROUP BY stanox", listOf(hc)) { rs ->
                while (rs.next()) {
                    val sx = rs.getString(1) ?: continue
                    tdPairs.add(sx to rs.getLong(2))
                }
            }
            query(
                "SELECT stanox, actual_time FROM movements WHERE uid = ? AND actual_time IS NOT NULL AND event_ts >= ? AND event_ts < ?",
                listOf(clean(uid), svcDate, LocalDate.parse(svcDate).plusDays(2).toString())
            ) { rs ->
                while (rs.next()) see(rs.getString(1), secs(rs.getString(2)))
            }
        }
        // Keep TD points in an area this schedule runs through and not owned by another uid sharing the headcode.
        // If none are in-region, the stretch went off-route (a genuine diversion), so keep what wasn't excluded
        // rather than lose the real path.
        val tdNotOther = tdPairs.filter { (sx, _) -> sx !in otherUidStanoxes }
        val tdInRegion = tdNotOther.filter { (sx, _) -> region(sx) in allowedRegions }
        for ((sx, s) in tdInRegion.ifEmpty { tdNotOther }) see(sx, s)
        fun hm(s: Long) = Instant.ofEpochSecond(s).atZone(LONDON).format(HM)
        fun hms(s: Long) = Instant.ofEpochSecond(s).atZone(LONDON).format(HMS)

        val kept = cps.subList(a + 1, b).mapNotNull { cp ->
            if (cp === changed) return@mapNotNull (stanoxOf[cp.tiploc]?.let { seen[it] } ?: at) to cp
            val s = stanoxOf[cp.tiploc]?.let { seen[it] } ?: return@mapNotNull null
            s to cp.copy(actualTime = hm(s), actualArrival = "", actualDeparture = hm(s), actualDepartureSec = hms(s), isPredicted = false, isCancelled = false, cancelReason = null)
        }
        val known = cps.mapNotNull { stanoxOf[it.tiploc] }.toSet()
        val info = infoFor(seen.keys - known)
        val added = (seen - known).map { (sx, s) ->
            s to CallingPointResponse(
                tiploc = info[sx]?.tiploc?.takeIf { it.isNotEmpty() } ?: sx, crs = null, name = info[sx]?.name?.takeIf { it.isNotEmpty() },
                scheduledTime = "", platform = null, isPass = true, stopType = "PT",
                actualTime = hm(s), actualDeparture = hm(s), actualDepartureSec = hms(s), isTdPoint = true,
            )
        }
        return cps.subList(0, a + 1) + (kept + added).sortedBy { it.first }.map { it.second } + cps.subList(b, cps.size)
    }

    private fun activationDays(hc: String, dates: Set<LocalDate>): Map<LocalDate, Set<String>> {
        val out = HashMap<LocalDate, MutableSet<String>>()
        if (dates.isEmpty()) return out
        transaction {
            query("SELECT train_id, uid, updated_at FROM train_activations WHERE substr(train_id,3,4)=?", listOf(hc)) { rs ->
                while (rs.next()) {
                    val tid = rs.getString(1) ?: continue
                    val u = rs.getString(2) ?: continue
                    if (tid.length < 10) continue
                    val dom = tid.substring(8, 10).toIntOrNull() ?: continue
                    val upd = try {
                        LocalDate.parse(rs.getString(3).take(10))
                    } catch (e: Exception) {
                        null
                    }
                    for (d in dates) {
                        if (d.dayOfMonth == dom && (upd == null || Math.abs(ChronoUnit.DAYS.between(upd, d)) <= 1L)) {
                            out.getOrPut(d) { HashSet() }.add(u)
                        }
                    }
                }
            }
        }
        return out
    }

    private fun region(s: String?): String? = if (s != null && s.length >= 2) s.substring(0, 2) else null

    private fun sameRegion(x: String, a: String?, b: String?): Boolean {
        val rx = region(x) ?: return false
        return rx == region(a) || rx == region(b)
    }

    private fun stanoxesOfUid(u: String): Set<String> {
        val r = HashSet<String>()
        transaction {
            query(
                "SELECT DISTINCT lo.stanox FROM schedules s JOIN (SELECT tiploc, stanox FROM location_overrides UNION ALL SELECT tiploc, stanox FROM pif_loc WHERE tiploc NOT IN (SELECT tiploc FROM location_overrides WHERE stanox IS NOT NULL AND stanox != '') UNION ALL SELECT tiploc, stanox FROM nlc_loc WHERE tiploc NOT IN (SELECT tiploc FROM location_overrides WHERE stanox IS NOT NULL AND stanox != '')) lo ON lo.tiploc = s.tiploc WHERE s.uid=? AND lo.stanox IS NOT NULL AND lo.stanox != ''",
                listOf(clean(u))
            ) { rs ->
                while (rs.next()) {
                    val x = rs.getString(1)
                    if (x != null) r.add(x)
                }
            }
        }
        return r
    }

    private fun learn(hc: String, uid: String, anchors: List<Anchor>): List<Learned> {
        val anchorStanoxes = anchors.mapNotNull { it.stanox }.toSet()
        if (anchorStanoxes.size < 2) return emptyList()
        val from = Instant.now().epochSecond - HISTORY_DAYS * 86400L
        val evs = ArrayList<Ev>()
        transaction {
            query("SELECT stanox, event_s FROM td_events WHERE headcode=? AND event_s >= $from", listOf(hc)) { rs ->
                while (rs.next()) {
                    val sx = rs.getString(1)
                    if (sx != null) evs.add(Ev(sx, rs.getLong(2)))
                }
            }
        }
        if (evs.isEmpty()) return emptyList()
        val dates = evs.map { Instant.ofEpochSecond(it.s).atZone(LONDON).toLocalDate() }.toSet()
        val activated = activationDays(hc, dates)
        val otherCache = HashMap<String, Set<String>>()
        val firstMin = anchors.first().absMin
        val lastMin = anchors.last().absMin
        val covered = anchors.count { it.stanox != null }
        var validDays = 0
        val seen = HashMap<String, HashMap<Int, MutableList<Long>>>()
        for (d in dates) {
            val onDay = activated[d]
            if (onDay == null || uid !in onDay) continue
            val excl = HashSet<String>()
            for (o in onDay) {
                if (o == uid) continue
                excl.addAll(otherCache.getOrPut(o) { stanoxesOfUid(o) })
            }
            val dayStart = d.atStartOfDay(LONDON).toEpochSecond()
            val lo = dayStart + firstMin * 60L - LEAD_S
            val hi = dayStart + lastMin * 60L + TAIL_S
            val first = HashMap<String, Long>()
            for (e in evs) {
                if (e.s >= lo && e.s <= hi) {
                    val cur = first[e.stanox]
                    if (cur == null || e.s < cur) first[e.stanox] = e.s
                }
            }
            val anchorFirst = anchors.map { a ->
                val sx = a.stanox
                if (sx == null) null else first[sx]
            }
            val hits = anchorFirst.count { it != null }
            if (hits < 2 || hits * 2 < covered) continue
            validDays++
            for ((stanox, t) in first) {
                if (stanox !in anchorStanoxes && stanox !in excl) {
                    var prevIdx = -1
                    var prevT = 0L
                    var nextT = -1L
                    var nextIdx = -1
                    for (i in anchors.indices) {
                        val at = anchorFirst[i] ?: continue
                        if (at <= t) {
                            prevIdx = i
                            prevT = at
                        } else if (nextT < 0) {
                            nextT = at
                            nextIdx = i
                        }
                    }
                    if (prevIdx < 0 || nextT < 0 || nextT - prevT > MAX_GAP_S) continue
                    if (!sameRegion(stanox, anchors[prevIdx].stanox, anchors[nextIdx].stanox)) continue
                    seen.getOrPut(stanox) { HashMap() }.getOrPut(prevIdx) { ArrayList() }.add(t - dayStart)
                }
            }
        }
        if (validDays < MIN_VALID_DAYS) return emptyList()
        val out = ArrayList<Learned>()
        for ((stanox, byIdx) in seen) {
            val best = byIdx.entries.maxByOrNull { it.value.size } ?: continue
            if (best.value.size.toDouble() / validDays < MIN_SHARE) continue
            val sorted = best.value.sorted()
            out.add(Learned(stanox, best.key, sorted[sorted.size / 2]))
        }
        return out.sortedWith(compareBy({ it.prevIdx }, { it.medianSec }))
    }
}

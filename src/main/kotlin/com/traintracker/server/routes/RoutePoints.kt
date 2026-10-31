package com.traintracker.server.routes

import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap

private val rpLog = LoggerFactory.getLogger("RoutePoints")

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

    private fun inList(values: Collection<String>): String =
        values.map { clean(it) }.filter { it.isNotEmpty() }.distinct().joinToString(",") { "'$it'" }

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
            val stanoxByTiploc = HashMap<String, String>()
            transaction {
                exec("SELECT tiploc, stanox FROM location_overrides WHERE tiploc IN (${inList(cps.map { it.tiploc })}) AND stanox IS NOT NULL AND stanox != ''") { rs ->
                    while (rs.next()) {
                        val tl = rs.getString(1)
                        val sx = rs.getString(2)
                        if (tl != null && sx != null && !stanoxByTiploc.containsKey(tl)) stanoxByTiploc[tl] = sx
                    }
                }
            }
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
            val info = HashMap<String, Info>()
            transaction {
                exec("SELECT stanox, tiploc, name FROM location_overrides WHERE stanox IN (${inList(stanoxes)})") { rs ->
                    while (rs.next()) {
                        val sx = rs.getString(1)
                        val tl = rs.getString(2)
                        val nm = rs.getString(3)
                        if (sx != null) {
                            val cur = info[sx]
                            if (cur == null || cur.name.isNullOrEmpty() && !nm.isNullOrEmpty()) info[sx] = Info(tl, nm)
                        }
                    }
                }
            }
            val dayStart = LocalDate.parse(svcDate).atStartOfDay(LONDON).toEpochSecond()
            val lo = dayStart + mins.first() * 60L - LEAD_S
            val hi = dayStart + mins.last() * 60L + TAIL_S
            val observed = HashMap<String, Long>()
            val reqDate = LocalDate.parse(svcDate)
            val ranOnDate = activationDays(hc, setOf(reqDate))[reqDate]?.contains(uid) == true
            if (ranOnDate) {
                transaction {
                    exec("SELECT stanox, MIN(event_s) FROM td_events WHERE headcode='$hc' AND event_s >= $lo AND event_s <= $hi AND stanox IN (${inList(stanoxes)}) GROUP BY stanox") { rs ->
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
                            isPass = false,
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

    private fun activationDays(hc: String, dates: Set<LocalDate>): Map<LocalDate, Set<String>> {
        val out = HashMap<LocalDate, MutableSet<String>>()
        if (dates.isEmpty()) return out
        transaction {
            exec("SELECT train_id, uid, updated_at FROM train_activations WHERE substr(train_id,3,4)='$hc'") { rs ->
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
            exec("SELECT DISTINCT lo.stanox FROM schedules s JOIN location_overrides lo ON lo.tiploc = s.tiploc WHERE s.uid='${clean(u)}' AND lo.stanox IS NOT NULL AND lo.stanox != ''") { rs ->
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
            exec("SELECT stanox, event_s FROM td_events WHERE headcode='$hc' AND event_s >= $from") { rs ->
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

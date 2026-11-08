package com.traintracker.server.rdm

import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.GZIPInputStream

// ---------------------------------------------------------------------------
// PIF (Planning Interface File) parser.
//
// One streaming pass over the file. Each line is routed on its first tab-separated
// field (the 3-character record type) to a registered PifRecordHandler. Types with
// no handler are only counted, so adding ACC / TOC / PLT / NWK / TLK later means
// writing one handler class and registering it - no change to the parser.
//
// Dry run (no DB writes):
//   java -cp /opt/traintracker/traintracker.jar com.traintracker.server.rdm.PifParserKt [file]
// ---------------------------------------------------------------------------

private val PIF_DT: DateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")
private val STANOX_RE = Regex("^\\d{1,5}$")

private fun parseDt(s: String): LocalDateTime? {
    val t = s.trim()
    if (t.isEmpty()) return null
    return try { LocalDateTime.parse(t, PIF_DT) } catch (e: Exception) { null }
}

fun openPifText(file: File): BufferedReader {
    val raw = file.inputStream().buffered(1 shl 16)
    val stream = if (file.name.endsWith(".gz", ignoreCase = true)) GZIPInputStream(raw, 1 shl 16) else raw
    return BufferedReader(InputStreamReader(stream, Charsets.UTF_8), 1 shl 16)
}

// ---- Control (PIF) record ---------------------------------------------------

data class PifHeader(
    val centre: String,
    val owner: String,
    val validFrom: LocalDateTime,
    val validTo: LocalDateTime,
    val createdAt: LocalDateTime?
) {
    fun covers(day: LocalDate): Boolean =
        !day.isBefore(validFrom.toLocalDate()) && !day.isAfter(validTo.toLocalDate())
}

// PIF <ver> <centre> <owner> <from> <to> I D <created> <n>
fun parsePifHeader(line: String): PifHeader? {
    val f = line.split('\t')
    if (f.size < 9 || f[0] != "PIF") return null
    val from = parseDt(f[4]) ?: return null
    val to = parseDt(f[5]) ?: return null
    return PifHeader(f[2].trim(), f[3].trim(), from, to, parseDt(f[8]))
}

// ---- Handler contract -----------------------------------------------------

interface PifRecordHandler {
    /** 3-character record type, or "REF/XXX" for one REF subtype (ACC, TOC, ZNE ...). */
    val type: String
    fun handle(fields: List<String>)
}

/** Keeps every row of a record type untouched - a ready-made handler for types with no typed class yet. */
class RawHandler(override val type: String) : PifRecordHandler {
    val rows = ArrayList<List<String>>()
    override fun handle(fields: List<String>) { rows.add(fields) }
}

// ---- LOC (typed) --------------------------------------------------------

data class PifLoc(
    val action: String,
    val tiploc: String,
    val name: String,
    val startDate: LocalDate,
    val endDate: LocalDate?,
    val easting: Int?,
    val northing: Int?,
    val timingPointType: String,
    val zone: String,
    val stanox: String?,          // 5 digits, zero padded; null when blank/invalid
    val offNetwork: Boolean,
    val forceLpb: String
)

class LocHandler : PifRecordHandler {
    override val type = "LOC"
    val records = ArrayList<PifLoc>(13_000)
    var malformed = 0; private set
    var badStanox = 0; private set

    // LOC <action> <tiploc> <name> <start> <end> <E> <N> <type> <zone> <stanox> <offnet> <lpb>
    override fun handle(fields: List<String>) {
        if (fields.size < 12) { malformed++; return }
        val action = fields[1].trim()
        val tiploc = fields[2].trim()
        val start = parseDt(fields[4])?.toLocalDate()
        if (tiploc.isEmpty() || start == null) { malformed++; return }
        val rawStanox = fields[10].trim()
        val stanox = when {
            rawStanox.isEmpty() -> null
            STANOX_RE.matches(rawStanox) -> rawStanox.padStart(5, '0')
            else -> { badStanox++; null }
        }
        records.add(
            PifLoc(
                action = action,
                tiploc = tiploc,
                name = fields[3].trim(),
                startDate = start,
                endDate = parseDt(fields[5])?.toLocalDate(),
                easting = fields[6].trim().toIntOrNull(),
                northing = fields[7].trim().toIntOrNull(),
                timingPointType = fields[8].trim(),
                zone = fields[9].trim(),
                stanox = stanox,
                offNetwork = fields[11].trim().equals("Y", ignoreCase = true),
                forceLpb = fields.getOrNull(12)?.trim().orEmpty()
            )
        )
    }

    /**
     * One record per TIPLOC (action is always A in the feed). Expired records are dropped. Among the rest, a record that has
     * already started beats one that has not; two live records -> latest start wins; two
     * upcoming records -> soonest start wins.
     */
    fun currentByTiploc(today: LocalDate = LocalDate.now()): Map<String, PifLoc> {
        val best = HashMap<String, PifLoc>(records.size)
        for (r in records) {
            val end = r.endDate
            if (end != null && end.isBefore(today)) continue
            val cur = best[r.tiploc]
            if (cur == null || better(r, cur, today)) best[r.tiploc] = r
        }
        return best
    }

    private fun better(a: PifLoc, b: PifLoc, today: LocalDate): Boolean {
        val aLive = !a.startDate.isAfter(today)
        val bLive = !b.startDate.isAfter(today)
        return when {
            aLive && !bLive -> true
            !aLive && bLive -> false
            aLive -> a.startDate.isAfter(b.startDate)
            else -> a.startDate.isBefore(b.startDate)
        }
    }
}

// ---- Parser ---------------------------------------------------------------

data class PifParseResult(
    val header: PifHeader?,
    val lineCounts: Map<String, Int>,
    val footerCounts: Map<String, Int>
) {
    /** Footer (PIT) carries the record count of every type; any mismatch means a truncated/partial file. */
    fun mismatches(): List<String> = footerCounts.mapNotNull { (t, expected) ->
        val actual = lineCounts[t] ?: 0
        if (actual != expected) "$t: footer=$expected parsed=$actual" else null
    }
    fun isComplete(): Boolean = header != null && footerCounts.isNotEmpty() && mismatches().isEmpty()
}

object PifParser {
    fun parse(file: File, handlers: List<PifRecordHandler>): PifParseResult {
        val byType = handlers.associateBy { it.type }
        val counts = LinkedHashMap<String, Int>()
        var header: PifHeader? = null
        var footer: Map<String, Int> = emptyMap()
        openPifText(file).use { r ->
            var line = r.readLine()
            while (line != null) {
                val tab = line.indexOf('\t')
                val type = if (tab < 0) line.trim() else line.substring(0, tab)
                counts.merge(type, 1, Int::plus)
                when (type) {
                    "PIF" -> header = parsePifHeader(line)
                    "PIT" -> footer = parseFooter(line)
                    "REF" -> {
                        // REF <action> <subtype> ... : subtype handlers are registered as "REF/TOC" etc.
                        val fields = line.split('\t')
                        val sub = "REF/" + fields.getOrNull(2)?.trim().orEmpty()
                        counts.merge(sub, 1, Int::plus)
                        byType["REF"]?.handle(fields)
                        byType[sub]?.handle(fields)
                    }
                    else -> byType[type]?.handle(line.split('\t'))
                }
                line = r.readLine()
            }
        }
        return PifParseResult(header, counts, footer)
    }

    // PIT REF 1434 0 0 TLD 458 0 0 LOC 12128 0 0 ...  -> groups of 4: type, count, 0, 0
    private fun parseFooter(line: String): Map<String, Int> {
        val f = line.split('\t').drop(1).map { it.trim() }
        val out = LinkedHashMap<String, Int>()
        var i = 0
        while (i + 1 < f.size) {
            val n = f[i + 1].toIntOrNull()
            if (f[i].length == 3 && n != null) out[f[i]] = n
            i += 4
        }
        return out
    }
}

// ---- File selection ---------------------------------------------------------

object PifFiles {
    private val NAME = Regex("^PIF.*\\.txt(\\.gz)?$", RegexOption.IGNORE_CASE)

    fun readHeader(file: File): PifHeader? = try {
        openPifText(file).use { it.readLine()?.let(::parsePifHeader) }
    } catch (e: Exception) { null }

    /**
     * RDM delivers several PIFs at once, each covering a different timetable period, so
     * "newest file" is the WRONG pick. Choose the file whose validity window covers today
     * (ties -> latest created). If none covers today, the soonest upcoming period, else the
     * one that ended most recently.
     */
    fun selectFor(dir: File, today: LocalDate = LocalDate.now()): File? =
        selectFrom(dir.listFiles().orEmpty().toList(), today)

    fun selectFrom(files: List<File>, today: LocalDate = LocalDate.now()): File? {
        val candidates = files
            .filter { it.isFile && NAME.matches(it.name) }
            .mapNotNull { f -> readHeader(f)?.let { f to it } }
        if (candidates.isEmpty()) return null

        candidates.filter { it.second.covers(today) }
            .maxWithOrNull(compareBy<Pair<File, PifHeader>>({ it.second.createdAt }, { it.first.name }))
            ?.let { return it.first }

        candidates.filter { it.second.validFrom.toLocalDate().isAfter(today) }
            .minByOrNull { it.second.validFrom }
            ?.let { return it.first }

        return candidates.maxByOrNull { it.second.validTo }?.first
    }
}

// ---- Dry run ----------------------------------------------------------------

fun main(args: Array<String>) {
    val file = if (args.isNotEmpty()) File(args[0]) else PifFiles.selectFor(File("/srv/rdm/in"))
    if (file == null) { println("No PIF file found"); return }
    val loc = LocHandler()
    val t0 = System.currentTimeMillis()
    val res = PifParser.parse(file, listOf(loc))
    println("file      : ${file.name}  (${System.currentTimeMillis() - t0} ms)")
    println("header    : ${res.header}")
    println("counts    : ${res.lineCounts}")
    println("complete  : ${res.isComplete()}  ${res.mismatches()}")
    val cur = loc.currentByTiploc()
    val withStanox = cur.values.count { it.stanox != null }
    val shared = cur.values.filter { it.stanox != null }.groupBy { it.stanox }.count { it.value.size > 1 }
    println("LOC       : parsed=${loc.records.size} current=${cur.size} withStanox=$withStanox " +
            "stanoxShared=$shared malformed=${loc.malformed} badStanox=${loc.badStanox}")
    for (t in listOf("ABDARE", "ABDARAR", "ABDAPEN", "ABARASQ", "ABDO", "AACHEN")) {
        println("  $t -> ${cur[t]?.let { "${it.name} | stanox=${it.stanox} | off=${it.offNetwork}" }}")
    }
}

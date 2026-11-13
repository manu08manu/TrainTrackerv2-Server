package com.traintracker.server.cif

import com.traintracker.server.Config
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.database.sqlQuote
import com.traintracker.server.rdm.LocHandler
import com.traintracker.server.rdm.NlcEntry
import com.traintracker.server.rdm.NlcParser
import com.traintracker.server.rdm.PifFiles
import com.traintracker.server.rdm.PifLoc
import com.traintracker.server.rdm.PifParser
import com.traintracker.server.rdm.RdmFeed
import com.traintracker.server.rdm.RdmFiles
import com.traintracker.server.rdm.RdmWatcher
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("LocationData")

/**
 * Location reference data. Where each value comes from:
 *   - which TIPLOCs are current ........ the CIF feed (cif_cache.gz) - the ONLY thing taken from CIF
 *   - name and STANOX .................. PIF / BPLAN  LOC records
 *   - CRS .............................. NLC / CORPUS xml
 * The three arrive independently (CIF on its own schedule, PIF and NLC via the RDM watcher), so
 * nothing is applied to CorpusLookup until all three have loaded in this process; until then the
 * previously saved maps stay in use. Load order does not matter.
 *
 * Both RDM files are also written, in full, to tables (pif_loc, nlc_loc); RoutePoints reads them.
 * Manual overrides (location_overrides) are untouched and still win inside CorpusLookup.
 */
object LocationData {
    /** Only keep TIPLOCs that are in the current CIF. Set false to use every PIF/NLC TIPLOC. */
    private const val FILTER_BY_CIF = true

    /**
     * STANOX source rule. NLC (CORPUS) is on top: where NLC and PIF disagree the NLC value is used, and where PIF
     * has none but NLC does, NLC fills the gap. PIF supplies the STANOX wherever NLC has none. Every disagreement
     * is logged. Set false to make PIF win a disagreement instead (PIF still fills where it is the only source).
     * Reason: PIF carries some wrong values, e.g. CARR333 has 86212 where CORPUS and CIF both say 01173.
     */
    private const val NLC_STANOX_WINS = true

    /** LOCATION_DATA_SHADOW=1: build everything and log the comparison with the saved maps, apply nothing. */
    private val SHADOW = System.getenv("LOCATION_DATA_SHADOW") == "1"

    private class Built(
        val tiplocToCrs: Map<String, String>,
        val stanoxToCrs: Map<String, String>,
        val stanoxToTiploc: Map<String, String>,
        val names: Map<String, String>
    )

    /** The nightly CIF download. Its TiplocV1 records are the "current TIPLOC" filter. */
    private val CIF_CACHE = Config.CIF_CACHE
    private val TIPLOC_CODE = Regex("\"tiploc_code\"\\s*:\\s*\"([^\"]*)\"")

    private val lock = Any()
    @Volatile private var pif: Map<String, PifLoc> = emptyMap()
    @Volatile private var nlc: Map<String, NlcEntry> = emptyMap()
    @Volatile private var cifTiplocs: Set<String>? = null

    /** Registers the two feeds only. Call before SmartLookup.start, which starts the shared watcher loop. */
    fun register() {
        RdmWatcher.register(
            RdmFeed(
                "pif-loc",
                RdmFiles.ofType("PIF.txt.gz"),
                pick = { files -> PifFiles.selectFrom(files) }
            ) { file, _ -> loadPif(file) }
        )
        RdmWatcher.register(
            RdmFeed("nlc", RdmFiles.ofType("NLC.xml.gz")) { file, _ -> loadNlc(file) }
        )
    }

    // ---- entry points ------------------------------------------------------

    /** Called by CifParser after every CIF parse: the set of TIPLOCs present in the current CIF. */
    fun onCif(currentTiplocs: Set<String>) {
        synchronized(lock) {
            cifTiplocs = currentTiplocs
            applyIfReady()
        }
    }

    /** PIF/NLC STANOX for a TIPLOC. NLC wins, PIF fills the gap (matches build()). */
    fun stanoxForTiploc(tiploc: String): String? {
        val n = cleanStanox(nlc[tiploc]?.stanox)
        val p = cleanStanox(pif[tiploc]?.stanox)
        return n ?: p
    }

    private fun onSourceLoaded() {
        synchronized(lock) {
            // The CIF import is skipped on restart when the cache is already up to date, so onCif would not
            // be called until the next nightly run. Read the filter from the cached file instead.
            if (cifTiplocs == null && FILTER_BY_CIF) loadCifTiplocsFromCache()
            applyIfReady()
        }
    }

    private fun loadCifTiplocsFromCache() {
        try {
            if (!CIF_CACHE.isFile) {
                log.warn("LocationData: ${CIF_CACHE.path} not found, waiting for the CIF import")
                return
            }
            val t0 = System.currentTimeMillis()
            val set = readCifTiplocs(CIF_CACHE)
            if (set.isEmpty()) {
                log.warn("LocationData: no TiplocV1 records found in ${CIF_CACHE.name}, waiting for the CIF import")
                return
            }
            cifTiplocs = set
            log.info("LocationData: ${set.size} current TIPLOCs read from ${CIF_CACHE.name} (${System.currentTimeMillis() - t0} ms)")
        } catch (e: Exception) {
            // e.g. the file is mid-download: wait for the nightly import to call onCif instead
            log.warn("LocationData: could not read ${CIF_CACHE.name}: ${e.message}")
        }
    }

    /** TiplocV1 records come before the schedules in the CIF JSON, so stop at the first schedule. */
    private fun readCifTiplocs(file: File): Set<String> {
        val out = HashSet<String>(16_000)
        GZIPInputStream(file.inputStream().buffered(1 shl 16), 1 shl 16).use { gz ->
            BufferedReader(InputStreamReader(gz, Charsets.UTF_8), 1 shl 16).use { r ->
                var line = r.readLine()
                while (line != null) {
                    val t = line.trimStart()
                    if (t.startsWith("{\"TiplocV1\"")) {
                        TIPLOC_CODE.find(t)?.groupValues?.get(1)?.trim()?.takeIf { it.isNotEmpty() }?.let { out.add(it) }
                    } else if (t.startsWith("{\"JsonScheduleV1\"")) {
                        break
                    }
                    line = r.readLine()
                }
            }
        }
        return out
    }

    private fun loadPif(file: File) {
        val loc = LocHandler()
        val res = PifParser.parse(file, listOf(loc))
        if (!res.isComplete()) error("PIF ${file.name} failed its footer check: ${res.mismatches()}")
        val current = loc.currentByTiploc()
        if (current.isEmpty()) error("PIF ${file.name} has no current LOC records")
        persistPif(file.name, current.values)
        synchronized(lock) { pif = current }
        log.info("PIF loaded: ${file.name} ${current.size} TIPLOCs, ${current.values.count { it.stanox != null }} with STANOX (${res.header?.validFrom?.toLocalDate()}..${res.header?.validTo?.toLocalDate()})")
        onSourceLoaded()
    }

    private fun loadNlc(file: File) {
        val entries = NlcParser.parse(file)
        if (entries.isEmpty()) error("NLC ${file.name} parsed to zero entries")
        val byTiploc = NlcParser.byTiploc(entries)
        persistNlc(file.name, byTiploc.values)
        synchronized(lock) { nlc = byTiploc }
        log.info("NLC loaded: ${file.name} ${byTiploc.size} TIPLOCs, ${byTiploc.values.count { it.crs.isNotEmpty() }} with CRS")
        onSourceLoaded()
    }

    // ---- combining -----------------------------------------------------------

    private fun applyIfReady() {
        val p = pif
        val n = nlc
        if (p.isEmpty() || n.isEmpty()) return
        val current = cifTiplocs
        // An empty CIF set means the CIF parse found nothing - never treat that as "everything expired".
        if (FILTER_BY_CIF && (current == null || current.isEmpty())) return
        val b = build(p, n, if (FILTER_BY_CIF) current else null)
        logDiff(b)
        if (SHADOW) {
            log.info("LocationData: SHADOW mode, nothing applied")
            return
        }
        CorpusLookup.mergeFromFeed(b.tiplocToCrs)
        CorpusLookup.mergeStanoxFromFeed(b.stanoxToCrs)
        AppDatabase.saveStanoxMap(b.stanoxToCrs)
        CorpusLookup.mergeStanoxToTiplocFromFeed(b.stanoxToTiploc)
        AppDatabase.saveStanoxTiplocMap(b.stanoxToTiploc)
        CorpusLookup.mergeCifNamesFromFeed(b.names)
    }

    private fun build(pif: Map<String, PifLoc>, nlc: Map<String, NlcEntry>, onlyThese: Set<String>?): Built {
        val universe: List<String> = (onlyThese ?: (pif.keys + nlc.keys)).sorted()
        val tiplocToCrs = HashMap<String, String>(universe.size * 2)
        val names = HashMap<String, String>(universe.size * 2)
        val stanoxTiplocs = HashMap<String, MutableList<String>>()
        val stanoxCrs = HashMap<String, MutableList<Pair<String, String>>>()   // STANOX -> (crs, tiploc)
        var stanoxConflicts = 0
        var stanoxNlcOnly = 0
        val conflictSamples = ArrayList<String>()
        for (t in universe) {
            val crs = nlc[t]?.crs?.takeIf { it.isNotEmpty() }
            if (crs != null) tiplocToCrs[t] = crs
            val p = pif[t]
            val name = p?.name
            if (!name.isNullOrEmpty()) names[t] = name
            val pifStanox = p?.stanox
            val nlcStanox = cleanStanox(nlc[t]?.stanox)
            val stanox = when {
                nlcStanox == null -> pifStanox
                pifStanox == null -> { stanoxNlcOnly++; nlcStanox }
                pifStanox == nlcStanox -> pifStanox
                else -> {
                    stanoxConflicts++
                    val use = if (NLC_STANOX_WINS) nlcStanox else pifStanox
                    conflictSamples.add("$t pif=$pifStanox nlc=$nlcStanox using=$use")
                    use
                }
            }
            if (stanox != null) {
                stanoxTiplocs.getOrPut(stanox) { ArrayList() }.add(t)
                if (crs != null) {
                    val list = stanoxCrs.getOrPut(stanox) { ArrayList() }
                    list.add(crs to t)
                }
            }
        }
        // A STANOX shared by several TIPLOCs is ambiguous for STANOX->TIPLOC and is left out.
        val stanoxToTiploc = HashMap<String, String>()
        var ambiguous = 0
        for ((sx, list) in stanoxTiplocs) if (list.size == 1) stanoxToTiploc[sx] = list[0] else ambiguous++
        // Preserve every STANOX/CRS/TIPLOC relationship.
        // Only create a single STANOX->CRS mapping when all associated TIPLOCs
        // agree on the CRS. Never select an arbitrary candidate.
        val stanoxToCrs = HashMap<String, String>()
        var crsAmbiguous = 0
        val crsAmbiguousSamples = ArrayList<String>()
        for ((sx, list) in stanoxCrs) {
            val distinctCrs = list.map { it.first }.distinct()

            if (distinctCrs.size == 1) {
                stanoxToCrs[sx] = distinctCrs.single()
            } else {
                crsAmbiguous++
                crsAmbiguousSamples.add("$sx candidates=" + list.joinToString(",") { "${it.first}(${it.second})" })
            }
        }

        crsAmbiguousSamples.sorted().take(50).forEach {
            log.info("LocationData STANOX->CRS ambiguous; preserving TIPLOC-specific relationships: $it")
        }
        conflictSamples.take(30).forEach { log.info("LocationData STANOX conflict $it") }
        val dropped = if (onlyThese != null) pif.keys.count { it !in onlyThese } else 0
        log.info(
            "LocationData built: universe=${universe.size} tiploc->crs=${tiplocToCrs.size} stanox->crs=${stanoxToCrs.size} " +
                "stanox->tiploc=${stanoxToTiploc.size} names=${names.size} ambiguousStanox=$ambiguous " +
                "stanoxCrsAmbiguous=$crsAmbiguous stanoxPifNlcConflicts=$stanoxConflicts stanoxFromNlcOnly=$stanoxNlcOnly " +
                "pifTiplocsNotInCif=$dropped/${pif.size}"
        )
        return Built(tiplocToCrs, stanoxToCrs, stanoxToTiploc, names)
    }

    /** 1-5 digit numeric STANOX, zero padded to 5; anything else (blank, junk) is treated as absent. */
    private fun cleanStanox(raw: String?): String? {
        val t = raw?.trim().orEmpty()
        return if (t.isNotEmpty() && t.length <= 5 && t.all { it.isDigit() }) t.padStart(5, '0') else null
    }

    /** Compares the new STANOX maps with the ones saved by the previous run (log only). */
    private fun logDiff(b: Built) {
        fun diff(label: String, fresh: Map<String, String>, saved: Map<String, String>) {
            var same = 0; var changed = 0; var onlyNew = 0
            val samples = ArrayList<String>()
            for ((k, v) in fresh) {
                val o = saved[k]
                when {
                    o == null -> onlyNew++
                    o == v -> same++
                    else -> { changed++; if (samples.size < 10) samples.add("$k: saved=$o new=$v") }
                }
            }
            val onlySaved = saved.keys.count { it !in fresh }
            log.info("LocationData diff $label: same=$same changed=$changed onlyNew=$onlyNew onlySaved=$onlySaved")
            samples.forEach { log.info("LocationData diff $label sample $it") }
        }
        try {
            diff("stanox->crs", b.stanoxToCrs, AppDatabase.loadStanoxMap())
            diff("stanox->tiploc", b.stanoxToTiploc, AppDatabase.loadStanoxTiplocMap())
        } catch (e: Exception) {
            log.warn("LocationData diff skipped: ${e.message}")
        }
    }

    // ---- tables --------------------------------------------------------------

    private fun q(s: String): String = sqlQuote(s)
    private fun qn(s: String?): String = if (s == null) "NULL" else q(s)
    private fun n(v: Int?): String = v?.toString() ?: "NULL"

    /** Replaces one reference table in a transaction, inserting sorted rows in bounded batches. */
    private fun <T> replaceReferenceTable(
        table: String,
        rows: Collection<T>,
        insertPrefix: String,
        rowValues: (T) -> String,
    ) {
        try {
            transaction {
                exec("DELETE FROM $table")
                for (chunk in rows.chunked(400)) {
                    val values = chunk.joinToString(",") { rowValues(it) }
                    exec("$insertPrefix$values")
                }
            }
        } catch (e: Exception) {
            log.warn("$table table write failed (in-memory data still used): ${e.message}")
        }
    }

    /** Full replace in one transaction: a failed load leaves the previous contents in place. */
    private fun persistPif(source: String, rows: Collection<PifLoc>) =
        replaceReferenceTable(
            "pif_loc",
            rows.sortedBy { it.tiploc },
            "INSERT INTO pif_loc (tiploc, name, start_date, end_date, os_easting, os_northing, timing_point_type, zone, stanox, off_network, force_lpb, action, source_file) VALUES ",
        ) { r ->
            "(${q(r.tiploc)},${q(r.name)},${q(r.startDate.toString())},${qn(r.endDate?.toString())},${n(r.easting)},${n(r.northing)}," +
                "${q(r.timingPointType)},${q(r.zone)},${qn(r.stanox)},${if (r.offNetwork) 1 else 0},${q(r.forceLpb)},${q(r.action)},${q(source)})"
        }

    private fun persistNlc(source: String, rows: Collection<NlcEntry>) =
        replaceReferenceTable(
            "nlc_loc",
            rows.sortedBy { it.tiploc },
            "INSERT INTO nlc_loc (tiploc, nlc, capts_ident, check_char, description, capri_desc, crs, tiploc_owner, uic, stanox, po_mcp, source_file) VALUES ",
        ) { r ->
            "(${q(r.tiploc)},${q(r.nlc)},${q(r.captsIdent)},${q(r.checkChar)},${q(r.description)},${q(r.capriDesc)},${q(r.crs)}," +
                "${q(r.tiplocOwner)},${q(r.euroLocation)},${q(r.stanox)},${q(r.poMcp)},${q(source)})"
        }
}

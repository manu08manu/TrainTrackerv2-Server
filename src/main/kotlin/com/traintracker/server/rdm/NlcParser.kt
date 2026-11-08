package com.traintracker.server.rdm

import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.time.LocalDate
import java.util.zip.GZIPInputStream
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants

// ---------------------------------------------------------------------------
// NLC (CORPUS) parser. The RDM file is a namespaced XML document:
//   <ns2:TSDBData><ns2:TSDBDataItem><ns2:NLCTiplocCode>..</ns2:NLCTiplocCode> ... </ns2:TSDBDataItem>...
// Elements are matched on their local name, so the namespace prefix does not matter.
// Optional elements (NLCCrsCode, NLCStanoxCode) are simply absent on some items, and unused
// ones hold a single space - both come out as "".
//
// Dry run (no DB writes, PIF + NLC together):
//   java -cp build/libs/traintracker-backend-2.0.0.jar com.traintracker.server.rdm.LocationDryRun
// ---------------------------------------------------------------------------

data class NlcEntry(
    val tiploc: String,        // NLCTiplocCode
    val captsIdent: String,    // NLCCapts-Ident
    val nlc: String,           // NLCLocationCode (national location code, unique key upstream)
    val checkChar: String,     // NLCCheckChar
    val description: String,   // NLCDescription
    val capriDesc: String,     // NLCCapriDesc
    val crs: String,           // NLCCrsCode - 3 letters, or "" when absent/invalid
    val tiplocOwner: String,   // NLCTiplocOwner
    val euroLocation: String,  // NLCEuroLocation (UIC)
    val stanox: String,        // NLCStanoxCode
    val poMcp: String          // NLCPoMcpCode
)

object NlcParser {
    private fun openReader(file: File): BufferedReader {
        val raw = file.inputStream().buffered(1 shl 16)
        val stream = if (file.name.endsWith(".gz", ignoreCase = true)) GZIPInputStream(raw, 1 shl 16) else raw
        return BufferedReader(InputStreamReader(stream, Charsets.UTF_8), 1 shl 16)
    }

    fun parse(file: File): List<NlcEntry> {
        val factory = XMLInputFactory.newFactory()
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
        val out = ArrayList<NlcEntry>(13_000)
        openReader(file).use { reader ->
            val xr = factory.createXMLStreamReader(reader)
            try {
                var current: HashMap<String, String>? = null
                while (xr.hasNext()) {
                    when (xr.next()) {
                        XMLStreamConstants.START_ELEMENT -> {
                            val name = xr.localName
                            val item = current
                            if (name == "TSDBDataItem") current = HashMap()
                            else if (item != null) item[name] = xr.elementText.trim()
                        }
                        XMLStreamConstants.END_ELEMENT -> if (xr.localName == "TSDBDataItem") {
                            current?.let { toEntry(it) }?.let { out.add(it) }
                            current = null
                        }
                    }
                }
            } finally {
                xr.close()
            }
        }
        return out
    }

    private fun toEntry(m: Map<String, String>): NlcEntry? {
        val tiploc = m["NLCTiplocCode"].orEmpty()
        if (tiploc.isEmpty()) return null
        val crs = m["NLCCrsCode"].orEmpty().uppercase().takeIf { it.length == 3 } ?: ""
        return NlcEntry(
            tiploc = tiploc,
            captsIdent = m["NLCCapts-Ident"].orEmpty(),
            nlc = m["NLCLocationCode"].orEmpty(),
            checkChar = m["NLCCheckChar"].orEmpty(),
            description = m["NLCDescription"].orEmpty(),
            capriDesc = m["NLCCapriDesc"].orEmpty(),
            crs = crs,
            tiplocOwner = m["NLCTiplocOwner"].orEmpty(),
            euroLocation = m["NLCEuroLocation"].orEmpty(),
            stanox = m["NLCStanoxCode"].orEmpty(),
            poMcp = m["NLCPoMcpCode"].orEmpty()
        )
    }

    /** One entry per TIPLOC. If a TIPLOC repeats, the entry with a CRS wins, then one with a STANOX, then the later one. */
    fun byTiploc(entries: List<NlcEntry>): Map<String, NlcEntry> {
        val map = HashMap<String, NlcEntry>(entries.size * 2)
        for (e in entries) {
            val old = map[e.tiploc]
            if (old == null || score(e) >= score(old)) map[e.tiploc] = e
        }
        return map
    }

    private fun score(e: NlcEntry) = (if (e.crs.isNotEmpty()) 2 else 0) + (if (e.stanox.isNotEmpty()) 1 else 0)
}

/** Read-only check of both files together: coverage, and where PIF and NLC disagree on STANOX. */
object LocationDryRun {
    @JvmStatic
    fun main(args: Array<String>) {
        val dir = File("/srv/rdm/in")
        val pifFile = PifFiles.selectFor(dir)
        val nlcFile = dir.listFiles().orEmpty().filter { it.name.startsWith("NLC") && it.name.contains(".xml") }
            .maxByOrNull { it.name }
        if (pifFile == null || nlcFile == null) { println("missing file: pif=$pifFile nlc=$nlcFile"); return }

        val loc = LocHandler()
        val res = PifParser.parse(pifFile, listOf(loc))
        val pif = loc.currentByTiploc(LocalDate.now())
        val t0 = System.currentTimeMillis()
        val entries = NlcParser.parse(nlcFile)
        val nlc = NlcParser.byTiploc(entries)
        println("PIF  : ${pifFile.name} complete=${res.isComplete()} current=${pif.size} withStanox=${pif.values.count { it.stanox != null }}")
        println("NLC  : ${nlcFile.name} items=${entries.size} distinctTiploc=${nlc.size} withCrs=${nlc.values.count { it.crs.isNotEmpty() }} " +
                "withStanox=${nlc.values.count { it.stanox.isNotEmpty() }} (${System.currentTimeMillis() - t0} ms)")

        val both = pif.keys.intersect(nlc.keys)
        println("TIPLOC overlap: both=${both.size} pifOnly=${pif.size - both.size} nlcOnly=${nlc.size - both.size}")

        var same = 0; var differ = 0; var pifOnlyStanox = 0; var nlcOnlyStanox = 0
        val samples = ArrayList<String>()
        for (t in both.sorted()) {
            val ps = pif[t]?.stanox
            val ns = nlc[t]?.stanox?.takeIf { it.isNotEmpty() }
            when {
                ps != null && ns != null -> if (ps == ns) same++ else { differ++; if (samples.size < 12) samples.add("$t pif=$ps nlc=$ns") }
                ps != null -> pifOnlyStanox++
                ns != null -> nlcOnlyStanox++
            }
        }
        println("STANOX (PIF vs NLC): same=$same differ=$differ onlyInPif=$pifOnlyStanox onlyInNlc=$nlcOnlyStanox")
        samples.forEach { println("  differ: $it") }

        val pifStanoxNoCrs = pif.values.count { it.stanox != null && nlc[it.tiploc]?.crs.isNullOrEmpty() }
        println("PIF TIPLOCs with STANOX but no NLC CRS: $pifStanoxNoCrs")
        for (t in listOf("ABDARE", "ABDARAR", "ABDO", "ABCWM", "ABARASQ", "FENTON")) {
            val p = pif[t]; val n = nlc[t]
            println("  $t -> pif=${p?.name}/${p?.stanox}  nlc=${n?.description}/crs=${n?.crs}/stanox=${n?.stanox}/nlc=${n?.nlc}")
        }
    }
}

package com.traintracker.server.rdm

import com.traintracker.server.database.AppDatabase
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.io.File
import java.util.concurrent.CopyOnWriteArrayList

object RdmFiles {
    val dir = File("/srv/rdm/in")
    private const val SETTLE_MS = 60_000L   // ignore files still being written

    /** Same rule as prune_rdm.sh: file name with digits and spaces removed. */
    fun typeKey(name: String): String = name.filterNot { it == ' ' || it in '0'..'9' }

    /** Matcher for every file of one type, e.g. ofType("berth-steps-.csv"). */
    fun ofType(key: String): (String) -> Boolean = { typeKey(it) == key }

    fun list(): List<File> = dir.listFiles().orEmpty().filter { it.isFile }

    /** Newest matching file: the digits in the name decide first, then mtime. */
    fun newest(files: List<File>, matches: (String) -> Boolean): File? =
        files.filter { matches(it.name) }.maxWithOrNull(Comparator { a, b -> compare(a, b) })

    private fun digits(f: File) = f.name.filter { it in '0'..'9' }

    private fun compare(a: File, b: File): Int {
        val da = digits(a)
        val db = digits(b)
        if (da.length != db.length) return da.length.compareTo(db.length)
        val c = da.compareTo(db)
        return if (c != 0) c else a.lastModified().compareTo(b.lastModified())
    }

    fun isSettled(f: File) = System.currentTimeMillis() - f.lastModified() >= SETTLE_MS
    /**
     * Identity of a file's content: name, size and SHA-256. The mtime is ignored on purpose, because the
     * feed folder gets files re-copied hourly with identical content (a re-gzipped copy only changes the header timestamp), which must not count as a new file. For .gz files the decompressed content is hashed.
     */
    fun versionKey(f: File): String {
        val md = java.security.MessageDigest.getInstance("SHA-256")
        val raw = f.inputStream().buffered()
        val stream: java.io.InputStream =
            if (f.name.endsWith(".gz", ignoreCase = true)) java.util.zip.GZIPInputStream(raw) else raw
        stream.use { input ->
            val buf = ByteArray(65536)
            while (true) {
                val n = input.read(buf)
                if (n < 0) break
                md.update(buf, 0, n)
            }
        }
        return "${f.name}|${f.length()}|${md.digest().joinToString("") { "%02x".format(it) }}"
    }

    /** Helper for CSV feeds. */
    fun readCsv(f: File): List<List<String>> {
        val text = f.readText(Charsets.UTF_8).removePrefix("\uFEFF")
        val rows = ArrayList<List<String>>()
        var row = ArrayList<String>()
        val sb = StringBuilder()
        var inQ = false
        var i = 0
        while (i < text.length) {
            val c = text[i]
            if (inQ) {
                if (c == '"') {
                    if (i + 1 < text.length && text[i + 1] == '"') { sb.append('"'); i++ } else inQ = false
                } else sb.append(c)
            } else when (c) {
                '"' -> if (sb.isEmpty()) inQ = true else sb.append(c)
                ',' -> { row.add(sb.toString()); sb.setLength(0) }
                '\r' -> {}
                '\n' -> { row.add(sb.toString()); sb.setLength(0); rows.add(row); row = ArrayList() }
                else -> sb.append(c)
            }
            i++
        }
        if (sb.isNotEmpty() || row.isNotEmpty()) { row.add(sb.toString()); rows.add(row) }
        return rows.filter { r -> r.any { it.isNotBlank() } }
    }
}

/**
 * One RDM feed. matches picks its files by name; the newest match is loaded whenever it
 * changes (new name, or the same name rewritten). tick runs on every pass, used for retries.
 */
class RdmFeed(
    val name: String,
    val matches: (String) -> Boolean,
    private val tick: () -> Unit = {},
    /** Optional: chooses the file to load from this feed's matching files (default: newest by name digits). */
    val pick: ((List<File>) -> File?)? = null,
    private val load: (file: File, isNew: Boolean) -> Unit
) {
    private val log = LoggerFactory.getLogger("RdmFeed-$name")
    @Volatile private var lastKey = ""
    private val metaKey = "rdm_last_$name"

    fun refresh(f: File?) {
        try {
            if (f == null) {
                log.warn("$name: no matching file in ${RdmFiles.dir.path}")
                return
            }
            if (!RdmFiles.isSettled(f)) return
            val key = RdmFiles.versionKey(f)
            if (key == lastKey) return
            load(f, AppDatabase.getCifMeta(metaKey) != key)
            lastKey = key
            AppDatabase.setCifMeta(metaKey, key)
            log.info("$name loaded from ${f.name}")
        } finally {
            runCatching { tick() }
        }
    }
}

object RdmWatcher {
    private val log = LoggerFactory.getLogger("RdmWatcher")
    private val feeds = CopyOnWriteArrayList<RdmFeed>()
    /** Passes run at this minute past every hour (the RDM drops land about :10-:12), not an hour after start-up. */
    private const val PASS_MINUTE = 20

    private fun millisUntilNextPass(now: Long = System.currentTimeMillis()): Long {
        val hour = 3_600_000L
        return hour - Math.floorMod(now - PASS_MINUTE * 60_000L, hour)
    }
    @Volatile private var job: Job? = null

    fun register(feed: RdmFeed) {
        if (feeds.none { it.name == feed.name }) feeds.add(feed)
    }

    /** Starts the single hourly loop once; later calls return the running job. */
    @Synchronized
    fun ensureStarted(scope: CoroutineScope): Job {
        job?.takeIf { it.isActive }?.let { return it }
        val j = scope.launch(Dispatchers.IO) {
            while (isActive) {
                val files = RdmFiles.list()   // one directory listing per pass
                for (feed in feeds) {
                    try {
                        val chosen = feed.pick?.let { pk -> pk(files.filter { f -> feed.matches(f.name) }) }
                        feed.refresh(if (feed.pick != null) chosen else RdmFiles.newest(files, feed.matches))
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        log.warn("${feed.name} refresh failed: ${e.message}")
                    }
                }
                val wait = millisUntilNextPass()
                log.info("RDM pass complete (${feeds.size} feeds), next in ${wait / 60_000} min")
                delay(wait)
            }
        }
        job = j
        return j
    }
}

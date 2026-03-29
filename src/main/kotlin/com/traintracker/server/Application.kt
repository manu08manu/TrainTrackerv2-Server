package com.traintracker.server

import com.traintracker.server.cif.CifDownloader
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.kafka.TrustConsumer
import com.traintracker.server.kafka.VstpConsumer
import com.traintracker.server.kafka.AllocationConsumer
import com.traintracker.server.routes.configureRoutes
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.cors.routing.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.plugins.compression.*
import io.ktor.server.response.*
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors

private val log = LoggerFactory.getLogger("Application")

fun main() {
    embeddedServer(Netty, port = Config.port, host = "0.0.0.0") {
        configure()
    }.start(wait = true)
}

private fun Application.configure() {
    // ── JSON serialisation ────────────────────────────────────────────────
    install(ContentNegotiation) {
        json(Json {
            prettyPrint       = false
            isLenient         = true
            ignoreUnknownKeys = true
            encodeDefaults    = true
        })
    }

    // ── Response compression ─────────────────────────────────────────────
    install(Compression) {
        gzip { priority = 1.0 }
        deflate { priority = 0.9 }
    }

    // ── CORS — allow Android app to call from any origin ─────────────────
    // In production, restrict to your domain / app package.
    install(CORS) {
        allowMethod(HttpMethod.Get)
        allowMethod(HttpMethod.Post)
        allowHeader(HttpHeaders.Authorization)
        allowHeader(HttpHeaders.ContentType)
        anyHost()
    }

    // ── Error handling ────────────────────────────────────────────────────
    install(StatusPages) {
        exception<IllegalArgumentException> { call, cause ->
            call.respond(HttpStatusCode.BadRequest, mapOf("error" to (cause.message ?: "Bad request")))
        }
        exception<Exception> { call, cause ->
            val app = call.application
            app.log.error("Unhandled error", cause)
            call.respond(HttpStatusCode.InternalServerError, mapOf("error" to "Internal server error"))
        }
    }

    // ── Routes ────────────────────────────────────────────────────────────
    configureRoutes()

    // ── Background tasks ──────────────────────────────────────────────────
    val backgroundScope = CoroutineScope(
        SupervisorJob() + Dispatchers.IO + CoroutineName("background")
    )

    backgroundScope.launch {
        startupTasks(backgroundScope)
    }

    environment.monitor.subscribe(ApplicationStopped) {
        backgroundScope.cancel()
    }
}

private suspend fun startupTasks(scope: CoroutineScope) {
    log.info("TrainTracker backend starting…")

    // 1. Initialise database (create tables if needed)
    withContext(Dispatchers.IO) {
        AppDatabase.init()
    CorpusLookup.init()
    }

    // 2. Download/refresh CIF schedule (blocks until done on first run)
    try {
        CifDownloader.downloadIfNeeded()
    } catch (e: Exception) {
        log.error("CIF download failed on startup: ${e.message}")
        // Non-fatal — carry on with whatever is already in the DB
    }

    // 3. Start Kafka consumers
    TrustConsumer.start(scope)
    VstpConsumer.start(scope)
    AllocationConsumer.start(scope)
    log.info("Kafka consumers started")

    // 4. Nightly CIF refresh at 03:00
    scope.launch {
        scheduleDailyAt(hour = 3, minute = 0) {
            try {
                log.info("Nightly CIF refresh starting…")
                CifDownloader.downloadIfNeeded()
            } catch (e: Exception) {
                log.error("Nightly CIF refresh failed: ${e.message}")
            }
        }
    }

    // 6. Nightly unit allocation snapshot at 00:01
    scope.launch {
        scheduleDailyAt(hour = 0, minute = 1) {
            try {
                val yesterday = java.time.LocalDate.now().minusDays(1).toString()
                log.info("Snapshotting unit allocations for $yesterday…")
                AppDatabase.snapshotUnitAllocations(yesterday)
            } catch (e: Exception) {
                log.error("Unit allocation snapshot failed: ${e.message}")
            }
        }
    }

    // 5. Hourly TRUST movement pruner (removes events older than 24h) + allocation pruner
    scope.launch {
        while (isActive) {
            delay(3_600_000L) // 1 hour
            try {
                AppDatabase.pruneTrustMovements()
                AppDatabase.pruneAllocations()
                log.debug("TRUST movements and allocations pruned")
            } catch (e: Exception) {
                log.warn("Prune error: ${e.message}")
            }
        }
    }

    log.info("TrainTracker backend ready on port ${Config.port}")
}

/**
 * Suspends until the next occurrence of [hour]:[minute], then loops daily.
 */
private suspend fun scheduleDailyAt(hour: Int, minute: Int, block: suspend () -> Unit) {
    while (true) {
        val now = java.time.LocalDateTime.now()
        var next = now.toLocalDate().atTime(hour, minute)
        if (!now.isBefore(next)) {
            next = next.plusDays(1)
        }
        val delayMs = java.time.Duration.between(now, next).toMillis()
        delay(delayMs)
        block()
    }
}

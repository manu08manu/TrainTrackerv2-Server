package com.traintracker.server

import com.traintracker.server.auth.AuthDatabase
import com.traintracker.server.cif.CifDownloader
import com.traintracker.server.cif.CorpusLookup
import com.traintracker.server.cif.SmartLookup
import com.traintracker.server.database.AppDatabase
import com.traintracker.server.kafka.AllocationConsumer
import com.traintracker.server.kafka.DarwinConsumer
import com.traintracker.server.kafka.TdConsumer
import com.traintracker.server.kafka.TrustConsumer
import com.traintracker.server.kafka.VstpConsumer
import com.traintracker.server.kafka.darwinPlatforms
import com.traintracker.server.kafka.ridToUid
import com.traintracker.server.kafka.trainIdToRid
import com.traintracker.server.kb.KbClient
import com.traintracker.server.routes.configureRoutes
import io.ktor.http.CacheControl
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.CachingOptions
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationStopped
import io.ktor.server.application.install
import io.ktor.server.application.log
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.cachingheaders.CachingHeaders
import io.ktor.server.plugins.compression.Compression
import io.ktor.server.plugins.compression.deflate
import io.ktor.server.plugins.compression.gzip
import io.ktor.server.plugins.conditionalheaders.ConditionalHeaders
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.cors.routing.CORS
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.request.path
import io.ktor.server.response.respond
import io.ktor.server.routing.IgnoreTrailingSlash
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

private val log = LoggerFactory.getLogger("Application")

fun main() {
    AppDatabase.init()
    AuthDatabase.init()
    CorpusLookup.init()
    embeddedServer(Netty, port = Config.port, host = "0.0.0.0") {
        configure()
    }.start(wait = true)
}

private fun Application.configure() {
    install(IgnoreTrailingSlash)

    install(ContentNegotiation) {
        json(Json {
            prettyPrint = false
            isLenient = true
            ignoreUnknownKeys = true
            encodeDefaults = true
        })
    }
    install(Compression) {
        gzip { priority = 1.0 }
        deflate { priority = 0.9 }
    }
    install(ConditionalHeaders)
    install(CachingHeaders) {
        options { call, content ->
            if (call.request.path().startsWith("/css/")) {
                CachingOptions(cacheControl = CacheControl.MaxAge(maxAgeSeconds = 86400))
            } else {
                null
            }
        }
    }
    install(CORS) {
        allowMethod(HttpMethod.Get)
        allowMethod(HttpMethod.Post)
        allowHeader(HttpHeaders.Authorization)
        allowHeader(HttpHeaders.ContentType)
        anyHost()
    }
    install(StatusPages) {
        exception<IllegalArgumentException> { call, cause ->
            call.respond(HttpStatusCode.BadRequest, mapOf("error" to (cause.message ?: "Bad request")))
        }
        exception<Exception> { call, cause ->
            val app = call.application
            app.log.error("Unhandled error", cause)
            call.respond(HttpStatusCode.InternalServerError, mapOf("error" to "Something went wrong on our side. Please try again."))
        }
    }

    KbClient.disruptionsKey = Config.kbDisruptionsKey
    KbClient.disruptionsUrl = Config.kbDisruptionsUrl
    KbClient.stationsKey = Config.kbStationsKey
    KbClient.stationsSecret = Config.kbStationsSecret
    KbClient.stationsUrl = Config.kbStationsUrl
    KbClient.tocKey = Config.kbTocKey
    KbClient.tocSecret = Config.kbTocSecret
    KbClient.tocTokenUrl = Config.kbTocTokenUrl
    KbClient.tocUrl = Config.kbTocUrl

    configureRoutes()

    val backgroundScope = CoroutineScope(SupervisorJob() + Dispatchers.IO + CoroutineName("background"))
    backgroundScope.launch {
        startupTasks(backgroundScope)
    }
    monitor.subscribe(ApplicationStopped) {
        backgroundScope.cancel()
    }
}

private suspend fun startupTasks(scope: CoroutineScope) {
    log.info("TrainTracker backend starting…")
    try {
        CifDownloader.downloadIfNeeded()
    } catch (e: Exception) {
        log.error("CIF download failed on startup: ${e.message}")
    }
    withContext(Dispatchers.IO) {
        val activations = AppDatabase.loadTrainActivations()
        if (activations.isNotEmpty()) {
            trainIdToRid.putAll(activations)
            log.info("Reloaded ${activations.size} train activations from DB")
        }
        val ridUidMap = AppDatabase.loadDarwinRidUid()
        if (ridUidMap.isNotEmpty()) {
            ridToUid.putAll(ridUidMap)
            log.info("Reloaded ${ridUidMap.size} Darwin RID→UID mappings from DB")
        }
        val stanoxMap = AppDatabase.loadStanoxMap()
        if (stanoxMap.isNotEmpty()) {
            CorpusLookup.mergeStanoxFromFeed(stanoxMap)
            log.info("Stanox map reloaded from DB: ${stanoxMap.size} entries")
        } else {
            log.warn("Stanox map empty — delays may not show until next CIF download")
        }
        val stanoxTiplocMap = AppDatabase.loadStanoxTiplocMap()
        if (stanoxTiplocMap.isNotEmpty()) {
            CorpusLookup.mergeStanoxToTiplocFromFeed(stanoxTiplocMap)
            log.info("Stanox->tiploc map reloaded from DB: ${stanoxTiplocMap.size} entries")
        }
    }
    TrustConsumer.start(scope)
    VstpConsumer.start(scope)
    AllocationConsumer.start(scope)
    DarwinConsumer.start(scope)
    SmartLookup.start(scope)
    TdConsumer.start(scope)
    log.info("Kafka consumers started")

    scope.launch {
        scheduleDailyAt(3, 15) {
            try {
                log.info("Nightly CIF refresh starting…")
                CifDownloader.downloadIfNeeded()
            } catch (e: Exception) {
                log.error("Nightly CIF refresh failed: ${e.message}")
            }
        }
    }

    scope.launch {
        while (isActive) {
            delay(millisUntilNextHour())
            try {
                AppDatabase.pruneTrustMovements()
                AppDatabase.pruneTrainLocations()
                AppDatabase.pruneAllocations()
                AppDatabase.pruneAllocationLegs()
                AppDatabase.pruneAllocationHistory()
                AppDatabase.pruneAllocationChanges()
                AppDatabase.cleanupDarwinPlatforms()
                AppDatabase.cleanupDarwinRidUid()
                AppDatabase.pruneHspCache()
                AppDatabase.pruneTrainActivations()
                AppDatabase.pruneVstpAmendments()
                AppDatabase.pruneCifChangeAlerts()
                AppDatabase.pruneFeedback()
                val liveActivations = AppDatabase.loadTrainActivations()
                trainIdToRid.keys.retainAll(liveActivations.keys)
                val liveRids = AppDatabase.loadDarwinRidUid()
                ridToUid.keys.retainAll(liveRids.keys)
                darwinPlatforms.clear()
                log.info("TRUST movements, allocations, Darwin platforms and HSP cache pruned")
            } catch (e: Exception) {
                log.warn("Prune error: ${e.message}")
            }
        }
    }

    scope.launch {
        while (isActive) {
            delay(60_000)
            try {
                CorpusLookup.flushMissingTiplocs()
            } catch (e: Exception) {
                log.warn("Missing-tiploc flush error: ${e.message}")
            }
            try {
                CorpusLookup.flushMissingTscs()
            } catch (e: Exception) {
                log.warn("Missing-TSC flush error: ${e.message}")
            }
        }
    }

    log.info("TrainTracker backend ready on port ${Config.port}")
}

private suspend fun scheduleDailyAt(hour: Int, minute: Int, block: suspend () -> Unit) {
    while (true) {
        val now = LocalDateTime.now()
        var next = now.toLocalDate().atTime(hour, minute)
        if (!now.isBefore(next)) next = next.plusDays(1)
        val delayMs = Duration.between(now, next).toMillis()
        delay(delayMs)
        block()
    }
}

private fun millisUntilNextHour(): Long {
    val now = LocalDateTime.now()
    val next = now.truncatedTo(ChronoUnit.HOURS).plusHours(1)
    return Duration.between(now, next).toMillis()
}

package com.traintracker.server.kafka

import com.traintracker.server.database.Movements
import com.traintracker.server.database.Schedules
import com.traintracker.server.database.Schema
import com.traintracker.server.database.TrainActivations
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction

/**
 * Change of origin against a real (temporary) SQLite database with the production schema, so column limits
 * and constraints apply. Modelled on 9M14 (C24965) on 2026-11-10: Newcastle - Liverpool, restarted at Leeds.
 */
class ChangeOfOriginDbTest {
    private val dbFile = Files.createTempFile("traintracker-test", ".db")
    private val stanox = mapOf("NWCSTLE" to "13226", "DRHM" to "13708", "YORK" to "16200", "LEEDS" to "17132", "LVRPLSH" to "35026")

    private val trainId = "129M14MM15"
    private val uid = "C24965"
    private val date = "2026-01-15"

    @BeforeTest
    fun setUp() {
        Database.connect("jdbc:sqlite:$dbFile", driver = "org.sqlite.JDBC")
        Schema.create()
        // tiploc, crs, time, stop type, is pass
        val calls = listOf(
            listOf("NWCSTLE", "NCL", "14:01", "LO", false),
            listOf("KEBGNJN", null, "14:05", "LI", true),
            listOf("DRHM", "DHM", "14:17", "LI", false),
            listOf("YORK", "YRK", "15:04", "LI", false),
            listOf("LEEDS", "LDS", "15:30", "LI", false),
            listOf("LVRPLSH", "LIV", "17:05", "LT", false),
        )
        transaction {
            Schedules.batchInsert(calls.withIndex()) { (i, c) ->
                this[Schedules.uid] = uid
                this[Schedules.headcode] = "9M14"
                this[Schedules.stpIndicator] = 'P'
                this[Schedules.tiploc] = c[0] as String
                this[Schedules.crs] = c[1] as String?
                this[Schedules.scheduledTime] = c[2] as String
                this[Schedules.stopType] = c[3] as String
                this[Schedules.isPass] = c[4] as Boolean
                this[Schedules.originTiploc] = "NWCSTLE"
                this[Schedules.destTiploc] = "LVRPLSH"
                this[Schedules.scheduleStartDate] = "2026-01-01"
                this[Schedules.scheduleEndDate] = "2026-12-31"
                this[Schedules.scheduleDate] = date
                this[Schedules.stopSequence] = i
            }
        }
    }

    @AfterTest
    fun tearDown() {
        Files.deleteIfExists(dbFile)
    }

    private fun changeOrigin(tiploc: String) =
        LinxMovementWriter.changeOfOrigin(trainId, uid, "9M14", date, tiploc, stanox[tiploc], "I1") { stanox[it] }

    /** (crs, scheduled time, stanox, reason) of each stop cancelled by the change of origin. */
    private fun cooRows() = transaction {
        Movements.selectAll().where { Movements.eventType eq LinxMovementWriter.COO_CANCELLATION }
            .map { listOf(it[Movements.crs], it[Movements.scheduledTime], it[Movements.stanox], it[Movements.cancelReason]) }
            .sortedBy { it[1] }
    }

    private fun activationOrigin() = transaction {
        TrainActivations.selectAll().where { TrainActivations.trainId eq trainId }.single()[TrainActivations.originStanox]
    }

    @Test
    fun `each stop before the new origin gets its own cancellation`() {
        changeOrigin("LEEDS")
        assertEquals(
            listOf(
                listOf("NCL", "14:01", "13226", "I1"),
                listOf("DHM", "14:17", "13708", "I1"),
                listOf("YRK", "15:04", "16200", "I1"),
            ),
            cooRows(),
            "passes are skipped; every stop keeps its own STANOX so none overwrite each other",
        )
        assertEquals("17132", activationOrigin())
    }

    @Test
    fun `a later change of origin replaces the earlier one`() {
        changeOrigin("LEEDS")
        changeOrigin("YORK")
        assertEquals(listOf("NCL", "DHM"), cooRows().map { it[0] })
        assertEquals("16200", activationOrigin())
    }

    @Test
    fun `moving the start back to the timetabled origin cancels nothing`() {
        changeOrigin("LEEDS")
        changeOrigin("NWCSTLE")   // like 2T38 on 2026-11-10: Bedlington, then back to Ashington
        assertEquals(emptyList(), cooRows())
        assertEquals("13226", activationOrigin())
    }
}

package com.traintracker.server.kafka

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Parses real LINX EI messages captured on 2026-11-10 (src/test/resources/linx). */
class LinxParserTest {
    private val stanox = mapOf("DLTN" to "16200", "LEEDS" to "17132", "NTNG" to "52226", "MRGT" to "87001")
    private val parser = LinxParser(stanoxOf = { stanox[it] }, crsOf = { if (it == "DLTN") "DAR" else null })

    private fun parse(name: String, flow: String = name.substringBefore('_')): LinxParsed {
        val xml = javaClass.getResource("/linx/$name.xml")!!.readText()
        return parser.parse(flow, xml, receivedMs = 1_000L)
    }

    @Test
    fun `S510 departure`() {
        val p = parse("S510_04")
        val m = assertIs<LinxMovement>(p.message)
        assertEquals("125A171I10", m.trainId)
        assertEquals("K06385", m.uid)
        assertEquals("5A17", m.headcode)
        assertEquals("DLTN", m.tiploc)
        assertEquals("16200", m.stanox)
        assertEquals("DAR", m.crs)
        assertEquals("DEPARTURE", m.eventType)
        assertEquals("2026-11-10T13:20:00-00:00", m.actualTs)
        assertEquals("", m.bookedTs)
        assertNull(p.problem)
        assertEquals("2026-11-10T13:20:43-00:00", p.sentAt)
    }

    @Test
    fun `S510 arrival and arrival at destination`() {
        assertEquals("ARRIVAL", assertIs<LinxMovement>(parse("S510_03").message).eventType)
        val dest = assertIs<LinxMovement>(parse("S510_01").message)
        assertEquals("DESTINATION", dest.eventType)
        assertEquals("2026-11-10T13:19:30-00:00", dest.bookedTs)
        assertEquals(1, dest.delay)   // "+0001": a minute late
    }

    @Test
    fun `S510 without a known STANOX is kept but flagged`() {
        val p = parse("S510_01")   // STHH is not in the lookup
        assertIs<LinxMovement>(p.message)
        assertEquals("S510:no-stanox", p.problem)
    }

    @Test
    fun `space-padded 5-character uid is read`() {
        val m = assertIs<LinxMovement>(parse("S510_padded").message)
        assertEquals("32205", m.uid)
        assertEquals("0Z66", m.headcode)
        assertEquals(0, m.delay)   // "-0000"
    }

    @Test
    fun `S509 activation takes the UID parameter and origin`() {
        val a = assertIs<LinxActivation>(parse("S509").message)
        assertEquals("551Y20MO10", a.trainId)
        assertEquals("G89763", a.uid)
        assertEquals("NTNG", a.originTiploc)
        assertEquals("52226", a.originStanox)
        assertEquals("14:17:00", a.originTime)
    }

    @Test
    fun `S513 cancellation, en route cancellation and reinstatement`() {
        val c = assertIs<LinxModification>(parse("S513_91").message)
        assertEquals("CANCELLATION", c.kind)
        assertEquals("MRGT", c.tiploc)
        assertEquals("87001", c.stanox)
        assertEquals("PL", c.reason)
        assertEquals("10:10:00", c.time)
        assertEquals("CANCELLATION_EN_ROUTE", assertIs<LinxModification>(parse("S513_92").message).kind)
        val r = assertIs<LinxModification>(parse("S513_96").message)
        assertEquals("REINSTATEMENT", r.kind)
        assertEquals("DALMUIR", r.tiploc)
    }

    @Test
    fun `S513 change of origin`() {
        val m = assertIs<LinxModification>(parse("S513_94").message)
        assertEquals("CHANGE_OF_ORIGIN", m.kind)
        assertEquals("9M13", m.headcode)
        assertEquals("G15900", m.uid)
        assertEquals("LEEDS", m.tiploc)
        assertEquals("17132", m.stanox)
        assertEquals("14:30:00", m.time)
        assertEquals("YI", m.reason)
    }

    @Test
    fun `S515 change of identity carries old and new headcode`() {
        val m = assertIs<LinxModification>(parse("S515").message)
        assertEquals("CHANGE_OF_IDENTITY", m.kind)
        assertEquals("396V75CF10", m.trainId)
        assertEquals("0V75", m.headcode)
        assertEquals("6V75", m.previousHeadcode)
        assertEquals("H57708", m.uid)
    }

    @Test
    fun `unknown flow is kept raw`() {
        val p = parse("S509", flow = "S999")
        val raw = assertIs<LinxRaw>(p.message)
        assertEquals("S999", raw.flow)
        assertTrue(raw.xml.startsWith("<?xml"))
    }

    @Test
    fun `message without a train ID is skipped with a reason`() {
        val xml = javaClass.getResource("/linx/S510_04.xml")!!.readText().replace("--125A171I10", "nonsense")
        val p = parser.parse("S510", xml, 1_000L)
        assertNull(p.message)
        assertEquals("S510:no-id", p.problem)
    }
}

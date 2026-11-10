package com.traintracker.server.kafka

import java.io.StringReader
import javax.xml.parsers.DocumentBuilderFactory
import org.w3c.dom.Element
import org.xml.sax.InputSource

/** A LINX EI message, parsed. Times are as LINX sent them; [LinxClock] turns them into UK clock time. */
internal sealed class LinxMessage {
    abstract val receivedMs: Long
}

/** S510: an arrival, departure (or pass) or arrival at destination. */
internal data class LinxMovement(
    val trainId: String, val uid: String, val headcode: String, val startDate: String,
    val tiploc: String, val stanox: String?, val crs: String?, val locStatus: String, val eventType: String,
    val bookedTs: String, val actualTs: String, val delay: Int?, val referenceOtn: String?, override val receivedMs: Long,
) : LinxMessage()

/** S509: the train is activated against a schedule (uid) at its origin. */
internal data class LinxActivation(
    val trainId: String, val uid: String, val headcode: String, val startDate: String,
    val originTiploc: String?, val originStanox: String?, val originTime: String?, override val receivedMs: Long,
) : LinxMessage()

/** S513 cancellation / change of origin / reinstatement, or S515 change of identity. */
internal data class LinxModification(
    val trainId: String, val uid: String, val headcode: String, val startDate: String,
    val code: String, val kind: String, val tiploc: String?, val stanox: String?, val time: String?,
    val reason: String?, val modifiedAt: String?, override val receivedMs: Long, val previousHeadcode: String? = null,
    /** Every LocationModified, as "TIPLOC|ModificationStatusIndicator|TrainLocationStatus|booked" joined by ";". */
    val locations: String? = null,
) : LinxMessage()

/** A message kept as-is: an unknown flow or one that failed to parse. */
internal data class LinxRaw(val flow: String, val note: String, val xml: String, override val receivedMs: Long) : LinxMessage()

/**
 * [message] is null when the message was skipped; [problem] then names why (a counter key). [sentAt] is
 * LINX's MessageDateTime, used to tell how its times are labelled.
 */
internal class LinxParsed(val message: LinxMessage?, val problem: String? = null, val sentAt: String? = null)

/**
 * Parses LINX EI TAF/TSI XML. Pure apart from the two lookups, so it can be tested with sample messages.
 *
 * Identifiers: the "TR" core is the TRUST-format train ID with a "--" prefix; the "PA" core is headcode +
 * uid + 2 digits. LocationPrimaryCode is not a STANOX, so locations are read by TIPLOC and mapped.
 */
internal class LinxParser(
    private val stanoxOf: (String) -> String?,
    private val crsOf: (String) -> String?,
) {
    private val dbf: DocumentBuilderFactory = DocumentBuilderFactory.newInstance().apply {
        isNamespaceAware = false
        setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
        setFeature("http://xml.org/sax/features/external-general-entities", false)
        setFeature("http://xml.org/sax/features/external-parameter-entities", false)
        isXIncludeAware = false
        isExpandEntityReferences = false
    }

    /** Parses one message of [flow]; throws on malformed XML. */
    fun parse(flow: String, xml: String, receivedMs: Long): LinxParsed {
        val root = dbf.newDocumentBuilder().parse(InputSource(StringReader(xml))).documentElement
        val sentAt = root.text("MessageDateTime")
        val ids = ids(root)
        return when (flow) {
            "S510" -> if (ids == null) LinxParsed(null, "S510:no-id", sentAt) else running(root, ids, receivedMs, sentAt)
            "S509" -> if (ids == null) LinxParsed(null, "S509:no-id", sentAt) else LinxParsed(activation(root, ids, receivedMs), sentAt = sentAt)
            // Both flows are a TrainJourneyModificationMessage.
            "S513", "S515" -> if (ids == null) LinxParsed(null, "modification:no-id", sentAt) else LinxParsed(modification(root, ids, receivedMs), sentAt = sentAt)
            else -> LinxParsed(LinxRaw(flow, "unhandled flow", xml, receivedMs), sentAt = sentAt)
        }
    }

    private class Ids(val trainId: String, val uid: String, val headcode: String, val startDate: String)

    private fun ids(root: Element): Ids? {
        var tr: String? = null
        var pa: String? = null
        val types = root.getElementsByTagName("ObjectType")
        for (i in 0 until types.length) {
            val block = types.item(i).parentNode as? Element ?: continue
            when (types.item(i).textContent.trim()) {
                "TR" -> tr = tr ?: block.text("Core")
                "PA" -> pa = pa ?: block.text("Core")
            }
        }
        val trainId = tr?.trimStart('-')?.takeIf { it.length == 10 && it.all(Char::isLetterOrDigit) } ?: return null
        // Headcode + uid + 2 digits; a 5-character uid (VSTP and special workings) is space-padded: "1Z11 3224112".
        val uid = pa?.takeIf { it.length >= 10 }?.substring(4, 10)?.trim()?.takeIf { it.isNotEmpty() && it.all(Char::isLetterOrDigit) } ?: ""
        val headcode = root.text("OperationalTrainNumber")?.take(4) ?: trainId.substring(2, 6)
        val startDate = root.text("StartDate")?.take(10) ?: ""
        return Ids(trainId, uid, headcode, startDate)
    }

    private fun running(root: Element, ids: Ids, receivedMs: Long, sentAt: String?): LinxParsed {
        val report = root.first("TrainLocationReport") ?: return LinxParsed(null, "S510:no-report", sentAt)
        val tiploc = tiplocIn(report.first("Location")) ?: return LinxParsed(null, "S510:no-tiploc", sentAt)
        val status = report.text("TrainLocationStatus") ?: ""
        val eventType = when (status) {
            "01" -> "DESTINATION"
            "03" -> "ARRIVAL"
            "04" -> "DEPARTURE"
            else -> "STATUS_$status"
        }
        val movement = LinxMovement(
            ids.trainId, ids.uid, ids.headcode, ids.startDate, tiploc, stanoxOf(tiploc), crsOf(tiploc), status, eventType,
            bookedTs = report.text("BookedLocationDateTime") ?: "",
            actualTs = report.text("LocationDateTime") ?: "",
            delay = report.first("TrainDelay")?.text("AgainstBooked")?.toIntOrNull(),
            referenceOtn = root.text("ReferenceOTN"),
            receivedMs = receivedMs,
        )
        return LinxParsed(movement, if (movement.stanox == null) "S510:no-stanox" else null, sentAt)
    }

    private fun activation(root: Element, ids: Ids, receivedMs: Long): LinxActivation {
        // The first PlannedJourneyLocation is the origin; the UID is a NetworkSpecificParameter.
        val origin = root.first("PlannedJourneyLocation")
        var uid = ids.uid
        val params = root.getElementsByTagName("NetworkSpecificParameter")
        for (i in 0 until params.length) {
            val p = params.item(i) as? Element ?: continue
            if (p.text("Name") == "UID") p.text("Value")?.takeIf { it.all(Char::isLetterOrDigit) }?.let { uid = it }
        }
        val tiploc = tiplocIn(origin)
        return LinxActivation(ids.trainId, uid, ids.headcode, ids.startDate, tiploc, tiploc?.let(stanoxOf), origin?.text("Time"), receivedMs)
    }

    private fun modification(root: Element, ids: Ids, receivedMs: Long): LinxModification {
        val mod = root.first("TrainJourneyModification")
        val code = mod?.text("TrainJourneyModificationIndicator") ?: ""
        val kind = when (code) {
            "91" -> "CANCELLATION"
            "92" -> "CANCELLATION_EN_ROUTE"
            "94" -> "CHANGE_OF_ORIGIN"
            "96" -> "REINSTATEMENT"
            "07" -> "CHANGE_OF_IDENTITY"   // S515: OperationalTrainNumber is the new headcode, ReferenceOTN the old
            else -> "CODE_$code"
        }
        val loc = mod?.first("LocationModified")
        val tiploc = tiplocIn(loc)
        // A message can hold several modifications, each listing locations marked by ModificationStatusIndicator
        // (e.g. 95 at Heathrow T2 & 3 plus 06 adding West Drayton); keep them all. [code] is the first one's.
        val all = root.getElementsByTagName("LocationModified")
        val locations = (0 until all.length).mapNotNull { all.item(it) as? Element }.joinToString(";") { l ->
            listOf(
                tiplocIn(l) ?: "", l.text("ModificationStatusIndicator") ?: "", l.text("TrainLocationStatus") ?: "",
                l.text("BookedLocationDateTime") ?: l.text("Time") ?: "",
            ).joinToString("|")
        }.ifEmpty { null }
        val reason = root.first("ModificationReason")?.getAttribute("NationalDelayCode")?.takeIf { it.isNotEmpty() }
        return LinxModification(
            ids.trainId, ids.uid, ids.headcode, ids.startDate, code, kind, tiploc, tiploc?.let(stanoxOf),
            loc?.text("Time"), reason, root.text("TrainJourneyModificationTime"), receivedMs,
            previousHeadcode = root.first("ReferenceOTN")?.text("OperationalTrainNumber")?.take(4),
            locations = locations,
        )
    }

    /** LocationSubsidiaryCode (the TIPLOC) inside [el]. */
    private fun tiplocIn(el: Element?): String? = el?.text("LocationSubsidiaryCode")?.takeIf { it.length <= 7 && it.all(Char::isLetterOrDigit) }

    // Default namespace and no namespace awareness, so element names are unprefixed.
    private fun Element.first(tag: String): Element? = getElementsByTagName(tag).item(0) as? Element
    private fun Element.text(tag: String): String? = first(tag)?.textContent?.trim()?.takeIf { it.isNotEmpty() }
}

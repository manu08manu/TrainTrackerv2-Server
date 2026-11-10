package com.traintracker.server.kafka

import com.traintracker.server.Config
import com.traintracker.server.database.Allocations
import java.io.StringReader
import javax.xml.parsers.DocumentBuilderFactory
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import org.json.JSONObject
import org.slf4j.LoggerFactory
import org.w3c.dom.Element
import org.xml.sax.InputSource

private val log = LoggerFactory.getLogger("AllocationConsumer")

/** Passenger train allocation and consist feed: which units and vehicles work each train. */
object AllocationConsumer {
    private const val TOPIC = "prod-1033-Passenger-Train-Allocation-and-Consist-1_0"

    private val xmlFactory = DocumentBuilderFactory.newInstance().apply {
        isNamespaceAware = false
        setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
        setFeature("http://xml.org/sax/features/external-general-entities", false)
        setFeature("http://xml.org/sax/features/external-parameter-entities", false)
        setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
        isXIncludeAware = false
        isExpandEntityReferences = false
    }

    fun start(scope: CoroutineScope): Job =
        launchConsumer(scope, "Allocation", TOPIC, Config.kafkaGroupIdAllocation, log) { _, records ->
            if (records.count() > 0) log.debug("Allocation: ${records.count()} record(s)")
            for (record in records) record.value()?.let { handleMessage(it) }
        }

    private fun handleMessage(value: String) {
        val xml = extractXml(value)
        if (xml == null) {
            log.warn("Allocation: could not extract XML — raw(200): ${value.take(200)}")
            return
        }
        try {
            parseAllocationXml(xml)
        } catch (e: Exception) {
            log.warn("Allocation XML parse error: ${e.message}")
        }
    }

    /** The message is either raw XML or JSON wrapping it in one of a few fields. */
    private fun extractXml(value: String): String? {
        val trimmed = value.trimStart()
        if (trimmed.startsWith('<')) return trimmed
        return try {
            val json = JSONObject(value)
            listOf("body", "xml", "payload", "data").firstNotNullOfOrNull { json.optString(it).takeIf { s -> s.isNotEmpty() } }
        } catch (_: Exception) {
            null
        }
    }

    private fun parseAllocationXml(xml: String) {
        val doc = xmlFactory.newDocumentBuilder().parse(InputSource(StringReader(xml)))
        fun first(tag: String) = doc.getElementsByTagName(tag).item(0)?.textContent?.trim() ?: ""
        val coreId = first("Core")
        val serviceDate = first("StartDate")
        val operator = first("ResponsibleRU")
        val headcode = first("OperationalTrainNumber").uppercase()
        if (coreId.isEmpty() || headcode.isEmpty()) {
            log.debug("Allocation skipped: missing coreId or headcode")
            return
        }
        val unitIds = mutableListOf<String>()
        val vehicles = mutableListOf<String>()
        val resourceGroups = doc.getElementsByTagName("ResourceGroup")
        for (i in 0 until resourceGroups.length) {
            val rg = resourceGroups.item(i) as Element
            val groupId = childText(rg, "ResourceGroupId") ?: ""
            if (groupId.isNotEmpty() && groupId !in unitIds) unitIds.add(groupId)
            for (vehicleId in vehicleIds(rg)) if (vehicleId !in vehicles) vehicles.add(vehicleId)
        }
        val legs = mutableListOf<Allocations.Leg>()
        val allocationEls = doc.getElementsByTagName("Allocation")
        for (i in 0 until allocationEls.length) {
            val allocEl = allocationEls.item(i) as Element
            val seq = childText(allocEl, "AllocationSequenceNumber")?.toIntOrNull() ?: continue
            val originStanox = locationField(allocEl, "AllocationOriginLocation", "LocationPrimaryCode") ?: continue
            val destStanox = locationField(allocEl, "AllocationDestinationLocation", "LocationPrimaryCode") ?: continue
            val originTiploc = locationField(allocEl, "AllocationOriginLocation", "LocationSubsidiaryCode")
            val destTiploc = locationField(allocEl, "AllocationDestinationLocation", "LocationSubsidiaryCode")
            val originTime = timeOnly(allocEl, "AllocationOriginDateTime")
            val destTime = timeOnly(allocEl, "AllocationDestinationDateTime")
            val legGroups = allocEl.getElementsByTagName("ResourceGroup")
            for (g in 0 until legGroups.length) {
                val rg = legGroups.item(g) as Element
                val groupId = childText(rg, "ResourceGroupId") ?: continue
                val legVehicles = vehicleIds(rg)
                if (legVehicles.isNotEmpty()) {
                    legs.add(Allocations.Leg(seq, groupId, legVehicles, originStanox, originTiploc, originTime, destStanox, destTiploc, destTime))
                }
            }
        }
        if (legs.isNotEmpty() || vehicles.isEmpty()) Allocations.replaceLegs(coreId, headcode, serviceDate, legs)
        val allocated = vehicles.isNotEmpty()
        Allocations.upsert(coreId, headcode, serviceDate, operator, vehicles, if (allocated) unitIds.size else 0, unitIds)
        if (allocated) {
            log.info("Allocation saved: headcode=$headcode core=$coreId date=$serviceDate operator=$operator units=$unitIds vehicles=$vehicles")
        } else {
            log.info("Allocation cleared: headcode=$headcode core=$coreId date=$serviceDate")
        }
    }

    private fun childText(el: Element, tag: String): String? = el.getElementsByTagName(tag).item(0)?.textContent?.trim()

    private fun vehicleIds(el: Element): List<String> {
        val vehicleEls = el.getElementsByTagName("Vehicle")
        return (0 until vehicleEls.length).mapNotNull { childText(vehicleEls.item(it) as Element, "VehicleId")?.takeIf { id -> id.isNotEmpty() } }
    }

    private fun locationField(el: Element, tag: String, child: String): String? {
        val locEl = el.getElementsByTagName(tag).item(0) as? Element ?: return null
        return childText(locEl, child)
    }

    /** "2026-01-02T03:04:00..." -> "03:04". */
    private fun timeOnly(el: Element, tag: String): String? {
        val timePart = childText(el, tag)?.substringAfter('T', "") ?: return null
        return if (timePart.length >= 5) timePart.substring(0, 5) else null
    }
}

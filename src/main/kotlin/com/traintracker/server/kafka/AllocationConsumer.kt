package com.traintracker.server.kafka

import com.traintracker.server.Config
import com.traintracker.server.database.AppDatabase
import java.io.StringReader
import java.time.Duration
import java.util.concurrent.CancellationException
import javax.xml.parsers.DocumentBuilderFactory
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.json.JSONObject
import org.slf4j.LoggerFactory
import org.w3c.dom.Element
import org.xml.sax.InputSource

private val log = LoggerFactory.getLogger("AllocationConsumer")

object AllocationConsumer {
    private val topic = "prod-1033-Passenger-Train-Allocation-and-Consist-1_0"
    private val xmlFactory = DocumentBuilderFactory.newInstance().apply {
        isNamespaceAware = false
        setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
        setFeature("http://xml.org/sax/features/external-general-entities", false)
        setFeature("http://xml.org/sax/features/external-parameter-entities", false)
        setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
        isXIncludeAware = false
        isExpandEntityReferences = false
    }

    fun start(scope: CoroutineScope): Job = scope.launch(Dispatchers.IO) {
        while (isActive) {
            try {
                buildConsumer(groupId = Config.kafkaGroupIdAllocation).use { consumer ->
                    consumer.subscribe(listOf(topic))
                    log.info("Allocation subscribed to $topic (group=${Config.kafkaGroupIdAllocation})")
                    while (isActive) {
                        val records = consumer.poll(Duration.ofSeconds(5L))
                        if (records.count() > 0) log.debug("Allocation: ${records.count()} record(s)")
                        for (record in records) {
                            val v = record.value() ?: continue
                            handleMessage(v)
                        }
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("Allocation error: ${e.message} — retrying in 10s")
                delay(10000L)
            }
        }
    }

    private fun handleMessage(value: String) {
        try {
            val xml = extractXml(value)
            if (xml == null) {
                log.warn("Allocation: could not extract XML — raw(200): ${value.take(200)}")
                return
            }
            parseAllocationXml(xml)
        } catch (e: Exception) {
            log.debug("Allocation parse error: ${e.message}")
        }
    }

    private fun extractXml(value: String): String? {
        val trimmed = value.trimStart()
        if (trimmed.startsWith('<')) return trimmed
        return try {
            val json = JSONObject(value)
            json.optString("body").takeIf { it.isNotEmpty() }
                ?: json.optString("xml").takeIf { it.isNotEmpty() }
                ?: json.optString("payload").takeIf { it.isNotEmpty() }
                ?: json.optString("data").takeIf { it.isNotEmpty() }
        } catch (_: Exception) {
            null
        }
    }

    private fun parseAllocationXml(xml: String) {
        try {
            val doc = xmlFactory.newDocumentBuilder().parse(InputSource(StringReader(xml)))
            val coreId = doc.getElementsByTagName("Core").item(0)
                ?.textContent
                ?.trim()
                ?: ""
            val serviceDate = doc.getElementsByTagName("StartDate").item(0)
                ?.textContent
                ?.trim()
                ?: ""
            val operator = doc.getElementsByTagName("ResponsibleRU").item(0)
                ?.textContent
                ?.trim()
                ?: ""
            val headcode = doc.getElementsByTagName("OperationalTrainNumber").item(0)
                ?.textContent
                ?.trim()
                ?.uppercase()
                ?: ""
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
                if (groupId.isNotEmpty() && !unitIds.contains(groupId)) unitIds.add(groupId)
                for (vehicleId in vehicleIds(rg)) {
                    if (!vehicles.contains(vehicleId)) vehicles.add(vehicleId)
                }
            }
            val legs = mutableListOf<AppDatabase.AllocationLegData>()
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
                val legResourceGroups = allocEl.getElementsByTagName("ResourceGroup")
                for (g in 0 until legResourceGroups.length) {
                    val rg = legResourceGroups.item(g) as Element
                    val groupId = childText(rg, "ResourceGroupId") ?: continue
                    val legVehicles = vehicleIds(rg)
                    if (legVehicles.isNotEmpty()) {
                        legs.add(
                            AppDatabase.AllocationLegData(
                                seq, groupId, legVehicles, originStanox, originTiploc, originTime, destStanox, destTiploc, destTime
                            )
                        )
                    }
                }
            }
            if (legs.isNotEmpty() || vehicles.isEmpty()) {
                AppDatabase.replaceAllocationLegs(coreId, headcode, serviceDate, legs)
            }
            val allocated = vehicles.isNotEmpty()
            AppDatabase.upsertAllocation(coreId, headcode, serviceDate, operator, vehicles, if (allocated) unitIds.size else 0, unitIds)
            if (allocated) {
                log.info("Allocation saved: headcode=$headcode core=$coreId date=$serviceDate operator=$operator units=$unitIds vehicles=$vehicles")
            } else {
                log.info("Allocation cleared: headcode=$headcode core=$coreId date=$serviceDate")
            }
        } catch (e: Exception) {
            log.warn("Allocation XML parse error: ${e.message}")
        }
    }

    private fun childText(el: Element, tag: String): String? = el.getElementsByTagName(tag).item(0)?.textContent?.trim()

    private fun vehicleIds(el: Element): List<String> {
        val out = mutableListOf<String>()
        val vehicleEls = el.getElementsByTagName("Vehicle")
        for (v in 0 until vehicleEls.length) {
            val vehicleEl = vehicleEls.item(v) as Element
            val id = childText(vehicleEl, "VehicleId") ?: ""
            if (id.isNotEmpty()) out.add(id)
        }
        return out
    }

    private fun locationField(el: Element, tag: String, child: String): String? {
        val locEl = el.getElementsByTagName(tag).item(0) as? Element ?: return null
        return childText(locEl, child)
    }

    private fun timeOnly(el: Element, tag: String): String? {
        val dt = childText(el, tag) ?: return null
        val timePart = dt.substringAfter('T', "")
        return if (timePart.length >= 5) timePart.substring(0, 5) else null
    }

    private fun buildConsumer(): KafkaConsumer<String, String> = buildConsumer("latest", Config.kafkaGroupIdAllocation)
}

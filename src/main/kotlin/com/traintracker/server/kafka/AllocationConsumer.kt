package com.traintracker.server.kafka

import com.traintracker.server.Config
import com.traintracker.server.database.AppDatabase
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.xml.sax.InputSource
import java.io.StringReader
import java.time.Duration
import java.util.Properties
import javax.xml.parsers.DocumentBuilderFactory

private val log = LoggerFactory.getLogger("AllocationConsumer")

object AllocationConsumer {

    private val topic = "prod-1033-Passenger-Train-Allocation-and-Consist-1_0"

    fun start(scope: CoroutineScope): Job = scope.launch(Dispatchers.IO) {
        while (isActive) {
            try {
                buildConsumer().use { consumer ->
                    consumer.subscribe(listOf(topic))
                    log.info("Allocation subscribed to $topic (group=${Config.allocationGroupId})")
                    while (isActive) {
                        val records = consumer.poll(Duration.ofSeconds(5))
                        if (records.count() > 0) {
                            log.debug("Allocation: ${records.count()} record(s)")
                        }
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
                delay(10_000)
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
            val json = org.json.JSONObject(value)
            json.optString("body").takeIf { it.isNotEmpty() }
                ?: json.optString("xml").takeIf { it.isNotEmpty() }
                ?: json.optString("payload").takeIf { it.isNotEmpty() }
                ?: json.optString("data").takeIf { it.isNotEmpty() }
        } catch (_: Exception) { null }
    }

    /**
     * Parses TAFTSI/ERA 5.3 PassengerTrainConsistMessage.
     *
     * Key fields:
     *   <Core>1J23L6453116</Core>          — globally unique train ID (primary key)
     *   <StartDate>2026-03-27</StartDate>  — service date
     *   <Company>9971</Company>            — operator company code
     *   <OperationalTrainNumber>1J23</OperationalTrainNumber> — headcode
     *   <ResourceGroupId>158821</ResourceGroupId> — unit number
     *   <VehicleId>57821</VehicleId>       — vehicle number
     */
    private fun parseAllocationXml(xml: String) {
        try {
            val factory = DocumentBuilderFactory.newInstance().apply {
                isNamespaceAware = false
                setFeature("http://apache.org/xml/features/disallow-doctype-decl", false)
            }
            val doc = factory.newDocumentBuilder().parse(InputSource(StringReader(xml)))

            // Primary key — first <Core> element
            val coreId = doc.getElementsByTagName("Core")
                .item(0)?.textContent?.trim() ?: ""

            // Service date — first <StartDate> element
            val serviceDate = doc.getElementsByTagName("StartDate")
                .item(0)?.textContent?.trim() ?: ""

            // Operator — first <Company> element (responsible RU)
            val operator = doc.getElementsByTagName("ResponsibleRU")
                .item(0)?.textContent?.trim() ?: ""

            // Headcode
            val headcode = doc.getElementsByTagName("OperationalTrainNumber")
                .item(0)?.textContent?.trim()?.uppercase() ?: ""

            if (coreId.isEmpty() || headcode.isEmpty()) {
                log.debug("Allocation skipped: missing coreId or headcode")
                return
            }

            val unitIds  = mutableListOf<String>()
            val vehicles = mutableListOf<String>()

            val resourceGroups = doc.getElementsByTagName("ResourceGroup")
            for (i in 0 until resourceGroups.length) {
                val rg = resourceGroups.item(i) as org.w3c.dom.Element

                val groupId = rg.getElementsByTagName("ResourceGroupId")
                    .item(0)?.textContent?.trim() ?: ""
                if (groupId.isNotEmpty() && !unitIds.contains(groupId)) {
                    unitIds.add(groupId)
                }

                val vehicleEls = rg.getElementsByTagName("Vehicle")
                for (v in 0 until vehicleEls.length) {
                    val vehicleEl = vehicleEls.item(v) as org.w3c.dom.Element
                    val vehicleId = vehicleEl.getElementsByTagName("VehicleId")
                        .item(0)?.textContent?.trim() ?: ""
                    if (vehicleId.isNotEmpty() && !vehicles.contains(vehicleId)) {
                        vehicles.add(vehicleId)
                    }
                }
            }

            if (vehicles.isNotEmpty()) {
                AppDatabase.upsertAllocation(
                    coreId      = coreId,
                    headcode    = headcode,
                    serviceDate = serviceDate,
                    operator    = operator,
                    vehicles    = vehicles,
                    unitCount   = unitIds.size,
                    units       = unitIds
                )
                log.info("Allocation saved: headcode=$headcode core=$coreId date=$serviceDate operator=$operator units=$unitIds vehicles=$vehicles")
            } else {
                log.debug("Allocation skipped: headcode='$headcode' no vehicles")
            }

        } catch (e: Exception) {
            log.warn("Allocation XML parse error: ${e.message}")
        }
    }

    private fun buildConsumer(): KafkaConsumer<String, String> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        Config.trustBootstrap)
            put(ConsumerConfig.GROUP_ID_CONFIG,                 Config.allocationGroupId)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "latest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,       "30000")
            put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,       "45000")
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         "200")
            put("security.protocol", "SASL_SSL")
            put("sasl.mechanism",    "PLAIN")
            put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"${Config.allocationUsername}\" password=\"${Config.allocationPassword}\";")
            put("metric.reporters",          "")
            put("auto.include.jmx.reporter", "false")
        }
        return KafkaConsumer(props)
    }
}

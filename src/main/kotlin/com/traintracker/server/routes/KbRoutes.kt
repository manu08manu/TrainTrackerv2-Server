package com.traintracker.server.routes

import com.traintracker.server.auth.checkAuth
import com.traintracker.server.kb.KbClient
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

fun Route.kbRoutes() {
    get("/kb/station-messages") {
        val crs = call.request.queryParameters["crs"]?.uppercase()?.trim()
        if (crs.isNullOrBlank()) {
            call.badRequest("crs parameter required")
            return@get
        }
        if (!isValidCrs(crs)) {
            call.badRequest("crs must be exactly 3 letters")
            return@get
        }
        val messages = withContext(Dispatchers.IO) { KbClient.getStationMessages(crs) }
        if (messages == null) {
            call.respond(HttpStatusCode.ServiceUnavailable, mapOf("error" to "upstream unavailable"))
        } else {
            call.respond(messages)
        }
    }
    get("/kb/incidents") {
        val crs = call.request.queryParameters["crs"]?.uppercase()?.trim() ?: ""
        val incidents = withContext(Dispatchers.IO) { KbClient.getIncidents(crs) }
        call.respond(incidents.toJson())
    }
    get("/kb/incidents/by-toc/{toc}") {
        val toc = call.parameters["toc"]?.uppercase()?.trim()
        if (toc.isNullOrBlank()) {
            call.badRequest("toc parameter required")
            return@get
        }
        val incidents = withContext(Dispatchers.IO) { KbClient.getIncidentsByToc(toc) }
        call.respond(incidents.toJson())
    }
    get("/kb/nsi") {
        val nsi = withContext(Dispatchers.IO) { KbClient.getNsi() }
        call.respond(
            buildJsonArray {
                for (entry in nsi) {
                    add(buildJsonObject {
                        put("tocCode", entry.tocCode)
                        put("tocName", entry.tocName)
                        put("status", entry.status)
                        put("statusDescription", entry.statusDescription)
                        put("twitterHandle", entry.twitterHandle)
                        put("additionalInfo", entry.additionalInfo)
                        put("disruptions", buildJsonArray {
                            for (d in entry.disruptions) {
                                add(buildJsonObject {
                                    put("detail", d.detail)
                                    put("url", d.url)
                                })
                            }
                        })
                    })
                }
            }
        )
    }
    get("/kb/toc") {
        val tocs = withContext(Dispatchers.IO) { KbClient.getToc() }
        call.respond(
            buildJsonArray {
                for (t in tocs) {
                    add(buildJsonObject {
                        put("code", t.code)
                        put("name", t.name)
                        put("website", t.website)
                        put("customerServicePhone", t.customerServicePhone)
                        put("assistedTravelPhone", t.assistedTravelPhone)
                        put("assistedTravelUrl", t.assistedTravelUrl)
                        put("lostPropertyUrl", t.lostPropertyUrl)
                    })
                }
            }
        )
    }
    get("/kb/station/{crs}") {
        val crs = call.validCrs() ?: return@get
        val station = withContext(Dispatchers.IO) { KbClient.getStation(crs) }
        if (station == null) {
            call.respond(HttpStatusCode.NotFound, mapOf("error" to "Station $crs not found"))
        } else {
            call.respond(
                buildJsonObject {
                    put("crs", station.crs)
                    put("name", station.name)
                    put("address", station.address)
                    put("latitude", station.latitude)
                    put("longitude", station.longitude)
                    put("stationOperator", station.stationOperator)
                    put("staffingNote", station.staffingNote)
                    put("ticketOfficeHours", station.ticketOfficeHours)
                    put("sstmAvailability", station.sstmAvailability)
                    put("stepFreeAccess", station.stepFreeAccess)
                    put("assistanceAvail", station.assistanceAvail)
                    put("inductionLoop", station.inductionLoop)
                    put("accessibleTicketMachines", station.accessibleTicketMachines)
                    put("rampForTrainAccess", station.rampForTrainAccess)
                    put("wheelchairsAvailable", station.wheelchairsAvailable)
                    put("nationalKeyToilets", station.nationalKeyToilets)
                    put("ticketGates", station.ticketGates)
                    put("wifi", station.wifi)
                    put("toilets", station.toilets)
                    put("waitingRoom", station.waitingRoom)
                    put("cctv", station.cctv)
                    put("babyChange", station.babyChange)
                    put("seatedArea", station.seatedArea)
                    put("trolleys", station.trolleys)
                    put("leftLuggage", station.leftLuggage)
                    put("stationBuffet", station.stationBuffet)
                    put("showers", station.showers)
                    put("atmMachine", station.atmMachine)
                    put("firstClassLounge", station.firstClassLounge)
                    put("customerHelpPoints", station.customerHelpPoints)
                    put("impairedMobilitySetDown", station.impairedMobilitySetDown)
                    put("telephones", station.telephones)
                    put("postBox", station.postBox)
                    put("shops", station.shops)
                    put("heightAdjustedCounter", station.heightAdjustedCounter)
                    put("accessibleTaxis", station.accessibleTaxis)
                    put("cycleHire", station.cycleHire)
                    put("taxi", station.taxi)
                    put("busInterchange", station.busInterchange)
                    put("airport", station.airport)
                    put("carParking", station.carParking)
                    put("carParkName", station.carParkName)
                    put("carParkOperator", station.carParkOperator)
                    put("carParkTotal", station.carParkTotal)
                    put("cycleSpaces", station.cycleSpaces)
                    put("cycleSheltered", station.cycleSheltered)
                }
            )
        }
    }
    post("/kb/station/cache/clear") {
        if (!call.checkAuth()) return@post
        KbClient.clearStationCache()
        call.respond(mapOf("status" to "ok"))
    }
    get("/kb/station/{crs}/raw") {
        val crs = call.validCrs() ?: return@get
        val raw = withContext(Dispatchers.IO) { KbClient.getRawStation(crs) }
        if (raw == null) {
            call.respond(HttpStatusCode.NotFound, "Station $crs not found")
        } else {
            call.respondText(raw, ContentType.Application.Xml)
        }
    }
    get("/kb/station/{crs}/messages") {
        val crs = call.validCrs() ?: return@get
        val msgs = withContext(Dispatchers.IO) { KbClient.getStationMessages(crs) }
        if (msgs == null) {
            call.respond(
                buildJsonObject {
                    put("crs", crs)
                    put("disruptions", buildJsonArray {})
                    put("stationAlerts", "")
                }
            )
        } else {
            call.respond(
                buildJsonObject {
                    put("crs", msgs.crs)
                    put("stationAlerts", if (msgs.disruptions.isEmpty()) msgs.stationAlerts else "")
                    put("disruptions", buildJsonArray {
                        msgs.disruptions.forEach { d ->
                            add(buildJsonObject {
                                put("category", d.category)
                                put("severity", d.severity)
                                put("summary", d.summary)
                            })
                        }
                    })
                }
            )
        }
    }
}

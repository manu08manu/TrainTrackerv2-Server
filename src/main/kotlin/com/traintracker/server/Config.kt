package com.traintracker.server

object Config {
    private fun env(name: String): String =
        System.getenv(name) ?: error("Required env var $name is not set")

    val port: Int = System.getenv("PORT")?.toIntOrNull() ?: 8080

    val kafkaBootstrap: String = env("KAFKA_BOOTSTRAP")
    val kafkaUsername: String = env("KAFKA_USERNAME")
    val kafkaPassword: String = env("KAFKA_PASSWORD")
    val kafkaGroupIdTrust: String = env("KAFKA_GROUP_ID_TRUST")
    val kafkaGroupIdVstp: String = env("KAFKA_GROUP_ID_VSTP")
    val kafkaGroupIdDarwin: String = env("KAFKA_GROUP_ID_DARWIN")
    val kafkaGroupIdAllocation: String = env("KAFKA_GROUP_ID_ALLOCATION")
    val kafkaGroupIdTd: String = env("KAFKA_GROUP_ID_TD")

    val scheduleUrl: String = env("SCHEDULE_URL")
    val scheduleUsername: String = env("SCHEDULE_USERNAME")
    val schedulePassword: String = env("SCHEDULE_PASSWORD")


    val sqlitePath: String = System.getenv("SQLITE_PATH") ?: "./traintracker.db"

    val hspApiKey: String = System.getenv("HSP_API_KEY") ?: ""

    val kbDisruptionsKey: String = System.getenv("KB_DISRUPTIONS_KEY") ?: ""
    val kbDisruptionsUrl: String = System.getenv("KB_DISRUPTIONS_URL") ?: ""
    val kbStationsKey: String = System.getenv("KB_STATIONS_KEY") ?: ""
    val kbStationsSecret: String = System.getenv("KB_STATIONS_SECRET") ?: ""
    val kbStationsUrl: String = System.getenv("KB_STATIONS_URL") ?: ""
    val kbTocKey: String = System.getenv("KB_TOC_KEY") ?: ""
    val kbTocSecret: String = System.getenv("KB_TOC_SECRET") ?: ""
    val kbTocTokenUrl: String = System.getenv("KB_TOC_TOKEN_URL") ?: ""
    val kbTocUrl: String = System.getenv("KB_TOC_URL") ?: ""
}

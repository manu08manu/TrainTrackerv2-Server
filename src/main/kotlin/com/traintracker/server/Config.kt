package com.traintracker.server

object Config {
    // HTTP
    val port: Int = System.getenv("PORT")?.toIntOrNull() ?: 8080

    // Kafka — shared across TRUST, VSTP, Allocation, Darwin
    val kafkaBootstrap: String = env("KAFKA_BOOTSTRAP")
    val kafkaUsername:  String = env("KAFKA_USERNAME")
    val kafkaPassword:  String = env("KAFKA_PASSWORD")
    val kafkaGroupId:   String = env("KAFKA_GROUP_ID")

    // CIF schedule download
    val scheduleUrl:      String = env("SCHEDULE_URL")
    val scheduleUsername: String = env("SCHEDULE_USERNAME")
    val schedulePassword: String = env("SCHEDULE_PASSWORD")

    // Database
    val oracleDbUrl:      String? = System.getenv("ORACLE_DB_URL")
    val oracleDbUser:     String? = System.getenv("ORACLE_DB_USER")
    val oracleDbPassword: String? = System.getenv("ORACLE_DB_PASSWORD")
    val oracleWalletDir:  String? = System.getenv("ORACLE_WALLET_DIR")
    val sqlitePath:       String  = System.getenv("SQLITE_PATH") ?: "./traintracker.db"

    // HSP
    val hspApiKey: String = System.getenv("HSP_API_KEY") ?: ""

    // Knowledgebase
    val kbDisruptionsKey: String = System.getenv("KB_DISRUPTIONS_KEY") ?: ""
    val kbDisruptionsUrl: String = System.getenv("KB_DISRUPTIONS_URL") ?: ""
    val kbStationsKey:    String = System.getenv("KB_STATIONS_KEY") ?: ""
    val kbStationsSecret: String = System.getenv("KB_STATIONS_SECRET") ?: ""
    val kbStationsUrl:    String = System.getenv("KB_STATIONS_URL") ?: ""
    val kbTocKey:         String = System.getenv("KB_TOC_KEY") ?: ""
    val kbTocSecret:      String = System.getenv("KB_TOC_SECRET") ?: ""
    val kbTocTokenUrl:    String = System.getenv("KB_TOC_TOKEN_URL") ?: ""
    val kbTocUrl:         String = System.getenv("KB_TOC_URL") ?: ""

    val useOracle: Boolean get() = oracleDbUrl != null

    private fun env(name: String): String =
        System.getenv(name) ?: error("Required env var $name is not set")
}

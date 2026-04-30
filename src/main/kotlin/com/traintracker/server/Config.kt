package com.traintracker.server

/**
 * All configuration is read from environment variables.
 * This means no credentials are stored in code or config files.
 *
 * Required on Oracle VM (set via /etc/environment or systemd unit):
 *   TRUST_BOOTSTRAP   — Confluent Cloud bootstrap server
 *   TRUST_USERNAME    — NR RDM Kafka username
 *   TRUST_PASSWORD    — NR RDM Kafka password
 *   VSTP_USERNAME     — NR RDM VSTP Kafka username (may differ from TRUST)
 *   VSTP_PASSWORD     — NR RDM VSTP Kafka password
 *   ALLOCATION_USERNAME — NR RDM Allocation Kafka username
 *   ALLOCATION_PASSWORD — NR RDM Allocation Kafka password
 *   SCHEDULE_URL      — CIF full daily URL
 *   SCHEDULE_USERNAME — NR Open Data username
 *   SCHEDULE_PASSWORD — NR Open Data password
 *
 * Oracle DB (optional — falls back to SQLite if omitted):
 *   ORACLE_DB_URL      — jdbc:oracle:thin:@(description=...)
 *   ORACLE_DB_USER     — DB username
 *   ORACLE_DB_PASSWORD — DB password
 *   ORACLE_WALLET_DIR  — Path to unzipped wallet directory (for mTLS)
 *
 * Optional:
 *   PORT               — HTTP port (default 8080)
 *   SQLITE_PATH        — SQLite DB path (default ./traintracker.db)
 */
object Config {
    // HTTP
    val port: Int = System.getenv("PORT")?.toIntOrNull() ?: 8080

    // Kafka — TRUST
    val trustBootstrap: String = env("TRUST_BOOTSTRAP")
    val trustUsername:  String = env("TRUST_USERNAME")
    val trustPassword:  String = env("TRUST_PASSWORD")
    val trustGroupId:   String = env("TRUST_GROUP_ID")

    // Kafka — VSTP
    val vstpUsername: String = env("VSTP_USERNAME")
    val vstpPassword: String = env("VSTP_PASSWORD")
    val vstpGroupId:  String = env("VSTP_GROUP_ID")
    val vstpBootstrap: String = env("VSTP_BOOTSTRAP")

    // Kafka — Allocation
    val allocationUsername: String = env("ALLOCATION_USERNAME")
    val allocationPassword: String = env("ALLOCATION_PASSWORD")
    val allocationGroupId:  String = System.getenv("ALLOCATION_GROUP_ID") ?: "traintracker-alloc"

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


    // Knowledgebase feeds
    val kbNsiKey:          String = System.getenv("KB_NSI_KEY") ?: ""
    val kbNsiSecret:       String = System.getenv("KB_NSI_SECRET") ?: ""
    val kbNsiTokenUrl:     String = System.getenv("KB_NSI_TOKEN_URL") ?: ""
    val kbNsiUrl:          String = System.getenv("KB_NSI_URL") ?: ""
    val kbIncidentsKey:    String = System.getenv("KB_INCIDENTS_KEY") ?: ""
    val kbIncidentsSecret: String = System.getenv("KB_INCIDENTS_SECRET") ?: ""
    val kbIncidentsTokenUrl: String = System.getenv("KB_INCIDENTS_TOKEN_URL") ?: ""
    val kbIncidentsUrl:    String = System.getenv("KB_INCIDENTS_URL") ?: ""
    val kbStationsKey:     String = System.getenv("KB_STATIONS_KEY") ?: ""
    val kbStationsSecret:  String = System.getenv("KB_STATIONS_SECRET") ?: ""
    val kbStationsUrl:     String = System.getenv("KB_STATIONS_URL") ?: ""
    val kbTocKey:          String = System.getenv("KB_TOC_KEY") ?: ""
    val kbTocSecret:       String = System.getenv("KB_TOC_SECRET") ?: ""
    val kbTocTokenUrl:     String = System.getenv("KB_TOC_TOKEN_URL") ?: ""
    val kbTocUrl:          String = System.getenv("KB_TOC_URL") ?: ""

    val useOracle: Boolean get() = oracleDbUrl != null

    private fun env(name: String): String =
        System.getenv(name) ?: error("Required env var $name is not set")
}

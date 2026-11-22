package com.traintracker.server

import java.io.File

/** Settings from the environment (see /opt/traintracker/env.conf) and fixed file locations. */
object Config {
    private fun env(name: String): String =
        System.getenv(name) ?: error("Required env var $name is not set")

    private fun optional(name: String): String = System.getenv(name) ?: ""

    /** Web pages, static assets and data files the server reads and writes. */
    const val DATA_DIR: String = "/opt/traintracker"

    /** The nightly full schedule, copied from the RDM file. */
    val SCHEDULE_CACHE: File = File(DATA_DIR, "schedule_cache.gz")

    /** Local RDM feed - the daily full schedule JSON gzip. Override with SCHEDULE_SOURCE_PATH if needed. */
    val scheduleSourcePath: String =
        System.getenv("SCHEDULE_SOURCE_PATH") ?: "/srv/rdm/in/CIF_ALL_FULL_DAILY_toc-full.json.gz"

    val port: Int = System.getenv("PORT")?.toIntOrNull() ?: 8080

    val kafkaBootstrap: String = env("KAFKA_BOOTSTRAP")
    val kafkaUsername: String = env("KAFKA_USERNAME")
    val kafkaPassword: String = env("KAFKA_PASSWORD")
    val kafkaGroupIdVstp: String = env("KAFKA_GROUP_ID_VSTP")
    val kafkaGroupIdDarwin: String = env("KAFKA_GROUP_ID_DARWIN")
    val kafkaGroupIdAllocation: String = env("KAFKA_GROUP_ID_ALLOCATION")
    val kafkaGroupIdTd: String = env("KAFKA_GROUP_ID_TD")
    /** NWR LINX EI: the source of train activations, movements, cancellations and changes of origin. */
    val kafkaGroupIdLinx: String = env("KAFKA_GROUP_ID_LINX")


    val sqlitePath: String = System.getenv("SQLITE_PATH") ?: "./traintracker.db"


    val kbDisruptionsKey: String = optional("KB_DISRUPTIONS_KEY")
    val kbDisruptionsUrl: String = optional("KB_DISRUPTIONS_URL")
    val kbStationsKey: String = optional("KB_STATIONS_KEY")
    val kbStationsSecret: String = optional("KB_STATIONS_SECRET")
    val kbStationsUrl: String = optional("KB_STATIONS_URL")
    val kbTocKey: String = optional("KB_TOC_KEY")
    val kbTocSecret: String = optional("KB_TOC_SECRET")
    val kbTocTokenUrl: String = optional("KB_TOC_TOKEN_URL")
    val kbTocUrl: String = optional("KB_TOC_URL")
}

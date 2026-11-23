package com.traintracker.server

/**
 * How long data is kept. Change DAYS and everything follows.
 * Keep 61 to show 60: yesterday's punctuality isn't built until noon, so the extra day means
 * 60 days are available even in the morning.
 */
object Retention {
    const val DAYS = 61L

    /** Default window shown to users (league, station card, service card). */
    const val DEFAULT_DAYS = 30

    /** Max punctuality days shown, e.g. via search or the 60-day league tab: one less than kept. */
    const val PUNCTUALITY_SHOWN_DAYS = DAYS - 1

    // Everything else is kept for DAYS. Each has its own name so one can be changed on its own later.
    const val PUNCTUALITY_STORED_DAYS = DAYS
    const val MOVEMENT_DAYS = DAYS
    const val TRAIN_ACTIVATION_DAYS = 31L
    const val TD_EVENT_DAYS = DAYS
    const val ALLOCATION_DAYS = DAYS
    const val EXPIRED_SCHEDULE_DAYS = DAYS.toInt()
    const val FEEDBACK_DISMISSED_DAYS = DAYS.toInt()
    const val LINX_MODIFICATION_DAYS = DAYS.toInt()
    const val SERVICE_HISTORY_DAYS = DEFAULT_DAYS

    /** League periods users can pick. Default is DEFAULT_DAYS; longest is PUNCTUALITY_SHOWN_DAYS. */
    val LEAGUE_PERIODS = setOf(1, 7, DEFAULT_DAYS, PUNCTUALITY_SHOWN_DAYS.toInt())
}

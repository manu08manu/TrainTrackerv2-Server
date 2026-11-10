package com.traintracker.server

/**
 * How long data is kept. Change DAYS and everything follows.
 * Keep 31 to show 30: yesterday's punctuality isn't built until noon, so the extra day means
 * 30 days are available even in the morning.
 */
object Retention {
    const val DAYS = 31L

    /** Punctuality days shown to users: one less than kept. */
    const val PUNCTUALITY_SHOWN_DAYS = DAYS - 1

    // Everything else is kept for DAYS. Each has its own name so one can be changed on its own later.
    const val PUNCTUALITY_STORED_DAYS = DAYS
    const val MOVEMENT_DAYS = DAYS
    const val TRAIN_ACTIVATION_DAYS = DAYS
    const val TD_EVENT_DAYS = DAYS
    const val ALLOCATION_DAYS = DAYS
    const val EXPIRED_SCHEDULE_DAYS = DAYS.toInt()
    const val FEEDBACK_DISMISSED_DAYS = DAYS.toInt()
    const val LINX_MODIFICATION_DAYS = DAYS.toInt()
    const val SERVICE_HISTORY_DAYS = DAYS.toInt()

    /** League periods users can pick. The longest should not exceed PUNCTUALITY_SHOWN_DAYS. */
    val LEAGUE_PERIODS = setOf(1, 7, PUNCTUALITY_SHOWN_DAYS.toInt())
}

package tv.own.owntv.features.settings

import tv.own.owntv.core.settings.EpgAutoRefresh
import tv.own.owntv.core.settings.EpgRefresh

/**
 * How often a newly added guide refreshes: daily. A guide left on Off keeps the programmes it first
 * downloaded and runs dry within a day or two, which reads as a broken guide. Existing guides keep
 * whatever was chosen for them.
 */
internal val NEW_GUIDE_REFRESH = EpgRefresh(EpgAutoRefresh.HOURS_24)

package tv.own.owntv.features.epg

/**
 * Names the hand-made channel order the TV Guide keeps for a view Live TV has no order for: All
 * channels, Catch-up, History, or the Catch-up / Favourites-first sorts. Stored with the other list
 * orders (content_order), so it belongs to the profile and goes with its backups.
 */
internal object GuideOrderKeys {
    private const val PREFIX = "guide:"
    private const val SEP = "|"

    fun of(view: String, sort: String): String = PREFIX + view + SEP + sort

    fun isGuideOwn(key: String): Boolean = key.startsWith(PREFIX)
}

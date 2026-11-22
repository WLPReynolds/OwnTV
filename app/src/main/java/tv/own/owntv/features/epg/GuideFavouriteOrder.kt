package tv.own.owntv.features.epg

/**
 * The guide shows only some of the favourites (it leaves out channels with no programmes, hidden
 * channels, and, in Favourites first inside a category, every favourite outside that category), but the
 * Favorites order is one list shared with Live TV. A Move in the guide must therefore not save just the
 * channels on screen, or every favourite that isn't visible loses its place.
 *
 * [mergeVisibleFavouriteOrder] takes the [full] Favorites order as it stands and the [visible] favourites
 * in their new order, and puts the visible ones back into the slots they already held, in their new
 * order. Every other favourite keeps its slot. A visible id that isn't in [full] yet (a favourite added
 * since the order was read) goes on the end. A repeated id in [full] keeps its first slot only.
 */
internal fun mergeVisibleFavouriteOrder(full: List<Long>, visible: List<Long>): List<Long> {
    val distinctFull = full.distinct()
    val fullSet = distinctFull.toHashSet()
    val visibleSet = visible.toHashSet()
    // The visible favourites that already hold a slot, in their new order: they fill those slots.
    val fillers = visible.filter { it in fullSet }.distinct().iterator()
    val merged = ArrayList<Long>(distinctFull.size + visible.size)
    for (id in distinctFull) merged += if (id in visibleSet) fillers.next() else id
    // Favourites not in the stored order yet go on the end, in their on-screen order.
    visible.distinct().filterTo(merged) { it !in fullSet }
    return merged
}

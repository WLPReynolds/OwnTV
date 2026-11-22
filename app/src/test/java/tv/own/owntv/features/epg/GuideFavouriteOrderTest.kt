package tv.own.owntv.features.epg

import org.junit.Assert.assertEquals
import org.junit.Test

class GuideFavouriteOrderTest {

    @Test
    fun `hidden favourites keep their slots when visible ones swap`() {
        // 2 and 4 are visible in the guide (say, a Sports category); 4 moved above 2.
        val merged = mergeVisibleFavouriteOrder(full = listOf(1L, 2, 3, 4, 5), visible = listOf(4L, 2))
        assertEquals(listOf(1L, 4, 3, 2, 5), merged)
    }

    @Test
    fun `all favourites visible gives the on-screen order`() {
        val merged = mergeVisibleFavouriteOrder(full = listOf(1L, 2, 3), visible = listOf(3L, 1, 2))
        assertEquals(listOf(3L, 1, 2), merged)
    }

    @Test
    fun `a move with nothing changed leaves the order alone`() {
        val full = listOf(9L, 7, 5, 3)
        assertEquals(full, mergeVisibleFavouriteOrder(full, visible = listOf(7L, 3)))
    }

    @Test
    fun `a visible favourite missing from the full order goes on the end`() {
        val merged = mergeVisibleFavouriteOrder(full = listOf(1L, 2, 3), visible = listOf(8L, 2))
        assertEquals(listOf(1L, 2, 3, 8), merged)
    }

    @Test
    fun `no favourites lost and none duplicated`() {
        val full = listOf(5L, 1, 5, 2, 3)
        val merged = mergeVisibleFavouriteOrder(full, visible = listOf(3L, 5))
        assertEquals(listOf(3L, 1, 2, 5), merged)
        assertEquals(merged.size, merged.toSet().size)
    }

    @Test
    fun `empty inputs`() {
        assertEquals(emptyList<Long>(), mergeVisibleFavouriteOrder(emptyList(), emptyList()))
        assertEquals(listOf(4L), mergeVisibleFavouriteOrder(emptyList(), listOf(4L)))
        assertEquals(listOf(1L, 2), mergeVisibleFavouriteOrder(listOf(1L, 2), emptyList()))
    }
}

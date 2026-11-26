package tv.own.owntv.features.more

import android.content.Context
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.tv.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.produceState
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.style.TextOverflow
import androidx.room.useReaderConnection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.koin.compose.koinInject
import tv.own.owntv.core.database.OwnTVDatabase
import tv.own.owntv.ui.theme.StageColors
import tv.own.owntv.ui.theme.stageAccent
import tv.own.owntv.ui.theme.stageText
import tv.own.owntv.ui.stage.StageTile
import tv.own.owntv.ui.theme.mpx
import java.io.File
import java.util.Locale

/*
 * STORAGE BREAKDOWN (diagnostic). Where the app's share of the TV's storage goes: the database split
 * into live channels, movies, series, the guide and the search index, then the image cache, downloads
 * kept in app storage, and everything else. English only, like the guide's timing readout: it is a
 * tool for deciding what to trim, not a finished screen.
 *
 * The bundled SQLite has no dbstat table, so a table's on-disk size can't be read directly. Instead
 * each table's stored bytes are added up (every column of every row), and the database file's real
 * size is shared out in proportion. Indexes and page overhead are spread the same way, so the parts
 * are estimates ("~") that add up to the true total.
 */

/** One line of the breakdown: what it is, its estimated size, and how many rows it holds. */
internal class StoragePart(val label: String, val bytes: Long, val rows: String?)

internal class StorageReport(
    val databaseBytes: Long,
    val databaseParts: List<StoragePart>,
    val imageCacheBytes: Long,
    val downloadsBytes: Long,
    val otherBytes: Long,
) {
    val totalBytes get() = databaseBytes + imageCacheBytes + downloadsBytes + otherBytes
}

/** Which part of the breakdown a table belongs to; null for everything else. */
private fun groupOf(table: String): String? = when {
    "_fts" in table -> "Search index"
    table == "channels" -> "Live channels"
    table == "movies" -> "Movies"
    table == "series" || table == "seasons" || table == "episodes" || table == "series_sort_order" -> "Series"
    table.startsWith("epg_") || table == "tv_provider_programs" -> "Guide"
    else -> null
}

private val GROUP_ORDER = listOf("Live channels", "Movies", "Series", "Guide", "Search index", "Everything else")

internal suspend fun measureStorage(context: Context, db: OwnTVDatabase): StorageReport = withContext(Dispatchers.IO) {
    val dbFile = context.getDatabasePath(OwnTVDatabase.NAME)
    val databaseBytes = listOf("", "-wal", "-shm").sumOf { File(dbFile.path + it).length() }

    // Stored bytes and rows per table. Text and blobs by their byte length, numbers by their text form
    // (close enough to their stored width for a share), plus a few bytes a row for its header.
    class TableStat(val name: String, val rows: Long, val bytes: Double)
    val stats = db.useReaderConnection { conn ->
        val tables = conn.usePrepared(
            "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        ) { st ->
            buildList {
                while (st.step()) {
                    val sql = if (st.isNull(1)) "" else st.getText(1)
                    // A virtual table (FTS) keeps its data in its shadow tables, which are listed too.
                    if (!sql.startsWith("CREATE VIRTUAL", ignoreCase = true)) add(st.getText(0))
                }
            }
        }
        tables.mapNotNull { table ->
            val columns = conn.usePrepared("PRAGMA table_info(`$table`)") { st ->
                buildList { while (st.step()) add(st.getText(1)) }
            }
            if (columns.isEmpty()) return@mapNotNull null
            val payload = columns.joinToString(" + ") { "IFNULL(length(CAST(`$it` AS BLOB)), 0)" }
            conn.usePrepared("SELECT COUNT(*), TOTAL($payload) FROM `$table`") { st ->
                if (!st.step()) null
                else {
                    val rows = st.getLong(0)
                    TableStat(table, rows, st.getDouble(1) + rows * (columns.size + 4.0))
                }
            }
        }
    }

    val allBytes = stats.sumOf { it.bytes }.coerceAtLeast(1.0)
    val byGroup = stats.groupBy { groupOf(it.name) ?: "Everything else" }
    fun rowsOf(vararg tables: String) = tables.sumOf { t -> stats.firstOrNull { it.name == t }?.rows ?: 0L }
    val parts = GROUP_ORDER.mapNotNull { group ->
        val tablesInGroup = byGroup[group] ?: return@mapNotNull null
        val share = tablesInGroup.sumOf { it.bytes } / allBytes
        val rows = when (group) {
            "Live channels" -> count(rowsOf("channels"), "channels")
            "Movies" -> count(rowsOf("movies"), "movies")
            "Series" -> count(rowsOf("series"), "series") + ", " + count(rowsOf("episodes"), "episodes")
            "Guide" -> count(rowsOf("epg_programmes"), "programmes")
            else -> null
        }
        StoragePart(group, (databaseBytes * share).toLong(), rows)
    }

    val imageCacheBytes = dirSize(File(context.cacheDir, "image_cache"))
    val downloadsBytes = dirSize(File(context.getExternalFilesDir(null) ?: context.filesDir, "OwnTV"))
    val dataDir = context.filesDir.parentFile
    // The app's private folder (database, cache, files) and its folder in shared storage (downloads and
    // the external cache live under it), which Android counts in the app's data too.
    val everything = (dataDir?.let(::dirSize) ?: 0L) +
        (context.getExternalFilesDir(null)?.parentFile?.let(::dirSize) ?: 0L)
    val otherBytes = (everything - databaseBytes - imageCacheBytes - downloadsBytes).coerceAtLeast(0L)

    StorageReport(databaseBytes, parts, imageCacheBytes, downloadsBytes, otherBytes)
}

private fun count(n: Long, what: String) = String.format(Locale.ROOT, "%,d %s", n, what)

/** Every file under [dir], following no links. */
private fun dirSize(dir: File): Long =
    if (!dir.exists()) 0L else dir.walkTopDown().onEnter { !java.nio.file.Files.isSymbolicLink(it.toPath()) }
        .filter { it.isFile }.sumOf { it.length() }

private fun megabytes(bytes: Long): String = when {
    bytes >= 1_000_000_000L -> String.format(Locale.ROOT, "%.1f GB", bytes / 1_000_000_000.0)
    bytes >= 10_000_000L -> String.format(Locale.ROOT, "%d MB", bytes / 1_000_000L)
    else -> String.format(Locale.ROOT, "%.1f MB", bytes / 1_000_000.0)
}

/** The tile on the About page: measured each time the page opens (a few seconds on a large playlist). */
@Composable
internal fun StorageBreakdownTile(modifier: Modifier = Modifier) {
    val context = LocalContext.current
    val db: OwnTVDatabase = koinInject()
    val report by produceState<StorageReport?>(null) { value = runCatching { measureStorage(context, db) }.getOrNull() }
    val accent = stageAccent.accent
    StageTile(modifier.width(1000.mpx)) {
        Column {
            Row(horizontalArrangement = Arrangement.spacedBy(12.mpx)) {
                Text("Storage on this TV", style = stageText(22, 800), color = StageColors.Text, maxLines = 1, overflow = TextOverflow.Ellipsis)
                report?.let { Text(megabytes(it.totalBytes), style = stageText(22, 800), color = accent, maxLines = 1, overflow = TextOverflow.Ellipsis) }
            }
            val r = report
            if (r == null) {
                Text("Measuring…", style = stageText(16, 500), color = StageColors.Muted, maxLines = 1, overflow = TextOverflow.Ellipsis, modifier = Modifier.padding(top = 8.mpx))
            } else {
                StorageLine("Database", megabytes(r.databaseBytes), null, strong = true)
                r.databaseParts.forEach { StorageLine("   " + it.label, "~" + megabytes(it.bytes), it.rows) }
                StorageLine("Image cache", megabytes(r.imageCacheBytes), null, strong = true)
                StorageLine("Downloads in app storage", megabytes(r.downloadsBytes), null, strong = true)
                StorageLine("Other files", megabytes(r.otherBytes), null, strong = true)
            }
        }
    }
}

@Composable
private fun StorageLine(label: String, size: String, detail: String?, strong: Boolean = false) {
    Row(Modifier.padding(top = 4.mpx), horizontalArrangement = Arrangement.spacedBy(10.mpx)) {
        Text(label, style = stageText(15.5f, if (strong) 700 else 500), color = if (strong) StageColors.Text else StageColors.Muted,
            maxLines = 1, overflow = TextOverflow.Ellipsis, modifier = Modifier.width(330.mpx))
        Text(size, style = stageText(15.5f, 700), color = StageColors.Text, maxLines = 1, overflow = TextOverflow.Ellipsis, modifier = Modifier.width(150.mpx))
        detail?.let { Text(it, style = stageText(15.5f, 500), color = StageColors.Muted, maxLines = 1, overflow = TextOverflow.Ellipsis) }
    }
}

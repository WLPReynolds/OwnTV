package tv.own.owntv.features.settings

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import tv.own.owntv.core.backup.PlaylistLists
import tv.own.owntv.core.companion.CompanionController
import tv.own.owntv.core.companion.CompanionLink
import tv.own.owntv.core.database.entity.SourceEntity
import tv.own.owntv.core.settings.SettingsRepository
import java.io.File

/**
 * Sends one playlist's category and channel lists to a browser on the same Wi-Fi, and takes its edited
 * hide / rename / order settings back. The transfer rides the same PIN-protected companion server the
 * backup pages use; the file itself is built and applied by [PlaylistLists].
 */
class PlaylistListsViewModel(
    private val lists: PlaylistLists,
    private val companion: CompanionController,
    private val settings: SettingsRepository,
) : ViewModel() {

    sealed interface Outcome {
        data object None : Outcome
        data object Preparing : Outcome
        data class ExportFailed(val detail: String) : Outcome
        data class Imported(val hiddenCategories: Int, val hiddenItems: Int, val renamed: Int, val reordered: Int) : Outcome
        /** The uploaded file is not a playlist edits file (for instance a whole backup was sent). */
        data object NotAnEditsFile : Outcome
        data object ImportFailed : Outcome
    }

    private val _outcome = MutableStateFlow<Outcome>(Outcome.None)
    val outcome: StateFlow<Outcome> = _outcome.asStateFlow()

    val remoteState get() = companion.state
    val remoteFiles get() = companion.backups

    fun clearOutcome() { _outcome.value = Outcome.None }

    /** The export being written, until it has started the server or failed. [stop] cancels it. */
    private var exportJob: Job? = null

    fun startExport(source: SourceEntity) {
        exportJob?.cancel()
        exportJob = viewModelScope.launch {
            _outcome.value = Outcome.Preparing
            val profileId = settings.activeProfileId.first()
            lists.export(source.id, profileId, companion.backupExportDir).fold(
                onSuccess = { file ->
                    // Left while it was still being written: no server for a screen that has closed.
                    ensureActive()
                    companion.startForBackupDownload(CompanionLink.DEFAULT_PORT, file)
                    _outcome.value = Outcome.None
                },
                onFailure = {
                    if (it is CancellationException) throw it // stopped, not failed
                    _outcome.value = Outcome.ExportFailed(it.toString().take(300))
                },
            )
        }
    }

    fun startImport() = companion.startForBackupRestore(CompanionLink.DEFAULT_PORT)

    /** Back or Stop: an export still being written is abandoned before it can start the server. */
    fun stop() {
        exportJob?.cancel()
        exportJob = null
        if (_outcome.value == Outcome.Preparing) _outcome.value = Outcome.None
        companion.stop()
    }

    fun importFile(source: SourceEntity, file: File) {
        viewModelScope.launch {
            val profileId = settings.activeProfileId.first()
            lists.importEdits(file, source.id, profileId).fold(
                onSuccess = { s -> _outcome.value = Outcome.Imported(s.hiddenCategories, s.hiddenItems, s.renamed, s.reordered) },
                onFailure = { e ->
                    _outcome.value = if (e is PlaylistLists.BadFileException) Outcome.NotAnEditsFile else Outcome.ImportFailed
                },
            )
            runCatching { file.delete() }
        }
    }
}

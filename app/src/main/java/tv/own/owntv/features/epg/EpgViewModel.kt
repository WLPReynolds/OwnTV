@file:OptIn(FlowPreview::class, kotlinx.coroutines.ExperimentalCoroutinesApi::class) // debounce, flatMapLatest

package tv.own.owntv.features.epg

import tv.own.owntv.core.live.serialize
import tv.own.owntv.core.epg.displayLogoUrl
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import tv.own.owntv.core.customize.SectionCustomizations
import tv.own.owntv.core.database.dao.ChannelDao
import tv.own.owntv.core.database.dao.EpgDao
import tv.own.owntv.core.database.dao.ProfileDao
import tv.own.owntv.core.database.dao.SourceDao
import tv.own.owntv.core.database.dao.resolveExistingProfileId
import tv.own.owntv.core.epg.CatchupUrl
import tv.own.owntv.core.epg.EpgAutoMatcher
import tv.own.owntv.core.epg.GuideCandidate
import tv.own.owntv.core.epg.GuideCandidates
import tv.own.owntv.core.model.SourceType
import tv.own.owntv.core.parser.XtreamClient
import tv.own.owntv.core.customize.CustomizationStore
import tv.own.owntv.core.customize.CustomizeKeys
import tv.own.owntv.core.customize.railCategories
import tv.own.owntv.core.database.entity.ChannelEntity
import tv.own.owntv.core.database.entity.EpgProgrammeEntity
import tv.own.owntv.core.model.MediaType
import tv.own.owntv.core.network.ConnectivityObserver
import tv.own.owntv.core.repository.EpgRepository
import tv.own.owntv.core.repository.SourceRepository
import tv.own.owntv.core.repository.activeSourceIds
import tv.own.owntv.core.settings.SettingsRepository
import tv.own.owntv.core.settings.GuideWidthShares

sealed interface EpgMessage {
    data object CreateProfile : EpgMessage
    data object AddPlaylist : EpgMessage
    data class NoChannelsForQuery(val query: String) : EpgMessage
    data object MismatchedIds : EpgMessage
}

sealed interface EpgMatchSummary {
    data object CatchupUnavailable : EpgMatchSummary
    data object MatchedNoProgrammes : EpgMatchSummary
    data object AddPlaylist : EpgMatchSummary
    data object NoData : EpgMatchSummary
    data class AutoMatched(val applied: Int, val review: Int) : EpgMatchSummary
    data object AllMatched : EpgMatchSummary
    data class NoMatch(val channelName: String) : EpgMatchSummary
}

data class EpgUiState(
    /** All channels with guide data in the window; each row loads its own programmes lazily. */
    val channels: List<ChannelEntity> = emptyList(),
    val windowStart: Long = 0,
    val windowEnd: Long = 0,
    val now: Long = 0,
    val loading: Boolean = true,
    val refreshing: Boolean = false,
    val message: EpgMessage? = null,
    /** False when the user hasn't added any EPG source yet → the screen shows an "Add EPG" prompt. */
    val hasEpgSources: Boolean = true,
)

/**
 * Drives the EPG guide grid. Loads the active profile's EPG-capable channels and the programmes in a
 * rolling window from the DB — as many days forward as Settings' "Guide days to keep" stores —
 * and can re-download the bulk XMLTV guide via [EpgRepository].
 */
class EpgViewModel(
    private val settings: SettingsRepository,
    private val sourceRepository: SourceRepository,
    private val channelDao: ChannelDao,
    private val epgDao: EpgDao,
    private val profileDao: ProfileDao,
    private val epgRepository: EpgRepository,
    private val epgSourceStore: tv.own.owntv.core.epg.EpgSourceStore,
    private val connectivity: ConnectivityObserver,
    private val customize: CustomizationStore,
    private val sourceDao: SourceDao,
    private val xtream: XtreamClient,
    private val favoriteDao: tv.own.owntv.core.database.dao.FavoriteDao,
    private val userDataWriter: tv.own.owntv.core.backup.UserDataWriter,
    private val categoryDao: tv.own.owntv.core.database.dao.CategoryDao,
    private val streamUrlResolver: tv.own.owntv.core.stalker.StreamUrlResolver,
    private val externalPlayerLauncher: tv.own.owntv.core.player.ExternalPlayerLauncher,
    private val customCategoryDao: tv.own.owntv.core.database.dao.CustomCategoryDao,
    private val recordings: tv.own.owntv.core.recording.RecordingManager,
    /** The provider-guide half of a row, for channels whose stored guide stops short — the app-wide
     *  reader Live TV uses too, so both screens share one cache. */
    private val liveEpgReader: tv.own.owntv.core.live.LiveEpgReader,
    /** Which guide feeds are syncing right now: the grid waits for them instead of reloading per batch. */
    private val epgActivity: tv.own.owntv.core.sync.EpgActivityTracker,
    /** "Recently watched" in the category list reads the same history Live TV does. */
    private val historyDao: tv.own.owntv.core.database.dao.HistoryDao,
    private val reminderManager: tv.own.owntv.core.reminder.ReminderManager,
    private val archiveGuide: tv.own.owntv.core.epg.ArchiveGuideFiller,
    private val contentOrderDao: tv.own.owntv.core.database.dao.ContentOrderDao,
    /** Only for its invalidation tracker: a write to the channel or programme tables drops [channelReads]. */
    private val database: tv.own.owntv.core.database.OwnTVDatabase,
) : ViewModel() {

    /** The categories' search text, held here so it survives the player (see LiveViewModel). */
    private val _categoryQuery = kotlinx.coroutines.flow.MutableStateFlow("")
    val categoryQuery: kotlinx.coroutines.flow.StateFlow<String> = _categoryQuery
    fun setCategoryQuery(query: String) { _categoryQuery.value = query }

    /** The one candidate set the picker and both auto-match paths read — see [GuideCandidates]. */
    private val guideCandidates = GuideCandidates(epgDao)

    /** The match scan itself — core's, so the television, the phone and the single-channel run agree. */
    private val autoMatcher = EpgAutoMatcher(channelDao, guideCandidates)

    // --- Recording, from the guide ---------------------------------------------------------------

    /**
     * This profile's recordings, so a programme cell knows whether it is already spoken for.
     *
     * The whole list rather than a per-programme lookup: the guide draws hundreds of cells and a
     * query each would be hundreds of queries. There are never many recordings.
     */
    val recordingRows: StateFlow<List<tv.own.owntv.core.database.entity.RecordingEntity>> =
        settings.activeProfileId
            .flatMapLatest { pid -> if (pid < 0) flowOf(emptyList()) else recordings.observe(pid) }
            .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), emptyList())

    /** The recording already covering this programme, if there is one. */
    fun recordingFor(
        channel: ChannelEntity,
        programme: EpgProgrammeEntity,
    ): tv.own.owntv.core.database.entity.RecordingEntity? = recordingRows.value.firstOrNull {
        it.channelId == channel.id && it.programmeStartMs == programme.startMs &&
            it.status != tv.own.owntv.core.model.RecordingStatus.CANCELLED
    }

    /**
     * Record this programme — scheduled if it is still to come, pulled from the archive if it has
     * already been on and the channel keeps one.
     *
     * A programme that has already finished and whose channel has no catch-up cannot be recorded at
     * all; [canRecord] is what stops the button being offered for it.
     */
    fun record(channel: ChannelEntity, programme: EpgProgrammeEntity) {
        viewModelScope.launch {
            val pid = currentProfileId() ?: return@launch
            val source = sourceDao.getById(channel.sourceId) ?: return@launch
            val now = System.currentTimeMillis()
            if (programme.stopMs <= now) {
                recordings.recordFromArchive(
                    profileId = pid,
                    channel = channel,
                    programme = programme,
                    source = source,
                    timeZone = settings.resolveCatchupTimeZone(source),
                    xtream = xtream,
                )
                return@launch
            }
            val window = recordings.windowFor(programme.startMs, programme.stopMs)
            recordings.schedule(
                tv.own.owntv.core.database.entity.RecordingEntity(
                    profileId = pid,
                    sourceId = channel.sourceId,
                    channelId = channel.id,
                    channelName = channel.name,
                    channelIconUrl = channel.logoUrl,
                    epgChannelId = channel.epgChannelId,
                    streamUrl = channel.streamUrl,
                    httpHeaders = channel.httpHeaders,
                    title = programme.title,
                    description = programme.description,
                    programmeStartMs = programme.startMs,
                    programmeStopMs = programme.stopMs,
                    startMs = window.first,
                    stopMs = window.last,
                ),
            )
        }
    }

    /**
     * Whether Record can be offered for this programme at all: still to come, or already been on and
     * within a catch-up channel's archive.
     */
    fun canRecord(channel: ChannelEntity, programme: EpgProgrammeEntity, now: Long): Boolean =
        programme.stopMs > now || canCatchup(channel, programme, now)

    fun stopRecording(recording: tv.own.owntv.core.database.entity.RecordingEntity) =
        recordings.stop(recording)

    fun cancelRecording(recording: tv.own.owntv.core.database.entity.RecordingEntity) =
        recordings.cancel(recording)

    /** The standing "record every showing" rule covering this programme, or null. */
    suspend fun seriesRuleFor(
        channel: ChannelEntity,
        programme: EpgProgrammeEntity,
    ): tv.own.owntv.core.database.entity.RecordingRuleEntity? {
        val pid = currentProfileId() ?: return null
        return recordings.ruleFor(pid, channel.id, programme.title)
    }

    /** Record every future showing of this title on this channel. */
    fun recordSeries(channel: ChannelEntity, programme: EpgProgrammeEntity) {
        viewModelScope.launch {
            val pid = currentProfileId() ?: return@launch
            recordings.addSeriesRule(pid, channel, programme.title)
        }
    }

    /** Stop the standing rule, and drop the showings it had queued but not yet recorded. */
    fun stopSeries(rule: tv.own.owntv.core.database.entity.RecordingRuleEntity) {
        viewModelScope.launch { recordings.removeSeriesRule(rule) }
    }

    /** Every windowed guide read this screen makes. The caches around it stay here — what to keep
     *  depends on how the grid scrolls, which is the screen's business, not core's. */
    private val guideReader =
        tv.own.owntv.core.live.GuideReader(epgDao, epgSourceStore, sourceDao, liveEpgReader)

    // This profile's customizations — shared by the Guide picker, guide rows, and manual EPG match.
    // It must be initialized before guideCategories (Kotlin property initializers run top-to-bottom).
    private val custom: StateFlow<SectionCustomizations> = settings.activeProfileId
        .flatMapLatest { pid -> if (pid < 0) flowOf(SectionCustomizations()) else customize.observe(pid, MediaType.LIVE) }
        .stateIn(viewModelScope, SharingStarted.Eagerly, SectionCustomizations())

    /** Global guide shift in minutes; a per-channel override in [custom] wins over it. */
    private val epgOffset: StateFlow<Int> = settings.epgOffsetMinutes
        .stateIn(viewModelScope, SharingStarted.Eagerly, 0)

    /** Guide category filter, from the same list as Live TV: null = All channels. */
    private val _categoryFilter = MutableStateFlow<tv.own.owntv.core.live.LiveKey?>(null)
    val categoryFilter: StateFlow<tv.own.owntv.core.live.LiveKey?> = _categoryFilter.asStateFlow()

    val guideWidthShares: StateFlow<GuideWidthShares?> = combine(
        settings.guideWidthEnabled,
        settings.guideWidthShares,
    ) { enabled, shares -> shares.takeIf { enabled } }
        .stateIn(viewModelScope, SharingStarted.Eagerly, null)

    /** The TV Guide's own category while the side menu's Favourites borrows this view model. */
    var guideCategoryBeforeFavourites: tv.own.owntv.core.live.LiveKey? = null

    fun setCategoryFilter(key: tv.own.owntv.core.live.LiveKey?) {
        val next = key.takeUnless { it == tv.own.owntv.core.live.LiveKey.All }
        _categoryFilter.value = next
        // The list on screen belongs to another category until the reload lands: say so at once, so the
        // screen covers it instead of showing, say, Favourites under the TV Guide's Sports.
        _switchingCategory.value = builtFilter !== NotBuilt && next != builtFilter && _state.value.channels.isNotEmpty()
    }

    /** The category the list on screen was built for ([NotBuilt] before the first load). */
    @Volatile private var builtFilter: Any? = NotBuilt
    private object NotBuilt

    /** True while the list on screen is another category's and the one asked for is still loading. */
    private val _switchingCategory = MutableStateFlow(false)
    val switchingCategory: StateFlow<Boolean> = _switchingCategory.asStateFlow()

    // Channels hidden from the guide's menus that a reload must keep out until it has read the store
    // after their hide was written ([hiddenWritten]); then the store's own hidden list takes over, so an
    // unhide in Settings brings the channel back as usual.
    private val justHidden: MutableSet<Long> = java.util.Collections.synchronizedSet(HashSet())
    private val hiddenWritten: MutableSet<Long> = java.util.Collections.synchronizedSet(HashSet())
    private val _state = MutableStateFlow(EpgUiState())
    val state: StateFlow<EpgUiState> = _state.asStateFlow()

    private val _query = MutableStateFlow("")
    val query: StateFlow<String> = _query.asStateFlow()

    /**
     * The rows the grid has already read, newest-used first and **hard-capped**.
     *
     * This used to be an unbounded [java.util.concurrent.ConcurrentHashMap] pre-filled by one batch
     * query that read the entire guide window. On a real television that batch was 349,077
     * programmes in a single list, and the app died of an OutOfMemoryError inside it. Rows are now
     * read one channel at a time as they scroll into view, and the cache is an LRU: the grid keeps
     * what the user is actually looking at and forgets the rest, so memory is a function of the
     * screen rather than of the catalogue.
     *
     * Keyed by channel id and shift, not by EPG id: two channels can share one guide channel, and the
     * same channel at two offsets is two different rows.
     */
    private val rowCache = object : LinkedHashMap<String, List<EpgProgrammeEntity>>(64, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, List<EpgProgrammeEntity>>?) =
            size > MAX_CACHED_ROWS
    }

    private fun cacheKey(channel: ChannelEntity, shift: Int) = "${channel.id}|$shift"

    private fun cachedRow(key: String): List<EpgProgrammeEntity>? = synchronized(rowCache) { rowCache[key] }

    private fun putRow(key: String, rows: List<EpgProgrammeEntity>) {
        synchronized(rowCache) { rowCache[key] = rows }
    }

    private fun clearRows() {
        synchronized(rowCache) { rowCache.clear() }
    }

    private fun cachedRowCount(): Int = synchronized(rowCache) { rowCache.size }

    /**
     * Ask for the past days a catch-up [channel]'s guide feed lacks, from its provider's archive. Called
     * for a row that stays on screen, so the past is there by the time the user scrolls back to it; the
     * queue, its pacing and the "once every few hours per channel" rule are core's
     * ([tv.own.owntv.core.epg.ArchiveGuideFiller]).
     */
    fun requestArchiveGuide(channel: ChannelEntity) = archiveGuide.request(channel, custom.value)

    /** Drop the cached rows of one channel (every shift), so the next read of that row goes to the DB. */
    private fun dropRows(channelId: Long) {
        val prefix = "$channelId|"
        synchronized(rowCache) { rowCache.keys.removeAll { it.startsWith(prefix) } }
    }

    override fun onCleared() {
        // Rows of a guide nobody is looking at any more are not asked for.
        archiveGuide.forgetPending()
    }

    /** The row prefetch in flight. One at a time — see [prefetchAfter]. */
    private var prefetchJob: kotlinx.coroutines.Job? = null

    /** The load in flight, so the next one can cancel it instead of racing it. */
    private var loadJob: kotlinx.coroutines.Job? = null
    private val loadMutex = kotlinx.coroutines.sync.Mutex()

    @Volatile private var loadedSourceIds: List<Long> = emptyList()
    // The window the cached rows belong to. A sort / filter / category change keeps the same window,
    // so the rows stay valid and nothing is re-read.
    @Volatile private var cachedWindow: Pair<Long, Long>? = null
    @Volatile private var lastStored = -1 // stored programme count the cache was built from (data-change guard)
    // Bumped when cached rows are dropped, so visible rows know to ask again.
    private val _cacheRevision = MutableStateFlow(0)
    val cacheRevision: StateFlow<Int> = _cacheRevision.asStateFlow()

    /** Minutes this channel's guide is shifted by (per-channel override, else the global offset). */
    private fun shiftFor(channel: ChannelEntity): Int =
        tv.own.owntv.core.epg.EpgShift.minutesFor(custom.value, channel, epgOffset.value)

    /** Synchronous cache peek — lets a re-composed row render instantly without a loading flash. */
    fun cachedProgrammes(channel: ChannelEntity): List<EpgProgrammeEntity>? =
        cachedRow(cacheKey(channel, shiftFor(channel)))

    /**
     * One row's programmes, read as the row scrolls into view.
     *
     * This is now the **only** way the grid gets programmes. It is one indexed query per channel, so
     * eight visible rows cost eight small reads instead of one read of the whole database — which is
     * what the deleted batch did, and what ran the television out of memory.
     */
    suspend fun programmesFor(channel: ChannelEntity): List<EpgProgrammeEntity> {
        val s = _state.value
        val shift = shiftFor(channel)
        val key = cacheKey(channel, shift)
        cachedRow(key)?.let { return it }
        // The read itself — window, shift and all — is core's; only what to keep is this screen's.
        val list = guideReader.row(channel, custom.value, epgOffset.value, s.windowStart, s.windowEnd)
        // An empty answer is not kept: while many rows load at once the provider fallback can still be
        // on its way, and a cached empty row stayed blank until something else re-read it.
        if (list.isNotEmpty()) putRow(key, list)
        prefetchAfter(channel)
        return list
    }

    /**
     * Warm the rows just below the one that was asked for, so flinging the grid does not draw blanks.
     *
     * One job at a time, and the newest wins: rows come into view one after another, so each request
     * cancels the previous run rather than adding to it. That is what keeps this a prefetch and not
     * the N+1 query storm the deleted batch load was originally written to prevent — at most
     * [PREFETCH_AHEAD] small indexed reads are ever in flight, sequentially, and the LRU bounds what
     * they cost to keep.
     */
    private fun prefetchAfter(channel: ChannelEntity) {
        val channels = _state.value.channels
        val from = channels.indexOfFirst { it.id == channel.id }
        if (from < 0) return
        val next = channels.subList((from + 1).coerceAtMost(channels.size), (from + 1 + PREFETCH_AHEAD).coerceAtMost(channels.size))
        if (next.isEmpty()) return
        prefetchJob?.cancel()
        prefetchJob = viewModelScope.launch {
            val s = _state.value
            for (ahead in next) {
                val key = cacheKey(ahead, shiftFor(ahead))
                if (cachedRow(key) != null) continue
                guideReader.row(ahead, custom.value, epgOffset.value, s.windowStart, s.windowEnd)
                    .takeIf { it.isNotEmpty() }?.let { putRow(key, it) }
            }
        }
    }

    /** One channel's programmes from [dayStart] for a day, whole — synopsis and details — for the
     *  Program details page. Not cached: the page reads one day at a time, on demand. */
    suspend fun dayProgrammes(channel: ChannelEntity, dayStart: Long): List<EpgProgrammeEntity> =
        guideReader.row(channel, custom.value, epgOffset.value, dayStart, dayStart + DAY_MS, withText = true)

    /** The whole programme — synopsis and details — for the programme block (the grid load drops both). */
    suspend fun programmeFull(programmeId: Long): EpgProgrammeEntity? =
        runCatching { epgDao.programmeById(programmeId) }.getOrNull()

    init {
        // Any write to the channels (a playlist refresh) or the programmes (a guide sync, a match top-up)
        // makes the cached reads stale; the next load reads them again.
        database.invalidationTracker.createFlow("channels", "epg_programmes", emitInitialState = false)
            .onEach { channelReadsDirty = true }
            .launchIn(viewModelScope)
        // Re-filter the grid as the user types (DB-level, so it searches ALL guide channels, not
        // just the visible rows). drop(1): the screen triggers the initial load itself.
        _query
            .drop(1)
            .debounce(300)
            .distinctUntilChanged()
            .onEach { load() }
            .launchIn(viewModelScope)
        // Reload the guide when its own sort, or (for the LIVE_TV mode) the Live sort, changes.
        // drop(1): the screen triggers the initial load itself.
        settings.sortGuide
            .drop(1)
            .distinctUntilChanged()
            .onEach { load() }
            .launchIn(viewModelScope)
        settings.guideShowEmpty
            .drop(1)
            .distinctUntilChanged()
            .onEach { load() }
            .launchIn(viewModelScope)
        // Reload when the category filter changes (#8).
        _categoryFilter
            .drop(1)
            .distinctUntilChanged()
            .onEach { load() }
            .launchIn(viewModelScope)
        // Reload the guide when the active-playlist filter (Settings "Default" / Browse picker) changes,
        // so the grid narrows to the chosen playlist's channels (or back to all). drop(1): initial load
        // is triggered by the screen.
        settings.defaultSourceId
            .drop(1)
            .distinctUntilChanged()
            .onEach { load() }
            .launchIn(viewModelScope)
        // Reload when the guide's own data changes underneath an open screen — a feed added,
        // re-synced or deleted. Leaving the Guide and coming back already picks it up (`load()` runs
        // on every mount and compares the stored count), but a background auto-refresh finishing
        // while the grid is on screen had no way to show itself. Room reports every write to the
        // programme table, so this waits for the writes to stop rather than reloading per batch, and
        // `load()`'s own stored-count guard means a settled sync that changed nothing costs nothing.
        epgSourceStore.sources
            .map { sources -> sources.map { it.id } }
            .distinctUntilChanged()
            .flatMapLatest { ids ->
                if (ids.isEmpty()) flowOf(0) else combine(ids.map { epgDao.countForSource(it) }) { it.sum() }
            }
            // A big feed writes for minutes, one batch every few seconds; the debounce alone rebuilt the
            // whole grid (≈2 s of database work) every time a pause crossed it, all through the sync.
            // So nothing reloads while a feed is still syncing — the change is picked up when it ends.
            .combine(epgActivity.active.map { it.isEmpty() }.distinctUntilChanged()) { count, idle -> count to idle }
            .filter { (_, idle) -> idle }
            .map { (count, _) -> count }
            .distinctUntilChanged()
            .debounce(GUIDE_DATA_SETTLE_MS)
            // The first value is the guide as it already stands — unless a sync was running when the
            // screen opened, in which case the first value is that sync's result and must be loaded.
            .drop(if (epgActivity.active.value.isEmpty()) 1 else 0)
            .onEach { load(fresh = true) }
            .launchIn(viewModelScope)
        // Re-read a row when its channel's past days arrive from the provider's archive (asked for by the
        // rows on screen, see requestArchiveGuide). They are stored apart from every feed, so the stored
        // count load() compares does not move: that channel's cached rows are dropped instead, and the
        // other visible rows answer from the cache.
        archiveGuide.added
            .onEach {
                dropRows(it)
                _cacheRevision.value++
            }
            .launchIn(viewModelScope)
        // Reload when a category lock closes or opens (the relock on leaving the app, a PIN entry), so
        // the grid and the zap list armed from it follow. drop(1): the screen triggers the initial load.
        settings.activeProfileId
            .flatMapLatest { pid ->
                if (pid < 0) flowOf(emptySet()) else customize.observeVisible(pid, MediaType.LIVE).map { it.sealedCategories }
            }
            .distinctUntilChanged()
            .drop(1)
            .onEach { load() }
            .launchIn(viewModelScope)
    }

    /** The Guide's current sort, for the header button. */
    val sortGuide: StateFlow<SettingsRepository.GuideSort> = settings.sortGuide
        .stateIn(viewModelScope, SharingStarted.Eagerly, SettingsRepository.GuideSort.LIVE_TV)

    /** Order ▾ › Show › Channels without guide: list every channel, so a blank one can be matched from here. */
    val showEmpty: StateFlow<Boolean> = settings.guideShowEmpty
        .stateIn(viewModelScope, SharingStarted.Eagerly, false)

    fun setShowEmpty(show: Boolean) {
        viewModelScope.launch { settings.setGuideShowEmpty(show) }
    }

    /** Order ▾ › Show › Video preview: the focused channel plays in the pane; off shows its logo only. */
    val guidePreview: StateFlow<Boolean> = settings.guidePreview
        .stateIn(viewModelScope, SharingStarted.Eagerly, true)

    fun setGuidePreview(show: Boolean) {
        viewModelScope.launch { settings.setGuidePreview(show) }
    }

    /** "OK previews first" for the Guide: on, OK brings a channel into the preview; off, OK watches it. */
    val guidePreviewOnOk: StateFlow<Boolean> = settings.guidePreviewOnOk
        .stateIn(viewModelScope, SharingStarted.Eagerly, true)

    /** The Order menu's choice; remembered, so the guide opens this way next time. */
    fun setGuideSort(sort: SettingsRepository.GuideSort) {
        viewModelScope.launch { settings.setSortGuide(sort) }
    }

    fun setQuery(q: String) {
        _query.value = q
    }

    /** The channel last tuned from the guide — the screen refocuses its row after fullscreen exits. */
    var lastTunedChannelId: Long? = null
        private set

    /** Set when the player closes. The guide is not on screen while the player is, so the mark is kept here:
     *  the next focus restore goes to the channel that was playing and arms its preview. */
    var playerClosed: Boolean = false

    /** Record that this channel was tuned from the guide, so focus returns to its row on Back.
     *  Call this before delegating playback to liveVm, so lastTunedChannelId is set correctly. */
    fun noteChannelTuned(channel: ChannelEntity) {
        lastTunedChannelId = channel.id
    }

    // Two more live-start paths used to live here — a `play(channel)` that tuned a Guide channel
    // straight on mpv, and a `playCatchup(...)` that opened the archive itself. Both bypassed the
    // ExoPlayer-first ladder, the per-channel engine pin and the learned decode quirks, and `play()`
    // also recorded its own history row. They were unreachable (the shell always supplies the
    // callbacks), so EpgScreen now REQUIRES them and these copies are gone: LiveViewModel is the one
    // place a live channel or an archive programme starts. `playCatchupExternal` below is different —
    // it hands the URL to another app, so it stays.

    /** Which player takes a catch-up archive — read by the Guide's programme dialog to route itself. */
    val catchupPlayer: StateFlow<SettingsRepository.CatchupPlayer> = settings.catchupPlayer
        .stateIn(viewModelScope, SharingStarted.Eagerly, SettingsRepository.CatchupPlayer.INTERNAL)

    /** Hand an archive programme to an external app (VLC, MX Player) instead of the in-app player. */
    fun playCatchupExternal(channel: ChannelEntity, programme: EpgProgrammeEntity) {
        viewModelScope.launch {
            val profileId = settings.activeProfileId.first()
            if (!tv.own.owntv.core.content.AdultCategoryClassifier.allows(profileId, channel.categoryId, profileDao, categoryDao) || customize.sealsCategory(profileId, channel.categoryId)) return@launch
            val url = withContext(kotlinx.coroutines.Dispatchers.IO) { catchupUrlFor(channel, programme) }
            if (url == null) {
                _matchSummary.value = EpgMatchSummary.CatchupUnavailable
                return@launch
            }
            val source = withContext(kotlinx.coroutines.Dispatchers.IO) { sourceDao.getById(channel.sourceId) }
            externalPlayerLauncher.launch(
                url = url,
                title = channel.name,
                subtitle = programme.title,
                userAgent = source?.userAgent,
                httpHeaders = tv.own.owntv.core.settings.SourceOverrides.headersWithReferer(channel.httpHeaders, source),
            )
        }
    }

    /** Build the catch-up URL for a [programme] on [channel], or null if the provider can't serve it. */
    private suspend fun catchupUrlFor(channel: ChannelEntity, programme: EpgProgrammeEntity): String? {
        val source = sourceDao.getById(channel.sourceId) ?: return null
        // Stalker archive URLs are minted per-play via create_link; the others are
        // pure string templates handled by CatchupUrl.
        if (source.type == SourceType.STALKER) {
            return channel.remoteId?.let { rid ->
                runCatching { streamUrlResolver.resolveCatchup(source, rid, programme.startMs, programme.stopMs) }
                    .onFailure { android.util.Log.w("EpgViewModel", "Stalker catch-up resolve failed channelId=${channel.id}", it) }
                    .getOrNull()
            }
        }
        return CatchupUrl.forSource(channel, programme, source, settings.resolveCatchupTimeZone(source), xtream)
    }

    /** True when a programme can be played from the archive: a catch-up channel, already started, and
     *  still inside the channel's archive window. The Guide gates its "Watch from start" button on this. */
    fun canCatchup(channel: ChannelEntity, programme: EpgProgrammeEntity, now: Long): Boolean {
        if (!channel.catchup || programme.startMs > now) return false
        val windowMs = channel.catchupDays.coerceAtLeast(1) * 24L * 60 * 60 * 1000
        return now - programme.startMs <= windowMs
    }

    /** Live channels this profile has favourited — so the Guide can show/toggle a channel's star. */
    val favoriteChannelIds: StateFlow<Set<Long>> = settings.activeProfileId
        .flatMapLatest { pid -> if (pid < 0) flowOf(emptyList()) else favoriteDao.observeFavoriteIds(pid, MediaType.LIVE) }
        .map { it.toSet() }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), emptySet())

    /** Add/remove a channel from Favourites directly from the Guide (channel long-press / detail). */
    /** Hide the channel from every list for this profile (undo: Settings → Customize → Hidden channels). */
    fun hideChannel(channel: ChannelEntity) {
        // Fork: the row leaves the grid at once (the menu closes onto the next channel), instead of
        // only after the next reload — switching category and back used to be the only way to see it go.
        justHidden += channel.id
        _state.value = _state.value.copy(channels = _state.value.channels.filterNot { it.id == channel.id })
        viewModelScope.launch {
            val pid = currentProfileId() ?: return@launch
            customize.setItemHidden(pid, MediaType.LIVE, CustomizeKeys.channel(channel), channel.name, true)
            hiddenWritten += channel.id
        }
    }


    fun toggleFavoriteChannel(channel: ChannelEntity) {
        viewModelScope.launch {
            val pid = currentProfileId() ?: return@launch
            if (favoriteChannelIds.value.contains(channel.id)) {
                userDataWriter.removeFavorite(pid, MediaType.LIVE, channel.id)
            } else {
                favoriteDao.add(tv.own.owntv.core.database.entity.FavoriteEntity(profileId = pid, mediaType = MediaType.LIVE, itemId = channel.id))
            }
            // The "Favorites" guide sort shows only favourited channels — refresh it to match.
            if (settings.sortGuide.first() == SettingsRepository.GuideSort.FAVORITES) load()
        }
    }

    // --- Programme reminders ----------------------------------------------------------

    /** This profile's reminders: the bell in a cell, and "Reminder set" on the programme's button. */
    val reminderRows: StateFlow<List<tv.own.owntv.core.database.entity.ReminderEntity>> =
        settings.activeProfileId
            .flatMapLatest { pid -> if (pid < 0) flowOf(emptyList()) else reminderManager.observe(pid) }
            .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), emptyList())

    /** How early a reminder comes (Settings › Sources & guide › Reminder time) — the menu's "5 min before". */
    val reminderLeadMinutes: StateFlow<Int> = settings.reminderLeadMinutes
        .stateIn(viewModelScope, SharingStarted.Eagerly, tv.own.owntv.core.reminder.ReminderSchedule.DEFAULT_LEAD_MINUTES)

    fun reminderFor(channel: ChannelEntity, programme: EpgProgrammeEntity): tv.own.owntv.core.database.entity.ReminderEntity? =
        reminderRows.value.firstOrNull { it.channelId == channel.id && it.startMs == programme.startMs }

    /** Remind me — or, when this programme already has a reminder, take it away again. */
    fun toggleReminder(channel: ChannelEntity, programme: EpgProgrammeEntity) {
        viewModelScope.launch {
            reminderFor(channel, programme)?.let { reminderManager.dismiss(it.id); return@launch }
            val pid = currentProfileId() ?: return@launch
            reminderManager.add(
                pid, channel.id, channel.name, channel.epgChannelId, programme.title,
                programme.startMs, programme.stopMs, reminderLeadMinutes.value,
            )
        }
    }

    /** "Play catch-up in…" in the programme menu steps through Ask · OwnTV player · External player. */
    fun setCatchupPlayer(mode: SettingsRepository.CatchupPlayer) {
        viewModelScope.launch { settings.setCatchupPlayer(mode) }
    }

    /** The channel's current manual EPG match (or null if auto-matched). */
    fun currentEpgMatch(channel: ChannelEntity): String? = custom.value.epgMatchResolver.epgIdFor(channel)

    /** The channel's own guide shift in minutes, or null when it follows the global offset. */
    fun currentEpgShift(channel: ChannelEntity): Int? =
        tv.own.owntv.core.epg.EpgShift.overrideFor(custom.value, channel)

    /** The global guide shift — the per-channel dialog's "follow global" default. */
    fun globalEpgShift(): Int = epgOffset.value

    /** Shift this channel's guide by [minutes] (null → follow the global offset), then refresh. */
    fun setEpgShift(channel: ChannelEntity, minutes: Int?) {
        viewModelScope.launch {
            val pid = settings.activeProfileId.first()
            val key = CustomizeKeys.channel(channel)
            customize.setEpgShift(pid, MediaType.LIVE, key, minutes)
            // Rows re-read the shift from `custom` — wait for the DataStore edit to land there first,
            // or the refresh below can still render with the old offset.
            kotlinx.coroutines.withTimeoutOrNull(1_000) { custom.first { it.epgShifts[key]?.toIntOrNull() == minutes } }
            // Cached rows are keyed by shift, so the old offset's rows are simply stale. One cache now,
            // so this drops them all rather than only the shifted ones.
            clearRows()
            _cacheRevision.value++ // visible rows re-read with the new shift
        }
    }

    /**
     * Save a whole run's matches in ONE write. Each [CustomizationStore.setEpgMatch] re-reads, rewrites
     * and re-saves the profile's entire customisation, so a lineup of a few thousand matches made that
     * many writes of an ever-growing list — minutes on a television, with the Guide still saying
     * "Matching…" until the last one landed. Same ids, same normalisation, one write.
     */
    private suspend fun saveEpgMatches(profileId: Long, matches: Collection<Pair<String, String>>) {
        val clean = matches.mapNotNull { (key, epgId) ->
            epgId.trim().lowercase().takeIf { it.isNotEmpty() }?.let { key to it }
        }.toMap()
        if (clean.isEmpty()) return
        customize.update(profileId, MediaType.LIVE) { it.copy(epgMatches = it.epgMatches + clean) }
    }

    /** Set/clear a channel's manual EPG match (null clears → auto-match), then reload the guide. */
    fun setEpgMatch(channel: ChannelEntity, epgChannelId: String?) {
        viewModelScope.launch {
            val pid = settings.activeProfileId.first()
            customize.setEpgMatch(pid, MediaType.LIVE, CustomizeKeys.channel(channel), epgChannelId)
            if (epgChannelId != null) fillMatchedInBackground(listOf(epgChannelId)) else load()
        }
    }

    /** Fill in matched channels' programmes from the cached XMLTV (no network) and refresh the guide — in the
     *  BACKGROUND so the match action/spinner returns instantly and doesn't wait on a full cache re-parse. */
    private fun fillMatchedInBackground(epgIds: Collection<String>) {
        viewModelScope.launch {
            val ids = epgIds.map { it.trim().lowercase() }.filterTo(HashSet()) { it.isNotBlank() }
            if (ids.isEmpty()) return@launch
            // One cache pass for the whole set; only re-sync over the network if the cache is gone/stale
            // (returns false when it held none of the matched channels' programmes).
            val handled = runCatching { epgRepository.storeProgrammesForIdsFromCache(ids) }.getOrDefault(false)
            if (!handled) runCatching { refreshAllEpgFromNetwork() }
            load(fresh = true)
            // Single-channel match (manual pick / review Accept / single auto-match): if the feed has no
            // current-or-upcoming programmes for it, its guide row will be empty even though the match
            // succeeded — say so, instead of leaving the user staring at a blank row (the provider simply
            // hasn't published a current schedule for that channel).
            if (ids.size == 1) {
                val upcoming = runCatching { epgDao.countUpcomingForChannel(ids.first(), System.currentTimeMillis()) }.getOrDefault(1)
                if (upcoming == 0) {
                    _matchSummary.value = EpgMatchSummary.MatchedNoProgrammes
                }
            }
        }
    }


    private suspend fun refreshAllEpgFromNetwork() {
        val pid = settings.activeProfileId.first()
        if (pid >= 0) sourceRepository.observeSources(pid).first().forEach { runCatching { epgRepository.refresh(it) } }
        epgSourceStore.getAll().forEach { runCatching { epgRepository.refreshUrl(it.id, it.url, it.userAgent) } }
    }

    // ---- Smart EPG matching: scan channels with no working guide and match them by name ----

    /** A proposed EPG match for a channel that didn't auto-resolve confidently enough to apply. */
    data class EpgMatchSuggestion(
        val channel: ChannelEntity,
        val epgChannelId: String,
        val epgName: String?,
        val score: Double,
    )

    private val _matching = MutableStateFlow(false)
    val matching: StateFlow<Boolean> = _matching.asStateFlow()

    /** While a bulk run scores channels: (scored so far, to score). Null before the count is known and when idle. */
    private val _matchProgress = MutableStateFlow<Pair<Int, Int>?>(null)
    val matchProgress: StateFlow<Pair<Int, Int>?> = _matchProgress.asStateFlow()

    /**
     * Which channels a bulk run covers: the category the guide is showing, not the whole playlist. Null
     * (everything) for All, and for any category kind not listed here.
     */
    private suspend fun matchScope(
        pid: Long,
        filter: tv.own.owntv.core.live.LiveKey?,
    ): ((tv.own.owntv.core.database.entity.ChannelEntity) -> Boolean)? = when (filter) {
        null, tv.own.owntv.core.live.LiveKey.All -> null
        tv.own.owntv.core.live.LiveKey.Catchup -> channelTest { ch -> ch.catchup }
        is tv.own.owntv.core.live.LiveKey.Folder -> channelTest { ch -> ch.categoryId == filter.id }
        tv.own.owntv.core.live.LiveKey.Favorites ->
            channelIn(favoriteDao.observeFavoriteIds(pid, MediaType.LIVE).first().toSet())
        tv.own.owntv.core.live.LiveKey.History ->
            channelIn(historyDao.getForProfileType(pid, MediaType.LIVE).mapTo(HashSet()) { it.itemId })
        is tv.own.owntv.core.live.LiveKey.Custom ->
            channelIn(customCategoryDao.itemIds(pid, MediaType.LIVE, filter.id).toSet())
        else -> null
    }

    // Kotlin reads a bare { } after a when arrow as a block, so the predicates are built by these.
    private fun channelTest(test: (tv.own.owntv.core.database.entity.ChannelEntity) -> Boolean) = test
    private fun channelIn(ids: Set<Long>): (tv.own.owntv.core.database.entity.ChannelEntity) -> Boolean = { it.id in ids }

    /** Low-confidence suggestions awaiting the user's accept/skip (high-confidence ones auto-apply). */
    private val _review = MutableStateFlow<List<EpgMatchSuggestion>>(emptyList())
    val review: StateFlow<List<EpgMatchSuggestion>> = _review.asStateFlow()

    /**
     * The review window's "Include guide logos" tick. Starts ticked when some EPG source already has
     * "Use this guide's logos" on; while ticked, every match this run makes also turns that option on for
     * the EPG sources that carry a logo for it.
     */
    private val _includeGuideLogos = MutableStateFlow(false)
    val includeGuideLogos: StateFlow<Boolean> = _includeGuideLogos.asStateFlow()

    fun setIncludeGuideLogos(include: Boolean) {
        _includeGuideLogos.value = include
    }

    /** Guide ids the current run applied on its own, before the review window opened. */
    private var runApplied: List<String> = emptyList()

    private suspend fun includeLogosIfTicked(epgIds: List<String>) {
        if (_includeGuideLogos.value) tv.own.owntv.core.epg.EpgLogoStore.includeLogosFor(settings, epgDao, runApplied + epgIds)
    }

    private suspend fun openReview(suggestions: List<EpgMatchSuggestion>) {
        if (suggestions.isNotEmpty()) _includeGuideLogos.value = settings.epgUseLogos.first().isNotEmpty()
        _review.value = suggestions
    }

    /** One-line outcome of the last auto-match run, shown as a transient banner. */
    private val _matchSummary = MutableStateFlow<EpgMatchSummary?>(null)
    val matchSummary: StateFlow<EpgMatchSummary?> = _matchSummary.asStateFlow()

    /**
     * The outcome of a bulk run, once, as it finishes — for the shell to say when the Guide is not on
     * screen. A run outlives leaving the Guide, and its review waits in [review] for the Guide to come
     * back, but nothing said so: someone who pressed Auto-match and went to watch a channel never knew
     * there was a review waiting, or that the run had finished at all.
     */
    private val _matchFinished = MutableSharedFlow<EpgMatchSummary>(extraBufferCapacity = 1)
    val matchFinished: SharedFlow<EpgMatchSummary> = _matchFinished.asSharedFlow()

    private fun finishRun(summary: EpgMatchSummary) {
        _matchSummary.value = summary
        _matchFinished.tryEmit(summary)
    }

    /**
     * Scan every channel that has no working guide (no manual match and its tvg-id isn't in the EPG
     * feed), match it by name, auto-apply high-confidence hits and queue the rest for review.
     */
    fun autoMatchEpg() {
        if (_matching.value) return
        viewModelScope.launch {
            _matching.value = true
            try {
                val pid = settings.activeProfileId.first()
                // The playlist(s) the guide is showing (the top-right choice), not every playlist on the
                // profile: with a 400-channel playlist picked it scored a 55k lineup too.
                val playlistIds = if (pid < 0) emptyList() else activeSourceIds(settings, sourceDao, pid, MediaType.LIVE)
                if (playlistIds.isEmpty()) { _matchSummary.value = EpgMatchSummary.AddPlaylist; return@launch }
                val cust = customize.observe(pid, MediaType.LIVE).first()

                // Only the category the guide is showing; the count comes back as the channels are scored.
                val outcome = autoMatcher.run(
                    cust, playlistIds,
                    only = matchScope(pid, _categoryFilter.value),
                    onProgress = { done, total -> _matchProgress.value = done to total },
                )
                if (!outcome.hadCandidates) { finishRun(EpgMatchSummary.NoData); return@launch }

                // Persist the confident hits — in one write, see [saveEpgMatches].
                saveEpgMatches(pid, outcome.applied)

                runApplied = outcome.applied.map { it.second }
                openReview(outcome.review.map {
                    EpgMatchSuggestion(it.channel, it.epgChannelId, it.displayName, it.score)
                })
                val applied = outcome.applied.size
                finishRun(when {
                    applied == 0 && outcome.review.isEmpty() -> EpgMatchSummary.AllMatched
                    // Everything found pointed at a guide channel with nothing scheduled. Saying
                    // "matched" here would be a success message for a row that stays blank.
                    applied == 0 && outcome.withheldForNoProgrammes == outcome.review.size ->
                        EpgMatchSummary.MatchedNoProgrammes
                    else -> EpgMatchSummary.AutoMatched(applied, outcome.review.size)
                })
                if (applied > 0) fillMatchedInBackground(outcome.applied.map { it.second }) // off the spinner
            } finally {
                _matching.value = false
                _matchProgress.value = null
            }
        }
    }

    /**
     * Auto-match a SINGLE channel by name (Guide long-press → "Auto-match"). Surfaces the best candidate
     * in the same review dialog (one entry, so no accept/skip-all) for the user to accept or skip, rather
     * than applying silently — or reports none found.
     */
    fun autoMatchOne(channel: ChannelEntity) {
        if (_matching.value) return
        viewModelScope.launch {
            _matching.value = true
            try {
                val best = autoMatcher.one(channel)
                if (best == null) {
                    _matchSummary.value = EpgMatchSummary.NoMatch(channel.name)
                } else {
                    // Show it in the review dialog (accept/skip) instead of applying silently. acceptSuggestion
                    // persists the match + fills the guide; dismissSuggestion just drops it.
                    runApplied = emptyList()
                    openReview(listOf(EpgMatchSuggestion(channel, best.epgChannelId, best.displayName, best.score)))
                    // Say so up front when the winner's guide channel is empty, instead of letting the
                    // user accept it and find a blank row.
                    if (!best.hasProgrammes) _matchSummary.value = EpgMatchSummary.MatchedNoProgrammes
                }
            } finally {
                _matching.value = false
            }
        }
    }

    /** Accept a reviewed suggestion → persist the match and drop it from the review list. */
    fun acceptSuggestion(s: EpgMatchSuggestion) {
        viewModelScope.launch {
            val pid = settings.activeProfileId.first()
            customize.setEpgMatch(pid, MediaType.LIVE, CustomizeKeys.channel(s.channel), s.epgChannelId)
            _review.value = _review.value.filterNot { it.channel.id == s.channel.id }
            includeLogosIfTicked(listOf(s.epgChannelId))
            fillMatchedInBackground(listOf(s.epgChannelId))
        }
    }

    /** Skip a suggestion without matching it (just remove it from the review list). */
    fun dismissSuggestion(s: EpgMatchSuggestion) {
        _review.value = _review.value.filterNot { it.channel.id == s.channel.id }
    }

    /** Accept every remaining suggestion at once, then clear the review list and reload the guide. */
    fun acceptAllSuggestions() {
        val all = _review.value
        if (all.isEmpty()) return
        viewModelScope.launch {
            val pid = settings.activeProfileId.first()
            saveEpgMatches(pid, all.map { CustomizeKeys.channel(it.channel) to it.epgChannelId })
            _review.value = emptyList()
            includeLogosIfTicked(all.map { it.epgChannelId })
            fillMatchedInBackground(all.map { it.epgChannelId })
        }
    }

    /** The one-line outcome beside Auto-match EPG goes after a while. */
    fun clearSummary() {
        _matchSummary.value = null
    }

    /** Close the review list / clear the summary banner. */
    fun clearReview() {
        // Closing the window still honours the tick for the matches the run applied on its own.
        if (_review.value.isNotEmpty() && runApplied.isNotEmpty()) viewModelScope.launch { includeLogosIfTicked(emptyList()) }
        _review.value = emptyList()
        _matchSummary.value = null
    }

    // A guide-local `zap(delta)` used to live here, stepping the guide list and calling the deleted
    // `play()`. It was already unreachable: a Guide tune goes through LiveViewModel.watchFromGuide,
    // which sets the shell's zapSource to LIVE_TV, so CH+/CH- always used LiveViewModel.zap — the one
    // with the ExoPlayer ladder, the pending-tune anchor and the bounded rebuild.

    /**
     * Rebuild the grid's channel list.
     *
     * **Serialised, and the newest wins.** A guide sync writes in batches and each batch changes the
     * stored programme count, so this is called again and again while one is running. It used to start
     * a fresh coroutine every time with nothing to stop the previous one; on a real television
     * six of them were in flight at once, each holding its own copy of the window, and the app died of
     * an OutOfMemoryError. Cancelling the one in progress makes the answer the latest one rather than
     * a race between six stale ones, and the mutex closes the gap while a cancelled load unwinds.
     */
    /** Moving a channel by hand needs a list with a saved order: a provider folder, a custom category or
     *  Favourites, in the Provider or A–Z order (Catch-up first and Favourites first reshuffle it). */
    /**
     * Move up/down works on every channel in every guide view. Folders, custom categories and Favourites
     * keep the order Live TV shares with the guide; any other view (All channels, Catch-up, History, the
     * Catch-up or Favourites-first sorts) keeps a guide order of its own. In Favourites first the
     * favourites on top move among themselves and the other channels among themselves.
     */
    fun canMoveChannels(channel: ChannelEntity? = null): Boolean = _query.value.isBlank()

    private val moveSaves = kotlinx.coroutines.sync.Mutex()
    @Volatile private var favouriteIdsOnScreen: Set<Long> = emptySet()

    /** Where the list on screen saves its order. [nonFavourite]: the channels under the favourites in
     *  Favourites first. Null when the view has nothing to save against (a folder that has gone). */
    private suspend fun moveContext(filter: tv.own.owntv.core.live.LiveKey?, sort: SettingsRepository.GuideSort, nonFavourite: Boolean): String? {
        val favouritesFirst = sort == SettingsRepository.GuideSort.FAVORITES && filter != tv.own.owntv.core.live.LiveKey.Favorites
        val liveTvSort = sort == SettingsRepository.GuideSort.PROVIDER || sort == SettingsRepository.GuideSort.ALPHA || sort == SettingsRepository.GuideSort.LIVE_TV
        val folderKey = (filter as? tv.own.owntv.core.live.LiveKey.Folder)?.let { categoryDao.getById(it.id) }?.let(CustomizeKeys::category)
        return when {
            filter == tv.own.owntv.core.live.LiveKey.Favorites -> tv.own.owntv.core.database.entity.ContentOrderEntity.FAV_CONTEXT
            favouritesFirst && !nonFavourite -> tv.own.owntv.core.database.entity.ContentOrderEntity.FAV_CONTEXT
            liveTvSort && filter is tv.own.owntv.core.live.LiveKey.Folder -> folderKey
            liveTvSort && filter is tv.own.owntv.core.live.LiveKey.Custom -> filter.id
            else -> {
                // The guide's own order for this view. A folder is named by its stable key, so a playlist
                // refresh (new database ids) keeps it.
                val view = if (filter is tv.own.owntv.core.live.LiveKey.Folder) (folderKey ?: return null) else (filter ?: tv.own.owntv.core.live.LiveKey.All).serialize()
                GuideOrderKeys.of(view, sort.name)
            }
        }
    }

    /** One step of [delta] (-1 up, +1 down) in the list on screen. The list on screen changes at once and
     *  the order is saved behind it, in press order. Returns the row the channel is on afterwards, or
     *  null when it could not move. */
    suspend fun moveChannel(channel: ChannelEntity, delta: Int): Int? {
        if (!canMoveChannels(channel)) return null
        val pid = currentProfileId() ?: return null
        val filter = _categoryFilter.value
        val sort = sortGuide.value
        val favs = favouriteIdsOnScreen
        val favouritesFirst = sort == SettingsRepository.GuideSort.FAVORITES && filter != tv.own.owntv.core.live.LiveKey.Favorites
        val nonFavourite = favouritesFirst && channel.id !in favs
        val contextKey = moveContext(filter, sort, nonFavourite) ?: return null
        val list = _state.value.channels.toMutableList()
        val from = list.indexOfFirst { it.id == channel.id }
        val to = from + delta
        // Favourites first: the favourites sit on top; each group moves within itself.
        val favCount = if (favouritesFirst) list.count { it.id in favs } else 0
        val lo = if (nonFavourite) favCount else 0
        val hi = if (favouritesFirst && !nonFavourite) favCount else list.size
        if (from < lo || to < lo || to >= hi) return null
        list.add(to, list.removeAt(from))
        _state.value = _state.value.copy(channels = list)
        val order = when {
            !favouritesFirst -> list.map { it.id }
            nonFavourite -> list.filter { it.id !in favs }.map { it.id }
            else -> list.filter { it.id in favs }.map { it.id }
        }
        viewModelScope.launch {
            moveSaves.withLock {
                val toSave = if (contextKey == tv.own.owntv.core.database.entity.ContentOrderEntity.FAV_CONTEXT) fullFavouriteOrder(pid, order) else order
                contentOrderDao.saveMove(pid, MediaType.LIVE, contextKey, toSave, setOf(channel.id))
            }
        }
        return to
    }

    /**
     * The whole Favorites order with the guide's [visible] favourites moved as on screen. Favorites is one
     * order shared with Live TV and saved whole, while the guide shows only some favourites, so the move
     * is merged into the full order (see [mergeVisibleFavouriteOrder]). Read under [moveSaves] so the
     * previous press is already saved. Favourites outside the active playlists keep their saved place
     * after the rest.
     */
    private suspend fun fullFavouriteOrder(pid: Long, visible: List<Long>): List<Long> {
        val fav = tv.own.owntv.core.database.entity.ContentOrderEntity.FAV_CONTEXT
        val playlistIds = activeSourceIds(settings, sourceDao, pid, MediaType.LIVE)
        val effective = channelDao.snapshotFavoritesManual(pid, fav, playlistIds, MAX_CHANNELS).map { it.id }
        val effectiveSet = effective.toHashSet()
        // Still favourites, so a row left by an unfavourited channel is dropped as before.
        val favourites = favoriteDao.observeFavoriteIds(pid, MediaType.LIVE).first().toHashSet()
        val elsewhere = contentOrderDao.positions(pid, MediaType.LIVE, fav)
            .sortedBy { it.position }.map { it.itemId }.filter { it !in effectiveSet && it in favourites }
        return mergeVisibleFavouriteOrder(effective + elsewhere, visible)
    }

    /** Puts the channels moved in a guide-own order (see [moveContext]) back where they were left. */
    private suspend fun applyGuideOrder(
        pid: Long,
        list: List<ChannelEntity>,
        filter: tv.own.owntv.core.live.LiveKey?,
        sort: SettingsRepository.GuideSort,
        favs: Set<Long>,
    ): List<ChannelEntity> {
        if (list.size < 2) return list
        val favouritesFirst = sort == SettingsRepository.GuideSort.FAVORITES && filter != tv.own.owntv.core.live.LiveKey.Favorites
        val key = moveContext(filter, sort, nonFavourite = true) ?: return list
        if (!GuideOrderKeys.isGuideOwn(key)) return list // Live TV's shared orders are already applied
        val saved = runCatching { contentOrderDao.positions(pid, MediaType.LIVE, key) }.getOrDefault(emptyList())
        if (saved.isEmpty()) return list
        val head = if (favouritesFirst) list.takeWhile { it.id in favs } else emptyList()
        val tail = list.drop(head.size)
        val byId = tail.associateBy { it.id }
        val pinned = saved.mapNotNull { row -> byId[row.itemId]?.let { it to row.position } }
        if (pinned.isEmpty()) return list
        val pinnedIds = pinned.mapTo(HashSet()) { it.first.id }
        return head + tv.own.owntv.core.customize.placePinned(tail.filter { it.id !in pinnedIds }, pinned.sortedBy { it.second })
    }

    /** Rebuilds the list. [fresh] re-reads the database even when [channelReads] looks current: for
     *  callers that have just written guide data themselves. */
    fun load(fresh: Boolean = false) {
        if (fresh) channelReadsDirty = true
        val previous = loadJob
        loadJob = viewModelScope.launch {
            previous?.cancelAndJoin()
            // Calls that arrive together become one load: leaving Favourites for the TV Guide hands the
            // category back (one call) and opens the guide (another) in the same frame. The earlier call
            // is cancelled here, in the wait, before it has read anything.
            kotlinx.coroutines.delay(LOAD_COALESCE_MS)
            loadMutex.withLock { runLoad() }
        }
    }

    private suspend fun runLoad() {
        val loadStartedAt = android.os.SystemClock.elapsedRealtime()
        // Show the spinner only on the FIRST load. The EpgViewModel is shared, so on re-entry the guide
        // is already populated — keep the existing list on screen and refresh it silently, so opening the
        // Guide menu is instant instead of flashing a spinner and re-rendering from scratch every time.
        if (_state.value.channels.isEmpty()) _state.value = _state.value.copy(loading = true, message = null)
        val pid = currentProfileId()
        if (pid == null) {
            _state.value = EpgUiState(
                loading = false,
                message = EpgMessage.CreateProfile,
                hasEpgSources = epgSourceStore.getAll().isNotEmpty(),
            )
            builtFilter = NotBuilt; _switchingCategory.value = false
            return
        }
        val playlistIds = activeSourceIds(settings, sourceDao, pid, MediaType.LIVE)
        val epgIds = epgSourceStore.getAll().map { it.id }
        // Channels come from the playlists; guide data is matched from BOTH the playlists' own EPG
        // (kept for compatibility) and the standalone EPG sources — by epgChannelId across all ids.
        val ids = playlistIds + epgIds

        if (playlistIds.isEmpty()) {
            _state.value = EpgUiState(loading = false, message = EpgMessage.AddPlaylist)
            builtFilter = NotBuilt; _switchingCategory.value = false
            return
        }

        val now = System.currentTimeMillis()
        val nowAligned = now - (now % HALF_HOUR_MS) // align to the half hour
        // Always retain two recent hours so the centered "now" marker has programmes on its left.
        // Catch-up channels can extend the same window farther back to their archive limit.
        val q = _query.value.trim()
        val reads = channelReads(playlistIds, ids, q, settings.guideShowEmpty.first())
        val maxCatchupDays = reads.maxCatchupDays
        val catchupLookbackMs = if (maxCatchupDays > 0)
            minOf(maxCatchupDays * DAY_MS, CATCHUP_LOOKBACK_CAP_MS) else 0L
        val lookbackMs = maxOf(GUIDE_VISIBLE_PAST_MS, catchupLookbackMs)
        val windowStart = nowAligned - lookbackMs
        // How far the grid can scroll forward is the SAME value that decides how much is stored.
        // Storing a week and only letting the user reach tomorrow would be half a feature — and the
        // 48-hour horizon this replaced is the reason the guide appeared to "run dry" at all.
        val windowEnd = nowAligned + settings.guideDaysToKeep.first() * DAY_MS

        // Respect customizations: hidden channels stay out of the guide, renames show.
        val hideSettled = synchronized(hiddenWritten) { hiddenWritten.toSet() }
        val cust = customize.observeVisible(pid, MediaType.LIVE).first()
        // Hides written before that read are now in cust; only the ones still being written stay filtered.
        justHidden.removeAll(hideSettled); hiddenWritten.removeAll(hideSettled)
        val rawChannels = reads.raw
        val favoriteIds = favoriteDao.observeFavoriteIds(pid, MediaType.LIVE).first().toSet()
        favouriteIdsOnScreen = favoriteIds
        val sortGuideMode = settings.sortGuide.first()
        // Hidden categories keep their channels out of the guide too (parity with Live TV), and a
        // filter pointing at a now-hidden category falls back to "All" instead of an empty grid.
    val isKidsProfile = profileDao.getById(pid)?.isKids == true
    val hiddenCatIds = if (cust.hiddenCategories.isEmpty() && !isKidsProfile) {
        emptySet()
    } else {
        tv.own.owntv.core.content.AdultCategoryClassifier.hiddenCategoryIds(
            categoryDao.observe(ids, MediaType.LIVE).first(),
            cust.hiddenCategories,
            isKidsProfile,
        )
    }
        val filter = _categoryFilter.value
        // A provider folder keeps out the channels the user moved from it into a custom category.
        val folderKey = (filter as? tv.own.owntv.core.live.LiveKey.Folder)
            ?.let { categoryDao.getById(it.id) }?.let(CustomizeKeys::category)
        val memberIds: Set<Long>? = when (filter) {
            is tv.own.owntv.core.live.LiveKey.Custom -> if (filter.id in cust.sealedCategories) emptySet() else customCategoryDao.itemIds(pid, MediaType.LIVE, filter.id).toSet()
            tv.own.owntv.core.live.LiveKey.Favorites -> favoriteIds
            tv.own.owntv.core.live.LiveKey.History ->
                historyDao.getForProfileType(pid, MediaType.LIVE).mapTo(HashSet()) { it.itemId }
            else -> null
        }
        // Provider and A–Z are Live TV's own lists: in a folder, a custom category or Favourites the guide
        // takes the order the Live TV queries give, moved channels in their place; elsewhere, All included,
        // the playlist's order or plain A–Z.
        val alphaSort = sortGuideMode == SettingsRepository.GuideSort.ALPHA
        val liveTvSort = alphaSort || sortGuideMode == SettingsRepository.GuideSort.PROVIDER || sortGuideMode == SettingsRepository.GuideSort.LIVE_TV ||
            filter == tv.own.owntv.core.live.LiveKey.Favorites
        // "Favourites first" puts the favourites on top in their own hand-made order (Move channel up/down).
        val favouritesFirstOrder: Map<Long, Int>? =
            if (sortGuideMode != SettingsRepository.GuideSort.FAVORITES) null
            else channelDao.snapshotFavoritesManual(pid, tv.own.owntv.core.database.entity.ContentOrderEntity.FAV_CONTEXT, playlistIds, MAX_CHANNELS)
                .withIndex().associate { (i, ch) -> ch.id to i }
        val liveOrder: Map<Long, Int>? = if (!liveTvSort) null else when (filter) {
            is tv.own.owntv.core.live.LiveKey.Folder -> folderKey?.let { channelDao.snapshotByCategoryManual(filter.id, pid, it, alphaSort, MAX_CHANNELS) }
            is tv.own.owntv.core.live.LiveKey.Custom -> customCategoryDao.snapshotChannels(pid, filter.id, playlistIds, alphaSort, MAX_CHANNELS)
            tv.own.owntv.core.live.LiveKey.Favorites ->
                channelDao.snapshotFavoritesManual(pid, tv.own.owntv.core.database.entity.ContentOrderEntity.FAV_CONTEXT, playlistIds, MAX_CHANNELS)
            else -> null
        }?.withIndex()?.associate { (i, ch) -> ch.id to i }
        // Heavy work — filter hidden, apply renames + manual EPG matches, sort, category-filter — runs off
        // the main thread (#3/#5) so a 50k-channel playlist never freezes the UI building the guide list.
        val channels = withContext(Dispatchers.Default) {
            val auto = rawChannels
                .filter { CustomizeKeys.channel(it) !in cust.hiddenItems && it.categoryId !in cust.sealedCategoryIds }
                .filter {
                    memberIds != null || it.categoryId == null || it.categoryId !in hiddenCatIds
                }
                .map { ch -> cust.itemNames[CustomizeKeys.channel(ch)]?.let { ch.copy(name = it) } ?: ch }
            // The category first, then the order: a category is a few dozen channels, so the sort no longer
            // runs over every channel of the playlists. Same result — the sort is stable and the category
            // test does not depend on position.
            val matched = applyEpgMatches(auto, cust, playlistIds, q, reads).let { all ->
                when (filter) {
                    null, tv.own.owntv.core.live.LiveKey.All -> all
                    tv.own.owntv.core.live.LiveKey.Catchup -> all.filter { it.catchup }
                    is tv.own.owntv.core.live.LiveKey.Folder -> all.filter { ch ->
                        ch.categoryId == filter.id && (folderKey == null || cust.movedFromOrigin[CustomizeKeys.channel(ch)] != folderKey)
                    }
                    else -> all.filter { it.id in memberIds.orEmpty() }
                }
            }
            // The Order menu: Provider (also the old stored LIVE_TV value), A–Z,
            // Catch-up first, Favourites first. The last two only float those channels to the top.
            val byAlpha = compareBy<ChannelEntity> { it.name.lowercase() }
            val byProvider = compareBy<ChannelEntity>({ it.sourceId }, { it.sortOrder }, { it.name.lowercase() })
            val byLiveTv = liveOrder?.let { order -> compareBy<ChannelEntity> { order[it.id] ?: Int.MAX_VALUE }.then(if (alphaSort) byAlpha else byProvider) }
            when (if (filter == tv.own.owntv.core.live.LiveKey.Favorites) SettingsRepository.GuideSort.LIVE_TV else sortGuideMode) {
                SettingsRepository.GuideSort.ALPHA -> matched.sortedWith(byLiveTv ?: byAlpha)
                SettingsRepository.GuideSort.PROVIDER -> matched.sortedWith(byLiveTv ?: byProvider)
                SettingsRepository.GuideSort.CATCHUP -> matched.sortedWith(compareByDescending<ChannelEntity> { it.catchup }.then(byProvider))
                SettingsRepository.GuideSort.FAVORITES -> matched.sortedWith(
                    compareByDescending<ChannelEntity> { it.id in favoriteIds }.then(compareBy<ChannelEntity> { favouritesFirstOrder?.get(it.id) ?: Int.MAX_VALUE }).then(byProvider),
                )
                // LIVE_TV, the old default, is the provider's order too (Channel number was the same thing).
                SettingsRepository.GuideSort.LIVE_TV -> matched.sortedWith(byLiveTv ?: byProvider)
            }
        }.let { list -> if (q.isNotEmpty()) list else applyGuideOrder(pid, list, filter, sortGuideMode, favoriteIds) }
        val stored = reads.stored


        // Per-row programmes are loaded in ONE batched query and grouped into rowCache here, instead of
        // each row firing its own programmesForChannel (an N+1 query storm on cold open). Only re-batch
        // when the window or sources actually change — a sort / filter / category change keeps the same
        // window, so the cache stays valid and we reuse it. Grouping runs off the main thread.
        // Also re-batch when the stored programme count changed (a sync, or a post-match top-up, added
        // data) even if the window is unchanged — so freshly-matched channels' guides appear.
        val windowChanged = cachedWindow != (windowStart to windowEnd) || loadedSourceIds != ids || stored != lastStored
        // Logged so a trace shows which of the three conditions fired: a
        // sync can re-trigger the whole-table scan through the count alone.
        val rebuildReason = when {
            !windowChanged -> "cache-reused"
            cachedWindow == null -> "first-load"
            cachedWindow != (windowStart to windowEnd) -> "window-moved"
            loadedSourceIds != ids -> "sources-changed"
            else -> "stored-count-changed"
        }
        loadedSourceIds = ids
        lastStored = stored
        if (windowChanged) {
            // Dropping the cached rows is the whole of it. There is no preload any more: the rows
            // on screen re-read themselves through the indexed per-channel query, which costs one
            // small query each instead of materialising the window. That is also why this is now
            // safe to run repeatedly during a sync — it is a map clear, not a 349,077-row read.
            cachedWindow = windowStart to windowEnd
            clearRows()
            _cacheRevision.value++
        }

        val hasEpg = epgIds.isNotEmpty()
        // A guide that synced its channels but no programmes: none of its channel ids are the playlist's,
        // so the sync kept nothing. That is a mismatch Auto-match can fix (its candidates are these same
        // guide channels), not an empty guide; a first sync still running has nothing stored yet either,
        // and the sync pill already says so.
        val unmatchedGuide = stored == 0 && hasEpg && epgActivity.active.value.isEmpty() &&
            epgDao.guideChannelNames(1).isNotEmpty()
        val message = when {
            stored == 0 && !unmatchedGuide -> null // handled by the "No EPG added" prompt (hasEpgSources=false)
            channels.isEmpty() && q.isNotBlank() -> EpgMessage.NoChannelsForQuery(q)
            channels.isEmpty() -> EpgMessage.MismatchedIds
            else -> null
        }
        _state.value = EpgUiState(
            channels = if (justHidden.isEmpty()) channels else channels.filterNot { it.id in justHidden }, windowStart = windowStart, windowEnd = windowEnd, now = now,
            loading = false, message = message, hasEpgSources = hasEpg,
        )
        builtFilter = filter
        _switchingCategory.value = _categoryFilter.value != filter

        tv.own.owntv.core.CorePerf.log {
            "guide_load channels=${channels.size} rawChannels=${rawChannels.size} " +
                "stored=$stored sources=${ids.size} " +
                "rebuilt=$windowChanged reason=$rebuildReason cachedRows=${cachedRowCount()} " +
                "lookbackH=${lookbackMs / 3_600_000} " +
                "totalMs=${android.os.SystemClock.elapsedRealtime() - loadStartedAt}"
        }

        // A second background pass used to merge the older catch-up history in afterwards, because
        // the blocking pass could not afford the whole lookback. Both are gone: a row is read for
        // the full window in one query, so the history is simply there when the row is drawn.
    }

    /** What a load reads from the channel and programme tables, kept between loads (see [channelReads]). */
    private data class ChannelReadKey(val playlistIds: List<Long>, val ids: List<Long>, val showEmpty: Boolean)
    private class ChannelReads(val key: ChannelReadKey, val raw: List<ChannelEntity>, val stored: Int, val maxCatchupDays: Int)
    @Volatile private var channelReads: ChannelReads? = null
    /** Set by any write to the channel or programme tables (see init), and by [load] with fresh. */
    @Volatile private var channelReadsDirty = true

    /**
     * Every guide channel of the playlists, the stored programme count and the longest catch-up: the
     * expensive part of a load (a pass over every channel and the programme table), and the same for
     * every category, order and favourite change. So it is read once and kept until the tables are
     * written to or what it was read for changes; switching category only re-filters it in memory.
     * A search reads afresh each time and is not kept.
     */
    private suspend fun channelReads(playlistIds: List<Long>, ids: List<Long>, query: String, showEmpty: Boolean): ChannelReads {
        val key = ChannelReadKey(playlistIds, ids, showEmpty)
        if (query.isEmpty()) channelReads?.takeIf { it.key == key && !channelReadsDirty }?.let { return it }
        // Cleared before reading, so a write that lands during the read marks it stale again.
        if (query.isEmpty()) channelReadsDirty = false
        val raw = if (showEmpty) channelDao.channelsForGuide(playlistIds, query, MAX_CHANNELS)
            else channelDao.channelsWithGuide(ids, query, MAX_CHANNELS)
        val reads = ChannelReads(key, raw, epgDao.countForSources(ids), channelDao.maxCatchupDays(playlistIds))
        if (query.isEmpty()) channelReads = reads
        return reads
    }

    private class MatchLookup(val reads: ChannelReads, val remoteIds: List<String>, val resolved: Map<String, List<ChannelEntity>>)
    @Volatile private var matchLookup: MatchLookup? = null

    /** Apply per-channel manual EPG overrides to the auto-matched guide list. */
    private suspend fun applyEpgMatches(
        auto: List<ChannelEntity>,
        cust: tv.own.owntv.core.customize.SectionCustomizations,
        playlistIds: List<Long>,
        query: String,
        reads: ChannelReads,
    ): List<ChannelEntity> {
        val matches = cust.epgMatches
        if (matches.isEmpty()) return auto
        val resolver = cust.epgMatchResolver
        val byKey = auto.associateBy { CustomizeKeys.channel(it) }
        // Override the epg id of channels already in the list. Through the resolver, so a match made
        // before the playlist was deleted and re-added still applies under the channel's new key.
        val overridden = auto.map { ch -> resolver.epgIdFor(ch)?.let { ch.copy(epgChannelId = it) } ?: ch }.toMutableList()
        // Add matched channels that didn't auto-appear. Resolve them with ONE bulk query (all channels keyed
        // by their customize key) instead of two DB lookups per match — the old per-match resolveChannel was a
        // query-storm that dominated guide load time once a lot of channels had been smart-matched.
        val missingKeys = matches.keys.filter { it !in cust.hiddenItems }
        if (missingKeys.isNotEmpty()) {
            // Resolve only the matched channels (their key's tail is the remoteId/Xtream stream id) in one
            // query, instead of loading the entire channel table — that table-load was seconds on big lists.
            // In batches: SQLite rejects a statement with more than 999 bound parameters on older
            // Android, and someone who has matched a thousand channels by hand would otherwise crash
            // the app on every guide load. Still one query per batch, not the two per match this
            // replaced.
            //
            // Grouped by the key's TAIL rather than the whole key: the stored key carries the source id
            // the match was made under, which is not the one the channel has after a re-import. Only an
            // unambiguous tail is used — two playlists carrying one provider id must not pick one.
            val remoteIds = missingKeys.mapNotNull { CustomizeKeys.tailOf(it).takeIf { r -> r.isNotEmpty() } }.distinct()
            // Kept with the channel reads it was resolved against: the same channels, the same answer.
            val resolved = matchLookup?.takeIf { it.reads === reads && it.remoteIds == remoteIds }?.resolved
                ?: remoteIds.chunked(QUERY_CHUNK)
                    .flatMap { channelDao.findByRemoteIds(playlistIds, it) }
                    .groupBy { it.remoteId ?: it.name }
                    .also { matchLookup = MatchLookup(reads, remoteIds, it) }
            for (key in missingKeys) {
                val epgId = matches[key] ?: continue
                val ch = resolved[CustomizeKeys.tailOf(key)]?.singleOrNull() ?: continue
                if (CustomizeKeys.channel(ch) in byKey) continue // already in the list, already overridden
                val name = cust.itemNames[key] ?: ch.name
                if (query.isNotBlank() && !name.contains(query, ignoreCase = true)) continue
                overridden.add(ch.copy(epgChannelId = epgId, name = name))
            }
        }
        return overridden
    }

    /** Guide channels for the manual "Match EPG" picker, ranked so ones resembling [channelName]
     *  come first. The candidate set is core's and is filtered by no source — see [GuideCandidates]. */
    suspend fun availableEpgChannels(channelName: String, query: String): List<GuideCandidate> {
        currentProfileId() ?: return emptyList()
        return guideCandidates.forPicker(channelName, query)
    }

    private suspend fun currentProfileId(): Long? {
        val preferred = settings.activeProfileId.first()
        return if (preferred >= 0) profileDao.resolveExistingProfileId(preferred) else null
    }

    companion object {
        // A sync writes the guide in batches, so every batch is reported. This must be longer than
        // the gap between two batch writes, or it coalesces nothing and the grid reloads per batch —
        // which is exactly what it used to do. Measured on a real television:
        // batches land 2.3-6.1 s apart, and 1,500 ms never once coalesced them. 10 s clears the
        // longest observed gap with room to spare; a background refresh showing up ten seconds after
        // it settles is not something anyone is waiting on.
        private const val GUIDE_DATA_SETTLE_MS = 10_000L
        /** How long [load] waits for a second call before it starts (see there). */
        private const val LOAD_COALESCE_MS = 40L
        private const val HALF_HOUR_MS = 30L * 60 * 1000
        private const val GUIDE_VISIBLE_PAST_MS = 2L * 60 * 60 * 1000
        // How many guide rows to keep read. A screen shows ~8; this is generous enough that scrolling
        // back never re-reads, and small enough that the grid's memory does not grow with the
        // catalogue. ~100 programmes a row, so a few megabytes at the cap.
        private const val MAX_CACHED_ROWS = 240
        // Rows read ahead of the one that came into view. Roughly a screen's worth on a television,
        // measure before making it two screens; do not raise it on a hunch.
        private const val PREFETCH_AHEAD = 10
        private const val DAY_MS = 24L * 60 * 60 * 1000
        // How far back the Guide may extend for catch-up (must stay within EpgRepository's retention).
        private const val CATCHUP_LOOKBACK_CAP_MS = 7L * 24 * 60 * 60 * 1000
        // Generous safety bound only (rows load lazily, so this is about the channel list itself).
        private const val MAX_CHANNELS = 20_000
        // Cap the candidate set the bulk matcher scans against (keeps the O(channels×candidates) scan bounded).
        // Bound values sent to a single `IN (...)`. SQLite's parameter ceiling is 999 on older
        // Android; core chunks its own bulk lookups at the same size.
        private const val QUERY_CHUNK = 500
    }
}

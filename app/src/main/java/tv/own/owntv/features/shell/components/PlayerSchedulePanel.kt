package tv.own.owntv.features.shell.components

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.ui.Alignment
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.clipToBounds
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.produceState
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.runtime.collectAsState
import androidx.tv.material3.Text
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import org.koin.compose.koinInject
import tv.own.owntv.R
import tv.own.owntv.core.customize.CustomizationStore
import tv.own.owntv.core.customize.SectionCustomizations
import tv.own.owntv.core.database.entity.ChannelEntity
import tv.own.owntv.core.database.entity.EpgProgrammeEntity
import tv.own.owntv.core.live.GuideReader
import tv.own.owntv.core.live.ProviderTags
import tv.own.owntv.core.model.MediaType
import tv.own.owntv.core.settings.SettingsRepository
import tv.own.owntv.ui.stage.stageGlass
import tv.own.owntv.ui.theme.StageColors
import tv.own.owntv.ui.theme.StageRadii
import tv.own.owntv.ui.theme.mpx
import tv.own.owntv.ui.theme.stageAccent
import tv.own.owntv.ui.theme.stageText
import java.util.Date

/**
 * The right-hand pane of the player's channel list: the highlighted channel's name, what is on now
 * and the rest of today, with the synopsis of the programme on air — the TiviMate "TV guide" look.
 * It follows [channel] as focus moves down the list; the read waits a moment so scrolling quickly
 * does not fire one query per row, and reads one channel only (the same read a Guide row uses).
 */
@Composable
internal fun PlayerSchedulePanel(
    channel: ChannelEntity?,
    modifier: Modifier = Modifier,
) {
    if (channel == null) return
    val guideReader: GuideReader = koinInject()
    val settings: SettingsRepository = koinInject()
    val customize: CustomizationStore = koinInject()
    val epgDao: tv.own.owntv.core.database.dao.EpgDao = koinInject()
    val context = LocalContext.current
    val timeFormat = remember(context) { android.text.format.DateFormat.getTimeFormat(context) }
    val profileId by settings.activeProfileId.collectAsState(initial = -1L)
    val offset by settings.epgOffsetMinutes.collectAsState(initial = 0)

    val programmes by produceState<List<EpgProgrammeEntity>?>(initialValue = null, channel.id, profileId, offset) {
        value = null
        delay(250)
        val cust = if (profileId < 0) SectionCustomizations() else runCatching {
            customize.observe(profileId, MediaType.LIVE).first()
        }.getOrDefault(SectionCustomizations())
        val now = System.currentTimeMillis()
        // Now to the end of the evening: enough for "what's on later", small enough to stay quick.
        value = runCatching {
            guideReader.row(channel, cust, offset, now, now + 12L * 3_600_000L, withText = true)
        }.getOrDefault(emptyList()).let { rows ->
            // The feed's synopsis can be missing from the row read; the stored programme has it
            // (the same lookup the Guide's Programme details page uses). Only the rows shown, only blanks.
            val shown = rows.filter { it.stopMs > now }.take(SHOWN)
            val filled = shown.map { p ->
                if (p.description.isNullOrBlank() && p.id > 0L) {
                    runCatching { epgDao.programmeById(p.id) }.getOrNull()
                        ?.description?.takeIf { it.isNotBlank() }
                        ?.let { d -> p.copy(description = d) } ?: p
                } else p
            }
            filled + rows.filter { it.stopMs > now }.drop(SHOWN)
        }
    }

    val now = remember(programmes) { System.currentTimeMillis() }
    val upcoming = programmes.orEmpty().filter { it.stopMs > now }.take(SHOWN)
    val onAir = upcoming.firstOrNull()?.takeIf { it.startMs <= now }
    val accent = stageAccent.accent

    Column(
        modifier = modifier
            .stageGlass(StageRadii.Sheet, overContent = true)
            .padding(horizontal = 28.mpx, vertical = 26.mpx),
        verticalArrangement = Arrangement.spacedBy(8.mpx),
    ) {
        // Header: the channel, as the TV guide names it.
        Row(horizontalArrangement = Arrangement.spacedBy(14.mpx), verticalAlignment = Alignment.CenterVertically) {
            channel.number?.let { n ->
                Text(n.toString(), style = stageText(32, 800), color = StageColors.Muted, maxLines = 1)
            }
            Text(
                ProviderTags.parse(channel.name).name,
                style = stageText(32, 800), color = StageColors.Text,
                maxLines = 1, overflow = TextOverflow.Ellipsis,
            )
        }
        when {
            programmes == null -> Text(
                stringResource(R.string.fork_schedule_loading),
                style = stageText(22, 400), color = StageColors.Dim,
            )
            upcoming.isEmpty() -> Text(
                stringResource(R.string.fork_schedule_none),
                style = stageText(22, 400), color = StageColors.Dim,
            )
            else -> {
                // Summary of the programme on air: title, span, time left and progress.
                onAir?.let { p ->
                    Column(
                        Modifier.fillMaxWidth().padding(top = 6.mpx, bottom = 10.mpx),
                        verticalArrangement = Arrangement.spacedBy(6.mpx),
                    ) {
                        Text(
                            p.title, style = stageText(36, 800), color = StageColors.Text,
                            maxLines = 2, overflow = TextOverflow.Ellipsis,
                        )
                        Row(horizontalArrangement = Arrangement.spacedBy(16.mpx), verticalAlignment = Alignment.CenterVertically) {
                            Text(
                                stringResource(R.string.fork_schedule_span, timeFormat.format(Date(p.startMs)), timeFormat.format(Date(p.stopMs))),
                                style = stageText(22, 600), color = accent,
                            )
                            Text(
                                stringResource(R.string.fork_schedule_left, ((p.stopMs - now) / 60_000L).coerceAtLeast(1L).toString()),
                                style = stageText(22, 500), color = StageColors.Muted,
                            )
                        }
                        val total = (p.stopMs - p.startMs).coerceAtLeast(1L)
                        val done = ((now - p.startMs).toFloat() / total).coerceIn(0f, 1f)
                        Box(
                            Modifier.fillMaxWidth().height(4.mpx).clip(RoundedCornerShape(2.mpx))
                                .background(StageColors.Muted.copy(alpha = 0.3f)),
                        ) {
                            Box(Modifier.fillMaxWidth(done).height(4.mpx).background(accent))
                        }
                    }
                }
                // The rest of the day, the programme on air first.
                Column(Modifier.fillMaxWidth().weight(1f).clipToBounds(), verticalArrangement = Arrangement.spacedBy(10.mpx)) {
                    upcoming.forEach { p ->
                        val live = p === onAir
                        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(16.mpx)) {
                            Text(
                                timeFormat.format(Date(p.startMs)),
                                style = stageText(22, if (live) 700 else 500),
                                color = if (live) accent else StageColors.Muted,
                                modifier = Modifier.width(104.mpx),
                            )
                            Column(Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(2.mpx)) {
                                Text(
                                    p.title, style = stageText(22, if (live) 700 else 500),
                                    color = if (live) accent else StageColors.Text,
                                    maxLines = 1, overflow = TextOverflow.Ellipsis,
                                )
                                p.description?.takeIf { it.isNotBlank() }?.let { d ->
                                    Text(
                                        d, style = stageText(20, 400), color = StageColors.Dim,
                                        maxLines = if (live) 3 else 2, overflow = TextOverflow.Ellipsis,
                                    )
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/** How many programmes the pane lists; each has room for a line or two of synopsis. */
private const val SHOWN = 7

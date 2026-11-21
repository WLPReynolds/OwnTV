package tv.own.owntv.features.shell.components

import androidx.compose.foundation.layout.Arrangement
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
        }.getOrDefault(emptyList())
    }

    val now = remember(programmes) { System.currentTimeMillis() }
    val upcoming = programmes.orEmpty().filter { it.stopMs > now }.take(8)
    val onAir = upcoming.firstOrNull()?.takeIf { it.startMs <= now }
    val accent = stageAccent.accent

    Column(
        modifier = modifier
            .stageGlass(StageRadii.Sheet, overContent = true)
            .padding(horizontal = 26.mpx, vertical = 26.mpx),
        verticalArrangement = Arrangement.spacedBy(6.mpx),
    ) {
        Text(
            ProviderTags.parse(channel.name).name,
            style = stageText(24, 800), color = StageColors.Text,
            maxLines = 1, overflow = TextOverflow.Ellipsis,
        )
        when {
            programmes == null -> Text(
                stringResource(R.string.fork_schedule_loading),
                style = stageText(17, 400), color = StageColors.Dim,
            )
            upcoming.isEmpty() -> Text(
                stringResource(R.string.fork_schedule_none),
                style = stageText(17, 400), color = StageColors.Dim,
            )
            else -> {
                onAir?.let { p ->
                    Text(
                        p.title, style = stageText(20, 700), color = accent,
                        maxLines = 2, overflow = TextOverflow.Ellipsis,
                    )
                    Text(
                        stringResource(R.string.fork_schedule_span, timeFormat.format(Date(p.startMs)), timeFormat.format(Date(p.stopMs))),
                        style = stageText(16, 500), color = StageColors.Muted,
                    )
                    p.description?.takeIf { it.isNotBlank() }?.let { d ->
                        Text(
                            d, style = stageText(16, 400), color = StageColors.Dim,
                            maxLines = 4, overflow = TextOverflow.Ellipsis,
                            modifier = Modifier.padding(top = 2.mpx, bottom = 8.mpx),
                        )
                    }
                }
                Column(verticalArrangement = Arrangement.spacedBy(7.mpx)) {
                    upcoming.filter { it !== onAir }.forEach { p ->
                        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(14.mpx)) {
                            Text(
                                timeFormat.format(Date(p.startMs)),
                                style = stageText(17, 500), color = StageColors.Muted,
                                modifier = Modifier.width(78.mpx),
                            )
                            Text(
                                p.title, style = stageText(17, 400), color = StageColors.Text,
                                maxLines = 1, overflow = TextOverflow.Ellipsis,
                            )
                        }
                    }
                }
            }
        }
    }
}

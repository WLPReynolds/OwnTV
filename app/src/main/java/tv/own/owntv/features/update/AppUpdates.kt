package tv.own.owntv.features.update

/**
 * Whether the app offers to check for and install its own updates (Settings › Check for updates, the
 * update-on-start row and quick tile, their search entries, and the check at start). Off in this fork:
 * it publishes no releases for the check to find, and it no longer holds REQUEST_INSTALL_PACKAGES, which
 * the installer needs. Turn it on once the fork publishes releases and the permission is back.
 */
internal const val APP_UPDATES_AVAILABLE = false

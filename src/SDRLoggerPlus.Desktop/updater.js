const { app, dialog, shell } = require('electron');
const log = require('electron-log');

// Update checks query the latest GitHub release of the SDRLoggerPlus repo.
// NOTE: n9bc/SDRLoggerPlus is currently a PRIVATE repo with no releases yet, so
// the unauthenticated release check below will 404 until releases are published
// and the repo is public (or a token is added). Failures are handled gracefully,
// so enabling this early is harmless. (The fork's origin, brianbruff/Log4YM,
// must NOT be used here: its releases are a different product.)
const UPDATES_ENABLED = true;
const GITHUB_REPO = 'n9bc/SDRLoggerPlus';
const RELEASES_URL = `https://api.github.com/repos/${GITHUB_REPO}/releases/latest`;
const RELEASES_PAGE = `https://github.com/${GITHUB_REPO}/releases/latest`;

/**
 * Compare two semver version strings
 * Returns: 1 if v1 > v2, -1 if v1 < v2, 0 if equal
 */
function compareVersions(v1, v2) {
  // Strip a leading 'v' and any prerelease/build suffix before comparing the
  // numeric X.Y.Z core. Dev builds carry versions like "2.0.1-dev.3"; without
  // stripping, Number("1-dev") is NaN and the comparison silently breaks. A
  // dev build therefore treats the matching stable release as "not newer" and
  // a later stable X.Y.Z as an available upgrade.
  const core = (v) => v.replace(/^v/, '').split('-')[0];

  const parts1 = core(v1).split('.').map(Number);
  const parts2 = core(v2).split('.').map(Number);

  for (let i = 0; i < Math.max(parts1.length, parts2.length); i++) {
    const num1 = parts1[i] || 0;
    const num2 = parts2[i] || 0;

    if (num1 > num2) return 1;
    if (num1 < num2) return -1;
  }

  return 0;
}

/**
 * Fetch the latest release info from GitHub
 */
async function getLatestRelease() {
  const response = await fetch(RELEASES_URL, {
    headers: {
      'Accept': 'application/vnd.github.v3+json',
      'User-Agent': 'SDRLoggerPlus-Desktop'
    }
  });

  if (!response.ok) {
    throw new Error(`GitHub API returned ${response.status}`);
  }

  return response.json();
}

/**
 * Check for updates and optionally show dialog
 * @param {boolean} silent - If true, only show dialog when update is available
 * @returns {Promise<{updateAvailable: boolean, latestVersion: string | null}>}
 */
async function checkForUpdates(silent = true) {
  if (!UPDATES_ENABLED) {
    log.info('Update check skipped: updater disabled until a SDRLoggerPlus release repository exists');
    if (!silent) {
      dialog.showMessageBox({
        type: 'info',
        title: 'Updates',
        message: 'Automatic updates are not configured for this build.',
      });
    }
    return;
  }
  const currentVersion = app.getVersion();
  log.info(`Checking for updates... Current version: ${currentVersion}`);

  try {
    const release = await getLatestRelease();
    const latestVersion = release.tag_name; // e.g., "v1.6.0"
    const latestClean = latestVersion.replace(/^v/, '');

    log.info(`Latest version on GitHub: ${latestVersion}`);

    const comparison = compareVersions(latestClean, currentVersion);

    if (comparison > 0) {
      // New version available
      log.info(`Update available: ${currentVersion} → ${latestClean}`);

      const result = await dialog.showMessageBox({
        type: 'info',
        title: 'Update Available',
        message: `A new version of SDRLoggerPlus is available!`,
        detail: `Current version: ${currentVersion}\nLatest version: ${latestClean}\n\n${release.name || ''}\n\nWould you like to download it?`,
        buttons: ['Download', 'Later'],
        defaultId: 0,
        cancelId: 1
      });

      if (result.response === 0) {
        // Open releases page in browser
        await shell.openExternal(RELEASES_PAGE);
      }

      return { updateAvailable: true, latestVersion: latestClean };
    } else {
      // No update available
      log.info('No update available - running latest version');

      if (!silent) {
        await dialog.showMessageBox({
          type: 'info',
          title: 'No Updates',
          message: 'You are running the latest version',
          detail: `Current version: ${currentVersion}`,
          buttons: ['OK']
        });
      }

      return { updateAvailable: false, latestVersion: latestClean };
    }
  } catch (err) {
    log.error(`Update check failed: ${err.message}`);

    if (!silent) {
      await dialog.showMessageBox({
        type: 'error',
        title: 'Update Check Failed',
        message: 'Could not check for updates',
        detail: `Please check your internet connection and try again.\n\nError: ${err.message}`,
        buttons: ['OK']
      });
    }

    return { updateAvailable: false, latestVersion: null };
  }
}

module.exports = {
  checkForUpdates,
  compareVersions
};

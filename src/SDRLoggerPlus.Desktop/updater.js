const { app, dialog, shell } = require('electron');
const log = require('electron-log');
const fs = require('fs');
const path = require('path');

// Update checks query the GitHub releases of the SDRLoggerPlus repo. The
// canonical home is N8SDR1/SDRLoggerPlus. The unauthenticated check below 404s
// gracefully until a v2 release is published, so enabling this early is
// harmless. (The fork's origin, brianbruff/Log4YM, must NOT be used here: its
// releases are a different product.)
//
// We query ALL releases (not /latest) so PRE-RELEASES are visible to testers —
// matching Lyra's updater — and pick the highest non-draft version ourselves.
const UPDATES_ENABLED = true;
const GITHUB_REPO = 'N8SDR1/SDRLoggerPlus';
const RELEASES_URL = `https://api.github.com/repos/${GITHUB_REPO}/releases`;
const RELEASES_PAGE = `https://github.com/${GITHUB_REPO}/releases`;

// Remember the last version we prompted about, so a silent (startup) check
// doesn't re-nag every launch. Manual checks always show. Mirrors Lyra's
// "first-time-per-version" behaviour.
function notifiedVersionFile() {
  return path.join(app.getPath('userData'), 'update-notified.json');
}
function getLastNotifiedVersion() {
  try { return JSON.parse(fs.readFileSync(notifiedVersionFile(), 'utf8')).version || null; }
  catch { return null; }
}
function setLastNotifiedVersion(version) {
  try { fs.writeFileSync(notifiedVersionFile(), JSON.stringify({ version })); }
  catch (e) { log.warn(`Could not persist last-notified version: ${e.message}`); }
}

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
 * Fetch the full list of GitHub releases (newest first).
 */
async function fetchReleases() {
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
 * Highest non-draft release strictly newer than currentVersion. Pre-releases
 * are included (so testers see betas). Returns the release object, or null.
 */
function pickBestNewer(releases, currentVersion) {
  if (!Array.isArray(releases)) return null;
  let best = null;
  for (const r of releases) {
    if (!r || r.draft || !r.tag_name) continue;
    if (compareVersions(r.tag_name.replace(/^v/, ''), currentVersion) <= 0) continue;
    if (!best || compareVersions(r.tag_name, best.tag_name) > 0) best = r;
  }
  return best;
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
    const releases = await fetchReleases();
    const best = pickBestNewer(releases, currentVersion);

    if (best) {
      const latestClean = best.tag_name.replace(/^v/, '');
      log.info(`Update available: ${currentVersion} → ${latestClean}${best.prerelease ? ' (pre-release)' : ''}`);

      // Once-per-version: in a silent (startup) check, don't re-nag about a
      // version we already prompted for. A manual check always shows.
      if (silent && getLastNotifiedVersion() === latestClean) {
        log.info('Already notified for this version — suppressing the silent prompt');
        return { updateAvailable: true, latestVersion: latestClean };
      }

      // Include the release notes (changelog) in the prompt, trimmed so a long
      // body doesn't produce a giant dialog.
      const body = (best.body || '').trim();
      const changelog = body ? `\n\n${body.length > 600 ? body.slice(0, 600) + '…' : body}` : '';

      const result = await dialog.showMessageBox({
        type: 'info',
        title: 'Update Available',
        message: `A new version of SDRLoggerPlus is available!`,
        detail: `Current version: ${currentVersion}\nLatest version: ${latestClean}${best.prerelease ? ' (pre-release)' : ''}${changelog}\n\nWould you like to download it?`,
        buttons: ['Download', 'Later'],
        defaultId: 0,
        cancelId: 1
      });

      setLastNotifiedVersion(latestClean);

      if (result.response === 0) {
        // Open releases page in browser
        await shell.openExternal(RELEASES_PAGE);
      }

      return { updateAvailable: true, latestVersion: latestClean };
    } else {
      // No newer release
      log.info('No update available - running the latest version');

      if (!silent) {
        await dialog.showMessageBox({
          type: 'info',
          title: 'No Updates',
          message: 'You are running the latest version',
          detail: `Current version: ${currentVersion}`,
          buttons: ['OK']
        });
      }

      return { updateAvailable: false, latestVersion: currentVersion };
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

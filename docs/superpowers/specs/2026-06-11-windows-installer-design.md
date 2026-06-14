# Windows Installer / Uninstaller — Design

**Date:** 2026-06-11 (rev 2 — post panel review, 4/4 consensus)
**Goal:** A working, locally buildable Windows installer (`SDRLoggerPlus-<version>-win-x64.exe`) with a matching uninstaller, produced by the existing electron-builder NSIS pipeline.

## Background

The repo already declares an NSIS installer in `src/SDRLoggerPlus.Desktop/electron-builder.yml`
(assisted wizard: `oneClick: false`, choosable install directory, desktop + start-menu
shortcuts, Add/Remove Programs entry; NSIS auto-generates the uninstaller). The GitHub
Actions `release.yml` workflow builds it correctly, but **local packaging is doubly
broken**:

1. **Path mismatch:** `npm run package:win` publishes the backend to
   `src/SDRLoggerPlus.Server/bin/Release/net10.0/win-x64/publish/`, while
   `electron-builder.yml` (`extraResources`) and `main.js`
   (`process.resourcesPath/backend/...`) expect it at `src/SDRLoggerPlus.Desktop/backend/`.
2. **Non-portable copy:** the `copy:frontend:*` scripts use `cp -r`, which does not
   exist under npm's default cmd.exe script shell on Windows — the scripts fail outright.

There is also no uninstall-time handling of user data.

## Decisions (user + 4-agent review panel, unanimous)

- **Approach:** fix the existing electron-builder/NSIS pipeline; no hand-rolled NSIS,
  no MSI/WiX.
- **Scope:** local Windows installer only. No GitHub Release, no updater re-enable,
  no code signing (unsigned; SmartScreen warning is accepted).
- **Uninstall data policy: ask.** Non-silent, non-update uninstalls prompt; **Yes**
  deletes the whole `%APPDATA%\SDRLoggerPlus` folder (including default-location backups),
  **No** (the default) keeps everything. Silent (`/S`) and update uninstalls never
  prompt and never delete.

## Design

### 1. Portable packaging helper (`src/SDRLoggerPlus.Desktop/scripts/prepare-backend.js`)

A Node script (location pinned — the npm script runs from `src/SDRLoggerPlus.Desktop`, so
the invocation is `node scripts/prepare-backend.js <rid>`). Steps, each failing loudly:

1. Read `version` from `src/SDRLoggerPlus.Desktop/package.json`.
2. Build the frontend: `npm run build` in `src/SDRLoggerPlus.Web` with
   `VITE_APP_VERSION=<version>` in the environment (otherwise the About dialog shows
   the `'dev'` fallback from `src/SDRLoggerPlus.Web/src/version.ts`).
3. **Clean** `src/SDRLoggerPlus.Desktop/backend/` (`fs.rmSync` recursive). `dotnet publish -o`
   does not clean its output; without this, repeat builds ship stale files, a nested
   `wwwroot/dist`, or a stray `src/SDRLoggerPlus.Server/wwwroot/` auto-published by
   `Microsoft.NET.Sdk.Web` (CLAUDE.md's manual production flow creates one).
4. `dotnet publish ../SDRLoggerPlus.Server -c Release --self-contained -r <rid> -o ./backend`
5. `fs.cpSync('../SDRLoggerPlus.Web/dist', './backend/wwwroot', { recursive: true })` —
   identical tree to CI's `cp -r dist backend/wwwroot` (dest does not exist after the
   clean, so CI's rename semantics and cpSync produce the same layout).

`package.json` script changes:

- `package:win` = `node scripts/prepare-backend.js win-x64 && electron-builder --win --x64 --publish never`
  (`--publish never` matters: the `repository` field plus an ambient `GH_TOKEN` would
  otherwise let electron-builder attempt a GitHub release upload).
- `package:mac` = same pattern with `osx-arm64` + `--mac --arm64` (the osx-x64 publish
  leg is dead weight — `electron-builder.yml` only targets mac arm64 — and is dropped).
- `package:linux` = same pattern with `linux-x64`.
- `package:all` is **deleted**: with a single shared `./backend/` output, one
  multi-platform electron-builder invocation would bundle the last-published RID's
  backend into every artifact. Cross-platform packaging happens in CI (one platform
  per matrix job).
- The old `build:frontend`, `copy:frontend:*`, and `build:backend:*` scripts are removed.

Facts verified by the panel:

- Hamlib native DLLs for Windows ship from `src/SDRLoggerPlus.Server/runtimes/win-x64/native/`
  via the explicit csproj item `<Content Include="runtimes\**\native\*" ...>`
  (`SDRLoggerPlus.Server.csproj:36`) plus the custom `DllImportResolver`
  (`Native/Hamlib/HamlibNative.cs`) — **not** NuGet's RID-graph runtimes mechanism.
  CI's extra Hamlib provisioning is mac/linux-only; Windows needs nothing.
- `src/SDRLoggerPlus.Desktop/backend/` and `dist/` are already gitignored.
- Local builds always stamp version 1.0.0 (only CI rewrites it from the tag). Accepted.
- CI (`release.yml:94-106`) duplicates what the helper does; converging CI onto the
  helper is **explicit future work** — the workflow file is untouched in this effort
  (it only fires on `v*` tags and can't be tested without a real release).

### 2. `main.js` fixes

- Make `app.setName('SDRLoggerPlus')` unconditional (currently darwin-only) **and move it
  above the `app.getPath('userData')` call at line 10** — on Windows, userData
  currently resolves to `%APPDATA%\SDRLoggerPlus` only via `productName`; the uninstaller
  aims `RMDir /r` at that folder name, so the app must pin it explicitly, before first
  use.
- `getBackendPath()` dev fallback: point at `path.join(__dirname, 'backend', execName)`
  (the helper's output) instead of the abolished `bin/Release/.../publish` layout.
- Update the "Backend Not Found" dialog text to name the new command
  (`node scripts/prepare-backend.js <rid>`), not the deleted `build:backend:*` scripts.

### 3. Uninstall data prompt (`src/SDRLoggerPlus.Desktop/assets/installer.nsh`)

File lives in `assets/` (the electron-builder `buildResources` dir) **and** is wired
explicitly via `nsis.include: installer.nsh` in `electron-builder.yml` — explicit
resolution checks buildResources first and throws `InvalidConfigurationError` if the
file is missing (a silently dropped include would ship an uninstaller with no guards).
Add `!assets/installer.nsh` to the `files` list so the script isn't bundled into
app.asar (the existing `assets/**/*` entry re-includes build resources by design).

**`customUnInstall` macro** (electron-builder inserts it in `Section "un.install"`
*after* app-file/registry removal — correct for a data prompt, but too late for
process kills, see §4):

- All guards in LogicLib style. `${isUpdated}` is a **runtime** flag (generated
  `StdUtils.TestParameter` test of the `--updated` switch) — never `!ifdef`.
  During upgrades electron-builder always invokes the old uninstaller with
  `/S ... --updated`, so the silent and updated guards each independently protect the
  upgrade path (defense in depth, both kept).
- `${ifNot} ${isUpdated}` → `${ifNot} ${Silent}` →
  `IfFileExists "$APPDATA\SDRLoggerPlus\*.*"` (skip the prompt when the folder is already
  gone, e.g. after the built-in `--delete-app-data` flag ran) →
  `MessageBox MB_YESNO|MB_ICONQUESTION|MB_DEFBUTTON2 ... /SD IDNO`.
  `MB_DEFBUTTON2` is load-bearing: plain `MB_YESNO` defaults to **Yes**, so an
  Enter-key slip would delete the QSO database. `/SD IDNO` is redundant behind the
  Silent guard — kept as belt-and-suspenders.
- Prompt wording (honest about scope):
  *"Also delete your SDRLoggerPlus data folder? This permanently deletes your QSO database,
  settings, and any backups stored in the SDRLoggerPlus data folder
  (%APPDATA%\SDRLoggerPlus). Backups saved to other locations are not touched."*
  (The backup destination is user-configurable, `Settings.BackupSettings.DestinationPath`;
  only the default lives inside the data folder.)
- On Yes, replicate the template's shell-context dance (per-machine installs otherwise
  resolve `$APPDATA` to `C:\ProgramData` — wrong folder, real data missed): if
  `$installMode == "all"`, `SetShellVarContext current` around the delete, restore after.
- **Junction guard** (NSIS 3.0.4.1 `RMDir /r` recurses through directory junctions and
  deletes the *target's* contents): using `${GetFileAttributes} ... "REPARSE_POINT"`,
  (1) if the data folder root is a reparse point, remove it with plain `RMDir`
  (deletes the link only); (2) else if `backups\` is a reparse point, plain-`RMDir` it
  first; then `RMDir /r` the folder. This makes the "backups saved to other locations
  are not touched" promise true even for the two plausible junction points. Residual
  risk — a junction at any *other* subdirectory would still be traversed — is accepted
  and documented here.

### 4. Backend process handling (`customInit` / `customUnInit`)

electron-builder's `CHECK_APP_RUNNING` only manages `SDRLoggerPlus.exe`; its `taskkill /f`
fallback bypasses Electron's `before-quit` cleanup and orphans the spawned
`SDRLoggerPlus.Server.exe`, which holds locks on `$INSTDIR` files and on `sdrloggerplus.db`
(partial deletes, failed upgrades). So:

- In **both** `customInit` (installer `.onInit`) and `customUnInit` (uninstaller
  `un.onInit`): kill the current user's backend —
  `taskkill /F /IM SDRLoggerPlus.Server.exe /FI "USERNAME eq %USERNAME%"` via
  `nsExec::Exec` (no console flash), errors ignored, then `Sleep 1000` to let file
  handles settle before any RMDir. `/F` from the start is required: the backend is a
  windowless console process and rejects graceful `taskkill`. The `USERNAME` filter
  prevents an admin's per-machine uninstall from killing another logged-in user's
  backend.
- Recorded alternative (not chosen): `customCheckAppRunning` would close the residual
  window where a user relaunches the app mid-wizard and `CHECK_APP_RUNNING`'s `/f`
  fallback re-orphans a backend — but defining it suppresses the template's
  `getProcessInfo.nsh` include and `Var pid` declaration (the custom include would
  have to redeclare both), which is fragile across electron-builder upgrades. The
  residual window requires the user to relaunch during the wizard *and* the app to
  resist graceful close; accepted.
- Known UX wart (accepted): `customInit` runs at wizard start, so cancelling the
  install still leaves the backend killed.

### 5. Build & verify

- Build: `npm run package:win` in `src/SDRLoggerPlus.Desktop` produces
  `dist/SDRLoggerPlus-1.0.0-win-x64.exe`.
- Smoke test (autonomous):
  - Silent install (`/S`), confirm files land in `%LOCALAPPDATA%\Programs\SDRLoggerPlus`,
    uninstaller and Add/Remove Programs registry entry exist.
  - Launch the installed `SDRLoggerPlus.exe`, confirm the bundled backend process starts
    and serves; **quit the app and confirm `SDRLoggerPlus.Server.exe` exited** before the
    uninstall step (force-killing the Electron process would itself orphan the backend
    and make the uninstall check flaky).
  - Silent uninstall, confirm the install dir is removed and `%APPDATA%\SDRLoggerPlus`
    survives (silent = keep data).
  - Inspect the generated uninstaller script (or the `assets/installer.nsh` as
    compiled) to confirm `MB_DEFBUTTON2` and `/SD IDNO` are present — the interactive
    prompt path ships unverified otherwise, and that's exactly where the
    default-button bug would hide.
  - The interactive Yes/No prompt is left for the user to exercise.

## Error handling

- The helper script fails loudly at each step (non-zero exit aborts `package:win`).
- The uninstaller's delete branch only ever targets `$APPDATA\SDRLoggerPlus`; it never
  touches the legacy `%APPDATA%\Log4YM` folder (the pre-fork safety net) or anything
  outside `%APPDATA%`.
- Accepted quirk, do not "fix": uninstall-with-Yes followed by reinstall resurrects
  pre-fork data — `LegacyMigration.MigrateIfNeeded` re-runs whenever the SDRLoggerPlus
  folder is absent and re-migrates from the untouched Log4YM folder. That is the
  safety net working as designed; the prompt wording must never imply it protects
  post-fork QSOs.

## Out of scope / follow-ups

- macOS/Linux installer verification (scripts restructured but unbuilt).
- Code signing, GitHub Release, auto-updater re-enable (updater stays disabled).
- CI workflow convergence onto `prepare-backend.js` (future work; CI's Windows
  installer output *does* gain the new prompt/kill behavior on the next tag, since it
  consumes the same `electron-builder.yml`).
- **Separate bug found during review:** "on exit" scheduled backups never run on
  packaged Windows builds — Electron stops the backend with `kill('SIGTERM')`, which
  Node implements on Windows as unconditional `TerminateProcess`, so the .NET host's
  graceful-shutdown backup path is unreachable. File and fix separately.

## Testing

No unit-testable surface (build scripts + NSIS macro). Verification is the smoke test
above. Existing frontend/backend test suites must still pass (main.js and package.json
changes don't touch them, but run as regression).

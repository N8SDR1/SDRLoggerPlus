# Windows Installer / Uninstaller Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a working local Windows NSIS installer + uninstaller for SDRLoggerPlus via the existing electron-builder pipeline, with an ask-before-deleting-data uninstall prompt.

**Architecture:** A portable Node helper (`prepare-backend.js`) replaces the broken `cp -r` npm scripts and reproduces CI's exact `backend/` layout; a custom NSIS include (`assets/installer.nsh`) adds backend-process kills at installer/uninstaller init and a guarded data-deletion prompt in `customUnInstall`. Spec: `docs/superpowers/specs/2026-06-11-windows-installer-design.md` (rev 2, panel-approved).

**Tech Stack:** electron-builder 25.1.8 (NSIS 3.0.4.1), Node 20, .NET 10 self-contained publish.

**No TDD:** there is no unit-testable surface (build orchestration + NSIS macro). Verification is the smoke test in Task 4 plus regression suites in Task 5.

---

### Task 1: Packaging helper + npm scripts

**Files:**
- Create: `src/SDRLoggerPlus.Desktop/scripts/prepare-backend.js`
- Modify: `src/SDRLoggerPlus.Desktop/package.json` (scripts section)

- [ ] **Step 1.1: Create `src/SDRLoggerPlus.Desktop/scripts/prepare-backend.js`**

```js
#!/usr/bin/env node
/**
 * Prepares the bundled .NET backend for electron-builder packaging.
 * Produces the layout that electron-builder.yml (extraResources) and CI
 * (.github/workflows/release.yml, "Build .NET backend" step) expect:
 *   ./backend/           self-contained SDRLoggerPlus.Server publish output
 *   ./backend/wwwroot/   built frontend (SDRLoggerPlus.Web/dist)
 *
 * Usage: node scripts/prepare-backend.js <rid>
 *   rid: win-x64 | osx-arm64 | linux-x64 (others accepted for completeness)
 */
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const RIDS = ['win-x64', 'win-x86', 'osx-x64', 'osx-arm64', 'linux-x64', 'linux-arm64'];
const rid = process.argv[2];
if (!RIDS.includes(rid)) {
  console.error(`Usage: node scripts/prepare-backend.js <rid>  (one of: ${RIDS.join(', ')})`);
  process.exit(1);
}

const desktopDir = path.resolve(__dirname, '..');
const webDir = path.resolve(desktopDir, '..', 'SDRLoggerPlus.Web');
const serverDir = path.resolve(desktopDir, '..', 'SDRLoggerPlus.Server');
const backendDir = path.join(desktopDir, 'backend');
const version = require(path.join(desktopDir, 'package.json')).version;

function run(cmd, cwd, env) {
  console.log(`\n> ${cmd}`);
  execSync(cmd, { cwd, stdio: 'inherit', env: { ...process.env, ...env } });
}

// 1. Frontend build. VITE_APP_VERSION is load-bearing: without it the About
//    dialog shows the 'dev' fallback (src/SDRLoggerPlus.Web/src/version.ts).
run('npm run build', webDir, { VITE_APP_VERSION: version });

// 2. Clean: `dotnet publish -o` does not clean its output dir; stale files
//    (or a nested wwwroot from a previous run) would ship in the installer.
fs.rmSync(backendDir, { recursive: true, force: true });

// 3. Publish the backend self-contained.
run(
  `dotnet publish "${serverDir}" -c Release --self-contained -r ${rid} -o "${backendDir}"`,
  desktopDir
);

// 4. Replace any wwwroot the publish emitted (Microsoft.NET.Sdk.Web auto-
//    publishes a stray src/SDRLoggerPlus.Server/wwwroot/ if the manual production
//    flow ever created one) with the freshly built frontend.
const wwwroot = path.join(backendDir, 'wwwroot');
fs.rmSync(wwwroot, { recursive: true, force: true });
const dist = path.join(webDir, 'dist');
if (!fs.existsSync(path.join(dist, 'index.html'))) {
  console.error(`Frontend build output missing or incomplete: ${dist}`);
  process.exit(1);
}
fs.cpSync(dist, wwwroot, { recursive: true });

console.log(`\nBackend prepared at ${backendDir} (rid=${rid}, version=${version})`);
```

- [ ] **Step 1.2: Replace the scripts block in `src/SDRLoggerPlus.Desktop/package.json`**

Replace the entire `"scripts"` object (removing `build:frontend`, all `copy:frontend:*`,
all `build:backend:*`, and `package:all`) with:

```json
  "scripts": {
    "start": "electron .",
    "dev": "electron . --dev",
    "dev:vite": "electron . --dev --vite",
    "prepare:backend:win": "node scripts/prepare-backend.js win-x64",
    "package:win": "node scripts/prepare-backend.js win-x64 && electron-builder --win --x64 --publish never",
    "package:mac": "node scripts/prepare-backend.js osx-arm64 && electron-builder --mac --arm64 --publish never",
    "package:linux": "node scripts/prepare-backend.js linux-x64 && electron-builder --linux --x64 --publish never"
  },
```

Notes: `--publish never` mirrors CI and prevents an ambient `GH_TOKEN` from
triggering a release upload (the `repository` field is set). `package:all` is
deleted — a single shared `./backend/` cannot serve a multi-platform invocation
(last published RID would ship in every artifact). The dead `osx-x64` leg is
dropped (electron-builder.yml only targets mac arm64).

- [ ] **Step 1.3: Sanity-check the helper argument validation (fast, no publish)**

Run (from `src/SDRLoggerPlus.Desktop`): `node scripts/prepare-backend.js bogus-rid`
Expected: usage message, exit code 1.

- [ ] **Step 1.4: Commit**

```bash
git add src/SDRLoggerPlus.Desktop/scripts/prepare-backend.js src/SDRLoggerPlus.Desktop/package.json
git commit -m "feat(installer): portable backend-prep helper, fix local packaging layout"
```

---

### Task 2: main.js identity + path fixes

**Files:**
- Modify: `src/SDRLoggerPlus.Desktop/main.js:9-11` (setName before first getPath), `:35-38` (remove darwin-only block), `:106-109` (dev fallback), `:132` (error text)

- [ ] **Step 2.1: Pin the app name before any `app.getPath()` call**

Replace lines 9–11:

```js
// Zoom level persistence using a simple JSON file
const userDataPath = app.getPath('userData');
const zoomConfigPath = path.join(userDataPath, 'zoom-config.json');
```

with:

```js
// Pin the app identity before any app.getPath() call: userData must resolve
// to %APPDATA%/SDRLoggerPlus on every platform — the uninstaller deletes user data
// by exactly that folder name.
app.setName('SDRLoggerPlus');

// Zoom level persistence using a simple JSON file
const userDataPath = app.getPath('userData');
const zoomConfigPath = path.join(userDataPath, 'zoom-config.json');
```

- [ ] **Step 2.2: Remove the now-redundant darwin-only block (lines 35–38 pre-edit)**

Delete:

```js
// Set app name for macOS menu bar (must be before ready)
if (process.platform === 'darwin') {
  app.setName('SDRLoggerPlus');
}
```

- [ ] **Step 2.3: Point the unpackaged fallback at the helper's output**

In `getBackendPath()`, replace:

```js
  } else {
    // In development, use the published output
    return path.join(__dirname, '..', 'SDRLoggerPlus.Server', 'bin', 'Release', 'net10.0', runtimeId, 'publish', execName);
  }
```

with:

```js
  } else {
    // Unpackaged (npm start): use the output of scripts/prepare-backend.js
    return path.join(__dirname, 'backend', execName);
  }
```

`runtimeId` stays — it is used in the error message below.

- [ ] **Step 2.4: Fix the stale "Backend Not Found" instructions**

Replace the `errorMsg` line (`main.js:132` pre-edit):

```js
    const errorMsg = `Backend not found at: ${backendPath}\n\nPlease build the backend first using:\nnpm run build:backend:${process.platform === 'win32' ? 'win' : process.platform === 'darwin' ? 'mac-' + process.arch : 'linux'}`;
```

with:

```js
    const errorMsg = `Backend not found at: ${backendPath}\n\nPlease build the backend first using:\nnode scripts/prepare-backend.js ${runtimeId}`;
```

- [ ] **Step 2.5: Syntax check + commit**

Run (from `src/SDRLoggerPlus.Desktop`): `node --check main.js`
Expected: no output, exit 0.

```bash
git add src/SDRLoggerPlus.Desktop/main.js
git commit -m "fix(desktop): pin app name before userData use; un-stale backend dev path and error text"
```

---

### Task 3: NSIS custom include + electron-builder wiring

**Files:**
- Create: `src/SDRLoggerPlus.Desktop/assets/installer.nsh`
- Modify: `src/SDRLoggerPlus.Desktop/electron-builder.yml` (files list + nsis block)

- [ ] **Step 3.1: Create `src/SDRLoggerPlus.Desktop/assets/installer.nsh`**

```nsis
; SDRLoggerPlus custom NSIS hooks for electron-builder.
; Compiled into BOTH the installer and the uninstaller (shared header).
; LogicLib and the runtime ${isUpdated} flag are provided by the generated
; electron-builder script before this file is included.

; ---------------------------------------------------------------------------
; Kill the spawned .NET backend. electron-builder's CHECK_APP_RUNNING only
; manages SDRLoggerPlus.exe; its taskkill /f fallback bypasses Electron's
; before-quit cleanup and orphans SDRLoggerPlus.Server.exe, which holds locks on
; $INSTDIR files and on sdrloggerplus.db. /F from the start: the backend is a
; windowless console process and rejects graceful termination. The USERNAME
; filter keeps an admin's per-machine uninstall from killing another logged-in
; user's backend.
; ---------------------------------------------------------------------------
!macro qtKillBackend
  ReadEnvStr $R8 "USERNAME"
  nsExec::Exec 'taskkill /F /IM "SDRLoggerPlus.Server.exe" /FI "USERNAME eq $R8"'
  Pop $R8
  Sleep 1000
!macroend

!macro customInit
  !insertmacro qtKillBackend
!macroend

!macro customUnInit
  !insertmacro qtKillBackend
!macroend

; ---------------------------------------------------------------------------
; Data-deletion prompt. Runs after app files/registry are removed (electron-
; builder inserts customUnInstall at the end of Section "un.install").
; Never deletes on update (${isUpdated} is a runtime test of the --updated
; switch; upgrades always run the old uninstaller with /S --updated) and never
; on silent uninstall. MB_DEFBUTTON2 is load-bearing: plain MB_YESNO defaults
; to Yes, and Enter must NOT delete the QSO database. /SD IDNO is redundant
; behind the Silent guard — kept as belt-and-suspenders.
; ---------------------------------------------------------------------------
!macro customUnInstall
  ${ifNot} ${isUpdated}
    ${ifNot} ${Silent}
      IfFileExists "$APPDATA\SDRLoggerPlus\*.*" 0 qtDataDone
      MessageBox MB_YESNO|MB_ICONQUESTION|MB_DEFBUTTON2 \
        "Also delete your SDRLoggerPlus data folder?$\r$\n$\r$\nThis permanently deletes your QSO database, settings, and any backups stored in the SDRLoggerPlus data folder ($APPDATA\SDRLoggerPlus).$\r$\n$\r$\nBackups saved to other locations are not touched." \
        /SD IDNO IDYES qtDoDelete
      Goto qtDataDone

qtDoDelete:
      ; Per-machine uninstalls run with SetShellVarContext all, where $APPDATA
      ; is C:\ProgramData — switch to the user context around the delete
      ; (same dance as electron-builder's own delete-app-data block).
      ${if} $installMode == "all"
        SetShellVarContext current
      ${endIf}

      ; Junction guard: NSIS RMDir /r recurses THROUGH directory junctions and
      ; deletes the target's contents. If the data folder root or backups\ is
      ; a reparse point, remove the link itself (plain RMDir) and never recurse
      ; into it. GetFileAttributes returns -1 for a missing path; -1 & 0x400 is
      ; nonzero, so a missing path harmlessly takes the plain-RMDir branch.
      System::Call 'kernel32::GetFileAttributes(t "$APPDATA\SDRLoggerPlus") i .R7'
      IntOp $R7 $R7 & 0x400
      ${if} $R7 <> 0
        RMDir "$APPDATA\SDRLoggerPlus"
      ${else}
        System::Call 'kernel32::GetFileAttributes(t "$APPDATA\SDRLoggerPlus\backups") i .R7'
        IntOp $R7 $R7 & 0x400
        ${if} $R7 <> 0
          RMDir "$APPDATA\SDRLoggerPlus\backups"
        ${endIf}
        RMDir /r "$APPDATA\SDRLoggerPlus"
      ${endIf}

      ${if} $installMode == "all"
        SetShellVarContext all
      ${endIf}

qtDataDone:
    ${endIf}
  ${endIf}
!macroend
```

- [ ] **Step 3.2: Wire it up in `src/SDRLoggerPlus.Desktop/electron-builder.yml`**

In the `files:` list, after `- "assets/**/*"`, add:

```yaml
  - "!assets/installer.nsh"
```

(The `assets/**/*` entry deliberately re-includes build resources because main.js
loads splash/icon from there at runtime; the NSIS script has no business inside
app.asar.)

In the existing `nsis:` block, add the include (explicit so a missing file fails
the build with `InvalidConfigurationError` instead of silently shipping an
unguarded uninstaller):

```yaml
nsis:
  oneClick: false
  allowToChangeInstallationDirectory: true
  createDesktopShortcut: true
  createStartMenuShortcut: true
  shortcutName: SDRLoggerPlus
  include: installer.nsh
```

- [ ] **Step 3.3: Commit**

```bash
git add src/SDRLoggerPlus.Desktop/assets/installer.nsh src/SDRLoggerPlus.Desktop/electron-builder.yml
git commit -m "feat(installer): uninstall data prompt + backend process kill (NSIS custom include)"
```

---

### Task 4: Build + smoke test

**Files:** none modified (build + verification only)

- [ ] **Step 4.1: Build the installer**

Run (from `src/SDRLoggerPlus.Desktop`): `npm run package:win`
Expected: frontend build → backend publish → electron-builder; NSIS compile
succeeds (warnings-as-errors is on, so a malformed installer.nsh fails here);
artifact at `src/SDRLoggerPlus.Desktop/dist/SDRLoggerPlus-1.0.0-win-x64.exe`.

- [ ] **Step 4.2: Pre-flight — record %APPDATA% state**

PowerShell: `Test-Path "$env:APPDATA\SDRLoggerPlus"` → note result (likely True on
this dev machine; it must be unchanged after Step 4.6).

- [ ] **Step 4.3: Silent install + verify footprint**

PowerShell (from `src/SDRLoggerPlus.Desktop`):

```powershell
Start-Process -Wait .\dist\SDRLoggerPlus-1.0.0-win-x64.exe -ArgumentList '/S'
Test-Path "$env:LOCALAPPDATA\Programs\SDRLoggerPlus\SDRLoggerPlus.exe"                       # expect True
Test-Path "$env:LOCALAPPDATA\Programs\SDRLoggerPlus\resources\backend\SDRLoggerPlus.Server.exe"  # expect True
Test-Path "$env:LOCALAPPDATA\Programs\SDRLoggerPlus\resources\backend\wwwroot\index.html"   # expect True
Get-ChildItem "HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall" |
  Get-ItemProperty | Where-Object DisplayName -like 'SDRLoggerPlus*' |
  Select-Object DisplayName, DisplayVersion, UninstallString                       # expect one entry, 1.0.0
```

- [ ] **Step 4.4: Launch, verify backend, graceful quit**

```powershell
Start-Process "$env:LOCALAPPDATA\Programs\SDRLoggerPlus\SDRLoggerPlus.exe"
Start-Sleep 12
Get-Process SDRLoggerPlus, SDRLoggerPlus.Server -ErrorAction SilentlyContinue | Select-Object Name  # expect both
taskkill /IM SDRLoggerPlus.exe        # graceful WM_CLOSE — NO /F (would orphan the backend)
Start-Sleep 8
Get-Process SDRLoggerPlus.Server -ErrorAction SilentlyContinue                                # expect none
```

If `SDRLoggerPlus.Server` lingers, that's the known cleanup path failing — investigate
before uninstalling (a locked db would invalidate Step 4.6).

- [ ] **Step 4.5: Verify uninstaller guard compiled in**

The interactive prompt can't be exercised headlessly; instead confirm the shipped
script content (the source of truth electron-builder compiled):

```powershell
Select-String -Path assets\installer.nsh -Pattern 'MB_DEFBUTTON2', '/SD IDNO', 'REPARSE', 'isUpdated'
```

Expect all four to match. (The default-button bug is exactly the kind that ships
silently — this check is mandatory, per panel.)

- [ ] **Step 4.6: Silent uninstall + verify data survives**

```powershell
Start-Process -Wait "$env:LOCALAPPDATA\Programs\SDRLoggerPlus\Uninstall SDRLoggerPlus.exe" -ArgumentList '/S'
Start-Sleep 5   # NSIS uninstaller copies itself to %TEMP% and may outlive -Wait
Test-Path "$env:LOCALAPPDATA\Programs\SDRLoggerPlus"   # expect False (or only empty residue)
Test-Path "$env:APPDATA\SDRLoggerPlus"                 # expect SAME as Step 4.2 (silent never deletes)
Get-ChildItem "HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall" |
  Get-ItemProperty | Where-Object DisplayName -like 'SDRLoggerPlus*'   # expect none
```

- [ ] **Step 4.7: Leave the interactive Yes/No walk-through for the user**

Note in the final report: re-install + interactive uninstall shows the prompt;
default (Enter) must be No.

---

### Task 5: Regression + wrap-up

- [ ] **Step 5.1: Frontend tests**

Run (from `src/SDRLoggerPlus.Web`): `npx vitest run`
Expected: all pass (192 at last run).

- [ ] **Step 5.2: Backend unit tests**

Run (repo root): `dotnet test src/SDRLoggerPlus.Server.Tests --filter "Category=Unit" --nologo -v q`
Expected: all pass (603 at last run).

- [ ] **Step 5.3: Update CLAUDE.md packaging docs**

CLAUDE.md's "Building for Distribution" section references `npm run package:all`
and the old flow. Update that section to:

```markdown
### Building for Distribution

```bash
cd src/SDRLoggerPlus.Desktop

# Build for current platform (prepares backend + frontend, then packages)
npm run package:win    # Windows: NSIS installer in dist/
npm run package:mac    # macOS (arm64 DMG)
npm run package:linux  # Linux (AppImage + deb)
```

Each `package:*` script runs `node scripts/prepare-backend.js <rid>` (frontend
build + self-contained backend publish into `src/SDRLoggerPlus.Desktop/backend/`),
then electron-builder. Cross-platform packaging happens in CI (release.yml).
```

Also update the "Running without Hot Reload" section's `cp -r dist/* ../SDRLoggerPlus.Server/wwwroot/`
guidance if touched by review — otherwise leave (it's a dev-server flow, not packaging).

- [ ] **Step 5.4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: packaging commands reflect prepare-backend helper"
```

---

## Self-review notes

- Spec §1 → Task 1; §2 → Task 2; §3 → Task 3 (prompt/junction/context dance);
  §4 → Task 3 (qtKillBackend + hooks); §5 → Task 4; Testing → Task 5. The
  spec's "on-exit backups dead on Windows" follow-up is intentionally NOT in
  this plan (separate bug, out of scope).
- `$R7`/`$R8` used (not `$0`/`$1`) to avoid clobbering template registers in
  the un.install section; `IntOp ... & 0x400` = FILE_ATTRIBUTE_REPARSE_POINT.
- `${if} $R7 <> 0` — LogicLib numeric comparison; `!=` is string comparison,
  `<>` is the numeric not-equal.
- Labels (`qtDoDelete`, `qtDataDone`) jump within the same section; LogicLib
  blocks compile to jumps, so crossing `${if}` boundaries with Goto is legal.

# SDRLoggerPlus Rebrand Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute the full rename per `docs/design/sdrloggerplus-rebrand-design.md`. Mechanical; zero behavior change except the one-time data migration and the disabled updater.

**Order matters:** bulk text replace runs BEFORE the migration code is written, because the migration must contain literal `"Log4YM"` strings that the bulk replace would otherwise destroy.

### Task 1: Folder/file renames + bulk text replace
- [ ] `git mv` the five `src/Log4YM.*` folders to `src/SDRLoggerPlus.*`; rename the `.csproj` files inside, `Log4YM.sln` → `SDRLoggerPlus.sln`, `Log4YM.Server.Tests.csproj` etc.
- [ ] Bulk replace in `src/**` (code + json + sln + csproj), `.github/**`, root `README.md`, `CLAUDE.md`: `Log4YM`→`SDRLoggerPlus`, `log4ym`→`sdrloggerplus`, `LOG4YM`→`SDRLOGGERPLUS`. **Excluded:** `docs/**` (historical specs keep origin names), `LICENSE`, `.git`.
- [ ] Regenerate `package-lock.json` names via the same replace (or `npm install` after).
- [ ] Versions → 1.0.0: Desktop `package.json`, Web `package.json` + `version.ts`, Server csproj if versioned.
- [ ] Build server + typecheck web. Commit: `chore: rename Log4YM -> SDRLoggerPlus throughout`.

### Task 2: Data migration (contains intentional "Log4YM" literals)
- [ ] `UserConfigService`: before first config read, if `%APPDATA%/SDRLoggerPlus` missing and `%APPDATA%/Log4YM` exists → copy `config.json`, `log4ym.db` → `sdrloggerplus.db`, `backup-state.json`, `backups/`. Log results; failures non-fatal (fall back to setup wizard). Old dir untouched.
- [ ] Unit tests (temp dirs): fresh install no-op, full migrate, both-exist no-op, partial old dir.
- [ ] Commit: `feat: one-time data migration from Log4YM installs`.

### Task 3: Updater disable + credits + workflow cleanup
- [ ] Desktop main: short-circuit the GitHub update check with a logged notice + comment documenting re-enable (one URL).
- [ ] `AboutDialog.tsx`: credits — Log4YM (Brian Bruff, public-domain origin) and SDRLogger+ (ported feature designs). Update its test.
- [ ] `README.md`: rewrite header for SDRLoggerPlus + lineage/credits section + icon-placeholder note.
- [ ] Delete `.github/workflows/log4ym-agent.yml`; fix paths in remaining workflows; remove the Log4YM-Agent section from `CLAUDE.md`.
- [ ] Commit: `feat: disable upstream updater, add lineage credits, remove upstream agent workflow`.

### Task 4: Verification
- [ ] Full backend suite (`Category=Unit`) + frontend suite + tsc green.
- [ ] Remove `origin` remote.
- [ ] Relaunch backend + Electron: window titled SDRLoggerPlus, About shows credits, data intact post-migration.
- [ ] Commit any stragglers; mark design doc implemented.

# SDRLoggerPlus Rebrand — Design (IMPLEMENTED 2026-06-10)

**Date:** 2026-06-10
**Decision:** Fork Log4YM into the user's own logger, **SDRLoggerPlus** (spelling confirmed), as a **clean break** from upstream `github.com/brianbruff/Log4YM` — no future merges planned. Log4YM is published under the Unlicense (public domain), so forking and renaming is unrestricted; credits are given as a courtesy and at the owner's direction.

## Scope: full deep rename

All naming — internal and user-visible — becomes SDRLoggerPlus. No code behavior changes; every feature (including the seven SDRLogger+ ports from 2026-06-10) works identically.

### Rename map

| Area | From | To |
|---|---|---|
| Solution / projects | `Log4YM.sln`, `Log4YM.Server/.Web/.Desktop/.Contracts(.Tests)` | `SDRLoggerPlus.sln`, `SDRLoggerPlus.*` (folders, csproj, AssemblyName, RootNamespace) |
| C# namespaces | `Log4YM.*` | `SDRLoggerPlus.*` (mechanical solution-wide rename; `BsonElement` attribute strings are NOT touched — stored field names stay stable) |
| npm packages | `log4ym-web`, `log4ym-desktop` (per package.json names) | `sdrloggerplus-web`, `sdrloggerplus-desktop` |
| Electron | `productName: Log4YM`, appId | `SDRLoggerPlus`, `com.sdrloggerplus.app` |
| UI branding | Window title, SplashScreen, AboutDialog, SetupWizard, StatusBar text | SDRLoggerPlus |
| Config dir | `%APPDATA%\Log4YM` | `%APPDATA%\SDRLoggerPlus` |
| Database file | `log4ym.db` | `sdrloggerplus.db` |
| Backup folders | `Log4YM-<timestamp>` prefix; `log4ym.adi` | `SDRLoggerPlus-<timestamp>`; `sdrloggerplus.adi` (prune matches only the new prefix; old `Log4YM-*` folders are left untouched) |
| Version | 1.6.2 (inherited) | 1.0.0 |
| Docs | CLAUDE.md, README, docs/ paths and commands | SDRLoggerPlus equivalents |

### Data migration (one-time, on startup)

`UserConfigService` gains a migration step: if `%APPDATA%\SDRLoggerPlus` does not exist but `%APPDATA%\Log4YM` does, **copy** (not move) `config.json`, `log4ym.db` → `sdrloggerplus.db`, `backup-state.json`, and the `backups/` folder into the new location, then log what was migrated. The old folder is left intact as a safety net. MongoDB users are unaffected (connection string carries over in the copied config).

### Auto-updater

The Electron updater currently checks `brianbruff/Log4YM` GitHub releases (it offered "1.6.2 → 3.12.1" against the wrong product). It is **disabled** — the check is short-circuited with a logged notice — until a SDRLoggerPlus repository with releases exists; re-enabling is a one-line URL change documented in a code comment.

### Git identity

- Keep full history (useful blame; courteous to the origin).
- Remove the `origin` remote; the user adds his own remote when he creates the SDRLoggerPlus repo.
- Remove the Log4YM-Agent issue-triage workflow (`.github/workflows/log4ym-agent.yml`) — it is specific to Brian's repo. The CI test workflow is kept, with renamed paths.

### Credits (owner-directed)

- **README:** lineage section crediting **Log4YM** (Brian Bruff — public-domain origin of the codebase) and **SDRLogger+** (origin of the ported feature designs: awards counting rules, weather alert tiers, S.A.T. protocol handling, Hot List behavior, scheduled-backup scheme).
- **AboutDialog:** the same two credits in short form.
- The per-feature design docs in `docs/design/` (which cite SDRLogger+ in detail) ship with the fork.

## Explicitly out of scope

- Any feature or behavior change.
- Creating the GitHub repository / releases (user action; updater re-enable follows).
- New icon/logo artwork — the existing icon ships until the user supplies SDRLoggerPlus art (placeholder note in README).

## Error handling

- Migration failures are logged and non-fatal: the app falls back to first-run behavior (setup wizard) rather than crashing; the old Log4YM data remains untouched either way.
- If both old and new config dirs exist, no migration runs (new one wins).

## Testing / verification

- Full backend (`Category=Unit`) and frontend suites green after the rename.
- Migration covered by unit tests (fresh install, migrate-from-Log4YM, both-exist cases) against temp dirs.
- Manual: Electron app launches titled SDRLoggerPlus, About shows credits, existing QSOs/settings present after migration, backups write `SDRLoggerPlus-*` folders.

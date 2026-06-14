# Scheduled Auto-Backup — Design

**Date:** 2026-06-10 · Part of the [SDRLogger+ port roadmap](sdrloggerplus-port-roadmap.md)

## Purpose

Automatic, scheduled backups of the logbook with rolling retention, replicating SDRLogger+ v1.09 behavior: each run writes a timestamped folder containing both a raw database copy and an ADIF export, and retention pruning runs **only after a successful write** so a failed run never destroys prior backups.

## Reference behavior (SDRLogger+ `main.py` lines 3601–3870)

- Intervals: `daily` (default), `weekly`, `on_exit`.
- Schedule anchored to **last successful run**, persisted across restarts — a daily backup that already fired today does not re-fire on app relaunch.
- Daemon wakes every 60 s; fires when `now - last_run >= interval`. First enable with no history fires on next tick.
- Each run: create `<dest>/<AppName>-YYYY-MM-DD_HHMM/`, copy raw DB file(s), write ADIF export(s). Track written files and failures; partial success is still success.
- Retention: keep newest N timestamped folders (match on folder-name prefix), delete the rest. Skipped entirely if nothing was written.
- Status surface: enabled, interval, retention, dest, last_run, ok, message, path, next_due. Manual "Run now".

## SDRLoggerPlus design

### Backend — `BackupService` (`Services/BackupService.cs`)

`IHostedService` singleton with a 60-second `PeriodicTimer` loop.

- **What gets backed up:**
  - **LiteDB provider (default):** copy the `sdrloggerplus.db` file. Use `LiteDatabase.Checkpoint()` (via `LiteDbContext`) before copying to flush the WAL, then copy to `sdrloggerplus.db` in the backup folder. ADIF export written alongside as `sdrloggerplus.adi`.
  - **MongoDB provider:** raw dump is out of scope (no mongodump dependency); the ADIF export **is** the backup. Folder contains `sdrloggerplus.adi` only. Status message notes "ADIF only (MongoDB)".
- **ADIF export:** reuse `AdifService`'s existing export path (same code the manual export uses) so field coverage stays in one place.
- **Folder naming:** `SDRLoggerPlus-YYYY-MM-DD_HHmm` (UTC) under the destination root.
- **Default destination:** `<config dir>/backups` where config dir is the same directory that holds `sdrloggerplus.db` (see `Program.cs` `GetDbPath`). Configurable.
- **State persistence:** last_run/ok/message stored in a `backup-state.json` next to the DB (mirrors SDRLogger+'s separate state file; avoids writing UserSettings on every run).
- **On-exit interval:** hooked into host shutdown (`IHostApplicationLifetime.ApplicationStopping`) with a hard time budget (~10 s) so shutdown is never blocked indefinitely.
- **Safety rules (ported verbatim):**
  - Prune only after ≥1 file written successfully.
  - Prune only folders matching the `SDRLoggerPlus-` prefix inside the destination root.
  - All failures captured into the status message, never thrown out of the timer loop.

### Settings — `BackupSettings` on `UserSettings`

```
Enabled (bool, default false)
Interval (string: "daily" | "weekly" | "on_exit", default "daily")
Retention (int, default 10, min 1)
DestinationPath (string?, empty → default dir)
```

### API — `BackupController`

- `GET /api/backup/status` → `{ enabled, interval, retention, dest, lastRun, ok, message, path, nextDue }`
- `POST /api/backup/run` → runs immediately (trigger="manual"), returns updated status.

### Frontend

New **Backup** section in `SettingsPanel.tsx`: enable toggle, interval dropdown, retention number input, destination path input, status line (last run + next due + result message), **Back Up Now** button. No new panel plugin needed.

## Error handling

- Destination not creatable → status `ok:false` with message; daemon keeps ticking (will retry next due time).
- DB file missing/locked → recorded as failure for that artifact; ADIF export still attempted (and vice versa).
- Prune failures are silent per-folder (best effort), counted in the message.

## Testing

- Unit: schedule math (next_due from last_run, anchor-after-run, on_exit returns null), retention prune (keeps newest N, ignores foreign folders, skipped on failed write), state round-trip.
- Integration: run-now against a temp LiteDB file + temp dest dir; assert folder contents (`.db` + `.adi`) and status JSON.
- Filesystem operations behind small abstractions only where needed for unit tests; integration tests use real temp dirs.

## Out of scope

- Restore UI (manual restore = replace the file / import ADIF; document in settings help text).
- Cloud destinations.
- MongoDB binary dumps.

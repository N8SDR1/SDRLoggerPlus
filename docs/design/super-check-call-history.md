# Super Check Partial (SCP) + Call History — design & status

**Status:** v1 IMPLEMENTED (2026-07-26). Corrected after discovering SCP + N1MM-style
call-history files **already existed for contests** — the real work was extending them to
everyday logging + making them work out of the box.

## What already existed (contest-only)
- `Services/Contesting/ScpService` — loads `master.scp` from `%APPDATA%\SDRLoggerPlus\contests\`
  and merges it with the operator's logged calls (`ContestService.GetScpCallsAsync`), served at
  `GET /contest/scp`; the **Contest Entry** window matches partials locally.
- `Services/Contesting/CallHistoryService` — N1MM-style `callhistory.txt` (header + comma rows,
  `!!Order!!` handled) for contest **exchange** prefill. (This was the doc's original "Phase 2".)

## The gap (what was missing)
1. None of it reached the **General Log Entry** — no suggestions, no prior-QSO prefill.
2. No **bundled seed** — SCP was empty until the operator manually dropped in a file.
3. No way to **refresh** the master list without a manual file drop.

## What was built (v1)
**Backend**
- `ScpService` now falls back to an **embedded `Data/master.scp` seed** when no user file exists
  (fixes contest SCP being empty out of the box too). `ImportMaster()` + `UserFileUpdatedUtc`.
- `POST /api/callsigns/scp/update` — server-side fetch of the full ~50k `MASTER.SCP` from
  supercheckpartial.com → validate → save to the user file (which takes precedence) → reload.
  `GET /api/callsigns/scp/status` reports count + last-updated.
- `IQsoService.GetMostRecentByCallsignAsync` + `GET /api/qsos/recent-by-callsign` — for prefill.

**Frontend**
- **Log Entry**: as you type the call, a local prefix-then-contains dropdown over the merged
  `getScpCalls()` set. **Enter still logs the QSO** — it only accepts a suggestion once the
  operator arrows into the list; Tab/click accept, Esc closes. On accept/blur, name + grid
  prefill from the most recent QSO with that call (empty fields only).
- **Settings → Station → Callsign Suggestions**: "Update master list" button (count + date).

## Deferred / follow-ups
- **Worked-before ranking + last-worked hint** in the dropdown — the merged `/contest/scp` set
  is a flat `string[]` with no worked/master flag; richer ranking would need the suggest engine
  shape (reverted earlier to avoid duplicating the contest services). Prefill already delivers
  the "you've worked them" value.
- **QTH/state prefill** (currently name+grid only).
- Help-guide + wiki copy for the everyday SCP dropdown.

## Related, separately scoped
Contest **log separation** (per-contest ADIF export, Log History contest filter, and the bigger
**per-session operating callsign** → callsign-scoped storage for club/multi-op) is tracked in the
contest-audit notes, to be its own design doc. The deciding principle there: *is the operating
call the operator's own call or not.*

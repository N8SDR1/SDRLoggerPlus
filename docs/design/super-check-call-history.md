# Super Check Partial (SCP) + Call History — design

**Status:** proposed (2026-07-26). Scoped after a competitive-gap review — SCP/call-history
is standard in N1MM, Log4OM, Logger32 and absent here. Highest impact-per-effort of the
identified gaps; helps casual logging *and* contesting, not just one.

## What it is

As the operator types a partial callsign, offer the likely full calls, drawn from two
sources and clearly distinguished:

1. **Your own log (worked-before)** — calls you've actually worked. Most valuable: ranked
   first, flagged, and carrying last-worked band/mode/date.
2. **Master database (MASTER.SCP)** — the community list of ~50k active calls from
   supercheckpartial.com. Fills in calls you *haven't* worked yet.

**Call History** is the companion: once a full call is entered/selected, pre-fill the entry
form's empty fields from your most-recent prior QSO with that call (name / grid / state /
QTH), before/around the callbook lookup.

## Why it fits cleanly here

Most of the plumbing already exists:

- **Repo hooks:** `IQsoRepository.GetDistinctCallsignsAsync()` (worked-before universe) and
  `GetMostRecentByCallsignAsync(call)` (call-history pre-fill) — both already implemented.
- **Embedded-resource pattern:** `Data\cty.dat`, `us_counties.csv`, `ffma_grids.txt` are
  `<EmbeddedResource>`s loaded via reflection + `Lazy<>` (see `CtyService`,
  `FfmaGridReference`). **MASTER.SCP drops in as `Data\master.scp`** the same way.
- **Entry points:** exactly two callsign inputs to wire — `LogEntryPlugin.tsx` (General/
  POTA/SAT) and `ContestEntryPlugin.tsx`.

## Architecture

### Backend

- **`Data\master.scp`** — bundled snapshot (embedded resource). Plain text, one call per
  line (`#` comment/header lines skipped), same load style as the county/cty data.
- **`CallHistoryService`** (new, singleton):
  - `Lazy<HashSet<string>>` of master calls loaded once from the embedded resource.
  - Worked-before set from `GetDistinctCallsignsAsync()`, cached with a short TTL and
    invalidated on QSO create/import (reuse the existing invalidation signals).
  - `Suggest(partial, limit)` → ranked candidates: worked-before matches first
    (each decorated with last-worked via `GetMostRecentByCallsignAsync`), then master-only
    matches. Match = **prefix first, then contains** (classic SCP matches the fragment
    anywhere; we surface prefix hits above substring hits).
- **Endpoint** `GET /api/callsigns/suggest?q=<partial>&limit=8` → `[{ call, worked,
  lastWorked?: { date, band, mode }, source: 'log' | 'master' }]`. Debounced client-side;
  cheap (in-memory sets).
- **Master update (phase 1.5):** `POST /api/callsigns/scp/update` fetches the latest
  MASTER.SCP from supercheckpartial.com into app-data, preferred over the embedded snapshot
  when present. Offline-first: embedded snapshot always works with no network (cty.dat model).

### Frontend

- **`useCallsignSuggest(q)`** hook — debounced (~150 ms) fetch, cancels in-flight on new keys.
- **Autocomplete dropdown** under the callsign input in `LogEntryPlugin` and
  `ContestEntryPlugin`:
  - Keyboard: ↑/↓ to move, Enter/Tab to accept, Esc to dismiss — must not fight existing
    Enter-to-log behaviour (accept only when the list is open).
  - Rows show the call with the **matched fragment highlighted**; worked-before rows styled
    distinctly (e.g. green + a small "worked 20m · 3 mo ago" hint); master rows plain.
  - Selecting a row completes the call and triggers **call-history pre-fill**.
- **Call-history pre-fill** on commit: fetch most-recent QSO for the call, fill only *empty*
  fields (name/grid/state/QTH). Precedence: prior-QSO first (fast, offline, personal) →
  existing callbook gap-fill covers what's still blank. Never overwrite what the operator
  typed.

### Settings

- **Settings → Logging → Super Check Partial**: on/off (default on), and an **"Update master
  database"** button showing the current snapshot date + result.

## Scope

**v1 (this arc):**
- Embedded MASTER.SCP + `CallHistoryService` + suggest endpoint.
- Autocomplete on Log Entry + Contest Entry with worked-before ranking and last-worked hint.
- Call-history pre-fill of empty name/grid/state/QTH from the prior QSO.
- Settings toggle.

**v1.5:** online "update master database" fetch.

**Phase 2 (contest-grade, later):**
- **Importable N1MM-style call-history files** (call → user-defined exchange fields: section,
  zone, name, state, power) so the exchange auto-fills in contests — the piece serious
  contesters expect. v1 already covers exchange-from-your-own-prior-contest-QSOs implicitly
  via call history; external files are the add-on.
- Per-contest exchange memory / "same exchange as last time" prompts.

## Open decisions (for operator)

1. **Match style** — prefix-then-contains (proposed) vs strict SCP anywhere-match with
   highlight only. Prefix-first reads better for casual logging; contest purists expect
   anywhere-match. (Proposed: do both, prefix ranked above contains.)
2. **Where it appears** — Log Entry + Contest Entry for sure. Also the POTA/SAT tabs? (They
   share the Log Entry callsign field, so likely free.)
3. **Master snapshot cadence** — how often to refresh the bundled `master.scp` in releases
   (it drifts; supercheckpartial.com updates ~weekly during contest season).
4. **Worked-before "recency" display** — show last-worked band/mode/date inline, or only on
   hover? (Proposed: compact inline hint, full detail on hover.)

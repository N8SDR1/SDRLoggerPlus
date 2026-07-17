# WSJT-X Decode Alerts & Grid Tracker — Design

**Status:** Draft for review (Rick + Brent)
**Owner:** TBD (builds on `WsjtxService` + the DX Coach needs engine + award trackers)
**Goal:** Bring the *popular* JTAlert and GridTracker features **natively** into SDRLogger+ —
real-time needed-status alerting on the FT8/FT4 decode stream, with the one thing neither app
does cleanly: **geographically-scoped needed alerts** (e.g. *"only alert me for **needed US
grids in North America**"*). No third-party bridge; one integrated app.

The pitch: today an op runs WSJT-X/MSHV → **JTAlert** (alerts) → **GridTracker** (map) →
their logger, juggling a UDP chain of four programs. SDRLogger+ already *is* the logger, already
listens on the WSJT-X UDP port, already owns the award/needs data, the confirmation data, and a
2D map + 3D globe. We collapse the whole chain into one app.

---

## 1. The one principle that makes it cheap

> **A WSJT-X decode is just another spot. Point the DX Coach needs engine at it.**

The DX Coach already answers *"does working this call, on this band/mode, fill an award gap?"*
against the operator's award state (`ai-dx-coach.md` §3.1). A decode carries exactly the same
essentials as a cluster spot — **call, band, mode, (maybe) grid** — so the decode stream is
simply a **second source** feeding the same needs-matrix join. We are not building a new needs
engine; we are giving the existing one a new intake and adding two dimensions (grid, geographic
scope) plus a fast, per-decode alert path.

**Corollary:** the alerting is deterministic and testable. "Is this needed?" and "is it in my
chosen region?" are data questions, never guesses.

---

## 2. What we already have (this is mostly wiring, not new plumbing)

| Piece | Where it lives today |
|---|---|
| WSJT-X UDP socket, 2 settings-gated sources, dedupe, client table | `Services/Wsjtx/WsjtxService.cs` |
| Big-endian frame parser (magic/schema/type/id, strings, QDateTime) | `Services/Wsjtx/WsjtxMessageReader.cs` |
| Needs-matrix join (worked vs needed per band/mode) | DX Coach Opportunity Engine + award trackers (DXCC, WAS, WAZ, VUCC, WPX, …) |
| Confirmed vs worked per call/band/mode | Confirmation merge (`Qsl.{Lotw,Eqsl,Qrz}.Rcvd`) |
| Callsign → DXCC / state / zone / grid / continent | QRZ lookup + cty.dat fallback |
| Alerts + TTS + volume + quiet-hours precedent | RBN alerts, Hot List, DX Coach voice, shared Voice section |
| Map + globe to color grids on | 2D Leaflet map + 3D globe panels |
| Rig control backends for "take me there" | `TciRadioService.cs`, `HamlibNative.cs`, `FlrigService.cs` |

**New pieces:** Decode(2) parsing, a **decode → needs → alert-rule** path, the **geo-scoped
rule model**, outbound **Reply(4)** and **HighlightCallsign(13)**, and (later) a grid-tracker
map overlay.

---

## 3. The Stage-1 gap (small and well-scoped)

`WsjtxMessageReader.Parse` currently handles only types **0 / 5 / 6 / 12**; everything else —
including **Decode (2)** and **Status (1)** — hits `_ => null` and is dropped. MSHV is already
broadcasting decodes ("Enable Decoded Text" → 127.0.0.1:2237), so **the packets arrive and we
throw them away today.** Closing the gap:

- Add three primitives to the existing `Reader`: `ReadI32` (signed), `ReadDouble` (f64),
  `ReadBool` (u8).
- Add a `WsjtxDecode` record + `case 2:` to the parser.
- Route decodes to a new handler in `WsjtxService` (parallel to `LogQsoAsync`).

### Decode (type 2) payload — after the common `id`
| Field | Wire type | Use |
|---|---|---|
| New | bool (u8) | only alert on `New` decodes (skip replays) |
| Time | u32 (ms since midnight UTC) | decode timestamp |
| snr | **i32 (signed)** | display / sort |
| Delta time | f64 | display |
| Delta frequency | u32 (Hz, audio offset) | needed for the **Reply** (which audio slot to call) |
| Mode | utf8 | FT8/FT4/… |
| **Message** | utf8 | the decoded text — **parse this for call + grid** |
| Low confidence | bool | drop if set (optional) |
| Off air | bool | ignore |

> **Verify byte-widths against a live MSHV/WSJT-X capture in Stage 1** before trusting the
> parser — Qt's `QDataStream` float/double serialization is the one place to confirm empirically
> rather than from the header. Unit-test the reader against a captured datagram.

### Parsing the decoded message text
FT8/FT4 message text is the real payload. We extract **the caller and, when present, a 4-char
grid**:

- `CQ K1ABC FN42` / `CQ DX K1ABC FN42` → caller `K1ABC`, grid `FN42` — **CQ decodes are the
  primary alert trigger** (a station available to work).
- `W9XYZ K1ABC FN42` → reply-with-grid (K1ABC calling W9XYZ).
- `W9XYZ K1ABC -15` / `R-15` / `RR73` → report/roger exchanges (no grid).

When the message has no grid, resolve grid/state/DXCC from the **callsign** via the existing
cty.dat + QRZ path (same enrichment the DX Coach uses). So every decode ends up with:
`{ call, band, mode, grid?, dxcc, state?, cqZone?, continent, snr, audioOffsetHz, isCq }`.

---

## 4. The feature that beats both apps: geo-scoped needed alerts

The rule model — flexible enough to express *"needed US grids, NA only, 20m FT8"* as one row:

```
Rule = {
  enabled,
  award:      DXCC | State(WAS) | Grid(VUCC) | CQZone(WAZ) | Prefix(WPX) | Continent | AnyNew,
  neededOnly: bool,          // only fire if it fills a gap (per band+mode granularity)
  confirmedCountsAsWorked: bool,  // "needed" = not worked, or not confirmed
  scope: {                   // the GridTracker-killer — geographic filter
    continents?:  [NA, EU, …],
    dxccEntities?: [...],     // include/exclude list
    usStates?:    [...],      // subset of states
    gridFields?:  [FN, EN, …] // 2-char field prefixes / region box
  },
  bands?: [...], modes?: [...],
  actions: { sound?, popup?, highlightInWsjtx?, mapFlash?, pushSpotToRig? },
  priority
}
```

Evaluation per decode: `neededOnly` runs the **DX Coach needs join**; `scope` is a pure
predicate over the decode's resolved location; `bands/modes` filter; first matching rule (by
priority) fires its actions. All deterministic, all unit-testable, all offline.

This is the answer to the original gripe: JTAlert's alert categories aren't cleanly
region-filterable, so people jump to GridTracker's map to *see* needed grids — SDRLogger+ gives
both the **targeted alert** and the map from the same rule.

---

## 5. Outbound — the two messages that make it interactive

Both are new frames we *send* back to the decoding client's endpoint (small, well-documented):

- **Reply (type 4)** — "call this station." Double-click a needed decode → we send a Reply
  (echoing the decode's time/snr/deltas/message) → WSJT-X/JTDX sets up the QSO (moves to the
  station's audio offset, enables TX). This is transport-independent — the decoding app already
  owns the radio, so we touch no CAT. **Confirm MSHV honors inbound Reply**; if not, MSHV falls
  back to alert-only (operator clicks in MSHV).
- **HighlightCallsign (type 13)** — "color it in WSJT-X's band-activity window." Push a color per
  needed category (new DXCC / new state / new grid …) so the operator sees needs highlighted in
  WSJT-X itself, JTAlert-style. Pure UDP; works for every rig-control setup.

---

## 6. Rig control is optional and already largely built

The alert/needs/highlight/map features need **zero** rig control — they're read-side off the
decode stream. Rig control only powers the *optional* "take me to this station," as a pluggable
backend that the alert engine is blind to:

| Backend | Transport | Status |
|---|---|---|
| WSJT-X **Reply** | UDP back to the decoding app | best for digital; new frame |
| **TCI** → Lyra | WebSocket spot + click-to-tune | `TciRadioService.cs` exists; Lyra ingests spots |
| **Hamlib** | `rigctld` TCP :4532 | `HamlibNative.cs` exists |
| **FLRig** | XML-RPC :12345 | `FlrigService.cs` exists |
| none | — | alert-only, tune manually |

**Contention rule:** while MSHV/WSJT-X is running it *owns* the radio via its own
Hamlib/FLRig/TCI link — SDRLogger+ must not grab the same port. The digital path therefore uses
the **UDP Reply** (goes *through* the decoding app), and only takes direct rig control in the
standalone/non-digital case. The **TCI → Lyra spot** path is the integration showpiece: a needed
grid lands on Lyra's panadapter, click to tune.

---

## 7. The GridTracker view (later phase)

We already have the map + globe; the confirmation merge already knows worked vs confirmed. A
grid-tracker overlay is: draw the maidenhead grid, color each square **needed / worked /
confirmed** from award + confirmation state, and drop live decode pins (colored by needed
status, faded by age). Award-progress overlays (VUCC grid count, WAS states) reuse the awards
dashboards. This is presentation on top of data we already compute — it's deliberately *after*
the alert engine, not the foundation.

---

## 8. Settings (typed model, mirrored TS)

```
WsjtxAlertsSettings {
  Enabled (bool, default false)
  Rules   (List<AlertRule>)          // the §4 model; ship 2-3 sensible presets
  Highlight { Enabled, colorByCategory }   // outbound type 13
  Reply   { Enabled }                // outbound type 4 (double-click to call)
  Voice   { Enabled }                // reuse shared Voice section + volume
  QuietHours, GlobalCooldown, DedupWindow   // reuse RBN/Hot List/Coach patterns
  RigAction { backend: none|reply|tci|hamlib|flrig }
}
```

Lives under the existing WSJT-X settings section (next to the two UDP sources). Presets to ship:
*"New DXCC (any)," "Needed WAS state," "Needed grid — my continent only."*

---

## 9. Phasing

| Phase | Deliverable | Value |
|---|---|---|
| **1** | **Decode ingestion** — parse Decode(2), extract call+grid, enrich, surface a live "Decodes" list with needed-status coloring (reuse needs engine). No rules yet. | Proves the pipe; instantly useful; de-risks everything |
| **2** | **Geo-scoped alert rules** — the §4 rule model + evaluation + sound/popup/voice (reuse Voice + quiet-hours). **The flagship gripe-killer.** | The feature people leave JTAlert *and* GridTracker for |
| **3** | **Outbound** — HighlightCallsign(13) into WSJT-X + Reply(4) double-click-to-call (+ TCI spot → Lyra). | JTAlert-parity + the Lyra integration showpiece |
| **4** | **Grid-tracker map overlay** — needed/worked/confirmed grid coloring + live decode pins + VUCC/WAS overlays. | The GridTracker view, native |

**Ship Phase 1 first** — same discipline as the DX Coach: the decode→needs pipe is the
foundation, alerts and map layer on top.

---

## 10. Risks & non-goals

- **CAT contention** → never co-own the rig with the decoding app; digital = Reply, not CAT.
- **Alert fatigue** → `neededOnly` + geo scope + dedup + cooldown + quiet-hours + off-by-default
  (the Hot List / DX Coach "right and rare" lesson).
- **Wire-format surprises** → verify Decode(2) byte-widths against a live capture; unit-test the
  reader before trusting it.
- **MSHV inbound gaps** → Reply/Highlight may not be honored by MSHV; degrade to alert-only, never
  block the read-side features on it.
- **Needs accuracy** → reuse the award trackers as the single source of truth (don't recompute).
- **Non-goal:** we are not re-implementing all of GridTracker (logbook analytics, ADIF diffing,
  its own map tiles) — just the popular, high-signal pieces: needed alerts + a needs-colored grid
  map.

---

## 11. Open questions

1. Backend service shape — a `WsjtxDecodeService` (or fold into `WsjtxService`) that raises a
   SignalR `OnDecodeAlert` / `OnDecode`, so alerts fire without any panel open.
2. Rule storage/UI — a rule builder in Settings vs. a compact "add rule from this decode" action
   on the Decodes list (probably both).
3. Which awards in the v1 rule model — start **DXCC + WAS + Grid**, widen after (mirrors the DX
   Coach v1 scope).
4. Does MSHV honor inbound **Reply(4)** and **HighlightCallsign(13)**? (Bench test in Phase 3.)
5. Do CQ-only alerts cover the need, or do we also alert on reply/report decodes of needed calls?
   (Lean CQ-first to keep signal high.)
6. Decode volume throttling — a busy FT8 band is dozens of decodes per cycle; ensure the needs
   join + enrichment is cheap (cache callsign→location) and the SignalR fan-out is batched.

---

*Built on existing subsystems: `WsjtxService`, the DX Coach needs engine + award trackers, the
confirmation merge, QRZ/cty.dat enrichment, the RBN/Hot List/Voice alert stack, the map/globe,
and the existing TCI/Hamlib/FLRig rig backends. This feature is orchestration + two new UDP
message types, not new plumbing.*

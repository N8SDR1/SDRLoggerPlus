# Lyra ↔ SDRLogger+ "Combo" link — SDRLogger+ side

**Canonical spec** (shared message contract, contact object, decisions):
`SDRProject/lyra-cpp/docs/architecture/combo_link_design.md`.
Edit the canonical file; keep this pointer + the SL+-specific notes in sync.

## What SDRLogger+ does in the combo

The link rides the **existing TCI connection** SDRLogger+ already opens to Lyra —
no new connection, no bridge app. Lyra owns the single master toggle; SDRLogger+
has **no toggle**, it shows a read-only **`Lyra Combo: Linked ●`** indicator and
acts only while linked.

SDRLogger+'s role = the callbook + the log (what it already does best):

| Stage | Inbound (Lyra → SL+) | SL+ action |
|---|---|---|
| **A** | *(reuses `spot_activated`)* | Populate the log-entry callsign + QRZ/HamQTH lookup — **already implemented** (the `spot_activated` handler added 2026-07). Zero new code for the first proof. |
| **A′** | — | After the callbook resolves the call, **push the result back** to Lyra as `lyra_contact:sdrlog,<call>,,,<name>,<qth>,<grid>,;` so Lyra's `{NAME}`/`{QTH}` CW macro tokens fill. |
| **B** | `lyra_log:<call>,<rstSent>,<rstRcvd>,<mode>,<freqHz>;` | Submit the current log-entry form (log the QSO). |
| **C** | — | On spot click / manual select, send `lyra_contact:sdrlog,…` so Lyra's His Call fills. |

## Echo guard (required)

Do **not** re-broadcast a `lyra_contact` field that was applied from an inbound
`lyra_contact` (the `src` tag = `lyra` vs `sdrlog` marks provenance). Prevents the
call↔name bounce loop. See canonical spec §6.

## Implementation notes

### Stage A — SHIPPED (2026-07-07, call export Lyra→SL+ + Linked indicator)
- `TciRadioService.ProcessMessageAsync` handles two inbound commands:
  - `lyra_combo:on|off` → sets `_comboLinked`, broadcasts `ComboLinkChangedEvent`.
  - `lyra_contact:lyra,<call>,…` (while linked) → dedups on `_lastComboCall`,
    then broadcasts a `SpotSelectedEvent(call, rxFreqKhz, "CW", grid)` — reusing
    the existing spot→log-entry pipeline so the callsign populates + QRZ/HamQTH
    lookup fires. `src != lyra` (our own `sdrlog` echoes) is ignored.
- `ComboLinkChangedEvent(bool Linked, string RadioId)` in Contracts;
  `ILogHubClient.OnComboLinkChanged` + `BroadcastComboLinkChanged`;
  `signalr.ts` interface + handler; `appStore.comboLinked`; `useSignalR`
  `onComboLinkChanged → setComboLinked`.
- Indicator: a read-only **`● Lyra Combo`** badge in the Log Entry panel header
  (`LogEntryPlugin`), visible only while `comboLinked`.
- Backend builds clean; frontend typechecks clean.

### Stage A′ / B / C — TODO
- **A′ name-back:** after the callbook resolves (`LogHub.FocusCallsign`), if
  linked, send `lyra_contact:sdrlog,<call>,,,<name>,,<grid>,;` back over TCI so
  Lyra's `{NAME}` token fills. Needs an outbound-send path on `TciRadioService`
  (the WebSocket is client-side) + the echo guard (don't send back a call we
  just received without new info).
- **B `{LOG}`:** parse `lyra_log:<call>,<rstSent>,<rstRcvd>,<mode>,<freqHz>;` →
  submit the current log-entry form.
- **C reverse call push:** send `lyra_contact:sdrlog,<call>,…;` on spot click /
  manual select so Lyra's His Call fills.

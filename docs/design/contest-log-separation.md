# Contest log separation & per-session callsign — design

**Status:** proposed (2026-07-26). **Stage 1 is a release blocker** — it prevents a real
data-integrity/rules hazard and must ship in the same release as the SCP + grid work.

## The hazard (confirmed in code)
Contest QSOs are stored in the one shared logbook, tagged by session, but:
- **No per-QSO operating callsign.** The `Qso` model records no "who/what call logged this."
- **ADIF export** stamps `STATION_CALLSIGN` from the **global** `settings.Station.Callsign`
  (`AdifService.cs:464`); Cabrillo likewise uses the global call.
- **Upload sync** (`GetUnsyncedToQrzAsync`, LoTW equivalent) returns **all** unsynced QSOs —
  **no callsign filter**.

⟹ Run a contest under **any call that isn't your personal one** — a **club call** for Field
Day, a **/P**, a **special-event** call — and those QSOs land in your personal logbook and get
**auto-uploaded to your personal QRZ/LoTW certificate under the wrong callsign**. That pollutes
your personal awards and is wrong-call logging (breaks LoTW/award rules). Users must not be able
to do this by accident.

## Core principle
The dividing line is **operating callsign identity: "is this my personal call or not."**
- Own call (you, or a guest op, using *your* call at *your* station) → belongs in your log.
- A **different** call (club / special / rover under a club call) → **never** your personal
  log/LoTW. It's a distinct logbook identity.

## Data model (the spine)
1. **Per-session operating callsign** on `ContestSession` (+ `StartContestSessionRequest`),
   **defaulting to the station call**. The Start-Contest UI gets a callsign field, prefilled,
   editable (club/`/P`/special-event).
2. **Stamp it on every contest QSO** — add `Qso.StationCallsign` (ADIF `STATION_CALLSIGN` /
   `OPERATOR`). Null ⇒ personal/global call (all existing + casual QSOs stay null → unchanged).

## Stage 1 — SAFETY (release blocker; "do no harm")
Goal: it must be **impossible to silently pollute your personal log/LoTW** with QSOs made under
another call. Minimum, bounded, ships with the SCP/grid release.
1. Add the per-session callsign (model + `StartContestSessionRequest` + Start-Contest UI, default
   = station call).
2. Stamp `Qso.StationCallsign` on each contest QSO from the session.
3. **Upload guard:** the personal QRZ/LoTW/eQSL/ClubLog/HRDLog upload + auto-sync includes only
   QSOs where `StationCallsign` is null or equals the operator's personal call. Different-call
   QSOs are **excluded** from personal uploads (can't be sent to the wrong cert). They still
   Cabrillo-export for contest submission — the actual Field-Day deliverable.
4. **Export uses the QSO's own call** — ADIF `STATION_CALLSIGN` and Cabrillo `CALLSIGN:` come
   from `Qso.StationCallsign ?? global`, not the global unconditionally.
5. **Log History badge** — mark QSOs logged under a non-personal call (so they're visibly
   distinct, and the operator understands why they're not uploading personally).
6. **Exclude from personal awards/stats** *(operator decision 2026-07-26)* — a different-call QSO
   counts for NOTHING personal: not DXCC/WAS/grids/IOTA/counties/VUCC/FFMA, not statistics, and
   not the live "worked-before" needed-status for spots/decodes. Central rule: a QSO is "personal"
   iff `StationCallsign` is null or equals the operator's personal call.

Stage 1 deliberately does NOT yet let you upload under the *club's* LoTW cert (that needs the
club's TQSL cert/station location — Stage 2). It just stops the wrong upload. Cabrillo (the
contest submission) works regardless.

## Stage 2 — separation affordances (solo, post-release)
- **Per-contest ADIF export** — add a session/contestId filter to the ADIF export.
- **Log History filter** — All / Only-contest / Hide-contest / pick-a-contest / by-call.
- **Per-call LoTW/QRZ upload** — supply a club/alt call's TQSL cert + station location so a
  contest *can* upload under its own identity when the operator wants.
- Optional: exclude different-call QSOs from personal award stats (or a toggle).

## Stage 3 — multi-op / LAN (own design pass, later)
Networked multi-op (N1MM's shared-DB model): the contest log is a **separate shared database for
that period**, multiple operators writing concurrently over the LAN, **never** attached to one
operator's personal logbook; merge into a personal log only afterward, and only if the op used
their own call. This is the case that *forces* separate storage — see the competitor-gap note in
`sdrloggerplus-v2-rig-refactor` memory. Scope when the LAN feature is taken on.

## Migration / compatibility
- `Qso.StationCallsign` is nullable; **all existing QSOs stay null** and behave exactly as today
  (uploaded as personal, exported with the global call). No migration needed.
- The upload guard treats null as "personal," so nothing that works today changes for solo ops.

## Recommendation
Build **Stage 1** now, ship it with SCP + grid + Flex-supported. Stages 2–3 follow. Stage 1 is
small and additive but closes a rules-breaking footgun before it reaches users.

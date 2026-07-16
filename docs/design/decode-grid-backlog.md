# Decode / Grid Tracker — backlog

Small-polish and parked items for the WSJT-X decode-alerts + Grid Tracker arc
(Phases 1–4 are shipped on `v2-alpha`; see `wsjtx-decode-alerts-design.md`).
None of these are blocking — they're nice-to-haves captured so they aren't lost.

## Small polish (nice-to-have)

- **"Alert on ones like this" bell** on each Digital Decodes row — one click
  pre-fills a new Decode Alert rule from that decode (its need + region already
  set), then fine-tune in Settings → Decode Alerts.
- **Quiet hours** for decode alerts — a time window where rules don't fire
  sound/voice (reuse the RBN/Hot List quiet-hours pattern).
- **Configurable columns** on the Digital Decodes panel — let the operator pick
  from Call / Grid / DXCC / WPX / CQ-zone / ITU-zone / State / County / Continent
  / raw MSG / LoTW / eQSL (GridTracker "Call Roster" parity). Currently a fixed
  set.
- **Worked-unconfirmed as a distinct 3rd state** on the Digital Decodes panel —
  we already have confirmation data (Qsl.*.Rcvd); show needed / worked-unconfirmed
  (shaded) / confirmed like GridTracker does. Panel currently shows two.
- **Grid Tracker: configurable colors** — expose the worked/confirmed/needed/
  active colors in Settings (default red/green/cyan). Operators (and legibility
  needs) vary.
- **Grid Tracker: click-a-square detail** — popover showing what was worked in
  that grid (calls / when / band), or active stations there right now.

## Parked (bigger, deliberate)

- **Better announcement voices** — Electron only exposes local Windows SAPI
  voices (2–3), not the rich neural voices Edge shows. Options considered:
  - Azure Speech F0 (free 500K neural chars/mo, perpetual) — **REJECTED**: the
    free Azure account requires a credit card for verification (even though F0
    never bills), which is an adoption barrier for hams.
  - **Piper — bundled offline neural TTS (leading candidate)**: no account/key/
    card/internet, better than SAPI, works out of the box. Cost = installer size
    (~20–60 MB/voice) + per-platform binaries in CI. Voice-engine toggle (Local
    Windows / Bundled neural), local stays default.
  - Windows language-pack voices — zero engineering, more accents, still SAPI
    quality (documentation-only stopgap).
  - Edge "read-aloud" endpoint — free/no-key but unofficial/ToS-gray; not for a
    shipped product.
- **Option B — needed-only spots to Lyra** — instead of MSHV flooding Lyra's
  panadapter with every decode, SDRLogger+ pushes only *needed* ones (color-coded)
  as a smarter replacement. Requires the operator to turn MSHV's TCI spotting off.
  Parked because MSHV already spots to Lyra (ours would duplicate).

## Dropped

- **HighlightCallsign (WSJT-X type 13)** — redundant: needed decodes are already
  coloured in the Digital Decodes panel and visible in JTDX's own list.
- **Push-to-Lyra spots (as-is)** — would duplicate MSHV's existing TCI spots (see
  Option B above for the non-duplicate version).

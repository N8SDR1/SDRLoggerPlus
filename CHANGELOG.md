# Changelog

All notable changes to SDRLoggerPlus v2 are recorded here.
This file is bundled with the app and shown in About → Changelog.

## 2026-08-01 — v2.14.2 "Antares"

**Three fixes from the field — LoTW CW uploads, dark-theme paging buttons, and spot modes.**

### Fixed — LoTW/QRZ rejected CW contacts logged as CWL/CWU
SDRLogger+ tracks which CW sideband you're on (CWU/CWL) for rig control, but LoTW (TQSL) and the ADIF
spec only accept plain **`CW`** — so uploading a CWL contact failed validation ("error on line …").
Exports now emit **`CW`** for the whole CW family, across **LoTW, QRZ and file export**, so they
validate. USB/LSB and other modes are unchanged. *(Thanks K3UK.)*

### Fixed — Log History page buttons were invisible on dark themes
The pager arrows and "page X of Y" text weren't getting enough contrast on the dark themes (they use a
grid control whose secondary colors we hadn't themed). They now follow each theme's text color and read
clearly on **every** dark theme — verified from ~11:1 to 16:1 contrast. *(Thanks W1DFC.)*

### Fixed — a spot with no mode could leave the rig on the wrong mode
Clicking a spot whose feed carries a blank mode used to send an empty mode command to the radio, which
could land it on a wrong/digital mode. It now falls back to the band's phone mode (LSB below 10 MHz, USB
above). *(Thanks Zuzudaddy.)*

## 2026-07-31 — v2.14.1 "Antares"

**Hotfix — DX cluster spots now work in comma-decimal locales.**

### Fixed — "Error - Commas not allowed" when spotting
On systems whose regional format uses a **comma decimal separator** (e.g. Romanian, German), the spot's
frequency was sent as `21074,0` instead of `21074.0`, and DX cluster nodes reject the comma — so the spot
was refused. Frequencies are now always formatted with a period regardless of your locale, and any commas
in the comment are stripped too. *(Caught in the wild by YO8RFS thanks to v2.14.0's new cluster-reply
messages — which is exactly what surfaced the reason.)*

## 2026-07-31 — v2.14.0 "Antares"

**Whole-log filtering & export, ARRL-rules grid counting, cluster spot diagnostics, and a country-name cleanup tool.**

### ⚠ Heads-up — grid confirmation counts now default to LoTW + card
Grid Tracker, the map overlay, and VUCC now count **award-valid confirmations (LoTW + paper card)** by
default — the ARRL rule — instead of counting any confirmation. If you also count **eQSL / QRZ**, flip the
new **All conf** toggle and your numbers return. Nothing in your log changed; only what the grid views count.

### New — Filter the whole log, export what you see
Log History can now filter your **entire** log in the browser (call, name, band, mode, date range), and
**Export** gives you exactly the filtered set — across all pages, not just the current one. (#41)

### New — Standardize country names (Settings → Logbook Health)
Imported logs keep whatever the source file wrote for the country, so one entity can show up as
**"USA"**, **"UNITED STATES OF AMERICA"** and **"United States"** all at once. The new tool finds entities
logged under more than one spelling — grouped by **DXCC number**, so they're provably the same country —
and unifies them to a standard name. Opt-in, preview first, **backup taken automatically**, and **local-only**
— it never re-uploads to or changes QRZ / LoTW / eQSL.

### New — See what the DX cluster says when you spot
When you send a spot, SDRLogger+ now **reads the cluster's reply** and shows it — so a silently-rejected
spot (e.g. an unregistered user) tells you why instead of looking like it worked. Plus a per-cluster
**VE7CC extended (CC11)** toggle: turn it off to connect in plain mode like most loggers (no `-0` on your
call), which some nodes need to accept your spots.

### Improved — Awards & export
- **ARRL-rules confirmation counting** for grids/VUCC, with the LoTW+Card / All-conf toggle above. (#46)
- **Whole-log export/upload no longer truncates** — ADIF export and LoTW selection see every QSO, even past
  100k. Already-uploaded QSOs are still skipped, so nothing is re-sent. (#56)

### Fixed
- **Rotator (PSTRotator)** — resynchronises the controller stream so the beam stops lagging, and rebuilds
  the connection on a wider range of faults. (#58)

*Thanks to Brent (@n9bc) for the log-filtering, grid-counting, export and rotator work, and to Tic (YO8RFS)
and K3UK for the cluster and country-name reports.*

## 2026-07-31 — v2.13.1 "Vega"

**Fixes a lockout when hosting a shared log, and lets the multi-op panel follow your rig.**

### Fixed — "Share on network" could lock you out of your own app
Turning on **Share this log on my network** made the app require an access token for *every*
connection — including its own desktop UI, which doesn't send one. The result was an endless
"Reconnecting to the server…" screen that also blocked you from getting back into Settings to turn
hosting off. The local machine is now trusted on its own backend (only *other* stations on the
network need a token), so hosting works without locking yourself out. If you hit this on 2.13.0,
updating fixes it. (#57)

### Fixed — Never get trapped by a connection screen again
If the connection can't be re-established after a few tries, the reconnect overlay now offers
**"Continue to app (work offline)"** so you can always reach Settings and fix your connection —
reconnection keeps retrying in the background.

### New — Multi-op panel follows your rig
The **Multi-op Coordination** panel's *You're on* band/mode now auto-fill from your connected rig
(with "Follow radio" on) and update as you tune, instead of starting blank — so your presence on the
who's-on-what board is right without re-declaring it. The selectors stay editable.

### New — POTA spot fills the park you worked
Clicking a station in the **POTA** panel now carries its **park reference** into the log, not just the
callsign and frequency. In **POTA mode** it fills a new **Park (worked)** field — valid whether you're
activating or hunting from home — which uploads as a proper POTA hunt (`SIG_INFO`); the explicit
**P2P** field stays and mirrors into it. In **General mode** it drops a `POTA US-1234` note into
Remarks. (#59)

## 2026-07-28 — v2.13.0 "Vega"

**A batch of quality-of-life features from the issue tracker — plus the first multi-op contest-serial coordination.**

### New — Multi-op contest serials come from the host
When you're logging to a shared host (multi-op networked logging), contest serial numbers are now
drawn from the **host's** sequence instead of each machine counting on its own — so two operators
never log the same number. The on-screen "next serial" preview reflects the host's count, and if the
host briefly drops out, logging falls back to a local number rather than stalling. This matches how
N1MM and N3FJP behave in multi-op. (#52)

### New — Sort DXCC by prefix in Statistics
The DXCC breakdown can now be sorted by **primary prefix**, and the prefix is shown alongside each
entity — handy when you think in prefixes rather than country names. (#49)

### New — Q65 and MSK144 modes
Added **Q65** and **MSK144** to the mode dropdown for logging meteor-scatter and weak-signal
contacts. (#50)

### New — QRZ profile shows LoTW / eQSL / QSL badges
The QRZ profile card now shows at a glance whether a station accepts **LoTW**, **eQSL**, and paper
**QSL** — so you know how to confirm before you even finish the QSO. *(Thanks to Brent, @n9bc, for
this contribution.)* (#39)

### New — Turn off auto-filtering of Log History by callsign
The Log History panel auto-filters to the callsign you're entering. If you'd rather keep the full
history in view, there's now a **History On/Off** toggle next to the callsign field; your choice is
remembered. The AI and DX Coach features are unaffected either way. (#40)

### Docs
Help & Guide updated to cover the DXCC prefix sort, the Log History toggle, and the QRZ QSL badges.

## 2026-07-28 — v2.12.0 "Arcturus"

**Correct time, everywhere — plus new Logbook Health tools, an NTP clock sync, and a cleaner Help & Settings.**

### Changed — QSO times are now UTC end-to-end
The recurring "off by a day" gremlins are fixed at the root. Every QSO is stored, compared,
filtered, exported and uploaded in **UTC** — the convention every logger (N1MM, N3FJP, Log4OM…)
already uses — with date and time derived from a single instant so they can't drift.

- **Re-importing an ADIF no longer duplicates evening QSOs.** The importer's duplicate check was
  missing contacts near UTC-midnight; it now matches correctly, so a re-import of the same file
  skips 100%.
- **Exports/uploads agree.** ADIF, Cabrillo, QRZ, LoTW, eQSL, Club Log and HRDLog all emit the same
  UTC date **and** time.
- This is a read/logic fix — **no re-upload to QRZ/LoTW/eQSL is triggered**, and your data isn't
  rewritten.

### New — Logbook Health (Settings → Logbook Health)
Opt-in, backup-first maintenance for your **local** log — nothing changes without your confirmation,
and it never re-uploads to or deletes from QRZ / LoTW / eQSL.

- **Verify QSO times** — finds QSOs whose date lost its time-of-day to the old bug (the time still
  lives in the ADIF `TIME_ON` field) and reconstructs just those; anything uncertain is reported,
  never guessed.
- **Find duplicates** — finds the same station on the same band at the same minute, keeps one copy
  (preferring one already synced, then the most complete), and pre-checks the rest. When copies have
  **different modes** (e.g. CW vs FT8) it flags the set and lets **you** tick which to remove — one
  is always kept.
- **Super Check Partial** (callsign-suggestion master list) now lives here too.

### New — Sync your PC clock from NTP
**Right-click the header clock** to see how far your PC is off internet time (NTP) and, on Windows,
**Sync now** to correct it (asks for administrator approval).

### Fixed — Spotting your own QSO
When no spot-capable cluster is connected, the app now says so clearly — **SpotHole is receive-only**;
add a telnet DX cluster to send spots. If you run more than one cluster, a new **Spots** toggle on each
cluster (Settings → Cluster) picks which one sends your spots.

### Improved — Help & Settings
- **Help guide search** — type a term and a dropdown lists every matching section; new **Logbook
  Health** and **Clock & Time Sync** topics, plus **WAN sharing** guidance (Tailscale / ZeroTier) for
  multi-op over the internet.
- **Settings reorganized** into a logical flow (logbook → digital → spots → hardware → network →
  appearance), with **Logbook Health** right after Web Logbooks.

## 2026-07-27 — v2.11.0 "Deneb"

**Multi-operator networked logging arrives — as an early-testing (beta) feature.**

> ⚠️ **Please read: multi-op is brand new and in an EARLY TESTING phase.** It works and is
> covered by tests, but it has **not yet been proven at scale on real multi-station
> hardware**. **Don't rely on it for a critical contest yet.** Try it, and please report
> anything that breaks on **[GitHub Issues](https://github.com/N8SDR1/SDRLoggerPlus/issues)**
> or the **Discord**. Single-station logging is unchanged and unaffected.

### New — Multi-op networked logging *(early testing / beta)*
Share **one live log across several stations** for Field Day / multi-op. Each computer keeps
running its own app and its own radio — only the **log** is shared.

- **Host a shared log** (Settings → Server → "Host this log", "Share this log on my network")
  or **connect to one** (host address + a per-device access token). Everything token-secured.
- **Live across stations** — a contact logged on any station appears on all of them, with
  **shared dupe-checking** and running score.
- **Survives outages** — a host or Wi-Fi blip **never loses a QSO** (it's queued and re-sent),
  and dupe-checking keeps working from a local cache while you're disconnected.
- **Host-allocated serial numbers** — one atomic sequence, so a fast op and a slow op never
  hand out the same number (CQ WPX / Sweepstakes).
- **Time sync** — the host is the time authority, so all stations agree even with no internet;
  a warning shows if a clock drifts.
- **Multi-op coordination panel** — a "who's on what" board with **RF-collision warnings**
  ("⚠ Watch out — N9BC is also on 20m USB"), plus **operator-to-operator chat**.
- **Live ADIF evacuation mirror** — keep a continuously-current copy of the log on a USB drive
  (Settings → Backup) — pull-and-go failover, or just storm insurance.

### Fixed
- **Custom ADIF fields preserved across the shared log.** IOTA / SOTA / contest extras
  (`AdifExtra`) now round-trip faithfully over the networked-log path.

## 2026-07-27 — v2.10.1 "Capella"

Award-accuracy and confirmation fixes (your next LoTW download may pick up
confirmations that were being dropped for years), plus QRZ auto-upload and the
microwave bands.

### New
- **Auto-upload to QRZ after logging (opt-in).** Club Log, HRDLog and eQSL already
  uploaded each QSO the moment you logged it — now QRZ can too. Turn it on in
  **Settings → QRZ** ("Auto-upload after logging"); it's **off by default**, so if you
  sync QRZ manually nothing changes.
- **Microwave bands.** The log-entry band selector now includes **13cm, 9cm, 6cm and
  3cm** (plus 1.25m / 33cm / 23cm) — so **QO-100** (Es'hail-2: 2.4 GHz up / 10 GHz down)
  and other microwave contacts can be logged. The frequency→band map matches.

### Fixed
- **FFMA now counts your live-logged QSOs.** Grids confirmed on a QSO you logged in the
  app (rather than imported) were invisible to the FFMA award — a grid whose first
  LoTW confirmation arrived on a live-logged QSO stayed "worked" forever. It now reads
  the grid the same way the rest of the app does. *(thanks @n9bc)*
- **LoTW confirmations for submode QSOs were silently dropped for years.** LoTW reports
  a submode contact (MSK144, FT4, JS8, …) under its ADIF parent (`MFSK`), so the merge
  never matched and the confirmation vanished. Now matched most-specific-first, with a
  guard so it can't confirm a *different* mode's QSO. Your next download will likely pick
  up confirmations you'd been missing. *(thanks @n9bc)*
- **Bulk delete no longer leaves awards counting deleted QSOs.** A Log History bulk
  delete now invalidates the award snapshot like every other write. *(thanks @n9bc)*

## 2026-07-27 — v2.10.0 "Capella"

Contesting from either side of the pileup, Super Check Partial call history, and a
club-callsign fix so contest logs never pollute your personal logbook — plus a round
of scoring corrections for the North-America parties.

### New
- **Super Check Partial (call history).** As you type a callsign in Log Entry, a
  **suggestions dropdown** offers matching calls from a bundled Super Check Partial master
  list, with **stations you've worked before ranked first** and a *last-worked* hint. Pick
  one and it **prefills** from your last QSO with them (name, QTH, county, grid — all still
  editable, so a rover/portable can override). One-click **Update master list** pulls the
  latest from supercheckpartial.com.
- **Contesting works from either side of the contest.** For contests with a home-area split
  (ARRL DX, CQ 160, ARRL 10 m / 160 m / RTTY Roundup), a **DX operator** is now asked for —
  and sends — the DX-side exchange (power, CQ zone, or a serial) instead of a state, and
  captures the right field from the W/VE stations they work. The side is chosen automatically
  from your station country/callsign, with an **Operating as** toggle to override it.
- **Per-session operating callsign for contests.** Start a contest under a **club, portable,
  or special-event call** and those QSOs are logged/exported under *that* call — they no
  longer land in your personal logbook or upload under your personal LoTW/QRZ identity.
- **FlexRadio is now a supported radio** (out of experimental) in the in-app rig setup —
  6000-series auto-discovered on the LAN, alongside SmartSDR.
- **Log History gains a Grid column.**

### Fixed
- **Contest scoring — North-America parties.** **NAQP** and **NA Sprint** multipliers were
  over-counted: US/Canada were double-counted (state *and* country) and non-North-American
  DX contacts wrongly earned a multiplier. They now count US states + VE provinces + NA
  countries only; a European/Asian contact scores points, not a multiplier.
- **Contest scoring — CQ WW 160.** USA and Canada were counted both as a state/province *and*
  as a DX country, inflating the multiplier total. Fixed.
- **Contest exchange wiring.** Every required sent-exchange field is now wired end-to-end and
  guarded by a test — this closes the class of bug behind the missing Field Day **Class**
  (1E / 2F), which is again captured and exported to Cabrillo.
- **ADIF import.** Band and mode are validated on import, fabricated default values are
  flagged, and an **import-issue report** is shown in the results dialog; edits to an
  imported QSO no longer vanish.
- **Backend test hang** (CI) from a spot-status cache deadlock.

## 2026-07-25 — v2.9.3 "Altair"

A GridTracker-style map view, a fix for the digital double-click-to-call, and a
one-click cure for the imported-log QRZ upload storm.

### New
- **Grid Tracker gets a real map view.** A new **Map / Chart** toggle in the Grid Tracker
  panel. **Chart** is unchanged and stays the default; **Map** paints the same
  worked/confirmed grids on a real slippy basemap (Dark, OpenStreetMap, Satellite,
  Terrain) with pan/zoom — the GridTracker-app look. Grid designators label the squares
  as you zoom in, live decodes carry over (cyan active, red-pulse "chase now") and their
  **callsigns show in the tooltip**, plus **Fit-to-worked** and a **Chase** overlay that
  outlines the unworked grids in view. Your view + basemap choice are remembered.
- **Worked-grid layer on the 2D map.** The main Map plugin can now paint your worked grids
  as a toggleable overlay with its own band filter.
- **"Mark all as already synced to QRZ."** New maintenance action in **Settings → QRZ**
  (above Test & Save) that flips every pending QSO to already-synced **without uploading
  anything** — the clean fix if an imported QRZ export left thousands of QSOs re-uploading
  as duplicates. The import "already in QRZ" checkbox now spells out exactly when to use it.

### Fixed
- **Double-click to call a station stopped working with WSJT-X / JTDX.** A v2.9.2 change
  that resolved the decode's mode code for the "worked-before" colouring also changed the
  mode sent in the reply, so WSJT-X and JTDX no longer recognised it and silently ignored
  the call. The reply now echoes the decoder's original fields verbatim; double-click
  answers a CQ again. *(Reminder: the decoder needs "Accept UDP requests" enabled.)*
- **Imported QSOs no longer trigger a duplicate-upload storm** on every sync — see the new
  QRZ maintenance action above.

## 2026-07-24 — v2.9.2 "Altair"

A new award, more flexible layouts, and a FlexRadio reconnect fix.

### New
- **FFMA award (Fred Fish Memorial Award).** Track all **488** six-metre grid squares
  of the contiguous 48 states, confirmed by LoTW or paper QSL, in **Statistics → FFMA**.
  Shown as a checklist grouped by grid field — confirmed, worked-but-unconfirmed, and
  still-needed — with a **Needed** filter so you can see exactly what's left to chase.
- **A layout for each log mode.** In **Settings → Appearance → Layout for each log mode**
  you can bind a layout (a starter or one of your own) to **General / POTA / SAT /
  Contest**. Switch to that mode and SDRLogger+ *offers* to load it — always a prompt,
  never a silent swap, and it lets you save or discard unsaved arrangement changes first.
- **Up to 10 saved layouts** (was 3) — enough to keep a personalised version of each
  starter plus a few of your own.
- **Release the radio to a hardware sat controller.** New opt-in in **Settings → S.A.T.**
  stops SDRLogger+'s own direct CAT polling while the controller has the radio, so it
  makes no traffic on a shared CI-V bus (older rigs like the IC-9100). Direct Hamlib only.

### Fixed
- **FlexRadio didn't reconnect on startup.** A Flex connected manually but never came
  back after a restart, even with "Reconnect last radio on startup" on — the Flex backend
  had no startup reconnect, and the saved rig type wasn't recorded. Both fixed; it now
  reconnects once the radio's discovery beacon arrives. *(Reported by Bill, WK2X.)*

## 2026-07-24 — v2.9.1 "Altair"

A same-day follow-up to v2.9.0: one real connection bug, a serial-port warning,
and honest corrections after a long session on the bench with an IC-705.

### Fixed
- **A failed Hamlib connect made every later attempt silently do nothing.** After
  one failed connect, clicking Connect again produced no error, no log line and no
  change for the rest of the session — it read as "Hamlib is broken" when the radio
  and port were fine. The service marked itself as owning a radio it had never
  opened; it now clears that on failure and actually retries.

### New
- **Serial-port collision warning.** Windows can hand a paired radio a COM number
  that virtual-port software (VSPE/ELTIMA, com0com) already holds, because such
  software often doesn't register with the Windows COM name arbiter — and the port
  then opens the wrong device. The port picker now flags any number claimed by more
  than one device and tells you how to fix it.

### Verified
- **IC-705 over USB confirmed** and marked as such (the first bench-verified
  Popular Radios entry). Its setup hint now reflects what the radio actually does:
  two USB ports with only the lower one answering CI-V, baud rate that can stay on
  Auto, and echo-back that can be left on.

### Corrected
- **Bluetooth CAT is documented as unconfirmed, not working.** v2.9.0 said it worked;
  a controlled test told a different story. An IC-705 pairs, Windows builds the
  outgoing port, and the port then won't open — at every baud rate, with the radio
  registered as a data device, USB unplugged, and no duplicate COM registrations,
  and the same from a bare serial open with SDRLoggerPlus not running. So the radio
  isn't accepting the serial connection; we don't know why, and the docs now say only
  that. USB is the confirmed path.

## 2026-07-24 — v2.9.0 "Altair" 🛰️

Satellite operating gets hands-free, radio setup gets a lot friendlier, and the
FlexRadio detection problem testers hit is fixed.

### New
- **S.A.T. Web panel** — dock your CSN S.A.T. controller's *own* web interface right
  inside SDRLogger+: next passes, pick a satellite to track, TLE and frequency-database
  updates, rotator and pass log. Add the panel, put your controller's address in
  **Settings → S.A.T.**, and you're looking at your real controller with your real data.
- **Follow the controller (auto-activate)** — optional, off by default. SDRLogger+ watches
  your controller and switches itself on when AOS comes inside your lead time (90 s by
  default), then off after LOS — taking the Log Entry into **SAT** mode and back to
  **General** with it, and handing rig control to the controller for the pass. Clicking
  **Activate** yourself always wins: switch it off mid-pass and it stays off. Verified on a
  live AO-07 pass.
- **Popular Radios** — pick your radio by the name on its front panel (IC-7300, IC-705,
  FT-710, TS-590SG, K4 …) and the speed, bits and PTT settings are filled in, leaving only
  the port to choose. It also tells you the one setting **on the radio** that has to
  agree — Icom's CI-V baud, Yaesu's CAT RATE — which is what most "it won't connect" cases
  turn out to be. The full searchable Hamlib list is one click away.
- **Serial ports now say what they are** — "COM3 — Silicon Labs CP210x USB to UART Bridge"
  instead of a bare "COM3", with real USB hardware sorted above virtual ports. Bluetooth
  radios (IC-705 and similar) are labelled as such: pair in Windows and pick the port,
  nothing else needed.

### Fixed
- **FlexRadio wasn't detected after the first run.** A Flex announces itself about a second
  after the backend starts — usually before the app's window has finished connecting — so
  that announcement went nowhere, and the radio was never mentioned again for the rest of
  the session. The app also had no way to ask what the backend had already found. Both
  fixed: it no longer matters whether the radio speaks up before or after the window opens.
  *(Thanks to Andrew O'Brien for the report and the "worked once, then never again" detail
  that pinned it down.)*
- **Digital decodes never showed stations as already worked.** A decode carries a
  one-character mode code (`~` for FT8, `+` for FT4) rather than a mode name, and it went
  straight into the worked-before lookup — which could never match a logged "FT8". Every
  station you'd already worked on that band and mode showed as un-worked.
- **Settings were saved over and over when a configured radio was switched off.** With the
  Settings window open and the rig powered down, the app rewrote its settings several times
  a second — visible as the Save button flickering.
- **The S.A.T. Activate button was unreliable until a restart**, and the Log Entry didn't
  always follow it. Satellite state now stays in step with the controller across restarts
  and reconnects.

### Changed
- **PTT is presented honestly.** SDRLogger+ only ever *reads* PTT, to show whether you're
  transmitting — it never keys your radio. That setting now lives under **Advanced** with
  two choices (read over CAT, or don't). DTR and RTS are gone: they gain nothing when
  nothing transmits, and they're the lines many interfaces key from.
- **The Settings → S.A.T. "Controller Tools" section is gone** — the embedded S.A.T. Web
  panel shows the same information live from the controller, so a second copy that could
  drift was worse than none.
- **Help guide updated throughout** — including a new section on running alongside
  **WSJT-X / JTDX / MSHV / VarAC** without a COM-port splitter, and a plain statement that
  radio control needs **no separate Hamlib install** (with the two exceptions: rotators,
  and Linux).

### Still experimental
- **FlexRadio 6000** remains 🧪 experimental. The detection fault above explains the mixed
  tester reports, but no one has yet confirmed a Flex surviving a close-and-reopen cycle —
  that's the case to prove. Inert unless you own a Flex.
- **Popular Radios** starting values come from each radio's documentation, not a bench.
  They're marked 🧪 in the app until an operator confirms them on the actual rig — if one
  works, or needs a change, please tell us.

## 2026-07-23 — v2.8.1 "Rigel" 📡

The stable **2.8** release. Promotes the rebuilt rig engine + satellite / flrig
refinements from the v2.8.0 pre-release to stable, and fixes three
digital-logging bugs reported by testers. **Native FlexRadio 6000 support ships as
experimental** (still being verified on hardware) — it's inactive unless you own a
Flex 6000, so it doesn't affect other rigs.

### Fixed
- **JTDX logged QSOs weren't saved.** JTDX (an older WSJT-X fork) sends a shorter
  "QSO logged" UDP packet than current WSJT-X / MSHV; the parser read past the end and
  dropped the whole message, so QSOs logged in JTDX never reached the logbook. (The
  ADIF monitor and MSHV were unaffected.) JTDX now logs correctly.
- **Imported QSOs wouldn't upload to QRZ.** ADIF-monitor and ADIF-imported contacts
  were marked "already synced to QRZ," so the uploader skipped them ("all already
  synced"). Imports are now marked **not-synced** so they upload; the "mark as already
  synced" option remains for importing your existing QRZ export.
- **Imported QSOs didn't appear until Reload.** Background ADIF-monitor imports now
  refresh the logbook, summary and grid map live, like a directly-entered QSO.

### Included from the v2.8.0 pre-release (now stable)
Unified rig engine; **experimental** native FlexRadio 6000 (SmartSDR) support;
S.A.T. controller takes the radio during a pass (rig control paused + auto-switch to
SAT mode); flrig active-rig hand-off and frequency-follow fixes.

## 2026-07-23 — v2.8.0 "Rigel" 📡 (pre-release · Flex test)

A rebuilt rig-control engine, **native FlexRadio 6000 support**, and satellite +
flrig refinements. FlexRadio is new and **not yet bench-verified against hardware** —
this build is a pre-release for Flex owners to test.

### New
- **Native FlexRadio 6000 (SmartSDR) support** — Flex radios are **auto-discovered**
  on your LAN (no host/port to enter) and appear in **Settings → Station → Radio
  Type → FlexRadio**. SDRLogger+ connects to the radio's control API **alongside
  SmartSDR** (the API is multi-client, so nothing has to close), follows the active
  slice, and tunes frequency/mode. ⚠ **Unverified on hardware** — please report issues.
- **Unified rig engine** — TCI, Hamlib, flrig and FlexRadio now run through one
  internal backend abstraction. No behaviour change to existing rigs; it makes each
  new radio a clean plug-in (this is how FlexRadio was added).
- **S.A.T. takes the radio during a pass** — while the CSN S.A.T. controller is
  actively tracking, it fully owns the connected radio: SDRLogger+ **pauses its own
  rig control** (spot-click, band/mode dropdowns), shows a **🛰 "S.A.T. controlling
  radio — rig control paused"** badge with the dropdowns greyed, and only **reads**
  freq/mode from the S.A.T. output. Control resumes automatically when the pass ends.
- **S.A.T. auto-switch** — activating the controller drops the Log Entry into **SAT**
  mode; deactivating returns it to **General**.

### Fixed
- **flrig: active rig now surfaced.** When your selected rig disconnects, SDRLogger+
  hands off to another live rig (e.g. flrig / an IC-9100) app-wide, so the status bar
  and the Log Entry follow-gate track it instead of a dead rig — Band/Mode from the
  Log Entry now drive flrig.
- **flrig: frequency follows band changes.** After the app tuned flrig (e.g. a band
  change), the displayed frequency could lag; flrig now broadcasts its new state
  immediately so Follow-Radio stays in sync.
- **"Supported TCI radio"** wording on the Meters/Panadapter no-data hints (was
  Thetis-specific; TCI works with Lyra / Thetis / ExpertSDR3 too).

## 2026-07-23 — v2.7.0 "Vega" 🛰️

Contest scoring is now exact across the entire catalog, a big Satellite-panel
overhaul, and a batch of logbook-integrity features.

### New
- **Winter Field Day scoring** — QSO points by mode, a power multiplier
  (QRP ×4 / Low ×2 / High ×1), one multiplier per mode (Phone/CW/Digital) per
  band, and a self-declared **objective-bonus** box that adds to the final score.
- **ARRL/RAC section roster** — the Multipliers panel now shows **worked / total**
  sections and lists the ones you still need; the section field flags an unknown
  section as you type.
- **Satellite panel overhaul** — following a connected CSN S.A.T. controller, the
  Log Entry auto-fills satellite, **band**, and up/downlink frequency & mode, and
  the S.A.T. panel shows the live pass in **both miles and km**: azimuth/elevation,
  range, **altitude, footprint**, Doppler-shifted up/downlink, sub-satellite point,
  and signal. It follows the live (Doppler-corrected) frequency on screen but
  **logs the nominal** transponder frequency (what LoTW expects).
- **US Counties Award (USA-CA)** — county tracking and award progress, with county
  capture from ADIF import. *(Brent, N9BC)*
- **Duplicate-QSO warning** on log entry, and a **QSL sync ledger** with credential
  redaction in logs. *(Brent, N9BC)*
- **Credential encryption at rest** + a settings migration framework and a
  frequency-repair pass for older logs. *(Brent, N9BC)*

### Changed
- Contest **Power class** now covers every ruleset that uses a power multiplier
  (Field Day, Winter Field Day, Stew Perry, …), shown in the setup picker.
- Help guide updated for the contest, section, and satellite changes.

### Fixed
- **Contest scoring is now exact across the whole catalog** (issue #23). Newly
  correct: CQ WPX low-band ×2 + North-America exception, CQ WW DX NA↔NA = 2,
  CQ WW / ARRL VHF per-band points, ARRL 160 m any-DX = 5, 10-10 member points,
  distance scoring for Stew Perry & ARRL International Digital, and ARRL 10 m
  multipliers counted once per mode. Exchanges, dupes and multipliers were already
  correct; this closes the remaining scoring gaps.

## 2026-07-19 — v2.6.0 "Vega+" 🌟

Contest release: a rule-aware contest logger for the ARRL & CQ majors, plus a
serious frequency-unit fix and several logging correctness fixes.

### New
- **Contest suite** — data-driven contest logging for the ARRL / CQ majors
  (CQ WW / WPX / 160 / RTTY, ARRL DX / Sweepstakes / 10 m / 160 m / RTTY Roundup /
  Field Day, NAQP, NA Sprint and more). **Contest Entry** window with per-contest
  and per-worked-station exchange fields, live dupe flagging, running score,
  serials, and quick-edit / delete with logbook sync; standalone **Contest Score**
  and **Multipliers** panels; a dupe/mult-coloured **Bandmap**; power-class score
  multiplier; in-session config, role override, and a resume-session guard.
  Built-ins are read-only (clone to customise).
- **Starter layouts** — a desktop **View → Layouts** menu with five ready-made
  workspaces (General, Contest, POTA, Satellite, Digital) plus save / apply / reset.
- **Rig setup moved to Settings → Station** — the standalone Rig panel is gone;
  the status-bar rig selector's "Manage radios" opens Station settings.
- **Status-bar Mute** — one button silences all spoken announcements (green
  audible / red muted) and cuts off speech mid-sentence.
- **Log Entry auto-fill** — QTH (city + state) and a new **Grid** box fill from the
  callbook lookup; WSJT-X auto-logged QSOs get callbook gap-fill (external data
  keeps precedence).
- **Combo: auto "+dB over S9"** — with the Lyra Combo link, RST-Rcvd's over-S9
  field now fills from the meter (nearest 5 dB), on top of the auto S digit.

### Changed
- Panels size to their container, not the viewport — no more bottom clipping.

### Fixed
- **QSO frequency unit (1000×).** Uploads to ClubLog / eQSL / HRDLog sent the
  frequency **1000× too high**, and several write paths stored MHz into the kHz
  field. Frequency is now canonical **kHz** end-to-end (forms still show MHz).
- **Edit-a-QSO date shift.** Editing a QSO could move it a day (local vs UTC);
  edits are now done in UTC.
- **Contest QSO export time.** Contest QSOs exported `TIME_ON=000000`; the real
  time of day is now kept.
- **Settings numeric inputs.** Typed fields committed on every keystroke with a
  fallback (typing "500" could strand "5", e.g. silently disabling RBN alerts or
  over-pruning backups). Fields now commit on blur / Enter, clamped to range.
- **Contest rule corrections** — ARRL International Digital drops RTTY (excluded
  by the rules); CQ WW VHF adds FM; Stew Perry's non-existent grid multiplier is
  removed. (Remaining band-weighted / distance scoring is tracked as a known gap.)

## 2026-07-18 — v2.5.0 "Vega+" 🌟

Fit-more-on-screen release: scale the UI to your display, plus two logging fixes.

### New
- **UI Scale** — two independent, down-only (70–100%) zoom controls for smaller /
  high-density screens. **Whole-app zoom** (desktop): the View menu or
  **Ctrl + / − / 0**, and a stepper in **Settings → Appearance → UI Scale**.
  **Per-panel scale** — a `− % +` stepper in each panel's tabset header (click the
  % to reset), saved with your layout. Map / Globe / Panadapter stay at true
  pixels. *(N9BC)*

### Changed
- The old **Compact Mode** toggles (superseded by UI Scale) are gone.
- The per-panel "open settings" affordance on a tab is now a **gear icon**. *(N9BC)*
- **2D Map** fit-fixes at small sizes; the Layers / Solar fly-outs no longer clip
  under the globe. *(N9BC)*

### Fixed
- **"Today" QSO count** stayed at 0 for QSOs logged in the evening — it compared a
  locally-stored QsoDate against a UTC "today" boundary. It now counts by your
  **local** day.
- **Lightning banner distance** showed km even with the Weather **Alert range** set
  to miles — it followed the master unit system instead of the lightning unit.
  It now matches the Alert-range unit you set.

## 2026-07-16 — v2.4.0 "Vega+" 🌟

More reach, less friction: see who's hearing *you*, filter the FT8 firehose down
to your alert rules, and reach rig setup straight from the status bar.

### New
- **"Heard Me" signal-path layers** — PSK Reporter (digital) and RBN skimmer
  (CW/RTTY) arcs from your station to every receiver that recently spotted you,
  on both the **3D globe** and the **2D map**. Band follows your rig (or pick one
  / All bands), per-layer look-back windows, clickable receiver points showing
  the RX report (freq / mode / SNR / age). *(N9BC)*
- **Rig status-bar switcher** — the bottom-right pill shows the connected radio;
  **left-click** to switch or connect any configured rig, **right-click** (or
  "Add / manage radios") to open the Rig panel for setup. *(N9BC + N8SDR)*
- **"Match Alerts" decode filter** — the Digital Decodes list can now be narrowed
  to just the decodes that match your **Digital Decode Alert** rules, so a
  "needed grids, North America" rule filters the list too, not only the alerts.

### Changed
- The **Map's Overlays** menu is now simple toggles (Lightning, Rotate Globe,
  PSK Layer, RBN Layer) with shared band/window controls; RBN cluster-feed
  settings moved into **Settings → Map**. *(N9BC)*

### Fixed
- **8-character extended grids** (e.g. `EN54xl17` from PSK Reporter) were rejected
  by the grid→lat/lon parser, silently dropping every 2D-map arc. Now accepted. *(N9BC)*

## 2026-07-16 — v2.3.0 "Vega+" 🌟

Native FT8 / digital-mode tools — decode monitoring, smart alerting, and grid
tracking, built right into SDRLoggerPlus. No JTAlert or GridTracker needed.

### New
- **Digital Decodes panel** — the live FT8/FT4 decode stream from WSJT-X / JTDX /
  MSHV over UDP, each decode coloured by what it would give you (new DXCC / band /
  zone / grid), with Needed-only and CQ-only filters. **Double-click a decode**
  and your decoder answers that CQ (WSJT-X / JTDX, with "Accept UDP requests" on).
- **Digital Decode Alerts** (Settings → Digital Decode Alerts) — geo-scoped rules
  that sound / speak / pop only for what you want: an award need (DXCC / band /
  zone / grid) × a region (continent, DXCC entity, US call area, prefix, grid
  field) × band/mode. So "needed grids, North America only, 20m" is one rule.
  Quick-add presets; voice uses the shared Voice section.
- **Grid Tracker panel** — a Maidenhead grid map: worked (green, brighter =
  confirmed), needed (red tint), and stations active right now from the decode
  stream (cyan ring; a needed + live grid pulses "chase now"). Country outlines,
  pan / zoom, hover, VUCC progress, and **Follow Digital Decodes (FDD)** to lock
  the map to your decoding session.
- **Live log-entry populate** — when your decoder's DX Call changes (you call CQ,
  or a station answers you), it fills the Log Entry, fires the callbook lookup
  (QRZ Profile), and drops the station on the map. The finished FT8 QSO auto-logs
  to Log History.
- **Compact Log History summary** — QSOs / Countries / Grids / Today on one line.

### Changed
- **Settings → WSJT-X / JTDX** is now **Decoder Link (UDP)** — it feeds both
  auto-logging and the live decode stream.

### Fixed
- A grid you just worked no longer re-appears as "needed" on the next decode —
  logging now records the grid in the needed-status cache.

## 2026-07-15 — v2.2.1

### Fixed
- **Settings wouldn't save (HTTP 400).** A non-nullable field that shipped empty
  in 2.2.0 tripped implicit validation and blocked every settings save, on every
  tab. Fixed.

## 2026-07-15 — v2.2.0 "Vega" 🌟

### New
- **DX Coach** — turns live spots + your award needs + solar / gray-line data
  into proactive, factually-grounded operating suggestions, with a needs matrix
  (DXCC / WAS / zones), band-class filters, and optional voice.
- **Confirmations** — Log4OM-style confirmation ingest: LoTW / eQSL / QRZ merge
  into your log with a QSL column (L / E / Q / C), one-click downloads, and
  background auto-sync.
- **Voice** — a shared announcement voice (accent + male / female) for band
  openings, Hot List, RBN alerts, and the DX Coach, with a volume control.

## 2026-07-10 — v2.1.0 "Vega" 🌟

Feature release on the 2.0 "Vega" line.

### New
- **Units — Imperial / Metric master toggle** (Settings → Appearance). One
  switch drives distance, satellite range/altitude, wind, temperature, and
  lightning proximity. The Weather wind switch can still override it (defaults
  to follow the master).
- **WSJT-X / JTDX** now has its own Settings section, with a **second
  independent UDP source** — auto-log from two decoders at once (e.g. WSJT-X
  and JTDX on separate ports).
- **AI talk points — bring your own provider.** Chat AI works with any
  OpenAI-compatible endpoint via a Base-URL field: OpenAI, Anthropic, **Groq**,
  **OpenRouter**, and **Ollama** (local, no key), plus Custom. Provider errors
  now show the real reason. The Help Guide includes an Ollama setup walkthrough.
- **POTA Activators filters** — the DX-cluster toolbar on the POTA panel:
  Follow-rig (Band/Mode), Band, Mode, a Region filter (park location), and
  search. Follow-rig persists across restarts.
- **TCI rig setup — connection check + edit.** Adding a TCI rig probes the
  host:port first, so a wrong port tells you immediately instead of leaving a
  rig that never connects. Saved rigs now have an Edit button (name/host/port).
- **Panadapter** — spectrum height (SPC) and spectrum line-colour controls; the
  palette and line colour live behind a colour gear.
- **ADIF Monitor** — Browse buttons to pick the watched `.adi` files.

### Improvements
- Larger, more legible band-activity tiles in the header bar.
- About page shows the real version number.
- New landscape splash screen.

### Fixes
- **Lightning proximity alert** now reads the freshest strikes (last ~0–10 min)
  instead of data up to an hour old — storm warnings reflect current conditions.

## 2026-07-08 — v2.0.1

### Fixes
- **App icon + splash screen** — replaced the leftover QSOThief-lineage
  artwork with the SDRLogger+ icon. (A dedicated wide splash graphic is
  still to come in a later release.)

## 2026-07-08 — v2.0.0 "Vega" 🌟

First production release of SDRLoggerPlus v2 (.NET 10 backend + React /
Electron). Codename **Vega** — the alpha star of the Lyra constellation,
for the Lyra ↔ SDRLogger+ integration at the heart of this release.

### Lyra Combo Link (headline)
- Two-way link over the existing TCI socket — no bridge app. Grab a call
  in Lyra's CW decoder and it populates SDRLogger+ (call + lookup);
  SDRLogger+ sends the callbook first name back so Lyra's `{NAME}` token
  fills; a `{LOG}`-tagged CW macro logs the QSO in SDRLogger+ as it sends
  the signoff.
- **Auto received-S** — the "S" of RST-Rcvd is computed live from the
  shared S-meter (SNR-gated), with an Auto/Manual toggle; works on CW,
  SSB and digital.
- Survives Lyra restarts (auto-reconnect); a `● Lyra Combo` badge shows
  when linked. Requires Lyra v0.14.0.

### Maps
- **2D Map panel restored** and reworked — a responsive globe + map
  cockpit that stays usable docked small, the focused call/spot offset
  clear of the globe, and a **radio-tower DX marker** on the 2D-map globe.
- **3D globe day/night terminator** with a Settings → Map shade slider.

### DX cluster
- **Follow rig** with independent **Band** and **Mode** toggles — e.g.
  "CW across all bands" or "CW on the current band only" (CWU/CWL fix).

### Elsewhere
- **In-app User Guide** (About → Open the User Guide) covering every
  feature, plus GitHub + Discord links.
- **"Lyra" theme** (Settings → Appearance) styled after the Lyra SDR.
- **Analog meter** shows mode + frequency on one line (white RX / red TX),
  TCI-aware.
- **Updater** matches Lyra: pre-releases visible, once-per-version, shows
  the changelog in the prompt.

## 2026-07-06 — Panadapter deep-clean session

### Fixes
- **Panadapter spectrum orientation** — un-mirror HL2 baseband so the RF
  spectrum reads correctly (USB energy right of the VFO, LSB left).
  Matches Lyra's own panadapter on the same TCI stream.
- **Panadapter waterfall dynamic range** — auto-track noise floor with
  a dB-stretch mapping so background pixels reliably sit at LUT[0]
  regardless of signal strength. Matches Lyra's `AutoDbScaler`.
- **Panadapter waterfall grid alpha** — bumped from 6% → 35% so the grid
  is visible without fighting for attention.

### Additions
- **TCI-driven filter passband overlay** — translucent green rectangle
  over the RX filter's actual Hz range, with a compact mode + width
  chip in the header (`USB 100 to +2700 Hz`, `CWU 500 Hz`). Reads
  TCI's `rx_filter_band` and `cw_pitch` — no per-mode guessing.
- **Four waterfall palettes** — Classic, Heat, Viridis, Rainbow.
  Viridis and Rainbow are byte-for-byte identical to Lyra's palettes
  so the two apps render matching waterfalls side by side.
- **Panadapter STEP picker** — mouse-wheel tuning step
  (1 Hz / 10 Hz / 100 Hz / 500 Hz / 1 kHz / 2.5 kHz / 5 kHz / 10 kHz).
  Shared with the Rig panel's VFO wheel step.
- **Waterfall floor / ceiling sliders** — dB-stretch style controls
  that shift/compress the LUT into the interesting signal window.

## 2026-07-06 — POTA + spot + cluster QoL

### Additions
- **POTA self-spot** — the "Spot Myself" button in the LogEntry POTA
  banner now POSTs to `api.pota.app/spot`. Set POTA credentials in
  Settings → Web Logbooks → POTA.
- **DX cluster outbound spot** — LogEntry's Spot button sends
  `dx FREQ CALL COMMENT` to the primary DX cluster. Only one cluster
  gets the spot at a time (clusters peer + relay upstream, so posting
  to multiple would flag your call as a duplicate source).
- **Primary spot cluster picker** — Settings → Cluster gains an
  "Outbound Spots" dropdown to choose which of your configured
  telnet clusters receives self-spots.
- **DX cluster spot cap + age filter** — Settings → Cluster gains a
  Max Spots slider (50–300) and header dropdown for the age filter
  (5 / 10 / 15 / 30 / 60 min).

### Fixes
- **TCI spot click-twice-to-set** — LogHub now sends frequency before
  mode on the TCI path, matching Lyra's expected order. First click
  lands correctly.
- **TCI CWU/CWL routing** — inbound `modulation:0,CW` now derives
  CWU / CWL from the current dial (>10 MHz = CWU, <10 MHz = CWL,
  standard ham convention).
- **HamQTH lookup stall** — dropped 15 s → 5 s timeout, added a
  negative-result cache, hard-stopped at 6 s in the LogHub chain so
  a slow HamQTH server can't hang the callsign panel.

## 2026-07-05 — eQSL + UDP ADIF + Log-entry mode arc

### Additions
- **eQSL.cc real-time upload** — every logged QSO fires an
  `ImportADIF.cfm` POST if enabled (Settings → Web Logbooks → eQSL).
  Includes a Test Credentials button.
- **Generic UDP ADIF listener** — port 52001 (v1 default) auto-imports
  ADIF records broadcast by VarAC / N1MM / Logger32 / DXKeeper. Enable
  in Settings → ADIF File Monitor.
- **Log-entry mode arc** — the LogEntry panel now has General / POTA /
  SAT tabs (matching v1 SDRLogger+).
  - **General** — mode-switcher + v1.x-style field layout.
  - **POTA** — activating-park chip, P2P park ref field,
    `my_pota_ref` / `pota_ref` tagged in AdifExtra.
  - **SAT** — bespoke fields for satellite name, uplink/downlink
    freq + mode, worked-station grid. Auto-populates from the S.A.T.
    controller when it's tracking a pass.

### Fixes
- **Log History stats** — Countries + Grids now count correctly on
  imported logbooks (was counting by DXCC id, which was null on v1
  ADIF exports; now counts distinct Country name).
- **Log History Remarks column** — the empty column between Country
  and the edit/delete buttons now shows the QSO's Remarks / Comment.
- **Band case normalization in stats** — mixed-case bands ("40m" vs
  "40M") from different ADIF sources now collapse into one bucket.
- **SAT panel v1 parity** — added missing rows (AZ / EL, Max EL,
  Range, Time to LOS / AOS in, Elapsed) and a proper Transponder
  subsection. Text brightened from washed-out dark-300/400 to
  readable dark-100/200. Pass Log always renders its header.

### Settings
- **HamQTH Test Credentials button** — parity with the QRZ tab.
- **Weather alert banner** now renders at the top of the app
  (was pinned at the bottom above the status bar).

## Session notes

Each session's work is grouped chronologically above.
For fine-grained commit history, see `git log` on the `v2-alpha` branch.

# Batch 4 — Sprint / Club / Activity Contests

Summary: 14 contests researched — **12 verified**, **2 partial** (fists-sprint, k1usn-sst).
Notes: FISTS Sprint was **discontinued for 2026** (rules reconstructed from the SM3CER
mirror of the sponsor rules). K1USN SST's primary rules page is a JavaScript SPA that
returns only navigation to fetchers, so its scoring was assembled from secondary sources.
FT Roundup is **discontinued and superseded by the FT Challenge**, but its (still-published)
rules are captured here as requested.

All NCJ NAQP variants share one rules PDF (NAQP-Rules.pdf); all NCJ Sprint CW/RTTY
variants share one rules PDF (Sprint-Rules.pdf); the SSB Sprint has its own sponsor
(ssbsprint.com).

---

## North American QSO Party, CW  [id: naqp-cw]
- wa7bnm_ref: 218
- rules_url: https://www.ncjweb.com/NAQP-Rules.pdf
- confidence: verified
- modes: [CW]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: 2nd full weekend of January (1800Z Sat – 0559Z Sun) and 1st full weekend of August; 12h window (SO/SOA may operate max 10 of 12h; M2 all 12h)
- role_distinction: yes   # North American vs non-North American (DX)
- exchange_all:      n/a
- exchange_in_area:  sent=name + (state / province / NA-country) ; rcvd=name + (state / province / NA-country)     # North American stations
- exchange_out_area: sent=name only ; rcvd=name only                                                              # non-NA (DX) stations
- works_for_points_in_area:  everyone (NA and DX)
- works_for_points_out_area: North American stations only   # a valid QSO is between a NA station and any other station
- points: 1 point per valid QSO; no per-mode/continent/country/zone modifiers
- multipliers_in_area:  50 US states + DC + 13 VE provinces/territories + other NA DXCC entities; count once per band. Non-NA countries / MM / AM are NOT multipliers (worked for QSO credit only)
- multipliers_out_area: same set, per band (DX stations also accrue NA multipliers per band)
- dupe: per_band
- serial: none
- cabrillo_name: NAQP-CW
- notes: 100W max (Low Power); QRP ≤5W recognized; >100W = check log. Single operator name required throughout (incl. multiop). North American = ARRL DXCC List + Hawaii. Scoring = total valid QSOs × sum of multipliers on each band.

## North American QSO Party, SSB  [id: naqp-ssb]
- wa7bnm_ref: 230   # unconfirmed (matched via search, not the detail page itself)
- rules_url: https://www.ncjweb.com/NAQP-Rules.pdf
- confidence: verified
- modes: [SSB]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: 3rd full weekend of January and 3rd full weekend of August; 1800Z Sat – 0559Z Sun; 12h window (SO/SOA max 10h)
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=name + (state / province / NA-country) ; rcvd=name + (state / province / NA-country)
- exchange_out_area: sent=name only ; rcvd=name only
- works_for_points_in_area:  everyone (NA and DX)
- works_for_points_out_area: North American stations only
- points: 1 point per valid QSO
- multipliers_in_area:  50 states + DC + 13 VE provinces + other NA DXCC; once per band. Non-NA not a mult.
- multipliers_out_area: same set, per band
- dupe: per_band
- serial: none
- cabrillo_name: NAQP-SSB
- notes: Same rules doc/structure as the CW event. 100W Low-Power max; QRP ≤5W recognized.

## North American QSO Party, RTTY  [id: naqp-rtty]
- wa7bnm_ref: 263
- rules_url: https://www.ncjweb.com/NAQP-Rules.pdf
- confidence: verified
- modes: [RTTY]
- bands: [80, 40, 20, 15, 10]                # NO 160m for the RTTY event
- warc_excluded: yes
- period: RTTY runs the last Saturday of February (1800Z Sat – 0559Z Sun) and the 3rd full weekend of July; 12h window (SO/SOA max 10h)
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=name + (state / province / NA-country) ; rcvd=name + (state / province / NA-country)
- exchange_out_area: sent=name only ; rcvd=name only
- works_for_points_in_area:  everyone (NA and DX)
- works_for_points_out_area: North American stations only
- points: 1 point per valid QSO
- multipliers_in_area:  50 states + DC + 13 VE provinces + other NA DXCC; once per band. Non-NA not a mult.
- multipliers_out_area: same set, per band
- dupe: per_band
- serial: none
- cabrillo_name: NAQP-RTTY
- notes: KEY DIFFERENCE from CW/SSB — **no 160m**. 100W Low-Power max; QRP ≤5W recognized.

---

## North American Sprint, CW  [id: na-sprint-cw]
- wa7bnm_ref: 253
- rules_url: https://www.ncjweb.com/Sprint-Rules.pdf
- confidence: verified
- modes: [CW]
- bands: [80, 40, 20]
- warc_excluded: yes   # only 80/40/20 permitted
- period: two 4-hour Sprints per year (Feb and Sep), 0000–0359 UTC
- role_distinction: yes   # exchange identical, but who you may work for points differs
- exchange_all:      n/a
- exchange_in_area:  sent=[his call, my call] + serial + name + (state / province / NA-country) ; rcvd=same     # North American station
- exchange_out_area: sent=[his call, my call] + serial + name + "DX" ; rcvd=same                                # non-NA station sends "DX" for location
- works_for_points_in_area:  everyone
- works_for_points_out_area: North American stations only
- points: 1 point per valid QSO
- multipliers_in_area:  US states + DC + 13 VE provinces/territories + other NA countries; **all-band (counted once total, NOT per band)**. KH6 counts as a state. Non-NA countries are not multipliers (QSO credit only).
- multipliers_out_area: same all-band set
- dupe: per_band
- serial: all_band   # sequential, begins at 1, continuous across all bands
- cabrillo_name: NA-SPRINT-CW
- notes: **QSY rule** — after soliciting a call (CQ/QRZ/etc.) you may work only ONE station, then must move ≥1 kHz before calling another station or ≥5 kHz before soliciting again; may not make another contact on the vacated frequency until you complete one elsewhere. Single-op only, **unassisted** (no spotting). Power: HP ≤1500W / LP ≤100W / QRP ≤5W. NA defined by CQWW rules + KH6 (also FP, 4U1UN, VP9, OX are NA).

## North American Sprint, SSB  [id: na-sprint-ssb]
- wa7bnm_ref: unknown   # listed on WA7BNM as "North American SSB Sprint"; detail-page ref not confirmed
- rules_url: https://ssbsprint.com/rules/
- confidence: verified
- modes: [SSB]
- bands: [80, 40, 20]
- warc_excluded: yes
- period: two 4-hour Sprints per year (spring and fall; e.g. fall = Nov 1, 0000–0359 UTC)
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=[his call, my call] + serial + name + (state / province / NA-country) ; rcvd=same
- exchange_out_area: sent=[his call, my call] + serial + name + "DX" ; rcvd=same
- works_for_points_in_area:  everyone
- works_for_points_out_area: North American stations only
- points: 1 point per valid QSO
- multipliers_in_area:  US states + DC + 13 VE provinces + other NA countries; **all-band once**. KH6 = state. Non-NA not a mult.
- multipliers_out_area: same all-band set
- dupe: per_band
- serial: all_band
- cabrillo_name: NA-SPRINT-SSB
- notes: Separate sponsor (ssbsprint.com), same format as the NCJ CW/RTTY Sprints. QSY rule identical (work 1, then ≥1 kHz / ≥5 kHz). Power HP ≤1500 / LP ≤100 / QRP ≤5. Single-op only.

## North American Sprint, RTTY  [id: na-sprint-rtty]
- wa7bnm_ref: 155
- rules_url: https://www.ncjweb.com/Sprint-Rules.pdf
- confidence: verified
- modes: [RTTY]
- bands: [80, 40, 20]
- warc_excluded: yes
- period: two 4-hour Sprints per year (Mar and Sep), 0000–0359 UTC
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=[his call, my call] + serial + name + (state / province / NA-country) ; rcvd=same
- exchange_out_area: sent=[his call, my call] + serial + name + "DX" ; rcvd=same
- works_for_points_in_area:  everyone
- works_for_points_out_area: North American stations only
- points: 1 point per valid QSO
- multipliers_in_area:  US states + DC + 13 VE provinces + other NA countries; **all-band once**. KH6 = state. Non-NA not a mult.
- multipliers_out_area: same all-band set
- dupe: per_band
- serial: all_band
- cabrillo_name: NA-SPRINT-RTTY
- notes: Same rules PDF as CW Sprint. QSY rule applies. Power HP ≤1500 / LP ≤100 / QRP ≤5. Single-op unassisted.

---

## CWops Test (CWT)  [id: cwops-cwt]
- wa7bnm_ref: 498
- rules_url: https://cwops.org/cwops-tests/
- confidence: verified
- modes: [CW]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: weekly, 4 sessions of 60 min each — Wed 1300–1400Z and 1900–2000Z; Thu 0300–0400Z and 0700–0800Z
- role_distinction: no   # distinction is membership, not geography
- exchange_all:      sent=name + CWops member number (members) OR name + (state/province/DX-country) (non-members) ; rcvd=same
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone
- works_for_points_out_area: n/a
- points: 1 point per QSO (work each station once per band)
- multipliers_in_area:  number of unique call signs worked — counted **once per session regardless of band (all-band unique)**. Same call on another band = extra QSO point but no extra mult.
- multipliers_out_area: n/a
- dupe: per_band
- serial: none
- cabrillo_name: CWT
- notes: Score = total QSOs × unique call signs. Power QRP ≤5W / LP ≤100W / HP >100W. CW Academy participants may send "CWA" in place of a member number. Results uploaded to 3830scores.com.

## CWops CW Open  [id: cw-open]
- wa7bnm_ref: 532
- rules_url: https://cwops.org/cwops-tests/cw-open/
- confidence: verified
- modes: [CW]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: one day in early September (e.g. Sep 5, 2026); **three separate 4h sessions**: 0000–0359Z, 1200–1559Z, 2000–2359Z
- role_distinction: no
- exchange_all:      sent=serial number + name ; rcvd=serial number + name   # serial restarts at 001 each session
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone
- works_for_points_out_area: n/a
- points: 1 point per valid QSO; work each station once per band per session
- multipliers_in_area:  each unique call sign counts once per session (all-band within a session); same call on another band = QSO points but no extra mult
- multipliers_out_area: n/a
- dupe: per_band   # within a session
- serial: all_band   # running sequential number, restarts at 001 each session
- cabrillo_name: CW-OPEN
- notes: The three sessions are scored/logged **separately** (one Cabrillo log per session). Power QRP ≤5W / LP ≤100W / HP >100W.

## K1USN Slow Speed Test (SST)  [id: k1usn-sst]
- wa7bnm_ref: 681
- rules_url: http://www.k1usn.com/sst_rules.html
- confidence: partial   # primary rules page is a JS SPA (returns nav only to fetchers); scoring reconstructed from NCJ/ARRL-NEDIV/N1MM/3830 secondary sources
- modes: [CW]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: weekly, 60 min — Friday 2000–2100Z and Monday 0000–0100Z (Sunday evening in North America)
- role_distinction: no   # no membership number in this event
- exchange_all:      sent=name + (state / province) ; non-US/VE send name + "DX" ; rcvd=same
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone
- works_for_points_out_area: n/a
- points: 1 point per QSO (work each station once per band)
- multipliers_in_area:  SPC = US states + VE provinces + DX countries; per band (sum of multipliers on each band)   # per-band vs once is the main partial-confidence item
- multipliers_out_area: n/a
- dupe: per_band
- serial: none
- cabrillo_name: K1USN-SST
- notes: Max speed 20 wpm (participants urged to run ≤12 wpm). The separate annual **"SST Open"** adds a CW-speed multiplier and per-band K1USN bonus points; the regular weekly SST scoring is QSOs × SPC. 3830 power categories QRP/LP/HP. No membership number exchanged (SST has no membership). Scoring not verified against the primary page.

## ICWC Medium Speed Test (MST)  [id: icwc-mst]
- wa7bnm_ref: 720
- rules_url: https://internationalcwcouncil.org/mst-contest/
- confidence: verified
- modes: [CW]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: weekly, 60 min — Monday 1300–1400Z and 1900–2000Z; Tuesday 0300–0400Z
- role_distinction: no   # no membership distinction — everyone sends the same
- exchange_all:      sent=name + QSO serial number ; rcvd=name + QSO serial number
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone
- works_for_points_out_area: n/a
- points: 1 point per QSO (work each station once per band)
- multipliers_in_area:  number of unique call signs worked (all-band unique, per session)
- multipliers_out_area: n/a
- dupe: per_band
- serial: all_band   # running QSO serial number (part of the exchange)
- cabrillo_name: ICWC-MST
- notes: Target speed 20–25 wpm (reduce on request). Score = total QSOs × unique call signs (CWT-style). Power QRP ≤5W / LP 6–100W / HP >100W. Unlike CWT/SST, the exchange carries a **serial number**, not a member number or S/P.

---

## FISTS Sprint  [id: fists-sprint]
- wa7bnm_ref: unknown
- rules_url: https://www.fistsna.org/operating.php  (rules mirror: http://www.sk3bg.se/contest/fistsspr.htm)
- confidence: partial   # FISTS North America announced the Sprints will NOT continue in 2026 (lack of participation); rules reconstructed from the SM3CER mirror
- modes: [CW]
- bands: [80, 40, 20, 15, 10]   # 3.5 / 7 / 14 / 21 / 28 MHz
- warc_excluded: yes
- period: (as last run) four 4h Sprints/year — Winter (Feb), Spring (May), Summer (Jul), Fall (Oct); 1700–2100Z on the 2nd Saturday
- role_distinction: no   # distinction is FISTS member vs non-member (points + exchange), not geography
- exchange_all:      sent=RST + name + (US state / VE province / DXCC country) + FISTS number (members) OR power out (non-members) ; rcvd=same
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone
- works_for_points_out_area: n/a
- points: 5 points per QSO with a FISTS member; 2 points per QSO with a non-member
- multipliers_in_area:  each US state + each VE province + each DXCC country = 1 multiplier, counted once (all-band, per contest)
- multipliers_out_area: n/a
- dupe: per_band
- serial: none
- cabrillo_name: FISTS-SPRINT   # unverified
- notes: **DISCONTINUED for 2026.** Power categories QRO (>5W) / QRP (≤5W) / Club. Score = QSO points × multipliers. One source (fistsna search summary) mentioned a +250 bonus for logs with ≥25 QSOs and "all bands"; the SM3CER mirror shows no bonus and a 5-band HF list — treat the bonus as unverified / possibly a different FISTS activity.

## QRP ARCI QSO Party (Spring/Fall)  [id: qrp-arci]
- wa7bnm_ref: unknown
- rules_url: https://www.qrparci.org/contest/spring-qso-party  (Fall: https://qrparci.org/fall-qso-party/)
- confidence: verified
- modes: [CW]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: Spring QSO Party ~April, 0000–0600Z (6h); Fall QSO Party ~October (same format)
- role_distinction: no   # member vs non-member, and continent, affect points
- exchange_all:      sent=RST + (state/province/country) + ARCI member number (members) OR RST + (state/province/country) + power out (non-members) ; rcvd=same
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone
- works_for_points_out_area: n/a
- points: member = 5; non-member, different continent = 4; non-member, same continent = 2
- multipliers_in_area:  SPC (state/province/country) total for all bands (all-band once). PLUS a power multiplier applied to the whole score.
- multipliers_out_area: n/a
- dupe: per_band
- serial: none
- cabrillo_name: QRP-ARCI   # unverified
- notes: **Final Score = QSO points (all bands) × SPCs (all bands) × power multiplier.** Power multiplier: >5W ×1; >1–5W ×7; >250mW–1W ×10; >55–250mW ×15; ≤55mW ×20. QRP-focused; same station may be worked on multiple bands.

## FT Roundup  [id: ft-roundup]
- wa7bnm_ref: unknown
- rules_url: https://www.rttycontesting.com/ft-roundup/rules/
- confidence: verified   # rules read in full, but the event is discontinued
- modes: [FT8, FT4]   # digital only
- bands: [80, 40, 20, 15, 10]
- warc_excluded: yes
- period: first full weekend of December; 1800Z Sat – 2359Z Sun (30h window, only first 24h scored)
- role_distinction: yes   # W/VE send location, DX send serial
- exchange_all:      n/a
- exchange_in_area:  sent=RST + (state / VE province) ; rcvd=same          # W/VE stations
- exchange_out_area: sent=RST + serial number (starting 001) ; rcvd=same   # DX stations
- works_for_points_in_area:  everyone
- works_for_points_out_area: everyone
- points: 1 point per QSO
- multipliers_in_area:  US states + DC + VE provinces/territories + DXCC countries; **counted once, NOT per band** (all-band)
- multipliers_out_area: same all-band set
- dupe: per_band   # once per band regardless of FT4/FT8 mode
- serial: none for W/VE; DX stations send a running serial in the exchange
- cabrillo_name: FT-ROUNDUP
- notes: **DISCONTINUED — superseded by the FT Challenge** (https://www.rttycontesting.com/ft-challenge/). Sponsor is rttycontesting.com (NOT RTTYops / Helvetia). Power: LP ≤100W / QRP ≤5W. Modes limited to FT4/FT8; work a station once per band across both modes.

## 10-10 International QSO Party / Contest  [id: ten-ten]
- wa7bnm_ref: 81
- rules_url: https://www.ten-ten.org/qso-party-rules/
- confidence: verified   # phone QSO parties confirmed; CW/Digital variant dates less certain
- modes: [SSB, AM, FM] for Winter/Summer Phone parties; [CW] and [DIGITAL] for the Spring/Fall events; Sprint (Oct 10) allows all modes except weak-signal
- bands: [10]   # 28 MHz only
- warc_excluded: n/a   # single-band 10m contest
- period: Winter Phone = 1st full weekend of February (0001Z Sat – 2359Z Sun); Summer Phone = 1st full weekend of August; Spring/Fall CW-Digital run in spring/fall; Sprint = Oct 10, 0001–2359Z (24h)
- role_distinction: no
- exchange_all:      sent=call sign + name + 10-10 member number (or 0 if non-member) + QTH (state/province/country) ; rcvd=same
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone
- works_for_points_out_area: n/a
- points: 2 points per QSO with a 10-10 member; 1 point per QSO with a non-member
- multipliers_in_area:  none — final score is the sum of QSO points (there is no state/country multiplier; some events grant awards, not score, for working all 10 US call districts)
- multipliers_out_area: n/a
- dupe: per_contest   # single band; each station worked once per event
- serial: none
- cabrillo_name: unknown   # 10-10 typically accepts its own/CSV log formats, Cabrillo not required
- notes: 10m only. Member number is exchanged (0 = non-member). Winter & Summer = Phone; Spring & Fall = CW/Digital; the Oct 10 Sprint is all-mode (except weak-signal/FT). "Direct unassisted contacts only." Logs due 8 days after the event.

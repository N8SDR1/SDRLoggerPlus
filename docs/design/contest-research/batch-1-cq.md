# Batch 1 — CQ Contests

Summary: 8 contests researched — **8 verified**, 0 partial. All fields drawn from the sponsors' official rules pages (cqww.com, cqwpx.com, cq160.com, cqww-vhf.com, cqwwrtty.com). Cabrillo CONTEST names confirmed against each sponsor's cabrillo.htm / WA7BNM cabnames list.

---

## CQ WW DX Contest, CW  [id: cq-ww-cw]
- wa7bnm_ref: 192
- rules_url: https://www.cqww.com/rules.htm
- confidence: verified
- modes: [CW]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: Last full weekend of November, 48h (00:00:00 UTC Sat – 23:59:59 UTC Sun)
- role_distinction: no
- exchange_all:      sent=RST+CQzone ; rcvd=RST+CQzone
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone (any station may work any station)
- works_for_points_out_area: n/a
- points: Different continents = 3 pts. Same continent, different country = 1 pt. EXCEPTION: contacts between different countries within North America = 2 pts. Same country = 0 pts (still counts for zone + country multiplier). Points do NOT vary by band.
- multipliers_in_area:  CQ zones (1 per zone per band) + countries/DXCC entities incl. WAE country list (1 per country per band). PER-BAND.
- multipliers_out_area: n/a
- dupe: per_band
- serial: none
- cabrillo_name: CQ-WW-CW
- notes: The "same country / same continent / different continent" split is a POINTS-only role-like distinction; the exchange itself is identical for all stations (RST + CQ zone). Zone and country are counted separately per band, so a station can be both a new zone and a new country. WW uses WAE country list additions (e.g. separate multipliers beyond strict DXCC).

## CQ WW DX Contest, SSB  [id: cq-ww-ssb]
- wa7bnm_ref: 172
- rules_url: https://www.cqww.com/rules.htm
- confidence: verified
- modes: [SSB]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: Last full weekend of October, 48h (00:00:00 UTC Sat – 23:59:59 UTC Sun)
- role_distinction: no
- exchange_all:      sent=RS+CQzone ; rcvd=RS+CQzone
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone (any station may work any station)
- works_for_points_out_area: n/a
- points: Different continents = 3 pts. Same continent, different country = 1 pt. EXCEPTION: different countries within North America = 2 pts. Same country = 0 pts (still counts for zone + country multiplier). Points do NOT vary by band.
- multipliers_in_area:  CQ zones (1 per zone per band) + countries/DXCC entities incl. WAE list (1 per country per band). PER-BAND.
- multipliers_out_area: n/a
- dupe: per_band
- serial: none
- cabrillo_name: CQ-WW-SSB
- notes: Identical rules to the CW weekend except mode/dates. Exchange is RS + CQ zone (report is RS not RST for phone). Same-country/continent/continent split is points-only, not an exchange difference.

## CQ WW WPX Contest, CW  [id: cq-wpx-cw]
- wa7bnm_ref: none (found via contestcalendar.com; sponsor cqwpx.com)
- rules_url: http://cqwpx.com/rules/
- confidence: verified
- modes: [CW]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: Last full weekend of May, 48h (SO may operate 36 of 48h, min 60-min off periods)
- role_distinction: no
- exchange_all:      sent=RST+serial ; rcvd=RST+serial
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone (any station may work any station; own-country contacts allowed for points)
- works_for_points_out_area: n/a
- points: BY BAND. Different continents: 3 pts (28/21/14 MHz), 6 pts (7/3.5/1.8 MHz). Same continent, different country: 1 pt (28/21/14 MHz), 2 pts (7/3.5/1.8 MHz); EXCEPTION intra-North America: 2 pts (28/21/14 MHz), 4 pts (7/3.5/1.8 MHz). Same country: 1 pt on ALL bands.
- multipliers_in_area:  Prefixes (WPX prefix, e.g. K3, W3, VE7, DL8). Counted ONCE per contest regardless of band or how many times worked (NOT per-band).
- multipliers_out_area: n/a
- dupe: per_band
- serial: all_band (single ops use one continuous serial sequence across all bands; Multi-Two/Multi-Unlimited/Multi-Distributed use a separate sequence per band)
- cabrillo_name: CQ-WPX-CW
- notes: Key differences vs CQ WW: (1) multiplier is unique PREFIXES counted once for the whole contest, not per band; (2) QSO points vary by band (low bands worth double); (3) same-country QSOs are worth 1 pt (not 0); (4) exchange is a serial number, not a zone.

## CQ WW WPX Contest, SSB  [id: cq-wpx-ssb]
- wa7bnm_ref: 291
- rules_url: http://cqwpx.com/rules/
- confidence: verified
- modes: [SSB]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: Last full weekend of March, 48h (SO may operate 36 of 48h, min 60-min off periods)
- role_distinction: no
- exchange_all:      sent=RS+serial ; rcvd=RS+serial
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone (any station may work any station; own-country contacts allowed for points)
- works_for_points_out_area: n/a
- points: BY BAND. Different continents: 3 pts (28/21/14 MHz), 6 pts (7/3.5/1.8 MHz). Same continent, different country: 1 pt (28/21/14 MHz), 2 pts (7/3.5/1.8 MHz); EXCEPTION intra-North America: 2 pts (28/21/14 MHz), 4 pts (7/3.5/1.8 MHz). Same country: 1 pt on ALL bands.
- multipliers_in_area:  Prefixes (WPX prefix). Counted ONCE per contest, NOT per-band.
- multipliers_out_area: n/a
- dupe: per_band
- serial: all_band (single ops one sequence across bands; Multi-Two/Unlimited/Distributed separate per band)
- cabrillo_name: CQ-WPX-SSB
- notes: Same rules as CW WPX weekend, mode/dates differ; report is RS + serial for phone.

## CQ 160-Meter Contest, CW  [id: cq-160-cw]
- wa7bnm_ref: 232
- rules_url: http://cq160.com/rules/index.htm
- confidence: verified
- modes: [CW]
- bands: [160]
- warc_excluded: n/a (single-band 160m contest)
- period: Last full weekend of January, 2200Z Fri – 2200Z Sun (48h window; operating-time limits apply to some categories)
- role_distinction: yes (exchange differs by station location)
- exchange_all:      n/a
- exchange_in_area:  sent=RST+(US state / Canadian province) ; rcvd=RST+(state/province)   # W/VE stations
- exchange_out_area: sent=RST+CQzone ; rcvd=RST+CQzone   # DX stations (zone is a location indicator only, does NOT count as a multiplier)
- works_for_points_in_area:  everyone (all stations work all stations)
- works_for_points_out_area: everyone (all stations work all stations)
- points: Own country = 2 pts. Same continent, different country = 5 pts. Different continents = 10 pts. Maritime mobile = 5 pts (no multiplier value). Single band, so no per-band point variation.
- multipliers_in_area:  US states (48 contiguous + DC = 49) + Canadian provinces/areas (14) + DXCC countries (incl. WAE additions). Single band so effectively per-contest. Same multiplier set counts for W/VE and DX stations alike.
- multipliers_out_area: (same set as above — multiplier treatment does NOT differ by role)
- dupe: per_band (single band → per contest)
- serial: none
- cabrillo_name: CQ-160-CW
- notes: Role distinction is in the EXCHANGE only (W sends state, VE sends province, DX sends CQ zone) — CQ zone from DX is informational and is NOT a multiplier. Multipliers are the SAME set (states + provinces + countries) for everyone. KL7 (Alaska) and KH6 (Hawaii) count as DXCC countries, not states.

## CQ 160-Meter Contest, SSB  [id: cq-160-ssb]
- wa7bnm_ref: 259
- rules_url: http://cq160.com/rules/index.htm
- confidence: verified
- modes: [SSB]
- bands: [160]
- warc_excluded: n/a (single-band 160m contest)
- period: Last full weekend of February, 2200Z Fri – 2200Z Sun (48h window; operating-time limits apply to some categories)
- role_distinction: yes (exchange differs by station location)
- exchange_all:      n/a
- exchange_in_area:  sent=RS+(US state / Canadian province) ; rcvd=RS+(state/province)   # W/VE stations
- exchange_out_area: sent=RS+CQzone ; rcvd=RS+CQzone   # DX stations (zone is a location indicator only, not a multiplier)
- works_for_points_in_area:  everyone (all stations work all stations)
- works_for_points_out_area: everyone (all stations work all stations)
- points: Own country = 2 pts. Same continent, different country = 5 pts. Different continents = 10 pts. Maritime mobile = 5 pts (no multiplier value).
- multipliers_in_area:  US states (48 + DC = 49) + Canadian provinces/areas (14) + DXCC countries. Same set for all stations.
- multipliers_out_area: (same set — no role difference)
- dupe: per_band (single band → per contest)
- serial: none
- cabrillo_name: CQ-160-SSB
- notes: Identical structure to the CW weekend; report is RS (phone). Same exchange role split (state/province vs CQ zone).

## CQ Worldwide VHF Contest  [id: cq-vhf]
- wa7bnm_ref: 73
- rules_url: http://cqww-vhf.com/rules/
- confidence: verified
- modes: [SSB, CW, FM]  (analog weekend)  |  [FT4, FT8, MSK144, Q65, digital]  (separate digital weekend)
- bands: [6, 2]   (50 MHz and 144 MHz only)
- warc_excluded: n/a (VHF contest — HF/WARC bands not used)
- period: Analog (SSB/CW/FM): 3rd full weekend of July, 1400Z Sat – 1400Z Sun (24h). Digital (FT4/FT8/MSK144/Q65): separate weekend later in July, 1400Z Sat – 1400Z Sun (24h). (2026: analog Jul 4–5, digital Jul 18–19.)
- role_distinction: no
- exchange_all:      sent=4-char Maidenhead grid (e.g. EM15) ; rcvd=4-char Maidenhead grid
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone
- works_for_points_out_area: n/a
- points: 6m (50 MHz) = 1 pt per QSO. 2m (144 MHz) = 2 pts per QSO.
- multipliers_in_area:  Number of different 4-character grid locators worked, counted PER BAND. (Rovers may re-count the same grid if they relocate ≥100 m.)
- multipliers_out_area: n/a
- dupe: per_band (work each station once per band)
- serial: none
- cabrillo_name: CQ-VHF-SSBCW (analog) | CQ-VHF-DIGI (digital)
- notes: No signal report exchanged — only callsign + grid. Two separate weekends: analog (SSB/CW/FM) and digital (FT4/FT8/MSK144/Q65) — each has its own Cabrillo CONTEST name. Score = total QSO points × total grid multipliers (summed across both bands). If the catalog treats cq-vhf as one entry, model the two weekends as mode-set variants sharing points/multiplier structure.

## CQ Worldwide DX Contest, RTTY  [id: cq-ww-rtty]
- wa7bnm_ref: 130
- rules_url: https://www.cqwwrtty.com/rules.htm
- confidence: verified
- modes: [RTTY]   (45.45 baud, 170 Hz shift, ITA2/Baudot)
- bands: [80, 40, 20, 15, 10]
- warc_excluded: yes (and 160m is NOT used — 5 bands only, 3.5–28 MHz)
- period: Last full weekend of September, 48h (00:00:00 UTC Sat – 23:59:59 UTC Sun)
- role_distinction: yes (W/VE stations add a state / VE-area field to the exchange)
- exchange_all:      n/a
- exchange_in_area:  sent=RST+CQzone+(US state / Canadian call-area) ; rcvd=RST+CQzone+(state/area)   # W/VE stations, e.g. "599 05 MA"
- exchange_out_area: sent=RST+CQzone ; rcvd=RST+CQzone   # DX (non-W/VE) stations
- works_for_points_in_area:  everyone
- works_for_points_out_area: everyone
- points: Same country = 1 pt. Same continent, different country = 2 pts. Different continents = 3 pts. NO North-America exception (unlike CQ WW CW/SSB — NA gets no special differential). Points do NOT vary by band.
- multipliers_in_area:  Three multiplier types, ALL counted PER BAND: (1) CQ zones; (2) DXCC countries (incl. WAE list); (3) US states (48 continental + DC) and Canadian call areas (14: VE1–VE9, VO1–VO2, VY0–VY2). Same set counts for everyone.
- multipliers_out_area: (same three-type set — treatment does not differ by role)
- dupe: per_band
- serial: none
- cabrillo_name: CQ-WW-RTTY
- notes: RTTY-specific extra multiplier: US states + Canadian areas count PER BAND, in addition to zones and countries. Alaska (KL7) and Hawaii (KH6) count as COUNTRIES, not states. Exchange role split: W/VE stations append their state/VE-area; DX stations send only RST + zone. Points structure is the plain 1/2/3 (no NA=2 exception that CQ WW CW/SSB has).

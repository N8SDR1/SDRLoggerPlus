# Batch 5 — US State QSO Parties (A–K), part a

Summary: 13 contests researched — **11 verified**, **2 partial** (Delaware and Indiana:
official sponsor rules pages were unreachable during research, so their blocks are
sourced from the WA7BNM detail page plus secondary summaries and are flagged).

All 13 are QSO parties, so every one has `role_distinction: yes` (in-area vs
out-of-area exchange, multipliers, and who-works-whom differ).

---

## Alabama QSO Party  [id: qp-alabama]
- wa7bnm_ref: 134
- rules_url: http://alabamacontestgroup.org/aqp/rules/
- confidence: verified
- modes: [CW, PHONE]        # digital/FT8/RTTY NOT permitted; entrants pick CW-only, Phone-only, or Mixed
- bands: [80, 40, 20, 15, 10]
- warc_excluded: yes        # "No WARC bands"; no 160m, no 6m/VHF
- period: 4th Saturday of July, 1500Z–0300Z (12h); 2026 = Jul 25 1500Z–Jul 26 0300Z
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RS(T)+AL county abbrev ; rcvd=RS(T)+(AL county | state/prov | DX country prefix)
- exchange_out_area: sent=RS(T)+(state/prov for W/VE incl KH6/KL7 | country prefix for DX) ; rcvd=RS(T)+AL county
- works_for_points_in_area:  everyone (worldwide)
- works_for_points_out_area: AL stations only
- points: 2 points per QSO for BOTH CW and phone (mode-independent)
- multipliers_in_area:  US states (≤50) + VE provinces (13) + AL counties (67) + DXCC entities; counted once per mode
- multipliers_out_area: AL counties (max 67); counted once per mode
- dupe: per_band_mode   # not stated verbatim in rules; standard once/band/mode assumed
- serial: none
- cabrillo_name: AL-QSO-PARTY
- notes: Exchange uses RST + county/QTH, no serial, no name. Multipliers are per-mode (work a mult once on CW and again on Phone). Power categories: High/Low/QRP; also Mobile/Rover/Portable. 6m/VHF and 160m not used.

## Arkansas QSO Party  [id: qp-arkansas]
- wa7bnm_ref: 132
- rules_url: https://arkqp.com/arkansas-qso-party-rules/
- confidence: verified
- modes: [CW, PHONE, DIGITAL]   # "any digital mode is acceptable if the proper exchange can be accomplished" — FT8/FT4 effectively excluded (can't send free-form county), but not named as banned
- bands: [160, 80, 40, 20, 15, 10, 6, 2]
- warc_excluded: yes        # "160, 80, 40, 20, 15, 10, 6, and 2 meter bands only"
- period: 3rd Saturday of May, 1400Z–0200Z (12h); 2026 = May 16 1400Z–May 17 0200Z
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+AR county (3-letter) ; rcvd=RS(T)+(AR county | state/prov | DX)
- exchange_out_area: sent=RS(T)+(US state | VE province | "DX") ; rcvd=RS(T)+AR county
- works_for_points_in_area:  everyone
- works_for_points_out_area: AR stations only
- points: 1 point/QSO any band any mode for fixed stations; 2 points/QSO any band any mode for Mobile/Portable/Rover (points are by STATION CATEGORY, not by mode)
- multipliers_in_area:  AR counties (75) + US states except AR (49) + VE provinces (≤13) + DX (≤1)
- multipliers_out_area: AR counties (max 75)
- dupe: per_band_mode   # "once per band and mode (and per each AR county for mobile/portable/rover)"
- serial: none
- cabrillo_name: AR-QSO-PARTY
- notes: Note the unusual scoring — points depend on the SENDER's category (mobile/portable/rover = 2 pt), not on mode. Exchange is RST + county/state, no serial or name. Digital allowed generically but FT8/FT4 impractical for the required county/state exchange.

## California QSO Party (CQP)  [id: qp-california]
- wa7bnm_ref: 140
- rules_url: https://www.cqp.org/Rules.html
- confidence: verified
- modes: [CW, PHONE]        # no digital/FT8/RTTY
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes        # no WARC, no VHF/6m
- period: 1st full weekend of October, Sat 1600Z – Sun 2200Z (30h span, operate all); 2026 = Oct 3 1600Z–Oct 4 2200Z
- role_distinction: yes
- exchange_in_area:  sent=serial#+CA county (4-letter abbrev) ; rcvd=serial#+(CA county | state/VE prov | "DX")
- exchange_out_area: sent=serial#+(US state | VE province/territory | "DX") ; rcvd=serial#+CA county
- works_for_points_in_area:  everyone (CA works everyone)
- works_for_points_out_area: CA stations only (non-CA to non-CA does NOT count)
- points: Phone = 2 points; CW = 3 points (per non-dup contact)
- multipliers_in_area:  US states (50) + VE provinces/territories (13) = 63 total, max 58 scored
- multipliers_out_area: CA counties (58, max 58)
- dupe: per_band_mode   # dupes give no credit but no penalty; keep them in log
- serial: all_band      # single continuous QSO serial sequence across all bands
- cabrillo_name: CA-QSO-PARTY
- notes: Serial number exchange (not RST). Continuous serial across bands. Non-CA stations cannot work each other for credit. Multiplier cap 58 (of 63) reflects that CA stations rarely work all states+provinces.

## Colorado QSO Party (COQP)  [id: qp-colorado]
- wa7bnm_ref: none (found via ppraa.org/coqp)
- rules_url: https://ppraa.org/downloads/coqp/COQP%20Rules.pdf   # Rev 18-1 (2018), the currently-linked official PDF
- confidence: verified
- modes: [CW, PHONE, DIGITAL(RTTY/PSK), MIXED]   # digital explicitly allowed
- bands: [160, 80, 40, 20, 15, 10, 6, VHF, UHF]   # "160-10m excluding WARC" + ALL VHF/UHF including 6m
- warc_excluded: yes        # WARC bands the only HF exclusion; no repeater/satellite QSOs
- period: Saturday of Labor Day weekend, 1300Z–0400Z (15h)
- role_distinction: yes
- exchange_in_area:  sent=call+NAME+CO county (3-letter) ; rcvd=call+name+(county | state/prov | DXCC prefix)
- exchange_out_area: sent=call+NAME+(state/prov for W/VE incl KH6/KL7 | DXCC prefix for DX) ; rcvd=call+name+CO county
- works_for_points_in_area:  everyone
- works_for_points_out_area: CO stations only
- points: CO/US/Canada: Phone = 1, CW/digital = 2 (per band). DX (all other DXCC + MM/AM): Phone = 2, CW/digital = 4 (per band).
- multipliers_in_area:  CO counties (64) + states+DC (51) + VE provinces (13) + DXCC (excl US/Canada/KH6/KL7) + MM/AM ITU regions; counted once per MODE (not per band)
- multipliers_out_area: CO counties (max 64)
- dupe: per_band_mode   # "once per mode, per band (and mobiles in each new county)"
- serial: none          # exchange carries NAME, not serial
- cabrillo_name: CO-QSO-PARTY
- notes: Exchange includes operator NAME + county/state (no RST). Mode acts as a score MULTIPLIER too (Phone x1, CW x2, Digital x2; DXCC doubled). Power multipliers: QRP x3 / ≤150W x2 / >150W x1. Bonus: 1000 pts per CO county activated by mobile/portable (≥10 QSOs); 500 pts per QSO with PPRAA club stns AF0S/WA0VTU. Rules PDF is 2018 revision — verify no newer revision before shipping.

## Delaware QSO Party  [id: qp-delaware]
- wa7bnm_ref: 240
- rules_url: https://www.fsarc.org/qsoparty/rules.htm   # sponsor host unreachable during research (TLS cert error); block sourced from WA7BNM + secondary summaries
- confidence: partial
- modes: [CW, PHONE, DIGITAL/RTTY]   # WA7BNM lists CW/RTTY/Digital + Phone; FT8 status unconfirmed
- bands: [160, 80, 40, 20, 15, 10, 6, VHF]   # HF 160-10m per suggested freqs; "6 meters and up" allowed incl repeater contacts within DE — CONFIRM exact set
- warc_excluded: yes (assumed; unconfirmed)
- period: 1st full weekend of May, ~1700Z Sat – 2359Z Sun; 2026 = May 2–3
- role_distinction: yes
- exchange_in_area:  sent=class "1A" + DE county code (1A DE = New Castle / 2A DE = Kent / 3A DE = Sussex; county codes also NDE/KDE/SDE) ; rcvd=class + (DE county | section | DX)
- exchange_out_area: sent=class "1A" + ARRL/RAC section or "DX" (e.g. 1A AZ) ; rcvd=class + DE county
- works_for_points_in_area:  everyone (assumed)
- works_for_points_out_area: DE stations only (assumed)
- points: (inside DE) Phone = 1, Digital = 2, CW = 2 per QSO   # per-role point table not fully confirmed
- multipliers_in_area:  ARRL/RAC sections + DX (location multipliers) — CONFIRM
- multipliers_out_area: DE counties (3) — CONFIRM
- dupe: per_band_mode (assumed); DE mobiles changing county = new station
- serial: none          # Field-Day-style class + section exchange, no RST/serial
- cabrillo_name: DE-QSO-PARTY
- notes: PARTIAL — First State ARC (fsarc.org) was unreachable (broken TLS / connection refused) and Wayback was inaccessible; figures above come from the WA7BNM detail page and cached search summaries of the FSARC rules. Distinctive Field-Day-style exchange (class number+letter, e.g. "1A DE"). Power multiplier QRP x3 / ≤150W x2 / >150W x1. +50 point bonus for electronic log submission. RE-VERIFY against https://www.fsarc.org/qsoparty/rules.htm before encoding.

## Florida QSO Party (FQP)  [id: qp-florida]
- wa7bnm_ref: 325
- rules_url: http://floridaqsoparty.org/rules/
- confidence: verified
- modes: [CW, PHONE, MIXED]   # "No digital QSOs are allowed in the FQP"
- bands: [40, 20, 15, 10]     # NO 160/80m, no WARC, no VHF
- warc_excluded: yes
- period: Last full weekend of April; Sat 1600Z–Sun 0159Z + Sun 1200Z–2159Z (20h total); 2026 = Apr 25–26
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+FL county ; rcvd=RS(T)+(FL county | state | province | DXCC prefix)
- exchange_out_area: sent=RS(T)+(state for US/KH6/KL7 | province for VE | DXCC prefix for DX) ; rcvd=RS(T)+FL county
- works_for_points_in_area:  everyone (FL works inside and outside FL)
- works_for_points_out_area: FL stations only
- points: Phone = 1 point/band; CW = 2 points/band
- multipliers_in_area:  50 states + DC + VE provinces + DXCC countries + Maritime Mobile regions (no in-state county mult)
- multipliers_out_area: FL counties (67)
- dupe: per_band_mode   # "once per mode per band, max 8 QSOs" (4 bands x 2 modes)
- serial: none
- cabrillo_name: FL-QSO-PARTY
- notes: Only 4 bands (40/20/15/10). Strictly CW/Phone — no digital. Suggested CW segments band-edge+25–55 kHz; Phone in upper portions. In-state stations do NOT count FL counties as multipliers (out-of-state do).

## Georgia QSO Party  [id: qp-georgia]
- wa7bnm_ref: 328
- rules_url: https://gaqsoparty.com/georgia-qso-party-rules/
- confidence: verified
- modes: [CW, SSB]          # "Digital contacts aren't allowed in the Georgia QSO Party"
- bands: [160, 80, 40, 20, 15, 10, 6]   # suggested freqs include 1.8xx and 50.xxx MHz
- warc_excluded: yes
- period: 2nd full weekend of April; Sat 1800Z–Sun 0359Z + Sun 1400Z–2359Z
- role_distinction: yes
- exchange_in_area:  sent=RST+GA county abbrev ; rcvd=RST+(GA county | state | province | "DX")
- exchange_out_area: sent=RST+(USPS state abbrev | VE province | "DX") ; rcvd=RST+GA county
- works_for_points_in_area:  everyone
- works_for_points_out_area: GA stations only
- points: SSB = 1 point; CW = 2 points
- multipliers_in_area:  US states + DC incl GA (51) + VE provinces (13) = 128 possible (counted once per mode → x2)
- multipliers_out_area: GA counties (159), 318 possible (once per mode → x2)
- dupe: per_band_mode   # "once per band and mode for QSO points"
- serial: none
- cabrillo_name: GA-QSO-PARTY
- notes: CW/SSB only, no digital. Multipliers counted once per mode (CW mult + SSB mult separately). 159 GA counties.

## Hawaii QSO Party  [id: qp-hawaii]
- wa7bnm_ref: 96
- rules_url: https://www.hawaiiqsoparty.org/rules-page/
- confidence: verified
- modes: [CW, SSB, DIGITAL]   # digital explicitly incl RTTY, PSK, FT4/FT8 ("FT-x"), VarAC etc.
- bands: [160, 80, 40, 20, 15, 10]   # 6 HF bands; no WARC, no VHF/6m
- warc_excluded: yes
- period: 4th weekend of August; per 2026 rules 1800Z Aug 22 – 0359Z Aug 24 (reduced to 34h for 2026)
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+HI district (multiplier) ; rcvd=RS(T)+(state/prov | "DX")   # grid square allowed if mode can't send QTH
- exchange_out_area: sent=RS(T)+(US state/DC | VE province | "DX") ; rcvd=RS(T)+HI district
- works_for_points_in_area:  everyone (HI works anyone)
- works_for_points_out_area: HI stations only
- points: SSB = 2, CW = 3, digital = 3 (per QSO)
- multipliers_in_area:  14 HI districts + US states/DC + VE provinces + DXCC entities — counted ONCE ONLY (not per band)
- multipliers_out_area: 14 HI districts PER BAND (max 84 = 6 bands x 14 districts)
- dupe: per_band_mode   # once per band-mode; each station workable 3x per band (CW/SSB/digital)
- serial: none
- cabrillo_name: HI-QSO-PARTY
- notes: FT4/FT8 ARE allowed here (unlike most QP). Asymmetric multipliers: non-HI count HI districts PER BAND; HI stations count all mults once only. WA7BNM lists 1600Z Aug 22–0200Z Aug 24; sponsor 2026 rules page says 1800Z Aug 22–0359Z Aug 24 — use sponsor timing, re-confirm each year.

## Illinois QSO Party (ILQP)  [id: qp-illinois]
- wa7bnm_ref: 167
- rules_url: https://w9awe.org/ilqp/   # (2024/2025 rules PDF linked from this WIARC page)
- confidence: verified
- modes: [CW, DIGITAL, PHONE]   # digital allowed but FT4/FT8 excluded (per WA7BNM "CW/digital (no FT4/8), Phone")
- bands: [160, 80, 40, 20, 15, 10, 6, 2]
- warc_excluded: yes
- period: 3rd full weekend of October (Sunday), 1700Z–0059Z (8h)
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+IL county ; rcvd=RS(T)+(IL county | state/prov | country)   # RSTs assumed 59/599 by log checkers
- exchange_out_area: sent=RS(T)+(state | province | country) ; rcvd=RS(T)+IL county
- works_for_points_in_area:  everyone
- works_for_points_out_area: IL stations only
- points: Phone = 1 point; CW and all other (digital) modes = 2 points ("All digital modes other than CW count two points per contact")
- multipliers_in_area:  IL counties + states + countries
- multipliers_out_area: IL counties
- dupe: per_band_mode   # "worked once per mode on each band" (standard); confirm in current PDF
- serial: none
- cabrillo_name: IL-QSO-PARTY
- notes: 8-hour Sunday-only event; county-line portable operations popular (a single QSO can count for 2–4 counties). Log checkers assume 59/599 so RST is nominal. Points/modes confirmed from official WIARC page; exact dupe wording taken from convention — verify against current-year rules PDF.

## Indiana QSO Party (INQP)  [id: qp-indiana]
- wa7bnm_ref: 8
- rules_url: http://www.hdxcc.org/inqp/rules.html   # official host unreachable during research (connection refused); block sourced from WA7BNM + qsl.net/SM3CER mirror
- confidence: partial
- modes: [CW, PHONE, DIGITAL?]   # WA7BNM lists Phone/CW; mirror's point rule says "all other modes = 3 pts" implying digital allowed — CONFIRM
- bands: [160, 80, 40, 20, 15, 10]   # WA7BNM; mirror also lists 6m/2m/70cm freqs (possibly older version) — CONFIRM
- warc_excluded: yes
- period: 1st full weekend of May, Sat 1500Z–Sun 0259Z; 2026 = May 2 1600Z–May 3 0400Z (per WA7BNM)
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+IN county ; rcvd=RS(T)+(IN county | state/prov | country)
- exchange_out_area: sent=RS(T)+(state | province | country) ; rcvd=RS(T)+IN county
- works_for_points_in_area:  everyone
- works_for_points_out_area: IN stations only
- points: Phone = 2 points; all other modes (CW, digital) = 3 points   # per mirror; confirm
- multipliers_in_area:  IN counties + states + provinces + countries worked
- multipliers_out_area: IN counties
- dupe: per_band_mode   # "once per mode on each band"; mobiles once per mode per IN county
- serial: none
- cabrillo_name: IN-QSO-PARTY
- notes: PARTIAL — hdxcc.org (official INQP rules) refused connection repeatedly. Figures from WA7BNM detail page and a third-party rules mirror (qsl.net/yt1dz, SM3CER) that may be an OLD revision — note conflicts on bands (mirror shows VHF freqs incl 6m/2m/70cm; WA7BNM says 160–10m only) and on whether digital is permitted. RE-VERIFY at http://www.hdxcc.org/inqp/rules.html before encoding.

## Iowa QSO Party (IAQP)  [id: qp-iowa]
- wa7bnm_ref: 484
- rules_url: http://www.w0yl.com/IAQP
- confidence: verified
- modes: [CW, PHONE, DIGITAL]   # phone incl "digital phone"; digital excl digital-phone; FT8/FT4 explicitly NOT allowed (no free-form text). Satellite allowed; no repeaters.
- bands: [160, 80, 40, 20, 15, 10, 6, 2, 1.25, 0.70]   # any amateur band EXCEPT 60/30/17/12m
- warc_excluded: yes        # 30/17/12m excluded (and 60m)
- period: 3rd Saturday of September, 1400Z–0200Z (12h); 2026 ≈ Sep 19
- role_distinction: yes
- exchange_in_area:  sent=RS/T+IA county ; rcvd=RS/T+(IA county | state/prov | "DX")
- exchange_out_area: sent=RS/T+(state | province | "DX") ; rcvd=RS/T+IA county
- works_for_points_in_area:  everyone (IA works everybody including other IA)
- works_for_points_out_area: IA stations only
- points: Phone = 1; CW = 2; Digital = 2 (per QSO)
- multipliers_in_area:  IA counties + states + VE provinces
- multipliers_out_area: IA counties (one per county)
- dupe: per_band_mode   # non-IA: once per mode per band. IA: once per mode per band per county.
- serial: none
- cabrillo_name: IA-QSO-PARTY
- notes: FT8/FT4 explicitly excluded (software can't do free-form county exchange). Satellite QSOs count; repeater/network QSOs do not. Broad band set includes VHF/UHF up to 70cm.

## Kansas QSO Party (KSQP)  [id: qp-kansas]
- wa7bnm_ref: 483
- rules_url: https://ksqsoparty.org/rules/KSQPRules2025.pdf
- confidence: verified
- modes: [CW, SSB, RTTY]   # plus a SEPARATE optional FT4/FT8 category (own log, scored by QSO count only, no mults)
- bands: [80, 40, 20, 15, 10, 6]   # NO 160m; WARC excluded; simplex only, no repeaters/digipeaters/internet
- warc_excluded: yes
- period: Last full weekend of August; Sat 1400Z–0200Z + Sun 1400Z–2000Z; 2026 = Aug 29–30
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+KS county (3-char) ; rcvd=RS(T)+(KS county | state/prov | "DX")
- exchange_out_area: sent=RS(T)+(US state | VE province | "DX", 2-char) ; rcvd=RS(T)+KS county (KS abbrev not used)
- works_for_points_in_area:  everyone (KS works everyone)
- works_for_points_out_area: KS stations only
- points: Phone = 2; CW = 3; RTTY = 3 (per non-dup contact)
- multipliers_in_area:  US states (50; first KS county logged = the KS mult) + VE provinces/territories (13) + DX (1) = max 64
- multipliers_out_area: KS counties (max 105)
- dupe: per_band_mode   # once per band on each of Phone / CW / RTTY
- serial: none
- cabrillo_name: KS-QSO-PARTY
- notes: No 160m (starts at 80m). Main contest is CW/SSB/RTTY; FT4/FT8 is a separate parallel entry with its own log (grid-square exchange, workable once each on FT4 and FT8 per band/county, scored by non-dup QSO count with NO multipliers). Bonus: one-time +100 pts for working KS0KS. Score = (QSO pts x mults) + bonus (max 100). Part of the State QSO Party Challenge.

## Kentucky QSO Party  [id: qp-kentucky]
- wa7bnm_ref: 371
- rules_url: http://www.kyqsoparty.org/rules/
- confidence: verified
- modes: [CW, PHONE]        # "NO digital QSOs"; phone = SSB/FM; simplex only, no repeaters
- bands: [80, 40, 20, 15, 10, 6, 2]   # sponsor rules list 80m–2m (no 160m); WA7BNM lists 160m too — see notes
- warc_excluded: yes
- period: 1st Saturday of June, 1300Z–0100Z (12h); 2026 = Jun 6 1300Z–Jun 7 0100Z
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+KY county abbrev ; rcvd=RS(T)+(KY county | state | province | "DX")
- exchange_out_area: sent=RS(T)+(US state | VE province | "DX") ; rcvd=RS(T)+KY county
- works_for_points_in_area:  everyone (non-KY to non-KY does NOT count)
- works_for_points_out_area: KY stations only
- points: Phone = 1 point; CW = 2 points
- multipliers_in_area:  KY counties (in-state may also count states/provinces — CONFIRM; sponsor extract only stated KY counties)
- multipliers_out_area: KY counties
- dupe: per_band_mode   # "once per band, per mode, per county"
- serial: none
- cabrillo_name: KY-QSO-PARTY
- notes: CW/Phone only, no digital. KY mobiles/expeditions entering a new county may be reworked for multiplier credit. Band discrepancy: sponsor rules page (fetched) lists 80–2m without 160m, while WA7BNM lists 160m — defaulted to sponsor (no 160m); confirm. In-state multiplier detail (whether states/provinces also count for KY stations) was thin in the fetched rules — verify.

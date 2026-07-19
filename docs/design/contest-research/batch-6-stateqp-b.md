# Batch 6 — US State QSO Parties (B): 13 verified, 0 partial

All 13 contests verified against sponsors' official rules (official rules PDF/page
read directly for each). Notes call out mode/band/role quirks that matter for the
contest engine (many are CW/SSB-only with NO FT8; several put CW+Digital in one
mode group; two use NAME or optional-RST exchanges).

---

## Louisiana QSO Party  [id: qp-louisiana]
- wa7bnm_ref: 250
- rules_url: https://laqp.w5gad.org/rules
- confidence: verified
- modes: [CW, SSB, DIGITAL]        # "Digital" = RTTY or any digital mode carrying the exchange; no explicit FT8 provision
- bands: [160, 80, 40, 20, 15, 10, 6, 2]
- warc_excluded: yes               # rules state "No WARC band contacts"
- period: 1st Saturday of April, 1400Z–0200Z (12h) — 2026: Apr 4–5
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RST+parish ; rcvd=RST+(state/prov/country)
- exchange_out_area: sent=RST+(state/prov/country) ; rcvd=RST+parish
- works_for_points_in_area:  everyone
- works_for_points_out_area: Louisiana stations only
- points: Phone=2; CW/Digital=4 (per QSO)
- multipliers_in_area:  parishes + states(non-LA) + provinces + DXCC, counted per band/mode
- multipliers_out_area: Louisiana parishes only (64 possible), per band/mode
- dupe: per_band_mode — but note CW and Digital are ONE mode group: a fixed station may be worked once on CW/Digital and once on Phone per band
- serial: none
- cabrillo_name: LA-QSO-PARTY
- notes: Rovers may be worked once per band per activated parish. Bonuses: 50 pts/parish activated (rovers), 100 pts for working N5LCC. Multipliers are per band/mode even though the CW/Digital dupe rule groups those two modes.

## Maryland-DC QSO Party  [id: qp-maryland-dc]
- wa7bnm_ref: 86
- rules_url: https://www.w3vpr.org/sites/default/files/mdcqsoparty/Documents/Maryland-DC_QSO_Party_Rules_2020-03-04.pdf  (rev. 06 AUG 2024, v.5)
- confidence: verified
- modes: [CW, PHONE]               # "Only two transmitting modes are allowed": Phone (any phone mode) and CW. NO digital.
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes               # WARC, 60m, and VHF/UHF all NOT allowed (no 6m)
- period: 2nd Saturday of August, 1400Z Sat–0400Z Sun (14h)
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=county/city (Table 1; incl BAL, WDC) ; rcvd=(state/prov/country)   # NO RST in the exchange — call sign + location only
- exchange_out_area: sent=(state/prov/country) ; rcvd=county/city
- works_for_points_in_area:  everyone
- works_for_points_out_area: MDC stations only (Maryland + District of Columbia)
- points: CW=3; Phone=1 (per QSO)
- multipliers_in_area:  MD counties + Baltimore City + WDC (25) + US states less MD (49) + provinces (13) + DXCC; summed. PLUS a station-category multiplier (Club×1/Rover×4/Portable×3/Mobile×2/Fixed×1) and a power multiplier (QRP×3/Low×2/High×1) applied to ALL entrants. Location mults counted once (not per band/mode).
- multipliers_out_area: 25 MDC jurisdictions only; once. (Plus the same station-category and power multipliers.)
- dupe: per_band_mode (a station may be worked up to twice per band, once each mode)
- serial: none          # and NO RST either — exchange is call + location
- cabrillo_name: unknown (Cabrillo required; CONTEST tag not specified in rules — likely MD-QSO-PARTY, unconfirmed)
- notes: NO digital modes. Exchange carries NO signal report. Alaska/Hawaii count as states; DC and Baltimore City are separate multipliers; MD may NOT be counted as a worked state. Bonuses: 50 pts for working W3VPR; 500 pts for all-25-jurisdiction sweep (250 for 13). Cross-mode contacts prohibited.

## Michigan QSO Party  [id: qp-michigan]
- wa7bnm_ref: 323
- rules_url: https://miqp.org/index.php/rules/
- confidence: verified
- modes: [CW, SSB]                 # CW and SSB only. NO digital.
- bands: [80, 40, 20, 15, 10]      # no 160, no WARC, no 6m/VHF
- warc_excluded: yes
- period: Saturday of the 3rd full weekend of April, 1600Z Sat–0400Z Sun (12h)
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RST+MI county ; rcvd=RST+(state/prov/"DX")
- exchange_out_area: sent=RST+(state/prov/"DX") ; rcvd=RST+MI county
- works_for_points_in_area:  everyone
- works_for_points_out_area: Michigan stations only
- points: SSB=1; CW=2 (per QSO)
- multipliers_in_area:  83 MI counties + 49 states(excl MI) + DC + 13 provinces + DX; counted once per mode (same mult on CW and SSB = two)
- multipliers_out_area: 83 MI counties only, once per mode
- dupe: per_band_mode
- serial: none
- cabrillo_name: unknown (likely MI-QSO-PARTY, unconfirmed)
- notes: Power classes High(>100W)/Low(100W)/QRP(5W). Multipliers counted per mode, not per band.

## Minnesota QSO Party  [id: qp-minnesota]
- wa7bnm_ref: 238
- rules_url: https://www.w0aa.org/wp-content/docs/mnqp/MNQP_2027_Contest_Rules_A.pdf  (updated for 2027)
- confidence: verified
- modes: [CW, PHONE]               # Phone = SSB/DSB/FM/AM; CW. NO digital.
- bands: [160, 80, 40, 20, 15, 10] # HF 160–10 excluding WARC; no 6m
- warc_excluded: yes
- period: 1st Saturday of February, 1400Z–2359Z (~10h)
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=NAME + county(3-letter) ; rcvd=NAME + (state/prov)   # NO RST — first name is exchanged
- exchange_out_area: sent=NAME + (state/prov) ; rcvd=NAME + county   ; DX sends NAME only (log section as DX)
- works_for_points_in_area:  everyone
- works_for_points_out_area: Minnesota stations only
- points: Phone=2; CW=3 (per QSO; new for 2027)
- multipliers_in_area:  50 states(incl MN) + DC + 10 provinces + 3 territories + 1 DX = 65/mode (130 max). NEW 2027: in-state MN county multipliers NO LONGER count for MN stations (QSOs still count). Once per mode.
- multipliers_out_area: 87 MN counties, 87/mode (174 max)
- dupe: per_band_mode (once CW, once Phone per band)
- serial: none          # exchange uses operator NAME, not RST/serial
- cabrillo_name: MN-QSO-PARTY
- notes: Exchange is NAME + location (no signal report). MN power capped at 100W (except QRP 5W). Mobiles/rovers worked once per band/mode per county.

## Mississippi QSO Party  [id: qp-mississippi]
- wa7bnm_ref: 308
- rules_url: https://arrlmiss.org/wp-content/uploads/2025/12/2026-MS-QSO-PARTY-RULES-FINAL.pdf
- confidence: verified
- modes: [CW, SSB, RTTY, FT4, FT8] # digital + FT4/8 explicitly allowed
- bands: [160, 80, 40, 20, 15, 10, 6, 2]
- warc_excluded: yes
- period: 1st Saturday of April, 1400Z–0200Z (12h) — 2026: Apr 4–5
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RS(T)+MS county ; rcvd=RS(T)+(state/prov/country)   ; FT4/8: signal report + grid square (both sides)
- exchange_out_area: sent=RS(T)+(state/prov/country) ; rcvd=RS(T)+MS county   ; FT4/8: signal report + grid square
- works_for_points_in_area:  everyone
- works_for_points_out_area: Mississippi stations only (out-of-state mults come only from MS counties/grids)
- points: SSB=1; CW=2; RTTY=2; FT4/8=2 (per QSO)
- multipliers_in_area:  MS counties(82) + states(49) + provinces/territories(13) + DXCC(336) on SSB/CW/RTTY, PLUS (total FT8/4 grid squares worked ÷ 4, round up). Earned ONCE regardless of band/mode.
- multipliers_out_area: MS counties(82) on SSB/CW/RTTY + MS grid squares(9: EM41–44, EM50–54) on FT8/4. Once regardless of band/mode.
- dupe: per_band_mode (same station once per band per mode; e.g. 20m CW, 20m SSB, 20m RTTY, 20m FT4/8 all count)
- serial: none (RST; grid for FT4/8)
- cabrillo_name: MS-QSO-PARTY
- notes: FT4/8 uses grid-square exchange and a divide-by-4 multiplier scheme. No separate power categories.

## Missouri QSO Party  [id: qp-missouri]
- wa7bnm_ref: 327
- rules_url: https://www.w0ma.org/mo_qso_party/results/thisyear/moqp-2026-rules-final.pdf  (2026)
- confidence: verified
- modes: [CW, DIGITAL, PHONE]      # FT4/FT8 (and any mode without free-text) explicitly NOT allowed; satellite prohibited
- bands: [160, 80, 40, 20, 15, 10, 6, 2, 1.25, 0.70]   # includes 1.25m and 70cm; WARC excluded
- warc_excluded: yes
- period: two periods (2nd full weekend of April; 2026 shifted to Apr 11–12 for Easter): Day1 1400Z Sat–0400Z Sun, Day2 1400Z–2000Z Sun
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=call+RST+MO county ; rcvd=call+RST+(state/prov/terr or "DX")
- exchange_out_area: sent=call+RST+(state/prov/terr) ; rcvd=call+RST+MO county   ; DX sends RST+"DX"
- works_for_points_in_area:  everyone (MO↔MO once per mode per band per MO county)
- works_for_points_out_area: Missouri stations only (once per mode per band per MO county)
- points: Phone=1; CW=2; Digital=2 (per QSO)
- multipliers_in_area:  MO counties(115) + US states(49) + provinces/territories(13) + 1 DX (if ≥1 DX worked). Total worked, not per band.
- multipliers_out_area: MO counties(115). Total worked, not per band.
- dupe: per_band_mode; note digital = once per band across ALL digital modes (can't rework via a different digital mode)
- serial: none (RST)
- cabrillo_name: unknown (Cabrillo required; CONTEST tag not stated in rules — likely MO-QSO-PARTY, unconfirmed)
- notes: NO FT4/FT8; NO satellite; NO cross-band/cross-mode. Bonuses: W0MA +100, K0GQ +100, electronic Cabrillo +100, and +1/QSO on 40/80m during two Sat/Sun 1400–2000Z windows (up to 250). SHOWME/MISSOURI 1x1 spell-out certificate bonuses.

## Nebraska QSO Party  [id: qp-nebraska]
- wa7bnm_ref: 336
- rules_url: https://nebraskaqsoparty.com/qso-party-rules  (2026 rules; "Rules for 2026 Nebraska QSO Party")
- confidence: verified
- modes: [CW, PHONE, DIGITAL, SATELLITE]   # main scored contest: CW, Phone(SSB/AM/FM), Digital(RTTY/PSK) — NOT FT8/FT4; satellite is its own category. FT8/FT4 (grid exchange) is a SEPARATE competition.
- bands: [160, 80, 40, 20, 15, 10, 6, 2, and up]   # 160–10 excl. WARC; ALL VHF/UHF bands allowed
- warc_excluded: yes
- period: last full weekend of April, two days (2026: Apr 25–26; ~1400Z Sat through 0200Z Sun each day; WA7BNM lists 1300Z Apr 25 – 0100Z Apr 27)
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=NE county ; rcvd=(state/prov/country)     # signal report OPTIONAL
- exchange_out_area: sent=(state/prov/country) ; rcvd=NE county     # signal report OPTIONAL ; FT8/FT4 side-event uses grid square
- works_for_points_in_area:  everyone
- works_for_points_out_area: Nebraska stations only
- points: Digital=1; Phone=2; CW=3; Satellite=4 (per QSO). Separate FT8/FT4 grid competition = 2 pts/QSO.
- multipliers_in_area:  NE counties + states(50) + provinces(13) [S/P/C], counted once per contest. PLUS power multiplier (QRP×5 / <100W ×2 / >100W ×1).
- multipliers_out_area: NE counties only (93 max), once per contest. PLUS power multiplier.
- dupe: per_band_mode (same band different mode = unique; mobile/portable from a different county = unique)
- serial: none; RST optional
- cabrillo_name: n/a — Nebraska accepts combined ADIF logs (not Cabrillo); no CONTEST tag
- notes: Score = QSO points × power mult × geo mult + bonuses. Rare-grid activation bonus (DN91CE/DE/EE). Section-appointee bonuses: KA0BOJ +100, others +50 each. New Multi and TEAM NEBRASKA categories.

## New Jersey QSO Party  [id: qp-new-jersey]
- wa7bnm_ref: 92
- rules_url: https://sites.google.com/view/k2td-bcrc/nj-qp/2026-rules
- confidence: verified
- modes: [CW, PHONE, DIGITAL]      # digital = any mode capable of carrying the NJQP exchange
- bands: [80, 40, 20, 15, 10]      # "80, 40, 20, 15 and 10 meters ONLY" — no 160, no WARC, no 6m/VHF
- warc_excluded: yes
- period: 2nd Saturday of September, 1400Z–0200Z (10AM–10PM EDT, 12h) — 2026: Sep 12
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RS(T)+4-char county ; rcvd=RS(T)+(state/prov/"DX")
- exchange_out_area: sent=RS(T)+(2-char state/prov/"DX") ; rcvd=RS(T)+4-char county
- works_for_points_in_area:  everyone
- works_for_points_out_area: everyone for QSO points, but out-of-NJ earn multipliers ONLY from NJ counties (verify against full rules if strict who-works-whom matters)
- points: CW/Digital=2; Phone=1 (per QSO)
- multipliers_in_area:  21 NJ counties + 49 states + 13 provinces + 1 DX (84 max). PLUS power multiplier (High×1/Low×2/QRP×4).
- multipliers_out_area: 21 NJ counties only. PLUS power multiplier.
- dupe: per_band_mode
- serial: none (RST)
- cabrillo_name: unknown (likely NJQP/NJ-QSO-PARTY, unconfirmed)
- notes: Only 5 HF bands (80–10). Rookie overlay; Mobile/Rover/Portable can be reworked per new geographic area. Who-works-whom extracted from the official 2026 rules page summary; confirm the exact out-of-state working restriction if the engine enforces it.

## New Mexico QSO Party  [id: qp-new-mexico]
- wa7bnm_ref: 286
- rules_url: http://www.newmexicoqsoparty.org/wp/wp-content/uploads/2026/04/NMQP_Forms.pdf  (2026 rules, pp.3–8)
- confidence: verified
- modes: [CW, PHONE, DIGITAL]      # any computer-to-computer mode = digital. FT8/FT4 allowed ONLY if the county/SPC exchange is carried (grid/RST-only FT8 QSOs cannot be scored)
- bands: [160, 80, 40, 20, 15, 10, 6, 2]
- warc_excluded: yes               # WARC, 60m, and above 2m NOT permitted
- period: 2nd Saturday of April, 1400Z–0200Z (12h) — 2026: Apr 11
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RS(T)+NM county ; rcvd=RS(T)+(state/prov/DXCC)
- exchange_out_area: sent=RS(T)+(state/prov/DXCC) ; rcvd=RS(T)+NM county
- works_for_points_in_area:  everyone
- works_for_points_out_area: New Mexico stations only
- points: Phone=1; CW=2; Digital=2 (per QSO)
- multipliers_in_area:  NM counties(33) + states(50) + provinces/territories(13) + DX entities; each once regardless of mode/band. PLUS power mult (QRP×5/Low×2/High×1). AK/HI count as states; DC counts as Maryland.
- multipliers_out_area: NM counties(33) only, once regardless of mode/band. PLUS power mult.
- dupe: per_band_mode
- serial: none (RST)
- cabrillo_name: NM-QSO-PARTY   # confirmed in the rules' Cabrillo sample (CONTEST: NM-QSO-PARTY; modes PH/CW/RY)
- notes: FT8/FT4 must be edited to carry 599 + county/state/DX (grid+report-only logs unscoreable). Mobile bonus 5,000 pts/county with ≥15 QSOs. 2026-only W1AW/5 bonus (250 pts).

## New York QSO Party  [id: qp-new-york]
- wa7bnm_ref: 473
- rules_url: http://www.nyqp.org/NYQP_Rules.pdf  (sponsor's current rules pointer; cross-checked against 2026 WA7BNM summary — identical structure)
- confidence: verified
- modes: [CW, PHONE, DIGITAL]      # Phone, CW, RTTY/Digital (all digital modes = one mode)
- bands: [160, 80, 40, 20, 15, 10, 6, 2]   # "ARRL Band Plan excluding WARC — all FCC allocated amateur freqs available"
- warc_excluded: yes
- period: 3rd Saturday of October, 1400Z–0200Z (12h) — 2026: Oct 17–18
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RST+NY county(3-char) ; rcvd=RST+(state/prov/"DX")
- exchange_out_area: sent=RST+(state/prov/"DX") ; rcvd=RST+NY county
- works_for_points_in_area:  everyone
- works_for_points_out_area: New York stations only (DX counts for points but not as a multiplier)
- points: Phone=1; CW=2; RTTY/Digital=3 (per band; may work each station once per band per mode)
- multipliers_in_area:  US states(50) + NY counties(62) + Canadian provinces(9) = 121 max. DX is points-only, not a mult.
- multipliers_out_area: NY counties only (62 max)
- dupe: per_band_mode (max 3 contacts/band: one each Phone/CW/Digital)
- serial: none (RST)
- cabrillo_name: unknown (Cabrillo required; likely NY-QSO-PARTY, unconfirmed)
- notes: Provinces grouped to 9 (MAR, NL, QC, ON, MB, SK, AB, BC, NT[+YT+NU]). Digital tri-band scoring (3 pts) is the highest of this batch. Mobiles get a fresh 3-mode set of contacts per NY county.

## North Carolina QSO Party  [id: qp-north-carolina]
- wa7bnm_ref: 265
- rules_url: http://ncqsoparty.org/rules/  (2026 rules PDF: .../Rules_2026_251013.pdf)
- confidence: verified
- modes: [CW, PHONE, DIGITAL]      # Phone(SSB/AM/FM), CW, Digital(all non-FT8/4 = one mode). FT8/FT4 is a SEPARATE "Weak Signal Showcase" event with grid exchange — NOT part of the main contest log.
- bands: [80, 40, 20, 15, 10, 6, 2]   # "No 160, WARC, or above 2 meters"
- warc_excluded: yes
- period: Sunday 1500Z Mar 1 – 0100Z Mar 2 (10h) — 2026
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=call+NC county ; rcvd=call+(state/prov/"DX")     # signal report OPTIONAL
- exchange_out_area: sent=call+(state/prov/"DX") ; rcvd=call+NC county      # signal report OPTIONAL
- works_for_points_in_area:  everyone
- works_for_points_out_area: North Carolina stations only
- points: Phone=2; CW=3; Digital=5 (per QSO). "Rarest of NC" counties score 10× (Phone 20/CW 30/Digital 50).
- multipliers_in_area:  100 NC counties + 49 states + DC + 13 provinces/territories + 1 DX = 164. Counted once across all modes/bands/locations.
- multipliers_out_area: 100 NC counties only, once
- dupe: per_band_mode (same station once per mode per band)
- serial: none; RST optional
- cabrillo_name: NC-QSO-PARTY
- notes: Digital=5 pts is highest QSO value; RST optional in exchange. Sweep bonus 500 pts for ≥1 QSO in 5 "Rarest of NC" counties. FT8/FT4 Weak Signal Showcase is a separate ADIF competition (5 pts/QSO × grid squares), NOT mixed into the main Cabrillo log.

## North Dakota QSO Party  [id: qp-north-dakota]
- wa7bnm_ref: 468
- rules_url: https://ndarrlsection.com/2026/2026_nd_qsp_party_rules.pdf
- confidence: verified
- modes: [CW, PHONE, DIGITAL]      # Phone(SSB/FM), CW, Digital(RTTY/PSK). Explicitly "NO FT8".
- bands: [160, 80, 40, 20, 15, 10, 6, 2]
- warc_excluded: yes
- period: mid-April, 1800Z Sat – 1800Z Sun (24h) — 2026: Apr 11–12
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RST+ND county ; rcvd=RST+(state/prov/terr or DX country)
- exchange_out_area: sent=RST+(state/prov/terr) ; rcvd=RST+ND county   ; DX sends RST+country
- works_for_points_in_area:  everyone
- works_for_points_out_area: North Dakota stations only
- points: 1 point per QSO for ALL modes (CW/Digital/Phone), per band
- multipliers_in_area:  ND counties(53) + states/provinces/territories(63: 49 states + DC + 10 provinces + 3 territories) = 116. Counted ONCE overall (not per band/mode). ND may work DXCC for points only (no mult).
- multipliers_out_area: 53 ND counties only, once overall
- dupe: per_band_mode (ND stations also per band/mode/county)
- serial: none (RST)
- cabrillo_name: ND-QSO-PARTY
- notes: Flat 1 pt/QSO regardless of mode (unusual). NO FT8. Multipliers count once overall, not per band. No power limit (legal max). No repeater/remote/internet QSOs.

## Ohio QSO Party  [id: qp-ohio]
- wa7bnm_ref: 100
- rules_url: http://www.ohqp.org/index.php/rules/
- confidence: verified
- modes: [CW, SSB]                 # CW and SSB only. NO digital.
- bands: [160, 80, 40, 20, 15, 10] # no WARC, no 6m/VHF
- warc_excluded: yes
- period: 4th Saturday of August, 1600Z Sat–0400Z Sun (12h) — 2026: Aug 22–23
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RST+OH county ; rcvd=RST+(state/prov/"DX")
- exchange_out_area: sent=RST+(state/prov/"DX") ; rcvd=RST+OH county
- works_for_points_in_area:  everyone
- works_for_points_out_area: Ohio stations only
- points: SSB=1; CW=2 (per QSO)
- multipliers_in_area:  49 states(excl OH) + 11 provinces + 88 OH counties + 1 DX = 149; counted once per mode (same mult on CW and SSB counts twice)
- multipliers_out_area: 88 OH counties only, once per mode
- dupe: per_band_mode
- serial: none (RST)
- cabrillo_name: unknown (likely OH-QSO-PARTY, unconfirmed)
- notes: CW/SSB only, no digital. Multipliers counted per mode, not per band.

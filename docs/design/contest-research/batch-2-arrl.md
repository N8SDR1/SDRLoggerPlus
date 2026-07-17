# Batch 2 — ARRL family contests

Summary: 16 blocks — 15 verified (from official sponsor rules PDFs / rules pages), 1 partial (hpm — Hiram Percy Maxim, sourced from the 2019 "Happy 150!" celebration announcement; recurs irregularly, rules may vary by running).

Sources: ARRL rules PDFs at https://contests.arrl.org/ContestRules/ , ARRL.org rules pages, Winter Field Day Association rules PDF, WA7BNM contest calendar for cross-check.

---

## ARRL International DX Contest, CW  [id: arrl-dx-cw]
- wa7bnm_ref: 256
- rules_url: https://contests.arrl.org/ContestRules/DX-Rules.pdf
- confidence: verified
- modes: [CW]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: Third full weekend of February; 0000 UTC Sat through 2359 UTC Sun (48h, no time limit)
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RST + (US state / Canadian province) ; rcvd=RST + power    # in_area = W/VE stations
- exchange_out_area: sent=RST + power (number or abbrev) ; rcvd=RST + (state/province)    # out_area = DX stations
- works_for_points_in_area:  W/VE stations may ONLY contact DX stations
- works_for_points_out_area: DX stations may ONLY contact W/VE stations
- points: 3 points per QSO (all contacts)
- multipliers_in_area:  W/VE count DXCC entities worked (except USA and Canada); KH6 and KL7 count as DXCC entities. Per band.
- multipliers_out_area: DX count US states + DC + Canadian provinces/territories + Labrador (LB); VO1/VO2 counted separately. Per band.
- dupe: per_band
- serial: none
- cabrillo_name: ARRL-DX-CW
- notes: W/VE = US (excl AK/HI) + Canada (excl CY9/CY0). Hawaii (KH6), Alaska (KL7), St. Paul Is. (CY9), Sable Is. (CY0) participate AS DX stations. Power cats QRP(5W)/LP(100W)/HP(1500W). Categories SO, SOU, SOSB, SOUSB, MS, M2, MM. Limited Antennas overlay. Log-checking penalty = QSO point value for busted/NIL.

---

## ARRL International DX Contest, SSB  [id: arrl-dx-ssb]
- wa7bnm_ref: 289
- rules_url: https://contests.arrl.org/ContestRules/DX-Rules.pdf
- confidence: verified
- modes: [SSB]      # "Phone"
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: First full weekend of March; 0000 UTC Sat through 2359 UTC Sun (48h, no time limit)
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RS + (US state / Canadian province) ; rcvd=RS + power    # in_area = W/VE stations
- exchange_out_area: sent=RS + power (number or abbrev) ; rcvd=RS + (state/province)    # out_area = DX stations
- works_for_points_in_area:  W/VE stations may ONLY contact DX stations
- works_for_points_out_area: DX stations may ONLY contact W/VE stations
- points: 3 points per QSO (all contacts)
- multipliers_in_area:  W/VE count DXCC entities (except USA and Canada); KH6/KL7 count as DXCC. Per band.
- multipliers_out_area: DX count US states + DC + Canadian provinces/territories + Labrador (LB); VO1/VO2 separate. Per band.
- dupe: per_band
- serial: none
- cabrillo_name: ARRL-DX-SSB
- notes: Same rules as the CW weekend, Phone only. Same DX/W-VE role split and multiplier structure.

---

## ARRL 10-Meter Contest  [id: arrl-10m]
- wa7bnm_ref: 199
- rules_url: https://contests.arrl.org/ContestRules/10M-Rules.pdf
- confidence: verified
- modes: [CW, SSB]      # Mixed / CW-only / Phone-only categories
- bands: [10]      # 28 MHz only; CW must be below 28.3 MHz
- warc_excluded: n/a
- period: Second full weekend of December; 0000 UTC Sat through 2359 UTC Sun (max 36 of 48h)
- role_distinction: yes    # exchange differs by role; everyone works everyone
- exchange_all:      n/a
- exchange_in_area:  sent=RS(T) + (state/province) ; rcvd=RS(T) + (state/province or serial)    # in_area = W/VE + Mexican stations (DC sends "DC")
- exchange_out_area: sent=RS(T) + serial number ; rcvd=RS(T) + (state/province or serial)    # out_area = DX stations; Maritime mobile sends ITU region 1-3
- works_for_points_in_area:  everyone (all stations work all stations, on both CW and Phone)
- works_for_points_out_area: everyone
- points: Phone QSO = 2 points; CW QSO = 4 points
- multipliers_in_area:  US states + DC + Canadian provinces/territories + Labrador + Mexican states (KH6/KL7 count as US states). Counted once on phone and once on CW (per-mode, not per-band since single band).
- multipliers_out_area: DXCC entities + ITU regions (for MM). Once per mode.
- dupe: per_band_mode    # single band: once per mode (once on phone, once on CW)
- serial: all_band    # DX stations only send serial; single band so one series
- cabrillo_name: ARRL-10M
- notes: Single band (28 MHz). Hawaii/Alaska operate as US stations, NOT DX. Mexican (XE) stations send state/province and count as state multipliers. Maritime mobile sends ITU region. Multiplier counts once per mode.

---

## ARRL 160-Meter Contest  [id: arrl-160m]
- wa7bnm_ref: 194
- rules_url: https://contests.arrl.org/ContestRules/160M-Rules.pdf
- confidence: verified
- modes: [CW]
- bands: [160]      # 1.8 MHz only
- warc_excluded: n/a
- period: First full weekend of December; 2200 UTC Fri through 1559 UTC Sun (42h, no time limit)
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RST + ARRL/RAC Section ; rcvd=RST + (Section or nothing)    # in_area = W/VE stations
- exchange_out_area: sent=RST (only) ; rcvd=RST + Section    # out_area = DX stations
- works_for_points_in_area:  W/VE stations may contact ANY station (W/VE and DX)
- works_for_points_out_area: DX stations may ONLY contact W/VE stations
- points: W/VE-to-W/VE QSO = 2 points; QSO with a DX station = 5 points
- multipliers_in_area:  W/VE count ARRL/RAC Sections (+ Northern Territories) + DXCC entities. Single band (no per-band).
- multipliers_out_area: DX count ARRL/RAC Sections only. Single band.
- dupe: per_contest    # single band, once per station
- serial: none    # MM/AM stations send ITU region 1-3
- cabrillo_name: ARRL-160M
- notes: Single band (1.8 MHz), CW only. AK(KL7=AK), HI(KH6=PAC), Caribbean US poss (KP1-KP5 = PR or VI), Pacific territories (KH0-KH9 = PAC) participate as W/VE and count as ARRL sections; they may work BOTH domestic and DX. No 1830-1835 kHz DX window anymore.

---

## ARRL RTTY Roundup  [id: arrl-rtty-roundup]
- wa7bnm_ref: 217
- rules_url: https://contests.arrl.org/ContestRules/RTTY-RU-Rules.pdf
- confidence: verified
- modes: [RTTY]      # RY only; automated operation NOT permitted; non-RTTY digital NOT allowed
- bands: [80, 40, 20, 15, 10]
- warc_excluded: yes    # (160m also excluded)
- period: First full weekend of January (never Jan 1); 1800 UTC Sat through 2359 UTC Sun (SO max 24 of 30h; Multi full 30h)
- role_distinction: yes    # exchange differs by role; everyone works everyone
- exchange_all:      n/a
- exchange_in_area:  sent=RST + (US state / Canadian province) ; rcvd=RST + (state/prov or serial)    # in_area = W/VE
- exchange_out_area: sent=RST + serial number (beginning with 1) ; rcvd=RST + (state/prov or serial)    # out_area = DX
- works_for_points_in_area:  everyone (all stations work all stations)
- works_for_points_out_area: everyone
- points: 1 point per QSO (all contacts)
- multipliers_in_area:  DXCC entities (except USA and Canada) + US states + DC + Canadian provinces/territories + Labrador. KH6/KL7 count as DXCC entities. Counted ONCE (NOT per band).
- multipliers_out_area: same set — all stations use the same multiplier list. Counted once total.
- dupe: per_band
- serial: all_band    # DX stations only, single continuous series beginning with 1
- cabrillo_name: ARRL-RTTY
- notes: RTTY (RY) only; automated/robotic operation prohibited. Multipliers counted only once for the whole contest, not per band. Categories SO, SOU, MS, M2, MM. Limited Antennas overlay.

---

## ARRL Field Day  [id: arrl-field-day]
- wa7bnm_ref: 57
- rules_url: https://contests.arrl.org/ContestRules/Field-Day-Rules.pdf
- confidence: verified
- modes: [CW, SSB, DIGITAL]    # Phone, CW, non-CW Digital; three mode groups
- bands: [160, 80, 40, 20, 15, 10, 6, 2, and all bands 50 MHz and above]    # no WARC, no repeaters
- warc_excluded: yes
- period: Fourth full weekend of June; 1800 UTC Sat through 2059 UTC Sun (27h if no pre-setup, else 24h)
- role_distinction: yes    # exchange differs W/VE vs DX; no scoring role split
- exchange_all:      n/a
- exchange_in_area:  sent=Class + ARRL/RAC Section (e.g. "3A CT") ; rcvd=Class + Section    # in_area = ARRL/RAC section stations
- exchange_out_area: sent=Class + "DX" (e.g. "2A DX") ; rcvd=Class + Section/DX    # out_area = DX stations
- works_for_points_in_area:  everyone in ARRL/RAC field org + IARU Region 2; DX (other regions) may be worked for credit
- works_for_points_out_area: DX stations may be worked for credit but submit only as check-logs
- points: Phone QSO = 1 point; CW QSO = 2 points; Digital QSO = 2 points
- multipliers_in_area:  NOT section-based. Score = total QSO points x POWER multiplier (5=QRP≤5W off-grid, 2=≤150W... actually ≤100W or ≤5W on mains, 1=>100W) + bonus points. Sections/grids are NOT QSO multipliers.
- multipliers_out_area: n/a (same power-multiplier scoring)
- dupe: per_band_mode    # once per band per mode
- serial: none
- cabrillo_name: ARRL-FD
- notes: EXCHANGE IS CLASS + SECTION, not a scored multiplier — final score = QSO points x power multiplier + bonus points (emergency power, media publicity, GOTA, satellite, etc; 2000-pt transmitter bonus cap at 20 tx). Classes A/A-Battery/B/B-Battery/C(mobile)/D(home mains)/E(home emergency pwr)/F(EOC). Class D works all. Power ceilings: A/B/C ≤500W PEP, D/E/F ≤100W PEP. Digital/CW = 2 pts, Phone = 1 pt. GOTA + free VHF stations allowed for Class A/F.

---

## Winter Field Day  [id: winter-field-day]
- wa7bnm_ref: 421
- rules_url: https://winterfieldday.org/downloads/2026-rules-v3.pdf
- confidence: verified
- modes: [CW, SSB, DIGITAL]    # Phone / CW / Digital groups (Phone = SSB/AM/FM/DMR/C4FM; Digital = PSK/RTTY/Olivia/Packet/JS8/etc)
- bands: [160, 80, 40, 20, 15, 10, 6, 2, and all amateur bands except WARC]    # all amateur bands except 60/30/17/12m
- warc_excluded: yes    # 12, 17, 30, 60 m excluded
- period: Last full weekend of January; starts 1600 UTC Sat, 30-hour operational period
- role_distinction: yes    # exchange differs by location identifier; no scoring role split
- exchange_all:      sent=Category(# of transceivers) + Class(H/I/O/M) + Location(ARRL/RAC Section, or MX, or DX) ; rcvd=same  (e.g. "2M EPA", "1H GA", "4O WTX")
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone (any field day station)
- works_for_points_out_area: everyone
- points: Phone QSO = 1 point; CW QSO = 2 points; Digital QSO = 2 points
- multipliers_in_area:  NOT section-based. Score = total QSO points x (Objective Multiplier OM + 1). OMs earned for objectives (e.g. 3 QSOs on 6 different bands = x6; 12 bands = another x6; away-from-home, alternate power, satellite, Winlink, etc).
- multipliers_out_area: n/a
- dupe: per_band_mode    # once per band-mode; max 3x per band (Phone+CW+Digital)
- serial: none
- cabrillo_name: WFD    # Cabrillo OR ADIF accepted
- notes: Sponsor = Winter Field Day Association (WFDA), NOT ARRL. Class = H(home)/I(indoor)/O(outdoor)/M(mobile). Signal report NOT part of required exchange. Location identifier: US/Canada = ARRL/RAC Section, Mexico = MX, elsewhere = DX. Category/Class/Location must stay constant all event. Satellite contacts give multiplier only, no QSO points.

---

## ARRL January VHF Contest  [id: arrl-vhf]
- wa7bnm_ref: 231
- rules_url: https://contests.arrl.org/ContestRules/JanJunSep-VHF-Rules.pdf
- confidence: verified
- modes: [CW, SSB, FM, DIGITAL]    # any mode above 50 MHz
- bands: [6, 2, 1.25, 0.70, 0.33, 0.23, and all bands above 50 MHz]    # 50, 144, 222, 432, 902, 1296 MHz, 2.3 GHz+
- warc_excluded: n/a
- period: Third or fourth full weekend of January (announced); 1900 UTC Sat through 0359 UTC Mon (33h)
- role_distinction: yes    # who-works-whom differs; exchange same
- exchange_all:      sent=4-character Maidenhead grid square ; rcvd=grid square
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  US/Canada/possessions may work stations worldwide (KH0-9, KL7, KP1-KP5, CY9, CY0 count as W/VE)
- works_for_points_out_area: DX outside US/Canada may ONLY work US and Canadian stations
- points: 50/144 MHz = 1 pt; 222/432 MHz = 2 pts; 902/1296 MHz = 4 pts; 2.3 GHz+ = 8 pts (January values)
- multipliers_in_area:  number of different grid squares per band (each grid counts once per band). Fixed: total QSO pts x total mults. Rovers: extra mult per grid activated.
- multipliers_out_area: same (grids per band)
- dupe: per_band    # once per band per grid square location
- serial: none
- cabrillo_name: ARRL-VHF-JAN
- notes: 6m and up only. Any mode. Rover, FM-only (50/144/222/432/902/1296), and other categories. Power limits vary by band (e.g. 50/144 MHz ≤200W for some cats). Points scale up with frequency.

---

## ARRL International Digital Contest  [id: arrl-digital]
- wa7bnm_ref: 716
- rules_url: https://contests.arrl.org/ContestRules/Digital-Rules.pdf
- confidence: verified
- modes: [DIGITAL]    # any digital mode EXCLUDING RTTY (e.g. FT8, FT4) that carries the 4-digit grid
- bands: [160, 80, 40, 20, 15, 10, 6]    # 1.8, 3.5, 7, 14, 21, 28, 50 MHz
- warc_excluded: yes
- period: First full weekend of June; 1800 UTC Sat through 2359 UTC Sun (SO max 24 of 30h; Limited-Op overlay 8h; Multi full 30h)
- role_distinction: no
- exchange_all:      sent=4-digit Maidenhead grid square locator ; rcvd=grid square
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone (all stations work all stations)
- works_for_points_out_area: everyone
- points: 1 point per QSO + 1 additional point per 500 km of grid-center distance, rounded up (min 1 distance pt if <500 km)
- multipliers_in_area:  NONE — final score = total QSO points only (distance-based). Grid squares are the exchange, used for distance, NOT a separate multiplier.
- multipliers_out_area: n/a
- dupe: per_band    # once per band regardless of mode
- serial: none
- cabrillo_name: ARRL-DIGI
- notes: RTTY explicitly EXCLUDED (that is a separate contest). Low Power (≤100W) and QRP only — NO high power. Categories: SO 1-Radio, SO 2-Radio, Multi-Single. Distance scoring: e.g. 1565 km = 1 + 4 = 5 pts. Team competition (2-5 ops within 175 mi).

---

## IARU HF World Championship  [id: iaru-hf]
- wa7bnm_ref: 67
- rules_url: https://contests.arrl.org/ContestRules/IARU-HF-Rules.pdf
- confidence: verified
- modes: [CW, SSB]    # Mixed / CW-only / Phone-only
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: Second full weekend of July; 1200 UTC Sat through 1159 UTC Sun (24h, no time limit)
- role_distinction: yes    # HQ/official stations use a different exchange & multiplier role
- exchange_all:      sent=RS(T) + ITU Zone ; rcvd=RS(T) + (ITU Zone or society abbrev)    # normal stations
- exchange_in_area:  sent=RS(T) + IARU society abbreviation (HQ stations, e.g. W1AW sends "ARRL"; NU1AW sends "IARU"); IARU officials send AC/R1/R2/R3 ; rcvd=RS(T) + (zone/society)
- exchange_out_area: n/a
- works_for_points_in_area:  everyone (all stations work all stations)
- works_for_points_out_area: everyone
- points: same ITU zone = 1; contact with IARU HQ/official = 1; same zone different continent = 1; own continent different zone = 3; different continent AND different zone = 5
- multipliers_in_area:  ITU zones worked per band + IARU member-society HQ stations worked per band + IARU officials (max 4/band: AC, R1, R2, R3). Per band (not per mode). HQ/official stations do NOT count for zone mults.
- multipliers_out_area: same
- dupe: per_band_mode    # once per band on each mode
- serial: none    # exchange is zone or society abbrev
- cabrillo_name: IARU-HF
- notes: Co-sponsored ARRL/IARU. HQ stations send society abbreviation instead of zone. Youth overlay (op ≤25). MS must stay on band/mode 10 min. US spans ITU zones 6/7/8; see zone map.

---

## ARRL Sweepstakes, CW  [id: arrl-ss-cw]
- wa7bnm_ref: 177
- rules_url: https://contests.arrl.org/ContestRules/SS-Rules.pdf
- confidence: verified
- modes: [CW]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: First full weekend of November; 2100 UTC Sat through 0259 UTC Mon (max 24 of 30h)
- role_distinction: no    # W/VE work W/VE only; single exchange for all
- exchange_all:      sent = Serial# + Precedence + Call sign + Check + ARRL/RAC Section ; rcvd = same  (e.g. "123 A W9JJ 79 CT")
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  W/VE stations may ONLY contact other W/VE stations (US + Canada incl territories/possessions)
- works_for_points_out_area: n/a (DX not worked for credit)
- points: 2 points per QSO
- multipliers_in_area:  ARRL/RAC Sections (the "Clean Sweep" set — 84 sections). Counted once for the contest (a station is worked only once total, so mults are per-contest not per-band).
- multipliers_out_area: n/a
- dupe: per_contest    # each station worked only ONCE regardless of band
- serial: all_band    # single continuous serial series across all bands
- cabrillo_name: ARRL-SS-CW
- notes: FULL exchange = Serial# + Precedence + Call + Check + Section. Precedence: Q=SO QRP, A=SO Low Power, B=SO High Power, U=SO Unlimited, M=Multi-op, S=School Club. Check = last 2 digits of year first licensed (constant all contest). Own call must be sent in exchange. Multiplier = ARRL/RAC sections. Clean Sweep = all sections. Each station worked once regardless of band.

---

## ARRL Sweepstakes, SSB  [id: arrl-ss-ssb]
- wa7bnm_ref: 178
- rules_url: https://contests.arrl.org/ContestRules/SS-Rules.pdf
- confidence: verified
- modes: [SSB]    # Phone
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: Third full weekend of November; 2100 UTC Sat through 0259 UTC Mon (max 24 of 30h)
- role_distinction: no
- exchange_all:      sent = Serial# + Precedence + Call sign + Check + ARRL/RAC Section ; rcvd = same
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  W/VE stations may ONLY contact other W/VE stations
- works_for_points_out_area: n/a
- points: 2 points per QSO
- multipliers_in_area:  ARRL/RAC Sections (84). Per contest (station worked once total).
- multipliers_out_area: n/a
- dupe: per_contest    # each station worked only ONCE regardless of band
- serial: all_band
- cabrillo_name: ARRL-SS-SSB
- notes: Same rules as SS CW, Phone weekend. Same 6-element exchange and section multiplier.

---

## ARRL Rookie Roundup  [id: arrl-rookie-roundup]
- wa7bnm_ref: 502
- rules_url: https://www.arrl.org/rookie-roundup
- confidence: verified
- modes: [SSB, CW, RTTY]    # SSB=April, CW=December, RTTY=August (one mode per running)
- bands: [80, 40, 20, 15, 10]
- warc_excluded: yes    # (160m also excluded)
- period: Sunday of the 3rd full weekend of Apr/Aug/Dec; 1800 UTC through 2359 UTC (6h)
- role_distinction: yes    # Rookie vs non-Rookie affects points, not exchange
- exchange_all:      sent = Call sign + operator first name + 2-digit year first licensed + location (US state / Canadian province / call area / DX) ; rcvd = same
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  Rookies work everyone; non-Rookies work Rookies only (for credit)
- works_for_points_out_area: n/a
- points: Rookie-to-Rookie QSO = 2 points; Rookie-to-non-Rookie QSO = 1 point
- multipliers_in_area:  51 (US states + DC) + 13 Canadian provinces/territories + 5 Mexican call areas + 1 generic DX = 70 max. (Counted once; multiplier set worked.)
- multipliers_out_area: n/a
- dupe: per_band
- serial: none
- cabrillo_name: unknown    # Rookie Roundup uses the ARRL web-app log entry, not standard Cabrillo upload
- notes: "Rookie" = licensed in the current or preceding 3 calendar years (or first-time on that mode). All categories limited to 100 W output. Non-Rookies (Veterans) may work only Rookies for credit. Single mode per running (Apr SSB / Aug RTTY / Dec CW). Not on ARRL's standard Cabrillo submission list (ELOG exception).

---

## ARRL School Club Roundup  [id: school-club-roundup]
- wa7bnm_ref: 254
- rules_url: https://www.arrl.org/school-club-roundup
- confidence: verified
- modes: [CW, SSB, DIGITAL]    # Phone, CW, digital (RTTY/PSK/packet/etc)
- bands: [all HF/VHF/UHF except WARC]    # all amateur bands except 60, 30, 17, 12 m; simplex only, no repeaters
- warc_excluded: yes
- period: 2nd full school week of February & 3rd full school week of October; 1300 UTC Mon through 2359 UTC Fri (max 6h per 24h, 24h total of the 107h window)
- role_distinction: yes    # station type (School/Club/Individual) affects the multiplier weighting
- exchange_all:      sent = Call sign + RS(T) + Class (I/C/S) + location (US state / Canadian prov/terr / DXCC entity) ; rcvd = same
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone (all stations work all stations)
- works_for_points_out_area: everyone
- points: Phone QSO = 1 point; CW or Digital QSO = 2 points (once per band/mode)
- multipliers_in_area:  [# US states + Canadian provinces/territories + DXCC entities] + 2 x (# Clubs worked) + 5 x (# Schools worked)
- multipliers_out_area: same
- dupe: per_band_mode
- serial: none
- cabrillo_name: ARRL-SCR
- notes: Station classes: I = Individual/Single-op, C = Club/non-school multi-op, S = School club/group (K-12, college, university). Schools worth 5x and clubs 2x in the multiplier. Simplex only (no repeaters). ELOG exception — submitted via the SCR web app.

---

## ARRL Kids Day  [id: kids-day]
- wa7bnm_ref: 224
- rules_url: https://www.arrl.org/kids-day
- confidence: verified
- modes: [SSB, CW]    # any mode; primarily phone; local repeaters allowed with permission
- bands: [80, 40, 20, 17, 15, 12, 10]    # suggested freq windows on 80/40/20/17/15/12/10 m + repeaters
- warc_excluded: no    # 17m and 12m explicitly included in suggested frequencies
- period: First Saturday of January & third Saturday of June; 1800 UTC through 2359 UTC (6h; operate as much/little as you like)
- role_distinction: no
- exchange_all:      sent = name + age + location + favorite color ; rcvd = same
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  everyone
- works_for_points_out_area: everyone
- points: none — non-competitive, not scored
- multipliers_in_area:  none (not scored)
- multipliers_out_area: n/a
- dupe: n/a    # no scoring; work same station again after a break is fine
- serial: none
- cabrillo_name: none    # certificate-of-participation event, no Cabrillo log
- notes: NON-COMPETITIVE youth participation event, no scoring/ranking. Suggested freqs: 28.350-28.400, 24.960-24.980, 21.360-21.400, 18.140-18.145, 14.270-14.300, 7.270-7.290, 3.740-3.940 MHz, plus local repeaters (with owner permission). Certificate available; soapbox reports encouraged.

---

## Hiram Percy Maxim Birthday Celebration  [id: hpm]
- wa7bnm_ref: none
- rules_url: http://www.arrl.org/news/arrl-announces-happy-150-hiram-percy-maxim-birthday-celebration  (2019 "Happy 150!" running; also https://contests.arrl.org/contesthome.php?eid=26)
- confidence: partial
- modes: [CW, SSB, DIGITAL]    # three mode groups: CW / phone (any voice) / digital
- bands: [all HF/VHF/UHF except WARC]    # all amateur bands except 60, 30, 17, 12 m
- warc_excluded: yes
- period: Around HPM's birthday (Sep 2). 2019 running: 0000 UTC Aug 31 through 2359 UTC Sep 8 (9 days). Recurs irregularly — dates/rules vary by running.
- role_distinction: yes    # ARRL members (append /150) vs DX vs non-members
- exchange_all:      n/a
- exchange_in_area:  sent = RS(T) + ARRL/RAC Section (ARRL/RAC members append "/150" to call) ; rcvd = RS(T) + (Section or DX)    # in_area = ARRL/RAC members
- exchange_out_area: sent = RS(T) + "DX" ; rcvd = RS(T) + Section    # out_area = DX stations
- works_for_points_in_area:  everyone (all amateurs may participate)
- works_for_points_out_area: everyone
- points: contact with W1AW/150 = 3 points; contact with any ARRL member (call/150) = 2 points; contact with non-member = 1 point
- multipliers_in_area:  83 ARRL/RAC Sections + DX = 84 total
- multipliers_out_area: same
- dupe: per_band_mode    # work each station on each of the 3 modes on each band
- serial: none
- cabrillo_name: unknown    # 2019 running used the ARRL event web app for Cabrillo upload
- notes: PARTIAL — this is an occasional operating event, not a fixed annual contest; the captured rules are from the 2019 "Happy 150!" celebration and may differ in other runnings. ARRL/RAC members append "/150" (or year-specific suffix) to their call. 150 bonus points available for various activities. Scored/logged via ARRL web app (online submission only). Verify dates and any suffix/exchange changes against ARRL's announcement for the specific year before encoding.

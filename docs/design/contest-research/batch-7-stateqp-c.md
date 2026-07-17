# Batch 7 — US State / Regional QSO Parties (set C)

Summary: 12 contests researched. 8 verified (official sponsor rules read directly:
PA, SC, SD, TX, VA, WA Salmon Run, WI, NEQP). 4 partial (OK, TN, WV, 7QP) — the
sponsor sites (k5cm.com, ws7n.net/7qp.org, qsl.net/wvqp PDF, JS-rendered tnqp.org)
were unreachable or returned only stale mirrors during research; those blocks were
reconstructed from the WA7BNM detail page cross-checked against SM3CER/older-year
mirrors and web search of the current sponsor PDFs, and any residual ambiguity is
noted per-field.

General note on exchanges: these QSO parties send a signal report as RST/RS (CW/digital
vs phone) EXCEPT where noted. Only PA and VA use a serial (QSO) number instead of RST.
None of these use operator name.

---

## Oklahoma QSO Party  [id: qp-oklahoma]
- wa7bnm_ref: 293
- rules_url: http://www.k5cm.com/okqp2025rules.pdf  (host repeatedly reset; cross-checked https://www.qsl.net/okdxa/OKQP.htm + web search of the 2025 PDF)
- confidence: partial
- modes: [CW, PHONE, DIGITAL]   # digital = RTTY/PSK only; FT8/FT4 NOT allowed
- bands: [80, 40, 20, 15, 10, 6]
- warc_excluded: yes
- period: mid-March Saturday, ~1400Z Sat to 0200Z Sun (12h core, optional Sunday extension); 2026 = 1400Z Mar 14 to 0200Z Mar 15
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RS(T)+OK county ; rcvd=RS(T)+OK county   # OK station
- exchange_out_area: sent=RS(T)+(state/province/DXCC prefix) ; rcvd=RS(T)+OK county
- works_for_points_in_area:  everyone (OK works anyone)
- works_for_points_out_area: OK stations only
- points: 2 pts/phone QSO; 3 pts/CW QSO; 3 pts/digital QSO
- multipliers_in_area:  US states (50) + Canadian provinces + DXCC entities; counted once (not per band)
- multipliers_out_area: OK counties (max 77); counted once (not per band)
- dupe: per_band_mode  (once per band per mode: CW / digital / phone each separate)
- serial: none   # NOTE: current k5cm rules use RS(T)+county; some older/mirror versions used a serial (QSO) number — resolve against the current 2025/2026 PDF
- cabrillo_name: unknown (not confirmed; likely "OK-QSO-PARTY")
- notes: Digital restricted to RTTY/PSK — FT8/FT4 explicitly excluded. Bonus: 100 pts for at least one W0-type bonus-station QSO (see current rules). 160m NOT in current band list per WA7BNM (older versions listed 160m) — verify. Power: SOHP >100W, SOLP <=100W, QRP <=5W. Partial because current official PDF could not be fetched directly.

---

## Pennsylvania QSO Party  [id: qp-pennsylvania]
- wa7bnm_ref: 153
- rules_url: https://paqso.org/files/PAQSO_Rules.pdf  (2025 rules, rev 08/19/25)
- confidence: verified
- modes: [CW, PHONE]   # DIGITAL EXPLICITLY REMOVED (rules change log: "Removed digital modes"); Phone = SSB/FM/AM, any voice mode
- bands: [160, 80, 40, 20, 15, 10, 6, 2, VHF/UHF]
- warc_excluded: yes   # 12/17/30/60m not permitted; repeaters/satellites/VoIP not permitted
- period: 2nd Saturday of October, two sessions; 2025 = 1600Z Oct 11 to 0400Z Oct 12, and 1300Z-2200Z Oct 12
- role_distinction: yes
- exchange_in_area:  sent=serial# + PA county ; rcvd=serial# + PA county   # PA station
- exchange_out_area: sent=serial# + ARRL/RAC section (or "DX") ; rcvd=serial# + PA county
- exchange_all:      n/a
- works_for_points_in_area:  everyone (PA works PA + US + Canada + world)
- works_for_points_out_area: PA stations (non-PA try to work as many PA stations as possible)
- points: CW = 2 pts/QSO; Phone = 1 pt/QSO. (Multiple voice modes same band count once.)
- multipliers_in_area:  ARRL Sections + Canadian (RAC) Sections + PA Counties + 1 DX; each counts ONCE (NOT per band)
- multipliers_out_area: 67 PA Counties; each counts ONCE (NOT per band)
- dupe: per_band_mode   # "Work stations once per band and mode"; rovers/mobiles again when they change counties
- serial: all_band   # sequential serial; multi-stations may send serial by band; may be sent out of order
- cabrillo_name: unknown (not stated in rules; commonly "PAQP"/"PA-QSO-PARTY" in loggers — verify)
- notes: NON-PA stations send ARRL/RAC SECTION, not a state abbreviation — important distinction vs other QSO parties. QRP multiplier: QSO points x2 (only for QRP-specific or unspecified-power divisions). PA Mobile/Rover bonus: 500 pts per PA county with >=10 QSOs. Bonus station (2025 N3XF) = 200 pts/QSO + possible county mult. Power: HP >100W, LP >5-100W, QRP <=5W. Cabrillo 3.0 required.

---

## South Carolina QSO Party  [id: qp-south-carolina]
- wa7bnm_ref: 123
- rules_url: https://scqso.com/wp-content/uploads/2026/02/SCQPRULES2024_0126.pdf  (rev 2.2.26)
- confidence: verified
- modes: [PHONE, CW, DIGITAL, MIXED]   # Digital = RTTY/PSK/FT8/FT4/other (all digital equivalent for dupes)
- bands: [160, 80, 40, 20, 15, 10, 6, 2]
- warc_excluded: yes   # "160, 80, 40, 20, 15, 10, 6 and 2 meter bands only"
- period: 4th Saturday of February, 1500Z Sat to 0159Z Sun (11h); 2026 = Feb 28 1500Z
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+SC county abbr ; rcvd=RS(T)+SC county   # SC station
- exchange_out_area: sent=RS(T)+state (or province, or "DX") ; rcvd=RS(T)+SC county
- exchange_all:      n/a
- works_for_points_in_area:  everyone (SC works all stations)
- works_for_points_out_area: SC stations only (non-SC may NOT count non-SC or DX contacts)
- points: SC station: 2 pts per QSO with another SC station; 4 pts per QSO with an out-of-SC station (same for phone and CW/digital). Non-SC station: 2 pts per QSO with an SC station (phone or CW/digital).
- multipliers_in_area:  SC counties + each US state (incl SC and DC) + each Canadian province/territory; ONCE PER MODE PER BAND. (DX = points only, no mult.)
- multipliers_out_area: SC counties; ONCE PER MODE PER BAND
- dupe: per_band_mode   # once per mode per band; all digital submodes are one "mode" for duping
- serial: none
- cabrillo_name: SC-QSO-PARTY   # confirmed from rules Cabrillo example
- notes: SC is one of the few in this batch that fully allows FT8/FT4. DC is a multiplier; PR/USVI/Guam/US territories count as DX. Bonus stations: W4CAE 350, WW4SF 250, K4YTZ 250 (once per band per mode). Final score = QSO pts x mults + bonus pts. Power: QRP <=5W, Low <=100W, High >100W. Expedition category = SC stations only.

---

## South Dakota QSO Party  [id: qp-south-dakota]
- wa7bnm_ref: 492
- rules_url: https://pdarc.org/sd-qso-party
- confidence: verified
- modes: [CW, PHONE]   # "NO DIGITAL MODES" — digital explicitly excluded (no RTTY/FT8/etc.)
- bands: [160, 80, 40, 20, 15, 10, 6, 2, 1.25, 0.70]   # "160m thru 70cm (no WARC bands)"
- warc_excluded: yes
- period: 2nd full weekend of October, 1800Z Sat to 1800Z Sun (both days 1800Z); 2026 = Oct 10-11
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+SD county ; rcvd=RS(T)+SD county   # SD station
- exchange_out_area: sent=RS(T)+(state/province/DXCC country) ; rcvd=RS(T)+SD county
- exchange_all:      n/a
- works_for_points_in_area:  everyone (SD works anyone)
- works_for_points_out_area: SD stations only
- points: 1 pt/phone QSO; 2 pts/CW QSO
- multipliers_in_area:  SD counties + US states + provinces + DXCC countries; each once only
- multipliers_out_area: SD counties; each once only
- dupe: per_band_mode   # "once per mode per band" (repeater contacts excluded/not allowed)
- serial: none
- cabrillo_name: unknown (N1MM references "QSOP SD"; not confirmed in official text)
- notes: No digital modes at all. Includes VHF/UHF up to 70cm. Bonus: 100 pts for a QSO with W0OJY. Power: High >150W, Low <=150W, QRP <=5W.

---

## Tennessee QSO Party  [id: qp-tennessee]
- wa7bnm_ref: 115
- rules_url: https://tnqp.org/rules/  (JS-rendered PDF embed; could not fetch directly — cross-checked SM3CER mirror http://sk3bg.se/contest/tnqp.htm + WA7BNM + web search)
- confidence: partial
- modes: [CW, PHONE, DIGITAL]   # digital = any digital mode supporting the exchange; FT8 status not explicitly confirmed for current year
- bands: [160, 80, 40, 20, 15, 10, 6, VHF/UHF]   # WA7BNM "All except WARC"; older mirror listed 160-10 + VHF (50/144/223/446)
- warc_excluded: yes
- period: 1st Sunday of September, ~1700Z/1800Z Sun to 0100Z/0300Z Mon (extended to 10h in 2024); 2026 = Sep 6 1700Z to Sep 7 0300Z
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+TN county ; rcvd=RS(T)+TN county   # TN station
- exchange_out_area: sent=RS(T)+(state/province/DXCC country) ; rcvd=RS(T)+TN county
- exchange_all:      n/a
- works_for_points_in_area:  everyone (TN works anyone)
- works_for_points_out_area: TN stations only
- points: AMBIGUOUS — WA7BNM current summary says 3 pts/QSO (flat); older mirror rules say 2 pts/phone, 3 pts/CW+digital on HF (and 4/6 on VHF+). Verify against current PDF. Bonus QSO points exist (see rules).
- multipliers_in_area:  TN counties + US states + provinces + DXCC countries; per band (WA7BNM: "once per band")
- multipliers_out_area: TN counties (max 95); per band
- dupe: per_band_mode   # once per band/mode; mobiles again when they change counties
- serial: none
- cabrillo_name: unknown (not confirmed; commonly "TN-QSO-PARTY")
- notes: Hosted by Tennessee Contest Group; Rover category added 2024. Points structure and exact band set (6m/160m/VHF) need confirmation from the current tnqp.org PDF — the reachable mirror was a stale (2004) copy. Partial for that reason.

---

## Texas QSO Party  [id: qp-texas]
- wa7bnm_ref: 133
- rules_url: https://www.txqp.net/index.php/rules/operating-rules
- confidence: verified
- modes: [PHONE, CW, DIGITAL]   # "all modes ... except CW Only / Phone Only categories"; all digital submodes unified for duping
- bands: [160, 80, 40, 20, 15, 10, 6, 2, VHF/UHF]   # "all bands except 60, 30, 17, 12m"; VHF 50.200 / 144.200
- warc_excluded: yes   # plus 60m excluded
- period: 3rd weekend (Sat+Sun) of September; Sat 1400Z to Sun 0200Z, then Sun 1400Z-2000Z; 2026 = Sep 19-20
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+TX county (TQP abbr) ; rcvd=RS(T)+TX county   # TX station
- exchange_out_area: sent=RS(T)+(state/province/DXCC country/maritime region) ; rcvd=RS(T)+TX county
- exchange_all:      n/a
- works_for_points_in_area:  everyone (TX works anyone)
- works_for_points_out_area: TX stations only
- points: 2 pts/phone QSO; 3 pts/CW QSO; 3 pts/digital QSO
- multipliers_in_area:  US states (excl TX) + TX counties + Canadian provinces + DXCC countries (excl USA/Canada/AK/HI); each once regardless of mode
- multipliers_out_area: TX counties (max 254); each once regardless of mode
- dupe: per_band_mode   # once per band per mode; multipliers count once regardless of mode
- serial: none
- cabrillo_name: unknown (not stated; commonly "TX-QSO-PARTY"/"TQP" — verify)
- notes: Non-TX bonus: 500 pts per TX mobile worked in 5 different counties (any band/mode). Power (TX): LP <=150W, HP >150W, QRP <=5W CW / <=10W phone. Digital unified for dupes (an FT8 + RTTY on same band/station = dupe).

---

## Virginia QSO Party  [id: qp-virginia]
- wa7bnm_ref: 302
- rules_url: https://www.qsl.net/sterling/VA_QSO_Party/2026_VQP/2026_VAQP_Rules.htm
- confidence: verified
- modes: [PHONE, CW, DIGITAL, MIXED]   # digital = RTTY/PSK31/etc. (FT8 falls under digital, not separately restricted)
- bands: [160, 80, 40, 20, 15, 10, 6, 2, 1.25, 0.70]   # "160m and up, no WARC"; VHF 50/144/223/446 MHz
- warc_excluded: yes
- period: 3rd weekend of March; Sat 1400Z to Sun 0400Z, then Sun 1200Z-2400Z; 2026 = Mar 21-22
- role_distinction: yes
- exchange_in_area:  sent=serial# + VA county/independent-city ; rcvd=serial# + VA county/city   # VA station
- exchange_out_area: sent=serial# + (state/province/"DX") ; rcvd=serial# + VA county/city
- exchange_all:      n/a
- works_for_points_in_area:  everyone (VA works anyone)
- works_for_points_out_area: VA stations only
- points: 1 pt/phone QSO; 2 pts/CW QSO; 2 pts/digital QSO; 3 pts per QSO with a VA Mobile/Expedition/Rover
- multipliers_in_area:  VA counties/cities + US states (excl VA) + Canadian provinces + DXCC entities; counted once per band/mode
- multipliers_out_area: VA's 95 counties + 38 independent cities; counted once per band/mode
- dupe: per_band_mode   # fixed stations once per band/mode; mobiles per county/city
- serial: all_band   # QSO (serial) number, not RST
- cabrillo_name: unknown (not stated; commonly "VA-QSO-PARTY")
- notes: Uses SERIAL number + QTH (no RST). Bonus: 50 pts (one-time) per different VaQP Bonus Station worked. Power: High >150W, Low <=150W, QRP <=5W. "No rules changed for 2026."

---

## Washington State Salmon Run  [id: qp-washington-salmon-run]
- wa7bnm_ref: 126
- rules_url: https://salmonrun.wwdxc.org/rules/
- confidence: verified
- modes: [CW, PHONE]   # digital PROHIBITED — "cannot accept WSJT modes (FT-8/FT-4)"; no RTTY
- bands: [160, 80, 40, 20, 15, 10, 6]   # "no 60, 30, 17, 12m"
- warc_excluded: yes
- period: 3rd full weekend of September; Sat 1600Z to Sun 0700Z, then Sun 1600Z-2400Z; 2026 = Sep 19-20
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+WA county ; rcvd=RS(T)+WA county   # WA station
- exchange_out_area: sent=RS(T)+(state / Canadian mult / DXCC prefix) ; rcvd=RS(T)+WA county
- exchange_all:      n/a
- works_for_points_in_area:  everyone (WA works any state/VE/DXCC + WA counties)
- works_for_points_out_area: WA stations only (work only WA for points + county mult)
- points: 2 pts/phone QSO; 3 pts/CW QSO
- multipliers_in_area:  39 WA counties + 49 US states (excl WA) + 13 Canadian provinces/territories + up to 10 DXCC entities; each once regardless of mode/band
- multipliers_out_area: 39 WA counties; each once regardless of mode/band
- dupe: per_band_mode   # QSO counts once per band/mode; multipliers once overall
- serial: none
- cabrillo_name: unknown (not stated; commonly "WA-SALMON-RUN")
- notes: Digital fully excluded (CW/Phone only). Bonus: 500 pts per W7DX QSO once per mode (max 1000). Categories include Eastern-WA vs Out-of-State etc. Power: HP >100W, LP, QRP <=5W (SO classes).

---

## West Virginia QSO Party  [id: qp-west-virginia]
- wa7bnm_ref: 49
- rules_url: https://www.qsl.net/wvqp/  (2025 rules PDF embed; direct PDF 404'd during research — cross-checked SM3CER mirror http://sk3bg.se/contest/wvqp.htm + WA7BNM + web search)
- confidence: partial
- modes: [CW, SSB, DIGITAL]   # current adds Digital; older mirror was CW/SSB/Mixed only — confirm digital point value
- bands: [80, 40, 20, 15, 10]   # NO 160m, NO 6m, no WARC (per WA7BNM current + mirror "80m through 10m, excl WARC and VHF")
- warc_excluded: yes
- period: mid-to-late June, 1600Z Sat to 0400Z Sun; 2025 = Jun 21 1600Z to Jun 22 0400Z
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+WV county ; rcvd=RS(T)+WV county   # WV station
- exchange_out_area: sent=RS(T)+(state/province/DXCC country) ; rcvd=RS(T)+WV county
- exchange_all:      n/a
- works_for_points_in_area:  everyone (WV works anyone)
- works_for_points_out_area: WV stations only
- points: 1 pt/phone QSO; 2 pts/CW QSO; 2 pts/digital QSO. (Contacts with WV mobiles: higher — older mirror gave 3 CW / 2 SSB for WV mobiles; confirm current.)
- multipliers_in_area:  WV counties + US states + Canadian provinces + DXCC countries; once regardless of band/mode
- multipliers_out_area: WV counties (max 55); once regardless of band/mode
- dupe: per_band_mode   # once per band and per mode; county-line stations = 2 QSOs + 2 mults
- serial: none
- cabrillo_name: unknown (not confirmed; commonly "WV-QSO-PARTY")
- notes: Bonus: 100 pts per W8WVA QSO per band/mode; 100 pts per county activated (mobiles). Power: QRP <=5W CW / <=10W SSB, LP <=100W, HP >100W. Partial because the 2025 official PDF could not be fetched (only a stale 2008 mirror + WA7BNM/search were reachable); confirm current digital points and mobile-QSO points.

---

## Wisconsin QSO Party  [id: qp-wisconsin]
- wa7bnm_ref: 330
- rules_url: http://www.warac.org/wqp/wiqp_rules.htm  (2026 rules)
- confidence: verified
- modes: [CW, PHONE, DIGITAL]   # digital = RTTY/PSK/Olivia/Feld-Hell; "FT8/FT4 QSOs are NOT accepted"
- bands: [160, 80, 40, 20, 15, 10, 6, 2, 1.25, 0.70]   # "all amateur bands not prohibited for contesting"; suggested freqs list HF 160-10 + VHF/UHF
- warc_excluded: yes   # WARC not used (not in suggested frequencies; standard contest exclusion)
- period: 2nd or 3rd Sunday-ish of March, 1800Z to 0100Z next day; 2026 = Mar 15 1800Z to Mar 16 0100Z
- role_distinction: yes
- exchange_in_area:  sent=RS(T)+WI county ; rcvd=RS(T)+WI county   # WI station
- exchange_out_area: sent=RS(T)+(state/province/DXCC country) ; rcvd=RS(T)+WI county
- exchange_all:      n/a
- works_for_points_in_area:  everyone (WI works anywhere)
- works_for_points_out_area: WI stations only (stations anywhere contact WI stations for points)
- points: 1 pt/phone QSO; 2 pts/CW QSO; 2 pts/digital QSO
- multipliers_in_area:  WI counties (max 72) + US states (max 50) + Canadian provinces (max 13)
- multipliers_out_area: WI counties (max 72)
- dupe: per_band_mode   # once per mode on each band; mobiles/portables once per mode per county
- serial: none
- cabrillo_name: unknown (not stated; commonly "WI-QSO-PARTY")
- notes: FT8/FT4 explicitly excluded (other digital OK). Power multipliers: QRP (<5W) x2, Low (5-100W) x1.5, High (>100W) x1. Mobile bonus 500 pts/county (min 12 QSOs); 100 pts/band/mode for W9FK contacts below 50 MHz.

---

## 7th Call Area QSO Party (7QP)  [id: qp-7qp]
- wa7bnm_ref: 404
- rules_url: http://7qp.org/  (and http://www.ws7n.net/7QP/new/Page.asp?content=rules — both hosts returned connection reset / HTTP 500 during research; reconstructed from WA7BNM detail page + web search of the official rules)
- confidence: partial
- modes: [CW, PHONE, DIGITAL]   # "no FT8"; other digital (RTTY/PSK) allowed
- bands: [160, 80, 40, 20, 15, 10]   # NO 6m, no WARC
- warc_excluded: yes
- period: 1st Saturday of May, 1300Z Sat to 0700Z Sun; 2025 = May 3 1300Z to May 4 0700Z
- role_distinction: yes   # in-area = the 7 states of US call district 7: WA, OR, ID, MT, WY, NV, UT
- exchange_in_area:  sent=RS(T) + 5-letter code (county + state, e.g. CCCSS) ; rcvd=RS(T)+ county/state code   # 7th-area station
- exchange_out_area: sent=RS(T)+(state/province/"DX") ; rcvd=RS(T)+7th-area county+state code
- exchange_all:      n/a
- works_for_points_in_area:  everyone (7th-area stations work everyone, including other 7th-area stations)
- works_for_points_out_area: 7th-area stations only
- points: 2 pts/SSB QSO; 3 pts/CW QSO; 4 pts/digital QSO
- multipliers_in_area:  each US state + each Canadian province (once) + each DXCC entity (max 10) + (7th-area counties, per concurrent-party convention); per the WA7BNM summary "each state or province once" and "each DXCC once (max 10)"
- multipliers_out_area: each 7th-area county (county+state code); once
- dupe: per_band_mode   # once per band/mode; 7th-area mobiles worked again as they enter new counties
- serial: none
- cabrillo_name: unknown (not confirmed; commonly "7QP")
- notes: Runs concurrently with several state parties (INQP, NEQP, etc.); 7th-area mobiles re-workable per new county. FT8 excluded; RTTY/PSK OK. Partial because neither 7qp.org nor the ws7n.net mirror could be fetched — confirm exact in-area multiplier set (whether 7th-area stations also count 7th-area counties/states-of-area) and power categories against the live rules page.

---

## New England QSO Party (NEQP)  [id: qp-neqp]
- wa7bnm_ref: 10
- rules_url: https://neqp.org/rules/
- confidence: verified
- modes: [CW, PHONE, DIGITAL]   # digital scored as CW; phone = SSB
- bands: [80, 40, 20, 15, 10]   # NO 160m, NO 6m/VHF, no WARC
- warc_excluded: yes
- period: 1st full weekend of May; Sat 2000Z to Sun 0500Z, then Sun 1300Z-2400Z; 2026 = May 2-3
- role_distinction: yes   # "in-area" side = the 6 New England states: CT, ME, MA, NH, RI, VT
- exchange_in_area:  sent=RS(T)+state+county ; rcvd=RS(T)+state+county   # NE station sends BOTH its state and county
- exchange_out_area: sent=RS(T)+(state/province, or "DX") ; rcvd=RS(T)+NE state+county
- exchange_all:      n/a
- works_for_points_in_area:  everyone (New England stations work anyone, including each other)
- works_for_points_out_area: New England stations only (work NE stations once per band/mode)
- points: 1 pt/phone QSO; 2 pts/CW QSO (digital included in the CW category = 2 pts)
- multipliers_in_area:  US states (50) + Canadian provinces (14) + DXCC countries (excl USA); NE stations also copy county for other-NE QSOs
- multipliers_out_area: 68 New England counties (CT 9, MA 14, ME 16, NH 10, RI 5, VT 14)
- dupe: per_band_mode   # once per band/mode; mobiles changing counties = new station; no cross-mode/cross-band QSOs
- serial: none
- cabrillo_name: unknown (not stated; commonly "NEQP"/"NE-QSO-PARTY")
- notes: Multi-state "in-area" party — the 6 NE states are collectively the in-area side; NE stations work each other for points AND must exchange county. Digital counts as CW (2 pts). Power: HP, LP <=150W, QRP <=5W, + multi-op variants.

---

## Cross-cutting notes for the engine

- WARC (30/17/12m) excluded in ALL 12. 60m also excluded where mentioned (TX, WA Salmon Run).
- 6m included: OK, SC, SD, TX, VA, WA Salmon Run, WI, (TN likely). 6m EXCLUDED: PA has 6m yes; WV NO 6m; NEQP NO 6m; 7QP NO 6m.
- 160m EXCLUDED: OK (current), WV, NEQP, 7QP. 160m INCLUDED: PA, SC, SD, TX, VA, WA Salmon Run, WI, (TN "all except WARC").
- Digital (incl FT8/FT4) allowed: SC only (fully). Digital allowed but FT8/FT4 EXCLUDED: OK (RTTY/PSK only), TX (unified digital), WI (RTTY/PSK/Olivia/Hell), 7QP (no FT8). Digital allowed, FT8 status unconfirmed: TN, VA, WV. NO digital at all: PA, SD, WA Salmon Run.
- Serial number (not RST): PA, VA. All others use RS(T).
- Role pattern (all 12): in-area station works everyone; out-of-area station works in-area stations only. In-area sends county (NEQP: state+county; 7QP: county+state code); out-of-area sends state/province/DX.
- Multi-state in-area parties: 7QP (7 states: WA, OR, ID, MT, WY, NV, UT) and NEQP (6 states: CT, ME, MA, NH, RI, VT). For both, in-area stations work each other for points; their multipliers are states/provinces/DXCC while out-of-area stations' multipliers are the in-area counties.

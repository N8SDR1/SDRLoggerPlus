# Batch 3 — DX / Regional Contests

Summary: 18 blocks. **15 verified** (official sponsor rules read directly),
**3 partial** (rac — official PDF unreadable, reconstructed from RAC-sourced
search summary + WA7BNM; africa-dx — read a 2016 archived ruleset, current
edition not directly confirmed; eu-dx — 2026 rules changed and only a
model-summarized PDF was available for the multiplier). jota is an activity, not
a scored contest (verified as such).

Cross-cutting notes captured per the brief: the All Asian **age** exchange, the
WAE **QTC traffic** system, and the IOTA **island-reference** multipliers are
detailed in their blocks. Continent/entity-based point structures are given
exactly. Where a serial is "continuous vs per-band" was not explicit in the
source, it is flagged `verify`.

---

## All Asian DX Contest, CW  [id: all-asian-cw]
- wa7bnm_ref: 47
- rules_url: https://www.jarl.org/English/4_Library/A-4-3_Contests/aadx_eng.html
- confidence: verified
- modes: [CW]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: 3rd full weekend of June, 24h (0000Z Sat – 2400Z Sun); 2026 = Jun 20–21
- role_distinction: yes   # Asia vs non-Asia (who you may work + points + mults)
- exchange_all:      n/a
- exchange_in_area:  sent=RST + operator age (2-digit) ; rcvd=RST + age
- exchange_out_area: sent=RST + operator age (2-digit) ; rcvd=RST + age
  # Age exchange is the signature of this contest. Multi-op sends AVERAGE age of ops.
  # YL / operators declining to give age may send "00" (some years "01").
- works_for_points_in_area:  everyone (Asian stations work Asian + non-Asian); same-DXCC-entity QSOs score 0
- works_for_points_out_area: Asian stations only (non-Asian stations may only work Asia)
- points: |
    Asian station:  vs Asian QSO  -> 160m=3, 80m=2, 10m=2, other(40/20/15)=1
                    vs non-Asian  -> 160m=9, 80m=6, 10m=6, other=3
    Non-Asian station: vs Asian    -> 160m=3, 80m=2, 10m=2, other=1
    (Note the asymmetry: the same Asia<->DX QSO is worth 9/6/6/3 to the Asian
    side and 3/2/2/1 to the non-Asian side.)
- multipliers_in_area:  Asian station: different DXCC entities worked per band
- multipliers_out_area: non-Asian station: different Asian prefixes (WPX rules) worked per band
- dupe: per_band
- serial: none   # age is exchanged, not a serial
- cabrillo_name: ALL-ASIAN-DX-CW   # convention; not stated in the rules text
- notes: Categories Single-Op (band or all-band, HP/LP) and Multi-Op. Multi-op sends average age. Score = sum(QSO pts) x sum(mults), summed per band for all-band.

---

## All Asian DX Contest, Phone  [id: all-asian-ssb]
- wa7bnm_ref: 102
- rules_url: https://www.jarl.org/English/4_Library/A-4-3_Contests/aadx_eng.html
- confidence: verified
- modes: [SSB]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: 1st full weekend of September, 24h (0000Z Sat – 2400Z Sun); 2026 = Sep 5–6
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RS + operator age (2-digit) ; rcvd=RS + age
- exchange_out_area: sent=RS + operator age (2-digit) ; rcvd=RS + age
- works_for_points_in_area:  everyone; same-DXCC-entity QSOs score 0
- works_for_points_out_area: Asian stations only
- points: |
    Asian station:  vs Asian -> 160m=3,80m=2,10m=2,other=1 ; vs non-Asian -> 160m=9,80m=6,10m=6,other=3
    Non-Asian station: vs Asian -> 160m=3,80m=2,10m=2,other=1
- multipliers_in_area:  Asian station: DXCC entities per band
- multipliers_out_area: non-Asian station: Asian prefixes (WPX) per band
- dupe: per_band
- serial: none
- cabrillo_name: ALL-ASIAN-DX-PHONE   # convention; not stated in rules
- notes: Identical ruleset to the CW leg; only the mode and date differ.

---

## WAE DX Contest, CW  [id: wae-cw]
- wa7bnm_ref: 85
- rules_url: https://www.darc.de/der-club/referate/conteste/wae-dx-contest/en/wae-rules/
- confidence: verified
- modes: [CW]
- bands: [80, 40, 20, 15, 10]
- warc_excluded: yes    # also NO 160m in WAE CW/SSB
- period: 2nd full weekend of August, 48h window 0000Z Sat – 2359Z Sun (single-op works max 36 of 48h); 2026 = Aug 8–9
- role_distinction: yes   # Europe vs non-Europe (DX); QTC direction depends on it
- exchange_all:      n/a
- exchange_in_area:  sent=RST + serial (from 001, per band) ; rcvd=RST + serial   # European station
- exchange_out_area: sent=RST + serial (from 001, per band) ; rcvd=RST + serial   # non-European (DX) station
- works_for_points_in_area:  non-European (DX) stations only   # EU works only DX
- works_for_points_out_area: European stations only            # DX works only EU
- points: 1 point per valid QSO (all bands equal). Extra points come from QTCs.
- multipliers_in_area:  European station: each non-European DXCC entity per band, PLUS numbered call-area sub-mults for W, VE, VK, ZL, ZS, JA, BY, PY, RA8/9/0
- multipliers_out_area: non-European station: each WAE country (WAE list, ~51 entities) per band
- multiplier_band_weight: mult total is band-weighted — 80m x4, 40m x3, 20m/15m/10m x2 (summed across bands)
- dupe: per_band
- serial: per_band   # serials restart at 001 on each band
- cabrillo_name: WAEDC-CW
- notes: |
    QTC TRAFFIC SYSTEM (the defining feature): a QTC is a report of a previously
    logged QSO (time, callsign, serial) that a DX station passes to a European
    station. In CW/SSB, QTCs flow ONE WAY: DX -> Europe. Each correctly
    transferred QTC = 1 point for BOTH sender and receiver. A given station pair
    may exchange at most 10 QTCs total; sent in numbered series "series/count"
    (e.g. 3/7). A QSO may not be reported back to the station that was part of it.
    Final score = (total QSOs + total QTCs, all bands) x (band-weighted mult sum).
    Power cats: SO-LOW (<=100W), SO-HIGH (>100W), Multi-Op.

---

## WAE DX Contest, SSB  [id: wae-ssb]
- wa7bnm_ref: (WA7BNM lists it; ref not separately captured)
- rules_url: https://www.darc.de/der-club/referate/conteste/wae-dx-contest/en/wae-rules/
- confidence: verified
- modes: [SSB]
- bands: [80, 40, 20, 15, 10]
- warc_excluded: yes    # no 160m
- period: 2nd full weekend of September, 48h window (SO max 36h); 2026 = Sep 12–13
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RS + serial (from 001, per band) ; rcvd=RS + serial   # European
- exchange_out_area: sent=RS + serial (from 001, per band) ; rcvd=RS + serial   # DX
- works_for_points_in_area:  non-European (DX) stations only
- works_for_points_out_area: European stations only
- points: 1 point per QSO; QTCs add points.
- multipliers_in_area:  European: non-EU DXCC entities per band + call-area sub-mults (W,VE,VK,ZL,ZS,JA,BY,PY,RA8/9/0)
- multipliers_out_area: non-European: WAE countries per band
- multiplier_band_weight: 80m x4, 40m x3, 20/15/10m x2
- dupe: per_band
- serial: per_band
- cabrillo_name: WAEDC-SSB
- notes: QTCs again flow DX -> Europe only; max 10 per pair; +1 pt each side. Same scoring engine as WAE CW.

---

## WAE DX Contest, RTTY  [id: wae-rtty]
- wa7bnm_ref: 183
- rules_url: https://www.darc.de/der-club/referate/conteste/wae-dx-contest/en/wae-rules/
- confidence: verified
- modes: [RTTY]
- bands: [80, 40, 20, 15, 10]
- warc_excluded: yes    # no 160m
- period: 2nd full weekend of November, 48h window (SO max 36h); 2026 = Nov 14–15
- role_distinction: no    # RTTY EXCEPTION: everybody works everybody (no continent lock)
- exchange_all:      sent=RST + serial (from 001, per band) ; rcvd=RST + serial
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  n/a
- works_for_points_out_area: n/a
- points: 1 point per QSO (any continent counts in RTTY); QTCs add points.
- multipliers_in_area:  n/a
- multipliers_out_area: n/a
- multipliers_all: DXCC entities + call-area sub-mults per band (as WAE list); band-weighted 80m x4, 40m x3, 20/15/10m x2
- dupe: per_band
- serial: per_band
- cabrillo_name: WAEDC-RTTY
- notes: |
    Key RTTY differences vs CW/SSB: no continental restriction — everyone can
    work everyone. QTCs may be exchanged in BOTH directions but ONLY between
    stations on DIFFERENT continents; the 10-QTC-per-pair cap is on the aggregate
    of sent+received. All other scoring identical to WAE CW/SSB.

---

## Oceania DX Contest, CW  [id: oceania-cw]
- wa7bnm_ref: 151
- rules_url: https://www.oceaniadxcontest.com/rules  (PDF: ocdx rules, oceaniadxcontest.com)
- confidence: verified
- modes: [CW]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: 2nd full weekend of October, 24h from 0600Z Sat – 0600Z Sun; 2026 CW = Oct 10–11
- role_distinction: yes   # only who-you-may-work differs; points/mults identical both sides
- exchange_all:      n/a
- exchange_in_area:  sent=RST + serial ; rcvd=RST + serial   # Oceania station
- exchange_out_area: sent=RST + serial ; rcvd=RST + serial   # non-Oceania station
- works_for_points_in_area:  everyone (Oceania stations work anyone)
- works_for_points_out_area: Oceania stations only (stations outside Oceania may only work Oceania)
- points: per band, SAME for both sides — 160m=20, 80m=10, 40m=5, 20m=1, 15m=2, 10m=3
- multipliers_in_area:  different prefixes (ARRL/DXCC prefix list, WPX-style) worked per band
- multipliers_out_area: different prefixes worked per band (same rule)
- dupe: per_band
- serial: all_band (running serial; verify — rules say serial from 001, continuity across bands not explicit)
- cabrillo_name: OCEANIA-DX-CW
- notes: Score = sum(contact points, all bands) x sum(prefix mults, all bands; each prefix once per band). Power cats High/Low/QRP; also VK/ZL sections. Note the unusual point ladder that FAVORS low bands (160m worth 20).

---

## Oceania DX Contest, Phone  [id: oceania-ssb]
- wa7bnm_ref: 142
- rules_url: https://www.oceaniadxcontest.com/rules
- confidence: verified
- modes: [SSB]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: 1st full weekend of October, 24h 0600Z Sat – 0600Z Sun; 2026 Phone = Oct 3–4
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RS + serial ; rcvd=RS + serial
- exchange_out_area: sent=RS + serial ; rcvd=RS + serial
- works_for_points_in_area:  everyone
- works_for_points_out_area: Oceania stations only
- points: per band — 160m=20, 80m=10, 40m=5, 20m=1, 15m=2, 10m=3
- multipliers_in_area:  prefixes per band
- multipliers_out_area: prefixes per band
- dupe: per_band
- serial: all_band (verify)
- cabrillo_name: OCEANIA-DX-SSB
- notes: Phone leg runs one weekend BEFORE the CW leg. Otherwise identical scoring to Oceania CW.

---

## JIDX CW Contest  [id: jidx-cw]
- wa7bnm_ref: 314
- rules_url: http://www.jidx.org/jidxrule-e.html
- confidence: verified
- modes: [CW]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: 2nd full weekend of April, 0700Z Sat – 1300Z Sun (30h window); 2026 = Apr 11–12
- role_distinction: yes   # Japan vs non-Japan: different exchange, different mults, who-you-work
- exchange_all:      n/a
- exchange_in_area:  sent=RST + prefecture number (01–50) ; rcvd=RST + CQ zone   # Japanese station
- exchange_out_area: sent=RST + CQ zone number ; rcvd=RST + prefecture number    # non-Japanese station
- works_for_points_in_area:  worldwide DX (Japanese stations work the whole world); JA-JA = 0
- works_for_points_out_area: Japanese (JA) stations only
- points: per band, both sides — 160m=4, 80m=2, 10m=2, 40m/20m/15m=1
- multipliers_in_area:  Japanese station: different DXCC entities + CQ zones per band
- multipliers_out_area: non-Japanese station: different Japanese prefectures per band, max 50 (47 prefectures + JD1 Ogasawara + JD1 Minami Torishima groups)
- dupe: per_band
- serial: none   # zone / prefecture exchanged, not a serial
- cabrillo_name: JIDX-CW   # convention; not stated in rules
- notes: Power cats High (>=100W) / Low (<=100W incl QRP). Low bands + 10m carry a point bonus (160m=4).

---

## JIDX Phone Contest  [id: jidx-ssb]
- wa7bnm_ref: 184
- rules_url: http://www.jidx.org/jidxrule-e.html
- confidence: verified
- modes: [SSB]
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: 2nd full weekend of November, 0700Z Sat – 1300Z Sun (30h window)
- role_distinction: yes
- exchange_all:      n/a
- exchange_in_area:  sent=RS + prefecture number (01–50) ; rcvd=RS + CQ zone
- exchange_out_area: sent=RS + CQ zone number ; rcvd=RS + prefecture number
- works_for_points_in_area:  worldwide DX; JA-JA = 0
- works_for_points_out_area: Japanese stations only
- points: 160m=4, 80m=2, 10m=2, 40m/20m/15m=1
- multipliers_in_area:  DXCC entities + CQ zones per band (JA station)
- multipliers_out_area: Japanese prefectures per band, max 50 (non-JA station)
- dupe: per_band
- serial: none
- cabrillo_name: JIDX-SSB   # convention
- notes: Same ruleset as JIDX CW; Phone leg in November.

---

## RAC Canada Day / Winter Contest  [id: rac]
- wa7bnm_ref: 60 (Canada Day) / 205 (Winter)
- rules_url: https://www.rac.ca/contesting-results/  (PDF: RAC-Contest-Rules-<year>.pdf on rac.ca)
- confidence: partial   # official PDF would not render as text; values from RAC-sourced search summary + WA7BNM
- modes: [CW, SSB]   # CW and Phone count as separate QSOs on each band (effectively mixed)
- bands: [160, 80, 40, 20, 15, 10, 6, 2]   # note VHF 6m & 2m INCLUDED; WARC excluded
- warc_excluded: yes
- period: Canada Day = Jul 1, 0000Z–2359Z (24h). Winter = last Sat of December, 0000Z–2359Z (24h).
- role_distinction: yes   # Canadian (VE) vs non-Canadian: exchange differs
- exchange_all:      n/a
- exchange_in_area:  sent=RST + province/territory ; rcvd=RST + serial-or-prov   # Canadian (VE) station
- exchange_out_area: sent=RST + serial (from 001) ; rcvd=RST + province/territory  # non-Canadian station
- works_for_points_in_area:  everyone (Canadian stations work anyone)
- works_for_points_out_area: everyone (non-Canadians benefit from Canadian contacts; may work anyone)
- points: |
    Per QSO (same for both sides):
      contact with a station in Canada (VE) or VE0  = 10 points
      contact with a RAC official station (RAC suffix) = 20 points
      contact with any other station                = 2 points
- multipliers_in_area:  13 Canadian provinces/territories, counted once per band per mode
- multipliers_out_area: 13 Canadian provinces/territories, counted once per band per mode
  # 10 provinces + 3 territories: NB NS PE QC ON MB SK AB BC (VE1..VE7-ish) + NL, plus YT NT NU (verify exact abbr set used in Cabrillo)
- dupe: per_band_mode   # may work each station once per band on CW and once on Phone
- serial: all_band (running serial for non-VE; verify per-band vs continuous)
- cabrillo_name: RAC-CANADA-DAY  (Winter: RAC-WINTER)
- notes: |
    Multiplier count is 13 (provinces + territories) per band per mode. RAC HQ
    "official" stations (callsign suffix RAC) are worth 20 pts. Includes 6m and
    2m. PARTIAL because the authoritative rules PDF could not be parsed; point
    and multiplier values corroborated across RAC-sourced summary and WA7BNM but
    the exact province/territory abbreviation list and serial continuity should
    be re-verified against the current-year PDF.

---

## Russian DX Contest  [id: rdxc]
- wa7bnm_ref: 310
- rules_url: https://www.rdxc.org/rules_eng
- confidence: verified
- modes: [CW, SSB]   # mixed; single-mode and mixed categories exist
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: 3rd full weekend of March, 24h 1200Z Sat – 1200Z Sun; e.g. 2025 = Mar 15–16
- role_distinction: yes   # Russian vs non-Russian: exchange differs; everyone still works everyone
- exchange_all:      n/a
- exchange_in_area:  sent=RST + 2-letter oblast code ; rcvd=RST + serial-or-oblast   # Russian station
- exchange_out_area: sent=RST + serial (from 001) ; rcvd=RST + oblast-or-serial       # non-Russian station
- works_for_points_in_area:  everyone
- works_for_points_out_area: everyone
- points: |
    Per QSO (applies to all stations):
      QSO with a Russian (R) station           = 10 points
      QSO with own DXCC country                = 2 points
      QSO with different country, same continent = 3 points
      QSO with a different continent            = 5 points
    (A Russian station scores its own QSOs by the same 2/3/5 geography;
     Russia-to-Russia counts as own-country = 2.)
- multipliers_in_area:  each different oblast + each different DXCC country, per band
- multipliers_out_area: each different oblast + each different DXCC country, per band
- dupe: per_band_mode   # dupe = same station, same band AND same mode
- serial: all_band (continuous serial for non-R stations; verify)
- cabrillo_name: RDXC
- notes: ~2-letter oblast codes are the regional mult; DXCC countries are a second mult class. Power cats HP/LP/QRP with senior (age 75+) variants.

---

## ARI International DX Contest  [id: ari-dx]
- wa7bnm_ref: 9
- rules_url: https://www.qsl.net/contest_ari/DX_rul_ing_new.html  (also ari.it contest pages)
- confidence: verified
- modes: [CW, SSB, RTTY]   # single-mode CW/SSB/RTTY and Mixed categories
- bands: [80, 40, 20, 15, 10]   # RTTY limited to 80–10; 160m allowed for CW/SSB per one source but WA7BNM lists 80–10. WARC excluded. (verify 160m)
- warc_excluded: yes
- period: 1st full weekend of May, ~24h; 2026 = May 2–3. NOTE period-of-day discrepancy: WA7BNM/ari.it 2026 give 1200Z Sat – 1159Z Sun; the qsl.net ruleset text says 2000Z Sat – 1959Z Sun. Treat start time as the current-year official (1200Z) and verify.
- role_distinction: yes   # Italian vs non-Italian: exchange differs
- exchange_all:      n/a
- exchange_in_area:  sent=RST + 2-letter province code ; rcvd=RST + serial-or-province   # Italian (I/IS0/IT9) station
- exchange_out_area: sent=RST + serial (from 001, not restarting per band/mode) ; rcvd=RST + province-or-serial   # non-Italian
- works_for_points_in_area:  everyone EXCEPT no QSO between two Italian stations (per ari.it 2026; the qsl.net text conflicts and allows I-I — verify)
- works_for_points_out_area: everyone
- points: |
    QSO with an Italian station (I & IS0 & IT9) = 10 points
    QSO same continent                          = 1 point
    QSO different continent                     = 3 points
    QSO with own country                        = 0 points (mult credit only)
- multipliers_in_area:  each Italian province (110 total) + each DXCC country (except I/IS0/IT9), counted once per band
- multipliers_out_area: same — Italian provinces (110) + DXCC/WAE countries per band
- dupe: per_band_mode   # once per band on each of SSB/CW/RTTY; first QSO carries mult
- serial: all_band   # non-Italian serial does NOT restart per band/mode
- cabrillo_name: ARI-DX
- notes: 110 Italian provinces are the headline mult. Same-country QSO = 0 pts but still gives a mult. Period start-time and 160m eligibility are the two items to re-verify against the current official rules.

---

## Africa All-Mode International DX Contest  [id: africa-dx]
- wa7bnm_ref: (find — listed on WA7BNM; ref not separately captured)
- rules_url: (SARL) e.g. mysarl.org.za/contest-resources — read via archived ruleset http://africadxcontest.blogspot.com/2016/09/africa-all-modeinternational-dx-contest.html
- confidence: partial   # read a 2016 archived edition; current-year official page not directly confirmed
- modes: [CW, SSB, RTTY]   # single-mode or mixed; mixed may work same station once per mode per band
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes    # 12/17/30/60m explicitly barred
- period: typically a weekend in September, 24h continuous (2016 edition: 1200Z Sat – 1200Z Sun)
- role_distinction: no    # everyone works everyone; African entities simply worth more
- exchange_all:      sent=RS(T) + serial (from 001) ; rcvd=RS(T) + serial
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  n/a
- works_for_points_out_area: n/a
- points: QSO with an African (AF) DXCC entity = 10 points ; all other QSOs = 1 point
- multipliers_in_area:  n/a
- multipliers_out_area: n/a
- multipliers_all: each AF-DXCC entity, counted per band per mode (CW/SSB/RTTY tracked separately)
- dupe: per_band_mode
- serial: all_band (serial from 001; per-band continuity not explicit — verify)
- cabrillo_name: unknown   # not stated in the archived ruleset
- notes: Sponsor = South African Radio League (SARL). Power cats QRP (<=5W) / Low (<=100W) / High (<=1500W). RST not checked in log-checking. PARTIAL: confirm the current-year date, exact name ("Africa All-Mode Intl DX" vs "All Africa Intl DX"), and Cabrillo name against the live SARL page.

---

## European HF Championship  [id: eu-dx]
- wa7bnm_ref: 82
- rules_url: https://euhf.s5cc.eu/euhfc_rules/  (PDF: https://euhf.s5cc.eu/rules/euhfc_rules_latest.pdf)
- confidence: partial   # 2026 rules were revised; multiplier detail only via model-summarized PDF
- modes: [CW, SSB]   # single combined period, mixed CW+SSB
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: 1st Saturday of August, 12h, 1200Z–2359Z; 2026 = Aug 1
- role_distinction: no    # everyone works everyone
- exchange_all:      sent=RS(T) + 2-digit year first licensed ; rcvd=RS(T) + year
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  n/a
- works_for_points_out_area: n/a
- points: 1 point per valid QSO (no per-band or per-mode differentiation)
- multipliers_all: different "year first licensed" values per band (classic rule). A 2026 PDF summary also indicated DXCC-country-per-band may combine into the mult — VERIFY the exact 2026 multiplier definition. Mult counted once per band regardless of mode.
- dupe: per_band_mode   # one QSO per band per mode; but a station may be worked on both CW and SSB per band
- serial: none   # year first licensed is exchanged, not a serial
- cabrillo_name: EU-HF-CHAMPIONSHIP   # (a.k.a. EUHFC)
- notes: |
    Sponsor: Slovenia Contest Club (s5cc). Signature exchange = 2-digit year of
    first license (e.g. "88"). PARTIAL because the rules were changed for 2026
    and only a summarized PDF was available: re-verify whether the multiplier is
    (a) different years-first-licensed per band only, or (b) years combined with
    DXCC countries per band. Categories Single-Op only, with power tiers.

---

## RSGB IOTA Contest  [id: iota]
- wa7bnm_ref: (find — RSGB; ref not separately captured)
- rules_url: https://www.rsgbcc.org/hf/rules/2026/riota.shtml
- confidence: verified
- modes: [CW, SSB]
- bands: [80, 40, 20, 15, 10]   # 3.5/7/14/21/28 MHz; 160m and WARC EXCLUDED
- warc_excluded: yes
- period: last full weekend of July, 24h 1200Z Sat – 1200Z Sun; 2026 = Jul 25–26
- role_distinction: yes   # Island station vs World station: different exchange AND different points
- exchange_all:      n/a
- exchange_in_area:  sent=RS(T) + serial + IOTA reference (e.g. "599 378 EU-115") ; rcvd=RS(T)+serial(+IOTA)   # Island station
- exchange_out_area: sent=RS(T) + serial (no IOTA ref) ; rcvd=RS(T)+serial(+IOTA)                              # World (non-island) station
- works_for_points_in_area:  everyone (island stations work island + world)
- works_for_points_out_area: everyone (world stations work island + world)
- points: |
    Island station earns:  vs World station        = 5 points
                           vs Island (same IOTA ref) = 5 points
                           vs Island (other IOTA ref) = 15 points
    World station earns:   vs World station         = 2 points
                           vs Island station        = 15 points
- multipliers_in_area:  IOTA references — each different IOTA reference counted once per band on CW, plus once per band on SSB
- multipliers_out_area: same IOTA-reference multiplier (per band per mode)
- dupe: per_band_mode
- serial: all_band (running serial; verify per-band vs continuous)
- cabrillo_name: IOTA-CONTEST
- notes: |
    The island-reference (IOTA number, e.g. EU-005, NA-001) IS the multiplier and
    the headline of the contest. Multiplier is per band AND per mode (CW refs and
    SSB refs counted separately). Score = total QSO points x total IOTA-ref mults.
    Entry categories include World / Island, DXpedition, and power/operator tiers;
    single-transmitter within 500m radius.

---

## Stew Perry Topband Challenge  [id: stew-perry]
- wa7bnm_ref: 207
- rules_url: https://www.kkn.net/stew/stew_rules.html
- confidence: verified
- modes: [CW]
- bands: [160]   # 160m ONLY
- warc_excluded: n/a
- period: 24h, 1500Z Sat – 1500Z Sun; runs multiple times a year (Dec "Big Stew" is the main one; also spring/summer/fall pre-Stews)
- role_distinction: no
- exchange_all:      sent=4-character grid square (RST optional) ; rcvd=4-char grid square
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  n/a
- works_for_points_out_area: n/a
- points: |
    DISTANCE-BASED: each QSO = 1 point + 1 point per 500 km between the two grid-
    square centers (e.g. 1750 km -> 4 points). Then per-QSO POWER MULTIPLIERS
    applied from the RECEIVED (worked) station's power: working a Low-power
    station (<=100W) x2 ; working a QRP station (<=5W) x4.
- multipliers_in_area:  none (no grid/entity multiplier — grids drive the distance points, not a mult count)
- multipliers_out_area: n/a
- final_score: sum of (distance points x worked-station power factor). Additionally the ENTRANT's own category scales the total: Low-power entrant x1.5, QRP entrant x3.
- dupe: per_contest   # work each station once (single band)
- serial: none   # grid square exchanged
- cabrillo_name: STEW-PERRY-TOPBAND   # convention; not explicit in rules
- notes: |
    Unusual scoring: distance rewards long paths, and BOTH the worked station's
    power (per-QSO x2/x4) and the entrant's own power (overall x1.5/x3) boost the
    score — a deliberate low-power/QRP incentive. Power tiers: High (<=1500W /
    legal limit), Low (<=100W), QRP (<=5W). No conventional multiplier.

---

## World Wide Digi DX Contest  [id: ww-digi]
- wa7bnm_ref: 650
- rules_url: https://ww-digi.com/rules/
- confidence: verified
- modes: [FT4, FT8]   # digital only — FT4 and FT8
- bands: [160, 80, 40, 20, 15, 10]
- warc_excluded: yes
- period: last full weekend of August, 24h 1200Z Sat – 1200Z Sun; 2026 = Aug 29–30
- role_distinction: no    # all stations worldwide work all — no continental restriction
- exchange_all:      sent=4-character Maidenhead grid square ; rcvd=4-char grid square
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  n/a
- works_for_points_out_area: n/a
- points: DISTANCE-BASED: each QSO = 1 point + 1 point for each 3000 km between grid-square centers (short path). Counts once per band regardless of mode (FT4/FT8 don't double).
- multipliers_all: 1 multiplier for each different 2-character Maidenhead GRID FIELD (e.g. FN, EM, JO) contacted, counted per band
- dupe: per_band   # once per band; FT4+FT8 on same band = still one QSO credit
- serial: none   # grid exchanged
- cabrillo_name: WW-DIGI
- notes: Grid FIELD (first two locator chars) is the multiplier, per band. Distance divisor is 3000 km (vs Stew Perry's 500 km). Power cats High (<=1500W) / Low (<=100W) / QRP (<=5W), Single/Multi.

---

## Jamboree On The Air (JOTA)  [id: jota]
- wa7bnm_ref: (find — not a WA7BNM scored contest)
- rules_url: https://www.scouting.org/international/jota-joti/jota/  (also https://www.jotajoti.info/jota)
- confidence: verified   # verified that it is an ACTIVITY, not a scored contest
- modes: [any — CW, SSB, digital, all permitted]
- bands: [any amateur band]
- warc_excluded: no    # any band may be used
- period: 3rd full weekend of October (Fri evening through Sun evening); no fixed hours
- role_distinction: no
- exchange_all:      no formal exchange — Scouts exchange name, location, age, and rag-chew
- exchange_in_area:  n/a
- exchange_out_area: n/a
- works_for_points_in_area:  n/a
- works_for_points_out_area: n/a
- points: NONE — JOTA is NOT a contest and has NO scoring. Aim is Scouting contacts, not maximizing QSOs.
- multipliers_in_area:  n/a
- multipliers_out_area: n/a
- dupe: n/a
- serial: none
- cabrillo_name: n/a   # no log submission / not a Cabrillo contest
- notes: |
    IMPORTANT: JOTA (Jamboree-on-the-Air) is the largest Scouting event in the
    world, using amateur radio to link Scouts globally. It is explicitly NOT a
    contest — there is no exchange, no points, no multipliers, and no scoring.
    Include in a contest catalog only as an "activity/event" flag, not a scored
    engine. Runs concurrently with JOTI (Jamboree-on-the-Internet).

# Contest Rules Research Brief

You are researching the exact, official rules for a batch of amateur-radio
contests so they can be encoded into SDRLoggerPlus's contest engine. Accuracy
and consistency matter more than speed. Another process will consume your output
verbatim, so **follow the output template exactly**.

## Method (per contest)

1. Start at the WA7BNM contest calendar detail page:
   `https://www.contestcalendar.com/contestdetails.php?ref=<REF>` (a ref number
   is given for most contests below). If no ref, find the contest at
   `https://www.contestcalendar.com/` (or its state-parties / alphabetical pages).
2. On the detail page, find the **rules link** (labeled "Find rules at:" or a
   URL to the sponsor's site). Fetch the **sponsor's official rules** — that is
   the authoritative source.
3. Extract the fields in the template below from the official rules. Use the
   WA7BNM "Exchange:" summary only as a cross-check.
4. If the official rules are unreachable (dead link, PDF you can't read), fall
   back to the WA7BNM detail page and set `confidence: partial` with a note.
5. Never invent values. If something isn't stated, write `unknown` and note it.

## Key things to capture precisely

- **Modes**: the EXACT allowed modes (CW, SSB/PHONE, RTTY, FT8, FT4, DIGITAL,
  MIXED). Many contests are CW/SSB only — do NOT assume digital is allowed.
- **Bands**: the EXACT allowed bands. Amateur contests almost never use the WARC
  bands (30m/17m/12m) — state explicitly whether they're excluded. Note VHF/UHF
  bands where relevant.
- **Role distinction (critical)**: does the exchange / scoring differ for
  stations INSIDE the contest's home area vs OUTSIDE (and DX)? This is true for
  all QSO parties (in-state sends county, out sends S/P/DX) and for ARRL DX,
  CQ 160, etc. If so, capture each role separately.
- **Who you can work for points**: e.g. in QSO parties, out-of-state stations
  usually work only in-state stations; in-state stations work everyone.
- **Points**: exact per-QSO points, including per-mode (e.g. CW=2/PH=1),
  per-continent, per-country, per-zone modifiers.
- **Multipliers**: exact multiplier sources and whether per-band / per-mode, and
  whether they differ by role.

## Output template (one block per contest, verbatim structure)

```
## <Contest Name>  [id: <catalog-id>]
- wa7bnm_ref: <N or none>
- rules_url: <sponsor rules URL you used>
- confidence: verified | partial | unverified
- modes: [CW, SSB, ...]
- bands: [160, 80, 40, 20, 15, 10, 6, ...]      # list the actual bands
- warc_excluded: yes | no | n/a
- period: <e.g. "1st full weekend of October, 14h">
- role_distinction: yes | no
# If role_distinction = no, fill the "all" block and leave the others "n/a".
- exchange_all:      sent=<fields> ; rcvd=<fields>
- exchange_in_area:  sent=<fields> ; rcvd=<fields>     # e.g. sent=RST+county
- exchange_out_area: sent=<fields> ; rcvd=<fields>     # e.g. sent=RST+(state/prov/DX)
- works_for_points_in_area:  <everyone | ...>
- works_for_points_out_area: <in-area stations only | everyone | ...>
- points: <exact rule, incl per-mode/continent/country/zone>
- multipliers_in_area:  <sources + per-band? e.g. "states+provinces+DXCC+in-area counties, per contest">
- multipliers_out_area: <sources + per-band? e.g. "in-area counties, per band">
- dupe: per_band | per_band_mode | per_contest
- serial: none | per_band | all_band
- cabrillo_name: <NAME as it appears in CONTEST: line, if known>
- notes: <power categories, digital differences, quirks, anything unusual>
```

Write your whole batch to the output file you were given. Start the file with a
one-line summary (how many verified vs partial), then the blocks in order.

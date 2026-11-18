# DXCC entity glance

Spec only. This describes the Log Entry grid Andy K3UK asked for. It does not change code. Stage 2 starts only after stage 1 is in.

Read against `origin/v2-alpha`. Names below are the panels, services, and colors in that tree.

A spot click, a decode click, and a typed call all land in Log Entry. One grid there covers both moments. He should not have to open Statistics.

## Goal

While a call is in Log Entry, show one small grid for that call's DXCC entity:

- One row of bands the log already uses.
- Three mode groups: phone, CW, and digital. Not every submode.
- The same marks Statistics already uses: green Confirmed, amber Worked, a dash for Not worked.

The operator can see, without leaving the log line, which band and mode group is still needed for that country.

## Non-goals

- Do not add a prefix box to Statistics → DXCC, and do not add mode columns to that table.
- Do not change 5BDXCC. That is a different screen (`FiveBandDxccTab`, `AwardsService.Get5BDxccStatisticsAsync`, bands 80m, 40m, 20m, 15m, 10m).
- Do not change WAZ or WAS. Those screens already show modes.
- Do not add a New Mode cluster color.
- Do not put a band grid inside the cluster row, the decode row, or the DX Coach row.
- Do not build a second award rules engine. Do not invent a new definition of confirmed.

## What the app does today

Statistics → DXCC (`StatisticsPlugin`, tab `dxcc`) calls `api.getDxccStatistics` → `GET /api/statistics/dxcc` → `AwardsService.GetDxccStatisticsAsync`.

`BandCell` in `StatisticsPlugin.tsx`:

- Confirmed: a square, class `bg-accent-success` (light `#388e3c`, dark `#3AA855`).
- Worked, not confirmed: a square, class `bg-accent-warning` (light `#d2911e`, dark `#D4A72E`).
- Not worked: a dash, `-`, class `text-dark-400`. The summary legend also draws an empty `bg-dark-600` square with a `border-glass-100` border and the label "Not worked". The cell itself is the dash.

Columns are `visibleBands`: `ALL_BANDS` from `src/SDRLoggerPlus.Web/src/utils/spotBands.ts`, kept only when some entity has that band, in `ALL_BANDS` order. There is no mode axis. Countries never worked are absent. An empty log shows "No DXCC entities found" and no grid.

`GetDxccStatisticsAsync` sets `BandStatus.Worked = true` for every band that has at least one QSO, and `Confirmed` when `bandQsos.Any(IsConfirmed)`. `IsConfirmed` is `ConfirmationPolicy.Any`. Detail is under "Worked and confirmed" below.

Cluster row color is one spot, from `SpotStatusService.GetSpotStatus`:

- `newDxcc` — New DXCC. This entity has never been worked.
- `newBand` — New Band. The entity has been worked, not on this band.
- `worked` — Worked. The entity has already been worked on this band and this mode.
- `null` — no verdict (cache not ready, no country, unknown band, or the entity and band are worked but this mode is not). There is no New Mode.

Default colors in `settingsStore` `spotStatus.colors`: New DXCC `#ff3a09`, New Band `#4cc850`, Worked `#6d6d6d`. `dimWorked` defaults to true. `ClusterPlugin.getRowStyle` draws the worked row at opacity `0.6`.

Decode status column (`DecodesPlugin.decodeStatus`), most valuable first: New DXCC, New Band, New Zone (`#00e5ff`), Zone+Band, New Grid (`#ff8c3b`), Grid+Band, Worked. "Needed only" uses `isDecodeNeeded`. DXCC and band chips use the same spot colors.

DX Coach chips come from `reasonsFor` in `src/SDRLoggerPlus.Web/src/utils/dxCoach.ts`: "New DXCC" and "New band". A click calls `selectSpot`, which tunes and fills Log Entry through the existing spot-selected path.

Log Entry (`LogEntryPlugin`):

- The callsign info card is absent only when the call is shorter than 3 characters (`MIN_CALLSIGN_LENGTH`). While a lookup is in flight and the callsign is non-empty, the card is on screen and says "Looking up callsign...". When the callbook has no name, the card shows "No name on file". It also shows country and grid.
- Super Check Partial paints a green dot (`bg-accent-success`, title "Worked before") on a suggested callsign that is already in the worked-call set. That dot is the callsign, not the DXCC entity.
- "History On" (`autoHistory`) filters Log History to that call. It does not change the callbook lookup.

QRZ Profile shows `focusedCallsignInfo.dxcc` as the entity number. That number is not worked or needed.

The QRV panel is presence. It is not a DXCC need.

## Order

Stage 1 is the entity grid. Stage 2 is the callsign mark on those same cells.

Do not start stage 2 until stage 1 is in and behaving as this spec says. Stage 1 does not draw the callsign mark.

## Stage 1

### Where it sits

Log Entry is one vertical form. The callsign, band, and mode share one row. Under that row is the dupe strip, then the callsign info card (`bg-dark-700/50`, name, country, grid, flag). The card is full width. There is no column beside it.

Put the grid in that same column, as its own full-width block immediately under the callsign info card and above the Name field.

The card is on screen while a lookup is in flight ("Looking up callsign...") whenever the callsign is non-empty, and it shows "No name on file" when the callbook has no name. The grid sits under that card. The card is absent only when the call is shorter than 3 characters. When the card is absent, the grid stays in that same slot: under the dupe strip, or under the call / band / mode row when there is no dupe. It does not sit inside the card, and it does not sit in the callsign row.

The grid does not wait for QRZ. The card and the grid are independent. The card can say "Looking up callsign..." while the grid is already up, and the card can show a country while the grid stays hidden because the prefix is still ambiguous.

The block shows the cty.dat primary prefix and country name for the resolved entity, so the operator can see which country the cells belong to. It does not show the QRZ DXCC number. If QRZ's country string and the cty.dat name differ, the card keeps the QRZ string and the grid keeps the cty.dat name.

The grid is not a control. Clicking a cell does not tune the radio and does not change the band or mode dropdowns.

### What one grid looks like

Three rows, labeled Phone, CW, and Digital, in that order. One header row of band names above them. That is the "one row of bands."

Each cell is one band and one mode group:

- Confirmed: the green `bg-accent-success` square, same as `BandCell`.
- Worked, not confirmed: the amber `bg-accent-warning` square.
- Not worked: the dash `-` in `text-dark-400`. Empty still needed. Do not invent a fourth color.

A legend under or beside the grid uses Statistics' three labels: Confirmed, Worked, Not worked. The Not worked legend mark may be the empty square Statistics already draws. The cell stays the dash.

Tooltip, same shape as `BandCell`: the QSO count, and "Confirmed" when the cell is confirmed.

A country never worked still draws the grid, every cell a dash. Statistics hides those countries. This grid does not.

### When it appears, and when it does not

The grid follows the call string in Log Entry (`formData.callsign`).

Show the grid only when that string maps to exactly one DXCC entity, by the rule in the next section. Then every band column is drawn, worked or not.

Show nothing — no grid, no error, no guessed country — when:

- the call is empty,
- the typed text could still be several countries,
- the text maps to no DXCC entity.

Clearing the call clears the grid.

### How the entity is resolved

Use the callbook's own split, then the cty.dat map. Do not call QRZ, HamQTH, or any network lookup. Tests stay mocked.

The callbook path is `LogHub.FocusCallsign`:

- `CallsignHelper.ExtractBaseCall` is what QRZ and HamQTH are queried with.
- Prefix form (`CallsignHelper.IsPrefixForm`, for example `F/HB9GUX`): the DXCC entity is the leading token when that token is a cty.dat key. France, not Switzerland. A leading token that is not a key (`FX/W1AW`) is not an entity. Do not call `GetEntityFromCallsign` on the full string, which would match the shorter key `F`.
- Suffix form (`HB9GUX/P`, and the other compound calls that are not prefix form): the entity stays the base call's entity.
- `CtyService.FindEntity` is a longest-prefix match. It always returns one entity when any prefix fits. That is a guess on a partial call. The glance must not display that guess while another entity is still possible.

`CallsignHelper.KnownSuffixes` is `P`, `M`, `MM`, `AM`, `A`, `QRP`, `QRPP`, `LH`, `B`, `R`. A single trailing digit is a call-area suffix (`W1AW/4`). None of these select a different entity.

Normalize the Log Entry text by trimming it and uppercasing it. Then:

1. Suffix form (compound, and not prefix form). Strip to `ExtractBaseCall` and test that base call. `HB9GUX/P`, `HB9GUX/MM`, and `HB9GUX/QRP` are the same entity as `HB9GUX`.
2. Prefix form. The leading token, the text before the first slash, is closed. If that token equals a key in the cty.dat map, the entity is that key's entity. Longer keys that merely start with the token are no longer possible, because the slash ended the token. `F/HB9GUX` is France when `F` is France's key. `KH6/W1AW` is Hawaii when `KH6` is Hawaii's key. Do not resolve prefix form from the base call. A prefix form whose leading token is not a map key shows nothing. Do not fall through to a shorter key. `FX/W1AW` must not show France just because `F` is a key.
3. Anything else, including a plain partial. Let T be the text under test.

   cty.dat has 29 primary prefixes that contain a slash (`3D2/c`, `VP8/g`, `GM/s`, `FT/z`, and the rest of that set: `3D2/r`, `3Y/b`, `3Y/p`, `E5/n`, `E5/s`, `FK/c`, `FO/a`, `FO/c`, `FO/m`, `FT/g`, `FT/j`, `FT/t`, `FT/w`, `FT/x`, `HK0/a`, `HK0/m`, `JD/m`, `JD/o`, `JW/b`, `KH8/s`, `SV/a`, `VP6/d`, `VP8/h`, `VP8/o`, `VP8/s`). A key `AAA/x` matches a call that begins `AAAX` (the slash is not typed). Conway Reef's key `3D2/c` matches a call that begins `3D2C`, including `3D2CCC`, and that call is not Fiji.

   Collect every key whose match-form starts with T. A normal key's match-form is the key. A slash key's match-form is the call-form `AAAX`. An exact-call override (`=` in cty.dat) is not one of these keys — `CtyService` skips `=` entries when it builds the prefix map — but when T equals that call, the override still selects its entity, before this collection.

   - One distinct primary prefix (`CtyEntity.Prefix`) among those keys: that entity. Show the grid.
   - More than one primary prefix: ambiguous. Show nothing. Do not keep the longest prefix, and do not fall through to the parent country on the next character. Typing `3D2`, `VP8`, or `GM` is ambiguous (the parent and the slash entities are all still open). `3D2C` is Conway Reef, not Fiji.
   - No match-form starts with T: the call is longer than every prefix. An exact-call override that equals T still selects its entity. Otherwise a slash key whose call-form is a prefix of T selects that entity (the longest call-form). `3D2CCC` is Conway Reef, not Fiji; do not take the parent that `GetEntityFromCallsign` would return by matching `3D2`. Otherwise use `CtyService.GetEntityFromCallsign(T)`. One entity shows the grid. That method returns `(null, null, null)`, not a null tuple, when nothing fits. No country means no DXCC entity, and the grid stays hidden.

   This matching is the glance only. Do not change `CtyService.FindEntity` or `GetEntityFromCallsign`. Cluster colors keep the longest-prefix match they have today.

A shorter key that T has already grown past is not added back. `KH6` does not stay "United States" just because `K` is a prefix of `KH6`, as long as every key that starts with `KH6` is Hawaii.

Worked examples the tests lock:

| Typed text | Grid |
| --- | --- |
| empty | hidden |
| `K` | hidden — United States, Hawaii, Alaska, Puerto Rico, and the other `K*` entities are still open |
| `KH6` | Hawaii, once no longer key starting with `KH6` belongs to a different primary prefix |
| `W1AW` | the one entity `GetEntityFromCallsign` returns (United States) |
| `HB9GUX/P`, `/MM`, `/QRP` | the same entity as `HB9GUX` |
| `F/HB9GUX` | France, the leading token, not Switzerland |
| `FX/W1AW` | hidden — `FX` is not a map key, so not France |
| `3D2`, `VP8`, `GM` | hidden — the parent and the slash entities are still open |
| `3D2C`, `3D2CCC` | Conway Reef, not Fiji |
| a string no prefix fits | hidden |

The 3-character card gate does not apply to the grid. An unambiguous 2-character prefix shows the grid before the card. An ambiguous 4-character string shows the card and hides the grid. QRZ's country and DXCC number never break a tie.

### Data source

Add `GetDxccEntityGlanceAsync(string callsign)` to `IAwardsService` and implement it on `AwardsService`, beside `GetDxccStatisticsAsync`.

Expose it as `GET /api/statistics/dxcc/glance?call=`. `200` with the body below when the grid should show. `204` when the call is empty, ambiguous, or has no entity. Do not use `404` for a partial call. Leave `GET /api/statistics/dxcc` unchanged.

The method reads QSOs only through `AllQsosAsync()`, the same personal-QSO set `GetDxccStatisticsAsync` uses. It does not take `StatisticsFilters`. Continent, band, status, and date filters on the Statistics screen do not change this grid.

It confirms with `IsConfirmed`, which is `ConfirmationPolicy.Any`. It normalizes a band with `(q.Band ?? "").Trim().ToLowerInvariant()`, the same expression the DXCC loop uses.

It does not call `SpotStatusService.GetSpotStatus`. That method answers one spot. It has no confirmation flag and no phone / CW / digital groups. Cluster colors keep using it, unchanged.

DTO, in `StatisticsDto.cs`:

- `EntityName`, `PrimaryPrefix` — from the resolved `CtyEntity`.
- `Bands` — the column list, in order.
- `Rows` — three rows, `phone`, `cw`, `digital`, in that order.
- Each cell: `Worked`, `Confirmed`, `QsoCount`.
- `CallsignWorked` on the cell. Stage 1 leaves it false and the UI does not read it. Stage 2 fills it on this same method. No second endpoint.

### Which QSOs count for the entity

Start from `AllQsosAsync()`, the same personal QSOs Statistics reads.

`QsoService.CreateAsync` writes the callbook country only onto `Station.Country` and leaves `Qso.Country` null. `GetDxccStatisticsAsync` drops every row with an empty `Qso.Country`, so a glance that copied that drop would miss a QSO just logged from Log Entry or WSJT-X. On create, when `Qso.Country` is empty and `Station.Country` is set, copy `Station.Country` onto `Qso.Country`. Statistics → DXCC then includes those QSOs too. That is a create-path fix, not a new Statistics UI, not new mode columns, and not a prefix box. The glance reads the same QSOs Statistics reads after that copy.

An existing row with empty `Qso.Country` and a filled `Station.Country` is treated as that station country when painting the grid, so older rows are not blank until re-saved. A row whose `Qso.Country` and `Station.Country` are both empty is still dropped, the same way `GetDxccStatisticsAsync` drops it.

A remaining QSO is in this entity when either of these is true:

- `CtyService.GetPrimaryPrefixFromCallsign(qso.Callsign)` equals the resolved primary prefix. That is the same helper the Statistics prefix column uses.
- The callsign does not resolve, and `GetEntityKey(qso)` equals `country:{NAME}` for the cty.dat country (compare names case-insensitively). `GetEntityKey` is the private method in `AwardsService`: `dxcc:{code}` from `Qso.Dxcc` or `Station.Dxcc` when a code is stored, otherwise `country:{Country}`.

Also include QSOs whose `GetEntityKey` is `dxcc:{n}` when some other personal QSO already matched this primary prefix and is stored under that same `n`. That is the statistics group for this entity, including rows that have a code but a callsign that no longer resolves. Do not pull in a different code because the country strings look similar.

If no QSO matches, the entity has never been worked. Return the grid with every cell a dash.

Do not build a second grouping key. `GetEntityKey`, `GetPrimaryPrefixFromCallsign`, and `IsConfirmed` are the rules.

### Worked and confirmed

This is the DXCC statistics rule, copied from the code. It is wider than "paper card or LoTW."

`AwardsService.GetDxccStatisticsAsync` sets `Confirmed` with `IsConfirmed`. `AwardsService.IsConfirmed` is `ConfirmationPolicy.Any` (`src/SDRLoggerPlus.Server/Services/ConfirmationPolicy.cs`).

`ConfirmationPolicy.Any` is true when any one of these ADIF received flags is `Y` (case-insensitive):

- LoTW: `Qso.Qsl.Lotw.Rcvd`
- Paper card: `Qso.Qsl.Rcvd`
- eQSL: `Qso.Qsl.Eqsl.Rcvd`
- QRZ Logbook: `Qso.Qsl.Qrz.Rcvd`

`ConfirmationPolicy.AwardRules` is LoTW or paper card only. VUCC and FFMA use it. `GetDxccStatisticsAsync` does not. An eQSL-only or QRZ-Logbook-only QSO is Confirmed on Statistics → DXCC, so it is green here too. Do not switch this grid to `AwardRules`.

A sent flag (`Qsl.Sent`, `Lotw.Sent`, and the rest) is not a confirmation. `Rcvd` of `N`, `R`, `I`, or `V` is not a confirmation. Only `Y` counts. That is `ConfirmationPolicy.Received`.

For one cell:

- Worked when at least one counting QSO is on that band and in that mode group.
- Confirmed when any of those QSOs passes `IsConfirmed`. Confirmed wins over Worked: the cell is green, not amber.
- `QsoCount` is the number of those QSOs.
- A QSO whose mode maps to no group fills no cell.
- A QSO with a blank band fills no cell. Statistics stores that band as `""`, and `""` is not an `ALL_BANDS` column.

### Contest contacts

`GetDxccStatisticsAsync` does not exclude contest QSOs. `AllQsosAsync` keeps every QSO `QsoOwnership.IsPersonalQso` accepts.

`QsoOwnership.IsPersonalQso` is true when `Contest.StationCallsign` is null or blank, and true when that callsign equals the operator's station call (trimmed, case-insensitive). A contest QSO logged under a different club or special call is not personal. Statistics drops it. This grid drops it. That is the whole contest rule.

Do not also apply `QrvQsoTotals.IsContest`. That helper excludes every contest contact from the QRV totals. Statistics does not. A personal contest QSO counts toward DXCC here.

### Bands

Columns are the bands Statistics already shows, in the same order.

`ALL_BANDS` (`spotBands.ts`), longest wavelength first:

`2200m`, `630m`, `160m`, `80m`, `60m`, `40m`, `30m`, `20m`, `17m`, `15m`, `12m`, `10m`, `6m`, `2m`, `1.25m`, `70cm`, `33cm`, `23cm`.

When the personal log has at least one QSO on a band in that list, the columns are those bands only, in that order. This is `StatisticsPlugin`'s `visibleBands`: `ALL_BANDS.filter(b => bandsInData.has(b))`. `40M` and `40m` are one column because the band key is lowercased.

An empty log has no such bands. Statistics would show no table. This grid still shows, once the entity resolves, with every `ALL_BANDS` column present and every cell a dash.

The radio's current band does not add a column. A band joins the row when a personal QSO on that band is in the log, and only if the band is in `ALL_BANDS`.

Log Entry's own dropdown also lists `13cm`, `9cm`, `6cm`, `3cm`. Those are in `BandHelper` and are not in `ALL_BANDS`. Statistics does not show them. This grid does not show them.

`AwardsService.GetBandOrder` is only the sort for band summaries. It is not the column order. Use `ALL_BANDS`.

### Mode groups

Three groups. Not every submode. Compare the mode after trim and uppercase.

Phone, the modes the log already treats as phone:

- The ones this design names: `SSB`, `USB`, `LSB`, `AM`, `FM`.
- The rest of the Log Entry "AM / FM" group: `SAM`, `NFM`.
- `DSB`, from `QrvQsoTotals.VoiceModes`.
- `PH`, `PHONE`, `VOICE`, which `LogbookHealthService.NormalizeMode` already collapses to `SSB`.

CW:

- The ones this design names: `CW`, `CWU`, `CWL`.
- `CWR` and `CW-R`. `AdifService.ToAdifExportMode` already exports those as `CW`, and `QrvProtocol.ModeForLogger` maps them to `CWL`.

Digital. This includes `FT8`, `FT4`, `RTTY`, `PSK`, and the data modes the app already treats as digital.

From the Log Entry mode dropdown (`MODE_GROUPS`, "Digital (TCI)" and "Digital (Log)"): `DIGU`, `DIGL`, `FT8`, `FT4`, `JS8`, `Q65`, `MSK144`, `RTTY`, `PSK31`, `WSPR`, `JT65`, `JT9`, `DIGI`.

Aliases the app already folds into those:

- `PSK`, `PSK63`, `PSK125` — `SpotStatusService.NormalizeMode` and `AdifService.NormalizeModeForMatch` fold `PSK31`, `PSK63`, and `PSK125` to `PSK`.
- `FSK` → `RTTY`, and `FSK-R` / `FSKR` → `RTTYR` — `QrvProtocol.ModeForLogger` and `LogbookHealthService`.
- `DATA-U`, `PKT-U`, `USB-D`, `USBD` → `DIGU`. `DATA-L`, `PKT-L`, `LSB-D`, `LSBD` → `DIGL`. Those are the rig names `QrvProtocol.ModeForLogger` and the Log Entry comment already map onto the digital passthrough modes.
- `JT65A`, `JT65B`, `JT65C` → `JT65` — `AdifService.ModeFamilies`.
- `FST4` — `AdifService.ModeFamilies` (MFSK family, same as `FT4`, `JS8`, `MSK144`, `Q65`).

Other data tokens `QrvProtocol.SendableModes` already accepts, and which are not phone or CW: `DIG`, `DATA`, `DRM`, `FT2`, `FST4W`, `HELL`, `QPSK`, `OLIVIA`, `CONTESTIA`, `MFSK`, `DOMINOEX`, `THOR`, `MT63`, `PACKET`, `VARA`, `VARAC`, `VARAHF`, `Q65C`, `Q65D`, `Q65E`, `MSKMS`, `JS8CX2`, `RTTYR`.

`SPEC` is on that QRV send list and is not a data mode. It fills no cell.

Unknown mode: a value that is not in the phone set, the CW set, or the digital set fills no cell. It is not digital. Do not call `QrvQsoTotals.ModeBucket` for this grid. That function puts every leftover mode in digital, and the QRV totals are not DXCC.

A blank mode fills no cell.

`USB` and `LSB` stay phone. Do not copy the cluster filter that rewrites them to `SSB` and then lose them. `SSB` is also phone. `CWU` and `CWL` are CW, not two columns.

### How the grid stays current

Re-read when the call in Log Entry changes. That includes:

- typing, in `handleCallsignChange`,
- a cluster row click: `ClusterPlugin.handleRowClick` → `selectSpot` → `onSpotSelected` → the `selectedSpot` effect, which sets `formData.callsign` to `selectedSpot.dxCall` and replaces whatever was typed,
- a DX Coach click, which uses the same `selectSpot`,
- a decode that fills the call: double-click runs `DecodesPlugin.callStation` (the decoder answers the CQ). The call arrives in Log Entry when `WsjtxService.HandleDxCallAsync` broadcasts a `SpotSelectedEvent`. The grid follows that call. Do not add a second fill path on the decode row.

Each server read goes through `AllQsosAsync()`, so it sees `QsoSnapshotCache` after `LiteQsoRepository` invalidates it on commit. Do not keep a second cache of worked entities inside the glance.

Logging a QSO clears the call today (`callsign: ''` in the create success path). Clearing the call clears the grid. The new QSO is in the snapshot. The next time that entity resolves, the cell includes it. If a path leaves the call in the box, refetch immediately and update the cell in place.

Import of old QSOs writes through the repository and invalidates the same snapshot. The next read includes them. No import-only rule.

A LoTW confirmation that arrives later flips `Qsl.Lotw.Rcvd` to `Y`. The next read uses `ConfirmationPolicy.Any`, so an amber cell becomes green. The same is true for a paper card, an eQSL confirmation, and a QRZ Logbook confirmation. There is no "pending" mark.

On the client, refetch when the call string changes, when the create-QSO mutation succeeds, and when a confirmation merge or an ADIF import finishes while a call is still in the box. Use the same 60 second stale time as the `dxcc-statistics` query so a background sync is not later than Statistics.

## Stage 2

Only after stage 1 is in.

Add a second mark on the same cells, the way Log4OM uses a blue square for "this callsign." It answers a different question from the country mark: this callsign has been worked on that band and mode group.

Draw it as a blue corner on the cell, class `bg-accent-info` (light `#3878ac`, dark `#4580C4`), plus a fourth legend entry, "This callsign." `accent-info` is the steel blue already in the theme. Do not reuse the green, the amber, or the cluster worked gray (`#6d6d6d`).

The corner does not replace the square or the dash. A cell can be green or amber and also carry the corner. The corner does not turn a dash into Worked or Confirmed. It does not change confirmed, worked, or needed for the entity. It does not change spot colors, decode chips, or DX Coach chips.

Set `CallsignWorked` on a cell when a QSO already counted for that cell has `CallsignHelper.ExtractBaseCall(qso.Callsign)` equal to `ExtractBaseCall` of the Log Entry call (case-insensitive).

- `HB9GUX`, `HB9GUX/P`, `HB9GUX/MM`, and `HB9GUX/QRP` share the corner.
- A different call in the same entity does not. `W1AW` worked on 20m phone does not put a corner on `K1JT`.
- The QSO has to be in this entity. A Switzerland QSO with `HB9GUX` does not put a corner on the France grid for `F/HB9GUX`. A France QSO whose base call is `HB9GUX` does.
- The corner means worked, not confirmed. A LoTW confirmation does not add a second callsign mark.

Stage 1 UI ignores `CallsignWorked` even if a response contains it.

## What must not change

- Cluster row color. It stays the one-spot status it has today: New DXCC, New Band, Worked, or no verdict. No band grid inside the row. No New Mode. Default colors and `dimWorked` stay as they are.
- Decode status column: New DXCC, New Band, New Zone, Zone+Band, New Grid, Grid+Band, Worked. The Needed only filter stays.
- DX Coach chips: New DXCC, New band. Click still tunes and fills Log Entry.
- Statistics tabs, including DXCC. No prefix search box. No mode columns on the DXCC table. 5BDXCC stays the five-band screen it is.
- WAZ and WAS. They already show modes.
- QRV. It is presence, not a DXCC need.
- QRZ Profile badges, and the DXCC number on that profile. The number is the entity number, not worked or needed.
- Super Check Partial. The green "Worked before" dot stays the callsign.
- History On. It still only filters Log History.
- Contest logging, the contest entry panel, and dupe checking.
- Spot dimming.
- Award filters on the Statistics screen.

## Edge cases

| Case | Result |
| --- | --- |
| No call | No grid. |
| Partial call, several entities still possible (`K`, `G`, `KH`) | No grid. Do not guess. |
| Partial call, one entity (`KH6` when its longer keys are one entity, a finished call such as `W1AW`) | Grid. |
| Call with no DXCC entity | No grid. Not an empty grid labeled Unknown. |
| `/P`, `/MM`, `/QRP`, and the other known suffixes | Same entity as the base call. Stage 2 shares the callsign mark with the base call. |
| Prefix form `F/HB9GUX` | France. The base call is still `HB9GUX` for the stage 2 corner, and only France QSOs count. |
| Same entity, different call | Entity colors match. Stage 2 corner follows the call in the box. |
| Spot click or decode DX Call replaces a typed call | The old grid is gone. The new call is resolved on its own. |
| Radio band not in the log yet | No new column. Columns are bands the log already uses (or all of `ALL_BANDS` when the log is empty). |
| Country never worked | Grid, every cell a dash. |
| Empty log, entity resolves | Grid, all `ALL_BANDS` columns, every cell a dash. |
| QSO just logged | On create, an empty `Qso.Country` is copied from `Station.Country` when that is set, so Statistics and this grid both include it. Snapshot updates on commit. The cell includes the QSO the next time that entity is shown. If the call is still in the box, update the cell now. If logging cleared the call, the grid clears with it. |
| Import of old QSOs | The next read includes them. Same snapshot as Statistics. An imported row that still has an empty `Qso.Country` and a filled `Station.Country` paints as that station country. |
| LoTW, card, eQSL, or QRZ Logbook confirmation arrives later | The next read turns that cell from amber to green when `ConfirmationPolicy.Any` becomes true. |
| Personal contest QSO | Counts. Statistics counts it. |
| Contest QSO under a different station call | Does not count. Statistics drops it. |
| QSO with empty `Qso.Country` and filled `Station.Country` | Counts as that station country when painting the grid. |
| QSO with both countries empty | Does not count. Statistics drops it. |
| `3D2`, `VP8`, `GM` | No grid. Do not guess the parent, and do not fall through to it on the next character. |
| `3D2C`, `3D2CCC` | Conway Reef. Not Fiji. |
| `FX/W1AW` | No grid. Not France. |
| Exact-call override | That entity. `GetEntityFromCallsign` returns `(null, null, null)`, not a null tuple, when nothing fits. |
| Unknown or blank mode | That QSO fills no cell. |
| Band outside `ALL_BANDS` (`13cm`, `9cm`, `6cm`, `3cm`) | Not a column. |
| eQSL-only or QRZ-Logbook-only confirmation | Green. That is `ConfirmationPolicy.Any`, which Statistics → DXCC already uses. |

## Tests

Unit tests, in-memory QSOs, no live QRZ and no live HamQTH. Category `Unit`. New file next to the other awards tests: `src/SDRLoggerPlus.Server.Tests/Tests/Services/AwardsServiceDxccGlanceTests.cs`. Call `GetDxccEntityGlanceAsync` directly. Do not stand up the QRZ client.

Lock all of the following:

- Empty call, ambiguous `K`, and a call no prefix fits return no grid.
- `3D2`, `VP8`, and `GM` return no grid. `3D2CCC` is Conway Reef, not Fiji. `FX/W1AW` returns no grid, not France.
- `KH6` (unique) returns a grid for that entity. A never-worked entity returns every cell not worked and not confirmed.
- `W1AW` returns the United States entity `CtyService` resolves, not a guess among `W*` entities that the full call has already grown past.
- `HB9GUX/P`, `HB9GUX/MM`, and `HB9GUX/QRP` resolve to the same entity as `HB9GUX`.
- `F/HB9GUX` resolves to France. A Switzerland QSO for `HB9GUX` does not fill a France cell.
- One phone QSO (`USB`, `LSB`, `SSB`, `AM`, `FM`) fills only the phone cell. `CWU` fills only CW. `FT8`, `FT4`, `RTTY`, and `PSK31` fill only digital. `PSK63` and `PSK125` fill digital. `CWR` fills CW. `DATA-U` fills digital.
- An unknown mode, a blank mode, and `SPEC` fill no cell.
- Confirmed: a QSO with only `Qsl.Lotw.Rcvd = Y` is green. The same for `Qsl.Rcvd`, `Qsl.Eqsl.Rcvd`, and `Qsl.Qrz.Rcvd`. A sent-only flag, and `Rcvd` of `N`, stays amber. This locks `ConfirmationPolicy.Any`, not `AwardRules`.
- Two QSOs on the same cell: the cell is confirmed if either one is. `QsoCount` is 2.
- A personal contest QSO (null station call, or the station call equal to the operator) fills the cell. A contest QSO with a different `Contest.StationCallsign` does not.
- A create with only `Station.Country` set copies that value onto `Qso.Country`, and both the glance and `GetDxccStatisticsAsync` count the QSO. Tests must not only build QSOs that already have `Country` set.
- An older row with empty `Qso.Country` and a filled `Station.Country` still paints the cell. A row with both empty does not.
- Band order matches `ALL_BANDS`, and a band the log never uses is absent, except the empty-log case, which returns every `ALL_BANDS` column with empty cells.
- `40M` and `40m` share one column.
- Stage 2: `CallsignWorked` is true only for the base call on that cell. A different call in the same entity leaves it false. Stage 1 behavior does not depend on the flag.

Leave `SpotStatusServiceTests` as they are. Do not add a New Mode case. `GetSpotStatus` still returns `newDxcc`, `newBand`, `worked`, or null.

A frontend test may render the grid from a mocked glance response and assert the dash, `bg-accent-success`, and `bg-accent-warning`. It must not call QRZ. It is not a substitute for the service tests above.

## Out of scope

- A new prefix search on Statistics.
- Mode columns on the Statistics DXCC table.
- A New Mode cluster color.
- A full award window inside the log line.
- Any change to 5BDXCC, WAZ, WAS, QRV, the QRZ profile, Super Check Partial, History On, contest logging, spot dimming, or the award filters.

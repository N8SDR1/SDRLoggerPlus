# QRV last worked — already in this folder

**Status:** implemented in this folder. Not committed. Not in the installed SDRLogger+ build.

Rebuild and install from:

`Y:\Claude local\SDRLoggerPlus-qrv-fix`

Do not rebuild from `Y:\Claude local\SDRLoggerPlus-v2`. Do not redesign the JSON. The website and the Hostinger service already expect this shape.

## What the post gains

The snapshot is still a replacement of the three totals. Each bucket gains one field, `last`: the newest counted contact’s call in that period.

```json
{
  "call": "N8SDR",
  "app": "Logger",
  "install_key": "...",
  "week":  { "voice": 0, "cw": 0, "digital": 50, "last": "N9BC" },
  "month": { "voice": 0, "cw": 0, "digital": 80, "last": "N9BC" },
  "year":  { "voice": 1, "cw": 2, "digital": 400, "last": "N9BC" }
}
```

Rules already in `QrvQsoTotals.Count` and `QrvQsoTotals.Encode`:

- `last` is the worked station (`Qso.Callsign`), not the operator.
- Newest `QsoDate` in that UTC period wins. Week, month, and year are counted separately. A quiet week leaves `week.last` empty while month and year still carry the newer contact inside those periods.
- Contest contacts stay out of the count and cannot become `last`.
- A call that is not a callsign is skipped for `last`. The QSO still counts. The previous valid call remains.
- Imported contacts count, including STD, WSJT-X, JTDX, MSHV, and VARAC.
- Turning totals off sends zeros and `last` of `""` for all three periods, so the column clears.
- No ADIF, no frequencies, no notes, no list of calls. One call per period.

The server drops a `last` it cannot normalize and still stores the counts. An old build that omits `last` keeps posting totals; the column stays blank until this build.

## What this build does not do

The gold color and the ↔ mark are decided on qrvsdr.cc. Gold means that call is on the live roster. ↔ means the other station’s own snapshot points back at this call. Logger does not send a flag for either.

Discord `#qsos` stays the totals line. There is no confirmed-contact counter.

## Files

- `src/SDRLoggerPlus.Server/Services/Qrv/QrvQsoTotals.cs`
- `src/SDRLoggerPlus.Server.Tests/Tests/Services/QrvQsoTotalsTests.cs`
- `src/SDRLoggerPlus.Server.Tests/Tests/Services/QrvQsoTotalsLocalBoardTests.cs`
- `src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx`
- `src/SDRLoggerPlus.Web/src/components/AboutDialog.tsx`
- `docs/qrv-qso-totals.md`

## After install

Leave **Share QSO totals** on. Within about a minute the board’s Last worked column fills from the log already there. No extra QSO is required. The next counted contact replaces that call.

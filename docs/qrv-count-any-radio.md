# QRV QSO totals — a logged contact counts

**Status:** local change in this folder. Not committed. Not in the installed 2.15.3.

Rebuild and install from:

`Y:\Claude local\SDRLoggerPlus-qrv-fix`

Do not rebuild from `Y:\Claude local\SDRLoggerPlus-v2`.

## What 2.15.3 already does

GitHub release 2.15.3 is “Count QRV QSO totals from any radio.” That build is installed and it is posting. The weekly board stays empty because the count still skips a contact that has no radio stamp.

STD, WSJT-X, JTDX, MSHV, and VARAC do not save through Logger’s log button. They append ADIF, and Logger’s ADIF monitor (or ADIF-over-UDP) imports the new record. Those rows have no stamp, so the posted total is 0. The same hole hits a manual ADIF import.

## The rule now

Every non-contest contact already in the SDRLogger+ log counts for the UTC week, month, and year. The path does not matter:

- Log entry inside SDRLogger+
- ADIF monitor (STD `std.adi`, WSJT-X, JTDX, MSHV, VARAC, or any other watched file)
- ADIF-over-UDP
- A manual ADIF import

Contest contacts stay out. POTA contacts count in Voice, CW, or Digital. Lyra, STD, and Combo link are not required. The roster is not used to guess a source.

Week is the ISO week (Monday 00:00 UTC). Month and year are the calendar periods in UTC. A log that already has contacts in 2026 will show those on Monthly and Yearly. This week’s board is only contacts dated since Monday 00:00 UTC.

## Files

- `src/SDRLoggerPlus.Server/Services/Qrv/QrvQsoTotals.cs` — `IsCountedContact` no longer requires a stamp
- `src/SDRLoggerPlus.Server.Tests/Tests/Services/QrvQsoTotalsTests.cs`
- `src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx`
- `src/SDRLoggerPlus.Web/src/components/AboutDialog.tsx`
- `docs/qrv-qso-totals.md`

The ADIF monitor and UDP listener also stamp new imports `Logger`. The count does not depend on that stamp.

## After the new build is installed

Leave **Share QSO totals** on. Do not log another contact to test it. Within about a minute the existing log is recounted and posted. Today’s FT8 contacts should show under Digital on qrvsdr.cc Weekly and in Discord `#qsos`.

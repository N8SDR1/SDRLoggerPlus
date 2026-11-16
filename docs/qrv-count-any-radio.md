# QRV QSO totals — count any radio

**Status:** fixed locally in this folder. Not committed. Not on GitHub. The running SDRLogger+ build still has the old rule.

Rebuild and install from:

`Y:\Claude local\SDRLoggerPlus-qrv-fix`

Do not rebuild from `Y:\Claude local\SDRLoggerPlus-v2`. That checkout does not contain this code. Do not rebuild from `origin/cursor/qrv-qso-totals-8d8c` alone. That branch is the parent (`ba48514`) and still counts only Lyra and STD.

## What was wrong

Share QSO totals was on and Logger posted a snapshot. The server accepted it. The website and Discord `#qsos` stayed empty because the snapshot was 0 voice, 0 CW, and 0 digital.

A contact was counted only when it was stamped `Lyra` or `STD` at save time. Lyra tells Logger its TCI name is `ExpertSDR3` unless that emulation is turned off. The old rule treated `ExpertSDR3` as Lyra only while Combo link was on. Combo is often off, so those FT8 contacts were saved with a blank source and left out. STD is a TCI client of Lyra, so it did not get its own label on Logger’s connection either.

Lyra and STD are Windows-only. SDRLogger+ is Windows, Mac, and Linux. Requiring Lyra or STD kept every other radio, and every non-Windows install, off the board.

## The rule now

- A contact logged in SDRLogger+ counts, on any radio.
- Combo link is not required.
- Lyra and STD are not required. Mac and Linux count the same as Windows.
- When the TCI session identifies itself, the saved label is still `Lyra` or `STD`. Otherwise the label is `Logger`. Either label counts.
- Contest contacts stay out. POTA contacts count in Voice, CW, or Digital.
- Contacts already in the log stay out. An ADIF import stays out. There is no backfill. Only a contact saved after this build gets a stamp.
- The public board and `#qsos` still hide a total of 0.
- Presence (who is on the air) is unchanged. The roster does not feed the totals.

## Files changed in this folder

- `src/SDRLoggerPlus.Server/Services/Qrv/QrvQsoTotals.cs`
- `src/SDRLoggerPlus.Server/Services/Qrv/QsoRadioSourceStamp.cs`
- `src/SDRLoggerPlus.Server/Services/Qrv/QrvQsoTotalsService.cs`
- `src/SDRLoggerPlus.Server.Tests/Tests/Services/QrvQsoTotalsTests.cs`
- `src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx`
- `src/SDRLoggerPlus.Web/src/components/AboutDialog.tsx`
- `docs/qrv-qso-totals.md`

`QrvQsoTotalsTests` was run here: 37 passed.

## After the new build is installed

1. Leave **Share QSO totals** on.
2. Log one new contact. The FT8 contacts already in the log will not appear.
3. Within about a minute, qrvsdr.cc Weekly and Discord `#qsos` should show that call with the new contact under Digital, Voice, or CW.

The website legend was updated separately in `Y:\Claude local\SDRProject\QRVSDR\public\index.html`. Upload that file to Hostinger when the sentence under the leaderboard should say any radio counts. The server already stores whatever snapshot Logger posts. `qso.php` does not need another upload for this fix.

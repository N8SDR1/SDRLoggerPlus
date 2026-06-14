# SDRLoggerPlus

**Modern amateur radio logging.**

> **Alpha Software** — SDRLoggerPlus is under active development. Features may change and bugs are expected.

SDRLoggerPlus is an amateur radio logger for Windows, macOS, and Linux with drag-and-drop panels, real-time DX cluster and RBN, interactive maps, award tracking (DXCC, WAS, WAZ, WPX, WAC, VUCC, 5BWAS, 5BDXCC), WSJT-X auto-logging, CSN S.A.T. satellite support, station weather alerts, scheduled backups, AI talk points, and optional cloud sync via MongoDB.

## Development

```bash
# Backend
cd src/SDRLoggerPlus.Server && dotnet run

# Frontend (hot reload)
cd src/SDRLoggerPlus.Web && npm run dev

# Desktop shell
cd src/SDRLoggerPlus.Desktop && npm run dev:vite
```

See `CLAUDE.md` for the full development and testing guide.

## Credits & Lineage

SDRLoggerPlus stands on two projects:

- **[Log4YM](https://github.com/brianbruff/Log4YM)** by Brian Keating (EI6LF) — the origin of this codebase, released into the public domain (Unlicense). SDRLoggerPlus began as a fork of Log4YM v1.6.2.
- **SDRLogger+** — the origin of several feature designs ported into this logger: the awards counting rules, weather alert tiers, CSN S.A.T. protocol handling, Hot List behavior, and the scheduled-backup scheme.

## License

Public domain (Unlicense), same as the Log4YM origin. See `LICENSE`.

---

*Icon/logo: currently using placeholder artwork inherited from the fork — SDRLoggerPlus artwork TBD.*

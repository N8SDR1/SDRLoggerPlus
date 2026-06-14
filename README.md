# SDRLoggerPlus

**Modern amateur radio logging.**

> **Alpha Software** — SDRLoggerPlus is under active development. Features may change and bugs are expected.

SDRLoggerPlus is an amateur radio logger for Windows, macOS, and Linux with drag-and-drop panels, real-time DX cluster and RBN, interactive maps, award tracking (DXCC, WAS, WAZ, WPX, WAC, VUCC, 5BWAS, 5BDXCC), WSJT-X auto-logging, CSN S.A.T. satellite support, station weather alerts, scheduled backups, and AI talk points.

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

- **Lead developer:** Rick Langford (N8SDR) — creator of SDRLogger+.
- **Contributor:** Brent Crier (N9BC) — built this cross-platform .NET / React / Electron edition.

It builds on the work of two earlier projects:

- **[Log4YM](https://github.com/brianbruff/Log4YM)** by Brian Keating (EI6LF) — the original codebase, released into the public domain (Unlicense). This edition began as a fork of Log4YM v1.6.2.
- **[SDRLogger+](https://github.com/N8SDR1/SDRLoggerPlus)** by Rick Langford (N8SDR) — the project this logger continues; origin of the awards rules, weather alert tiers, CSN S.A.T. handling, Hot List, and scheduled-backup designs.

## License

MIT License — © 2026 Rick Langford (N8SDR) and contributors. See `LICENSE`.
Bundled Hamlib and libusb are LGPL-2.1; see `THIRD-PARTY-NOTICES.txt`.

---

*Icon/logo: currently using placeholder artwork inherited from the fork — SDRLoggerPlus artwork TBD.*

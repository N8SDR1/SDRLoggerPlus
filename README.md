# SDRLoggerPlus

**Modern, cross-platform amateur radio logging.**

> ⚠️ **Alpha software** — SDRLoggerPlus is under active development. Features may change and bugs are expected.

SDRLoggerPlus is a desktop amateur radio logger for **Windows, macOS, and Linux**. It pairs
a fast .NET backend with a React UI of drag-and-drop, dockable panels so you can lay out
exactly the station dashboard you want — logbook, DX cluster, maps, award progress, rig and
rotator control, and more, all updating in real time.

**[⬇ Download](https://github.com/N8SDR1/SDRLoggerPlus/releases)** · **[💬 Join the Discord](https://discord.gg/r3Cuj5NA9p)**

## Features

**Logging**
- Dockable, drag-and-drop panel layout (FlexLayout) with saved workspaces
- Quick log entry with live callsign lookup, plus a full searchable logbook/history
- ADIF import and export, and an ADIF **file monitor** that auto-imports new QSOs from external programs
- QRZ.com profile lookups and callsign images

**DX spotting & alerts**
- Real-time **DX cluster** (telnet) with new-DXCC / new-band / worked spot status
- **Reverse Beacon Network (RBN)** with band-opening alerts and voice announcements
- spothole.app spot source feeding the same dedup pipeline
- DXpeditions and DX news panels
- Hot List with text-to-speech alerts; ITU out-of-band warnings

**Maps & visualization**
- Interactive 2D map and a 3D globe with great-circle beam headings to the focused DX
- Panadapter and meter panels driven by TCI (Thetis / SunSDR) spectrum and audio

**Awards & statistics**
- Award tracking for **DXCC, WAS, WAZ, WPX, WAC, 5BWAS, 5BDXCC, VUCC, POTA, and IOTA**
- Per-band / per-mode detail with filtering

**Radio & station control**
- CAT control via **Hamlib** (bundled)
- **TCI** radio control (Lyra / Thetis / SunSDR) with panadapter and meters
- **Lyra Combo Link** — two-way link with the Lyra SDR console over TCI: band / mode / frequency sync, spot push with click-to-tune, and CW logging with call + RST
- **CW keyer** panel
- **Rotator** control — `rotctld` and ARCO (GS-232A over TCP)
- Station accessory integrations: Antenna Genius, Tuner Genius, PGXL amplifier

**Digital & online services**
- **WSJT-X / JTDX** auto-logging (two independent UDP sources)
- **LoTW** upload (via TQSL), **Club Log** realtime, **HRDLog**, and **PSK Reporter**

**Satellite & propagation**
- CSN S.A.T. controller integration and satellite tracking
- Propagation, solar, aurora, and space-weather panels

**Station weather**
- Severe-weather alerts from NWS, plus Ambient Weather and Ecowitt personal weather stations

**Contesting & productivity**
- Contest logging panel
- **AI talk points** (OpenAI or Anthropic) for conversational prompts
- Scheduled automatic backups; settings export / import

## Architecture

| Project | Role |
|---|---|
| `src/SDRLoggerPlus.Server` | .NET 10 (ASP.NET Core) API + SignalR hubs; DX cluster, radio, and service integrations |
| `src/SDRLoggerPlus.Web` | React + Vite + FlexLayout frontend, SignalR for real-time updates |
| `src/SDRLoggerPlus.Desktop` | Electron shell that packages the backend and serves the web app |
| `src/SDRLoggerPlus.Contracts` | Shared DTOs and contracts |

## Requirements

- **.NET SDK 10** (pinned in `global.json`)
- **Node.js** (LTS) for the web and desktop projects
- Hamlib is bundled with desktop builds for rig control

## Install

SDRLoggerPlus ships as a desktop app (Windows NSIS installer, macOS DMG, Linux AppImage).
Download the latest build from the **[Releases page](https://github.com/N8SDR1/SDRLoggerPlus/releases)**,
or run it from source (below) or build a package with `npm run package:<platform>`.

## Development

Run the three pieces in separate terminals for instant hot reload:

```bash
# Backend API
cd src/SDRLoggerPlus.Server && dotnet run

# Frontend (Vite hot reload)
cd src/SDRLoggerPlus.Web && npm install && npm run dev

# Desktop shell
cd src/SDRLoggerPlus.Desktop && npm install && npm run dev:vite
```

Vite serves the UI at `http://localhost:5173` and proxies `/api` and `/hubs` to the backend.
See `CLAUDE.md` for the full development, packaging, and testing guide.

## Testing

```bash
# Backend unit tests
dotnet test src/SDRLoggerPlus.Server.Tests --filter "Category=Unit"

# Frontend tests
cd src/SDRLoggerPlus.Web && npm test
```

## Data & migration

User data lives in your OS application-data folder under `SDRLoggerPlus`
(`%APPDATA%\SDRLoggerPlus` on Windows, `~/Library/Application Support/SDRLoggerPlus` on macOS,
`~/.config/SDRLoggerPlus` on Linux). On first run, an existing **QSOThief** installation is
automatically copied over (copy, not move — the original is left as a safety net).

## Credits & Lineage

- **Lead developer:** Rick Langford (N8SDR) — creator of SDRLogger+.
- **Contributor:** Brent Crier (N9BC) — cross-platform .NET / React / Electron edition.

It builds on the work of two earlier projects:

- **[Log4YM](https://github.com/brianbruff/Log4YM)** by Brian Keating (EI6LF) — the original codebase, released into the public domain (Unlicense). This edition began as a fork of Log4YM v1.6.2.
- **[SDRLogger+](https://github.com/N8SDR1/SDRLoggerPlus)** by Rick Langford (N8SDR) — the project this logger continues; origin of the awards rules, weather alert tiers, CSN S.A.T. handling, Hot List, and scheduled-backup designs.

## License

MIT License — © 2026 Rick Langford (N8SDR) and contributors. See `LICENSE`.
Bundled Hamlib and libusb are LGPL-2.1; see `THIRD-PARTY-NOTICES.txt`.

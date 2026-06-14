# CLAUDE.md

This file provides guidance to Claude Code when working with this repository.

## Development Commands

### Running with Hot Reload (Recommended for Development)

Run these three commands in separate terminals:

```bash
# Terminal 1: .NET Backend API
cd src/SDRLoggerPlus.Server
dotnet run

# Terminal 2: Vite Dev Server (hot reload for frontend)
cd src/SDRLoggerPlus.Web
npm run dev

# Terminal 3: Electron Desktop App
cd src/SDRLoggerPlus.Desktop
npm run dev:vite
```

This setup provides instant hot reload - any changes to frontend files are reflected immediately without rebuilding.

### Running without Hot Reload

If you need to test the production build flow:

```bash
# Build frontend and copy to server wwwroot
cd src/SDRLoggerPlus.Web
npm run build
cp -r dist/* ../SDRLoggerPlus.Server/wwwroot/

# Run backend (serves static files)
cd src/SDRLoggerPlus.Server
dotnet run

# Run Electron pointing to backend
cd src/SDRLoggerPlus.Desktop
npm run dev
```

### Building for Distribution

```bash
cd src/SDRLoggerPlus.Desktop

# Build for current platform (prepares backend + frontend, then packages)
npm run package:win    # Windows: NSIS installer in dist/
npm run package:mac    # macOS (arm64 DMG)
npm run package:linux  # Linux (AppImage + deb)
```

Each `package:*` script runs `node scripts/prepare-backend.js <rid>` (frontend
build + self-contained backend publish into `src/SDRLoggerPlus.Desktop/backend/`),
then electron-builder. Cross-platform packaging happens in CI (release.yml).

Windows note: if electron-builder fails extracting `winCodeSign` ("Cannot create
symbolic link"), either enable Windows Developer Mode or extract the cached
`.7z` manually into `%LOCALAPPDATA%\electron-builder\Cache\winCodeSign\winCodeSign-2.6.0`
(the failing symlinks are darwin-only and unused on Windows).

## Architecture Overview

- **SDRLoggerPlus.Web**: React frontend using Vite, FlexLayout for panels, SignalR for real-time updates
- **SDRLoggerPlus.Server**: .NET 10 backend API with SignalR hubs, DX cluster integration
- **SDRLoggerPlus.Desktop**: Electron wrapper that loads the web app
- **SDRLoggerPlus.Contracts**: Shared DTOs and contracts

## Key URLs in Development

- Vite Dev Server: http://localhost:5173 (frontend with HMR)
- .NET Backend: http://localhost:5050 (API and SignalR hubs)
- Vite proxies `/api` and `/hubs` requests to the backend automatically

## Testing

### Backend Tests (.NET)

```bash
# Run all unit tests
dotnet test src/SDRLoggerPlus.Server.Tests --filter "Category=Unit"

# Run integration tests
dotnet test src/SDRLoggerPlus.Server.Tests --filter "Category=Integration"

# Run AI live tests (requires API keys, costs money)
AI_LIVE_TESTS=true OPENAI_API_KEY=xxx dotnet test src/SDRLoggerPlus.Server.Tests --filter "Category=LiveAI"

# Run QRZ rate-limited tests (requires QRZ credentials)
QRZ_LIVE_TESTS=true QRZ_USERNAME=xxx QRZ_PASSWORD=xxx dotnet test src/SDRLoggerPlus.Server.Tests --filter "Category=RateLimited"
```

### Manual E2E: TCI meters pipeline

```bash
# With the server already running (default http://localhost:5217), from src/SDRLoggerPlus.Web:
node ../../scripts/e2e-tci-meters.mjs
# Spins up a mock Thetis TCI server on 127.0.0.1:50101, drives ConnectTci over
# SignalR, and asserts OnTciMeters events arrive with correct values/throttling.
```

### Frontend Tests (React)

```bash
cd src/SDRLoggerPlus.Web

npm run test           # Run all tests once
npm run test:watch     # Run in watch mode
npm run test:coverage  # Run with coverage report
```

### Test Categories

| Category | Trigger | Cost | Description |
|----------|---------|------|-------------|
| Unit | Always runs | Free | Pure logic, mocked dependencies |
| Integration | Always runs | Free | Component interactions, mocked external services |
| LiveAI | `AI_LIVE_TESTS=true` | ~$0.01/test | Real OpenAI/Anthropic API calls |
| RateLimited | `QRZ_LIVE_TESTS=true` | Free but rate-limited | Real QRZ API calls (shared fixture, single call) |

### CI/CD

Tests run automatically on PRs targeting `main` via GitHub Actions. Expensive tests (AI, QRZ) are disabled by default and can be enabled via GitHub Secrets or manual workflow dispatch.

Required GitHub Secrets for live tests:
- `AI_LIVE_TESTS` / `OPENAI_API_KEY` / `ANTHROPIC_API_KEY`
- `QRZ_LIVE_TESTS` / `QRZ_USERNAME` / `QRZ_PASSWORD` / `QRZ_API_KEY`

## Git Commit Instructions

- Never push unless instructed

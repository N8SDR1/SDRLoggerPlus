# microHAM ARCO Rotator Support — Design & Plan

**Date:** 2026-06-11
**Source:** Ported from SDRLogger+ (`C:\dev\SDRLoggerPlus\main.py`, the `arco_tcp`
rotator protocol). User decisions: add ARCO as a **selectable protocol** alongside the
existing hamlib rotctld backend; **azimuth-only** for now (elevation/fault captured later).

## Protocol (microHAM ARCO = GS-232A emulation over TCP)

Faithful to `main.py`:
- **Poll:** send `C2\r` → reply `+0nnn+0eee` (AZ then EL, signed 3–4 digit). Parse the
  first number as azimuth (regex `[+\-]?\d{3,4}`). Elevation parsed-but-ignored now.
- **Set azimuth:** send `M###\r` — 3-digit, **truncated** integer degrees
  (`main.py` uses `int(az)`; e.g. 270.6 → `M270\r`). No response.
- **Stop:** send `S\r` (stops all motion). No response.
- Commands are raw `\r`-terminated (not `\r\n`). ARCO allows up to 4 simultaneous TCP
  connections, so a persistent poll connection coexists with other clients.

Contrast with rotctld (existing): `p` → two numeric lines; `P az el`; `S`; all read an
`RPRT` response.

## Architecture: protocol strategy

Extract the three wire operations from `RotatorService` into a strategy. `RotatorService`
keeps connection lifecycle, the poll loop, azimuth normalization, moving/target tracking,
and SignalR broadcasting — it just delegates wire I/O to the selected protocol.

```csharp
// src/SDRLoggerPlus.Server/Services/Rotator/IRotatorProtocol.cs
public interface IRotatorProtocol
{
    // Raw azimuth in degrees (not normalized); null if unavailable.
    Task<double?> PollAzimuthAsync(StreamReader reader, StreamWriter writer, CancellationToken ct);
    Task SetAzimuthAsync(double azimuth, StreamReader reader, StreamWriter writer, CancellationToken ct);
    Task StopAsync(StreamReader reader, StreamWriter writer, CancellationToken ct);
}
```

- **`RotctldProtocol`** — today's exact behavior moved verbatim (uses `WriteLineAsync`,
  reads RPRT). Characterization tests prove the refactor is behavior-preserving.
- **`ArcoTcpProtocol`** — new. Uses `WriteAsync` with explicit `\r`; parses `C2` reply.

`RotatorService` selects the strategy from `_settings.Protocol` and treats a protocol
change like a connection-setting change (reconnect).

## Settings

- `RotatorSettings.Protocol` (`string`, default `"rotctld"`): `"rotctld"` | `"arco_tcp"`.
  Existing installs default to rotctld → **zero behavior change**.
- Port default stays 4533 (rotctld). ARCO uses the user-configured port for its
  GS-232A-over-TCP listener.

## Frontend

- `settingsStore.ts`: add `protocol: 'rotctld' | 'arco_tcp'` to `RotatorSettings` and the
  default object (`protocol: 'rotctld'`).
- `SettingsPanel.tsx`: a Protocol dropdown above the IP/Port grid in the network section
  (hamlib rotctld / microHAM ARCO). Contextual hint when ARCO is selected; the hamlib
  model picker is rotctld-only.

## Stable contract

`LogHub` keeps calling `RotatorService.SetPositionAsync` / `StopAsync` /
`GetCurrentStatus` — unchanged. `RotatorPositionEvent` unchanged (AZ-only).

## Tasks

1. **Protocol strategy + ARCO (TDD).** `IRotatorProtocol`, `RotctldProtocol`,
   `ArcoTcpProtocol`; unit tests against in-memory `StreamReader`/`StreamWriter`:
   - ARCO poll: `C2\r` written; `+0270+0000\r\n` → 270.0; `+0090\r\n` → 90.0;
     garbage/empty → null; tolerates `\r\n` and `\r`.
   - ARCO set: 5.0 → `M005\r`; 270.6 → `M270\r` (truncation); 359.9 → `M359\r`.
   - ARCO stop: `S\r`.
   - rotctld characterization: `p` poll parse; `P {az:F1} 0` set; `S` stop.
2. **Settings field + RotatorService delegation.** Add `Protocol` to `RotatorSettings`;
   refactor `RotatorService` to own connection + loop and delegate wire ops to the
   selected `IRotatorProtocol`; reconnect on protocol change.
3. **Frontend.** TS type + default + Protocol dropdown.
4. **Verify + commit.** Backend unit tests, frontend tests, `tsc`, solution build.

## Testing

No hardware: protocols are pure strategies tested on in-memory streams. `RotatorService`'s
loop is unchanged behavior (characterization via the rotctld tests). Live ARCO validation
on the user's controller is a follow-up.

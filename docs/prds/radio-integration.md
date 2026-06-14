# Radio Integration PRD

**Version:** 1.0
**Author:** SDRLoggerPlus Development Team
**Date:** 2025-12-16
**Status:** Draft

---

## Executive Summary

This PRD defines the requirements for integrating amateur radio transceiver control into SDRLoggerPlus. The Radio Plugin enables automatic frequency, mode, and band tracking from connected radios, eliminating manual data entry during logging.

**Phase 1** focuses on FlexRadio integration via the SmartSDR TCP API.
**Phase 2** adds TCI protocol support for ANAN, Thetis, and Hermes Lite radios.

The integration provides real-time radio state synchronization to the LogEntryPlugin through a "Follow Radio" toggle, automatically populating QSO frequency, band, and mode fields.

---

## Background

### Current State

SDRLoggerPlus currently supports:
- Manual frequency/band/mode entry in LogEntryPlugin
- Basic FlexRadioService with UDP discovery (port 4992) and TCP connection
- TciRadioService with UDP discovery (port 1024) and WebSocket connection
- RadioPlugin UI with FlexRadio/TCI radio type selection and inline discovery

### Problem Statement

Amateur radio operators frequently switch frequencies and modes during operating sessions. Manual entry of this data into logging software is:
1. Time-consuming and interrupts operating flow
2. Error-prone, especially during contests or pileups
3. Inconsistent when operators forget to update fields

### Solution Overview

Automatic radio tracking eliminates these issues by:
- Discovering radios on the local network
- Maintaining persistent connections with real-time state updates
- Pushing frequency/mode changes to the logging interface
- Providing a user-controlled toggle to enable/disable auto-population

---

## Architecture Overview

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': { 'primaryColor': '#4a9eff', 'primaryTextColor': '#fff', 'primaryBorderColor': '#2d7dd2', 'lineColor': '#6b7280', 'secondaryColor': '#50c878', 'tertiaryColor': '#ffa500'}}}%%
flowchart TB
    subgraph Radio["Radio Hardware"]
        FLEX[("FlexRadio\n6000/8000 Series")]
        TCI[("TCI Radio\nANAN / Hermes Lite")]
    end

    subgraph Backend["ASP.NET Core Backend"]
        RS["RadioService\n(Coordinator)"]
        FRS["FlexRadioService\nTCP API"]
        TCIS["TciRadioService\nWebSocket"]
        HUB["LogHub\nSignalR"]
    end

    subgraph Frontend["React Frontend"]
        RP["RadioPlugin\nDiscovery & Status"]
        LEP["LogEntryPlugin\nFollow Radio Toggle"]
        STORE["AppStore\nZustand State"]
    end

    FLEX -- "UDP 4992\nVITA-49 Discovery" --> FRS
    FLEX -- "TCP 4992\nSmartSDR API" --> FRS
    TCI -- "UDP 1024\nDiscovery" --> TCIS
    TCI -- "WS 40001\nTCI Protocol" --> TCIS

    FRS --> RS
    TCIS --> RS
    RS --> HUB
    HUB -- "SignalR\nReal-time Events" --> RP
    HUB -- "SignalR\nReal-time Events" --> LEP
    RP --> STORE
    STORE --> LEP

    style FLEX fill:#3b82f6,stroke:#1d4ed8,color:#fff
    style TCI fill:#8b5cf6,stroke:#6d28d9,color:#fff
    style RS fill:#22c55e,stroke:#16a34a,color:#fff
    style FRS fill:#22c55e,stroke:#16a34a,color:#fff
    style TCIS fill:#22c55e,stroke:#16a34a,color:#fff
    style HUB fill:#22c55e,stroke:#16a34a,color:#fff
    style RP fill:#f97316,stroke:#ea580c,color:#fff
    style LEP fill:#f97316,stroke:#ea580c,color:#fff
    style STORE fill:#f97316,stroke:#ea580c,color:#fff
```

---

## Radio Type Comparison

| Feature | FlexRadio (Phase 1) | TCI Radio (Phase 2) |
|---------|---------------------|---------------------|
| **Target Radios** | FLEX-6000, 8000 series | ANAN, Thetis, Hermes Lite |
| **Discovery** | UDP 4992 (VITA-49) | UDP 1024 |
| **Connection** | TCP (plain text) | WebSocket |
| **Default Port** | 4992 | 40001 |
| **Frequency Format** | `RF_frequency=14.250` (MHz) | `vfo:0,14250000;` (Hz) |
| **Mode Format** | `mode=USB` | `modulation:0,USB;` |
| **Multi-receiver** | Slices (0-7) | Receivers (0-n) |
| **CAT Integration** | SmartSDR / SmartCAT | Native TCI |

---

## Objectives

### Primary Goals

| ID | Objective | Success Metric |
|----|-----------|----------------|
| O1 | FlexRadio CAT slice tracking | Frequency updates within 100ms of radio change |
| O2 | Automatic log field population | 95% reduction in manual frequency entry |
| O3 | Seamless discovery experience | One-click connection after initial setup |
| O4 | Reliable connection management | Auto-reconnect within 5 seconds of disconnect |

### Non-Goals (Phase 1)

- TX/PTT control from SDRLoggerPlus
- Multi-radio simultaneous connections
- Direct rig control (frequency changes from software)
- Omni-Rig or Hamlib integration

---

## User Stories

### US-1: Radio Discovery
**As a** FlexRadio operator
**I want to** discover my radio on the network
**So that** I can connect without manual IP configuration

**Acceptance Criteria:**
- RadioPlugin displays discovered radios within 3 seconds
- Radio list shows model, nickname, IP address
- Refresh button triggers new discovery scan

### US-2: Radio Connection
**As a** radio operator
**I want to** connect to my discovered radio
**So that** SDRLoggerPlus can track my frequency and mode

**Acceptance Criteria:**
- Single click connects to selected radio
- Connection status displayed (Disconnected/Connecting/Connected/Monitoring)
- Error messages shown for connection failures

### US-3: Follow Radio Toggle
**As a** logger
**I want to** toggle automatic frequency tracking
**So that** I can control when the log entry updates

**Acceptance Criteria:**
- Toggle visible in LogEntryPlugin header
- When enabled, frequency/band/mode auto-update from radio
- When disabled, fields remain editable but static
- Toggle state persists across sessions

### US-4: Multi-Slice Radio Handling
**As a** FlexRadio operator with multiple slices
**I want** SDRLoggerPlus to track only my CAT-designated slice
**So that** logging reflects my transmit frequency

**Acceptance Criteria:**
- Only the CAT slice (exposed by SmartSDR/SmartCAT) is tracked
- Slice changes in SmartSDR are reflected in SDRLoggerPlus
- Other slices are ignored for logging purposes

---

## Technical Design

### FlexRadio Protocol Details

```
Discovery (UDP 4992):
+-- VITA-49 discovery packets broadcast
+-- Parse: model, serial, nickname, IP, version
+-- Radios respond every ~3 seconds

Connection (TCP):
+-- Connect to radio IP on discovered port
+-- Send: "sub slice all" (subscribe to slice updates)
+-- Receive: slice status messages
+-- Parse: RF_frequency, mode, tx (TX state)
```

**Slice Status Message Format:**
```
S<handle>|slice <num> RF_frequency=<MHz> mode=<mode> tx=<0|1> ...
```

### Radio Connection State Machine

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': { 'primaryColor': '#3b82f6', 'primaryTextColor': '#fff'}}}%%
stateDiagram-v2
    [*] --> Disconnected

    Disconnected --> Discovering: Start Discovery
    Discovering --> Disconnected: No Radios Found
    Discovering --> RadioFound: Radio Detected

    RadioFound --> Connecting: User Clicks Connect
    RadioFound --> Disconnected: User Cancels

    Connecting --> Connected: Connection Success
    Connecting --> ConnectionFailed: Timeout/Error

    ConnectionFailed --> Connecting: Retry (Auto)
    ConnectionFailed --> Disconnected: Max Retries

    Connected --> Monitoring: Slice Selected
    Connected --> Disconnected: User Disconnect
    Connected --> Reconnecting: Connection Lost

    Monitoring --> Connected: Slice Deselected
    Monitoring --> Reconnecting: Connection Lost

    Reconnecting --> Connected: Reconnect Success
    Reconnecting --> Disconnected: Reconnect Failed
```

### Data Flow: Radio to Log Entry

```mermaid
%%{init: {'theme': 'dark'}}%%
sequenceDiagram
    participant Radio as FlexRadio
    participant FRS as FlexRadioService
    participant Hub as LogHub (SignalR)
    participant Store as AppStore
    participant LEP as LogEntryPlugin

    Radio->>FRS: TCP: slice status update
    Note over FRS: Parse RF_frequency,<br/>mode, tx state
    FRS->>Hub: BroadcastRadioStateChanged
    Hub->>Store: setRadioState(state)

    alt Follow Radio Enabled
        Store->>LEP: radioState updated
        Note over LEP: Auto-update frequency,<br/>band, mode fields
    else Follow Radio Disabled
        Note over LEP: Ignore state update
    end
```

### LogEntryPlugin Integration

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': { 'primaryColor': '#f97316'}}}%%
flowchart LR
    subgraph LogEntryPlugin
        TOGGLE["Follow Radio\nToggle Switch"]
        FREQ["Frequency\nField"]
        BAND["Band\nSelector"]
        MODE["Mode\nSelector"]
    end

    subgraph RadioState["Radio State (from AppStore)"]
        RF["frequencyHz"]
        RB["band"]
        RM["mode"]
    end

    TOGGLE -->|enabled| SYNC{Sync}
    RF --> SYNC
    RB --> SYNC
    RM --> SYNC
    SYNC --> FREQ
    SYNC --> BAND
    SYNC --> MODE

    style TOGGLE fill:#22c55e,stroke:#16a34a,color:#fff
    style SYNC fill:#3b82f6,stroke:#1d4ed8,color:#fff
```

---

## FlexRadio CAT Slice Handling

FlexRadio supports multiple simultaneous slices (virtual receivers). SDRLoggerPlus tracks only the **CAT slice** which is:

| Platform | CAT Slice Source |
|----------|------------------|
| **macOS** | SmartSDR designates active slice |
| **Windows** | SmartCAT exposes CAT-connected slice |

**Detection Strategy:**
1. Subscribe to all slices: `sub slice all`
2. Monitor for `tx=1` state to identify transmit slice
3. Use the TX slice as the logging source
4. Fall back to Slice A (slice 0) if no TX designation

---

## Configuration

### Inline Configuration (RadioPlugin)

The radio connection is configured inline within the RadioPlugin:

1. **Select Radio Type** - FlexRadio or TCI (different protocols)
2. **Discovery** - Scans network for available radios
3. **Select Radio** - Choose from discovered radios
4. **Connect** - Establish connection
5. **Select Slice/Instance** - Choose which receiver to track

### Persistent Settings

```typescript
interface RadioSettings {
  lastRadioType: 'FlexRadio' | 'Tci';
  lastConnectedRadioId: string | null;
  autoConnect: boolean;
  followRadioEnabled: boolean;
}
```

---

## Implementation Plan

### Phase 1: FlexRadio Integration

```mermaid
%%{init: {'theme': 'dark'}}%%
gantt
    title Phase 1: FlexRadio Integration
    dateFormat YYYY-MM-DD
    section Backend
    Complete TCP connection lifecycle    :a1, 2025-01-06, 3d
    Implement slice status parsing       :a2, after a1, 2d
    Add CAT slice detection             :a3, after a2, 2d
    section Frontend
    Add Follow Radio toggle to LEP      :b1, 2025-01-06, 2d
    Implement auto-population logic     :b2, after b1, 2d
    Add settings persistence           :b3, after b2, 1d
    section Testing
    Integration testing                 :c1, after a3, 3d
    End-to-end with real radio         :c2, after c1, 2d
```

#### Tasks

| Task | Description | Priority |
|------|-------------|----------|
| FLEX-1 | Complete FlexRadioService TCP connection lifecycle | P0 |
| FLEX-2 | Parse slice status (RF_frequency, mode, tx) | P0 |
| FLEX-3 | Implement CAT slice detection (TX slice tracking) | P0 |
| LEP-1 | Add Follow Radio toggle to LogEntryPlugin header | P0 |
| LEP-2 | Subscribe to radioState and auto-populate fields | P0 |
| LEP-3 | Persist followRadio setting | P1 |
| TEST-1 | Test with real FlexRadio hardware | P0 |

### Phase 2: TCI Protocol Support (Future)

| Task | Description | Priority |
|------|-------------|----------|
| TCI-1 | Update TciRadioService WebSocket to use port 40001 | High |
| TCI-2 | Implement TCI command parsing | High |
| TCI-3 | Update RadioPlugin for TCI-specific config | Medium |
| TCI-4 | Test with Thetis, ANAN, Hermes Lite | High |

---

## Error Handling

| Error Condition | Recovery Action |
|-----------------|-----------------|
| Discovery timeout | Show "No radios found" with retry button |
| Connection refused | Display error, offer manual IP entry |
| Connection lost | Auto-reconnect with exponential backoff |
| Parse error | Log warning, continue with last known state |
| Invalid frequency | Ignore update, maintain previous value |

---

## Testing Strategy

### Unit Tests
- FlexRadioService message parsing
- Frequency-to-band conversion
- State machine transitions

### Integration Tests
- End-to-end discovery flow
- SignalR event propagation
- RadioContext state synchronization

### Manual Testing Checklist

- [ ] Discover FlexRadio on local network
- [ ] Connect and verify status indicator
- [ ] Change frequency on radio, verify LogEntry updates
- [ ] Change mode on radio, verify LogEntry updates
- [ ] Toggle Follow Radio off, verify fields don't update
- [ ] Disconnect radio, verify graceful handling
- [ ] Reconnect after network interruption
- [ ] Multi-slice: verify only CAT/TX slice tracked

---

## Open Questions

1. **Q:** Should Follow Radio toggle persist per-radio or globally?
   **A:** Globally - simpler UX, most operators want consistent behavior.

2. **Q:** How to handle SO2R (two-radio) setups?
   **A:** Deferred. May require separate LogEntry widgets per radio.

3. **Q:** Should we display non-tracked slices for reference?
   **A:** Future enhancement - show all slices with "tracking" indicator.

---

## References

- [FlexRadio SmartSDR TCP/IP API](https://community.flexradio.com/discussion/8031488/smartsdr-tcp-ip-api-frequency)
- [FlexRadio Developer Program](https://www.flexradio.com/api/developer-program/)
- [TCI Protocol - maksimus1210](https://github.com/maksimus1210/TCI)
- [TCI Protocol - ExpertSDR3](https://github.com/ExpertSDR3/TCI)
- [K3TZR xLib6000](https://github.com/K3TZR/xLib6000)

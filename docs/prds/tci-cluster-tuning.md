# TCI Cluster Tuning Feature

## Status
**Implemented** - v1.5.1

## Links
- Issue: https://github.com/brianbruff/Log4YM/issues/9 (reopened)
- Previous PR: https://github.com/brianbruff/Log4YM/pull/10
- Branch: `feature/tci-cluster-tuning`

## Summary
Implement radio tuning when clicking DX cluster spots, specifically for TCI-connected radios. Also remove the FlexRadio/SmartSDR option from the Radio Plugin UI.

## Requirements

### 1. Remove FlexRadio Option from Radio Plugin
- **File**: `src/SDRLoggerPlus.Web/src/plugins/RadioPlugin.tsx`
- **Change**: Remove the FlexRadio button from the Radio Type selection grid (lines 458-471)
- **Reason**: User doesn't have FlexRadio, focusing on TCI integration

### 2. Implement TCI Tune Functionality
When a user clicks a DX cluster spot and TCI is connected, tune the radio to that frequency.

#### Backend Changes

**TciRadioService.cs** (`src/SDRLoggerPlus.Server/Services/TciRadioService.cs`)
- Add `SetFrequencyAsync(string radioId, long frequencyHz)` method
- Add `SetModeAsync(string radioId, string mode)` method (optional)
- TCI Protocol commands to send:
  ```
  vfo:0,0,{frequency};     // Set VFO-A frequency (rx=0, channel=0)
  modulation:0,{MODE};     // Set mode (optional)
  ```

**TciRadioConnection class** (same file, line 391+)
- Add public method to send frequency command via WebSocket
- Use existing `SendCommandAsync` method (line 482)

**LogHub.cs** (`src/SDRLoggerPlus.Server/Hubs/LogHub.cs`)
- Modify `SelectSpot` method (line 217-229)
- Check if TCI is connected, then call tune method
- Current TODO at line 224-228 shows the intent

#### TCI Protocol Reference
Based on existing code analysis:
- Protocol format: `command:arg1,arg2,...;`
- VFO format: `vfo:rx,channel,frequency;` (rx=0/1, channel=0 for VFO-A)
- Modulation format: `modulation:rx,MODE;`
- Commands are sent as UTF-8 text over WebSocket

## Implementation Plan

### Step 1: Remove FlexRadio UI Option
Edit `RadioPlugin.tsx`:
- Remove FlexRadio button from grid (lines 458-471)
- Change grid from `grid-cols-3` to `grid-cols-2`
- Keep TCI and Hamlib options

### Step 2: Add TCI SetFrequency Method
In `TciRadioService.cs`:
```csharp
public async Task<bool> SetFrequencyAsync(string radioId, long frequencyHz)
{
    if (!_connections.TryGetValue(radioId, out var connection))
        return false;

    return await connection.SetFrequencyAsync(frequencyHz);
}
```

In `TciRadioConnection` class:
```csharp
public async Task<bool> SetFrequencyAsync(long frequencyHz)
{
    if (!IsConnected) return false;

    var command = $"vfo:0,0,{frequencyHz};";
    await SendCommandAsync(command);
    return true;
}
```

### Step 3: Integrate Tuning in LogHub.SelectSpot
```csharp
public async Task SelectSpot(SpotSelectedEvent evt)
{
    _logger.LogInformation("Spot selected: {DxCall} on {Frequency} kHz ({Mode})",
        evt.DxCall, evt.Frequency / 1000.0, evt.Mode ?? "unknown");

    // Broadcast to clients for log entry population
    await Clients.All.OnSpotSelected(evt);

    // Tune connected TCI radio (frequency is in Hz)
    var tciRadios = _tciRadioService.GetRadioStates().ToList();
    if (tciRadios.Any())
    {
        var radioId = tciRadios.First().RadioId;
        await _tciRadioService.SetFrequencyAsync(radioId, (long)evt.Frequency);
        _logger.LogInformation("Tuned TCI radio {RadioId} to {Frequency} Hz", radioId, evt.Frequency);
    }
}
```

### Step 4: Update Version
- Update version to 1.5.1 in relevant files

## Files to Modify

| File | Changes |
|------|---------|
| `src/SDRLoggerPlus.Web/src/plugins/RadioPlugin.tsx` | Remove FlexRadio button |
| `src/SDRLoggerPlus.Server/Services/TciRadioService.cs` | Add SetFrequencyAsync method |
| `src/SDRLoggerPlus.Server/Hubs/LogHub.cs` | Call tune in SelectSpot |
| Version files (TBD) | Bump to 1.5.1 |

## Testing
1. Connect to TCI radio (e.g., Thetis/ANAN)
2. Open DX Cluster plugin
3. Click on a spot
4. Verify:
   - Log entry is populated with callsign/frequency/mode
   - Radio tunes to the spot frequency

## Notes
- SmartUnlink (`SmartUnlinkService.cs`) is a separate test/simulation utility, not SmartSDR radio control
- FlexRadio discovery uses UDP port 4992
- TCI uses WebSocket on port 50001 (default)
- The existing TciRadioConnection already has `SendCommandAsync` for sending commands

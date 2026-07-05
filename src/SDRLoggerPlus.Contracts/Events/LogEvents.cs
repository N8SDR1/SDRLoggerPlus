namespace SDRLoggerPlus.Contracts.Events;

/// <summary>
/// Emitted when user focuses on a callsign (typing, clicking spot, etc.)
/// </summary>
public record CallsignFocusedEvent(
    string Callsign,
    string Source,
    string? Grid = null,
    double? Frequency = null,
    string? Mode = null
);

/// <summary>
/// Emitted after successful QRZ/callbook lookup. When QRZ (and HamQTH when
/// enabled) return nothing, the backend falls back to the AD1C country
/// centroid from cty.dat — in that case Latitude/Longitude are populated
/// but LatLonIsApproximate is true so the UI can render the bearing line
/// with an "approximate" visual style.
/// </summary>
public record CallsignLookedUpEvent(
    string Callsign,
    string? Name,
    string? Grid,
    double? Latitude,
    double? Longitude,
    string? Country,
    int? Dxcc,
    int? CqZone,
    int? ItuZone,
    string? State,
    string? ImageUrl,
    double? Bearing = null,
    double? Distance = null,
    bool LatLonIsApproximate = false
);

/// <summary>
/// Emitted when a QSO is logged
/// </summary>
public record QsoLoggedEvent(
    string Id,
    string Callsign,
    DateTime QsoDate,
    string TimeOn,
    string Band,
    string Mode,
    double? Frequency,
    string? RstSent,
    string? RstRcvd,
    string? Grid
);

/// <summary>
/// Current station location
/// </summary>
public record StationLocationEvent(
    string Callsign,
    string Grid,
    double Latitude,
    double Longitude
);

/// <summary>
/// New DX spot received
/// </summary>
public record SpotReceivedEvent(
    string Id,
    string DxCall,
    string Spotter,
    double Frequency,
    string? Mode,
    string? Comment,
    DateTime Timestamp,
    string Source,
    string? Country,
    int? Dxcc,
    string? Grid,
    string? SpotterCountry = null,
    int? SpotterDxcc = null,
    string? SpotterGrid = null,
    string? SpotterContinent = null,
    string? SpotStatus = null,
    bool IsHot = false
);

/// <summary>
/// Hot List membership or flags changed
/// </summary>
public record HotListChangedEvent(
    bool Enabled,
    bool TtsEnabled,
    List<string> Callsigns
);

/// <summary>
/// User clicked on a spot
/// </summary>
public record SpotSelectedEvent(
    string DxCall,
    double Frequency,
    string? Mode,
    string? Grid
);

/// <summary>
/// Current rotator position
/// </summary>
public record RotatorPositionEvent(
    string RotatorId,
    double CurrentAzimuth,
    bool IsMoving,
    double? TargetAzimuth = null
);

/// <summary>
/// Request to move rotator
/// </summary>
public record RotatorCommandEvent(
    string RotatorId,
    double TargetAzimuth,
    string Source
);

/// <summary>
/// Power spectrum for the panadapter, computed from the Thetis TCI IQ stream.
/// </summary>
public record SpectrumDataEvent(
    long LowFrequencyHz,
    long HighFrequencyHz,
    int[] Data
);

/// <summary>
/// Current rig frequency/mode
/// </summary>
public record RigStatusEvent(
    string RigId,
    double Frequency,
    string Mode,
    bool IsTransmitting
);

// ===== Antenna Genius Events =====

/// <summary>
/// Antenna Genius device discovered on network
/// </summary>
public record AntennaGeniusDiscoveredEvent(
    string IpAddress,
    int Port,
    string Version,
    string Serial,
    string Name,
    int RadioPorts,
    int AntennaPorts,
    string Mode,
    int Uptime
);

/// <summary>
/// Antenna Genius device disconnected
/// </summary>
public record AntennaGeniusDisconnectedEvent(
    string Serial
);

/// <summary>
/// Full status update from Antenna Genius
/// </summary>
public record AntennaGeniusStatusEvent(
    string DeviceSerial,
    string DeviceName,
    string IpAddress,
    string Version,
    bool IsConnected,
    List<AntennaGeniusAntennaInfo> Antennas,
    List<AntennaGeniusBandInfo> Bands,
    AntennaGeniusPortStatus PortA,
    AntennaGeniusPortStatus PortB
);

/// <summary>
/// Antenna info for events (without BSON attributes)
/// </summary>
public record AntennaGeniusAntennaInfo(
    int Id,
    string Name,
    ushort TxBandMask,
    ushort RxBandMask,
    ushort InbandMask
);

/// <summary>
/// Band info for events
/// </summary>
public record AntennaGeniusBandInfo(
    int Id,
    string Name,
    double FreqStart,
    double FreqStop
);

/// <summary>
/// Port status for events
/// </summary>
public record AntennaGeniusPortStatus(
    int PortId,
    bool Auto,
    string Source,
    int Band,
    int RxAntenna,
    int TxAntenna,
    bool IsTransmitting,
    bool IsInhibited
);

/// <summary>
/// Port status changed (antenna selection, band change, etc.)
/// </summary>
public record AntennaGeniusPortChangedEvent(
    string DeviceSerial,
    int PortId,
    bool Auto,
    string Source,
    int Band,
    int RxAntenna,
    int TxAntenna,
    bool IsTransmitting,
    bool IsInhibited
);

/// <summary>
/// Request to select antenna for a port (client to server)
/// </summary>
public record SelectAntennaCommand(
    string DeviceSerial,
    int PortId,
    int AntennaId
);

// ===== PGXL Amplifier Events =====

/// <summary>
/// PGXL amplifier discovered on network
/// </summary>
public record PgxlDiscoveredEvent(
    string IpAddress,
    int Port,
    string Serial,
    string Model
);

/// <summary>
/// PGXL amplifier disconnected
/// </summary>
public record PgxlDisconnectedEvent(
    string Serial
);

/// <summary>
/// Full status update from PGXL amplifier
/// </summary>
public record PgxlStatusEvent(
    string Serial,
    string IpAddress,
    bool IsConnected,
    bool IsOperating,
    bool IsTransmitting,
    string Band,
    string BiasA,
    string BiasB,
    PgxlMeters Meters,
    PgxlSetup Setup
);

/// <summary>
/// PGXL meter readings
/// </summary>
public record PgxlMeters(
    double ForwardPowerDbm,
    double ForwardPowerWatts,
    double ReturnLossDb,
    double SwrRatio,
    double DrivePowerDbm,
    double PaCurrent,
    double TemperatureC
);

/// <summary>
/// PGXL setup/configuration
/// </summary>
public record PgxlSetup(
    string BandSource,
    int SelectedAntenna,
    bool AttenuatorEnabled,
    int BiasOffset,
    int PttDelay,
    int KeyDelay,
    bool HighSwr,
    bool OverTemp,
    bool OverCurrent
);

/// <summary>
/// Request to set PGXL operate mode (client to server)
/// </summary>
public record SetPgxlOperateCommand(
    string Serial
);

/// <summary>
/// Request to set PGXL standby mode (client to server)
/// </summary>
public record SetPgxlStandbyCommand(
    string Serial
);

// ===== Radio CAT Control Events =====

/// <summary>
/// Type of radio/protocol
/// </summary>
public enum RadioType
{
    Tci,
    Hamlib
}

/// <summary>
/// Radio connection state
/// </summary>
public enum RadioConnectionState
{
    Disconnected,
    Discovering,
    Connecting,
    Connected,
    Monitoring,
    Error
}

/// <summary>
/// Radio discovered on network
/// </summary>
public record RadioDiscoveredEvent(
    string Id,
    RadioType Type,
    string Model,
    string IpAddress,
    int Port,
    string? Nickname
);

/// <summary>
/// Radio no longer available
/// </summary>
public record RadioRemovedEvent(
    string Id
);

/// <summary>
/// Radio connection state changed
/// </summary>
public record RadioConnectionStateChangedEvent(
    string RadioId,
    RadioConnectionState State,
    string? ErrorMessage = null
);

/// <summary>
/// Radio frequency/mode/TX state update
/// </summary>
public record RadioStateChangedEvent(
    string RadioId,
    long FrequencyHz,
    string Mode,
    bool IsTransmitting,
    string Band,
    string? SliceOrInstance
);

/// <summary>
/// Command to start radio discovery
/// </summary>
public record StartRadioDiscoveryCommand(
    RadioType Type
);

/// <summary>
/// Command to stop radio discovery
/// </summary>
public record StopRadioDiscoveryCommand(
    RadioType Type
);

/// <summary>
/// Command to connect to a radio
/// </summary>
public record ConnectRadioCommand(
    string RadioId
);

/// <summary>
/// Command to disconnect from a radio
/// </summary>
public record DisconnectRadioCommand(
    string RadioId
);

/// <summary>
/// Command to select an instance to monitor (TCI)
/// </summary>
public record SelectRadioInstanceCommand(
    string RadioId,
    int Instance
);

// ===== CW Keyer Commands and Events =====

/// <summary>
/// Command to send CW/Morse code text
/// </summary>
public record SendCwKeyCommand(
    string RadioId,
    string Message,
    int? SpeedWpm = null
);

/// <summary>
/// Command to stop CW keying immediately
/// </summary>
public record StopCwKeyCommand(
    string RadioId
);

/// <summary>
/// Command to set CW keyer speed
/// </summary>
public record SetCwSpeedCommand(
    string RadioId,
    int SpeedWpm
);

/// <summary>
/// CW keyer status event
/// </summary>
public record CwKeyerStatusEvent(
    string RadioId,
    bool IsKeying,
    int SpeedWpm,
    string? CurrentMessage = null
);

// ===== Hamlib Configuration Events =====

/// <summary>
/// Hamlib connection type
/// </summary>
public enum HamlibConnectionType
{
    Serial,
    Network
}

/// <summary>
/// Hamlib data bits options
/// </summary>
public enum HamlibDataBits
{
    Five = 5,
    Six = 6,
    Seven = 7,
    Eight = 8
}

/// <summary>
/// Hamlib stop bits options
/// </summary>
public enum HamlibStopBits
{
    One = 1,
    Two = 2
}

/// <summary>
/// Hamlib flow control options
/// </summary>
public enum HamlibFlowControl
{
    None,
    Hardware,
    Software
}

/// <summary>
/// Hamlib parity options
/// </summary>
public enum HamlibParity
{
    None,
    Even,
    Odd,
    Mark,
    Space
}

/// <summary>
/// Hamlib PTT type
/// </summary>
public enum HamlibPttType
{
    None,
    Rig,
    Dtr,
    Rts
}

/// <summary>
/// Information about a Hamlib rig model
/// </summary>
public record HamlibRigModelInfo(
    int ModelId,
    string Manufacturer,
    string Model,
    string Version,
    string DisplayName
);

/// <summary>
/// Hamlib rig capabilities
/// </summary>
public record HamlibRigCapabilities(
    bool CanGetFreq,
    bool CanGetMode,
    bool CanGetVfo,
    bool CanGetPtt,
    bool CanGetPower,
    bool CanGetRit,
    bool CanGetXit,
    bool CanGetKeySpeed,
    bool CanSendMorse,
    int DefaultDataBits,
    int DefaultStopBits,
    bool IsNetworkOnly,
    bool SupportsSerial,
    bool SupportsNetwork
);

/// <summary>
/// Hamlib rig configuration
/// </summary>
public record HamlibRigConfigDto(
    int ModelId,
    string ModelName,
    HamlibConnectionType ConnectionType,
    string? SerialPort,
    int BaudRate,
    HamlibDataBits DataBits,
    HamlibStopBits StopBits,
    HamlibFlowControl FlowControl,
    HamlibParity Parity,
    string? Hostname,
    int NetworkPort,
    HamlibPttType PttType,
    string? PttPort,
    bool GetFrequency,
    bool GetMode,
    bool GetVfo,
    bool GetPtt,
    bool GetPower,
    bool GetRit,
    bool GetXit,
    bool GetKeySpeed,
    int PollIntervalMs
);

/// <summary>
/// Hamlib rig list response
/// </summary>
public record HamlibRigListEvent(
    List<HamlibRigModelInfo> Rigs
);

/// <summary>
/// Hamlib rig capabilities response
/// </summary>
public record HamlibRigCapsEvent(
    int ModelId,
    HamlibRigCapabilities Capabilities
);

/// <summary>
/// Available serial ports
/// </summary>
public record HamlibSerialPortsEvent(
    List<string> Ports
);

/// <summary>
/// Hamlib configuration loaded
/// </summary>
public record HamlibConfigLoadedEvent(
    HamlibRigConfigDto? Config
);

/// <summary>
/// Hamlib library initialization status
/// </summary>
public record HamlibStatusEvent(
    bool IsInitialized,
    bool IsConnected,
    string? RadioId,
    string? ErrorMessage
);

/// <summary>
/// DX Cluster connection status changed
/// </summary>
public record ClusterStatusChangedEvent(
    string ClusterId,
    string Name,
    string Status,  // "connected" | "connecting" | "disconnected" | "error"
    string? ErrorMessage = null
);

// ===== QRZ Sync Events =====

/// <summary>
/// QRZ sync progress update
/// </summary>
public record QrzSyncProgressEvent(
    int Total,
    int Completed,
    int Successful,
    int Failed,
    bool IsComplete,
    string? CurrentCallsign,
    string? Message
);

// ===== ADIF Import Events =====

/// <summary>
/// ADIF import progress update
/// </summary>
public record AdifImportProgressEvent(
    int Total,
    int Processed,
    int Imported,
    int Skipped,
    int Failed,
    bool IsComplete,
    string? CurrentCallsign,
    string? Message
);

// ===== LOTW Upload Events =====

/// <summary>
/// LOTW upload progress update. Stages: "preparing", "signing", "uploading", "done", "error".
/// TQSL runs as an opaque subprocess, so per-QSO progress is not available — the event
/// reports coarse phase transitions plus totals and the TQSL exit code on completion.
/// </summary>
public record LotwUploadProgressEvent(
    string Stage,
    int QsoCount,
    bool IsComplete,
    int? TqslExitCode,
    string? Message
);

// ===== Tuner Genius Events =====

/// <summary>
/// Tuner Genius device discovered on network
/// </summary>
public record TunerGeniusDiscoveredEvent(
    string IpAddress,
    int Port,
    string Version,
    string Serial,
    string Name,
    string Model,
    int Uptime
);

/// <summary>
/// Tuner Genius device disconnected
/// </summary>
public record TunerGeniusDisconnectedEvent(
    string Serial
);

/// <summary>
/// Full status update from Tuner Genius XL.
/// The TGXL is a single tuner with one L/C network serving up to two radios.
/// Swr, Power, L/C positions and bypass/operate state are tuner-level (not per-radio).
/// FreqAMhz / FreqBMhz are the frequencies reported by the two radio inputs.
/// </summary>
public record TunerGeniusStatusEvent(
    string DeviceSerial,
    string DeviceName,
    string IpAddress,
    string Version,
    string Model,
    bool IsConnected,
    // Tuner state
    bool IsOperating,          // true = Operate, false = Standby
    bool IsBypassed,           // true = tuner bypassed (out of circuit)
    bool IsTuning,             // true = tune cycle in progress
    int ActiveRadio,           // 1 or 2
    // Metering
    double ForwardPowerWatts,
    double Swr,                // e.g. 1.5
    // Matching network positions (0-255)
    int L,
    int C1,
    int C2,
    // Per-radio frequency inputs
    double FreqAMhz,
    double FreqBMhz,
    // Legacy port wrappers (PortA = Radio 1, PortB = Radio 2)
    TunerGeniusPortStatus PortA,
    TunerGeniusPortStatus? PortB
);

/// <summary>
/// Per-radio input status (frequency and band).
/// L/C/SWR/power are tuner-level — see TunerGeniusStatusEvent.
/// </summary>
public record TunerGeniusPortStatus(
    int PortId,
    bool Auto,
    string Band,
    double FrequencyMhz,
    int Swr,               // SWR * 10 (e.g. 15 = 1.5:1) — kept for UI compat
    bool IsTuning,
    bool IsTransmitting,
    int? SelectedAntenna,
    string TuneResult      // "OK", "HighSWR", "Timeout", "Error"
);

/// <summary>
/// Fired on every status poll — carries full tuner + radio state.
/// </summary>
public record TunerGeniusPortChangedEvent(
    string DeviceSerial,
    int PortId,
    bool Auto,
    string Band,
    double FrequencyMhz,
    int Swr,
    bool IsTuning,
    bool IsTransmitting,
    int? SelectedAntenna,
    string TuneResult,
    // Tuner-level additions
    bool IsBypassed,
    bool IsOperating,
    double ForwardPowerWatts,
    double SwrDecimal,     // SWR as double e.g. 1.5
    int L,
    int C1,
    int C2,
    int ActiveRadio
);

/// <summary>
/// Command to initiate auto-tune (client to server)
/// </summary>
public record TuneTunerGeniusCommand(
    string DeviceSerial,
    int PortId
);

/// <summary>
/// Command to toggle bypass state (client to server)
/// </summary>
public record BypassTunerGeniusCommand(
    string DeviceSerial,
    int PortId,
    bool Bypass
);

/// <summary>
/// Command to set Operate / Standby mode (client to server)
/// </summary>
public record OperateTunerGeniusCommand(
    string DeviceSerial,
    bool Operate
);

/// <summary>
/// Command to activate a radio channel (client to server)
/// </summary>
public record ActivateChannelTunerGeniusCommand(
    string DeviceSerial,
    int Channel   // 1 or 2
);

/// <summary>
/// ADIF File Monitor imported newly appended QSOs from a watched external file.
/// </summary>
public record AdifMonitorImportEvent(
    string FileName,
    int Imported,
    int SkippedDuplicates
);

/// <summary>
/// RBN band-opening alert: a skimmer within the configured distance of the
/// station heard a signal on a watched VHF/UHF band.
/// </summary>
public record BandOpeningEvent(
    string Band,
    string DxCall,
    string Skimmer,
    double Distance,
    string Unit,
    int Snr,
    string Mode
);

/// <summary>
/// Live meter readings from a TCI radio's sensor stream (Thetis
/// rx_sensors / rx_channel_sensors_ex / tx_sensors frames), coalesced
/// server-side to at most one event per 100 ms. Null fields mean that
/// sensor has not reported yet.
/// </summary>
public record TciMetersEvent(
    string RadioId,
    double? RxSignalDbm,
    double? RxAvgSignalDbm,
    double? TxMicDbm,
    double? TxPowerWatts,
    double? TxPeakPowerWatts,
    double? TxSwr,
    bool IsTransmitting,
    DateTime TimestampUtc
);

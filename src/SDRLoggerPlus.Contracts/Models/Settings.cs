using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace SDRLoggerPlus.Contracts.Models;

[BsonIgnoreExtraElements]
public class UserSettings
{
    [BsonId]
    public string Id { get; set; } = "default";

    [BsonElement("station")]
    public StationSettings Station { get; set; } = new();

    [BsonElement("qrz")]
    public QrzSettings Qrz { get; set; } = new();

    [BsonElement("hamqth")]
    public HamQthSettings HamQth { get; set; } = new();

    [BsonElement("lotw")]
    public LotwSettings Lotw { get; set; } = new();

    [BsonElement("appearance")]
    public AppearanceSettings Appearance { get; set; } = new();

    [BsonElement("rotator")]
    public RotatorSettings Rotator { get; set; } = new();

    [BsonElement("radio")]
    public RadioSettings Radio { get; set; } = new();

    [BsonElement("map")]
    public MapSettings Map { get; set; } = new();

    [BsonElement("cluster")]
    public ClusterSettings Cluster { get; set; } = new();

    [BsonElement("spotStatus")]
    public SpotStatusSettings SpotStatus { get; set; } = new();

    [BsonElement("header")]
    public HeaderSettings Header { get; set; } = new();

    [BsonElement("ai")]
    public AiSettings Ai { get; set; } = new();

    [BsonElement("clubLog")]
    public ClubLogSettings ClubLog { get; set; } = new();

    [BsonElement("hrdLog")]
    public HrdLogSettings HrdLog { get; set; } = new();

    [BsonElement("eqsl")]
    public EqslSettings Eqsl { get; set; } = new();

    [BsonElement("pota")]
    public PotaSettings Pota { get; set; } = new();

    [BsonElement("adifMonitor")]
    public AdifMonitorSettings AdifMonitor { get; set; } = new();

    [BsonElement("adifUdp")]
    public AdifUdpSettings AdifUdp { get; set; } = new();

    [BsonElement("rbnAlerts")]
    public RbnAlertSettings RbnAlerts { get; set; } = new();

    [BsonElement("backup")]
    public BackupSettings Backup { get; set; } = new();

    [BsonElement("hotList")]
    public HotListSettings HotList { get; set; } = new();

    [BsonElement("wsjtx")]
    public WsjtxSettings Wsjtx { get; set; } = new();

    [BsonElement("weather")]
    public WeatherSettings Weather { get; set; } = new();

    [BsonElement("sat")]
    public SatSettings Sat { get; set; } = new();

    [BsonElement("layoutJson")]
    public string? LayoutJson { get; set; }

    // Named layout presets — operator can save up to 3 arrangements
    // (POTA setup, DXpedition setup, contest setup, etc.) and click to
    // switch between them. Separate from LayoutJson, which is the
    // auto-saved "current live arrangement" that persists across restarts.
    // Enforced max 3 on the write path (SettingsController).
    [BsonElement("savedLayouts")]
    public List<SavedLayoutSlot> SavedLayouts { get; set; } = new();

    [BsonElement("gridStates")]
    public Dictionary<string, string>? GridStates { get; set; }

    [BsonElement("window")]
    public WindowState? Window { get; set; }

    [BsonElement("updatedAt")]
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// Desktop window geometry, persisted so the app reopens at its last size/position.
/// Position is clamped to a visible display on restore by the desktop shell.
/// </summary>
public class WindowState
{
    [BsonElement("width")]
    public int? Width { get; set; }

    [BsonElement("height")]
    public int? Height { get; set; }

    [BsonElement("x")]
    public int? X { get; set; }

    [BsonElement("y")]
    public int? Y { get; set; }

    [BsonElement("maximized")]
    public bool Maximized { get; set; }
}

[BsonIgnoreExtraElements]
public class StationSettings
{
    [BsonElement("callsign")]
    public string? Callsign { get; set; } = string.Empty;

    [BsonElement("operatorName")]
    public string? OperatorName { get; set; } = string.Empty;

    [BsonElement("gridSquare")]
    public string? GridSquare { get; set; } = string.Empty;

    // ITU Region (1=EU/Africa/Russia, 2=Americas, 3=Asia-Pacific) — band plan & OOB VFO alerts
    [BsonElement("ituRegion")]
    public int ItuRegion { get; set; } = 2;

    [BsonElement("latitude")]
    public double? Latitude { get; set; }

    [BsonElement("longitude")]
    public double? Longitude { get; set; }

    [BsonElement("city")]
    public string? City { get; set; } = string.Empty;

    [BsonElement("country")]
    public string? Country { get; set; } = string.Empty;
}

[BsonIgnoreExtraElements]
public class QrzSettings
{
    [BsonElement("username")]
    public string? Username { get; set; } = string.Empty;

    [BsonElement("password")]
    public string? Password { get; set; } = string.Empty; // Stored obfuscated

    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    [BsonElement("apiKey")]
    public string? ApiKey { get; set; } = string.Empty; // QRZ API key for logbook uploads

    [BsonElement("hasXmlSubscription")]
    public bool? HasXmlSubscription { get; set; } // Cached subscription status

    [BsonElement("subscriptionCheckedAt")]
    public DateTime? SubscriptionCheckedAt { get; set; }
}

[BsonIgnoreExtraElements]
public class HamQthSettings
{
    // HamQTH.com callbook credentials. Free account, session-based XML API.
    // Used as a fallback lookup source when QRZ is not configured or returns
    // nothing. Session ID is cached in-memory only (not persisted).
    [BsonElement("username")]
    public string? Username { get; set; } = string.Empty;

    [BsonElement("password")]
    public string? Password { get; set; } = string.Empty; // Stored obfuscated

    [BsonElement("enabled")]
    public bool Enabled { get; set; }
}

[BsonIgnoreExtraElements]
public class LotwSettings
{
    // Absolute path to the local TQSL binary (e.g. C:\Program Files\TrustedQSL\tqsl.exe).
    // Entered manually in Settings; no auto-detection in v1.
    [BsonElement("tqslPath")]
    public string? TqslPath { get; set; } = string.Empty;

    // Optional TQSL station location name passed via `-l`. Null = TQSL default.
    [BsonElement("stationCallsign")]
    public string? StationCallsign { get; set; } = string.Empty;

    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    [BsonElement("lastUploadAt")]
    public DateTime? LastUploadAt { get; set; }
}

[BsonIgnoreExtraElements]
public class RbnAlertSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    [BsonElement("server")]
    public string? Server { get; set; } = "telnet.reversebeacon.net";

    [BsonElement("port")]
    public int Port { get; set; } = 7000;

    // SDRLogger+ defaults: 6m/2m/70cm on, 10m off
    [BsonElement("band10m")]
    public bool Band10m { get; set; }

    [BsonElement("band6m")]
    public bool Band6m { get; set; } = true;

    [BsonElement("band2m")]
    public bool Band2m { get; set; } = true;

    [BsonElement("band70cm")]
    public bool Band70cm { get; set; } = true;

    // Alert when a skimmer within this distance hears a signal
    [BsonElement("distance")]
    public double Distance { get; set; } = 500;

    [BsonElement("distanceUnit")]
    public string? DistanceUnit { get; set; } = "mi"; // mi | km

    [BsonElement("cooldownMinutes")]
    public int CooldownMinutes { get; set; } = 15;

    [BsonElement("voice")]
    public bool Voice { get; set; } = true;
}

public class AdifMonitorSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    // Up to two watched ADIF files (e.g. VarAC, MSHV)
    [BsonElement("file1")]
    public string? File1 { get; set; } = string.Empty;

    [BsonElement("file2")]
    public string? File2 { get; set; } = string.Empty;
}

/// <summary>
/// Generic ADIF-over-UDP listener — ports v1 SDRLogger+'s port-52001
/// auto-import that catches broadcasts from VarAC, DXKeeper, N1MM,
/// Logger32, and any other logger that emits an ADIF record over UDP
/// when a QSO is logged. Uses the same ImportAdifAsync pipeline as
/// file-based ADIF Monitor + explicit import, so dedup / DXCC lookup
/// / broadcast-to-clients all apply automatically.
/// </summary>
public class AdifUdpSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    // 52001 = v1 default; matches VarAC/N1MM/etc. common port choice.
    [BsonElement("port")]
    public int Port { get; set; } = 52001;
}

public class ClubLogSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    [BsonElement("email")]
    public string? Email { get; set; } = string.Empty;

    [BsonElement("password")]
    public string? Password { get; set; } = string.Empty;

    // Defaults to the station callsign when empty
    [BsonElement("callsign")]
    public string? Callsign { get; set; } = string.Empty;

    // Club Log application key — issued to application authors on request
    // (clublog.freshdesk.com article 54906); user-supplied.
    [BsonElement("apiKey")]
    public string? ApiKey { get; set; } = string.Empty;
}

public class HrdLogSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    // Defaults to the station callsign when empty
    [BsonElement("callsign")]
    public string? Callsign { get; set; } = string.Empty;

    // Per-account upload code from the hrdlog.net account page (My Account → Online Log).
    [BsonElement("uploadCode")]
    public string? UploadCode { get; set; } = string.Empty;
}

/// <summary>
/// POTA.app credentials for self-spotting the operator's own activation.
/// v1 SDRLogger+ used basic-auth on POST api.pota.app/spot; empty
/// credentials disable the "Spot Myself" button in the POTA banner.
/// </summary>
public class PotaSettings
{
    [BsonElement("username")]
    public string? Username { get; set; } = string.Empty;

    [BsonElement("password")]
    public string? Password { get; set; } = string.Empty;
}

/// <summary>
/// eQSL.cc credentials + upload behavior. eQSL uses form-based auth
/// (username = your callsign, password = the eQSL password) and
/// accepts single-record ADIF via ImportADIF.cfm — same shape as the
/// Club Log realtime uploader, ported into v2.
/// </summary>
public class EqslSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    // Your eQSL.cc username — typically the same as your callsign.
    [BsonElement("username")]
    public string? Username { get; set; } = string.Empty;

    [BsonElement("password")]
    public string? Password { get; set; } = string.Empty;

    // Optional QTH nickname (eQSL supports multiple QTHs per account).
    // Empty = eQSL uses the account's default QTH.
    [BsonElement("qthNickname")]
    public string? QthNickname { get; set; } = string.Empty;
}

public class AppearanceSettings
{
    [BsonElement("theme")]
    public string? Theme { get; set; } = "dark";  // light | dark | preset id | custom

    [BsonElement("compactMode")]
    public bool CompactMode { get; set; }

    // Hex colors for theme == "custom": accent, background, panel, text
    [BsonElement("customColors")]
    public Dictionary<string, string>? CustomColors { get; set; }
}

[BsonIgnoreExtraElements]
public class RotatorPreset
{
    [BsonElement("name")]
    public string? Name { get; set; } = string.Empty;

    [BsonElement("azimuth")]
    public int Azimuth { get; set; }
}

[BsonIgnoreExtraElements]
public class RotatorSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    [BsonElement("connectionType")]
    public string? ConnectionType { get; set; } = "network";  // "network" | "serial"

    [BsonElement("protocol")]
    public string Protocol { get; set; } = "rotctld";  // "rotctld" | "arco_tcp" (microHAM ARCO, GS-232A over TCP)

    [BsonElement("ipAddress")]
    public string? IpAddress { get; set; } = "127.0.0.1";

    [BsonElement("port")]
    public int Port { get; set; } = 4533;  // Default hamlib rotctld port

    [BsonElement("serialPort")]
    public string? SerialPort { get; set; } = string.Empty;  // e.g., 'COM3' or '/dev/ttyUSB0'

    [BsonElement("baudRate")]
    public int BaudRate { get; set; } = 9600;

    [BsonElement("hamlibModelId")]
    public int? HamlibModelId { get; set; }  // Hamlib rotator model ID

    [BsonElement("hamlibModelName")]
    public string? HamlibModelName { get; set; } = string.Empty;

    [BsonElement("pollingIntervalMs")]
    public int PollingIntervalMs { get; set; } = 500;

    [BsonElement("rotatorId")]
    public string? RotatorId { get; set; } = "default";

    [BsonElement("presets")]
    public List<RotatorPreset> Presets { get; set; } = new()
    {
        new RotatorPreset { Name = "N", Azimuth = 0 },
        new RotatorPreset { Name = "E", Azimuth = 90 },
        new RotatorPreset { Name = "S", Azimuth = 180 },
        new RotatorPreset { Name = "W", Azimuth = 270 },
    };
}

[BsonIgnoreExtraElements]
public class RadioSettings
{
    [BsonElement("followRadio")]
    public bool FollowRadio { get; set; } = true;

    [BsonElement("activeRigType")]
    public string? ActiveRigType { get; set; }  // "tci" | "hamlib" | null

    [BsonElement("autoReconnect")]
    public bool AutoReconnect { get; set; } = false;

    [BsonElement("autoConnectRigId")]
    public string? AutoConnectRigId { get; set; }

    [BsonElement("reconnectLastOnStartup")]
    public bool ReconnectLastOnStartup { get; set; } = true;

    [BsonElement("scrollTuneStepHz")]
    public int ScrollTuneStepHz { get; set; } = 100;

    [BsonElement("tci")]
    public TciSettings Tci { get; set; } = new();

    [BsonElement("flrig")]
    public FlrigSettings Flrig { get; set; } = new();
}

[BsonIgnoreExtraElements]
public class FlrigSettings
{
    // W1HKJ flrig XML-RPC integration. The v1.x port docs the whole
    // rationale — flrig is a lightweight rig-control bridge that exposes
    // rig.get_vfoA / rig.set_vfoA / rig.get_mode / rig.set_mode over XML-RPC.
    // We poll it every 1.5 s while enabled, following the same cadence
    // v1.x used.
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    [BsonElement("host")]
    public string Host { get; set; } = "127.0.0.1";

    [BsonElement("port")]
    public int Port { get; set; } = 12345;

    // Rig-specific digital passthrough mode override. flrig calls the same
    // "digital audio in SSB passband" mode different names on different
    // rigs (USB-D on Icom, DATA-U on Kenwood/Yaesu, PKT-U on some, DIGU
    // on others). We auto-detect the right name on connect, but this
    // override lets an operator pin it explicitly when detection picks
    // the wrong one.
    [BsonElement("digitalMode")]
    public string? DigitalMode { get; set; }

    // RTTY mode override — blank = "RTTY" sent as native radio RTTY.
    // Set to "USB-D" (or similar) for AFSK RTTY via fldigi where the rig
    // needs to stay in digital-passthrough mode.
    [BsonElement("rttyMode")]
    public string? RttyMode { get; set; }
}

[BsonIgnoreExtraElements]
public class TciSettings
{
    [BsonElement("host")]
    public string? Host { get; set; }

    [BsonElement("port")]
    public int Port { get; set; } = 50001;

    [BsonElement("name")]
    public string? Name { get; set; }

    [BsonElement("autoConnect")]
    public bool AutoConnect { get; set; }
}

[BsonIgnoreExtraElements]
public class MapSettings
{
    [BsonElement("tileLayer")]
    public string? TileLayer { get; set; } = "dark";

    [BsonElement("showSatellites")]
    public bool ShowSatellites { get; set; }

    [BsonElement("selectedSatellites")]
    public List<string> SelectedSatellites { get; set; } = new() { "ISS", "AO-91", "SO-50" };

    [BsonElement("rbn")]
    public RbnSettings Rbn { get; set; } = new();

    [BsonElement("showPotaOverlay")]
    public bool ShowPotaOverlay { get; set; }

    [BsonElement("showLightning")]
    public bool ShowLightning { get; set; }

    [BsonElement("showDayNightOverlay")]
    public bool ShowDayNightOverlay { get; set; }

    [BsonElement("showGrayLine")]
    public bool ShowGrayLine { get; set; }

    [BsonElement("showSunMarker")]
    public bool ShowSunMarker { get; set; } = true;

    [BsonElement("showMoonMarker")]
    public bool ShowMoonMarker { get; set; } = true;

    [BsonElement("dayNightOpacity")]
    public double DayNightOpacity { get; set; } = 0.5;

    [BsonElement("grayLineOpacity")]
    public double GrayLineOpacity { get; set; } = 0.6;

    [BsonElement("showCallsignImages")]
    public bool ShowCallsignImages { get; set; } = true;

    [BsonElement("maxCallsignImages")]
    public int MaxCallsignImages { get; set; } = 50;

    [BsonElement("showDxNewsTicker")]
    public bool ShowDxNewsTicker { get; set; } = true;

    [BsonElement("dxPathStyle")]
    public string? DxPathStyle { get; set; } = "sine"; // sine | dash

    [BsonElement("dxPathColor")]
    public string? DxPathColor { get; set; } = "#39ff14";
}

[BsonIgnoreExtraElements]
public class RbnSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; } = false;

    [BsonElement("opacity")]
    public double Opacity { get; set; } = 0.7;

    [BsonElement("showPaths")]
    public bool ShowPaths { get; set; } = true;

    [BsonElement("timeWindowMinutes")]
    public int TimeWindowMinutes { get; set; } = 5;

    [BsonElement("minSnr")]
    public int MinSnr { get; set; } = -10;

    [BsonElement("bands")]
    public List<string> Bands { get; set; } = new() { "all" };

    [BsonElement("modes")]
    public List<string> Modes { get; set; } = new() { "CW", "RTTY" };
}

[BsonIgnoreExtraElements]
public class SpotStatusSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; } = true;

    [BsonElement("colors")]
    public SpotStatusColors Colors { get; set; } = new();

    [BsonElement("show")]
    public SpotStatusEnabled Show { get; set; } = new();

    [BsonElement("dimWorked")]
    public bool DimWorked { get; set; } = true;
}

[BsonIgnoreExtraElements]
public class SpotStatusColors
{
    [BsonElement("newDxcc")]
    public string NewDxcc { get; set; } = "#ff3a09";

    [BsonElement("newBand")]
    public string NewBand { get; set; } = "#4cc850";

    [BsonElement("worked")]
    public string Worked { get; set; } = "#6d6d6d";
}

[BsonIgnoreExtraElements]
public class SpotStatusEnabled
{
    [BsonElement("newDxcc")]
    public bool NewDxcc { get; set; } = true;

    [BsonElement("newBand")]
    public bool NewBand { get; set; } = true;

    [BsonElement("worked")]
    public bool Worked { get; set; } = true;
}

[BsonIgnoreExtraElements]
public class ClusterSettings
{
    [BsonElement("connections")]
    public List<ClusterConnection> Connections { get; set; } = new();

    [BsonElement("deduplicationWindowSeconds")]
    public int DeduplicationWindowSeconds { get; set; } = 60;  // Default 1 minute

    // Spothole.app REST aggregator source (read-only; polled, not telnet).
    // Enabled by default — the default spot source for new installs.
    [BsonElement("spotholeEnabled")]
    public bool SpotholeEnabled { get; set; } = true;

    // Only show spots whose SPOTTER resolves to this country (cty.dat lookup).
    // Applies to ALL spot sources (telnet clusters and spothole), enforced in
    // DxClusterService at broadcast time. Empty = all spotters. Default: US.
    // (Bson key kept from when this was spothole-only.)
    // Empty = worldwide (recommended so the band-activity heat map reflects
    // global activity). Set a cty.dat country name to scope spots to its spotters.
    [BsonElement("spotholeSpotterCountry")]
    public string? SpotholeSpotterCountry { get; set; } = "";

    // Which connection to POST outbound self-spots to. The DX cluster
    // network is federated (VE7CC, DXSpider, AR-Cluster, CC Cluster all
    // peer and relay upstream), so posting the same spot to every
    // connected cluster is redundant at best and gets your call flagged
    // as a repeat-source spammer at worst. Empty = "first connected
    // cluster" fallback so brand-new setups still work.
    [BsonElement("primarySpotClusterId")]
    public string? PrimarySpotClusterId { get; set; } = "";

    // Mirror received DX spots onto the connected TCI radio's panadapter
    // (Lyra / Thetis) as coloured click-to-tune markers — v1 SDRLogger+
    // behaviour. Default on; operators who don't want their SDR waterfall
    // cluttered can turn it off. Colour follows worked-before status
    // (new DXCC / new band / worked / default).
    [BsonElement("pushSpotsToTci")]
    public bool PushSpotsToTci { get; set; } = true;

    // "Follow rig" — the spot list can track the connected rig independently by
    // BAND and/or MODE. Two toggles so the operator can chase "everything on the
    // rig's band", "the rig's mode across all bands", or both. The frontend
    // always sends these; without matching properties they'd be silently dropped
    // on save and the toggles would reset on every settings load.
    [BsonElement("followRigBand")]
    public bool FollowRigBand { get; set; }

    [BsonElement("followRigMode")]
    public bool FollowRigMode { get; set; }

    // Max spots kept in memory (both the backend replay buffer handed to
    // new clients and the frontend backing store). v1 SDRLogger+ hard-
    // capped at 200; v2 lets the operator dial it up to 300 for
    // contest-day busy periods.
    [BsonElement("maxSpots")]
    public int MaxSpots { get; set; } = 200;

    // Age filter — spots older than this drop off the visible list and
    // are pruned from the backing store. v1 offered 5/10/15/30 min; v2
    // adds 60 min. The store enforces the filter so a stale spot can't
    // linger just because no new spot arrived to trigger a render.
    [BsonElement("spotAgeMinutes")]
    public int SpotAgeMinutes { get; set; } = 10;
}

[BsonIgnoreExtraElements]
public class ClusterConnection
{
    [BsonElement("id")]
    public string? Id { get; set; } = Guid.NewGuid().ToString();

    [BsonElement("name")]
    public string? Name { get; set; } = string.Empty;

    [BsonElement("host")]
    public string? Host { get; set; } = string.Empty;

    [BsonElement("port")]
    public int Port { get; set; } = 23;

    [BsonElement("callsign")]
    public string? Callsign { get; set; }  // If null, uses station callsign

    [BsonElement("password")]
    public string? Password { get; set; }  // Optional password for closed clusters

    [BsonElement("enabled")]
    public bool Enabled { get; set; } = true;

    [BsonElement("autoReconnect")]
    public bool AutoReconnect { get; set; } = false;

    [BsonElement("filterSkimmer")]
    public bool FilterSkimmer { get; set; } = true;  // Drop RBN/skimmer (-#) spots — they're mostly FT8 noise

    [BsonElement("filterFt8")]
    public bool FilterFt8 { get; set; } = false;  // Drop FT8 spots entirely (skimmer filter usually enough)
}

[BsonIgnoreExtraElements]
public class HeaderSettings
{
    [BsonElement("timeFormat")]
    public string? TimeFormat { get; set; } = "24h";  // "12h" | "24h"

    [BsonElement("showWeather")]
    public bool ShowWeather { get; set; } = true;

    [BsonElement("weatherLocation")]
    public string? WeatherLocation { get; set; } = string.Empty;
}

[BsonIgnoreExtraElements]
public class AiSettings
{
    [BsonElement("provider")]
    public string? Provider { get; set; } = "anthropic"; // "anthropic" | "openai"

    [BsonElement("apiKey")]
    public string? ApiKey { get; set; } = string.Empty; // Stored obfuscated

    [BsonElement("model")]
    public string? Model { get; set; } = "claude-sonnet-4-5-20250929"; // Provider-specific model name

    [BsonElement("autoGenerateTalkPoints")]
    public bool AutoGenerateTalkPoints { get; set; } = true;

    [BsonElement("includeQrzProfile")]
    public bool IncludeQrzProfile { get; set; } = true;

    [BsonElement("includeQsoHistory")]
    public bool IncludeQsoHistory { get; set; } = true;

    [BsonElement("includeSpotComments")]
    public bool IncludeSpotComments { get; set; } = false;
}

public class BackupSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    /// <summary>"daily" | "weekly" | "on_exit"</summary>
    [BsonElement("interval")]
    public string Interval { get; set; } = "daily";

    [BsonElement("retention")]
    public int Retention { get; set; } = 10;

    /// <summary>Empty/null → default: &lt;config dir&gt;/backups</summary>
    [BsonElement("destinationPath")]
    public string? DestinationPath { get; set; }
}

public class SatSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    [BsonElement("controllerIp")]
    public string? ControllerIp { get; set; }

    [BsonElement("udpPort")]
    public int UdpPort { get; set; } = 9932;

    [BsonElement("adifPort")]
    public int AdifPort { get; set; } = 1100;
}

public class WeatherSettings
{
    [BsonElement("lightning")]
    public LightningSettings Lightning { get; set; } = new();

    [BsonElement("wind")]
    public WindSettings Wind { get; set; } = new();

    [BsonElement("credentials")]
    public WeatherCredentials Credentials { get; set; } = new();
}

public class LightningSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    [BsonElement("useBlitzortung")]
    public bool UseBlitzortung { get; set; } = true;

    [BsonElement("useNws")]
    public bool UseNws { get; set; } = true;

    [BsonElement("useAmbient")]
    public bool UseAmbient { get; set; }

    [BsonElement("useEcowitt")]
    public bool UseEcowitt { get; set; }

    [BsonElement("range")]
    public double Range { get; set; } = 50;

    /// <summary>"mi" | "km"</summary>
    [BsonElement("rangeUnit")]
    public string RangeUnit { get; set; } = "mi";
}

public class WindSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    [BsonElement("useNwsAlerts")]
    public bool UseNwsAlerts { get; set; } = true;

    [BsonElement("useNwsMetar")]
    public bool UseNwsMetar { get; set; }

    [BsonElement("metarStation")]
    public string? MetarStation { get; set; }

    [BsonElement("useAmbient")]
    public bool UseAmbient { get; set; }

    [BsonElement("useEcowitt")]
    public bool UseEcowitt { get; set; }

    /// <summary>Stored in mph regardless of display unit (ported decision)</summary>
    [BsonElement("threshSustainedMph")]
    public double ThreshSustainedMph { get; set; } = 30;

    [BsonElement("threshGustMph")]
    public double ThreshGustMph { get; set; } = 45;

    /// <summary>"mph" | "kph"</summary>
    [BsonElement("displayUnit")]
    public string DisplayUnit { get; set; } = "mph";

    [BsonElement("cooldownMinutes")]
    public int CooldownMinutes { get; set; } = 20;
}

public class WeatherCredentials
{
    [BsonElement("ambientApiKey")]
    public string? AmbientApiKey { get; set; }

    [BsonElement("ambientAppKey")]
    public string? AmbientAppKey { get; set; }

    [BsonElement("ecowittAppKey")]
    public string? EcowittAppKey { get; set; }

    [BsonElement("ecowittApiKey")]
    public string? EcowittApiKey { get; set; }

    [BsonElement("ecowittMac")]
    public string? EcowittMac { get; set; }
}

public class WsjtxSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    [BsonElement("port")]
    public int Port { get; set; } = 2237;

    /// <summary>Empty = unicast; set to e.g. 224.0.0.1 to join a multicast group</summary>
    [BsonElement("multicastAddress")]
    public string? MulticastAddress { get; set; }
}

public class HotListSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    [BsonElement("ttsEnabled")]
    public bool TtsEnabled { get; set; }

    /// <summary>Watched callsigns, stored uppercase</summary>
    [BsonElement("callsigns")]
    public List<string> Callsigns { get; set; } = new();

    /// <summary>Per-callsign TTS announcement cooldown</summary>
    [BsonElement("ttsCooldownMinutes")]
    public int TtsCooldownMinutes { get; set; } = 15;
}

public class PluginSettings
{
    [BsonId]
    public string Id { get; set; } = null!;  // Plugin ID

    [BsonElement("enabled")]
    public bool Enabled { get; set; } = true;

    [BsonExtraElements]
    public BsonDocument? Settings { get; set; }
}

public class Layout
{
    [BsonId]
    public string Id { get; set; } = null!;

    [BsonElement("name")]
    public string Name { get; set; } = null!;

    [BsonElement("isDefault")]
    public bool IsDefault { get; set; }

    [BsonElement("layout")]
    public BsonDocument LayoutJson { get; set; } = null!;

    [BsonElement("createdAt")]
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    [BsonElement("updatedAt")]
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// A single named layout preset the operator has saved (e.g. "POTA",
/// "Contest", "DXpedition"). Persisted inside UserSettings.SavedLayouts.
/// </summary>
[BsonIgnoreExtraElements]
public class SavedLayoutSlot
{
    [BsonElement("name")]
    public string Name { get; set; } = "";

    // FlexLayout IJsonModel serialized as a JSON string — same format the
    // live LayoutJson field uses. Kept as a string so the settings blob
    // stays small and BSON doesn't try to deserialize the layout tree.
    [BsonElement("layoutJson")]
    public string LayoutJson { get; set; } = "";

    [BsonElement("savedAt")]
    public DateTime SavedAt { get; set; } = DateTime.UtcNow;
}

import { create } from 'zustand';
import type { ThemeId, CustomColors } from '../theme/themes';
import { getSeedColors } from '../theme/themes';

// Settings types
export interface StationSettings {
  callsign: string;
  operatorName: string;
  gridSquare: string;
  latitude: number | null;
  longitude: number | null;
  city: string;
  country: string;
  /** ITU Region (1=EU/Africa/Russia, 2=Americas, 3=Asia-Pacific) — band plan & OOB VFO alerts. */
  ituRegion: number;
}

export interface QrzSettings {
  username: string;
  password: string;
  apiKey: string; // For QRZ logbook uploads
  enabled: boolean;
}

// HamQTH.com free callbook. Used as a fallback lookup source when QRZ has
// nothing (no subscription, unknown call, or QRZ down). Session is managed
// server-side in HamQthService; the client only owns the credentials.
export interface HamQthSettings {
  username: string;
  password: string;
  enabled: boolean;
}

export interface LotwSettings {
  enabled: boolean;
  // Absolute path to the local TQSL binary (e.g. C:\Program Files\TrustedQSL\tqsl.exe).
  // Entered manually; TQSL must be installed separately.
  tqslPath: string;
  // Optional TQSL station location name (passed as `-l <name>`); empty = TQSL default.
  stationCallsign: string;
  // LoTW website login (separate from the TQSL cert) — used to download the
  // confirmation report from lotw.arrl.org.
  username: string;
  password: string;
}

export interface RbnAlertSettings {
  enabled: boolean;
  server: string;
  port: number;
  band10m: boolean;
  band6m: boolean;
  band2m: boolean;
  band70cm: boolean;
  distance: number;
  distanceUnit: 'mi' | 'km';
  cooldownMinutes: number;
  voice: boolean;
}

export interface AdifUdpSettings {
  enabled: boolean;
  port: number; // default 52001, v1 SDRLogger+ convention
}

export interface AdifMonitorSettings {
  enabled: boolean;
  file1: string; // watched external ADIF files (VarAC, MSHV, ...)
  file2: string;
}

export interface ClubLogSettings {
  enabled: boolean;
  email: string;
  password: string;
  callsign: string; // defaults to station callsign when empty
  apiKey: string;   // Club Log application key (issued to app authors on request)
}

export interface HrdLogSettings {
  enabled: boolean;
  callsign: string;   // defaults to station callsign when empty
  uploadCode: string; // per-account upload code from hrdlog.net
}

export interface EqslSettings {
  enabled: boolean;
  username: string;    // typically your callsign
  password: string;
  qthNickname: string; // optional: pick a QTH when your eQSL account has multiple
}

/** Hands-off background confirmation sync (LoTW / eQSL). */
export interface ConfirmationSyncSettings {
  autoSync: boolean;
  intervalHours: number;
  syncOnStartup: boolean;
  lotw: boolean;
  eqsl: boolean;
  qrz: boolean;
}

export interface PotaSettings {
  username: string;  // POTA.app account username
  password: string;  // POTA.app account password (basic-auth on /spot)
  /** POTA Activators panel: follow the rig's band / mode (persisted, like the cluster). */
  followRigBand: boolean;
  followRigMode: boolean;
}

/** Shared voice used for every spoken announcement (band-opening, Hot List, RBN…). */
export interface VoiceSettings {
  /** SpeechSynthesisVoice.voiceURI to speak with. '' = the browser default voice. */
  voiceUri: string;
  /** Speaking rate, 0.5 (slow) – 1.5 (fast). */
  rate: number;
  /** Announcement volume, 0–1. Shared by every spoken alert. */
  volume: number;
}

export interface DxCoachSettings {
  /**
   * Minimum predicted path reliability (0–99%) an opportunity must clear to
   * appear in the DX Coach. Spots with no propagation data (no QTH set, or no
   * DX location resolved) are always shown — they can't be fairly judged.
   * 0 = show every opportunity.
   */
  minReliability: number;
  /** Speak newly-arriving high-value opportunities (new DXCC / new zone) aloud. */
  voice: boolean;
  /** Coach DXCC opportunities (new entity / new band-slot). */
  showDxcc: boolean;
  /** Coach WAZ opportunities (new CQ zone / new zone-band). */
  showWaz: boolean;
  /** Show MF/LF low-band opportunities (2200m, 630m). */
  showLowBand: boolean;
  /** Show HF opportunities (160m–10m). */
  showHf: boolean;
  /** Show 6m opportunities (its own toggle — HF+6m rigs are common). */
  show6m: boolean;
  /** Show VHF opportunities (2m, 1.25m). */
  showVhf: boolean;
  /** Show UHF opportunities (70cm, 33cm, 23cm and up). */
  showUhf: boolean;
}

export interface AppearanceSettings {
  theme: ThemeId;
  /** Used when theme === 'custom'; seeded from the previously active theme. */
  customColors: CustomColors;
  /**
   * Master unit system. Drives every physical readout app-wide (distance,
   * satellite range/altitude, lightning proximity, wind, temperature) unless a
   * feature explicitly overrides it.
   */
  unitSystem: 'imperial' | 'metric';
  /**
   * Effective distance unit, kept in sync with unitSystem (imperial → mi,
   * metric → km). Retained as the value distance consumers read directly.
   */
  distanceUnit: 'km' | 'mi';
}

export interface RotatorPreset {
  name: string;
  azimuth: number;
}

export type RotatorConnectionType = 'network' | 'serial';
export type RotatorProtocol = 'rotctld' | 'arco_tcp';

export interface RotatorSettings {
  enabled: boolean;
  // Connection type
  connectionType: RotatorConnectionType;
  // Wire protocol: hamlib rotctld, or microHAM ARCO (GS-232A over TCP)
  protocol: RotatorProtocol;
  // Network settings (for connecting to existing rotctld)
  ipAddress: string;
  port: number;
  // Serial settings (for direct serial connection)
  serialPort: string; // e.g., 'COM3' or '/dev/ttyUSB0'
  baudRate: number;
  // Hamlib configuration
  hamlibModelId: number | null; // Hamlib rotator model ID (e.g., 603 for Yaesu GS-232B)
  hamlibModelName: string; // Human-readable model name
  // Polling and identification
  pollingIntervalMs: number;
  rotatorId: string;
  presets: RotatorPreset[];
}

export interface TciSettings {
  host: string;
  port: number;
  name: string;
  autoConnect: boolean;
}

// W1HKJ flrig XML-RPC integration (ported from v1.x SDRLogger+). flrig
// runs as a separate desktop bridge to the physical rig; SDRLoggerPlus
// polls its XML-RPC endpoint (default port 12345) for freq/mode and
// pushes commands the same way.
export interface FlrigSettings {
  enabled: boolean;
  host: string;
  port: number;
  // Rig-specific digital passthrough mode override — auto-detected on
  // connect, this override pins it explicitly when detection guesses wrong.
  // Common values: USB-D (Icom), DATA-U (Kenwood/Yaesu), PKT-U, DIGU.
  digitalMode: string;
  // RTTY mode override — blank = "RTTY" (native), set to USB-D/DATA-U for
  // AFSK RTTY via fldigi where the rig should stay in digital passthrough.
  rttyMode: string;
}

export type RigType = 'tci' | 'hamlib' | 'flrig' | null;

export interface RadioSettings {
  followRadio: boolean;
  activeRigType: RigType;
  autoReconnect: boolean;
  autoConnectRigId: string | null;
  reconnectLastOnStartup: boolean;
  scrollTuneStepHz: number;
  tci: TciSettings;
  flrig: FlrigSettings;
}

export interface RbnSettings {
  enabled: boolean;
  opacity: number;
  showPaths: boolean;
  timeWindowMinutes: number;
  minSnr: number;
  bands: string[];
  modes: string[];
}

export interface MapSettings {
  tileLayer: 'osm' | 'dark' | 'satellite' | 'terrain';
  showSatellites: boolean;
  selectedSatellites: string[];
  rbn: RbnSettings;
  showPotaOverlay: boolean;
  showLightning: boolean;
  /** Slow auto-spin of the 2D Map's embedded globe circle. */
  rotateGlobe: boolean;
  showDayNightOverlay: boolean;
  showGrayLine: boolean;
  showSunMarker: boolean;
  showMoonMarker: boolean;
  dayNightOpacity: number;
  grayLineOpacity: number;
  showCallsignImages: boolean;
  maxCallsignImages: number;
  showDxNewsTicker: boolean; // scrolling DX-World news bar at the bottom of the 2D Map
  // 2D-map DX signal path: animated sine wave or a plain dashed line, in a
  // user-picked colour (hex).
  dxPathStyle: 'sine' | 'dash';
  dxPathColor: string;
  showPskOverlay: boolean;
  pskCallsign: string; // callsign to look up on PSK Reporter; empty = use station callsign
  showAuroraOverlay: boolean;
  // Long-path great-circle overlay on the 3D globe. Short path (red-orange)
  // is always drawn when a callsign is focused; the cyan long-path arc is
  // opt-in so operators who only care about SP get a cleaner view.
  showLongPath: boolean;
  // Draw the short path as an ionospheric-skip zigzag (bouncing between the
  // ground and the ionosphere) on the 3D Globe, and tilt the view to an
  // oblique angle so the hops are visible. Off = a single smooth arc.
  showIonosphereHops: boolean;
  // "Heard Me" globe layers — arcs from your station to stations that heard you.
  showGlobeHeardMePsk: boolean;   // PSK Reporter (digital)
  showGlobeHeardMeRbn: boolean;   // RBN (CW/RTTY skimmers)
  heardMeBand: string;            // manual band fallback when no rig connected
  heardMePskWindowMinutes: number; // PSK look-back, clamped [5,60]
  heardMeRbnWindowMinutes: number; // RBN look-back, clamped [5,15] (RbnService buffer retains ~15 min)
  show2dHeardMeRbn: boolean; // "Heard Me — RBN" overlay on the 2D map (distinct from the RBN cluster layer)
}

export interface HeaderSettings {
  timeFormat: '12h' | '24h';
  showWeather: boolean;
  weatherLocation: string;  // City name or coordinates for weather lookup
}

export interface ClusterConnection {
  id: string;
  name: string;
  host: string;
  port: number;
  callsign: string | null;  // If null, uses station callsign
  password: string | null;  // Optional password for closed clusters
  enabled: boolean;
  autoReconnect: boolean;
}

export interface ClusterSettings {
  connections: ClusterConnection[];
  /** Spothole.app REST aggregator — the default spot source (read-only, polled). */
  spotholeEnabled: boolean;
  /** Only show spots whose spotter resolves to this country; empty = all. */
  spotholeSpotterCountry: string;
  /** "Follow rig" band tracking — restrict spots to the connected rig's live band. */
  followRigBand: boolean;
  /** "Follow rig" mode tracking — restrict spots to the connected rig's live mode.
   *  Independent of followRigBand: enable either or both. */
  followRigMode: boolean;
  /** Which of the configured telnet clusters receives outbound spots. Empty = auto (only when a single cluster is connected). */
  primarySpotClusterId: string;
  /** Mirror received DX spots onto the connected TCI radio's panadapter (Lyra / Thetis). */
  pushSpotsToTci: boolean;
  /** Max spots kept in memory (backing store + backend replay buffer). 50–300. */
  maxSpots: number;
  /** Age filter — spots older than this minute count drop off the visible list. */
  spotAgeMinutes: number;
}

export interface SpotStatusColors {
  newDxcc: string;
  newBand: string;
  worked: string;
}

export interface SpotStatusEnabled {
  newDxcc: boolean;
  newBand: boolean;
  worked: boolean;
}

export interface SpotStatusSettings {
  enabled: boolean;
  colors: SpotStatusColors;
  show: SpotStatusEnabled;
  dimWorked: boolean;
}

export interface LightningAlertSettings {
  enabled: boolean;
  useBlitzortung: boolean;
  useNws: boolean;
  useAmbient: boolean;
  useEcowitt: boolean;
  range: number;
  rangeUnit: 'mi' | 'km';
}

export interface WindAlertSettings {
  enabled: boolean;
  useNwsAlerts: boolean;
  useNwsMetar: boolean;
  metarStation?: string | null;
  useAmbient: boolean;
  useEcowitt: boolean;
  threshSustainedMph: number;
  threshGustMph: number;
  /** 'auto' follows the master unit system; 'mph'/'kph' force a display unit. */
  displayUnit: 'auto' | 'mph' | 'kph';
  cooldownMinutes: number;
}

export interface WeatherCredentials {
  ambientApiKey?: string | null;
  ambientAppKey?: string | null;
  ecowittAppKey?: string | null;
  ecowittApiKey?: string | null;
  ecowittMac?: string | null;
}

export interface WeatherSettings {
  lightning: LightningAlertSettings;
  wind: WindAlertSettings;
  credentials: WeatherCredentials;
}

export interface SatControllerSettings {
  enabled: boolean;
  controllerIp?: string | null;
  udpPort: number;
  adifPort: number;
  /** Activate automatically when the controller starts (or is about to start) a pass. */
  autoActivate: boolean;
  /** How far ahead of AOS to activate, in seconds (mirrors the controller's AOS alarm). */
  autoActivateLeadSeconds: number;
}

/** One WSJT-X/JTDX UDP listener (a decoder app reporting to a host:port). */
export interface WsjtxSource {
  enabled: boolean;
  port: number;
  multicastAddress?: string | null;
}

export interface WsjtxSettings {
  // Source 1 (primary) — flat fields, unchanged for backward compatibility.
  enabled: boolean;
  port: number;
  multicastAddress?: string | null;
  // Source 2 (secondary) — a second decoder on its own port (e.g. JTDX while
  // WSJT-X runs on the primary). Disabled by default.
  source2: WsjtxSource;
}

/** One geo-scoped needed-status alert rule over the decode stream. */
export interface DecodeAlertRule {
  id: string;
  enabled: boolean;
  name: string;
  // Award needs — alert when the decode is any enabled kind.
  newDxcc: boolean;
  newBand: boolean;
  newZone: boolean;
  newGrid: boolean;
  // Scope filters — each, if non-empty, must match.
  continents: string[];
  dxccEntities: string[];
  callAreas: number[];   // US call districts 0-9
  prefixes: string[];    // e.g. W, K, VE3
  gridFields: string[];  // 2-char grid fields, e.g. EM
  bands: string[];
  modes: string[];
  // Actions.
  sound: boolean;
  voice: boolean;
  popup: boolean;
  cooldownMinutes: number;
}

export interface DecodeAlertsSettings {
  enabled: boolean;
  rules: DecodeAlertRule[];
}

export interface HotListSettings {
  enabled: boolean;
  ttsEnabled: boolean;
  callsigns: string[];
  ttsCooldownMinutes: number;
}

export interface BackupSettings {
  enabled: boolean;
  interval: 'daily' | 'weekly' | 'on_exit';
  retention: number;
  destinationPath?: string | null;
}

export type AiProvider = 'anthropic' | 'openai' | 'groq' | 'openrouter' | 'ollama' | 'custom';

export interface AiSettings {
  provider: AiProvider;
  apiKey: string;
  model: string;
  /** OpenAI-compatible API base URL; blank = the provider preset's default. */
  baseUrl: string;
  autoGenerateTalkPoints: boolean;
  includeQrzProfile: boolean;
  includeQsoHistory: boolean;
  includeSpotComments: boolean;
}

export interface ContestSettings {
  n1mmUdpEnabled: boolean;
  n1mmUdpHost: string;
  n1mmUdpPort: number;
  onlineScoreEnabled: boolean;
  onlineScoreUrl: string;
  // Definition ids hidden from the contest picker (reversible "remove").
  hiddenContestIds: string[];
}

export interface Settings {
  station: StationSettings;
  qrz: QrzSettings;
  hamQth: HamQthSettings;
  lotw: LotwSettings;
  clubLog: ClubLogSettings;
  hrdLog: HrdLogSettings;
  eqsl: EqslSettings;
  confirmationSync: ConfirmationSyncSettings;
  pota: PotaSettings;
  dxCoach: DxCoachSettings;
  voice: VoiceSettings;
  adifMonitor: AdifMonitorSettings;
  adifUdp: AdifUdpSettings;
  rbnAlerts: RbnAlertSettings;
  appearance: AppearanceSettings;
  rotator: RotatorSettings;
  radio: RadioSettings;
  map: MapSettings;
  cluster: ClusterSettings;
  spotStatus: SpotStatusSettings;
  header: HeaderSettings;
  ai: AiSettings;
  backup: BackupSettings;
  hotList: HotListSettings;
  wsjtx: WsjtxSettings;
  decodeAlerts: DecodeAlertsSettings;
  weather: WeatherSettings;
  sat: SatControllerSettings;
  contest: ContestSettings;
  gridStates: Record<string, string>;
}

export type SettingsSection = 'station' | 'weblogbooks' | 'wsjtx' | 'decodealerts' | 'alerts' | 'adifmonitor' | 'rbnalerts' | 'rotator' | 'appearance' | 'map' | 'header' | 'ai' | 'backup' | 'sat' | 'dxcoach' | 'voice' | 'about';

interface SettingsState {
  // Settings data
  settings: Settings;

  // UI state
  isOpen: boolean;
  activeSection: SettingsSection;
  isDirty: boolean;
  isSaving: boolean;
  isLoaded: boolean;
  error: string | null;

  // Actions
  openSettings: () => void;
  closeSettings: () => void;
  setActiveSection: (section: SettingsSection) => void;
  clearError: () => void;

  // Settings updates
  updateStationSettings: (station: Partial<StationSettings>) => void;
  updateQrzSettings: (qrz: Partial<QrzSettings>) => void;
  updateHamQthSettings: (hamQth: Partial<HamQthSettings>) => void;
  updateLotwSettings: (lotw: Partial<LotwSettings>) => void;
  updateClubLogSettings: (clubLog: Partial<ClubLogSettings>) => void;
  updateHrdLogSettings: (hrdLog: Partial<HrdLogSettings>) => void;
  updateEqslSettings: (eqsl: Partial<EqslSettings>) => void;
  updateConfirmationSyncSettings: (confirmationSync: Partial<ConfirmationSyncSettings>) => void;
  updatePotaSettings: (pota: Partial<PotaSettings>) => void;
  updateDxCoachSettings: (dxCoach: Partial<DxCoachSettings>) => void;
  updateVoiceSettings: (voice: Partial<VoiceSettings>) => void;
  updateAdifMonitorSettings: (adifMonitor: Partial<AdifMonitorSettings>) => void;
  updateAdifUdpSettings: (adifUdp: Partial<AdifUdpSettings>) => void;
  updateRbnAlertSettings: (rbnAlerts: Partial<RbnAlertSettings>) => void;
  updateAppearanceSettings: (appearance: Partial<AppearanceSettings>) => void;
  updateRotatorSettings: (rotator: Partial<RotatorSettings>) => void;
  updateRadioSettings: (radio: Partial<RadioSettings>) => void;
  updateTciSettings: (tci: Partial<TciSettings>) => void;
  updateFlrigSettings: (flrig: Partial<FlrigSettings>) => void;
  updateMapSettings: (map: Partial<MapSettings>) => void;
  updateClusterSettings: (cluster: Partial<ClusterSettings>) => void;
  updateClusterConnection: (connectionId: string, connection: Partial<ClusterConnection>) => void;
  updateHeaderSettings: (header: Partial<HeaderSettings>) => void;
  updateAiSettings: (ai: Partial<AiSettings>) => void;
  updateBackupSettings: (backup: Partial<BackupSettings>) => void;
  updateHotListSettings: (hotList: Partial<HotListSettings>) => void;
  updateWsjtxSettings: (wsjtx: Partial<WsjtxSettings>) => void;
  updateDecodeAlertsSettings: (decodeAlerts: Partial<DecodeAlertsSettings>) => void;
  updateWeatherSettings: (weather: Partial<WeatherSettings>) => void;
  updateSatSettings: (sat: Partial<SatControllerSettings>) => void;
  updateContestSettings: (contest: Partial<ContestSettings>) => void;
  updateSpotStatusSettings: (spotStatus: Partial<SpotStatusSettings>) => void;
  addClusterConnection: () => void;
  removeClusterConnection: (connectionId: string) => void;

  // Persistence (backend via API, no localStorage)
  saveSettings: () => Promise<void>;
  loadSettings: () => Promise<void>;
  resetSettings: () => void;
  // Reset loaded state (for reconnection scenarios)
  setNotLoaded: () => void;
}

const defaultSettings: Settings = {
  station: {
    callsign: '',
    operatorName: '',
    gridSquare: '',
    latitude: null,
    longitude: null,
    city: '',
    country: '',
    ituRegion: 2,
  },
  qrz: {
    username: '',
    password: '',
    apiKey: '',
    enabled: false,
  },
  hamQth: {
    username: '',
    password: '',
    enabled: false,
  },
  lotw: {
    enabled: false,
    tqslPath: '',
    stationCallsign: '',
    username: '',
    password: '',
  },
  clubLog: {
    enabled: false,
    email: '',
    password: '',
    callsign: '',
    apiKey: '',
  },
  hrdLog: {
    enabled: false,
    callsign: '',
    uploadCode: '',
  },
  eqsl: {
    enabled: false,
    username: '',
    password: '',
    qthNickname: '',
  },
  confirmationSync: {
    autoSync: false,
    intervalHours: 6,
    syncOnStartup: true,
    lotw: true,
    eqsl: true,
    qrz: true,
  },
  pota: {
    username: '',
    password: '',
    followRigBand: false,
    followRigMode: false,
  },
  dxCoach: {
    minReliability: 30,
    voice: false,
    showDxcc: true,
    showWaz: true,
    showLowBand: true,
    showHf: true,
    show6m: true,
    showVhf: true,
    showUhf: true,
  },
  voice: {
    voiceUri: '',
    rate: 0.95,
    volume: 0.8,
  },
  adifMonitor: {
    enabled: false,
    file1: '',
    file2: '',
  },
  adifUdp: {
    enabled: false,
    port: 52001,
  },
  rbnAlerts: {
    enabled: false,
    server: 'telnet.reversebeacon.net',
    port: 7000,
    band10m: false,
    band6m: true,
    band2m: true,
    band70cm: true,
    distance: 500,
    distanceUnit: 'mi',
    cooldownMinutes: 15,
    voice: true,
  },
  appearance: {
    theme: 'dark',
    customColors: getSeedColors('dark'),
    unitSystem: 'metric',
    distanceUnit: 'km',
  },
  rotator: {
    enabled: false,
    connectionType: 'network',
    protocol: 'rotctld',
    ipAddress: '127.0.0.1',
    port: 4533,
    serialPort: '',
    baudRate: 9600,
    hamlibModelId: null,
    hamlibModelName: '',
    pollingIntervalMs: 500,
    rotatorId: 'default',
    presets: [
      { name: 'N', azimuth: 0 },
      { name: 'E', azimuth: 90 },
      { name: 'S', azimuth: 180 },
      { name: 'W', azimuth: 270 },
    ],
  },
  radio: {
    followRadio: true,
    activeRigType: null,
    autoReconnect: false,
    autoConnectRigId: null,
    reconnectLastOnStartup: true,
    scrollTuneStepHz: 100,
    tci: {
      host: '',
      port: 50001,
      name: '',
      autoConnect: false,
    },
    flrig: {
      enabled: false,
      host: '127.0.0.1',
      port: 12345,
      digitalMode: '',
      rttyMode: '',
    },
  },
  map: {
    tileLayer: 'dark',
    showSatellites: false,
    selectedSatellites: ['ISS', 'AO-91', 'SO-50'],
    rbn: {
      enabled: false,
      opacity: 0.7,
      showPaths: true,
      timeWindowMinutes: 5,
      minSnr: -10,
      bands: ['all'],
      modes: ['CW', 'RTTY'],
    },
    showPotaOverlay: false,
    showLightning: false,
    rotateGlobe: false,
    showDayNightOverlay: false,
    showGrayLine: false,
    showSunMarker: true,
    showMoonMarker: true,
    dayNightOpacity: 0.5,
    grayLineOpacity: 0.6,
    showCallsignImages: true,
    maxCallsignImages: 50,
    showDxNewsTicker: true,
    dxPathStyle: 'sine', // 'sine' | 'dash'
    dxPathColor: '#39ff14', // matches accent-secondary green in the default theme
    showPskOverlay: false,
    pskCallsign: '',
    showAuroraOverlay: false,
    showLongPath: false, // default OFF (user call) — opt-in via Settings > Map
    showIonosphereHops: false, // opt-in — tilts the globe when on
    showGlobeHeardMePsk: false,
    showGlobeHeardMeRbn: false,
    heardMeBand: '20m',
    heardMePskWindowMinutes: 60,
    heardMeRbnWindowMinutes: 15,
    show2dHeardMeRbn: false,
  },
  cluster: {
    connections: [],
    spotholeEnabled: true,
    spotholeSpotterCountry: '', // empty = worldwide (fuller band-activity heat map)
    followRigBand: false,
    followRigMode: false,
    primarySpotClusterId: '', // empty = auto-pick when a single cluster is connected
    pushSpotsToTci: true,
    maxSpots: 200,
    spotAgeMinutes: 10,
  },
  spotStatus: {
    enabled: true,
    colors: {
      newDxcc: '#ff3a09',
      newBand: '#4cc850',
      worked: '#6d6d6d',
    },
    show: {
      newDxcc: true,
      newBand: true,
      worked: true,
    },
    dimWorked: true,
  },
  header: {
    timeFormat: '24h',
    showWeather: true,
    weatherLocation: '',
  },
  ai: {
    provider: 'anthropic',
    apiKey: '',
    model: 'claude-sonnet-4-5-20250929',
    baseUrl: '',
    autoGenerateTalkPoints: true,
    includeQrzProfile: true,
    includeQsoHistory: true,
    includeSpotComments: false,
  },
  backup: {
    enabled: false,
    interval: 'daily',
    retention: 10,
    destinationPath: '',
  },
  hotList: {
    enabled: false,
    ttsEnabled: false,
    callsigns: [],
    ttsCooldownMinutes: 15,
  },
  wsjtx: {
    enabled: false,
    port: 2237,
    multicastAddress: '',
    source2: { enabled: false, port: 2333, multicastAddress: '' },
  },
  decodeAlerts: {
    enabled: false,
    rules: [],
  },
  weather: {
    lightning: {
      enabled: false,
      useBlitzortung: true,
      useNws: true,
      useAmbient: false,
      useEcowitt: false,
      range: 50,
      rangeUnit: 'mi',
    },
    wind: {
      enabled: false,
      useNwsAlerts: true,
      useNwsMetar: false,
      metarStation: '',
      useAmbient: false,
      useEcowitt: false,
      threshSustainedMph: 30,
      threshGustMph: 45,
      displayUnit: 'auto',
      cooldownMinutes: 20,
    },
    credentials: {
      ambientApiKey: '',
      ambientAppKey: '',
      ecowittAppKey: '',
      ecowittApiKey: '',
      ecowittMac: '',
    },
  },
  sat: {
    enabled: false,
    controllerIp: '',
    udpPort: 9932,
    adifPort: 1100,
    autoActivate: false,
    autoActivateLeadSeconds: 90,
  },
  contest: {
    n1mmUdpEnabled: false,
    n1mmUdpHost: '127.0.0.1',
    n1mmUdpPort: 12060,
    onlineScoreEnabled: false,
    onlineScoreUrl: 'https://contestonlinescore.com/post/',
    hiddenContestIds: [],
  },
  gridStates: {},
};

// Settings are persisted on the backend (LiteDB) via the API, not localStorage
// This ensures sensitive data (QRZ credentials) are never stored client-side
export const useSettingsStore = create<SettingsState>()((set, get) => ({
  // Initial state
  settings: defaultSettings,
  isOpen: false,
  activeSection: 'station',
  isDirty: false,
  isSaving: false,
  isLoaded: false,
  error: null,

  // UI actions
  openSettings: () => set({ isOpen: true, error: null }),
  closeSettings: () => set({ isOpen: false, isDirty: false, error: null }),
  setActiveSection: (section) => set({ activeSection: section }),
  clearError: () => set({ error: null }),

  // Station settings
  updateStationSettings: (station) =>
    set((state) => ({
      settings: {
        ...state.settings,
        station: { ...state.settings.station, ...station },
      },
      isDirty: true,
    })),

  // QRZ settings
  updateQrzSettings: (qrz) =>
    set((state) => ({
      settings: {
        ...state.settings,
        qrz: { ...state.settings.qrz, ...qrz },
      },
      isDirty: true,
    })),

  // HamQTH settings (v2 — secondary lookup source, sits between QRZ and cty.dat)
  updateHamQthSettings: (hamQth) =>
    set((state) => ({
      settings: {
        ...state.settings,
        hamQth: { ...state.settings.hamQth, ...hamQth },
      },
      isDirty: true,
    })),

  // LOTW settings
  updateLotwSettings: (lotw) =>
    set((state) => ({
      settings: {
        ...state.settings,
        lotw: { ...state.settings.lotw, ...lotw },
      },
      isDirty: true,
    })),

  updateHrdLogSettings: (hrdLog) =>
    set((state) => ({
      settings: {
        ...state.settings,
        hrdLog: { ...state.settings.hrdLog, ...hrdLog },
      },
      isDirty: true,
    })),

  updateEqslSettings: (eqsl) =>
    set((state) => ({
      settings: {
        ...state.settings,
        eqsl: { ...state.settings.eqsl, ...eqsl },
      },
      isDirty: true,
    })),

  updateConfirmationSyncSettings: (confirmationSync) =>
    set((state) => ({
      settings: {
        ...state.settings,
        confirmationSync: { ...state.settings.confirmationSync, ...confirmationSync },
      },
      isDirty: true,
    })),

  updatePotaSettings: (pota) =>
    set((state) => ({
      settings: {
        ...state.settings,
        pota: { ...state.settings.pota, ...pota },
      },
      isDirty: true,
    })),

  updateDxCoachSettings: (dxCoach) =>
    set((state) => ({
      settings: {
        ...state.settings,
        dxCoach: { ...state.settings.dxCoach, ...dxCoach },
      },
      isDirty: true,
    })),

  updateVoiceSettings: (voice) =>
    set((state) => ({
      settings: {
        ...state.settings,
        voice: { ...state.settings.voice, ...voice },
      },
      isDirty: true,
    })),

  updateClubLogSettings: (clubLog) =>
    set((state) => ({
      settings: {
        ...state.settings,
        clubLog: { ...state.settings.clubLog, ...clubLog },
      },
      isDirty: true,
    })),

  updateAdifMonitorSettings: (adifMonitor) =>
    set((state) => ({
      settings: {
        ...state.settings,
        adifMonitor: { ...state.settings.adifMonitor, ...adifMonitor },
      },
      isDirty: true,
    })),

  updateAdifUdpSettings: (adifUdp) =>
    set((state) => ({
      settings: {
        ...state.settings,
        adifUdp: { ...state.settings.adifUdp, ...adifUdp },
      },
      isDirty: true,
    })),

  updateRbnAlertSettings: (rbnAlerts) =>
    set((state) => ({
      settings: {
        ...state.settings,
        rbnAlerts: { ...state.settings.rbnAlerts, ...rbnAlerts },
      },
      isDirty: true,
    })),

  // Appearance settings
  updateAppearanceSettings: (appearance) =>
    set((state) => ({
      settings: {
        ...state.settings,
        appearance: { ...state.settings.appearance, ...appearance },
      },
      isDirty: true,
    })),

  // Rotator settings
  updateRotatorSettings: (rotator) =>
    set((state) => ({
      settings: {
        ...state.settings,
        rotator: { ...state.settings.rotator, ...rotator },
      },
      isDirty: true,
    })),

  // Radio settings
  updateRadioSettings: (radio) =>
    set((state) => ({
      settings: {
        ...state.settings,
        radio: { ...state.settings.radio, ...radio },
      },
      isDirty: true,
    })),

  // TCI settings (nested under radio)
  updateTciSettings: (tci) =>
    set((state) => ({
      settings: {
        ...state.settings,
        radio: {
          ...state.settings.radio,
          tci: { ...state.settings.radio.tci, ...tci },
        },
      },
      isDirty: true,
    })),

  updateFlrigSettings: (flrig) =>
    set((state) => ({
      settings: {
        ...state.settings,
        radio: {
          ...state.settings.radio,
          flrig: { ...state.settings.radio.flrig, ...flrig },
        },
      },
      isDirty: true,
    })),

  // Map settings
  updateMapSettings: (map) =>
    set((state) => ({
      settings: {
        ...state.settings,
        map: { ...state.settings.map, ...map },
      },
      isDirty: true,
    })),

  // Header settings
  updateHeaderSettings: (header) =>
    set((state) => ({
      settings: {
        ...state.settings,
        header: { ...state.settings.header, ...header },
      },
      isDirty: true,
    })),

  // AI settings
  updateAiSettings: (ai) =>
    set((state) => ({
      settings: {
        ...state.settings,
        ai: { ...state.settings.ai, ...ai },
      },
      isDirty: true,
    })),


  // Backup settings
  updateBackupSettings: (backup) =>
    set((state) => ({
      settings: {
        ...state.settings,
        backup: { ...state.settings.backup, ...backup },
      },
      isDirty: true,
    })),

  // Hot list settings
  updateHotListSettings: (hotList) =>
    set((state) => ({
      settings: {
        ...state.settings,
        hotList: { ...state.settings.hotList, ...hotList },
      },
      isDirty: true,
    })),

  // WSJT-X settings
  updateWsjtxSettings: (wsjtx) =>
    set((state) => ({
      settings: {
        ...state.settings,
        wsjtx: { ...state.settings.wsjtx, ...wsjtx },
      },
      isDirty: true,
    })),

  // Decode-alert rules
  updateDecodeAlertsSettings: (decodeAlerts) =>
    set((state) => ({
      settings: {
        ...state.settings,
        decodeAlerts: { ...state.settings.decodeAlerts, ...decodeAlerts },
      },
      isDirty: true,
    })),

  // Weather settings (deep-merges the three subsections)
  updateWeatherSettings: (weather) =>
    set((state) => ({
      settings: {
        ...state.settings,
        weather: {
          lightning: { ...state.settings.weather.lightning, ...weather.lightning },
          wind: { ...state.settings.weather.wind, ...weather.wind },
          credentials: { ...state.settings.weather.credentials, ...weather.credentials },
        },
      },
      isDirty: true,
    })),

  // S.A.T. settings
  updateSatSettings: (sat) =>
    set((state) => ({
      settings: {
        ...state.settings,
        sat: { ...state.settings.sat, ...sat },
      },
      isDirty: true,
    })),

  // Contest interop settings (N1MM UDP / online score)
  updateContestSettings: (contest) =>
    set((state) => ({
      settings: {
        ...state.settings,
        contest: { ...state.settings.contest, ...contest },
      },
      isDirty: true,
    })),

  // Spot status settings
  updateSpotStatusSettings: (spotStatus) =>
    set((state) => ({
      settings: {
        ...state.settings,
        spotStatus: {
          ...state.settings.spotStatus,
          ...spotStatus,
          colors: spotStatus.colors
            ? { ...state.settings.spotStatus.colors, ...spotStatus.colors }
            : state.settings.spotStatus.colors,
          show: spotStatus.show
            ? { ...state.settings.spotStatus.show, ...spotStatus.show }
            : state.settings.spotStatus.show,
        },
      },
      isDirty: true,
    })),

  // Cluster settings
  updateClusterSettings: (cluster) =>
    set((state) => ({
      settings: {
        ...state.settings,
        cluster: { ...state.settings.cluster, ...cluster },
      },
      isDirty: true,
    })),

  // Update a specific cluster connection by ID
  updateClusterConnection: (connectionId, connection) =>
    set((state) => ({
      settings: {
        ...state.settings,
        cluster: {
          ...state.settings.cluster,
          connections: state.settings.cluster.connections.map((c) =>
            c.id === connectionId ? { ...c, ...connection } : c
          ),
        },
      },
      isDirty: true,
    })),

  // Add a new cluster connection
  addClusterConnection: () =>
    set((state) => {
      const newConnection: ClusterConnection = {
        id: crypto.randomUUID(),
        name: `Cluster ${state.settings.cluster.connections.length + 1}`,
        host: '',
        port: 23,
        callsign: null,
        password: null,
        enabled: true,
        autoReconnect: false,
      };
      return {
        settings: {
          ...state.settings,
          cluster: {
            ...state.settings.cluster,
            connections: [...state.settings.cluster.connections, newConnection],
          },
        },
        isDirty: true,
      };
    }),

  // Remove a cluster connection by ID
  removeClusterConnection: (connectionId) =>
    set((state) => ({
      settings: {
        ...state.settings,
        cluster: {
          ...state.settings.cluster,
          connections: state.settings.cluster.connections.filter(
            (c) => c.id !== connectionId
          ),
        },
      },
      isDirty: true,
    })),

  // Save to backend via API
  saveSettings: async () => {
    const { isLoaded } = get();
    if (!isLoaded) {
      const errorMsg = 'Settings not loaded yet, cannot save';
      console.warn(errorMsg);
      set({ error: errorMsg });
      return;
    }
    set({ isSaving: true, error: null });
    try {
      const { settings } = get();
      const response = await fetch('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(settings),
      });

      if (!response.ok) {
        // Try to get error message from response body
        let errorMsg = `Failed to save settings (HTTP ${response.status})`;
        try {
          const errorData = await response.json();
          if (errorData.error) {
            errorMsg = errorData.error;
          }
        } catch {
          // If JSON parsing fails, use default error message
        }
        throw new Error(errorMsg);
      }

      set({ isDirty: false, error: null });
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Failed to save settings';
      console.error('Failed to save settings:', error);
      set({ error: errorMsg });
      throw error;
    } finally {
      set({ isSaving: false });
    }
  },

  // Load from backend
  loadSettings: async () => {
    try {
      const response = await fetch('/api/settings');
      if (response.ok) {
        const settings = await response.json();
        // Deep merge with defaults to handle missing fields
        const mergedSettings: Settings = {
          station: { ...defaultSettings.station, ...settings.station },
          qrz: { ...defaultSettings.qrz, ...settings.qrz },
          hamQth: { ...defaultSettings.hamQth, ...settings.hamQth },
          lotw: { ...defaultSettings.lotw, ...settings.lotw },
          clubLog: { ...defaultSettings.clubLog, ...settings.clubLog },
          hrdLog: { ...defaultSettings.hrdLog, ...settings.hrdLog },
          eqsl: { ...defaultSettings.eqsl, ...settings.eqsl },
          confirmationSync: { ...defaultSettings.confirmationSync, ...settings.confirmationSync },
          pota: { ...defaultSettings.pota, ...settings.pota },
          dxCoach: { ...defaultSettings.dxCoach, ...settings.dxCoach },
          voice: {
            ...defaultSettings.voice,
            ...settings.voice,
            // Carry a customized volume over from the old RBN-only slider.
            volume:
              settings.voice?.volume ??
              (settings.rbnAlerts as { voiceVolume?: number } | undefined)?.voiceVolume ??
              defaultSettings.voice.volume,
          },
          adifMonitor: { ...defaultSettings.adifMonitor, ...settings.adifMonitor },
          adifUdp: { ...defaultSettings.adifUdp, ...settings.adifUdp },
          rbnAlerts: { ...defaultSettings.rbnAlerts, ...settings.rbnAlerts },
          appearance: {
            ...defaultSettings.appearance,
            ...settings.appearance,
            // Seed the master unit system for installs that predate it, from the
            // legacy standalone distance toggle (mi → imperial, else metric).
            unitSystem: settings.appearance?.unitSystem
              ?? (settings.appearance?.distanceUnit === 'mi' ? 'imperial' : 'metric'),
            customColors: {
              ...defaultSettings.appearance.customColors,
              ...settings.appearance?.customColors,
            },
          },
          rotator: { ...defaultSettings.rotator, ...settings.rotator },
          radio: {
            ...defaultSettings.radio,
            ...settings.radio,
            activeRigType: settings.radio?.activeRigType ?? null,
            autoReconnect: settings.radio?.autoReconnect ?? false,
            autoConnectRigId: settings.radio?.autoConnectRigId ?? null,
            reconnectLastOnStartup: settings.radio?.reconnectLastOnStartup ?? true,
            scrollTuneStepHz: settings.radio?.scrollTuneStepHz ?? 100,
            tci: { ...defaultSettings.radio.tci, ...settings.radio?.tci, host: settings.radio?.tci?.host ?? '', name: settings.radio?.tci?.name ?? '' },
            flrig: { ...defaultSettings.radio.flrig, ...settings.radio?.flrig },
          },
          map: {
            ...defaultSettings.map,
            ...settings.map,
            rbn: { ...defaultSettings.map.rbn, ...settings.map?.rbn },
          },
          cluster: { ...defaultSettings.cluster, ...settings.cluster },
          spotStatus: {
            ...defaultSettings.spotStatus,
            ...settings.spotStatus,
            colors: { ...defaultSettings.spotStatus.colors, ...settings.spotStatus?.colors },
            show: { ...defaultSettings.spotStatus.show, ...settings.spotStatus?.show },
          },
          header: { ...defaultSettings.header, ...settings.header },
          ai: { ...defaultSettings.ai, ...settings.ai },
          backup: { ...defaultSettings.backup, ...settings.backup },
          hotList: { ...defaultSettings.hotList, ...settings.hotList },
          wsjtx: {
            ...defaultSettings.wsjtx,
            ...settings.wsjtx,
            source2: { ...defaultSettings.wsjtx.source2, ...settings.wsjtx?.source2 },
          },
          decodeAlerts: {
            ...defaultSettings.decodeAlerts,
            ...settings.decodeAlerts,
            rules: settings.decodeAlerts?.rules ?? defaultSettings.decodeAlerts.rules,
          },
          weather: {
            lightning: { ...defaultSettings.weather.lightning, ...settings.weather?.lightning },
            wind: { ...defaultSettings.weather.wind, ...settings.weather?.wind },
            credentials: { ...defaultSettings.weather.credentials, ...settings.weather?.credentials },
          },
          sat: { ...defaultSettings.sat, ...settings.sat },
          contest: { ...defaultSettings.contest, ...settings.contest },
          gridStates: { ...defaultSettings.gridStates, ...settings.gridStates },
        };
        // One-time migration: clear the legacy "United States" spotter-country
        // default so existing installs receive worldwide spots (and a full
        // band-activity heat map). Runs once per machine, then never re-touches
        // the value — a user who deliberately sets a country later keeps it.
        const SPOTTER_MIGRATION_KEY = 'migration_spotterCountry_worldwide_v1';
        let migratedSettings = mergedSettings;
        try {
          if (!localStorage.getItem(SPOTTER_MIGRATION_KEY)) {
            if (mergedSettings.cluster.spotholeSpotterCountry === 'United States') {
              migratedSettings = {
                ...mergedSettings,
                cluster: { ...mergedSettings.cluster, spotholeSpotterCountry: '' },
              };
            }
            localStorage.setItem(SPOTTER_MIGRATION_KEY, '1');
          }
        } catch {
          /* localStorage unavailable — skip migration */
        }

        const didMigrate = migratedSettings !== mergedSettings;
        set({ settings: migratedSettings, isDirty: false, isLoaded: true });
        // Persist the cleared value so the backend stops filtering immediately.
        if (didMigrate) {
          get().saveSettings().catch((e) => console.warn('Spotter-country migration save failed:', e));
        }
      } else {
        // Non-OK response (e.g., 404, 500) - database might not be available
        // Still mark as loaded so user can configure initial settings
        console.warn('Settings API returned non-OK status:', response.status);
        set({ isLoaded: true });
      }
    } catch (error) {
      // Network error or database not connected
      // Still mark as loaded so user can configure initial settings
      console.warn('Failed to load settings (database may not be connected):', error);
      set({ isLoaded: true });
    }
  },

  // Reset to defaults
  resetSettings: () =>
    set({
      settings: defaultSettings,
      isDirty: true,
    }),

  // Reset loaded state (for reconnection scenarios - prevents saving stale data)
  setNotLoaded: () => set({ isLoaded: false }),
}));

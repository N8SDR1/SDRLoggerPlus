import { useEffect, useRef, useState } from 'react';
import {
  X,
  Settings,
  Radio,
  Globe,
  Palette,
  Info,
  Save,
  RotateCcw,
  Eye,
  EyeOff,
  MapPin,
  User,
  Key,
  CheckCircle,
  AlertCircle,
  Compass,
  Wifi,
  WifiOff,
  Loader2,
  Server,
  ExternalLink,
  ChevronDown,
  ChevronUp,
  HelpCircle,
  Download,
  Terminal,
  Copy,
  Check,
  Map,
  Bot,
  Sun,
  Moon,
  CloudUpload,
  FileCode,
  FolderOpen,
  Trash2,
  Archive,
  Waves,
  Bell,
  Satellite,
  RadioTower,
  Radar,
  Sparkles,
  Snowflake,
  MessageSquare,
  Newspaper,
  Activity,
} from 'lucide-react';
import { useSettingsStore, SettingsSection, StationSettings, WsjtxSource } from '../store/settingsStore';
import { getSeedColors, type ThemeId, type CustomColors } from '../theme/themes';
import { api, type BackupStatus, type WsjtxStatus, type SavedLayoutSlot } from '../api/client';
import { useLayoutStore } from '../store/layoutStore';
import { useWeatherPreviewStore } from '../store/weatherPreviewStore';
import { Model } from 'flexlayout-react';
import { gridToLatLon } from '../utils/maidenhead';
import { distanceUnitFor, resolveSpeedUnit } from '../utils/units';
import { APP_VERSION } from '../version';

// Settings navigation items
const SETTINGS_SECTIONS: { id: SettingsSection; name: string; icon: React.ReactNode; description: string }[] = [
  {
    id: 'station',
    name: 'Station',
    icon: <Radio className="w-5 h-5" />,
    description: 'Callsign, location, and operator info',
  },
  {
    id: 'weblogbooks',
    name: 'Web Logbooks',
    icon: <CloudUpload className="w-5 h-5" />,
    description: 'QRZ, LOTW, Club Log, HRDLog, eQSL',
  },
  {
    id: 'alerts',
    name: 'Alerts',
    icon: <Bell className="w-5 h-5" />,
    description: 'Weather and Hot List alerts',
  },
  {
    id: 'adifmonitor',
    name: 'ADIF Monitor',
    icon: <FileCode className="w-5 h-5" />,
    description: 'Auto-import QSOs from external .adi files',
  },
  {
    id: 'wsjtx',
    name: 'WSJT-X / JTDX',
    icon: <RadioTower className="w-5 h-5" />,
    description: 'Auto-log FT8/FT4 QSOs over UDP (WSJT-X, JTDX, MSHV)',
  },
  {
    id: 'rbnalerts',
    name: 'Band Openings',
    icon: <Waves className="w-5 h-5" />,
    description: 'RBN VHF/UHF band-opening alerts with voice',
  },
  {
    id: 'rotator',
    name: 'Rotator',
    icon: <Compass className="w-5 h-5" />,
    description: 'Hamlib rotctld connection',
  },
  {
    id: 'backup',
    name: 'Backup & Restore',
    icon: <Archive className="w-5 h-5" />,
    description: 'Logbook backups + settings export / import',
  },
  {
    id: 'sat',
    name: 'S.A.T.',
    icon: <Satellite className="w-5 h-5" />,
    description: 'CSN satellite controller link',
  },
  {
    id: 'appearance',
    name: 'Appearance',
    icon: <Palette className="w-5 h-5" />,
    description: 'Theme and display options',
  },
  {
    id: 'map',
    name: 'Map',
    icon: <Map className="w-5 h-5" />,
    description: 'Map overlay and satellite settings',
  },
  {
    id: 'header',
    name: 'Header Bar',
    icon: <Eye className="w-5 h-5" />,
    description: 'Customize header display settings',
  },
  {
    id: 'ai',
    name: 'Chat AI',
    icon: <Bot className="w-5 h-5" />,
    description: 'LLM API settings for talk points',
  },
  {
    id: 'about',
    name: 'About',
    icon: <Info className="w-5 h-5" />,
    description: 'Version and license info',
  },
];

// Station Settings Section
function StationSettingsSection() {
  const { settings, updateStationSettings } = useSettingsStore();
  const station = settings.station;
  const lastAutoFilledCoords = useRef<{ lat: number; lon: number } | null>(null);

  // Station info sync to app store is now handled in App.tsx
  // to ensure it runs even when settings panel is not open

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">Station Information</h3>
        <p className="text-sm text-dark-300">Configure your station callsign and location details.</p>
      </div>

      <div className="grid grid-cols-2 gap-4">
        {/* Callsign */}
        <div className="space-y-2">
          <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
            <Radio className="w-4 h-4 text-accent-primary" />
            Callsign
          </label>
          <input
            type="text"
            value={station.callsign}
            onChange={(e) => updateStationSettings({ callsign: e.target.value.toUpperCase() })}
            placeholder="e.g. N9BC"
            className="glass-input w-full font-mono uppercase"
          />
        </div>

        {/* Operator Name */}
        <div className="space-y-2">
          <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
            <User className="w-4 h-4 text-accent-primary" />
            Operator Name
          </label>
          <input
            type="text"
            value={station.operatorName}
            onChange={(e) => updateStationSettings({ operatorName: e.target.value })}
            placeholder="Your name"
            className="glass-input w-full"
          />
        </div>

        {/* Grid Square */}
        <div className="space-y-2">
          <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
            <MapPin className="w-4 h-4 text-accent-primary" />
            Grid Square (Maidenhead)
          </label>
          <input
            type="text"
            value={station.gridSquare}
            onChange={(e) => {
              const grid = e.target.value.toUpperCase();
              const updates: Partial<StationSettings> = { gridSquare: grid };

              // Auto-fill coordinates if not manually set, and refine as grid gets more precise
              const bothNull = station.latitude === null && station.longitude === null;
              const wasAutoFilled = lastAutoFilledCoords.current &&
                station.latitude === lastAutoFilledCoords.current.lat &&
                station.longitude === lastAutoFilledCoords.current.lon;

              if (bothNull || wasAutoFilled) {
                const coords = gridToLatLon(grid);
                if (coords) {
                  const lat = Math.round(coords.lat * 10000) / 10000;
                  const lon = Math.round(coords.lon * 10000) / 10000;
                  updates.latitude = lat;
                  updates.longitude = lon;
                  lastAutoFilledCoords.current = { lat, lon };
                }
              }

              updateStationSettings(updates);
            }}
            placeholder="e.g. IO63"
            maxLength={8}
            className="glass-input w-full font-mono uppercase"
          />
        </div>

        {/* City */}
        <div className="space-y-2">
          <label className="text-sm font-medium font-ui text-dark-200">City</label>
          <input
            type="text"
            value={station.city}
            onChange={(e) => updateStationSettings({ city: e.target.value })}
            placeholder="Your city"
            className="glass-input w-full"
          />
        </div>

        {/* Country */}
        <div className="space-y-2">
          <label className="text-sm font-medium font-ui text-dark-200">Country</label>
          <input
            type="text"
            value={station.country}
            onChange={(e) => updateStationSettings({ country: e.target.value })}
            placeholder="Your country"
            className="glass-input w-full"
          />
        </div>

        {/* ITU Region */}
        <div className="space-y-2">
          <label className="text-sm font-medium font-ui text-dark-200">ITU Region</label>
          <select
            value={station.ituRegion || 2}
            onChange={(e) => updateStationSettings({ ituRegion: parseInt(e.target.value) })}
            className="glass-input w-full"
          >
            <option value={1}>Region 1 — Europe, Africa, Middle East, Russia</option>
            <option value={2}>Region 2 — Americas</option>
            <option value={3}>Region 3 — Asia-Pacific</option>
          </select>
          <p className="text-xs text-dark-300">
            Sets the band plan used for out-of-band VFO warnings
          </p>
        </div>
      </div>

      {/* Coordinates */}
      <div className="border-t border-glass-100 pt-4">
        <h4 className="text-sm font-medium font-ui text-dark-200 mb-3">Coordinates (Optional)</h4>
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <label className="text-sm font-ui text-dark-300">Latitude</label>
            <input
              type="number"
              step="0.0001"
              value={station.latitude ?? ''}
              onChange={(e) =>
                updateStationSettings({
                  latitude: e.target.value ? parseFloat(e.target.value) : null,
                })
              }
              placeholder="e.g. 52.6667"
              className="glass-input w-full font-mono"
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm font-ui text-dark-300">Longitude</label>
            <input
              type="number"
              step="0.0001"
              value={station.longitude ?? ''}
              onChange={(e) =>
                updateStationSettings({
                  longitude: e.target.value ? parseFloat(e.target.value) : null,
                })
              }
              placeholder="e.g. -8.6333"
              className="glass-input w-full font-mono"
            />
          </div>
        </div>
      </div>
    </div>
  );
}

// QRZ Settings Section
function QrzSettingsSection() {
  const { settings, updateQrzSettings } = useSettingsStore();
  const [showPassword, setShowPassword] = useState(false);
  const [showApiKey, setShowApiKey] = useState(false);
  const [testStatus, setTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [testMessage, setTestMessage] = useState('');
  const [hasXmlSubscription, setHasXmlSubscription] = useState<boolean | null>(null);

  const qrz = settings.qrz;
  const password = qrz.password;
  const apiKey = qrz.apiKey;

  const handleTestConnection = async () => {
    setTestStatus('testing');
    setTestMessage('');
    try {
      const response = await fetch('/api/qrz/settings', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          username: qrz.username,
          password: password,
          apiKey: apiKey,
          enabled: qrz.enabled,
        }),
      });
      const data = await response.json();
      if (data.success) {
        setTestStatus('success');
        setTestMessage(data.message || 'Connected successfully');
        setHasXmlSubscription(data.hasXmlSubscription);
      } else {
        setTestStatus('error');
        setTestMessage(data.message || 'Connection failed');
      }
    } catch {
      setTestStatus('error');
      setTestMessage('Failed to connect to server');
    }
    setTimeout(() => {
      setTestStatus('idle');
      setTestMessage('');
    }, 5000);
  };

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">QRZ.com Integration</h3>
        <p className="text-sm text-dark-300">
          Configure your QRZ.com credentials for callsign lookups and log uploads.
        </p>
      </div>

      {/* Enable toggle */}
      <div className="flex items-center justify-between p-4 bg-dark-700/50 rounded-lg border border-glass-100">
        <div>
          <p className="font-medium font-ui text-dark-200">Enable QRZ Lookups</p>
          <p className="text-sm text-dark-300">Use QRZ.com for callsign information</p>
        </div>
        <button
          onClick={() => updateQrzSettings({ enabled: !qrz.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${
            qrz.enabled ? 'bg-accent-success' : 'bg-dark-600 border border-dark-400'
          }`}
        >
          <span
            className={`absolute top-0.5 left-0.5 w-5 h-5 rounded-full bg-white shadow transition-transform duration-200 ${
              qrz.enabled ? 'translate-x-5' : 'translate-x-0'
            }`}
          />
        </button>
      </div>

      {/* Subscription Status */}
      {hasXmlSubscription !== null && (
        <div className={`flex items-center gap-2 p-3 rounded-lg border ${
          hasXmlSubscription
            ? 'bg-accent-success/10 border-accent-success/30 text-accent-success'
            : 'bg-accent-primary/10 border-accent-primary/30 text-accent-primary'
        }`}>
          {hasXmlSubscription ? (
            <>
              <CheckCircle className="w-4 h-4" />
              <span className="text-sm">XML Subscription active - callsign lookups enabled</span>
            </>
          ) : (
            <>
              <AlertCircle className="w-4 h-4" />
              <span className="text-sm">No XML subscription - callsign lookups require a QRZ subscription</span>
            </>
          )}
        </div>
      )}

      {/* Credentials */}
      <div className={`space-y-4 ${!qrz.enabled ? 'opacity-50 pointer-events-none' : ''}`}>
        <div className="space-y-2">
          <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
            <User className="w-4 h-4 text-accent-primary" />
            QRZ Username (Callsign)
          </label>
          <input
            type="text"
            value={qrz.username}
            onChange={(e) => updateQrzSettings({ username: e.target.value.toUpperCase() })}
            placeholder="Your QRZ.com callsign"
            className="glass-input w-full font-mono uppercase"
            disabled={!qrz.enabled}
          />
        </div>

        <div className="space-y-2">
          <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
            <Key className="w-4 h-4 text-accent-primary" />
            QRZ Password
          </label>
          <div className="relative">
            <input
              type={showPassword ? 'text' : 'password'}
              value={password}
              onChange={(e) => updateQrzSettings({ password: e.target.value })}
              placeholder="Your QRZ.com password"
              className="glass-input w-full pr-10"
              disabled={!qrz.enabled}
            />
            <button
              type="button"
              onClick={() => setShowPassword(!showPassword)}
              className="absolute right-2 top-1/2 -translate-y-1/2 p-1 text-dark-300 hover:text-dark-200"
            >
              {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
            </button>
          </div>
          <p className="text-xs text-dark-300">
            Required for callsign lookups (requires XML subscription on QRZ.com).
          </p>
        </div>

        {/* API Key for Logbook */}
        <div className="pt-4 border-t border-glass-100">
          <h4 className="text-sm font-medium font-ui text-dark-200 mb-3">QRZ Logbook Integration</h4>
          <div className="space-y-2">
            <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
              <Key className="w-4 h-4 text-accent-info" />
              Logbook API Key
            </label>
            <div className="relative">
              <input
                type={showApiKey ? 'text' : 'password'}
                value={apiKey}
                onChange={(e) => updateQrzSettings({ apiKey: e.target.value })}
                placeholder="Your QRZ Logbook API Key"
                className="glass-input w-full pr-10 font-mono text-sm"
                disabled={!qrz.enabled}
              />
              <button
                type="button"
                onClick={() => setShowApiKey(!showApiKey)}
                className="absolute right-2 top-1/2 -translate-y-1/2 p-1 text-dark-300 hover:text-dark-200"
              >
                {showApiKey ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
              </button>
            </div>
            <p className="text-xs text-dark-300">
              Get your API key from{' '}
              <a
                href="https://logbook.qrz.com/logbook"
                target="_blank"
                rel="noopener noreferrer"
                className="text-accent-primary hover:underline"
              >
                QRZ.com Logbook Settings
              </a>
              . Required for uploading QSOs to QRZ.
            </p>
          </div>
        </div>

        {/* Test connection */}
        <div className="pt-4 flex items-center gap-4">
          <button
            onClick={handleTestConnection}
            disabled={!qrz.username || !password || testStatus === 'testing'}
            className="glass-button px-4 py-2 flex items-center gap-2 disabled:opacity-50"
          >
            {testStatus === 'testing' && (
              <div className="w-4 h-4 border-2 border-accent-primary border-t-transparent rounded-full animate-spin" />
            )}
            {testStatus === 'success' && <CheckCircle className="w-4 h-4 text-accent-success" />}
            {testStatus === 'error' && <AlertCircle className="w-4 h-4 text-accent-danger" />}
            {testStatus === 'idle' && <Globe className="w-4 h-4" />}
            <span>
              {testStatus === 'testing'
                ? 'Testing...'
                : testStatus === 'success'
                  ? 'Connected!'
                  : testStatus === 'error'
                    ? 'Failed'
                    : 'Test & Save Credentials'}
            </span>
          </button>
          {testMessage && (
            <span className={`text-sm ${testStatus === 'success' ? 'text-accent-success' : 'text-accent-danger'}`}>
              {testMessage}
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

// LOTW Settings Section — minimal: just what the upload path needs (TQSL binary + optional
// station location). LOTW user/password are only needed for the confirmation-download phase,
// which is a future add-on — not included here.
function LotwSettingsSection() {
  const { settings, updateLotwSettings } = useSettingsStore();
  const [testStatus, setTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [testMessage, setTestMessage] = useState('');

  const lotw = settings.lotw;

  const handleTest = async () => {
    if (!lotw.tqslPath) return;
    setTestStatus('testing');
    setTestMessage('');
    try {
      const response = await fetch('/api/lotw/test-tqsl', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: lotw.tqslPath }),
      });
      const data = await response.json();
      if (data.ok) {
        setTestStatus('success');
        setTestMessage(data.version || 'TQSL found');
      } else {
        setTestStatus('error');
        setTestMessage(data.error || 'Could not run TQSL');
      }
    } catch {
      setTestStatus('error');
      setTestMessage('Failed to reach backend');
    }
    setTimeout(() => { setTestStatus('idle'); setTestMessage(''); }, 5000);
  };

  // Shown only when running under Electron — the preload script exposes the native picker.
  // In browser/Vite-dev, the user types/pastes the path manually.
  const hasElectronFilePicker = typeof window !== 'undefined'
    && (window as unknown as { electronAPI?: { selectFile?: unknown } }).electronAPI?.selectFile !== undefined;

  const handleBrowse = async () => {
    const api = (window as unknown as { electronAPI: { selectFile: (opts: {
      title?: string;
      defaultPath?: string;
      filters?: { name: string; extensions: string[] }[];
    }) => Promise<string | null> } }).electronAPI;

    // Platform-aware filters. On macOS the user usually drills into Tqsl.app/Contents/MacOS/tqsl,
    // so we don't restrict by extension there. Windows wants .exe.
    const platform = navigator.platform.toLowerCase();
    const filters = platform.includes('win')
      ? [{ name: 'Executables', extensions: ['exe'] }, { name: 'All Files', extensions: ['*'] }]
      : [{ name: 'All Files', extensions: ['*'] }];

    const picked = await api.selectFile({
      title: 'Locate tqsl executable',
      defaultPath: lotw.tqslPath || undefined,
      filters
    });
    if (picked) updateLotwSettings({ tqslPath: picked });
  };

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">LOTW (Logbook of The World)</h3>
        <p className="text-sm text-dark-300">
          Sign and upload QSOs to ARRL LOTW via your local TQSL install.
          TQSL must be installed separately — download from{' '}
          <a
            href="https://lotw.arrl.org/lotw-help/installation/"
            target="_blank"
            rel="noopener noreferrer"
            className="text-accent-primary hover:underline"
          >
            ARRL
          </a>.
        </p>
      </div>

      {/* Enable toggle */}
      <div className="flex items-center justify-between p-4 bg-dark-700/50 rounded-lg border border-glass-100">
        <div>
          <p className="font-medium font-ui text-dark-200">Enable LOTW Upload</p>
          <p className="text-sm text-dark-300">Shows a "Push to LOTW" button in Log History</p>
        </div>
        <button
          onClick={() => updateLotwSettings({ enabled: !lotw.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${
            lotw.enabled ? 'bg-accent-success' : 'bg-dark-600 border border-dark-400'
          }`}
        >
          <span
            className={`absolute top-0.5 left-0.5 w-5 h-5 rounded-full bg-white shadow transition-transform duration-200 ${
              lotw.enabled ? 'translate-x-5' : 'translate-x-0'
            }`}
          />
        </button>
      </div>

      <div className={`space-y-4 ${!lotw.enabled ? 'opacity-50 pointer-events-none' : ''}`}>
        {/* TQSL binary path */}
        <div className="space-y-2">
          <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
            <FileCode className="w-4 h-4 text-accent-primary" />
            Path to TQSL Executable
          </label>
          <div className="flex gap-2">
            <input
              type="text"
              value={lotw.tqslPath}
              onChange={(e) => updateLotwSettings({ tqslPath: e.target.value })}
              placeholder="Not set — click Browse to locate tqsl"
              className="glass-input w-full font-mono text-sm"
              disabled={!lotw.enabled}
              spellCheck={false}
            />
            {hasElectronFilePicker && (
              <button
                onClick={handleBrowse}
                disabled={!lotw.enabled}
                className="glass-button px-4 py-2 flex items-center gap-2 disabled:opacity-50 whitespace-nowrap"
                title="Browse for the tqsl executable"
              >
                <FolderOpen className="w-4 h-4" />
                <span>Browse</span>
              </button>
            )}
            <button
              onClick={handleTest}
              disabled={!lotw.tqslPath || testStatus === 'testing'}
              className="glass-button px-4 py-2 flex items-center gap-2 disabled:opacity-50 whitespace-nowrap"
            >
              {testStatus === 'testing' && <Loader2 className="w-4 h-4 animate-spin" />}
              {testStatus === 'success' && <CheckCircle className="w-4 h-4 text-accent-success" />}
              {testStatus === 'error' && <AlertCircle className="w-4 h-4 text-accent-danger" />}
              {testStatus === 'idle' && <CheckCircle className="w-4 h-4" />}
              <span>{testStatus === 'testing' ? 'Testing...' : 'Test'}</span>
            </button>
          </div>
          {testMessage && (
            <p className={`text-xs ${testStatus === 'success' ? 'text-accent-success' : 'text-accent-danger'}`}>
              {testMessage}
            </p>
          )}
          <p className="text-xs text-dark-300">
            Absolute path to the tqsl binary. SDRLoggerPlus shells out to it for LOTW signing and upload —
            same mechanism TQSL uses when you run it manually.
          </p>
        </div>

        {/* Optional station location override */}
        <div className="space-y-2">
          <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
            <User className="w-4 h-4 text-accent-primary" />
            TQSL Station Location <span className="text-xs text-dark-300">(optional)</span>
          </label>
          <input
            type="text"
            value={lotw.stationCallsign}
            onChange={(e) => updateLotwSettings({ stationCallsign: e.target.value })}
            placeholder="Leave blank to use TQSL's default"
            className="glass-input w-full"
            disabled={!lotw.enabled}
          />
          <p className="text-xs text-dark-300">
            Passed to TQSL as <code className="font-mono">-l &lt;name&gt;</code>.
            Useful if your TQSL has multiple station locations configured.
          </p>
        </div>

      </div>
    </div>
  );
}

// Hamlib Rotator Model type
interface HamlibRotatorModel {
  modelId: number;
  manufacturer: string;
  modelName: string;
  displayName: string;
}

// Rotator Settings Section
function RotatorSettingsSection() {
  const { settings, updateRotatorSettings } = useSettingsStore();
  const rotator = settings.rotator;
  const [showSetupHelp, setShowSetupHelp] = useState(false);
  const [showModelBrowser, setShowModelBrowser] = useState(false);
  const [testStatus, setTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [testMessage, setTestMessage] = useState('');
  const [copiedCommand, setCopiedCommand] = useState<string | null>(null);
  const [rotatorModels, setRotatorModels] = useState<HamlibRotatorModel[]>([]);
  const [loadingModels, setLoadingModels] = useState(false);
  const [modelSearchTerm, setModelSearchTerm] = useState('');

  // Load available rotator models from hamlib
  const loadRotatorModels = async () => {
    setLoadingModels(true);
    try {
      const response = await fetch('/api/hamlib/rotators');
      if (response.ok) {
        const models = await response.json();
        setRotatorModels(models);
      } else {
        console.error('Failed to load rotator models');
      }
    } catch (error) {
      console.error('Error loading rotator models:', error);
    } finally {
      setLoadingModels(false);
    }
  };

  // Load models on component mount
  useEffect(() => {
    loadRotatorModels();
  }, []);

  const handleTestConnection = async () => {
    setTestStatus('testing');
    setTestMessage('');
    try {
      // Test via backend API - real TCP connection using the selected protocol
      const response = await fetch(`/api/hamlib/test-rotator?host=${encodeURIComponent(rotator.ipAddress ?? '127.0.0.1')}&port=${rotator.port}&protocol=${rotator.protocol}`, {
        signal: AbortSignal.timeout(5000),
      });
      const result = await response.json();
      if (result.success) {
        setTestStatus('success');
        setTestMessage(result.message ?? 'Connected to rotctld successfully!');
      } else {
        setTestStatus('error');
        setTestMessage(result.message ?? 'Failed to connect to rotctld.');
      }
    } catch (error) {
      if (error instanceof Error && error.name === 'TimeoutError') {
        setTestStatus('error');
        setTestMessage('Connection timeout. Check if rotctld is running and firewall settings.');
      } else {
        setTestStatus('error');
        setTestMessage('Failed to reach backend. Is the server running?');
      }
    }
    setTimeout(() => {
      setTestStatus('idle');
      setTestMessage('');
    }, 5000);
  };

  // Filter models based on search term
  const filteredModels = rotatorModels.filter((model) =>
    model.displayName.toLowerCase().includes(modelSearchTerm.toLowerCase()) ||
    model.manufacturer.toLowerCase().includes(modelSearchTerm.toLowerCase()) ||
    model.modelId.toString().includes(modelSearchTerm)
  );

  const copyToClipboard = (text: string, id: string) => {
    navigator.clipboard.writeText(text);
    setCopiedCommand(id);
    setTimeout(() => setCopiedCommand(null), 2000);
  };

  return (
    <>
      {/* Model Browser Modal */}
      {showModelBrowser && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center">
          <div className="absolute inset-0 bg-dark-900/80 backdrop-blur-sm" onClick={() => setShowModelBrowser(false)} />
          <div className="relative w-full max-w-3xl max-h-[80vh] mx-4 bg-dark-800 border border-glass-200 rounded-xl shadow-2xl flex flex-col">
            {/* Header */}
            <div className="flex items-center justify-between p-4 border-b border-glass-100">
              <div>
                <h3 className="text-lg font-semibold font-display text-dark-200">Hamlib Rotator Models</h3>
                <p className="text-sm text-dark-300">Select your rotator model</p>
              </div>
              <button onClick={() => setShowModelBrowser(false)} className="p-2 hover:bg-dark-700 rounded-lg transition-colors">
                <X className="w-5 h-5" />
              </button>
            </div>

            {/* Search */}
            <div className="p-4 border-b border-glass-100">
              <input
                type="text"
                value={modelSearchTerm}
                onChange={(e) => setModelSearchTerm(e.target.value)}
                placeholder="Search by manufacturer, model, or ID..."
                className="glass-input w-full"
                autoFocus
              />
            </div>

            {/* Model List */}
            <div className="flex-1 overflow-auto p-4">
              {loadingModels ? (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="w-6 h-6 animate-spin text-accent-primary" />
                  <span className="ml-2 text-dark-300">Loading models...</span>
                </div>
              ) : filteredModels.length === 0 ? (
                <div className="text-center py-8 text-dark-300">
                  No models found matching "{modelSearchTerm}"
                </div>
              ) : (
                <div className="space-y-1">
                  {filteredModels.map((model) => (
                    <button
                      key={model.modelId}
                      onClick={() => {
                        updateRotatorSettings({
                          hamlibModelId: model.modelId,
                          hamlibModelName: model.displayName,
                        });
                        setShowModelBrowser(false);
                        setModelSearchTerm('');
                      }}
                      className={`w-full text-left p-3 rounded-lg border transition-all ${
                        rotator.hamlibModelId === model.modelId
                          ? 'border-accent-primary bg-accent-primary/10'
                          : 'border-glass-100 hover:border-glass-200 hover:bg-dark-700'
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <div>
                          <div className="font-medium text-dark-200">{model.displayName}</div>
                          <div className="text-xs text-dark-300">
                            {model.manufacturer} • Model ID: {model.modelId}
                          </div>
                        </div>
                        {rotator.hamlibModelId === model.modelId && (
                          <CheckCircle className="w-5 h-5 text-accent-success" />
                        )}
                      </div>
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Footer */}
            <div className="p-4 border-t border-glass-100">
              <p className="text-xs text-dark-300">
                Showing {filteredModels.length} of {rotatorModels.length} supported rotator models
              </p>
            </div>
          </div>
        </div>
      )}

      <div className="space-y-6">
        <div>
          <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">Rotator Control</h3>
          <p className="text-sm text-dark-300">
            Configure connection to hamlib for antenna rotator control.
          </p>
        </div>

      {/* Setup Help Banner */}
      <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-4">
        <button
          onClick={() => setShowSetupHelp(!showSetupHelp)}
          className="w-full flex items-center justify-between text-left"
        >
          <div className="flex items-center gap-3">
            <HelpCircle className="w-5 h-5 text-blue-400" />
            <div>
              <p className="font-medium text-blue-300">Need help setting up rotctld?</p>
              <p className="text-sm text-blue-400/70">Click here for installation and setup instructions</p>
            </div>
          </div>
          {showSetupHelp ? (
            <ChevronUp className="w-5 h-5 text-blue-400" />
          ) : (
            <ChevronDown className="w-5 h-5 text-blue-400" />
          )}
        </button>

        {showSetupHelp && (
          <div className="mt-4 space-y-4 pt-4 border-t border-blue-500/30">
            {/* What is rotctld */}
            <div>
              <h4 className="text-sm font-semibold text-blue-300 mb-2">What is rotctld?</h4>
              <p className="text-sm text-gray-400">
                rotctld is a TCP network daemon from the Hamlib project that controls antenna rotators.
                Multiple applications (like SDRLoggerPlus and QLog) can connect to the same rotctld instance
                to share control of your rotator.
              </p>
            </div>

            {/* Installation */}
            <div>
              <h4 className="text-sm font-semibold text-blue-300 mb-2 flex items-center gap-2">
                <Download className="w-4 h-4" />
                Installation
              </h4>
              <div className="space-y-2 text-sm text-gray-400">
                <div>
                  <p className="font-medium text-gray-300 mb-1">Windows:</p>
                  <p>
                    Download from{' '}
                    <a
                      href="https://github.com/Hamlib/Hamlib/releases"
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-accent-primary hover:underline inline-flex items-center gap-1"
                    >
                      Hamlib releases <ExternalLink className="w-3 h-3" />
                    </a>
                  </p>
                  <p className="text-xs text-gray-500 mt-1">
                    Install to C:\Program Files\Hamlib\bin\ or similar
                  </p>
                </div>
                <div>
                  <p className="font-medium text-gray-300 mb-1">Linux:</p>
                  <div className="bg-dark-900 rounded p-2 font-mono text-xs">
                    <code>sudo apt install hamlib-utils</code> {/* Debian/Ubuntu */}
                  </div>
                </div>
                <div>
                  <p className="font-medium text-gray-300 mb-1">macOS:</p>
                  <div className="bg-dark-900 rounded p-2 font-mono text-xs">
                    <code>brew install hamlib</code>
                  </div>
                </div>
              </div>
            </div>

            {/* Finding your model */}
            <div>
              <h4 className="text-sm font-semibold text-blue-300 mb-2">Find your rotator model number</h4>
              <p className="text-sm text-gray-400 mb-2">
                Run this command to see all supported rotators:
              </p>
              <div className="bg-dark-900 rounded p-2 font-mono text-xs flex items-center justify-between">
                <code>rotctl -l</code>
                <button
                  onClick={() => copyToClipboard('rotctl -l', 'list')}
                  className="p-1 hover:bg-dark-700 rounded"
                >
                  {copiedCommand === 'list' ? (
                    <Check className="w-3 h-3 text-green-400" />
                  ) : (
                    <Copy className="w-3 h-3 text-gray-500" />
                  )}
                </button>
              </div>
              <div className="mt-2 text-xs text-gray-500">
                Common models: 202 (Easycomm), 601 (Yaesu GS-232A), 603 (GS-232B), 902 (SPID)
              </div>
            </div>

            {/* Starting rotctld */}
            <div>
              <h4 className="text-sm font-semibold text-blue-300 mb-2 flex items-center gap-2">
                <Terminal className="w-4 h-4" />
                Starting rotctld
              </h4>
              <div className="space-y-3">
                <div>
                  <p className="text-sm text-gray-400 mb-2">
                    <strong className="text-gray-300">Linux/macOS example</strong> (SPID on USB):
                  </p>
                  <div className="bg-dark-900 rounded p-2 font-mono text-xs flex items-center justify-between">
                    <code>rotctld -m 902 -r /dev/ttyUSB0</code>
                    <button
                      onClick={() => copyToClipboard('rotctld -m 902 -r /dev/ttyUSB0', 'linux')}
                      className="p-1 hover:bg-dark-700 rounded"
                    >
                      {copiedCommand === 'linux' ? (
                        <Check className="w-3 h-3 text-green-400" />
                      ) : (
                        <Copy className="w-3 h-3 text-gray-500" />
                      )}
                    </button>
                  </div>
                </div>
                <div>
                  <p className="text-sm text-gray-400 mb-2">
                    <strong className="text-gray-300">Windows example</strong> (Yaesu on COM3):
                  </p>
                  <div className="bg-dark-900 rounded p-2 font-mono text-xs flex items-center justify-between">
                    <code>rotctld.exe -m 603 -r COM3 -s 9600</code>
                    <button
                      onClick={() => copyToClipboard('rotctld.exe -m 603 -r COM3 -s 9600', 'windows')}
                      className="p-1 hover:bg-dark-700 rounded"
                    >
                      {copiedCommand === 'windows' ? (
                        <Check className="w-3 h-3 text-green-400" />
                      ) : (
                        <Copy className="w-3 h-3 text-gray-500" />
                      )}
                    </button>
                  </div>
                  <p className="text-xs text-gray-500 mt-1">
                    Replace COM3 with your serial port and 9600 with your baud rate
                  </p>
                </div>
                <div className="bg-yellow-500/10 border border-yellow-500/30 rounded p-2">
                  <p className="text-xs text-yellow-400">
                    <strong>Tip:</strong> Leave the terminal window open while using SDRLoggerPlus. rotctld must stay
                    running in the background.
                  </p>
                </div>
              </div>
            </div>

            {/* Full documentation link */}
            <div className="pt-2 border-t border-blue-500/30">
              <a
                href="https://github.com/Hamlib/Hamlib/wiki"
                target="_blank"
                rel="noopener noreferrer"
                className="text-sm text-accent-primary hover:underline inline-flex items-center gap-1"
              >
                View complete Hamlib documentation <ExternalLink className="w-3 h-3" />
              </a>
            </div>
          </div>
        )}
      </div>

      {/* Enable toggle */}
      <div className="flex items-center justify-between p-4 bg-dark-700/50 rounded-lg border border-glass-100">
        <div className="flex items-center gap-3">
          {rotator.enabled ? (
            <Wifi className="w-5 h-5 text-accent-success" />
          ) : (
            <WifiOff className="w-5 h-5 text-dark-300" />
          )}
          <div>
            <p className="font-medium font-ui text-dark-200">Enable Rotator Control</p>
            <p className="text-sm text-dark-300">Connect to hamlib rotator control</p>
          </div>
        </div>
        <button
          onClick={() => updateRotatorSettings({ enabled: !rotator.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${
            rotator.enabled ? 'bg-accent-success' : 'bg-dark-600 border border-dark-400'
          }`}
        >
          <span
            className={`absolute top-0.5 left-0.5 w-5 h-5 rounded-full bg-white shadow transition-transform duration-200 ${
              rotator.enabled ? 'translate-x-5' : 'translate-x-0'
            }`}
          />
        </button>
      </div>

      {/* Connection Type Selector */}
      <div className={`space-y-4 ${!rotator.enabled ? 'opacity-50 pointer-events-none' : ''}`}>
        <div className="space-y-2">
          <label className="text-sm font-medium font-ui text-dark-200">Connection Type</label>
          <div className="grid grid-cols-2 gap-3">
            <button
              onClick={() => updateRotatorSettings({ connectionType: 'network' })}
              disabled={!rotator.enabled}
              className={`p-4 rounded-lg border transition-all ${
                rotator.connectionType === 'network'
                  ? 'border-accent-primary bg-accent-primary/10'
                  : 'border-glass-100 hover:border-glass-200'
              }`}
            >
              <div className="flex items-center gap-2 mb-1">
                <Globe className="w-4 h-4" />
                <span className="font-medium">Network</span>
              </div>
              <span className="text-xs text-dark-300">Connect to existing rotctld TCP server</span>
            </button>
            <button
              onClick={() => updateRotatorSettings({ connectionType: 'serial' })}
              disabled={!rotator.enabled}
              className={`p-4 rounded-lg border transition-all ${
                rotator.connectionType === 'serial'
                  ? 'border-accent-primary bg-accent-primary/10'
                  : 'border-glass-100 hover:border-glass-200'
              }`}
            >
              <div className="flex items-center gap-2 mb-1">
                <Server className="w-4 h-4" />
                <span className="font-medium">Serial</span>
              </div>
              <span className="text-xs text-dark-300">Direct serial port connection</span>
            </button>
          </div>
        </div>

        {/* Rotator Model Selection - only needed for serial (rotctld handles model on network) */}
        {rotator.connectionType === 'serial' && (
          <div className="space-y-2">
            <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
              <Compass className="w-4 h-4 text-accent-primary" />
              Rotator Model
            </label>
            <div className="flex gap-2">
              <select
                value={rotator.hamlibModelId ?? ''}
                onChange={(e) => {
                  const modelId = e.target.value ? parseInt(e.target.value) : null;
                  const model = rotatorModels.find((m) => m.modelId === modelId);
                  updateRotatorSettings({
                    hamlibModelId: modelId,
                    hamlibModelName: model?.displayName ?? '',
                  });
                }}
                disabled={!rotator.enabled || loadingModels}
                className="glass-input flex-1 min-w-0 font-mono text-sm"
              >
                <option value="">Select rotator model...</option>
                {rotatorModels.map((model) => (
                  <option key={model.modelId} value={model.modelId}>
                    {model.modelId} - {model.displayName}
                  </option>
                ))}
              </select>
              <button
                onClick={() => setShowModelBrowser(true)}
                disabled={!rotator.enabled || loadingModels}
                className="glass-button px-4 py-2 whitespace-nowrap disabled:opacity-50"
              >
                Browse Models
              </button>
            </div>
            <p className="text-xs text-dark-300">
              Select your rotator model from Hamlib's supported list.
              {loadingModels && ' Loading models...'}
            </p>
          </div>
        )}
      </div>

      {/* Network Connection Settings */}
      {rotator.connectionType === 'network' && (
        <div className={`space-y-4 ${!rotator.enabled ? 'opacity-50 pointer-events-none' : ''}`}>
          {/* Protocol */}
          <div className="space-y-2">
            <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
              <Compass className="w-4 h-4 text-accent-primary" />
              Protocol
            </label>
            <select
              value={rotator.protocol}
              onChange={(e) => updateRotatorSettings({ protocol: e.target.value as 'rotctld' | 'arco_tcp' })}
              className="glass-input w-full"
              disabled={!rotator.enabled}
            >
              <option value="rotctld">hamlib rotctld</option>
              <option value="arco_tcp">microHAM ARCO (GS-232A over TCP)</option>
            </select>
            <p className="text-xs text-dark-300">
              {rotator.protocol === 'arco_tcp'
                ? 'Connects directly to a microHAM ARCO controller over TCP — no rotctld needed.'
                : 'Connects to a hamlib rotctld daemon (default port 4533).'}
            </p>
          </div>

          <div className="grid grid-cols-2 gap-4">
            {/* IP Address */}
            <div className="space-y-2">
              <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
                <Globe className="w-4 h-4 text-accent-primary" />
                IP Address
              </label>
              <input
                type="text"
                value={rotator.ipAddress}
                onChange={(e) => updateRotatorSettings({ ipAddress: e.target.value })}
                placeholder="127.0.0.1"
                className="glass-input w-full font-mono"
                disabled={!rotator.enabled}
              />
              <p className="text-xs text-dark-300">
                {rotator.protocol === 'arco_tcp'
                  ? "The microHAM ARCO controller's IP address on your network"
                  : 'Use 127.0.0.1 if rotctld runs locally, or your server\'s IP'}
              </p>
            </div>

            {/* Port */}
            <div className="space-y-2">
              <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
                <Compass className="w-4 h-4 text-accent-primary" />
                Port
              </label>
              <input
                type="number"
                value={rotator.port}
                onChange={(e) => updateRotatorSettings({ port: parseInt(e.target.value) || 4533 })}
                placeholder="4533"
                className="glass-input w-full font-mono"
                disabled={!rotator.enabled}
              />
              <p className="text-xs text-dark-300">
                {rotator.protocol === 'arco_tcp'
                  ? "The ARCO's GS-232A-over-TCP port"
                  : 'Default rotctld port is 4533'}
              </p>
            </div>
          </div>

          {/* Test Connection Button */}
          <div className="pt-2 flex items-center gap-4">
            <button
              onClick={handleTestConnection}
              disabled={testStatus === 'testing'}
              className="glass-button px-4 py-2 flex items-center gap-2 disabled:opacity-50"
            >
              {testStatus === 'testing' && (
                <Loader2 className="w-4 h-4 animate-spin" />
              )}
              {testStatus === 'success' && <CheckCircle className="w-4 h-4 text-accent-success" />}
              {testStatus === 'error' && <AlertCircle className="w-4 h-4 text-accent-danger" />}
              {testStatus === 'idle' && <Wifi className="w-4 h-4" />}
              <span>
                {testStatus === 'testing'
                  ? 'Testing...'
                  : testStatus === 'success'
                    ? 'Connected!'
                    : testStatus === 'error'
                      ? 'Failed'
                      : 'Test Connection'}
              </span>
            </button>
            {testMessage && (
              <span className={`text-sm ${testStatus === 'success' ? 'text-green-400' : 'text-red-400'}`}>
                {testMessage}
              </span>
            )}
          </div>
        </div>
      )}

      {/* Serial Port Configuration */}
      {rotator.connectionType === 'serial' && (
        <div className={`space-y-4 ${!rotator.enabled ? 'opacity-50 pointer-events-none' : ''}`}>
          <div className="grid grid-cols-2 gap-4">
            {/* Serial Port */}
            <div className="space-y-2">
              <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
                <Server className="w-4 h-4 text-accent-primary" />
                Serial Port
              </label>
              <input
                type="text"
                value={rotator.serialPort}
                onChange={(e) => updateRotatorSettings({ serialPort: e.target.value })}
                placeholder="COM3 or /dev/ttyUSB0"
                className="glass-input w-full font-mono"
                disabled={!rotator.enabled}
              />
              <p className="text-xs text-dark-300">
                Windows: COM1, COM3, etc. | Linux/macOS: /dev/ttyUSB0, /dev/ttyS0
              </p>
            </div>

            {/* Baud Rate */}
            <div className="space-y-2">
              <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
                Baud Rate
              </label>
              <select
                value={rotator.baudRate}
                onChange={(e) => updateRotatorSettings({ baudRate: parseInt(e.target.value) })}
                className="glass-input w-full font-mono"
                disabled={!rotator.enabled}
              >
                <option value={4800}>4800</option>
                <option value={9600}>9600</option>
                <option value={19200}>19200</option>
                <option value={38400}>38400</option>
                <option value={57600}>57600</option>
                <option value={115200}>115200</option>
              </select>
              <p className="text-xs text-dark-300">
                Serial communication speed (check your rotator manual)
              </p>
            </div>
          </div>

          <div className="bg-blue-500/10 border border-blue-500/30 rounded p-3">
            <p className="text-xs text-blue-400">
              <strong>Note:</strong> Serial mode will spawn a rotctld process automatically using the selected model and serial settings.
              Ensure your rotator is connected and powered on before enabling.
            </p>
          </div>
        </div>
      )}

      {/* Polling Interval & Rotator ID */}
      <div className={`space-y-4 ${!rotator.enabled ? 'opacity-50 pointer-events-none' : ''}`}>
        <div className="grid grid-cols-2 gap-4">
          {/* Polling Interval */}
          <div className="space-y-2">
            <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
              Polling Interval (ms)
            </label>
            <input
              type="number"
              value={rotator.pollingIntervalMs}
              onChange={(e) => updateRotatorSettings({ pollingIntervalMs: parseInt(e.target.value) || 500 })}
              min={100}
              max={5000}
              step={100}
              placeholder="500"
              className="glass-input w-full font-mono"
              disabled={!rotator.enabled}
            />
            <p className="text-xs text-dark-300">
              How often to poll for position updates (100-5000ms)
            </p>
          </div>

          {/* Rotator ID */}
          <div className="space-y-2">
            <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
              Rotator ID
            </label>
            <input
              type="text"
              value={rotator.rotatorId}
              onChange={(e) => updateRotatorSettings({ rotatorId: e.target.value })}
              placeholder="default"
              className="glass-input w-full font-mono"
              disabled={!rotator.enabled}
            />
            <p className="text-xs text-dark-300">
              Identifier for this rotator (useful with multiple rotators)
            </p>
          </div>
        </div>
      </div>

      {/* Help text */}
      <div className="pt-4 border-t border-glass-100">
        <p className="text-xs text-dark-300">
          {rotator.connectionType === 'network'
            ? 'Network mode connects to an existing rotctld daemon via TCP. Make sure rotctld is running and accessible at the configured address. Default port for rotctld is 4533.'
            : 'Serial mode will automatically start rotctld with your configured serial port and rotator model. Ensure your rotator is connected before enabling.'}
        </p>
      </div>
    </div>
    </>
  );
}


// RBN Band-Opening Alerts Settings Section
function RbnAlertsSettingsSection() {
  const { settings, updateRbnAlertSettings } = useSettingsStore();
  const rbn = settings.rbnAlerts;
  const bands: { key: 'band10m' | 'band6m' | 'band2m' | 'band70cm'; label: string }[] = [
    { key: 'band10m', label: '10m' },
    { key: 'band6m', label: '6m' },
    { key: 'band2m', label: '2m' },
    { key: 'band70cm', label: '70cm' },
  ];

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">RBN Band-Opening Alerts</h3>
        <p className="text-sm text-dark-300">
          Connects to the Reverse Beacon Network and alerts when VHF/UHF bands open near your
          location. RBN skimmers have known grid squares — when one close to you hears a signal
          on a watched band, the band is open at your QTH. Requires your grid square (Station
          settings) and QRZ credentials for skimmer location lookups.
        </p>
      </div>

      <div className="flex items-center justify-between p-3 bg-dark-700 rounded-lg">
        <div>
          <label className="text-sm font-medium text-dark-200">Enable Band-Opening Alerts</label>
          <p className="text-xs text-dark-400 mt-0.5">Maintain an RBN connection and watch the selected bands</p>
        </div>
        <button
          onClick={() => updateRbnAlertSettings({ enabled: !rbn.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${rbn.enabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
        >
          <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${rbn.enabled ? 'translate-x-5' : ''}`} />
        </button>
      </div>

      <div className={`space-y-4 ${!rbn.enabled ? 'opacity-50' : ''}`}>
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">RBN Server</label>
            <input
              type="text"
              value={rbn.server}
              onChange={(e) => updateRbnAlertSettings({ server: e.target.value })}
              placeholder="telnet.reversebeacon.net"
              className="glass-input w-full font-mono"
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">Port</label>
            <input
              type="number"
              value={rbn.port}
              onChange={(e) => updateRbnAlertSettings({ port: parseInt(e.target.value) || 7000 })}
              className="glass-input w-full font-mono"
            />
          </div>
        </div>

        <div className="space-y-2">
          <label className="text-sm font-medium font-ui text-dark-200">Watched Bands</label>
          <div className="flex items-center gap-4">
            {bands.map(({ key, label }) => (
              <label key={key} className="flex items-center gap-2 text-sm text-dark-200 cursor-pointer">
                <input
                  type="checkbox"
                  checked={rbn[key]}
                  onChange={(e) => updateRbnAlertSettings({ [key]: e.target.checked })}
                  className="w-4 h-4 accent-[rgb(var(--accent-primary))]"
                />
                {label}
              </label>
            ))}
          </div>
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">Alert Distance</label>
            <div className="flex items-center gap-2">
              <input
                type="number"
                value={rbn.distance}
                min={1}
                max={5000}
                onChange={(e) => updateRbnAlertSettings({ distance: parseFloat(e.target.value) || 500 })}
                className="glass-input flex-1 font-mono"
              />
              <select
                value={rbn.distanceUnit}
                onChange={(e) => updateRbnAlertSettings({ distanceUnit: e.target.value as 'mi' | 'km' })}
                className="glass-input w-20"
              >
                <option value="mi">mi</option>
                <option value="km">km</option>
              </select>
            </div>
            <p className="text-xs text-dark-300">Alert when a skimmer within this distance hears a signal</p>
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">Cooldown (minutes)</label>
            <input
              type="number"
              value={rbn.cooldownMinutes}
              min={1}
              max={120}
              onChange={(e) => updateRbnAlertSettings({ cooldownMinutes: parseInt(e.target.value) || 15 })}
              className="glass-input w-full font-mono"
            />
            <p className="text-xs text-dark-300">Minimum time between alerts for the same band</p>
          </div>
        </div>

        <div className="flex items-center justify-between p-3 bg-dark-700/50 rounded-lg border border-glass-100">
          <div>
            <label className="text-sm font-medium text-dark-200">Voice announce band openings</label>
            <p className="text-xs text-dark-400 mt-0.5">"Band opening! 6 meters…" via speech synthesis</p>
          </div>
          <button
            onClick={() => updateRbnAlertSettings({ voice: !rbn.voice })}
            className={`relative w-11 h-6 rounded-full transition-colors ${rbn.voice ? 'bg-accent-primary' : 'bg-dark-500'}`}
          >
            <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${rbn.voice ? 'translate-x-5' : ''}`} />
          </button>
        </div>
      </div>
    </div>
  );
}

// ADIF File Monitor Settings Section
function AdifMonitorSettingsSection() {
  const { settings, updateAdifMonitorSettings, updateAdifUdpSettings } = useSettingsStore();
  const monitor = settings.adifMonitor;
  const udp = settings.adifUdp;

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">ADIF File Monitor</h3>
        <p className="text-sm text-dark-300">
          Watch ADIF log files written by other programs (VarAC, MSHV, …) and automatically import
          newly appended QSOs. Only new bytes are read, duplicates are skipped, and each import
          shows a notification.
        </p>
      </div>

      <div className="flex items-center justify-between p-3 bg-dark-700 rounded-lg">
        <div>
          <label className="text-sm font-medium text-dark-200">Enable ADIF Monitor</label>
          <p className="text-xs text-dark-400 mt-0.5">Check the watched files every few seconds</p>
        </div>
        <button
          onClick={() => updateAdifMonitorSettings({ enabled: !monitor.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${monitor.enabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
        >
          <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${monitor.enabled ? 'translate-x-5' : ''}`} />
        </button>
      </div>

      <div className={`space-y-4 ${!monitor.enabled ? 'opacity-50' : ''}`}>
        <div className="space-y-2">
          <label className="text-sm font-medium font-ui text-dark-200">Watched File 1</label>
          <input
            type="text"
            value={monitor.file1}
            onChange={(e) => updateAdifMonitorSettings({ file1: e.target.value })}
            placeholder="C:\\VarAC\\VarAC_qsos.adi"
            className="glass-input w-full font-mono"
          />
        </div>
        <div className="space-y-2">
          <label className="text-sm font-medium font-ui text-dark-200">Watched File 2</label>
          <input
            type="text"
            value={monitor.file2}
            onChange={(e) => updateAdifMonitorSettings({ file2: e.target.value })}
            placeholder="C:\\MSHV\\log.adi (optional)"
            className="glass-input w-full font-mono"
          />
        </div>
        <p className="text-xs text-dark-300">
          Full paths to .adi files. QSOs already in the file when monitoring starts are not imported —
          only what other programs append afterwards.
        </p>
      </div>

      {/* ADIF-over-UDP listener (v1 SDRLogger+ port-52001 auto-import).
          Sits alongside the file monitor because it does the same thing
          via a different transport — no reason to separate them into
          their own tab. */}
      <div className="pt-6 border-t border-glass-100">
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">ADIF over UDP</h3>
        <p className="text-sm text-dark-300">
          Listen for ADIF QSO records broadcast over UDP by loggers like VarAC, DXKeeper, N1MM, or Logger32.
          Each datagram is parsed as a single QSO and imported through the same pipeline as file-based import,
          so dedup and DXCC back-fill apply automatically.
        </p>
      </div>

      <div className="flex items-center justify-between p-3 bg-dark-700 rounded-lg">
        <div>
          <label className="text-sm font-medium text-dark-200">Enable UDP Listener</label>
          <p className="text-xs text-dark-400 mt-0.5">Bind the socket and accept ADIF datagrams</p>
        </div>
        <button
          onClick={() => updateAdifUdpSettings({ enabled: !udp.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${udp.enabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
        >
          <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${udp.enabled ? 'translate-x-5' : ''}`} />
        </button>
      </div>

      <div className={`space-y-4 ${!udp.enabled ? 'opacity-50' : ''}`}>
        <div className="space-y-2 max-w-xs">
          <label className="text-sm font-medium font-ui text-dark-200">UDP Port</label>
          <input
            type="number"
            min={1024}
            max={65535}
            value={udp.port}
            onChange={(e) => {
              const v = parseInt(e.target.value, 10);
              if (!Number.isNaN(v)) updateAdifUdpSettings({ port: v });
            }}
            className="glass-input w-full font-mono"
          />
          <p className="text-xs text-dark-300">
            v1 default was 52001. Point your other logger's ADIF-broadcast setting at this port; multiple
            loggers can share it.
          </p>
        </div>
      </div>
    </div>
  );
}

// Club Log Settings Section
function ClubLogSettingsSection() {
  const { settings, updateClubLogSettings } = useSettingsStore();
  const clubLog = settings.clubLog;
  const [testStatus, setTestStatus] = useState<'idle' | 'testing' | 'ok' | 'fail'>('idle');
  const [testMessage, setTestMessage] = useState('');

  const handleTest = async () => {
    setTestStatus('testing');
    setTestMessage('');
    try {
      const response = await fetch('/api/clublog/test', { method: 'POST', signal: AbortSignal.timeout(15000) });
      const result = await response.json();
      setTestStatus(result.success ? 'ok' : 'fail');
      setTestMessage(result.message ?? '');
    } catch (err) {
      setTestStatus('fail');
      setTestMessage(`Test failed: ${err instanceof Error ? err.message : err}`);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">Club Log</h3>
        <p className="text-sm text-dark-300">
          Upload each QSO to Club Log in real time as you log it. Save your settings, then use
          Test Credentials — a successful test also re-enables uploads after an authentication failure.
        </p>
      </div>

      <div className="flex items-center justify-between p-3 bg-dark-700 rounded-lg">
        <div>
          <label className="text-sm font-medium text-dark-200">Enable Club Log Upload</label>
          <p className="text-xs text-dark-400 mt-0.5">Send every new QSO to Club Log automatically</p>
        </div>
        <button
          onClick={() => updateClubLogSettings({ enabled: !clubLog.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${clubLog.enabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
        >
          <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${clubLog.enabled ? 'translate-x-5' : ''}`} />
        </button>
      </div>

      <div className={`space-y-4 ${!clubLog.enabled ? 'opacity-50' : ''}`}>
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">Email</label>
            <input
              type="email"
              value={clubLog.email}
              onChange={(e) => updateClubLogSettings({ email: e.target.value })}
              placeholder="you@example.com"
              className="glass-input w-full"
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">Password</label>
            <input
              type="password"
              value={clubLog.password}
              onChange={(e) => updateClubLogSettings({ password: e.target.value })}
              placeholder="Club Log password"
              className="glass-input w-full"
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">Callsign</label>
            <input
              type="text"
              value={clubLog.callsign}
              onChange={(e) => updateClubLogSettings({ callsign: e.target.value.toUpperCase() })}
              placeholder={settings.station.callsign || 'Station callsign'}
              className="glass-input w-full font-mono"
            />
            <p className="text-xs text-dark-300">Leave empty to use your station callsign</p>
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">Application Key</label>
            <input
              type="password"
              value={clubLog.apiKey}
              onChange={(e) => updateClubLogSettings({ apiKey: e.target.value })}
              placeholder="Club Log application key"
              className="glass-input w-full font-mono"
            />
            <p className="text-xs text-dark-300">
              Issued by Club Log on request — see clublog.freshdesk.com (article 54906)
            </p>
          </div>
        </div>

        <div className="pt-2 flex items-center gap-4">
          <button
            onClick={handleTest}
            disabled={testStatus === 'testing'}
            className="glass-button px-4 py-2 flex items-center gap-2 disabled:opacity-50"
          >
            {testStatus === 'testing' ? <Loader2 className="w-4 h-4 animate-spin" /> : <CheckCircle className="w-4 h-4" />}
            Test Credentials
          </button>
          {testMessage && (
            <p className={`text-sm ${testStatus === 'ok' ? 'text-accent-success' : 'text-accent-danger'}`}>{testMessage}</p>
          )}
        </div>

        <div className="p-3 bg-accent-warning/10 border border-accent-warning/30 rounded-lg">
          <p className="text-xs text-dark-300">
            After an authentication failure (HTTP 403), uploads stop automatically to protect you from
            Club Log's IP firewall. Fix your credentials and run Test Credentials to re-enable.
          </p>
        </div>
      </div>
    </div>
  );
}

// HRDLog.net Settings Section
function HrdLogSettingsSection() {
  const { settings, updateHrdLogSettings } = useSettingsStore();
  const hrdLog = settings.hrdLog;

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">HRDLog.net</h3>
        <p className="text-sm text-dark-300">
          Upload each QSO to HRDLog.net in real time as you log it. Enter the upload code from your
          hrdlog.net account (My Account → Online Log), then save your settings.
        </p>
      </div>

      <div className="flex items-center justify-between p-3 bg-dark-700 rounded-lg">
        <div>
          <label className="text-sm font-medium text-dark-200">Enable HRDLog Upload</label>
          <p className="text-xs text-dark-400 mt-0.5">Send every new QSO to HRDLog.net automatically</p>
        </div>
        <button
          onClick={() => updateHrdLogSettings({ enabled: !hrdLog.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${hrdLog.enabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
        >
          <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${hrdLog.enabled ? 'translate-x-5' : ''}`} />
        </button>
      </div>

      <div className={`space-y-4 ${!hrdLog.enabled ? 'opacity-50' : ''}`}>
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">Callsign</label>
            <input
              type="text"
              value={hrdLog.callsign}
              onChange={(e) => updateHrdLogSettings({ callsign: e.target.value.toUpperCase() })}
              placeholder={settings.station.callsign || 'Station callsign'}
              className="glass-input w-full font-mono"
            />
            <p className="text-xs text-dark-300">Leave empty to use your station callsign</p>
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">Upload Code</label>
            <input
              type="password"
              value={hrdLog.uploadCode}
              onChange={(e) => updateHrdLogSettings({ uploadCode: e.target.value })}
              placeholder="HRDLog upload code"
              className="glass-input w-full font-mono"
            />
            <p className="text-xs text-dark-300">
              Found on hrdlog.net under My Account → Online Log
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

// eQSL.cc Settings Section — mirrors the ClubLog / HRDLog pattern.
// Real-time per-QSO ADIF upload via ImportADIF.cfm; the "Test
// Credentials" button posts an empty ADIF so eQSL's auth check runs
// without submitting a QSO.
function EqslSettingsSection() {
  const { settings, updateEqslSettings } = useSettingsStore();
  const eqsl = settings.eqsl;
  const [testStatus, setTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [testMessage, setTestMessage] = useState('');

  const handleTest = async () => {
    setTestStatus('testing');
    setTestMessage('');
    try {
      const resp = await fetch('/api/eqsl/test', { method: 'POST' });
      const data = await resp.json();
      if (data.success) {
        setTestStatus('success');
        setTestMessage(data.message || 'Connected');
        if (!eqsl.enabled) updateEqslSettings({ enabled: true });
      } else {
        setTestStatus('error');
        setTestMessage(data.message || 'Test failed');
      }
    } catch {
      setTestStatus('error');
      setTestMessage('Failed to reach server');
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">eQSL.cc</h3>
        <p className="text-sm text-dark-300">
          Upload each QSO to eQSL.cc in real time as you log it. Enter your eQSL username
          (typically your callsign) and password. Save your settings before running Test.
        </p>
      </div>

      <div className="flex items-center justify-between p-3 bg-dark-700 rounded-lg">
        <div>
          <label className="text-sm font-medium text-dark-200">Enable eQSL Upload</label>
          <p className="text-xs text-dark-400 mt-0.5">Send every new QSO to eQSL.cc automatically</p>
        </div>
        <button
          onClick={() => updateEqslSettings({ enabled: !eqsl.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${eqsl.enabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
        >
          <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${eqsl.enabled ? 'translate-x-5' : ''}`} />
        </button>
      </div>

      <div className={`space-y-4 ${!eqsl.enabled ? 'opacity-50' : ''}`}>
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">eQSL Username</label>
            <input
              type="text"
              value={eqsl.username}
              onChange={(e) => updateEqslSettings({ username: e.target.value.toUpperCase() })}
              placeholder={settings.station.callsign || 'YOUR-CALL'}
              className="glass-input w-full font-mono"
            />
            <p className="text-xs text-dark-300">Usually your callsign — the account username on eQSL.cc.</p>
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">eQSL Password</label>
            <input
              type="password"
              value={eqsl.password}
              onChange={(e) => updateEqslSettings({ password: e.target.value })}
              placeholder="eQSL account password"
              className="glass-input w-full font-mono"
            />
            <p className="text-xs text-dark-300">Stored in the local user config only.</p>
          </div>
        </div>
        <div className="space-y-2">
          <label className="text-sm font-medium font-ui text-dark-200">QTH Nickname (optional)</label>
          <input
            type="text"
            value={eqsl.qthNickname}
            onChange={(e) => updateEqslSettings({ qthNickname: e.target.value })}
            placeholder="Home / Portable / …"
            className="glass-input w-full font-mono max-w-xs"
          />
          <p className="text-xs text-dark-300">
            Only needed if your eQSL account has multiple QTHs configured.
          </p>
        </div>

        <div className="pt-2">
          <button
            onClick={handleTest}
            disabled={testStatus === 'testing' || !eqsl.username || !eqsl.password}
            className="glass-button-primary px-4 py-2 disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {testStatus === 'testing' ? 'Testing…' : 'Test Credentials'}
          </button>
          {testStatus === 'success' && (
            <span className="ml-3 text-sm text-accent-success">✓ {testMessage}</span>
          )}
          {testStatus === 'error' && (
            <span className="ml-3 text-sm text-red-400">✗ {testMessage}</span>
          )}
        </div>
      </div>
    </div>
  );
}

// POTA (Parks on the Air) Settings Section — credentials for self-spotting
// the operator's activation. POTA's /spot endpoint uses HTTP basic auth;
// the "Spot Myself" button in the LogEntry POTA banner is disabled until
// both fields are populated.
function PotaSettingsSection() {
  const { settings, updatePotaSettings } = useSettingsStore();
  const pota = settings.pota;
  const [showPassword, setShowPassword] = useState(false);

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">POTA.app</h3>
        <p className="text-sm text-dark-300">
          Credentials for self-spotting an activation on <a href="https://pota.app" target="_blank" rel="noreferrer" className="text-accent-primary hover:underline">pota.app</a>.
          The "Spot Myself" button in the Log Entry POTA banner uses these to POST to <code className="text-xs font-mono">api.pota.app/spot</code>.
          The rest of the POTA panel (activator feed, park lookups) works without credentials — only self-spotting needs them.
        </p>
      </div>

      <div className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">POTA Username</label>
            <input
              type="text"
              value={pota.username}
              onChange={(e) => updatePotaSettings({ username: e.target.value })}
              placeholder="Your pota.app username"
              className="glass-input w-full font-mono"
            />
            <p className="text-xs text-dark-300">Your pota.app account username (often your callsign).</p>
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium font-ui text-dark-200">POTA Password</label>
            <div className="relative">
              <input
                type={showPassword ? 'text' : 'password'}
                value={pota.password}
                onChange={(e) => updatePotaSettings({ password: e.target.value })}
                placeholder="Your pota.app password"
                className="glass-input w-full font-mono pr-10"
              />
              <button
                type="button"
                onClick={() => setShowPassword((s) => !s)}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-dark-400 hover:text-dark-200"
              >
                {showPassword ? '🙈' : '👁'}
              </button>
            </div>
            <p className="text-xs text-dark-300">Stored in the local user config only.</p>
          </div>
        </div>
      </div>
    </div>
  );
}

// Appearance Settings Section
function AppearanceSettingsSection() {
  const { settings, updateAppearanceSettings } = useSettingsStore();
  const appearance = settings.appearance;

  const themeOptions: {
    id: ThemeId;
    label: string;
    icon: React.ReactNode;
    description: string;
    preview: { bg: string; panel: string; accent: string; text: string };
  }[] = [
    {
      id: 'dark',
      label: 'Dark',
      icon: <Moon className="w-5 h-5" />,
      description: 'Deep, focused, professional',
      preview: { bg: '#1B1B1F', panel: '#27272B', accent: '#E08A3D', text: '#A1A1A8' },
    },
    {
      id: 'light',
      label: 'Light',
      icon: <Sun className="w-5 h-5" />,
      description: 'High-contrast charcoal & cream',
      preview: { bg: '#423f38', panel: '#e1ddd6', accent: '#d76c26', text: '#bcb9b2' },
    },
    {
      id: 'nightops',
      label: 'Night Ops',
      icon: <Radar className="w-5 h-5" />,
      description: 'Cyan console on deep navy',
      preview: { bg: '#0a0d12', panel: '#111620', accent: '#00e5ff', text: '#cdd9e5' },
    },
    {
      id: 'lyra',
      label: 'Lyra',
      icon: <Waves className="w-5 h-5" />,
      description: 'Styled after the Lyra SDR — glassy cyan & amber',
      preview: { bg: '#06090e', panel: '#0f1720', accent: '#00e5ff', text: '#cdd9e5' },
    },
    {
      id: 'midnight',
      label: 'Midnight',
      icon: <Moon className="w-5 h-5" />,
      description: 'Steel blue on dark slate',
      preview: { bg: '#0d1117', panel: '#161b22', accent: '#58a6ff', text: '#e6edf3' },
    },
    {
      id: 'dracula',
      label: 'Dracula',
      icon: <Sparkles className="w-5 h-5" />,
      description: 'Purple & pink classic',
      preview: { bg: '#21222c', panel: '#282a36', accent: '#bd93f9', text: '#f8f8f2' },
    },
    {
      id: 'nord',
      label: 'Nord',
      icon: <Snowflake className="w-5 h-5" />,
      description: 'Frosty blue, easy on the eyes',
      preview: { bg: '#2e3440', panel: '#3b4252', accent: '#88c0d0', text: '#eceff4' },
    },
    {
      id: 'blurple',
      label: 'Blurple',
      icon: <MessageSquare className="w-5 h-5" />,
      description: 'Indigo on neutral gray',
      preview: { bg: '#1e1f22', panel: '#2b2d31', accent: '#5865f2', text: '#f2f3f5' },
    },
    {
      id: 'custom',
      label: 'Custom',
      icon: <Palette className="w-5 h-5" />,
      description: 'Pick your own colors',
      preview: {
        bg: appearance.customColors.background,
        panel: appearance.customColors.panel,
        accent: appearance.customColors.accent,
        text: appearance.customColors.text,
      },
    },
  ];

  const selectTheme = (id: ThemeId) => {
    if (id === 'custom' && appearance.theme !== 'custom') {
      // Seed the pickers from the theme the user is leaving
      updateAppearanceSettings({ theme: 'custom', customColors: getSeedColors(appearance.theme) });
    } else {
      updateAppearanceSettings({ theme: id });
    }
  };

  const customColorFields: { key: keyof CustomColors; label: string; hint: string }[] = [
    { key: 'accent', label: 'Accent', hint: 'Highlights, buttons, active elements' },
    { key: 'background', label: 'Background', hint: 'App background (deepest layer)' },
    { key: 'panel', label: 'Panels', hint: 'Cards, panels, raised surfaces' },
    { key: 'text', label: 'Text', hint: 'Primary text color' },
  ];

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">Appearance</h3>
        <p className="text-sm text-dark-300">Customize the look and feel of the application.</p>
      </div>

      {/* Theme selection */}
      <div className="space-y-3">
        <label className="text-sm font-medium font-ui text-dark-200">Theme</label>
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          {themeOptions.map((opt) => (
            <button
              key={opt.id}
              onClick={() => selectTheme(opt.id)}
              className={`relative flex flex-col items-center gap-3 p-4 rounded-lg border transition-all ${
                appearance.theme === opt.id
                  ? 'border-accent-primary bg-accent-primary/10 ring-1 ring-accent-primary/30'
                  : 'border-glass-100 hover:border-glass-200'
              }`}
            >
              {/* Mini preview */}
              {opt.preview ? (
                <div
                  className="w-full h-14 rounded-md border border-dark-600/50 overflow-hidden flex items-end p-1.5 gap-1"
                  style={{ background: opt.preview.bg }}
                >
                  <div
                    className="flex-1 h-8 rounded"
                    style={{ background: opt.preview.panel, border: `1px solid ${opt.preview.accent}22` }}
                  />
                  <div
                    className="w-6 h-8 rounded"
                    style={{ background: opt.preview.panel, border: `1px solid ${opt.preview.accent}22` }}
                  />
                </div>
              ) : (
                <div className="w-full h-14 rounded-md border border-dark-600/50 overflow-hidden flex">
                  <div className="w-1/2 h-full bg-[#1B1B1F]" />
                  <div className="w-1/2 h-full bg-[#f6f7f9]" />
                </div>
              )}
              <div className="flex items-center gap-2 text-sm font-medium">
                <span className={appearance.theme === opt.id ? 'text-accent-primary' : 'text-dark-300'}>
                  {opt.icon}
                </span>
                <span>{opt.label}</span>
              </div>
              <p className="text-xs text-dark-300">{opt.description}</p>
            </button>
          ))}
        </div>
      </div>

      {/* Custom color pickers */}
      {appearance.theme === 'custom' && (
        <div className="space-y-3 p-4 bg-dark-700/50 rounded-lg border border-glass-100">
          <label className="text-sm font-medium font-ui text-dark-200">Custom Colors</label>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {customColorFields.map(({ key, label, hint }) => (
              <div key={key} className="flex items-center gap-3">
                <input
                  type="color"
                  value={appearance.customColors[key]}
                  onChange={(e) =>
                    updateAppearanceSettings({
                      customColors: { ...appearance.customColors, [key]: e.target.value },
                    })
                  }
                  className="w-10 h-10 rounded cursor-pointer border border-glass-200 bg-transparent"
                  title={label}
                />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-medium text-dark-200">{label}</span>
                    <span className="text-xs font-mono text-dark-300">{appearance.customColors[key]}</span>
                  </div>
                  <p className="text-xs text-dark-300 truncate">{hint}</p>
                </div>
              </div>
            ))}
          </div>
          <p className="text-xs text-dark-300">
            Colors apply live. Switch to another theme at any time — your custom colors are kept.
          </p>
        </div>
      )}

      {/* Compact mode toggle */}
      <div className="flex items-center justify-between p-4 bg-dark-700/50 rounded-lg border border-glass-100">
        <div>
          <p className="font-medium font-ui text-dark-200">Compact Mode</p>
          <p className="text-sm text-dark-300">Use smaller spacing and fonts</p>
        </div>
        <button
          onClick={() => updateAppearanceSettings({ compactMode: !appearance.compactMode })}
          className={`relative w-11 h-6 rounded-full transition-colors ${
            appearance.compactMode ? 'bg-accent-success' : 'bg-dark-600 border border-dark-400'
          }`}
        >
          <span
            className={`absolute top-0.5 left-0.5 w-5 h-5 rounded-full bg-white shadow transition-transform duration-200 ${
              appearance.compactMode ? 'translate-x-5' : 'translate-x-0'
            }`}
          />
        </button>
      </div>

      {/* Units — master imperial/metric preference driving every physical readout.
          Keeps distanceUnit in sync so distance consumers read it directly. */}
      <div className="flex items-center justify-between p-4 bg-dark-700/50 rounded-lg border border-glass-100">
        <div>
          <p className="font-medium font-ui text-dark-200">Units</p>
          <p className="text-sm text-dark-300">
            Imperial or metric app-wide — distance, satellite, wind, temperature
          </p>
        </div>
        <div className="flex rounded-lg overflow-hidden border border-glass-100">
          {(['metric', 'imperial'] as const).map((sys) => (
            <button
              key={sys}
              onClick={() => updateAppearanceSettings({ unitSystem: sys, distanceUnit: distanceUnitFor(sys) })}
              className={`px-3 py-1.5 text-sm font-ui transition-colors ${
                (appearance.unitSystem ?? 'metric') === sys
                  ? 'bg-accent-success text-dark-900 font-semibold'
                  : 'bg-dark-800 text-dark-300 hover:text-dark-200'
              }`}
            >
              {sys === 'metric' ? 'Metric · km °C' : 'Imperial · mi °F'}
            </button>
          ))}
        </div>
      </div>

      <LayoutPresetsSubsection />
    </div>
  );
}

// Layout Presets subsection — sits inside AppearanceSettingsSection so
// operators can save/load/delete up to 3 named panel arrangements from
// the same place they change themes and appearance settings. Uses the
// layoutStore's setLayout to persist swaps (which flows through the
// normal hasEverLoaded-gated auto-save path).
function LayoutPresetsSubsection() {
  const { layout, setLayout } = useLayoutStore();
  const [savedLayouts, setSavedLayouts] = useState<SavedLayoutSlot[]>([]);
  const [message, setMessage] = useState<{ kind: 'ok' | 'err'; text: string } | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    (async () => {
      try {
        const list = await api.getSavedLayouts();
        setSavedLayouts(list);
      } catch (e) {
        console.error('Failed to load saved layouts:', e);
      }
    })();
  }, []);

  const flash = (kind: 'ok' | 'err', text: string) => {
    setMessage({ kind, text });
    setTimeout(() => setMessage(null), 4000);
  };

  const handleSaveCurrent = async () => {
    const suggested = savedLayouts.length === 0 ? 'Default' : `Layout ${savedLayouts.length + 1}`;
    const name = window.prompt(
      savedLayouts.length >= 3
        ? 'You have 3 saved layouts (the max). Enter one of the existing names to overwrite it:'
        : 'Name for this layout:',
      suggested,
    );
    if (!name || !name.trim()) return;
    setLoading(true);
    try {
      const json = JSON.stringify(layout);
      const list = await api.saveNamedLayout(name.trim(), json);
      setSavedLayouts(list);
      flash('ok', `Saved as "${name.trim()}"`);
    } catch (e) {
      flash('err', e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleLoad = (slot: SavedLayoutSlot) => {
    try {
      const json = JSON.parse(slot.layoutJson);
      // Sanity: FlexLayout throws if the JSON isn't a valid model.
      Model.fromJson(json);
      setLayout(json);
      flash('ok', `Loaded "${slot.name}"`);
    } catch (e) {
      flash('err', `Failed to apply "${slot.name}": ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const handleDelete = async (name: string) => {
    if (!window.confirm(`Delete saved layout "${name}"?`)) return;
    setLoading(true);
    try {
      const list = await api.deleteNamedLayout(name);
      setSavedLayouts(list);
      flash('ok', `Deleted "${name}"`);
    } catch (e) {
      flash('err', e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="pt-4 mt-4 border-t border-glass-100">
      <div className="flex items-center justify-between mb-2 gap-3">
        <div className="flex-1 min-w-0">
          <h4 className="text-sm font-semibold font-ui text-dark-200">Layout Presets</h4>
          <p className="text-xs text-dark-300 mt-0.5">
            Save up to 3 named panel arrangements (POTA, Contest, DXpedition, etc.) and swap between them with one click. The one you loaded last also comes back automatically on the next restart.
          </p>
        </div>
        <button
          onClick={handleSaveCurrent}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-ui border border-accent-success/40 text-accent-success hover:bg-accent-success/10 transition-colors disabled:opacity-50 whitespace-nowrap"
          title="Save the current panel arrangement as a named preset"
        >
          <Save className="w-3.5 h-3.5" /> Save Current
        </button>
      </div>

      {savedLayouts.length === 0 ? (
        <p className="text-xs text-dark-400 font-ui italic mt-3">
          No saved layouts yet. Arrange your panels how you like them and click <b>Save Current</b>.
        </p>
      ) : (
        <div className="space-y-1.5 mt-3">
          {savedLayouts.map((slot) => (
            <div
              key={slot.name}
              className="flex items-center justify-between gap-2 px-3 py-2 rounded bg-dark-700/50 border border-glass-100 hover:bg-dark-700 transition-colors"
            >
              <div className="flex-1 min-w-0">
                <div className="text-sm font-ui text-dark-200 truncate">{slot.name}</div>
                <div className="text-[10px] text-dark-400 font-mono">
                  Saved {new Date(slot.savedAt).toLocaleString()}
                </div>
              </div>
              <button
                onClick={() => handleLoad(slot)}
                disabled={loading}
                title="Load this layout"
                className="p-1.5 rounded text-accent-primary hover:bg-accent-primary/10 transition-colors disabled:opacity-50"
              >
                <FolderOpen className="w-4 h-4" />
              </button>
              <button
                onClick={() => handleDelete(slot.name)}
                disabled={loading}
                title="Delete this layout"
                className="p-1.5 rounded text-dark-400 hover:text-accent-danger hover:bg-accent-danger/10 transition-colors disabled:opacity-50"
              >
                <Trash2 className="w-4 h-4" />
              </button>
            </div>
          ))}
        </div>
      )}
      {message && (
        <p className={`mt-2 text-xs font-ui ${
          message.kind === 'ok' ? 'text-accent-success' : 'text-accent-danger'
        }`}>
          {message.text}
        </p>
      )}
    </div>
  );
}

// Map Settings Section
function MapSettingsSection() {
  const { settings, updateMapSettings } = useSettingsStore();
  const map = settings.map;
  const [availableSatellites] = useState([
    'ISS', 'AO-91', 'AO-92', 'SO-50', 'PO-101', 'RS-44', 'IO-117',
    'TEVEL-1', 'TEVEL-2', 'TEVEL-3', 'TEVEL-4', 'TEVEL-5', 'TEVEL-6', 'TEVEL-7', 'TEVEL-8'
  ]);

  const toggleSatellite = (satellite: string) => {
    const selected = map.selectedSatellites || [];
    const newSelected = selected.includes(satellite)
      ? selected.filter(s => s !== satellite)
      : [...selected, satellite];
    updateMapSettings({ selectedSatellites: newSelected });
  };

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">Map Settings</h3>
        <p className="text-sm text-dark-300">Configure map overlays and satellite tracking options.</p>
      </div>

      <div className="space-y-4">
        {/* Show Satellites Toggle */}
        <label className="flex items-center justify-between p-4 bg-dark-700/50 rounded-lg border border-glass-100 cursor-pointer hover:bg-dark-700 transition-colors">
          <div className="flex items-center gap-3">
            <Map className="w-5 h-5 text-accent-primary" />
            <div>
              <div className="font-medium font-ui text-dark-200">Show Satellites</div>
              <div className="text-sm text-dark-300">Display satellite positions and orbital tracks on map</div>
            </div>
          </div>
          <input
            type="checkbox"
            checked={map.showSatellites}
            onChange={(e) => updateMapSettings({ showSatellites: e.target.checked })}
            className="w-5 h-5 rounded bg-dark-700 border-glass-100 text-accent-primary focus:ring-2 focus:ring-accent-primary focus:ring-offset-0 focus:ring-offset-dark-800"
          />
        </label>

        {/* Show Long Path Toggle — draws the cyan reflex-angle great-circle
            arc alongside the red-orange short-path arc when a callsign is
            focused on the 3D Globe. Info card also gains an "LP" row with
            the reciprocal bearing and distance. */}
        <label className="flex items-center justify-between p-4 bg-dark-700/50 rounded-lg border border-glass-100 cursor-pointer hover:bg-dark-700 transition-colors">
          <div className="flex items-center gap-3">
            <Map className="w-5 h-5" style={{ color: 'rgba(163, 230, 53, 0.95)' }} />
            <div>
              <div className="font-medium font-ui text-dark-200">Show Long Path</div>
              <div className="text-sm text-dark-300">
                Draw the reflex-angle great-circle arc (lime) alongside the short path (red) on the 3D Globe. Useful for gray-line contacts.
              </div>
            </div>
          </div>
          <input
            type="checkbox"
            checked={map.showLongPath !== false}
            onChange={(e) => updateMapSettings({ showLongPath: e.target.checked })}
            className="w-5 h-5 rounded bg-dark-700 border-glass-100 text-accent-primary focus:ring-2 focus:ring-accent-primary focus:ring-offset-0 focus:ring-offset-dark-800"
          />
        </label>

        {/* Ionospheric Hops — short path bounces off the ionosphere and the
            globe tilts to an oblique angle so the hops are visible. */}
        <label className="flex items-center justify-between p-4 bg-dark-700/50 rounded-lg border border-glass-100 cursor-pointer hover:bg-dark-700 transition-colors">
          <div className="flex items-center gap-3">
            <Radio className="w-5 h-5 text-accent-danger" />
            <div>
              <div className="font-medium font-ui text-dark-200">Ionospheric Hops</div>
              <div className="text-sm text-dark-300">Show the short path skipping off the ionosphere on the 3D Globe (tilts the view when a callsign is focused)</div>
            </div>
          </div>
          <input
            type="checkbox"
            checked={map.showIonosphereHops}
            onChange={(e) => updateMapSettings({ showIonosphereHops: e.target.checked })}
            className="w-5 h-5 rounded bg-dark-700 border-glass-100 text-accent-primary focus:ring-2 focus:ring-accent-primary focus:ring-offset-0 focus:ring-offset-dark-800"
          />
        </label>

        {/* Day/Night Shading — intensity for the 3D Globe's terminator shader
            shell. On/off lives on the Globe panel's sun button; this slider
            only sets how dark the night side gets. */}
        <div className="p-4 bg-dark-700/50 rounded-lg border border-glass-100">
          <div className="flex items-center gap-3 mb-3">
            <Sun className="w-5 h-5 text-amber-300" />
            <div>
              <div className="font-medium font-ui text-dark-200">Day/Night Shading</div>
              <div className="text-sm text-dark-300">Night-side shade intensity on the 3D Globe — toggle it with the sun button on the Globe panel</div>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-sm text-dark-300 w-28">Shade opacity</span>
            <input
              type="range"
              min={0.1}
              max={1}
              step={0.05}
              value={map.dayNightOpacity}
              onChange={(e) => updateMapSettings({ dayNightOpacity: parseFloat(e.target.value) })}
              aria-label="Day/night shade opacity"
              className="flex-1 h-2 bg-dark-800 rounded-lg appearance-none cursor-pointer accent-accent-primary"
            />
            <span className="text-sm font-mono text-dark-200 w-10 text-right">{Math.round(map.dayNightOpacity * 100)}%</span>
          </div>
        </div>

        {/* DX News Ticker — scrolling DX-World.net headlines at the bottom
            of the 2D Map panel */}
        <label className="flex items-center justify-between p-4 bg-dark-700/50 rounded-lg border border-glass-100 cursor-pointer hover:bg-dark-700 transition-colors">
          <div className="flex items-center gap-3">
            <Newspaper className="w-5 h-5 text-accent-warning" />
            <div>
              <div className="font-medium font-ui text-dark-200">DX News Ticker</div>
              <div className="text-sm text-dark-300">Scroll DX-World.net news across the bottom of the 2D Map</div>
            </div>
          </div>
          <input
            type="checkbox"
            checked={map.showDxNewsTicker}
            onChange={(e) => updateMapSettings({ showDxNewsTicker: e.target.checked })}
            className="w-5 h-5 rounded bg-dark-700 border-glass-100 text-accent-primary focus:ring-2 focus:ring-accent-primary focus:ring-offset-0 focus:ring-offset-dark-800"
          />
        </label>

        {/* Signal Path — style + colour of the 2D-map line to the focused DX */}
        <div className="p-4 bg-dark-700/50 rounded-lg border border-glass-100">
          <div className="flex items-center gap-3 mb-3">
            <Activity className="w-5 h-5 text-accent-secondary" />
            <div>
              <div className="font-medium font-ui text-dark-200">Signal Path</div>
              <div className="text-sm text-dark-300">How the path to the focused DX station is drawn on the 2D Map</div>
            </div>
          </div>
          <div className="flex items-center gap-6">
            <label className="flex items-center gap-2 text-sm font-ui text-dark-200">
              <span className="text-dark-300">Style</span>
              <select
                value={map.dxPathStyle ?? 'sine'}
                onChange={(e) => updateMapSettings({ dxPathStyle: e.target.value as 'sine' | 'dash' })}
                className="bg-dark-800 border border-glass-100 rounded px-2 py-1.5 text-sm font-ui text-gray-100 focus:outline-none focus:border-accent-secondary/50"
              >
                <option value="sine">Sine wave (animated)</option>
                <option value="dash">Dashed line</option>
              </select>
            </label>
            <label className="flex items-center gap-2 text-sm font-ui text-dark-200">
              <span className="text-dark-300">Color</span>
              <input
                type="color"
                value={map.dxPathColor || '#39ff14'}
                onChange={(e) => updateMapSettings({ dxPathColor: e.target.value })}
                className="w-9 h-7 rounded border border-glass-100 bg-dark-800 cursor-pointer p-0.5"
                title="Signal path color"
              />
              <span className="font-mono text-xs text-dark-300">{(map.dxPathColor || '#39ff14').toUpperCase()}</span>
            </label>
          </div>
        </div>

        {/* Satellite Selection */}
        {map.showSatellites && (
          <div className="space-y-2">
            <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
              Selected Satellites
            </label>
            <div className="grid grid-cols-3 gap-2">
              {availableSatellites.map((satellite) => {
                const isSelected = (map.selectedSatellites || []).includes(satellite);
                return (
                  <button
                    key={satellite}
                    onClick={() => toggleSatellite(satellite)}
                    className={`px-3 py-2 rounded-lg border transition-colors text-sm font-mono ${
                      isSelected
                        ? 'bg-accent-primary/10 border-accent-primary text-accent-primary'
                        : 'bg-dark-700/50 border-glass-100 text-dark-300 hover:bg-dark-700'
                    }`}
                  >
                    {satellite}
                  </button>
                );
              })}
            </div>
            <p className="text-xs text-dark-300">
              Select which amateur radio satellites to track on the map. TLE data is fetched from Celestrak.
            </p>
          </div>
        )}

        {/* Info about satellite tracking */}
        <div className="p-4 bg-dark-700/30 rounded-lg border border-glass-100">
          <div className="flex gap-3">
            <Info className="w-5 h-5 text-accent-info flex-shrink-0 mt-0.5" />
            <div className="space-y-2 text-sm text-dark-300">
              <p>
                <strong className="text-dark-200">Satellite Tracking Features:</strong>
              </p>
              <ul className="list-disc list-inside space-y-1 ml-2">
                <li>Real-time satellite positions updated every 5 seconds</li>
                <li>Orbital track lines showing 90-minute path</li>
                <li>Footprint circles indicating coverage area</li>
                <li>Azimuth/elevation angles when satellite is visible from your location</li>
                <li>Eclipse status (satellite in Earth's shadow)</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

// Header Settings Section
function HeaderSettingsSection() {
  const { settings, updateHeaderSettings } = useSettingsStore();
  const header = settings.header;

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">Header Bar Settings</h3>
        <p className="text-sm text-dark-300">Customize the header bar display with space weather indices and time formats.</p>
      </div>

      <div className="space-y-4">
        {/* Time Format */}
        <div className="space-y-2">
          <label className="flex items-center gap-2 text-sm font-medium font-ui text-dark-200">
            Time Format (Local)
          </label>
          <div className="flex gap-2">
            <button
              onClick={() => updateHeaderSettings({ timeFormat: '12h' })}
              className={`flex-1 px-4 py-2 rounded-lg border transition-colors ${
                header.timeFormat === '12h'
                  ? 'bg-accent-primary/10 border-accent-primary text-accent-primary'
                  : 'bg-dark-700/50 border-glass-100 text-dark-300 hover:bg-dark-700'
              }`}
            >
              12-Hour
            </button>
            <button
              onClick={() => updateHeaderSettings({ timeFormat: '24h' })}
              className={`flex-1 px-4 py-2 rounded-lg border transition-colors ${
                header.timeFormat === '24h'
                  ? 'bg-accent-primary/10 border-accent-primary text-accent-primary'
                  : 'bg-dark-700/50 border-glass-100 text-dark-300 hover:bg-dark-700'
              }`}
            >
              24-Hour
            </button>
          </div>
          <p className="text-xs text-dark-300">You can also click the local time in the header to toggle formats.</p>
        </div>

        {/* Show Weather */}
        <label className="flex items-center justify-between p-4 bg-dark-700/50 rounded-lg border border-glass-100 cursor-pointer hover:bg-dark-700 transition-colors">
          <div className="flex items-center gap-3">
            <Globe className="w-5 h-5 text-accent-primary" />
            <div>
              <div className="font-medium font-ui text-dark-200">Show Weather</div>
              <div className="text-sm text-dark-300">Display current weather in header (requires station coordinates)</div>
            </div>
          </div>
          <input
            type="checkbox"
            checked={header.showWeather}
            onChange={(e) => updateHeaderSettings({ showWeather: e.target.checked })}
            className="w-5 h-5 rounded border-glass-100 bg-dark-700 text-accent-primary focus:ring-accent-primary focus:ring-offset-0"
          />
        </label>

        {/* Info Box */}
        <div className="p-4 bg-accent-primary/5 border border-accent-primary/20 rounded-lg">
          <h4 className="font-medium font-ui text-accent-primary mb-2 flex items-center gap-2">
            <Info className="w-4 h-4" />
            About the Header Bar
          </h4>
          <div className="text-sm text-dark-300 space-y-2">
            <p>
              The header bar displays essential operating information inspired by OpenHamClock:
            </p>
            <ul className="list-disc list-inside space-y-1 ml-2">
              <li><strong className="text-white">UTC Time</strong>: Essential for logging (always 24-hour format)</li>
              <li><strong className="text-accent-primary">Local Time</strong>: Your system time (clickable to toggle format)</li>
              <li><strong className="text-accent-primary">SFI</strong>: Solar Flux Index (higher is better for HF)</li>
              <li><strong className="text-accent-success">K-Index</strong>: Geomagnetic activity (turns <span className="text-accent-danger">red</span> when ≥4)</li>
              <li><strong className="text-white">SSN</strong>: Sunspot Number (indicates solar activity)</li>
            </ul>
            <p className="pt-2 text-xs">
              Space weather data refreshes every 15 minutes. Weather data requires latitude/longitude in Station settings.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}


// AI Settings Section
function AiSettingsSection() {
  const { settings, updateAiSettings } = useSettingsStore();
  const ai = settings.ai;
  const [showApiKey, setShowApiKey] = useState(false);
  const [isTesting, setIsTesting] = useState(false);
  const [testResult, setTestResult] = useState<{ success: boolean; message?: string } | null>(null);

  const handleTestApiKey = async () => {
    if (!ai.apiKey) return;
    setIsTesting(true);
    setTestResult(null);
    try {
      const response = await fetch('/api/ai/test-key', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ provider: ai.provider, apiKey: ai.apiKey, model: ai.model }),
      });
      const result = await response.json();
      setTestResult({ success: result.isValid, message: result.errorMessage || 'API key is valid!' });
    } catch (error) {
      setTestResult({ success: false, message: 'Failed to test API key' });
    } finally {
      setIsTesting(false);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold text-gray-100 mb-1">AI Provider Settings</h3>
        <p className="text-sm text-gray-500">Configure your LLM provider for AI-powered talk points.</p>
      </div>
      <div className="space-y-2">
        <label className="flex items-center gap-2 text-sm font-medium text-gray-300">
          <Bot className="w-4 h-4 text-accent-primary" />
          Provider
        </label>
        <div className="flex gap-2">
          <button onClick={() => updateAiSettings({ provider: 'anthropic', model: 'claude-sonnet-4-5-20250929' })} className={`flex-1 px-4 py-2 rounded-lg border transition-colors ${ai.provider === 'anthropic' ? 'bg-accent-primary/10 border-accent-primary text-accent-primary' : 'bg-dark-700/50 border-glass-100 text-gray-400 hover:bg-dark-700'}`}>Anthropic</button>
          <button onClick={() => updateAiSettings({ provider: 'openai', model: 'gpt-5.2-chat-latest' })} className={`flex-1 px-4 py-2 rounded-lg border transition-colors ${ai.provider === 'openai' ? 'bg-accent-primary/10 border-accent-primary text-accent-primary' : 'bg-dark-700/50 border-glass-100 text-gray-400 hover:bg-dark-700'}`}>OpenAI</button>
        </div>
      </div>
      <div className="space-y-2">
        <label className="flex items-center gap-2 text-sm font-medium text-gray-300">
          <Key className="w-4 h-4 text-accent-primary" />
          API Key
        </label>
        <div className="flex gap-2">
          <div className="flex-1 relative">
            <input type={showApiKey ? 'text' : 'password'} value={ai.apiKey} onChange={(e) => updateAiSettings({ apiKey: e.target.value })} placeholder={ai.provider === 'anthropic' ? 'sk-ant-...' : 'sk-...'} className="glass-input w-full font-mono pr-10" />
            <button type="button" onClick={() => setShowApiKey(!showApiKey)} className="absolute right-2 top-1/2 -translate-y-1/2 p-1.5 text-gray-500 hover:text-gray-300 transition-colors">
              {showApiKey ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
            </button>
          </div>
          <button onClick={handleTestApiKey} disabled={!ai.apiKey || isTesting} className="glass-button px-4 py-2 disabled:opacity-50 disabled:cursor-not-allowed">
            {isTesting ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Test'}
          </button>
        </div>
        {testResult && <div className={`text-sm flex items-center gap-2 ${testResult.success ? 'text-green-400' : 'text-red-400'}`}>{testResult.success ? <CheckCircle className="w-4 h-4" /> : <AlertCircle className="w-4 h-4" />}{testResult.message}</div>}
        <p className="text-xs text-gray-500">Get your API key from {ai.provider === 'anthropic' ? 'console.anthropic.com' : 'platform.openai.com'}</p>
      </div>
      <div className="space-y-2">
        <label className="text-sm font-medium text-gray-300">Model</label>
        <select value={ai.model} onChange={(e) => updateAiSettings({ model: e.target.value })} className="glass-input w-full">
          {ai.provider === 'anthropic' ? (
            <>
              <option value="claude-sonnet-4-5-20250929">Claude Sonnet 4.5 (Recommended)</option>
              <option value="claude-haiku-4-5-20251001">Claude Haiku 4.5 (Faster)</option>
            </>
          ) : (
            <>
              <option value="gpt-5.2-chat-latest">GPT-5.2 Instant (Recommended)</option>
              <option value="gpt-5-mini">GPT-5 Mini (Faster)</option>
              <option value="gpt-5.2">GPT-5.2 Thinking (Most Capable)</option>
            </>
          )}
        </select>
      </div>
      <div className="border-t border-glass-100 pt-4">
        <h4 className="text-sm font-medium text-gray-300 mb-3">Behavior</h4>
        <div className="space-y-3">
          <label className="flex items-center justify-between p-3 bg-dark-700/50 rounded-lg border border-glass-100 cursor-pointer hover:bg-dark-700 transition-colors">
            <div><div className="font-medium text-gray-100">Auto-generate talk points</div><div className="text-sm text-gray-500">Generate talk points when callsign is focused</div></div>
            <input type="checkbox" checked={ai.autoGenerateTalkPoints} onChange={(e) => updateAiSettings({ autoGenerateTalkPoints: e.target.checked })} className="w-5 h-5 rounded border-gray-600 text-accent-primary focus:ring-accent-primary" />
          </label>
          <label className="flex items-center justify-between p-3 bg-dark-700/50 rounded-lg border border-glass-100 cursor-pointer hover:bg-dark-700 transition-colors">
            <div><div className="font-medium text-gray-100">Include QRZ profile</div><div className="text-sm text-gray-500">Use QRZ profile data for context</div></div>
            <input type="checkbox" checked={ai.includeQrzProfile} onChange={(e) => updateAiSettings({ includeQrzProfile: e.target.checked })} className="w-5 h-5 rounded border-gray-600 text-accent-primary focus:ring-accent-primary" />
          </label>
          <label className="flex items-center justify-between p-3 bg-dark-700/50 rounded-lg border border-glass-100 cursor-pointer hover:bg-dark-700 transition-colors">
            <div><div className="font-medium text-gray-100">Include QSO history</div><div className="text-sm text-gray-500">Use previous QSOs for context</div></div>
            <input type="checkbox" checked={ai.includeQsoHistory} onChange={(e) => updateAiSettings({ includeQsoHistory: e.target.checked })} className="w-5 h-5 rounded border-gray-600 text-accent-primary focus:ring-accent-primary" />
          </label>
        </div>
      </div>
      <div className="bg-dark-700/50 rounded-lg p-4 border border-glass-100">
        <p className="text-sm text-gray-400"><Key className="w-4 h-4 inline mr-2 text-accent-primary" />Your API key is stored locally and calls go directly to the provider. No data is sent through SDRLoggerPlus servers.</p>
      </div>
    </div>
  );
}

// Backup Settings Section
function BackupSettingsSection() {
  const { settings, updateBackupSettings, loadSettings } = useSettingsStore();
  const backup = settings.backup;
  const [status, setStatus] = useState<BackupStatus | null>(null);
  const [running, setRunning] = useState(false);
  const [ioMessage, setIoMessage] = useState<{ ok: boolean; text: string } | null>(null);
  const importFileRef = useRef<HTMLInputElement>(null);

  const handleExportSettings = async () => {
    try {
      const response = await fetch('/api/settings');
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const json = await response.text();
      const blob = new Blob([json], { type: 'application/json' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = `sdrloggerplus-settings-${new Date().toISOString().slice(0, 10)}.json`;
      a.click();
      URL.revokeObjectURL(a.href);
      setIoMessage({ ok: true, text: 'Settings exported.' });
    } catch (err) {
      setIoMessage({ ok: false, text: `Export failed: ${err instanceof Error ? err.message : err}` });
    }
  };

  const handleImportSettings = async (file: File) => {
    try {
      const text = await file.text();
      const parsed = JSON.parse(text);
      const knownKeys = ['station', 'rotator', 'cluster', 'appearance', 'qrz', 'backup', 'radio'];
      if (typeof parsed !== 'object' || parsed === null || !knownKeys.some((k) => k in parsed)) {
        throw new Error('not a SDRLoggerPlus settings file');
      }
      // Use the dedicated /api/settings/import endpoint (full replace)
      // rather than POST /api/settings (which preserves SavedLayouts +
      // LayoutJson from the DB — perfect for the "theme knob changed
      // shouldn't wipe presets" case, wrong for restoring a backup).
      const response = await fetch('/api/settings/import', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(parsed),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      await loadSettings(); // re-hydrate the store (and live theme etc.) from the imported data
      setIoMessage({ ok: true, text: 'Settings imported and applied.' });
    } catch (err) {
      setIoMessage({ ok: false, text: `Import failed: ${err instanceof Error ? err.message : err}` });
    }
  };

  const refreshStatus = async () => {
    try {
      setStatus(await api.getBackupStatus());
    } catch {
      // Backend unavailable — status stays null
    }
  };

  useEffect(() => {
    refreshStatus();
  }, []);

  const handleRunNow = async () => {
    setRunning(true);
    try {
      await api.runBackupNow();
    } catch {
      // Result is reflected via status refresh below
    } finally {
      await refreshStatus();
      setRunning(false);
    }
  };

  const formatUtc = (iso?: string | null) =>
    iso ? new Date(iso).toLocaleString() : '—';

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">Scheduled Backup</h3>
        <p className="text-sm text-dark-300">
          Automatically back up your logbook on a schedule. Each run writes a timestamped folder
          containing a database copy and an ADIF export. Old backups beyond the retention count
          are pruned only after a successful run.
        </p>
      </div>

      {/* Enable toggle */}
      <div className="flex items-center justify-between p-3 bg-dark-700 rounded-lg">
        <div>
          <label className="text-sm font-medium text-dark-200">Enable Scheduled Backups</label>
          <p className="text-xs text-dark-400 mt-0.5">Run backups automatically on the chosen interval</p>
        </div>
        <button
          onClick={() => updateBackupSettings({ enabled: !backup.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${backup.enabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
        >
          <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${backup.enabled ? 'translate-x-5' : ''}`} />
        </button>
      </div>

      {/* Interval */}
      <div>
        <label className="block text-sm font-medium text-dark-200 mb-1">Interval</label>
        <select
          value={backup.interval}
          onChange={(e) => updateBackupSettings({ interval: e.target.value as 'daily' | 'weekly' | 'on_exit' })}
          className="glass-input w-40"
        >
          <option value="daily">Daily</option>
          <option value="weekly">Weekly</option>
          <option value="on_exit">On exit</option>
        </select>
        <p className="text-xs text-dark-400 mt-1">
          Daily/Weekly anchor to the last successful run — reopening the app won't re-trigger a backup that already ran.
        </p>
      </div>

      {/* Retention */}
      <div>
        <label className="block text-sm font-medium text-dark-200 mb-1">Keep Last</label>
        <input
          type="number"
          value={backup.retention}
          onChange={(e) => updateBackupSettings({ retention: Math.max(1, parseInt(e.target.value) || 10) })}
          className="glass-input w-24"
          min={1}
        />
        <p className="text-xs text-dark-400 mt-1">Number of backup folders to retain (oldest pruned first)</p>
      </div>

      {/* Destination */}
      <div>
        <label className="block text-sm font-medium text-dark-200 mb-1">Destination Folder</label>
        <input
          type="text"
          value={backup.destinationPath ?? ''}
          onChange={(e) => updateBackupSettings({ destinationPath: e.target.value })}
          className="glass-input w-full"
          placeholder={status ? `Default: ${status.destination}` : 'Default: <config dir>\\backups'}
        />
        <p className="text-xs text-dark-400 mt-1">Leave blank to use the default backups folder next to your database</p>
      </div>

      {/* Status + Run now */}
      <div className="p-3 bg-dark-700 rounded-lg border border-glass-100 space-y-2">
        <div className="flex items-center justify-between">
          <h4 className="text-sm font-medium text-dark-200">Status</h4>
          <button
            onClick={handleRunNow}
            disabled={running}
            className="glass-button px-3 py-1.5 text-sm flex items-center gap-2"
          >
            {running ? <Loader2 className="w-4 h-4 animate-spin" /> : <Archive className="w-4 h-4" />}
            {running ? 'Backing up…' : 'Back Up Now'}
          </button>
        </div>
        <div className="text-xs text-dark-300 space-y-1">
          <p>Last run: <span className="text-dark-200">{formatUtc(status?.lastRunUtc)}</span></p>
          <p>Next due: <span className="text-dark-200">{formatUtc(status?.nextDueUtc)}</span></p>
          {status?.message && (
            <p className={status.ok === false ? 'text-red-400' : 'text-dark-300'}>
              {status.message}
            </p>
          )}
        </div>
      </div>

      {/* Settings export / import — moved to the bottom so the panel reads
          top-down as "here is the scheduled logbook backup config", and
          this credential-heavy manual escape hatch sits at the end where
          it's less likely to distract during routine schedule tweaks. */}
      <div className="p-4 bg-dark-700/50 rounded-lg border border-glass-100 space-y-3">
        <div>
          <label className="text-sm font-medium text-dark-200">Settings Export / Import</label>
          <p className="text-xs text-dark-400 mt-0.5">
            Save all application settings to a file, or restore them from one. The export includes
            credentials (QRZ, cluster passwords, API keys) — keep the file private.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <button onClick={handleExportSettings} className="glass-button px-4 py-2 flex items-center gap-2 text-sm">
            <Download className="w-4 h-4" />
            Export Settings
          </button>
          <button
            onClick={() => importFileRef.current?.click()}
            className="glass-button px-4 py-2 flex items-center gap-2 text-sm"
          >
            <CloudUpload className="w-4 h-4" />
            Import Settings
          </button>
          <input
            ref={importFileRef}
            type="file"
            accept=".json,application/json"
            className="hidden"
            onChange={(e) => {
              const file = e.target.files?.[0];
              if (file) handleImportSettings(file);
              e.target.value = '';
            }}
          />
        </div>
        {ioMessage && (
          <p className={`text-xs ${ioMessage.ok ? 'text-accent-success' : 'text-accent-danger'}`}>
            {ioMessage.text}
          </p>
        )}
      </div>
    </div>
  );
}

// Hot List Settings Section
function HotListSettingsSection() {
  const { settings, updateHotListSettings } = useSettingsStore();
  const hotList = settings.hotList;
  const [newCall, setNewCall] = useState('');

  const addCall = () => {
    const call = newCall.trim().toUpperCase();
    if (!call || hotList.callsigns.includes(call)) {
      setNewCall('');
      return;
    }
    updateHotListSettings({ callsigns: [...hotList.callsigns, call] });
    setNewCall('');
  };

  const removeCall = (call: string) =>
    updateHotListSettings({ callsigns: hotList.callsigns.filter(c => c !== call) });

  const clearAll = () => {
    if (hotList.callsigns.length === 0) return;
    if (confirm(`Clear ALL ${hotList.callsigns.length} callsign(s) from your Hot List?`)) {
      updateHotListSettings({ callsigns: [] });
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">Hot List</h3>
        <p className="text-sm text-dark-300">
          Watched callsigns. Spots of these calls are highlighted everywhere spots appear and can be
          announced via text-to-speech. Add calls here, or click the 🔥 icon next to a DXpedition.
        </p>
      </div>

      <div className="flex items-center justify-between p-3 bg-dark-700 rounded-lg">
        <div>
          <label className="text-sm font-medium text-dark-200">Enable Hot List</label>
          <p className="text-xs text-dark-400 mt-0.5">Highlight spots of watched callsigns</p>
        </div>
        <button
          onClick={() => updateHotListSettings({ enabled: !hotList.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${hotList.enabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
        >
          <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${hotList.enabled ? 'translate-x-5' : ''}`} />
        </button>
      </div>

      <div className="flex items-center justify-between p-3 bg-dark-700 rounded-lg">
        <div>
          <label className="text-sm font-medium text-dark-200">Voice Announcements</label>
          <p className="text-xs text-dark-400 mt-0.5">Speak hot spots aloud (per-call cooldown below)</p>
        </div>
        <button
          onClick={() => updateHotListSettings({ ttsEnabled: !hotList.ttsEnabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${hotList.ttsEnabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
        >
          <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${hotList.ttsEnabled ? 'translate-x-5' : ''}`} />
        </button>
      </div>

      <div>
        <label className="block text-sm font-medium text-dark-200 mb-1">Announcement Cooldown (minutes)</label>
        <input
          type="number"
          value={hotList.ttsCooldownMinutes}
          onChange={(e) => updateHotListSettings({ ttsCooldownMinutes: Math.max(1, parseInt(e.target.value) || 15) })}
          className="glass-input w-24"
          min={1}
        />
        <p className="text-xs text-dark-400 mt-1">A callsign won't be announced again until this many minutes pass</p>
      </div>

      <div>
        <label className="block text-sm font-medium text-dark-200 mb-2">Watched Callsigns</label>
        <div className="flex gap-2 mb-2">
          <input
            type="text"
            value={newCall}
            onChange={(e) => setNewCall(e.target.value.toUpperCase())}
            onKeyDown={(e) => e.key === 'Enter' && addCall()}
            className="glass-input w-40 font-mono"
            placeholder="e.g. K5P"
          />
          <button onClick={addCall} className="glass-button px-3 py-1.5 text-sm">Add</button>
          {hotList.callsigns.length > 0 && (
            <button onClick={clearAll} className="glass-button px-3 py-1.5 text-sm text-red-400 ml-auto">
              Clear All
            </button>
          )}
        </div>
        {hotList.callsigns.length === 0 ? (
          <p className="text-xs text-dark-400">No callsigns on your Hot List yet.</p>
        ) : (
          <div className="flex flex-wrap gap-1.5">
            {hotList.callsigns.map(call => (
              <span
                key={call}
                className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-dark-700 border border-glass-100 text-xs font-mono text-orange-300"
              >
                🔥 {call}
                <button
                  onClick={() => removeCall(call)}
                  className="text-dark-400 hover:text-red-400 ml-0.5"
                  title={`Remove ${call}`}
                >
                  ×
                </button>
              </span>
            ))}
          </div>
        )}
        <p className="text-xs text-dark-400 mt-2">Remember to Save after editing the list here.</p>
      </div>
    </div>
  );
}

// WSJT-X Settings Section — up to two independent decoder sources (e.g. WSJT-X
// on the primary port and JTDX on a second), each its own UDP listener.
function WsjtxSettingsSection() {
  const { settings, updateWsjtxSettings } = useSettingsStore();
  const wsjtx = settings.wsjtx;
  const [statuses, setStatuses] = useState<WsjtxStatus[] | null>(null);

  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const st = await api.getWsjtxStatus();
        if (!cancelled) setStatuses(st);
      } catch {
        if (!cancelled) setStatuses(null);
      }
    };
    poll();
    const timer = setInterval(poll, 5000);
    return () => { cancelled = true; clearInterval(timer); };
  }, []);

  const statusFor = (source: number) => statuses?.find((s) => s.source === source) ?? null;

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">WSJT-X / JTDX Auto-Logging</h3>
        <p className="text-sm text-dark-300">
          Listen for the WSJT-X UDP protocol and automatically log FT8/FT4 QSOs the moment they
          complete. Works with WSJT-X, JTDX, and MSHV. Enable a second source to run two decoders
          at once — each on its own port.
        </p>
      </div>

      <WsjtxSourceCard
        title="Source 1"
        subtitle="Primary decoder — default WSJT-X on port 2237"
        source={{ enabled: wsjtx.enabled, port: wsjtx.port, multicastAddress: wsjtx.multicastAddress }}
        defaultPort={2237}
        onPatch={(p) => updateWsjtxSettings(p)}
        status={statusFor(1)}
      />

      <WsjtxSourceCard
        title="Source 2"
        subtitle="Optional second decoder on its own port — e.g. JTDX on 2333"
        source={wsjtx.source2}
        defaultPort={2333}
        onPatch={(p) => updateWsjtxSettings({ source2: { ...wsjtx.source2, ...p } })}
        status={statusFor(2)}
      />
    </div>
  );
}

// One WSJT-X source: enable toggle, and (when enabled) its port, multicast and
// live listener status. onPatch applies to whichever source the parent wires in.
function WsjtxSourceCard({ title, subtitle, source, defaultPort, onPatch, status }: {
  title: string;
  subtitle: string;
  source: WsjtxSource;
  defaultPort: number;
  onPatch: (patch: Partial<WsjtxSource>) => void;
  status: WsjtxStatus | null;
}) {
  return (
    <div className="rounded-lg border border-glass-100 bg-dark-700/40 p-4 space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <label className="text-sm font-medium text-dark-200">{title}</label>
          <p className="text-xs text-dark-400 mt-0.5">{subtitle}</p>
        </div>
        <button
          onClick={() => onPatch({ enabled: !source.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${source.enabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
        >
          <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${source.enabled ? 'translate-x-5' : ''}`} />
        </button>
      </div>

      {source.enabled && (
        <>
          <div className="flex flex-wrap gap-6">
            <div>
              <label className="block text-sm font-medium text-dark-200 mb-1">UDP Port</label>
              <input
                type="number"
                value={source.port}
                onChange={(e) => onPatch({ port: parseInt(e.target.value) || defaultPort })}
                className="glass-input w-32"
                min={1024}
                max={65535}
              />
            </div>
            <div className="flex-1 min-w-[12rem]">
              <label className="block text-sm font-medium text-dark-200 mb-1">Multicast Group (optional)</label>
              <input
                type="text"
                value={source.multicastAddress ?? ''}
                onChange={(e) => onPatch({ multicastAddress: e.target.value })}
                className="glass-input w-full max-w-xs"
                placeholder="e.g. 224.0.0.1 (blank = unicast)"
              />
            </div>
          </div>
          <p className="text-xs text-dark-400">
            WSJT-X / JTDX: Settings → Reporting → UDP Server. Give each source a different port.
            Set a multicast group only if the decoder is configured for multicast.
          </p>

          <div className="p-3 bg-dark-700 rounded-lg border border-glass-100 space-y-1 text-xs">
            <h4 className="text-sm font-medium text-dark-200 mb-1">Status</h4>
            {status ? (
              <>
                <p className="text-dark-300">
                  Listener: {status.listening
                    ? <span className="text-accent-success">active on port {status.port}</span>
                    : <span className="text-dark-400">not listening{status.error ? ` — ${status.error}` : ''}</span>}
                </p>
                <p className="text-dark-300">
                  Clients: {status.clients.length > 0
                    ? status.clients.map((c) => `${c.id}${c.version ? ` v${c.version}` : ''}`).join(', ')
                    : 'none heard yet'}
                </p>
                {status.lastQsoCall && (
                  <p className="text-dark-300">
                    Last auto-logged: <span className="font-mono text-accent-secondary">{status.lastQsoCall}</span>
                    {status.lastQsoAtUtc && ` at ${new Date(status.lastQsoAtUtc).toLocaleTimeString()}`}
                  </p>
                )}
              </>
            ) : (
              <p className="text-dark-400">Status unavailable</p>
            )}
          </div>
        </>
      )}
    </div>
  );
}

// Weather Alerts Settings Section
function WeatherSettingsSection() {
  const { settings, updateWeatherSettings } = useSettingsStore();
  const weather = settings.weather;
  // The wind switch may be 'auto' — resolve it against the master unit system.
  const isKph = resolveSpeedUnit(weather.wind.displayUnit, settings.appearance.unitSystem) === 'kph';
  const toDisplay = (mph: number) => isKph ? Math.round(mph * 1.60934) : mph;
  const fromDisplay = (v: number) => isKph ? v / 1.60934 : v;

  const Toggle = ({ checked, onChange, label }: { checked: boolean; onChange: () => void; label: string }) => (
    <label className="flex items-center gap-2 text-xs text-dark-300 cursor-pointer">
      <input type="checkbox" checked={checked} onChange={onChange} className="accent-accent-primary" />
      {label}
    </label>
  );

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">Weather Alerts</h3>
        <p className="text-sm text-dark-300">
          Station-protection alerts: lightning proximity and high wind, shown as a banner above the
          status bar. Uses your station location (Settings / Station).
        </p>
      </div>

      {/* Lightning */}
      <div className="p-3 bg-dark-700 rounded-lg space-y-3">
        <div className="flex items-center justify-between">
          <div>
            <label className="text-sm font-medium text-dark-200">⚡ Lightning Detection</label>
            <p className="text-xs text-dark-400 mt-0.5">Polls every 90 seconds while enabled</p>
          </div>
          <button
            onClick={() => updateWeatherSettings({ lightning: { enabled: !weather.lightning.enabled } as never })}
            className={`relative w-11 h-6 rounded-full transition-colors ${weather.lightning.enabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
          >
            <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${weather.lightning.enabled ? 'translate-x-5' : ''}`} />
          </button>
        </div>
        <div className="grid grid-cols-2 gap-2">
          <Toggle label="Blitzortung strike feed" checked={weather.lightning.useBlitzortung}
            onChange={() => updateWeatherSettings({ lightning: { useBlitzortung: !weather.lightning.useBlitzortung } as never })} />
          <Toggle label="NWS warnings" checked={weather.lightning.useNws}
            onChange={() => updateWeatherSettings({ lightning: { useNws: !weather.lightning.useNws } as never })} />
          <Toggle label="Ambient Weather station" checked={weather.lightning.useAmbient}
            onChange={() => updateWeatherSettings({ lightning: { useAmbient: !weather.lightning.useAmbient } as never })} />
          <Toggle label="Ecowitt station" checked={weather.lightning.useEcowitt}
            onChange={() => updateWeatherSettings({ lightning: { useEcowitt: !weather.lightning.useEcowitt } as never })} />
        </div>
        <div className="flex items-center gap-2 text-xs">
          <span className="text-dark-300">Alert range</span>
          <input type="number" value={weather.lightning.range} min={5}
            onChange={(e) => updateWeatherSettings({ lightning: { range: parseInt(e.target.value) || 50 } as never })}
            className="glass-input w-20" />
          <select value={weather.lightning.rangeUnit}
            onChange={(e) => updateWeatherSettings({ lightning: { rangeUnit: e.target.value as 'mi' | 'km' } as never })}
            className="glass-input px-2 py-1">
            <option value="mi">miles</option>
            <option value="km">km</option>
          </select>
        </div>
      </div>

      {/* High wind */}
      <div className="p-3 bg-dark-700 rounded-lg space-y-3">
        <div className="flex items-center justify-between">
          <div>
            <label className="text-sm font-medium text-dark-200">💨 High Wind Alerts</label>
            <p className="text-xs text-dark-400 mt-0.5">Polls every 2 minutes; three severity tiers</p>
          </div>
          <button
            onClick={() => updateWeatherSettings({ wind: { enabled: !weather.wind.enabled } as never })}
            className={`relative w-11 h-6 rounded-full transition-colors ${weather.wind.enabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
          >
            <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${weather.wind.enabled ? 'translate-x-5' : ''}`} />
          </button>
        </div>
        <div className="grid grid-cols-2 gap-2">
          <Toggle label="NWS wind warnings" checked={weather.wind.useNwsAlerts}
            onChange={() => updateWeatherSettings({ wind: { useNwsAlerts: !weather.wind.useNwsAlerts } as never })} />
          <Toggle label="NWS METAR observation" checked={weather.wind.useNwsMetar}
            onChange={() => updateWeatherSettings({ wind: { useNwsMetar: !weather.wind.useNwsMetar } as never })} />
          <Toggle label="Ambient Weather station" checked={weather.wind.useAmbient}
            onChange={() => updateWeatherSettings({ wind: { useAmbient: !weather.wind.useAmbient } as never })} />
          <Toggle label="Ecowitt station" checked={weather.wind.useEcowitt}
            onChange={() => updateWeatherSettings({ wind: { useEcowitt: !weather.wind.useEcowitt } as never })} />
        </div>
        {weather.wind.useNwsMetar && (
          <div className="flex items-center gap-2 text-xs">
            <span className="text-dark-300">METAR station</span>
            <input type="text" value={weather.wind.metarStation ?? ''}
              onChange={(e) => updateWeatherSettings({ wind: { metarStation: e.target.value.toUpperCase() } as never })}
              className="glass-input w-24 font-mono" placeholder="KLUK" />
            <span className="text-dark-400">nearest airport ICAO code</span>
          </div>
        )}
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className="text-dark-300">Thresholds: sustained</span>
          <input type="number" value={toDisplay(weather.wind.threshSustainedMph)}
            onChange={(e) => updateWeatherSettings({ wind: { threshSustainedMph: fromDisplay(parseInt(e.target.value) || 30) } as never })}
            className="glass-input w-16" />
          <span className="text-dark-300">gust</span>
          <input type="number" value={toDisplay(weather.wind.threshGustMph)}
            onChange={(e) => updateWeatherSettings({ wind: { threshGustMph: fromDisplay(parseInt(e.target.value) || 45) } as never })}
            className="glass-input w-16" />
          <select value={weather.wind.displayUnit}
            onChange={(e) => updateWeatherSettings({ wind: { displayUnit: e.target.value as 'auto' | 'mph' | 'kph' } as never })}
            className="glass-input px-2 py-1">
            <option value="auto">Auto</option>
            <option value="mph">mph</option>
            <option value="kph">kph</option>
          </select>
          <span className="text-dark-300">cooldown</span>
          <input type="number" value={weather.wind.cooldownMinutes} min={1}
            onChange={(e) => updateWeatherSettings({ wind: { cooldownMinutes: parseInt(e.target.value) || 20 } as never })}
            className="glass-input w-16" />
          <span className="text-dark-400">min</span>
        </div>
        <p className="text-xs text-dark-400">
          Thresholds are stored in mph internally — switching the unit only changes how they display.
        </p>
      </div>

      {/* Credentials */}
      <div className="p-3 bg-dark-700 rounded-lg space-y-2">
        <h4 className="text-sm font-medium text-dark-200">Station API Credentials</h4>
        <div className="grid grid-cols-2 gap-2 text-xs">
          <div>
            <label className="block text-dark-300 mb-0.5">Ambient API Key</label>
            <input type="password" value={weather.credentials.ambientApiKey ?? ''}
              onChange={(e) => updateWeatherSettings({ credentials: { ambientApiKey: e.target.value } as never })}
              className="glass-input w-full" />
          </div>
          <div>
            <label className="block text-dark-300 mb-0.5">Ambient Application Key</label>
            <input type="password" value={weather.credentials.ambientAppKey ?? ''}
              onChange={(e) => updateWeatherSettings({ credentials: { ambientAppKey: e.target.value } as never })}
              className="glass-input w-full" />
          </div>
          <div>
            <label className="block text-dark-300 mb-0.5">Ecowitt Application Key</label>
            <input type="password" value={weather.credentials.ecowittAppKey ?? ''}
              onChange={(e) => updateWeatherSettings({ credentials: { ecowittAppKey: e.target.value } as never })}
              className="glass-input w-full" />
          </div>
          <div>
            <label className="block text-dark-300 mb-0.5">Ecowitt API Key</label>
            <input type="password" value={weather.credentials.ecowittApiKey ?? ''}
              onChange={(e) => updateWeatherSettings({ credentials: { ecowittApiKey: e.target.value } as never })}
              className="glass-input w-full" />
          </div>
          <div>
            <label className="block text-dark-300 mb-0.5">Ecowitt Gateway MAC</label>
            <input type="text" value={weather.credentials.ecowittMac ?? ''}
              onChange={(e) => updateWeatherSettings({ credentials: { ecowittMac: e.target.value } as never })}
              className="glass-input w-full font-mono" placeholder="AA:BB:CC:DD:EE:FF" />
          </div>
        </div>
        <p className="text-xs text-dark-400">
          One set of credentials per vendor drives both lightning and wind. Ecowitt readings are
          cached for 30 s so both pollers share a single API call.
        </p>
      </div>

      <WeatherPreviewSubsection />
    </div>
  );
}

// Preview subsection — inject fake lightning / wind statuses into the
// WeatherAlertBanner for 20 seconds so the operator can see what a real
// alert will look like without waiting for actual weather. Client-side
// only, no backend touched. Preview overrides the enable-check on the
// banner so it fires even when weather alerts are otherwise disabled.
function WeatherPreviewSubsection() {
  const { startPreview } = useWeatherPreviewStore();
  const kph = useSettingsStore(
    state => resolveSpeedUnit(state.settings.weather.wind.displayUnit, state.settings.appearance.unitSystem) === 'kph',
  );

  const fakeLightning = () => startPreview({
    lightning: {
      active: true,
      closestKm: 12,
      closestMi: 7,
      direction: 'SW',
      strikesLastHour: 23,
      sources: ['preview'],
      nwsWarning: null,
      lastUpdateUtc: new Date().toISOString(),
    },
  });

  const fakeWind = () => startPreview({
    wind: {
      active: true,
      severity: 'high',
      sustainedMph: 34,
      gustMph: 51,
      sustainedKph: Math.round(34 * 1.60934),
      gustKph: Math.round(51 * 1.60934),
      direction: 'WSW',
      sources: ['preview'],
      nwsAlert: null,
      lastUpdateUtc: new Date().toISOString(),
      unit: kph ? 'kph' : 'mph',
      threshSustMph: 30,
      threshGustMph: 45,
    },
  });

  const fakeBothExtreme = () => startPreview({
    lightning: {
      active: true,
      closestKm: 3,
      closestMi: 2,
      direction: 'W',
      strikesLastHour: 87,
      sources: ['preview'],
      nwsWarning: 'SEVERE THUNDERSTORM WARNING',
      lastUpdateUtc: new Date().toISOString(),
    },
    wind: {
      active: true,
      severity: 'extreme',
      sustainedMph: 62,
      gustMph: 88,
      sustainedKph: Math.round(62 * 1.60934),
      gustKph: Math.round(88 * 1.60934),
      direction: 'NW',
      sources: ['preview'],
      nwsAlert: 'HIGH WIND WARNING',
      lastUpdateUtc: new Date().toISOString(),
      unit: kph ? 'kph' : 'mph',
      threshSustMph: 30,
      threshGustMph: 45,
    },
  });

  return (
    <div>
      <h4 className="text-sm font-semibold font-ui text-dark-200 mb-1">Preview Alerts</h4>
      <p className="text-xs text-dark-300 mb-3">
        Show a fake alert banner for 20 seconds so you can see what real alerts will look like (and where they'll appear). Preview also fires when weather alerts are turned off — no real weather data or backend calls involved.
      </p>
      <div className="flex flex-wrap gap-2">
        <button
          onClick={fakeLightning}
          className="px-3 py-1.5 rounded text-xs font-ui border border-yellow-500/40 text-yellow-300 hover:bg-yellow-500/10 transition-colors"
          title="Show a fake lightning-only alert (elevated severity)"
        >
          ⚡ Lightning Only
        </button>
        <button
          onClick={fakeWind}
          className="px-3 py-1.5 rounded text-xs font-ui border border-orange-500/40 text-orange-300 hover:bg-orange-500/10 transition-colors"
          title="Show a fake high-wind alert"
        >
          💨 High Wind
        </button>
        <button
          onClick={fakeBothExtreme}
          className="px-3 py-1.5 rounded text-xs font-ui border border-red-500/40 text-red-300 hover:bg-red-500/10 transition-colors"
          title="Show both alerts at extreme severity — worst-case appearance"
        >
          ⚡💨 Both — Extreme
        </button>
      </div>
    </div>
  );
}

// S.A.T. Controller Settings Section
function SatSettingsSection() {
  const { settings, updateSatSettings } = useSettingsStore();
  const sat = settings.sat;

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">CSN S.A.T. Controller</h3>
        <p className="text-sm text-dark-300">
          Link to a CSN Technologies S.A.T. satellite controller: live pass status, transponder info,
          and automatic logging of SAT QSOs (with SAT_NAME / PROP_MODE preserved for LoTW credit).
          Add the S.A.T. Controller panel to your layout, then use its Activate button during passes.
        </p>
      </div>

      <div className="flex items-center justify-between p-3 bg-dark-700 rounded-lg">
        <div>
          <label className="text-sm font-medium text-dark-200">Enable S.A.T. Integration</label>
          <p className="text-xs text-dark-400 mt-0.5">
            Listeners bind only while the panel is Activated, keeping the ports free otherwise
          </p>
        </div>
        <button
          onClick={() => updateSatSettings({ enabled: !sat.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${sat.enabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
        >
          <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${sat.enabled ? 'translate-x-5' : ''}`} />
        </button>
      </div>

      <div>
        <label className="block text-sm font-medium text-dark-200 mb-1">Controller IP Address</label>
        <input
          type="text"
          value={sat.controllerIp ?? ''}
          onChange={(e) => updateSatSettings({ controllerIp: e.target.value })}
          className="glass-input w-64 font-mono"
          placeholder="e.g. 192.168.200.194"
        />
        <p className="text-xs text-dark-400 mt-1">Used to poll the controller's /track endpoint for live pass data</p>
      </div>

      <div className="flex gap-6">
        <div>
          <label className="block text-sm font-medium text-dark-200 mb-1">Broadcast UDP Port</label>
          <input
            type="number"
            value={sat.udpPort}
            onChange={(e) => updateSatSettings({ udpPort: parseInt(e.target.value) || 9932 })}
            className="glass-input w-28"
          />
          <p className="text-xs text-dark-400 mt-1">Default: 9932</p>
        </div>
        <div>
          <label className="block text-sm font-medium text-dark-200 mb-1">ADIF QSO Port</label>
          <input
            type="number"
            value={sat.adifPort}
            onChange={(e) => updateSatSettings({ adifPort: parseInt(e.target.value) || 1100 })}
            className="glass-input w-28"
          />
          <p className="text-xs text-dark-400 mt-1">Default: 1100 (S.A.T. QSO LOG TYPE)</p>
        </div>
      </div>
    </div>
  );
}

// About Section
function AboutSection() {
  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">About SDRLoggerPlus</h3>
        <p className="text-sm text-dark-300">Version and application information.</p>
      </div>

      <div className="space-y-4">
        <div className="p-4 bg-dark-700/50 rounded-lg border border-glass-100">
          <div className="flex items-center gap-4">
            <img src="./sdrloggerplus-icon.png" alt="SDRLoggerPlus" className="w-24 h-24 rounded-lg" />
            <div>
              <h4 className="text-xl font-bold font-display text-accent-primary">SDRLOGGERPLUS</h4>
              <p className="text-sm text-dark-300">Ham Radio Logging Software</p>
              <p className="text-xs text-dark-300 mt-1">Version {APP_VERSION}</p>
            </div>
          </div>
        </div>

        {/* Jump straight to the in-app User Guide (Help tab of the About
            dialog). The dialog lives in the always-mounted StatusBar, so a
            window event opens it on the Help tab from here. */}
        <button
          onClick={() => window.dispatchEvent(new CustomEvent('open-help-guide'))}
          className="w-full flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg bg-accent-primary/10 border border-accent-primary/30 text-accent-primary hover:bg-accent-primary/20 transition-colors text-sm font-ui"
        >
          <HelpCircle className="w-4 h-4" /> Open the User Guide
        </button>

        <div className="space-y-2 text-sm text-dark-300">
          <p>
            <strong className="text-dark-200">Authors:</strong> Rick Langford (N8SDR) and Brent Crier (N9BC)
          </p>
          <p>
            <strong className="text-dark-200">License:</strong> MIT License
          </p>
          <p>
            <strong className="text-dark-200">Website:</strong>{' '}
            <a href="https://github.com/N8SDR1/SDRLoggerPlus" className="text-accent-primary hover:underline">
              github.com/N8SDR1/SDRLoggerPlus
            </a>
          </p>
        </div>

        <div className="pt-4 border-t border-glass-100 space-y-2">
          <p className="text-xs text-dark-300">
            SDRLoggerPlus is a modern ham radio logging application designed for amateur radio operators.
            It features real-time DX cluster integration, rotator control, and QSO logging.
          </p>
          <p className="text-xs text-dark-400">
            Built on{' '}
            <a href="https://github.com/brianbruff/Log4YM" className="text-accent-primary hover:underline">
              Log4YM
            </a>{' '}
            by Brian Keating (EI6LF) — released under the Unlicense. Bundles Hamlib and libusb (LGPL-2.1)
            and the AD1C Country Files. See THIRD-PARTY-NOTICES for full open-source license information.
          </p>
        </div>
      </div>
    </div>
  );
}

// Web Logbooks — groups QRZ, LOTW, Club Log, HRDLog, eQSL and POTA under one category with sub-tabs.
type WebLogbookTab = 'qrz' | 'hamqth' | 'lotw' | 'clublog' | 'hrdlog' | 'eqsl' | 'pota' | 'countryfiles';

function WebLogbooksSection() {
  const [tab, setTab] = useState<WebLogbookTab>('qrz');
  const tabs: { id: WebLogbookTab; label: string }[] = [
    { id: 'qrz', label: 'QRZ.com' },
    { id: 'hamqth', label: 'HamQTH' },
    { id: 'lotw', label: 'LOTW' },
    { id: 'clublog', label: 'Club Log' },
    { id: 'hrdlog', label: 'HRDLog' },
    { id: 'eqsl', label: 'eQSL' },
    { id: 'pota', label: 'POTA' },
    { id: 'countryfiles', label: 'Country Files' },
  ];

  return (
    <div className="space-y-6">
      <div className="flex gap-1 border-b border-glass-100">
        {tabs.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-2 text-sm font-ui font-medium border-b-2 -mb-px transition-colors ${
              tab === t.id
                ? 'border-accent-primary text-accent-primary'
                : 'border-transparent text-dark-300 hover:text-dark-200'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'qrz' && <QrzSettingsSection />}
      {tab === 'hamqth' && <HamQthSettingsSection />}
      {tab === 'lotw' && <LotwSettingsSection />}
      {tab === 'clublog' && <ClubLogSettingsSection />}
      {tab === 'hrdlog' && <HrdLogSettingsSection />}
      {tab === 'eqsl' && <EqslSettingsSection />}
      {tab === 'pota' && <PotaSettingsSection />}
      {tab === 'countryfiles' && <CountryFilesSection />}
    </div>
  );
}

// HamQTH Settings Section — fallback callsign lookup source when QRZ isn't
// configured or comes up empty. Free account at hamqth.com; the server
// caches the session_id, so we only need creds here.
function HamQthSettingsSection() {
  const { settings, updateHamQthSettings } = useSettingsStore();
  const [showPassword, setShowPassword] = useState(false);
  const hq = settings.hamQth;

  // "Test Credentials" state — mirrors the QRZ section's pattern above.
  // On success we also flip enabled=true so the operator doesn't have to
  // separately remember to toggle the switch after a green tick.
  const [testStatus, setTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [testMessage, setTestMessage] = useState('');
  const handleTest = async () => {
    setTestStatus('testing');
    setTestMessage('');
    try {
      const resp = await fetch('/api/hamqth/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: hq.username, password: hq.password }),
      });
      const data = await resp.json();
      if (data.success) {
        setTestStatus('success');
        setTestMessage(data.message || 'Connected successfully');
        if (!hq.enabled) updateHamQthSettings({ enabled: true });
      } else {
        setTestStatus('error');
        setTestMessage(data.message || 'Connection failed');
      }
    } catch {
      setTestStatus('error');
      setTestMessage('Failed to reach server');
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">HamQTH Integration</h3>
        <p className="text-sm text-dark-300">
          Optional fallback callsign lookup for when QRZ is unconfigured or returns nothing.
          Free account at <a href="https://www.hamqth.com/register.php" target="_blank" rel="noreferrer" className="text-accent-primary hover:underline">hamqth.com</a>.
        </p>
      </div>

      <div className="flex items-center justify-between p-4 bg-dark-700/50 rounded-lg border border-glass-100">
        <div>
          <p className="font-medium font-ui text-dark-200">Enable HamQTH Lookups</p>
          <p className="text-sm text-dark-300">Used as a second-tier fallback — QRZ still wins when it has data.</p>
        </div>
        <button
          onClick={() => updateHamQthSettings({ enabled: !hq.enabled })}
          className={`relative w-11 h-6 rounded-full transition-colors ${
            hq.enabled ? 'bg-accent-success' : 'bg-dark-600 border border-dark-400'
          }`}
        >
          <span
            className={`absolute top-0.5 left-0.5 w-5 h-5 rounded-full bg-white shadow transition-transform duration-200 ${
              hq.enabled ? 'translate-x-5' : 'translate-x-0'
            }`}
          />
        </button>
      </div>

      <div className="space-y-4">
        <div>
          <label className="text-sm font-medium font-ui text-dark-200 mb-1 block">HamQTH Username</label>
          <input
            type="text"
            value={hq.username}
            onChange={(e) => updateHamQthSettings({ username: e.target.value })}
            placeholder="Your HamQTH username (usually your callsign)"
            className="glass-input w-full"
          />
        </div>
        <div>
          <label className="text-sm font-medium font-ui text-dark-200 mb-1 block">HamQTH Password</label>
          <div className="relative">
            <input
              type={showPassword ? 'text' : 'password'}
              value={hq.password}
              onChange={(e) => updateHamQthSettings({ password: e.target.value })}
              placeholder="Your HamQTH password"
              className="glass-input w-full pr-10"
            />
            <button
              type="button"
              onClick={() => setShowPassword((s) => !s)}
              className="absolute right-2 top-1/2 -translate-y-1/2 text-dark-400 hover:text-dark-200"
            >
              {showPassword ? '🙈' : '👁'}
            </button>
          </div>
          <p className="text-xs text-dark-400 mt-2">
            Stored in the local user config only. Session is negotiated on the server; no other machine sees it.
          </p>
        </div>

        {/* Test Credentials — parity with QRZ. A round-trip login to
            HamQTH's XML API proves the username/password work right now;
            success also flips enabled=true so a green tick can't be
            defeated by a forgotten master toggle. */}
        <div className="pt-2">
          <button
            onClick={handleTest}
            disabled={testStatus === 'testing' || !hq.username || !hq.password}
            className="glass-button-primary px-4 py-2 disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {testStatus === 'testing' ? 'Testing…' : 'Test Credentials'}
          </button>
          {testStatus === 'success' && (
            <span className="ml-3 text-sm text-accent-success">✓ {testMessage}</span>
          )}
          {testStatus === 'error' && (
            <span className="ml-3 text-sm text-red-400">✗ {testMessage}</span>
          )}
        </div>
      </div>
    </div>
  );
}

// Country Files (AD1C cty.dat) Section — status + one-click update.
// cty.dat is what powers the last-ditch centroid fallback when no callbook
// lookup returns coordinates. Users can refresh it against country-files.com
// without waiting for a new app release.
function CountryFilesSection() {
  const [status, setStatus] = useState<{
    prefixCount: number;
    updatedUtc?: string | null;
    source: string;
    version?: string | null;
    sourceUrl: string;
  } | null>(null);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<{ kind: 'ok' | 'err'; text: string } | null>(null);

  const loadStatus = async () => {
    try {
      const r = await fetch('/api/cty/status');
      if (r.ok) setStatus(await r.json());
    } catch { /* ignore */ }
  };

  useEffect(() => { loadStatus(); }, []);

  const handleUpdate = async () => {
    setBusy(true);
    setMsg(null);
    try {
      const r = await fetch('/api/cty/update', { method: 'POST' });
      if (r.ok) {
        const s = await r.json();
        setStatus(s);
        setMsg({ kind: 'ok', text: `Updated — ${s.prefixCount.toLocaleString()} prefixes${s.version ? `, version ${s.version}` : ''}.` });
      } else {
        const err = await r.json().catch(() => ({ error: r.statusText }));
        setMsg({ kind: 'err', text: err.error || `HTTP ${r.status}` });
      }
    } catch (e) {
      setMsg({ kind: 'err', text: e instanceof Error ? e.message : String(e) });
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">AD1C Country Files (cty.dat)</h3>
        <p className="text-sm text-dark-300">
          Provides DXCC prefix data and country centroids used by the callsign lookup fallback.
          Ships bundled — click <em>Update</em> to fetch the latest release from{' '}
          <a href={status?.sourceUrl ?? 'https://www.country-files.com/'} target="_blank" rel="noreferrer" className="text-accent-primary hover:underline">
            country-files.com
          </a>.
        </p>
      </div>

      <div className="p-4 bg-dark-700/50 rounded-lg border border-glass-100 space-y-2 text-sm">
        <div className="flex justify-between">
          <span className="text-dark-300">Prefixes loaded</span>
          <span className="font-mono text-dark-200">{status ? status.prefixCount.toLocaleString() : '—'}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-dark-300">Version</span>
          <span className="font-mono text-dark-200">{status?.version ?? 'unknown'}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-dark-300">Source</span>
          <span className="font-mono text-dark-200">{status?.source ?? '—'}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-dark-300">Last updated</span>
          <span className="font-mono text-dark-200">
            {status?.updatedUtc ? new Date(status.updatedUtc).toLocaleString() : 'never (bundled default)'}
          </span>
        </div>
      </div>

      <div className="flex items-center gap-3">
        <button
          onClick={handleUpdate}
          disabled={busy}
          className="px-4 py-2 rounded-lg bg-accent-primary hover:bg-accent-primary/90 text-white text-sm font-medium disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {busy ? 'Updating…' : 'Update Country Files'}
        </button>
        {msg && (
          <span className={`text-sm ${msg.kind === 'ok' ? 'text-accent-success' : 'text-accent-danger'}`}>
            {msg.text}
          </span>
        )}
      </div>
    </div>
  );
}

// Alerts — groups Weather and Hot List under one category with sub-tabs.
type AlertsTab = 'weather' | 'hotlist';

function AlertsSection() {
  const [tab, setTab] = useState<AlertsTab>('weather');
  const tabs: { id: AlertsTab; label: string }[] = [
    { id: 'weather', label: 'Weather' },
    { id: 'hotlist', label: 'Hot List' },
  ];

  return (
    <div className="space-y-6">
      <div className="flex gap-1 border-b border-glass-100">
        {tabs.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-2 text-sm font-ui font-medium border-b-2 -mb-px transition-colors ${
              tab === t.id
                ? 'border-accent-primary text-accent-primary'
                : 'border-transparent text-dark-300 hover:text-dark-200'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'weather' && <WeatherSettingsSection />}
      {tab === 'hotlist' && <HotListSettingsSection />}
    </div>
  );
}

// Main Settings Panel Component
export function SettingsPanel() {
  const {
    isOpen,
    closeSettings,
    activeSection,
    setActiveSection,
    isDirty,
    isSaving,
    saveSettings,
    resetSettings,
    error,
    clearError,
  } = useSettingsStore();

  if (!isOpen) return null;

  const handleSave = async () => {
    try {
      await saveSettings();
    } catch {
      // Error handled in store
    }
  };

  const renderSection = () => {
    switch (activeSection) {
      case 'station':
        return <StationSettingsSection />;
      case 'weblogbooks':
        return <WebLogbooksSection />;
      case 'wsjtx':
        return <WsjtxSettingsSection />;
      case 'rotator':
        return <RotatorSettingsSection />;
      case 'appearance':
        return <AppearanceSettingsSection />;
      case 'map':
        return <MapSettingsSection />;
      case 'header':
        return <HeaderSettingsSection />;
      case 'ai':
        return <AiSettingsSection />;
      case 'adifmonitor':
        return <AdifMonitorSettingsSection />;
      case 'rbnalerts':
        return <RbnAlertsSettingsSection />;
      case 'backup':
        return <BackupSettingsSection />;
      case 'alerts':
        return <AlertsSection />;
      case 'sat':
        return <SatSettingsSection />;
      case 'about':
        return <AboutSection />;
      default:
        return null;
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-dark-900/80 backdrop-blur-sm" onClick={closeSettings} />

      {/* Panel */}
      <div className="relative w-full max-w-4xl max-h-[85vh] mx-4 bg-dark-800 border border-glass-200 rounded-xl shadow-2xl flex overflow-hidden animate-fade-in">
        {/* Master - Navigation sidebar */}
        <div className="w-64 flex-shrink-0 bg-dark-850 border-r border-glass-100 flex flex-col">
          {/* Header */}
          <div className="p-4 border-b border-glass-100">
            <div className="flex items-center gap-3">
              <Settings className="w-6 h-6 text-accent-primary" />
              <h2 className="text-lg font-semibold font-display">Settings</h2>
            </div>
          </div>

          {/* Navigation */}
          <nav className="flex-1 p-2 space-y-1 overflow-auto">
            {SETTINGS_SECTIONS.map((section) => (
              <button
                key={section.id}
                onClick={() => setActiveSection(section.id)}
                className={`w-full flex items-center gap-3 px-3 py-3 rounded-lg text-left transition-all ${
                  activeSection === section.id
                    ? 'bg-accent-primary/10 text-accent-primary border border-accent-primary/30'
                    : 'hover:bg-dark-700 text-dark-300 hover:text-dark-200 border border-transparent'
                }`}
              >
                <span className={activeSection === section.id ? 'text-accent-secondary' : 'text-dark-300'}>
                  {section.icon}
                </span>
                <div>
                  <p className="font-medium font-ui">{section.name}</p>
                  <p className="text-xs text-dark-300">{section.description}</p>
                </div>
              </button>
            ))}
          </nav>

          {/* Footer actions */}
          <div className="p-3 border-t border-glass-100 space-y-2">
            <button
              onClick={handleSave}
              disabled={!isDirty || isSaving}
              className="w-full glass-button-success flex items-center justify-center gap-2 py-2 disabled:opacity-50"
            >
              {isSaving ? (
                <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
              ) : (
                <Save className="w-4 h-4" />
              )}
              <span>{isSaving ? 'Saving...' : 'Save Changes'}</span>
            </button>
            <button
              onClick={resetSettings}
              className="w-full glass-button flex items-center justify-center gap-2 py-2 text-dark-300"
            >
              <RotateCcw className="w-4 h-4" />
              <span>Reset to Defaults</span>
            </button>
          </div>
        </div>

        {/* Detail - Content area */}
        <div className="flex-1 min-w-0 flex flex-col">
          {/* Header with close button */}
          <div className="flex items-center justify-between p-4 border-b border-glass-100">
            <h3 className="text-lg font-semibold font-display text-dark-200">
              {SETTINGS_SECTIONS.find((s) => s.id === activeSection)?.name}
            </h3>
            <button onClick={closeSettings} className="p-2 hover:bg-dark-700 rounded-lg transition-colors">
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Content */}
          <div className="flex-1 p-6 overflow-auto">{renderSection()}</div>

          {/* Error indicator */}
          {error && (
            <div className="px-4 py-2 bg-accent-danger/10 border-t border-accent-danger/30 text-accent-danger text-sm flex items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <AlertCircle className="w-4 h-4" />
                <span>{error}</span>
              </div>
              <button onClick={clearError} className="p-1 hover:bg-accent-danger/20 rounded transition-colors">
                <X className="w-4 h-4" />
              </button>
            </div>
          )}

          {/* Dirty indicator */}
          {isDirty && !error && (
            <div className="px-4 py-2 bg-accent-primary/10 border-t border-accent-primary/30 text-accent-primary text-sm flex items-center gap-2">
              <AlertCircle className="w-4 h-4" />
              <span>You have unsaved changes</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

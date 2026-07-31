import { useState, useMemo, useCallback, useEffect } from 'react';
import { RadioTower, Map, Settings, Plus, Trash2, X, Search, Crosshair, Eraser, SlidersHorizontal, ChevronDown, Send } from 'lucide-react';
import { AgGridReact } from 'ag-grid-react';
import { ColDef, ICellRendererParams, RowClickedEvent, CellMouseOverEvent, CellMouseOutEvent, RowStyle } from 'ag-grid-community';
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-alpine.css';
import { useSignalR } from '../hooks/useSignalR';
import { GlassPanel } from '../components/GlassPanel';
import { MultiSelectDropdown, MultiSelectOption } from '../components/MultiSelectDropdown';
import { getCountryFlag } from '../core/countryFlags';
import { useSettingsStore, ClusterConnection, type SpotStatusColors, type SpotStatusEnabled } from '../store/settingsStore';
import { useAppStore, Spot } from '../store/appStore';
import { useAgGridState } from '../hooks/useAgGridState';
import { rigModeToSpotModes } from '../utils/rigTracking';
import { getBandFromFrequency, BAND_OPTIONS, MODE_OPTIONS } from '../utils/spotBands';
import { computeSpotGeo, formatDistance, formatBearing, type SpotGeo } from '../utils/spotGeo';

const STATUS_OPTIONS: MultiSelectOption[] = [
  { value: 'newDxcc', label: 'New DXCC' },
  { value: 'newBand', label: 'New Band' },
  { value: 'worked', label: 'Worked' },
  { value: 'none', label: 'Unknown' },
];

// Continent options for the spotter/DX continent filters — the seven Maidenhead
// continents, matching what cty.dat resolves onto each spot.
const CONTINENT_OPTIONS: MultiSelectOption[] = [
  { value: 'NA', label: 'North America' },
  { value: 'SA', label: 'South America' },
  { value: 'EU', label: 'Europe' },
  { value: 'AF', label: 'Africa' },
  { value: 'AS', label: 'Asia' },
  { value: 'OC', label: 'Oceania' },
  { value: 'AN', label: 'Antarctica' },
];


const formatFrequency = (freq: number) => {
  return (freq / 1000).toFixed(3);
};

const formatTime = (dateStr: string) => {
  if (!dateStr) return '--:--';
  try {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return '--:--';
    return date.toISOString().slice(11, 16);
  } catch {
    return '--:--';
  }
};

const getAge = (dateStr: string) => {
  if (!dateStr) return '-';
  try {
    const now = new Date();
    const spotted = new Date(dateStr);
    if (isNaN(spotted.getTime())) return '-';
    const minutes = Math.floor((now.getTime() - spotted.getTime()) / 60000);
    if (minutes < 1) return 'now';
    if (minutes < 60) return `${minutes}m`;
    return `${Math.floor(minutes / 60)}h`;
  } catch {
    return '-';
  }
};

// Infer mode from frequency if not provided
const inferModeFromFrequency = (freq: number): string | null => {
  // Common FT8 frequencies (in kHz)
  const ft8Freqs = [1840, 3573, 7074, 10136, 14074, 18100, 21074, 24915, 28074, 50313];
  // Common FT4 frequencies
  const ft4Freqs = [3575, 7047, 10140, 14080, 18104, 21140, 24919, 28180];
  // Check if frequency is within 5 kHz of known digital frequencies
  for (const f of ft8Freqs) {
    if (Math.abs(freq - f) <= 5) return 'FT8';
  }
  for (const f of ft4Freqs) {
    if (Math.abs(freq - f) <= 5) return 'FT4';
  }
  // CW portions (lower part of bands)
  if ((freq >= 1800 && freq <= 1840) ||
      (freq >= 3500 && freq <= 3570) ||
      (freq >= 7000 && freq <= 7040) ||
      (freq >= 10100 && freq <= 10130) ||
      (freq >= 14000 && freq <= 14070) ||
      (freq >= 18068 && freq <= 18095) ||
      (freq >= 21000 && freq <= 21070) ||
      (freq >= 24890 && freq <= 24920) ||
      (freq >= 28000 && freq <= 28070)) {
    return 'CW';
  }
  // SSB portions (typically above CW/digital)
  if ((freq >= 1840 && freq <= 2000) ||
      (freq >= 3600 && freq <= 4000) ||
      (freq >= 7040 && freq <= 7300) ||
      (freq >= 14100 && freq <= 14350) ||
      (freq >= 18110 && freq <= 18168) ||
      (freq >= 21150 && freq <= 21450) ||
      (freq >= 24930 && freq <= 24990) ||
      (freq >= 28300 && freq <= 29700)) {
    return 'SSB';
  }
  return null;
};

// Custom cell renderer for DX callsign with status dot
const HOT_SPOT_COLOR = '#ff5533'; // Hot List override — always wins over category colors

const DxCallCellRenderer = (props: ICellRendererParams<Spot>) => {
  const status = props.data?.status;
  const settings = useSettingsStore.getState().settings;
  const spotStatusSettings = settings.spotStatus;
  const isStatusActive = status && spotStatusSettings.enabled && spotStatusSettings.show[status];
  const isHot = settings.hotList.enabled && props.data?.isHot;
  const color = isHot ? HOT_SPOT_COLOR : (isStatusActive ? spotStatusSettings.colors[status] : undefined);
  return (
    <span className="flex items-center gap-1.5">
      {color && <span className="inline-block w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />}
      <span className="font-mono font-bold text-accent-primary">{props.value}</span>
    </span>
  );
};

// Custom cell renderer for mode badges
const ModeCellRenderer = (props: ICellRendererParams<Spot>) => {
  let mode = props.value;
  const freq = props.data?.frequency;

  // Try to infer mode if not provided
  if (!mode && freq) {
    mode = inferModeFromFrequency(freq);
  }

  if (!mode) return <span className="text-dark-300">?</span>;

  // Normalize mode display
  const displayMode = (m: string) => {
    const upper = m.toUpperCase();
    if (upper === 'USB' || upper === 'LSB') return 'SSB';
    return upper;
  };

  const getModeClass = (mode: string) => {
    switch (mode?.toUpperCase()) {
      case 'CW': return 'badge-cw';
      case 'SSB':
      case 'USB':
      case 'LSB': return 'badge-ssb';
      case 'FT8':
      case 'FT4': return 'badge-ft8';
      case 'RTTY':
      case 'PSK31': return 'badge-rtty';
      default: return 'bg-dark-600 text-dark-200';
    }
  };
  return <span className={`badge text-xs ${getModeClass(mode)}`}>{displayMode(mode)}</span>;
};

// Custom cell renderer for frequency
const FrequencyCellRenderer = (props: ICellRendererParams<Spot>) => {
  return <span className="frequency-display font-mono text-accent-info text-sm">{formatFrequency(props.value)}</span>;
};

// Custom cell renderer for country flag
const FlagCellRenderer = (props: ICellRendererParams<Spot>) => {
  const country = props.data?.dxStation?.country || props.data?.country;
  return <span className="text-lg">{getCountryFlag(country)}</span>;
};

// Custom cell renderer for time with age
const TimeCellRenderer = (props: ICellRendererParams<Spot>) => {
  // Use timestamp field (API returns 'timestamp', not 'time')
  const time = props.data?.timestamp || props.value;
  const age = getAge(time);
  return (
    <div className="flex items-center gap-2 text-dark-300">
      <span className="font-mono">{formatTime(time)}</span>
      <span className="text-xs text-dark-400">({age})</span>
    </div>
  );
};

// Cluster connection status type
type ClusterStatusType = 'connected' | 'connecting' | 'disconnected' | 'error';

// Spot Status Color Config Component
function SpotStatusColorConfig({
  colors,
  enabled,
  show,
  dimWorked,
  onColorChange,
  onEnabledChange,
  onShowChange,
  onDimWorkedChange,
}: {
  colors: SpotStatusColors;
  enabled: boolean;
  show: SpotStatusEnabled;
  dimWorked: boolean;
  onColorChange: (key: keyof SpotStatusColors, value: string) => void;
  onEnabledChange: (enabled: boolean) => void;
  onShowChange: (key: keyof SpotStatusEnabled, value: boolean) => void;
  onDimWorkedChange: (dimWorked: boolean) => void;
}) {
  const colorEntries: { key: keyof SpotStatusColors; label: string }[] = [
    { key: 'newDxcc', label: 'New DXCC' },
    { key: 'newBand', label: 'New Band' },
    { key: 'worked', label: 'Worked' },
  ];

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h5 className="text-sm font-medium font-ui text-dark-200">Spot Status Colors</h5>
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="checkbox"
            checked={enabled}
            onChange={(e) => onEnabledChange(e.target.checked)}
            className="w-4 h-4 rounded border-glass-200 bg-dark-900 text-accent-primary focus:ring-accent-primary/40"
          />
          <span className="text-sm font-ui text-dark-300">Enabled</span>
        </label>
      </div>

      {enabled && (
        <>
          <div className="grid grid-cols-3 gap-3">
            {colorEntries.map(({ key, label }) => (
              <div key={key} className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={show[key]}
                  onChange={(e) => onShowChange(key, e.target.checked)}
                  className="w-3.5 h-3.5 rounded border-glass-200 bg-dark-900 text-accent-primary focus:ring-accent-primary/40"
                />
                <input
                  type="color"
                  value={colors[key]}
                  onChange={(e) => onColorChange(key, e.target.value)}
                  className="w-7 h-7 rounded border border-glass-100 bg-transparent cursor-pointer"
                  disabled={!show[key]}
                  style={{ opacity: show[key] ? 1 : 0.4 }}
                />
                <span className={`text-xs font-ui ${show[key] ? 'text-dark-300' : 'text-dark-500'}`}>{label}</span>
              </div>
            ))}
          </div>

          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              checked={dimWorked}
              onChange={(e) => onDimWorkedChange(e.target.checked)}
              className="w-4 h-4 rounded border-glass-200 bg-dark-900 text-accent-primary focus:ring-accent-primary/40"
            />
            <span className="text-sm font-ui text-dark-300">Dim worked spots</span>
          </label>
        </>
      )}
    </div>
  );
}

// Cluster Settings Panel Component
function ClusterSettingsPanel({
  connections,
  onUpdateConnection,
  onAddConnection,
  onRemoveConnection,
  onConnect,
  onDisconnect,
  statuses,
  stationCallsign,
}: {
  connections: ClusterConnection[];
  onUpdateConnection: (id: string, updates: Partial<ClusterConnection>) => void;
  onAddConnection: () => void;
  onRemoveConnection: (id: string) => void;
  onConnect: (id: string) => void;
  onDisconnect: (id: string) => void;
  statuses: Record<string, ClusterStatusType>;
  stationCallsign: string;
}) {
  const canAddMore = connections.length < 4;

  // Which cluster sends outbound spots (the "Send self-spots to" pick), shown/settable per cluster.
  const primarySpotClusterId = useSettingsStore((s) => s.settings.cluster.primarySpotClusterId);
  const updateClusterSettings = useSettingsStore((s) => s.updateClusterSettings);
  const setSpotCluster = (id: string) =>
    updateClusterSettings({ primarySpotClusterId: primarySpotClusterId === id ? '' : id });

  const getStatusColor = (status?: ClusterStatusType) => {
    switch (status) {
      case 'connected': return 'bg-accent-success';
      case 'connecting': return 'bg-accent-primary animate-pulse';
      case 'error': return 'bg-accent-danger';
      default: return 'bg-dark-400';
    }
  };

  const getStatusText = (status?: ClusterStatusType) => {
    switch (status) {
      case 'connected': return 'Connected';
      case 'connecting': return 'Connecting...';
      case 'error': return 'Error';
      default: return 'Disconnected';
    }
  };

  return (
    <div className="space-y-4">
      {connections.map((conn, index) => {
        const status = statuses[conn.id];
        const isConnected = status === 'connected';
        const isConnecting = status === 'connecting';

        return (
          <div
            key={conn.id}
            className="p-4 bg-dark-800/50 border border-glass-100 rounded-lg space-y-3"
          >
            {/* Header with status and remove button */}
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <div className={`w-2.5 h-2.5 rounded-full ${getStatusColor(status)}`} />
                <span className="text-sm font-medium font-ui text-dark-200">
                  {conn.name || `Cluster ${index + 1}`}
                </span>
                <span className="text-xs text-dark-300">
                  ({getStatusText(status)})
                </span>
              </div>
              <div className="flex items-center gap-1">
                {/* Outbound-spot cluster toggle. Only ONE cluster sends spots (the network peers
                    them anyway); click to make this the one, click again to unset. */}
                <button
                  onClick={() => setSpotCluster(conn.id)}
                  title={primarySpotClusterId === conn.id
                    ? 'This cluster sends your spots — click to unset'
                    : 'Send your spots from this cluster'}
                  className={`flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] uppercase tracking-wide transition-colors ${
                    primarySpotClusterId === conn.id
                      ? 'bg-accent-secondary/20 text-accent-secondary'
                      : 'text-dark-400 hover:text-dark-200'}`}
                >
                  <Send className="w-3 h-3" /> {primarySpotClusterId === conn.id ? 'Spots ✓' : 'Spots'}
                </button>
                <button
                  onClick={() => onRemoveConnection(conn.id)}
                  className="p-1 text-dark-300 hover:text-accent-danger transition-colors"
                  title="Remove cluster"
                >
                  <Trash2 className="w-4 h-4" />
                </button>
              </div>
            </div>

            {/* Configuration Grid */}
            <div className="grid grid-cols-2 gap-3">
              {/* Name */}
              <div>
                <label className="block text-xs font-ui text-dark-300 mb-1">Name</label>
                <input
                  type="text"
                  value={conn.name}
                  onChange={(e) => onUpdateConnection(conn.id, { name: e.target.value })}
                  placeholder="Cluster name"
                  className="w-full px-2 py-1.5 bg-dark-900 border border-glass-100 rounded text-sm text-dark-200 placeholder-dark-400 focus:outline-none focus:border-accent-primary/50"
                />
              </div>

              {/* Host */}
              <div>
                <label className="block text-xs font-ui text-dark-300 mb-1">Host</label>
                <input
                  type="text"
                  value={conn.host}
                  onChange={(e) => onUpdateConnection(conn.id, { host: e.target.value })}
                  placeholder="e.g., ve7cc.net"
                  className="w-full px-2 py-1.5 bg-dark-900 border border-glass-100 rounded text-sm text-dark-200 placeholder-dark-400 focus:outline-none focus:border-accent-primary/50"
                />
              </div>

              {/* Port */}
              <div>
                <label className="block text-xs font-ui text-dark-300 mb-1">Port</label>
                <input
                  type="number"
                  value={conn.port}
                  onChange={(e) => onUpdateConnection(conn.id, { port: parseInt(e.target.value) || 23 })}
                  className="w-full px-2 py-1.5 bg-dark-900 border border-glass-100 rounded text-sm font-mono text-dark-200 focus:outline-none focus:border-accent-primary/50"
                />
              </div>

              {/* Callsign */}
              <div>
                <label className="block text-xs font-ui text-dark-300 mb-1">
                  Callsign <span className="text-dark-400">(blank = station)</span>
                </label>
                <input
                  type="text"
                  value={conn.callsign || ''}
                  onChange={(e) => onUpdateConnection(conn.id, { callsign: e.target.value || null })}
                  placeholder={stationCallsign || 'Your callsign'}
                  className="w-full px-2 py-1.5 bg-dark-900 border border-glass-100 rounded text-sm font-mono text-dark-200 placeholder-dark-400 focus:outline-none focus:border-accent-primary/50"
                />
              </div>

              {/* Password */}
              <div>
                <label className="block text-xs font-ui text-dark-300 mb-1">
                  Password <span className="text-dark-400">(optional)</span>
                </label>
                <input
                  type="password"
                  value={conn.password || ''}
                  onChange={(e) => onUpdateConnection(conn.id, { password: e.target.value || null })}
                  placeholder="For closed clusters"
                  className="w-full px-2 py-1.5 bg-dark-900 border border-glass-100 rounded text-sm font-mono text-dark-200 placeholder-dark-400 focus:outline-none focus:border-accent-primary/50"
                />
              </div>
            </div>

            {/* Options Row */}
            <div className="flex items-center justify-between pt-2 border-t border-glass-100">
              <div className="flex items-center gap-4">
                {/* Enabled Toggle */}
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={conn.enabled}
                    onChange={(e) => onUpdateConnection(conn.id, { enabled: e.target.checked })}
                    className="w-4 h-4 rounded border-glass-200 bg-dark-900 text-accent-primary focus:ring-accent-primary/40"
                  />
                  <span className="text-sm font-ui text-dark-300">Enabled</span>
                </label>

                {/* Auto-Reconnect Toggle */}
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={conn.autoReconnect}
                    onChange={(e) => onUpdateConnection(conn.id, { autoReconnect: e.target.checked })}
                    className="w-4 h-4 rounded border-glass-200 bg-dark-900 text-accent-primary focus:ring-accent-primary/40"
                  />
                  <span className="text-sm font-ui text-dark-300">Auto-reconnect</span>
                </label>

                {/* CC11 (VE7CC extended format) — off = plain mode like most loggers, which some
                    nodes need to accept your outbound spots (no "-0" stream id). */}
                <label className="flex items-center gap-2 cursor-pointer"
                       title="VE7CC extended spot format. Turn OFF if your spots aren't posting — connects in plain mode like most loggers (no “-0” on your call).">
                  <input
                    type="checkbox"
                    checked={conn.cc11Mode !== false}
                    onChange={(e) => onUpdateConnection(conn.id, { cc11Mode: e.target.checked })}
                    className="w-4 h-4 rounded border-glass-200 bg-dark-900 text-accent-primary focus:ring-accent-primary/40"
                  />
                  <span className="text-sm font-ui text-dark-300">VE7CC extended (CC11)</span>
                </label>
              </div>

              {/* Connect/Disconnect Button */}
              <button
                onClick={() => isConnected ? onDisconnect(conn.id) : onConnect(conn.id)}
                disabled={isConnecting || !conn.host}
                className={`
                  px-3 py-1.5 rounded text-sm font-medium font-ui transition-colors
                  ${isConnected
                    ? 'bg-accent-danger/20 text-accent-danger hover:bg-accent-danger/30'
                    : 'bg-accent-primary/20 text-accent-primary hover:bg-accent-primary/30'
                  }
                  disabled:opacity-50 disabled:cursor-not-allowed
                `}
              >
                {isConnecting ? 'Connecting...' : isConnected ? 'Disconnect' : 'Connect'}
              </button>
            </div>
          </div>
        );
      })}

      {/* Add Cluster Button */}
      {canAddMore && (
        <button
          onClick={onAddConnection}
          className="w-full flex items-center justify-center gap-2 px-4 py-3 border-2 border-dashed border-glass-100 rounded-lg text-dark-300 hover:border-accent-primary/50 hover:text-accent-primary transition-colors font-ui"
        >
          <Plus className="w-4 h-4" />
          Add Cluster (max 4)
        </button>
      )}

      {connections.length === 0 && (
        <div className="text-center py-6 text-dark-300">
          <p className="mb-2 font-ui">No cluster connections configured</p>
          <button
            onClick={onAddConnection}
            className="inline-flex items-center gap-2 px-4 py-2 bg-accent-primary/20 text-accent-primary rounded-lg hover:bg-accent-primary/30 transition-colors font-ui"
          >
            <Plus className="w-4 h-4" />
            Add your first cluster
          </button>
        </div>
      )}
    </div>
  );
}

export function ClusterPlugin() {
  const { onGridReady, onColumnChanged, onSortChanged } = useAgGridState('cluster');
  const { selectSpot } = useSignalR();
  const [selectedBands, setSelectedBands] = useState<string[]>([]);
  const [selectedModes, setSelectedModes] = useState<string[]>([]);
  const [selectedStatuses, setSelectedStatuses] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  // "More filters" (collapsible) — the geo/source set restored from v1, all filtering
  // over data cty.dat already resolves onto each spot, so no backend involvement.
  const [showMoreFilters, setShowMoreFilters] = useState(false);
  const [selectedSpotterContinents, setSelectedSpotterContinents] = useState<string[]>([]);
  const [selectedDxContinents, setSelectedDxContinents] = useState<string[]>([]);
  const [selectedSources, setSelectedSources] = useState<string[]>([]);
  const [cqZoneFilter, setCqZoneFilter] = useState('');
  const [showSettings, setShowSettings] = useState(false);

  // Get spots from app store (ephemeral, in-memory only)
  const spots = useAppStore((state) => state.dxClusterSpots);
  const clearDxClusterSpots = useAppStore((state) => state.clearDxClusterSpots);
  const pruneStaleDxClusterSpots = useAppStore((state) => state.pruneStaleDxClusterSpots);

  // Age-filter enforcement: v1 SDRLogger+ ran pruneSpots() every 30 s so
  // expired spots dropped off even when no new spot arrived. Same here.
  useEffect(() => {
    const id = setInterval(pruneStaleDxClusterSpots, 30_000);
    return () => clearInterval(id);
  }, [pruneStaleDxClusterSpots]);

  // Rig state for "follow rig" tracking
  const selectedRadioId = useAppStore((state) => state.selectedRadioId);
  const radioStates = useAppStore((state) => state.radioStates);
  const rigState = selectedRadioId ? radioStates.get(selectedRadioId) : undefined;
  const rigConnected = !!rigState;
  const rigFreqHz = rigState?.frequencyHz;
  const rigMode = rigState?.mode;

  // DX Cluster map overlay state from appStore
  const dxClusterMapEnabled = useAppStore((state) => state.dxClusterMapEnabled);
  const setDxClusterMapEnabled = useAppStore((state) => state.setDxClusterMapEnabled);
  const setHoveredSpotId = useAppStore((state) => state.setHoveredSpotId);

  // Get cluster settings from store
  const {
    settings,
    updateClusterConnection,
    addClusterConnection,
    removeClusterConnection,
    updateClusterSettings,
    updateSpotStatusSettings,
    saveSettings,
  } = useSettingsStore();

  const clusterConnections = settings.cluster.connections;
  // "Follow rig" is two INDEPENDENT toggles: follow the rig's BAND and/or its
  // MODE. Either or both — so you can chase the whole band, one mode across all
  // bands, or one mode on one band. Each only bites with a rig connected.
  const followBand = settings.cluster.followRigBand;
  const followMode = settings.cluster.followRigMode;
  const bandTracking = followBand && rigConnected;
  const modeTracking = followMode && rigConnected;
  const stationCallsign = settings.station.callsign;
  const spotStatusSettings = settings.spotStatus;
  const spotStatusEnabled = spotStatusSettings.enabled;
  const spotStatusColors = spotStatusSettings.colors;
  const spotStatusShow = spotStatusSettings.show;

  // Get cluster statuses from app store (populated via SignalR)
  const clusterStatusesFromStore = useAppStore((state) => state.clusterStatuses);

  // Convert to simple status map for the settings panel
  const clusterStatuses = useMemo(() => {
    const statuses: Record<string, ClusterStatusType> = {};
    for (const [id, status] of Object.entries(clusterStatusesFromStore)) {
      statuses[id] = status.status;
    }
    return statuses;
  }, [clusterStatusesFromStore]);

  // CQ-zone filter parsed to a set of numbers, e.g. "3, 4,5" -> [3,4,5].
  const cqZones = useMemo(
    () => cqZoneFilter.split(',').map(s => parseInt(s.trim(), 10)).filter(n => !isNaN(n)),
    [cqZoneFilter]);

  // Source options built from the sources actually present, so the dropdown always
  // matches the connected feeds (Cluster / RBN / POTA / SpotHole / …).
  const sourceOptions = useMemo<MultiSelectOption[]>(() => {
    const seen = [...new Set((spots ?? []).map(s => s.source).filter(Boolean) as string[])].sort();
    return seen.map(s => ({ value: s, label: s }));
  }, [spots]);

  // Filter spots based on selected bands, modes, statuses, and search query.
  // When "follow rig" is active, the rig's live band/mode override the manual
  // Band/Mode dropdowns (the manual selections are preserved but ignored).
  const filteredSpots = useMemo(() => {
    if (!spots) return [];

    const query = searchQuery.trim().toLowerCase();

    const rigBand = bandTracking ? getBandFromFrequency((rigFreqHz ?? 0) / 1000) : null;
    const rigModes = modeTracking ? rigModeToSpotModes(rigMode) : null;

    // Band: Follow Band on -> restrict to the rig's band ('?' = rig outside any
    // known band -> skip rather than show nothing); else the manual Band dropdown.
    const effectiveBands = bandTracking
      ? (rigBand && rigBand !== '?' ? [rigBand] : [])
      : selectedBands;
    // Mode: Follow Mode on -> restrict to the rig's mode(s) (null = unknown rig
    // mode -> skip); else the manual Mode dropdown. Fully independent of band.
    const effectiveModes = modeTracking
      ? (rigModes ?? [])
      : selectedModes;

    return spots.filter(spot => {
      // Fuzzy search filter - matches against multiple fields
      if (query) {
        const searchableText = [
          spot.dxCall,
          spot.spotter,
          spot.dxStation?.country || spot.country,
          spot.comment,
          getBandFromFrequency(spot.frequency),
          spot.mode,
        ].filter(Boolean).join(' ').toLowerCase();

        if (!searchableText.includes(query)) return false;
      }

      // Band filter
      if (effectiveBands.length > 0) {
        const band = getBandFromFrequency(spot.frequency);
        if (!effectiveBands.includes(band)) return false;
      }

      // Mode filter
      if (effectiveModes.length > 0) {
        let spotMode = spot.mode?.toUpperCase();
        // Normalize USB/LSB to SSB
        if (spotMode === 'USB' || spotMode === 'LSB') spotMode = 'SSB';
        // Try to infer mode if not provided
        if (!spotMode) {
          spotMode = inferModeFromFrequency(spot.frequency)?.toUpperCase() || undefined;
        }
        if (!spotMode || !effectiveModes.includes(spotMode)) return false;
      }

      // Status filter
      if (selectedStatuses.length > 0) {
        const spotStatus = spot.status || 'none';
        if (!selectedStatuses.includes(spotStatus)) return false;
      }

      // Spotter continent — "who's hearing this, and from where".
      if (selectedSpotterContinents.length > 0) {
        const c = spot.spotterStation?.continent;
        if (!c || !selectedSpotterContinents.includes(c)) return false;
      }

      // DX-station continent.
      if (selectedDxContinents.length > 0) {
        const c = spot.dxStation?.continent;
        if (!c || !selectedDxContinents.includes(c)) return false;
      }

      // Source (Cluster / RBN / POTA / …).
      if (selectedSources.length > 0) {
        if (!spot.source || !selectedSources.includes(spot.source)) return false;
      }

      // DX CQ zone — comma-separated list, e.g. "3,4,5".
      if (cqZones.length > 0) {
        if (spot.cqZone == null || !cqZones.includes(spot.cqZone)) return false;
      }

      return true;
    });
  }, [spots, selectedBands, selectedModes, selectedStatuses, searchQuery, bandTracking, modeTracking, rigFreqHz, rigMode,
      selectedSpotterContinents, selectedDxContinents, selectedSources, cqZones]);

  // Row style callback for status coloring
  const hotListEnabled = useSettingsStore(state => state.settings.hotList.enabled);
  // Station position drives the Dist/Bearing columns; re-subscribe so editing
  // the grid square in Settings refreshes them without a reload.
  const station = useSettingsStore(state => state.settings.station);
  const getRowStyle = useCallback((params: { data?: Spot }): RowStyle | undefined => {
    // Hot-list callsigns always win — they override all category colors
    if (hotListEnabled && params.data?.isHot) {
      return { backgroundColor: `${HOT_SPOT_COLOR}25` };
    }
    if (!spotStatusEnabled) return undefined;
    const status = params.data?.status;
    if (!status || !spotStatusShow[status]) return undefined;
    const color = spotStatusColors[status];
    if (!color) return undefined;

    const style: RowStyle = { backgroundColor: `${color}20` }; // 12% opacity
    if (status === 'worked' && spotStatusSettings.dimWorked) {
      style.opacity = '0.6';
    }
    return style;
  }, [hotListEnabled, spotStatusEnabled, spotStatusColors, spotStatusShow, spotStatusSettings.dimWorked]);

  const handleRowClick = async (event: RowClickedEvent<Spot>) => {
    const spot = event.data;
    if (spot) {
      await selectSpot(spot.dxCall, spot.frequency, spot.mode);
    }
  };

  const handleUpdateConnection = (id: string, updates: Partial<ClusterConnection>) => {
    updateClusterConnection(id, updates);
  };

  const handleAddConnection = () => {
    addClusterConnection();
  };

  const handleRemoveConnection = (id: string) => {
    removeClusterConnection(id);
  };

  const handleConnect = async (id: string) => {
    // Save settings first, then trigger connect via API
    await saveSettings();
    try {
      await fetch(`/api/cluster/connect/${id}`, { method: 'POST' });
    } catch (error) {
      console.error('Failed to connect cluster:', error);
    }
  };

  const handleDisconnect = async (id: string) => {
    try {
      await fetch(`/api/cluster/disconnect/${id}`, { method: 'POST' });
    } catch (error) {
      console.error('Failed to disconnect cluster:', error);
    }
  };

  const clearAllFilters = () => {
    setSelectedBands([]);
    setSelectedModes([]);
    setSelectedStatuses([]);
    setSearchQuery('');
    setSelectedSpotterContinents([]);
    setSelectedDxContinents([]);
    setSelectedSources([]);
    setCqZoneFilter('');
  };

  const handleToggleFollowBand = () => {
    updateClusterSettings({ followRigBand: !followBand });
    saveSettings();
  };
  const handleToggleFollowMode = () => {
    updateClusterSettings({ followRigMode: !followMode });
    saveSettings();
  };

  const moreFilterCount = selectedSpotterContinents.length + selectedDxContinents.length + selectedSources.length + (cqZones.length > 0 ? 1 : 0);
  const hasActiveFilters = selectedBands.length > 0 || selectedModes.length > 0 || selectedStatuses.length > 0 || searchQuery.trim().length > 0 || moreFilterCount > 0;
  const totalActiveFilters = selectedBands.length + selectedModes.length + selectedStatuses.length + (searchQuery.trim() ? 1 : 0) + moreFilterCount;

  // Count connected clusters
  const connectedCount = Object.values(clusterStatuses).filter(s => s === 'connected').length;

  const columnDefs = useMemo<ColDef<Spot>[]>(() => [
    {
      headerName: 'Time',
      field: 'timestamp',
      cellRenderer: TimeCellRenderer,
      width: 110,
      resizable: true,
    },
    {
      headerName: 'DX Call',
      field: 'dxCall',
      cellRenderer: DxCallCellRenderer,
      width: 110,
      resizable: true,
    },
    {
      headerName: 'Freq',
      field: 'frequency',
      cellRenderer: FrequencyCellRenderer,
      width: 90,
      resizable: true,
    },
    {
      headerName: 'Band',
      field: 'frequency',
      valueGetter: (params) => getBandFromFrequency(params.data?.frequency || 0),
      cellClass: 'font-mono text-dark-300',
      width: 60,
      resizable: true,
    },
    {
      headerName: 'Mode',
      field: 'mode',
      cellRenderer: ModeCellRenderer,
      width: 70,
      resizable: true,
    },
    {
      headerName: '',
      field: 'country',
      cellRenderer: FlagCellRenderer,
      width: 45,
      resizable: false,
      sortable: false,
    },
    {
      headerName: 'Country',
      valueGetter: (params) => params.data?.dxStation?.country || params.data?.country || '-',
      cellClass: 'text-dark-300',
      width: 100,
      resizable: true,
    },
    {
      headerName: 'Dist',
      colId: 'distance',
      headerTooltip: 'Great-circle distance from your station (km). "~" = country centroid, not a reported grid.',
      // The cell value is the whole SpotGeo so the formatter can show the
      // approximate marker without recomputing; sorting uses the raw km.
      valueGetter: (params) => computeSpotGeo(station, params.data),
      valueFormatter: (params) => formatDistance(params.value as SpotGeo | null),
      comparator: (a: SpotGeo | null, b: SpotGeo | null) =>
        (a?.distanceKm ?? Infinity) - (b?.distanceKm ?? Infinity),
      cellClass: 'font-mono text-dark-300 text-right',
      width: 75,
      resizable: true,
    },
    {
      headerName: 'Brg',
      colId: 'bearing',
      headerTooltip: 'Beam heading from your station, degrees true',
      valueGetter: (params) => computeSpotGeo(station, params.data),
      valueFormatter: (params) => formatBearing(params.value as SpotGeo | null),
      comparator: (a: SpotGeo | null, b: SpotGeo | null) =>
        (a?.bearingDeg ?? Infinity) - (b?.bearingDeg ?? Infinity),
      cellClass: 'font-mono text-dark-300 text-right',
      width: 65,
      resizable: true,
    },
    {
      headerName: 'Spotter',
      field: 'spotter',
      cellClass: 'font-mono text-dark-300',
      width: 120,
      resizable: true,
      valueGetter: (params) => {
        const spotter = params.data?.spotter || '-';
        const grid = params.data?.spotterStation?.grid;
        return grid ? `${spotter} (${grid})` : spotter;
      },
    },
    {
      headerName: 'Comment',
      field: 'comment',
      cellClass: 'text-dark-300 truncate',
      flex: 1,
      minWidth: 100,
      resizable: true,
    },
  ], [station]);

  const defaultColDef = useMemo<ColDef>(() => ({
    sortable: true,
    resizable: true,
    filter: true,
    enableRowGroup: false,
  }), []);

  return (
    <GlassPanel
      title="DX Cluster"
      icon={<RadioTower className="w-5 h-5" />}
      actions={
        <div className="flex items-center gap-2">
          {/* Connected clusters indicator */}
          {clusterConnections.length > 0 && (
            <span className="text-xs font-ui text-dark-300">
              {connectedCount}/{clusterConnections.length} connected
            </span>
          )}
          {/* Age filter — v1 SDRLogger+ parity. Compact dropdown; the
              actual filter/prune lives on the app store so it also
              affects the map overlay + any other spot consumer. */}
          <select
            value={settings.cluster.spotAgeMinutes}
            onChange={(e) => {
              const v = parseInt(e.target.value, 10);
              if (!Number.isNaN(v)) updateClusterSettings({ spotAgeMinutes: v });
            }}
            className="glass-input text-xs font-mono px-1 py-0.5"
            title="Show spots from the last N minutes"
          >
            <option value={5}>5 min</option>
            <option value={10}>10 min</option>
            <option value={15}>15 min</option>
            <option value={30}>30 min</option>
            <option value={60}>60 min</option>
          </select>
          <span className="text-sm font-mono text-dark-300">
            {filteredSpots?.length || 0} spots
          </span>
          {spots.length > 0 && (
            <button
              onClick={clearDxClusterSpots}
              className="glass-button p-1.5 text-dark-300 hover:text-accent-warning"
              title="Clear all spots"
            >
              <Eraser className="w-4 h-4" />
            </button>
          )}
          <button
            onClick={() => setDxClusterMapEnabled(!dxClusterMapEnabled)}
            className={`glass-button p-1.5 ${dxClusterMapEnabled ? 'text-accent-info' : 'text-dark-300'}`}
            title={dxClusterMapEnabled ? 'Hide map overlay' : 'Show map overlay'}
          >
            <Map className="w-4 h-4" />
          </button>
          <button
            onClick={() => setShowSettings(!showSettings)}
            className={`glass-button p-1.5 ${showSettings ? 'text-accent-primary' : 'text-dark-300'}`}
            title="Cluster settings"
          >
            <Settings className="w-4 h-4" />
          </button>
        </div>
      }
    >
      <div className="flex flex-col h-full relative">
        {/* Settings Overlay Panel — covers the full panel area, scrollable */}
        <div
          className={`
            absolute inset-0 z-20 flex flex-col
            bg-dark-900
            transition-opacity duration-200
            ${showSettings ? 'opacity-100 pointer-events-auto' : 'opacity-0 pointer-events-none'}
          `}
        >
          {/* Overlay Header */}
          <div className="flex items-center justify-between px-4 py-3 border-b border-glass-100 flex-shrink-0">
            <h4 className="text-sm font-medium font-ui text-dark-200">Cluster Settings</h4>
            <button
              onClick={() => setShowSettings(false)}
              className="p-1.5 text-dark-300 hover:text-dark-200 hover:bg-dark-700 rounded transition-colors"
              title="Close settings"
            >
              <X className="w-4 h-4" />
            </button>
          </div>

          {/* Scrollable Settings Content */}
          <div className="flex-1 overflow-y-auto p-4 space-y-6">
            {/* Spot Status Colors Section */}
            <SpotStatusColorConfig
              colors={spotStatusColors}
              enabled={spotStatusEnabled}
              show={spotStatusShow}
              dimWorked={spotStatusSettings.dimWorked}
              onColorChange={(key, value) => {
                updateSpotStatusSettings({ colors: { ...spotStatusColors, [key]: value } });
              }}
              onEnabledChange={(enabled) => updateSpotStatusSettings({ enabled })}
              onShowChange={(key, value) => {
                updateSpotStatusSettings({ show: { ...spotStatusShow, [key]: value } });
              }}
              onDimWorkedChange={(dimWorked) => updateSpotStatusSettings({ dimWorked })}
            />

            <div className="border-t border-glass-100" />

            {/* Spothole.app aggregator (default source) */}
            <div>
              <h5 className="text-sm font-medium font-ui text-dark-200 mb-3">Spothole.app</h5>
              <div className="space-y-3 p-3 bg-dark-700/50 rounded-lg border border-glass-100">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-dark-200">Spothole.app spots</p>
                    <p className="text-xs text-dark-400 mt-0.5">
                      Public REST aggregator (clusters, POTA, SOTA, RBN) — no login needed, read-only
                    </p>
                  </div>
                  <button
                    onClick={() => updateClusterSettings({ spotholeEnabled: !settings.cluster.spotholeEnabled })}
                    className={`relative w-11 h-6 rounded-full transition-colors ${settings.cluster.spotholeEnabled ? 'bg-accent-primary' : 'bg-dark-500'}`}
                  >
                    <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${settings.cluster.spotholeEnabled ? 'translate-x-5' : ''}`} />
                  </button>
                </div>
                <div className="flex items-center gap-3">
                  <label className="text-xs font-ui text-dark-300 shrink-0">Spotter country</label>
                  <input
                    type="text"
                    value={settings.cluster.spotholeSpotterCountry}
                    onChange={(e) => updateClusterSettings({ spotholeSpotterCountry: e.target.value })}
                    placeholder="United States (empty = all)"
                    className="glass-input flex-1 text-sm"
                  />
                </div>
                <p className="text-xs text-dark-400">
                  Only spots heard by spotters in this country are shown (cty.dat names, e.g. "United States").
                  Clear the field to receive spots from all spotters worldwide.
                </p>
              </div>
            </div>

            <div className="border-t border-glass-100" />

            {/* Outbound-spot cluster picker. The DX cluster network peers
                clusters together and relays spots upstream, so posting the
                same spot to multiple clusters gets your call flagged as a
                duplicate source. Pick ONE cluster to spot on. */}
            <div>
              <h5 className="text-sm font-medium font-ui text-dark-200 mb-3">Max Spots</h5>
              <div className="space-y-2 p-3 bg-dark-700/50 rounded-lg border border-glass-100 mb-4">
                <div className="flex items-center gap-3">
                  <label className="text-xs font-ui text-dark-300 shrink-0">Keep the last</label>
                  <input
                    type="number"
                    min={50}
                    max={300}
                    step={10}
                    value={settings.cluster.maxSpots}
                    onChange={(e) => {
                      const v = parseInt(e.target.value, 10);
                      if (!Number.isNaN(v)) {
                        updateClusterSettings({ maxSpots: Math.max(50, Math.min(300, v)) });
                      }
                    }}
                    className="glass-input w-24 text-sm font-mono"
                  />
                  <span className="text-xs font-ui text-dark-300">spots in memory (50–300)</span>
                </div>
                <p className="text-xs text-dark-400">
                  Both the in-memory spot list and the backend replay buffer honour this limit. Also
                  drives the age filter dropdown in the panel header — spots older than that fall off.
                </p>
              </div>

              <h5 className="text-sm font-medium font-ui text-dark-200 mb-3">Outbound Spots</h5>
              <div className="space-y-2 p-3 bg-dark-700/50 rounded-lg border border-glass-100">
                <label className="text-xs font-ui text-dark-300 block">Send self-spots to</label>
                <select
                  value={settings.cluster.primarySpotClusterId}
                  onChange={(e) => updateClusterSettings({ primarySpotClusterId: e.target.value })}
                  className="glass-input w-full text-sm"
                >
                  <option value="">Auto (only works when a single cluster is connected)</option>
                  {clusterConnections.map((c) => (
                    <option key={c.id} value={c.id}>{c.name || `${c.host}:${c.port}`}</option>
                  ))}
                </select>
                <p className="text-xs text-dark-400">
                  The cluster network peers all clusters and relays spots upstream, so posting to more than
                  one is redundant and can get your call flagged. Pick one. SpotHole isn't in the list —
                  it's a read-only aggregator that doesn't accept submitted spots.
                </p>
              </div>

              {/* Push spots to the connected TCI radio (Lyra / Thetis) */}
              <div className="space-y-2 p-3 mt-3 bg-dark-700/50 rounded-lg border border-glass-100">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-dark-200">Push spots to TCI radio</p>
                    <p className="text-xs text-dark-400 mt-0.5">
                      Mirror received DX spots onto the connected TCI radio's panadapter (Lyra / Thetis)
                      as coloured click-to-tune markers. Colour follows worked-before status.
                    </p>
                  </div>
                  <button
                    onClick={() => updateClusterSettings({ pushSpotsToTci: !settings.cluster.pushSpotsToTci })}
                    className={`relative w-11 h-6 rounded-full transition-colors shrink-0 ${settings.cluster.pushSpotsToTci ? 'bg-accent-primary' : 'bg-dark-500'}`}
                  >
                    <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform ${settings.cluster.pushSpotsToTci ? 'translate-x-5' : ''}`} />
                  </button>
                </div>
              </div>
            </div>

            <div className="border-t border-glass-100" />

            {/* Cluster Connections Section */}
            <div>
              <h5 className="text-sm font-medium font-ui text-dark-200 mb-3">Cluster Connections</h5>
              <ClusterSettingsPanel
                connections={clusterConnections}
                onUpdateConnection={handleUpdateConnection}
                onAddConnection={handleAddConnection}
                onRemoveConnection={handleRemoveConnection}
                onConnect={handleConnect}
                onDisconnect={handleDisconnect}
                statuses={clusterStatuses}
                stationCallsign={stationCallsign}
              />
            </div>
          </div>

          {/* Sticky Save Footer — always visible regardless of content height */}
          <div className="flex-shrink-0 flex items-center justify-between px-4 py-3 border-t border-glass-100 bg-dark-900">
            <span className="text-xs text-dark-400 font-ui">Changes take effect after saving</span>
            <button
              onClick={async () => { await saveSettings(); setShowSettings(false); }}
              className="px-4 py-2 bg-accent-primary text-white rounded-lg text-sm font-medium font-ui hover:bg-accent-primary/80 transition-colors"
            >
              Save &amp; Close
            </button>
          </div>
        </div>

        {/* Filters */}
        <div className="p-4 space-y-4 flex-shrink-0">
          <div className="flex gap-3 items-center">
            {/* Fuzzy Search Input */}
            <div className="relative flex-1">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-dark-300" />
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Search call, country, spotter..."
                className="w-full pl-9 pr-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 placeholder-dark-400 focus:outline-none focus:border-accent-primary/50"
              />
              {searchQuery && (
                <button
                  onClick={() => setSearchQuery('')}
                  className="absolute right-2 top-1/2 -translate-y-1/2 p-1 text-dark-300 hover:text-dark-200"
                >
                  <X className="w-3.5 h-3.5" />
                </button>
              )}
            </div>

            {/* Follow rig — two INDEPENDENT toggles (Band and/or Mode). Cyan when
                actively filtering (rig connected), amber when armed but no rig,
                gray when off. */}
            <div className="flex items-center gap-1.5 whitespace-nowrap">
              <span className="text-xs text-dark-300 font-ui flex items-center gap-1">
                <Crosshair className="w-4 h-4" /> Follow rig:
              </span>
              {([
                { key: 'band', label: 'Band', on: followBand, active: bandTracking, onClick: handleToggleFollowBand },
                { key: 'mode', label: 'Mode', on: followMode, active: modeTracking, onClick: handleToggleFollowMode },
              ] as const).map((p) => (
                <button
                  key={p.key}
                  onClick={p.onClick}
                  title={
                    p.on
                      ? (rigConnected
                          ? `Following rig ${p.key} — click to stop`
                          : `Follow ${p.key} armed — waiting for a connected rig`)
                      : `Follow the rig's ${p.key}`
                  }
                  className={`px-2.5 py-2 rounded-lg text-sm font-ui border transition-colors ${
                    p.active
                      ? 'bg-accent-primary/20 text-accent-primary border-accent-primary/30'
                      : p.on
                        ? 'bg-accent-warning/15 text-accent-warning border-accent-warning/40'
                        : 'bg-dark-800 text-dark-300 border-glass-100 hover:text-dark-200'
                  }`}
                >
                  {p.label}
                </button>
              ))}
            </div>

            <MultiSelectDropdown
              options={BAND_OPTIONS}
              selected={selectedBands}
              onChange={setSelectedBands}
              placeholder="All Bands"
              className="w-32"
              disabled={bandTracking}
              title={bandTracking ? 'Following rig band' : undefined}
            />

            <MultiSelectDropdown
              options={MODE_OPTIONS}
              selected={selectedModes}
              onChange={setSelectedModes}
              placeholder="All Modes"
              className="w-32"
              disabled={modeTracking}
              title={modeTracking ? 'Following rig mode' : undefined}
            />

            <MultiSelectDropdown
              options={STATUS_OPTIONS}
              selected={selectedStatuses}
              onChange={setSelectedStatuses}
              placeholder="All Status"
              className="w-32"
            />

            {/* More filters — spotter/DX continent, source, CQ zone. Collapsed by default
                to keep the row clean; the badge shows how many are active while hidden. */}
            <button
              onClick={() => setShowMoreFilters(v => !v)}
              className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm font-ui border transition-colors whitespace-nowrap ${
                showMoreFilters || moreFilterCount > 0
                  ? 'bg-accent-primary/15 text-accent-primary border-accent-primary/30'
                  : 'bg-dark-800 text-dark-300 border-glass-100 hover:text-dark-200'
              }`}
              title="More spot filters"
            >
              <SlidersHorizontal className="w-4 h-4" />
              <span>Filters{moreFilterCount > 0 ? ` (${moreFilterCount})` : ''}</span>
              <ChevronDown className={`w-3.5 h-3.5 transition-transform ${showMoreFilters ? 'rotate-180' : ''}`} />
            </button>

            {/* Clear All Filters Button */}
            {hasActiveFilters && (
              <button
                onClick={clearAllFilters}
                className="flex items-center gap-1.5 px-3 py-2 bg-accent-warning/20 text-accent-warning rounded-lg text-sm font-ui hover:bg-accent-warning/30 transition-colors whitespace-nowrap"
                title="Clear all filters"
              >
                <X className="w-4 h-4" />
                <span>Clear ({totalActiveFilters})</span>
              </button>
            )}
          </div>

          {/* Collapsible "more filters" row — geo/source set restored from v1. */}
          {showMoreFilters && (
            <div className="flex flex-wrap items-center gap-2 mt-2 pt-2 border-t border-glass-100">
              <MultiSelectDropdown
                options={CONTINENT_OPTIONS}
                selected={selectedSpotterContinents}
                onChange={setSelectedSpotterContinents}
                placeholder="Spotter continent"
                className="w-44"
              />
              <MultiSelectDropdown
                options={CONTINENT_OPTIONS}
                selected={selectedDxContinents}
                onChange={setSelectedDxContinents}
                placeholder="DX continent"
                className="w-40"
              />
              <MultiSelectDropdown
                options={sourceOptions}
                selected={selectedSources}
                onChange={setSelectedSources}
                placeholder="Source"
                className="w-36"
              />
              <input
                type="text"
                inputMode="numeric"
                value={cqZoneFilter}
                onChange={(e) => setCqZoneFilter(e.target.value)}
                placeholder="CQ zones e.g. 3,4,5"
                className="w-40 px-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-ui placeholder:text-dark-400 focus:outline-none focus:border-accent-primary/50"
                title="Filter by DX-station CQ zone — comma-separated"
              />
            </div>
          )}
        </div>

        {/* AG Grid Table */}
        <div className="flex-1 min-h-0 px-4 pb-4">
          <div className="ag-theme-alpine-dark h-full">
            {filteredSpots?.length === 0 ? (
              <div className="text-center py-8 text-dark-300">
                {hasActiveFilters ? (
                  <>
                    <p>No spots match your filters</p>
                    <button
                      onClick={clearAllFilters}
                      className="mt-2 text-accent-primary hover:underline font-ui"
                    >
                      Clear filters
                    </button>
                  </>
                ) : (
                  'No spots available'
                )}
              </div>
            ) : (
              <AgGridReact<Spot>
                rowData={filteredSpots}
                columnDefs={columnDefs}
                defaultColDef={defaultColDef}
                rowHeight={36}
                headerHeight={40}
                suppressCellFocus={true}
                animateRows={true}
                onRowClicked={handleRowClick}
                getRowStyle={getRowStyle}
                onCellMouseOver={(event: CellMouseOverEvent<Spot>) => {
                  if (event.data?.id) setHoveredSpotId(event.data.id);
                }}
                onCellMouseOut={(_event: CellMouseOutEvent<Spot>) => {
                  setHoveredSpotId(null);
                }}
                rowClass="cursor-pointer hover:bg-dark-600/50"
                getRowId={(params) => params.data.id}
                suppressMenuHide={true}
                onGridReady={onGridReady}
                onColumnMoved={onColumnChanged}
                onColumnResized={onColumnChanged}
                onColumnVisible={onColumnChanged}
                onColumnPinned={onColumnChanged}
                onSortChanged={onSortChanged}
              />
            )}
          </div>
        </div>
      </div>
    </GlassPanel>
  );
}

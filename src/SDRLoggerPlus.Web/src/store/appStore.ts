import { create } from 'zustand';
import { useSettingsStore } from './settingsStore';
import type {
  CallsignLookedUpEvent,
  RotatorPositionEvent,
  RigStatusEvent,
  AntennaGeniusStatusEvent,
  AntennaGeniusPortStatus,
  PgxlStatusEvent,
  TunerGeniusStatusEvent,
  TunerGeniusPortStatus,
  TunerGeniusPortChangedEvent,
  RadioDiscoveredEvent,
  RadioConnectionState,
  RadioStateChangedEvent,
  SpotSelectedEvent,
  CwKeyerStatusEvent,
} from '../api/signalr';
import type { PotaSpot, CallsignMapImage } from '../api/client';

// Connection state enum for detailed tracking
// - disconnected: No connection to backend
// - connecting: Initial connection attempt
// - reconnecting: Attempting to reconnect after disconnect
// - rehydrating: Connected, but reloading all data (settings, device states, etc.)
// - connected: Fully connected and all data loaded
export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting' | 'rehydrating';

interface AppState {
  // Connection status
  isConnected: boolean;
  connectionState: ConnectionState;
  reconnectAttempt: number;
  databaseConnected: boolean;
  setConnected: (connected: boolean) => void;
  setConnectionState: (state: ConnectionState, attempt?: number) => void;
  setDatabaseConnected: (connected: boolean) => void;

  // Current focused callsign
  focusedCallsign: string | null;
  focusedCallsignInfo: CallsignLookedUpEvent | null;
  isLookingUpCallsign: boolean;
  setFocusedCallsign: (callsign: string | null) => void;
  setFocusedCallsignInfo: (info: CallsignLookedUpEvent | null) => void;
  setLookingUpCallsign: (loading: boolean) => void;

  // Rotator
  rotatorPosition: RotatorPositionEvent | null;
  setRotatorPosition: (position: RotatorPositionEvent | null) => void;

  // Rig
  rigStatus: RigStatusEvent | null;
  setRigStatus: (status: RigStatusEvent | null) => void;

  // Station info
  stationCallsign: string;
  stationGrid: string;
  setStationInfo: (callsign: string, grid: string) => void;

  // Antenna Genius
  antennaGeniusDevices: Map<string, AntennaGeniusStatusEvent>;
  setAntennaGeniusStatus: (status: AntennaGeniusStatusEvent) => void;
  updateAntennaGeniusPort: (serial: string, portStatus: AntennaGeniusPortStatus) => void;
  removeAntennaGeniusDevice: (serial: string) => void;

  // PGXL Amplifier
  pgxlDevices: Map<string, PgxlStatusEvent>;
  setPgxlStatus: (status: PgxlStatusEvent) => void;
  removePgxlDevice: (serial: string) => void;
  // PGXL-TCI linking: radio IDs linked to PGXL sides A and B
  pgxlTciLinkA: string | null;
  pgxlTciLinkB: string | null;
  setPgxlTciLink: (side: 'A' | 'B', radioId: string | null) => void;

  // Tuner Genius
  tunerGeniusDevices: Map<string, TunerGeniusStatusEvent>;
  setTunerGeniusStatus: (status: TunerGeniusStatusEvent) => void;
  updateTunerGeniusPort: (evt: TunerGeniusPortChangedEvent) => void;
  removeTunerGeniusDevice: (serial: string) => void;

  // Radio CAT Control
  discoveredRadios: Map<string, RadioDiscoveredEvent>;
  radioConnectionStates: Map<string, RadioConnectionState>;
  radioStates: Map<string, RadioStateChangedEvent>;
  selectedRadioId: string | null;
  addDiscoveredRadio: (radio: RadioDiscoveredEvent) => void;
  removeDiscoveredRadio: (radioId: string) => void;
  setRadioConnectionState: (radioId: string, state: RadioConnectionState) => void;
  setRadioState: (state: RadioStateChangedEvent) => void;
  setSelectedRadio: (radioId: string | null) => void;
  clearRadioState: (radioId: string) => void;
  clearDiscoveredRadios: () => void;

  // CW Keyer
  cwKeyerStatus: Map<string, CwKeyerStatusEvent>;
  setCwKeyerStatus: (status: CwKeyerStatusEvent) => void;
  clearCwKeyerStatus: (radioId: string) => void;


  // QRZ Sync
  qrzSyncProgress: QrzSyncProgress | null;
  setQrzSyncProgress: (progress: QrzSyncProgress | null) => void;

  // LOTW Upload
  lotwUploadProgress: LotwUploadProgress | null;
  setLotwUploadProgress: (progress: LotwUploadProgress | null) => void;

  // ADIF Import
  adifImportProgress: AdifImportProgress | null;
  setAdifImportProgress: (progress: AdifImportProgress | null) => void;

  // Log History Callsign Filter (shared between LogEntry and LogHistory)
  logHistoryCallsignFilter: string | null;
  setLogHistoryCallsignFilter: (callsign: string | null) => void;
  clearCallsignFromAllControls: () => void;

  // Selected DX Cluster spot (for auto-populating log entry)
  selectedSpot: SpotSelectedEvent | null;
  setSelectedSpot: (spot: SpotSelectedEvent | null) => void;

  // DX Cluster connection statuses
  clusterStatuses: Record<string, ClusterStatus>;
  setClusterStatus: (clusterId: string, status: ClusterStatus) => void;

  // POTA spots
  potaSpots: PotaSpot[];
  setPotaSpots: (spots: PotaSpot[]) => void;

  // DX Cluster map overlay
  dxClusterMapEnabled: boolean;
  hoveredSpotId: string | null;
  setDxClusterMapEnabled: (enabled: boolean) => void;
  setHoveredSpotId: (id: string | null) => void;

  // DX Cluster spots (ephemeral, in-memory only)
  dxClusterSpots: Spot[];
  addDxClusterSpot: (spot: Spot) => void;
  clearDxClusterSpots: () => void;
  pruneStaleDxClusterSpots: () => void;

  // Callsign map images (persisted in MongoDB)
  callsignMapImages: CallsignMapImage[];
  setCallsignMapImages: (images: CallsignMapImage[]) => void;
  addCallsignMapImage: (image: CallsignMapImage) => void;
}

export interface Spot {
  id: string;
  dxCall: string;
  spotter: string;
  frequency: number;
  mode?: string;
  comment?: string;
  source?: string;
  timestamp: string;
  country?: string;
  status?: 'newDxcc' | 'newBand' | 'worked';
  isHot?: boolean;
  dxStation?: {
    country?: string;
    dxcc?: number;
    grid?: string;
    continent?: string;
  };
  spotterStation?: {
    country?: string;
    dxcc?: number;
    grid?: string;
    continent?: string;
  };
}

export interface ClusterStatus {
  clusterId: string;
  name: string;
  status: 'connected' | 'connecting' | 'disconnected' | 'error';
  errorMessage: string | null;
}

export interface QrzSyncProgress {
  total: number;
  completed: number;
  successful: number;
  failed: number;
  isComplete: boolean;
  currentCallsign: string | null;
  message: string | null;
}

export interface LotwUploadProgress {
  stage: string; // "preparing" | "signing" | "uploading" | "done" | "error"
  qsoCount: number;
  isComplete: boolean;
  tqslExitCode: number | null;
  message: string | null;
}

export interface AdifImportProgress {
  total: number;
  processed: number;
  imported: number;
  skipped: number;
  failed: number;
  isComplete: boolean;
  currentCallsign: string | null;
  message: string | null;
}

export const useAppStore = create<AppState>((set) => ({
  // Connection
  isConnected: false,
  connectionState: 'disconnected' as ConnectionState,
  reconnectAttempt: 0,
  databaseConnected: false,
  setConnected: (connected) => set({
    isConnected: connected,
    connectionState: connected ? 'connected' : 'disconnected',
    reconnectAttempt: connected ? 0 : undefined, // Keep attempt count when disconnected
  }),
  setConnectionState: (state, attempt) => set({
    connectionState: state,
    isConnected: state === 'connected',
    reconnectAttempt: attempt ?? 0,
  }),
  setDatabaseConnected: (connected) => set({ databaseConnected: connected }),

  // Focused callsign
  focusedCallsign: null,
  focusedCallsignInfo: null,
  isLookingUpCallsign: false,
  setFocusedCallsign: (callsign) => set({ focusedCallsign: callsign }),
  setFocusedCallsignInfo: (info) => set({ focusedCallsignInfo: info, isLookingUpCallsign: false }),
  setLookingUpCallsign: (loading) => set({ isLookingUpCallsign: loading }),

  // Rotator
  rotatorPosition: null,
  setRotatorPosition: (position) => set({ rotatorPosition: position }),

  // Rig
  rigStatus: null,
  setRigStatus: (status) => set({ rigStatus: status }),

  // Station
  stationCallsign: '',
  stationGrid: '',
  setStationInfo: (callsign, grid) => set({ stationCallsign: callsign, stationGrid: grid }),

  // Antenna Genius
  antennaGeniusDevices: new Map(),
  setAntennaGeniusStatus: (status) =>
    set((state) => {
      const devices = new Map(state.antennaGeniusDevices);
      devices.set(status.deviceSerial, status);
      return { antennaGeniusDevices: devices };
    }),
  updateAntennaGeniusPort: (serial, portStatus) =>
    set((state) => {
      const devices = new Map(state.antennaGeniusDevices);
      const device = devices.get(serial);
      if (device) {
        const updated = {
          ...device,
          portA: portStatus.portId === 1 ? portStatus : device.portA,
          portB: portStatus.portId === 2 ? portStatus : device.portB,
        };
        devices.set(serial, updated);
      }
      return { antennaGeniusDevices: devices };
    }),
  removeAntennaGeniusDevice: (serial) =>
    set((state) => {
      const devices = new Map(state.antennaGeniusDevices);
      devices.delete(serial);
      return { antennaGeniusDevices: devices };
    }),

  // PGXL Amplifier
  pgxlDevices: new Map(),
  setPgxlStatus: (status) =>
    set((state) => {
      const devices = new Map(state.pgxlDevices);
      devices.set(status.serial, status);
      return { pgxlDevices: devices };
    }),
  removePgxlDevice: (serial) =>
    set((state) => {
      const devices = new Map(state.pgxlDevices);
      devices.delete(serial);
      return { pgxlDevices: devices };
    }),
  // PGXL-TCI linking (initialized from localStorage)
  pgxlTciLinkA: localStorage.getItem('pgxlTciLinkA') || null,
  pgxlTciLinkB: localStorage.getItem('pgxlTciLinkB') || null,
  setPgxlTciLink: (side, radioId) => {
    // Persist to localStorage
    if (radioId) {
      localStorage.setItem(side === 'A' ? 'pgxlTciLinkA' : 'pgxlTciLinkB', radioId);
    } else {
      localStorage.removeItem(side === 'A' ? 'pgxlTciLinkA' : 'pgxlTciLinkB');
    }
    return set(side === 'A' ? { pgxlTciLinkA: radioId } : { pgxlTciLinkB: radioId });
  },

  // Tuner Genius
  tunerGeniusDevices: new Map(),
  setTunerGeniusStatus: (status) =>
    set((state) => {
      const devices = new Map(state.tunerGeniusDevices);
      devices.set(status.deviceSerial, status);
      return { tunerGeniusDevices: devices };
    }),
  updateTunerGeniusPort: (evt) =>
    set((state) => {
      const devices = new Map(state.tunerGeniusDevices);
      const device = devices.get(evt.deviceSerial);
      if (!device) return state;

      const portStatus: TunerGeniusPortStatus = {
        portId: evt.portId,
        auto: evt.auto,
        band: evt.band,
        frequencyMhz: evt.frequencyMhz,
        swr: evt.swr,
        isTuning: evt.isTuning,
        isTransmitting: evt.isTransmitting,
        selectedAntenna: evt.selectedAntenna,
        tuneResult: evt.tuneResult,
      };

      const updatedDevice: TunerGeniusStatusEvent = {
        ...device,
        isOperating: evt.isOperating,
        isBypassed: evt.isBypassed,
        isTuning: evt.isTuning,
        forwardPowerWatts: evt.forwardPowerWatts,
        swr: evt.swrDecimal,
        l: evt.l,
        c1: evt.c1,
        c2: evt.c2,
        activeRadio: evt.activeRadio,
        freqAMhz: evt.portId === 1 ? evt.frequencyMhz : device.freqAMhz,
        freqBMhz: evt.portId === 2 ? evt.frequencyMhz : device.freqBMhz,
        portA: evt.portId === 1 ? portStatus : device.portA,
        portB: evt.portId === 2 ? portStatus : device.portB,
      };

      devices.set(evt.deviceSerial, updatedDevice);
      return { tunerGeniusDevices: devices };
    }),
  removeTunerGeniusDevice: (serial) =>
    set((state) => {
      const devices = new Map(state.tunerGeniusDevices);
      devices.delete(serial);
      return { tunerGeniusDevices: devices };
    }),

  // Radio CAT Control
  discoveredRadios: new Map(),
  radioConnectionStates: new Map(),
  radioStates: new Map(),
  selectedRadioId: null,
  addDiscoveredRadio: (radio) =>
    set((state) => {
      const radios = new Map(state.discoveredRadios);
      radios.set(radio.id, radio);
      return { discoveredRadios: radios };
    }),
  removeDiscoveredRadio: (radioId) =>
    set((state) => {
      const radios = new Map(state.discoveredRadios);
      radios.delete(radioId);
      const connectionStates = new Map(state.radioConnectionStates);
      connectionStates.delete(radioId);
      const radioStates = new Map(state.radioStates);
      radioStates.delete(radioId);
      return { discoveredRadios: radios, radioConnectionStates: connectionStates, radioStates };
    }),
  setRadioConnectionState: (radioId, connectionState) =>
    set((state) => {
      const connectionStates = new Map(state.radioConnectionStates);
      connectionStates.set(radioId, connectionState);
      return { radioConnectionStates: connectionStates };
    }),
  setRadioState: (radioState) =>
    set((state) => {
      const radioStates = new Map(state.radioStates);
      radioStates.set(radioState.radioId, radioState);
      return { radioStates };
    }),
  setSelectedRadio: (radioId) => set({ selectedRadioId: radioId }),
  clearRadioState: (radioId) =>
    set((state) => {
      const radioStates = new Map(state.radioStates);
      radioStates.delete(radioId);
      return { radioStates };
    }),
  clearDiscoveredRadios: () =>
    set({
      discoveredRadios: new Map(),
      radioConnectionStates: new Map(),
      radioStates: new Map(),
      selectedRadioId: null,
    }),

  // CW Keyer
  cwKeyerStatus: new Map(),
  setCwKeyerStatus: (status) =>
    set((state) => {
      const cwStatus = new Map(state.cwKeyerStatus);
      cwStatus.set(status.radioId, status);
      return { cwKeyerStatus: cwStatus };
    }),
  clearCwKeyerStatus: (radioId) =>
    set((state) => {
      const cwStatus = new Map(state.cwKeyerStatus);
      cwStatus.delete(radioId);
      return { cwKeyerStatus: cwStatus };
    }),

  // QRZ Sync
  qrzSyncProgress: null,
  setQrzSyncProgress: (progress) => set({ qrzSyncProgress: progress }),

  // LOTW Upload
  lotwUploadProgress: null,
  setLotwUploadProgress: (progress) => set({ lotwUploadProgress: progress }),

  // ADIF Import
  adifImportProgress: null,
  setAdifImportProgress: (progress) => set({ adifImportProgress: progress }),

  // Log History Callsign Filter
  logHistoryCallsignFilter: null,
  setLogHistoryCallsignFilter: (callsign) => set({ logHistoryCallsignFilter: callsign }),
  clearCallsignFromAllControls: () => set({
    focusedCallsign: null,
    focusedCallsignInfo: null,
    logHistoryCallsignFilter: null,
    isLookingUpCallsign: false,
    selectedSpot: null,
  }),

  // Selected DX Cluster spot
  selectedSpot: null,
  setSelectedSpot: (spot) => set({ selectedSpot: spot }),

  // DX Cluster connection statuses
  clusterStatuses: {},
  setClusterStatus: (clusterId, status) =>
    set((state) => ({
      clusterStatuses: {
        ...state.clusterStatuses,
        [clusterId]: status,
      },
    })),

  // POTA spots
  potaSpots: [],
  setPotaSpots: (spots) => set({ potaSpots: spots }),

  // DX Cluster map overlay
  dxClusterMapEnabled: false,
  hoveredSpotId: null,
  setDxClusterMapEnabled: (enabled) => set({ dxClusterMapEnabled: enabled }),
  setHoveredSpotId: (id) => set({ hoveredSpotId: id }),

  // DX Cluster spots (ephemeral, in-memory only)
  dxClusterSpots: [],
  addDxClusterSpot: (spot) => set((state) => {
    // Reconnects replay recent spots with the same ids — don't duplicate
    if (state.dxClusterSpots.some((s) => s.id === spot.id)) {
      return state;
    }
    // Both limits are operator-tunable now (v1 SDRLogger+ parity). Read
    // straight from the settings store so the change takes effect on
    // the next spot without needing a reload. Clamp defensively so a
    // corrupt settings value can't yield a runaway array.
    const cluster = useSettingsStore.getState().settings.cluster;
    const maxSpots = Math.max(50, Math.min(300, cluster.maxSpots || 200));
    const maxAgeMs = Math.max(1, cluster.spotAgeMinutes || 10) * 60 * 1000;
    const cutoff = Date.now() - maxAgeMs;
    const fresh = state.dxClusterSpots.filter(
      (s) => new Date(s.timestamp).getTime() > cutoff
    );
    return { dxClusterSpots: [spot, ...fresh].slice(0, maxSpots) };
  }),
  clearDxClusterSpots: () => set({ dxClusterSpots: [] }),
  // Timer-driven eviction of expired spots so a stale entry can't linger
  // just because no new spot arrived to trigger addDxClusterSpot's filter.
  // v1 SDRLogger+ ran the equivalent every 30 s (pruneSpots()); v2's
  // ClusterPlugin owns the interval.
  pruneStaleDxClusterSpots: () => set((state) => {
    const cluster = useSettingsStore.getState().settings.cluster;
    const maxAgeMs = Math.max(1, cluster.spotAgeMinutes || 10) * 60 * 1000;
    const cutoff = Date.now() - maxAgeMs;
    const fresh = state.dxClusterSpots.filter(
      (s) => new Date(s.timestamp).getTime() > cutoff
    );
    return fresh.length === state.dxClusterSpots.length
      ? state
      : { dxClusterSpots: fresh };
  }),

  // Callsign map images
  callsignMapImages: [],
  setCallsignMapImages: (images) => set({ callsignMapImages: images }),
  addCallsignMapImage: (image) => set((state) => {
    // Replace existing entry for same callsign, or add new
    const filtered = state.callsignMapImages.filter(
      (i) => i.callsign.toUpperCase() !== image.callsign.toUpperCase()
    );
    return { callsignMapImages: [image, ...filtered] };
  }),
}));

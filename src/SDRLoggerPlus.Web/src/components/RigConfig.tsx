import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import { Radio, Wifi, WifiOff, Power, PowerOff, Plus, Pencil, Settings, ChevronDown, ChevronUp, RefreshCw, ExternalLink } from "lucide-react";
import { useAppStore } from "../store/appStore";
import { useSettingsStore } from "../store/settingsStore";
import { useSignalR } from "../hooks/useSignalR";
import { signalRService } from "../api/signalr";
import { resolvePopularRigs, type ResolvedPopularRig } from "../rigs/popularRigs";
import { portLabel, duplicatePorts } from "../rigs/serialPorts";
import type {
  HamlibRigModelInfo,
  HamlibRigCapabilities,
  HamlibRigConfigDto,
  HamlibDataBits,
  HamlibStopBits,
  HamlibFlowControl,
  HamlibParity,
  HamlibPttType,
  SerialPortDetail,
} from "../api/signalr";
import { HAMLIB_BAUD_RATES } from "../api/signalr";

const defaultHamlibConfig: HamlibRigConfigDto = {
  modelId: 0,
  modelName: "",
  connectionType: "Serial",
  baudRate: 9600,
  dataBits: 8,
  stopBits: 1,
  flowControl: "None",
  parity: "None",
  hostname: "localhost",
  networkPort: 4532,
  pttType: "Rig",
  getFrequency: true,
  getMode: true,
  getVfo: true,
  getPtt: true,
  getPower: false,
  getRit: false,
  getXit: false,
  getKeySpeed: false,
  pollIntervalMs: 250,
};

/**
 * Radio setup: pick a radio type, configure Hamlib / flrig / TCI, and manage
 * saved rigs. Lives in Settings > Station.
 *
 * Deliberately config-only — the live frequency readout, TX/RX state and
 * scroll-tuning that used to sit alongside this in the Rig panel are all
 * covered by the Meters and Panadapter panels, and per-radio connect/disconnect
 * is on the status-bar rig selector.
 */
export function RigConfig() {
  const {
    discoveredRadios,
    radioConnectionStates,
    radioStates,
    selectedRadioId,
    setSelectedRadio,
    removeDiscoveredRadio,
  } = useAppStore();

  const {
    connectRadio,
    disconnectRadio,
    getHamlibRigList,
    getHamlibRigCaps,
    getHamlibSerialPorts,
    getHamlibConfig,
    saveHamlibConfig,
    disconnectHamlibRig,
    deleteHamlibConfig,
    disconnectTci,
    saveTciConfig,
    deleteTciConfig,
    saveFlrigConfig,
  } = useSignalR();

  // Radio settings from store (persisted to database)
  const { settings, updateRadioSettings, updateTciSettings, updateFlrigSettings, saveSettings } = useSettingsStore();
  const tciSettings = settings.radio.tci;
  const flrigSettings = settings.radio.flrig;
  const { autoReconnect, autoConnectRigId, activeRigType, reconnectLastOnStartup } = settings.radio;

  // TCI form state
  const [showTciForm, setShowTciForm] = useState(false);
  const [isConnectingTci, setIsConnectingTci] = useState(false);
  const [editingRadioId, setEditingRadioId] = useState<string | null>(null);
  const [tciTestResult, setTciTestResult] = useState<{ ok: boolean; message: string } | null>(null);
  const [isTestingTci, setIsTestingTci] = useState(false);

  // flrig form state (v1.x SDRLogger+ port)
  const [showFlrigForm, setShowFlrigForm] = useState(false);

  // FlexRadio form state (native SmartSDR backend — discovery-based)
  const [showFlexForm, setShowFlexForm] = useState(false);

  // Hamlib form state
  const [showHamlibForm, setShowHamlibForm] = useState(false);
  const [isConnectingHamlib, setIsConnectingHamlib] = useState(false);
  const [hamlibRigs, setHamlibRigs] = useState<HamlibRigModelInfo[]>([]);
  // Why the model list is empty, when it is — a blank dropdown with no reason is
  // indistinguishable from a broken app.
  const [rigListError, setRigListError] = useState<string | null>(null);
  const [hamlibCaps, setHamlibCaps] = useState<HamlibRigCapabilities | null>(null);
  // Same ports with the device name behind each — "COM5" alone is not enough to choose
  // from when one radio exposes two of them.
  const [portDetails, setPortDetails] = useState<SerialPortDetail[]>([]);
  const [hamlibConfig, setHamlibConfig] = useState<HamlibRigConfigDto>(defaultHamlibConfig);
  const [rigSearch, setRigSearch] = useState("");
  const [showRigDropdown, setShowRigDropdown] = useState(false);
  // Which shortcut entry is selected, if any — drives the radio-side setup hint.
  const [pickedPopularId, setPickedPopularId] = useState<string | null>(null);
  const [showAllRigs, setShowAllRigs] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);

  // Set up Hamlib event handlers
  useEffect(() => {
    signalRService.setHandlers({
      onHamlibRigList: (evt) => {
        console.log('Hamlib rig list received:', evt.rigs.length, 'rigs');
        setHamlibRigs(evt.rigs);
        setRigListError(evt.error ?? null);
      },
      onHamlibRigCaps: (evt) => {
        console.log('Hamlib rig caps received:', evt.modelId);
        setHamlibCaps(evt.capabilities);
      },
      onHamlibSerialPorts: (evt) => {
        console.log('Serial ports received:', evt.ports);
        // Older servers send only the bare names; fall back to those rather than
        // showing nothing.
        setPortDetails(evt.details ?? evt.ports.map((p) => ({ port: p, isUsb: false })));
      },
      onHamlibConfigLoaded: (evt) => {
        console.log('Hamlib config loaded:', evt.config?.modelName);
        if (evt.config) {
          // A config saved when DTR/RTS were offered would otherwise display as
          // "Don't read" while still handing Hamlib a line-based PTT type. Since we
          // only ever read PTT, CAT is both the accurate reading and the one that
          // leaves the serial control lines alone.
          const cfg = evt.config.pttType === "Dtr" || evt.config.pttType === "Rts"
            ? { ...evt.config, pttType: "Rig" as HamlibPttType, pttPort: undefined }
            : evt.config;
          setHamlibConfig(cfg);
          // Also load caps for this model
          getHamlibRigCaps(evt.config.modelId);
        }
      },
    });
  }, [getHamlibRigCaps]);

  // Load Hamlib data when form opens
  useEffect(() => {
    if (showHamlibForm && hamlibRigs.length === 0) {
      getHamlibRigList();
      getHamlibSerialPorts();
      getHamlibConfig();
    }
  }, [showHamlibForm, hamlibRigs.length, getHamlibRigList, getHamlibSerialPorts, getHamlibConfig]);

  // Load capabilities when rig model changes
  useEffect(() => {
    if (hamlibConfig.modelId > 0) {
      getHamlibRigCaps(hamlibConfig.modelId);
    }
  }, [hamlibConfig.modelId, getHamlibRigCaps]);

  // Auto-select connection type based on capabilities only when caps are first loaded
  // This prevents overriding user's manual connection type selection
  useEffect(() => {
    if (hamlibCaps && hamlibConfig.modelId > 0) {
      // Only auto-adjust if current selection is not supported
      if (hamlibConfig.connectionType === "Serial" && !hamlibCaps.supportsSerial && hamlibCaps.supportsNetwork) {
        updateHamlibConfig({ connectionType: "Network", hostname: hamlibConfig.hostname || "localhost" });
      } else if (hamlibConfig.connectionType === "Network" && !hamlibCaps.supportsNetwork && hamlibCaps.supportsSerial) {
        updateHamlibConfig({ connectionType: "Serial" });
      } else if (hamlibConfig.connectionType === "Network" && !hamlibConfig.hostname) {
        // Default hostname to localhost for network connections
        updateHamlibConfig({ hostname: "localhost" });
      }
    }
    // Only run when caps change (i.e., when a new model is selected), not on every connection type change
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hamlibCaps]);

  // Filter rigs by search term
  const filteredRigs = useMemo(() => {
    if (!rigSearch) return hamlibRigs.slice(0, 50); // Show first 50 by default
    const search = rigSearch.toLowerCase();
    return hamlibRigs.filter(rig =>
      rig.displayName.toLowerCase().includes(search) ||
      rig.manufacturer.toLowerCase().includes(search) ||
      rig.model.toLowerCase().includes(search)
    ).slice(0, 50);
  }, [hamlibRigs, rigSearch]);

  // Convert Map to array for rendering. Memoised because the auto-connect effect below
  // depends on it: a fresh array every render made that effect fire every render.
  const radios = useMemo(() => Array.from(discoveredRadios.values()), [discoveredRadios]);
  const selectedConnectionState = selectedRadioId
    ? radioConnectionStates.get(selectedRadioId)
    : null;
  const selectedRadioState = selectedRadioId
    ? radioStates.get(selectedRadioId)
    : null;

  // Reset local connecting state when we receive connection state or radio data from SignalR
  useEffect(() => {
    if (selectedRadioState) {
      setIsConnectingTci(false);
      setIsConnectingHamlib(false);
    } else if (selectedConnectionState && selectedConnectionState !== "Connecting") {
      setIsConnectingTci(false);
      setIsConnectingHamlib(false);
    }
  }, [selectedConnectionState, selectedRadioState]);

  const handleConnect = useCallback(async (radioId: string) => {
    setSelectedRadio(radioId);
    await connectRadio(radioId);
    // Remember this as the reconnect target when reconnectLastOnStartup is enabled.
    if (reconnectLastOnStartup) {
      const radio = discoveredRadios.get(radioId);
      const rigType = radio?.type === "Hamlib" || radioId.startsWith("hamlib-")
        ? "hamlib" as const
        : radio?.type === "Tci" || radioId.startsWith("tci-")
          ? "tci" as const
          : null;
      // Only when it actually changes. Writing unconditionally persisted the same three
      // values on every connect ATTEMPT — and a rig that is switched off is retried
      // forever, so an unreachable radio turned into an endless stream of settings saves.
      const alreadyStored = autoReconnect && autoConnectRigId === radioId && activeRigType === rigType;
      if (!alreadyStored) {
        updateRadioSettings({ autoReconnect: true, autoConnectRigId: radioId, activeRigType: rigType });
        saveSettings();
      }
    }
  }, [setSelectedRadio, connectRadio, reconnectLastOnStartup, discoveredRadios,
      autoReconnect, autoConnectRigId, activeRigType, updateRadioSettings, saveSettings]);

  // Last (rig, connection-state) the auto-connect effect acted on, so it fires on
  // transitions rather than on every render.
  const lastAutoAttemptRef = useRef<string | null>(null);

  // Auto-connect to saved rig if autoReconnect is enabled and we have a discovered radio.
  // IMPORTANT: We must wait for connection state to arrive before deciding whether to connect.
  // The OnRadioDiscovered event arrives before OnRadioConnectionStateChanged from RequestRadioStatus,
  // so if we connect when connState is undefined, we'd tear down an already-working backend connection.
  useEffect(() => {
    if (!autoReconnect || radios.length === 0 || selectedRadioId || isConnectingHamlib || isConnectingTci) return;

    // Find the specific rig targeted for auto-connect, or fall back to first radio
    const targetRadio = autoConnectRigId
      ? radios.find(r => r.id === autoConnectRigId)
      : radios[0];

    if (!targetRadio) return;

    const connState = radioConnectionStates.get(targetRadio.id);

    // Act once per state transition, not once per render. Without this, any re-render
    // while the target rig is switched off re-fires the connect attempt immediately.
    const attempt = `${targetRadio.id}:${connState ?? "unknown"}`;
    if (lastAutoAttemptRef.current === attempt) return;
    lastAutoAttemptRef.current = attempt;

    if (connState === "Connected" || connState === "Monitoring") {
      // Backend already has this rig connected — just select it, no reconnect needed
      console.log("Auto-selecting already-connected rig:", targetRadio.id);
      setSelectedRadio(targetRadio.id);
    } else if (connState === "Disconnected" || connState === "Error") {
      // Rig is explicitly not connected — initiate connection
      console.log("Auto-connecting to saved rig:", targetRadio.id);
      handleConnect(targetRadio.id);
    }
    // If connState is undefined, the connection state event hasn't arrived yet — wait for it.
    // The useEffect will re-fire when radioConnectionStates updates.
  }, [autoReconnect, autoConnectRigId, radios, selectedRadioId, isConnectingHamlib, isConnectingTci, radioConnectionStates, handleConnect, setSelectedRadio]);

  // (flrig auto-select moved app-wide to App.tsx so it works with this panel closed,
  // and generalized to any connected rig — see the active-rig auto-select effect there.)


  const handleToggleAutoReconnectForRig = async (radioId: string) => {
    if (autoConnectRigId === radioId && autoReconnect) {
      // Already targeting this rig — disable
      updateRadioSettings({
        autoReconnect: false,
        autoConnectRigId: null,
        activeRigType: null,
      });
    } else {
      // Enable and target this specific rig
      const radio = discoveredRadios.get(radioId);
      const rigType = radio?.type === "Hamlib" || radioId.startsWith("hamlib-")
        ? "hamlib" as const
        : radio?.type === "Tci" || radioId.startsWith("tci-")
          ? "tci" as const
          : null;
      updateRadioSettings({
        autoReconnect: true,
        autoConnectRigId: radioId,
        activeRigType: rigType,
      });
    }
    await saveSettings();
  };

  const handleRemoveRig = async (radioId: string) => {
    try {
      const radio = discoveredRadios.get(radioId);
      const isHamlib = radio?.type === "Hamlib" || radioId.startsWith("hamlib-");
      const isTci = radio?.type === "Tci" || radioId.startsWith("tci-");

      // Disconnect if currently connected
      const connState = radioConnectionStates.get(radioId);
      if (connState && connState !== "Disconnected") {
        if (isHamlib) {
          await disconnectHamlibRig();
        } else if (isTci) {
          await disconnectTci(radioId);
        } else {
          await disconnectRadio(radioId);
        }
      }

      // Delete saved configuration based on radio type
      if (isHamlib) {
        await deleteHamlibConfig();
        setHamlibConfig(defaultHamlibConfig);
        setRigSearch("");
      } else if (isTci) {
        await deleteTciConfig(radioId);
        updateTciSettings({ host: "", port: 50001, name: "" });
      }

      // Remove from UI immediately
      removeDiscoveredRadio(radioId);
      setSelectedRadio(null);

      // Clear rig settings
      updateRadioSettings({ activeRigType: null, autoReconnect: false, autoConnectRigId: null });
      await saveSettings();
    } catch (error) {
      console.error("Failed to remove rig:", error);
    }
  };

  const handleAddHamlib = async () => {
    if (!hamlibConfig.modelId) return;

    // Validate based on connection type
    if (hamlibConfig.connectionType === "Serial" && !hamlibConfig.serialPort) {
      return;
    }
    if (hamlibConfig.connectionType === "Network" && !hamlibConfig.hostname) {
      return;
    }

    setShowHamlibForm(false);

    try {
      await saveHamlibConfig(hamlibConfig);
    } catch (error) {
      console.error('Failed to save Hamlib config:', error);
    }
  };

  // Probe a TCI host:port via the backend (WebSocket handshake, 4 s timeout).
  const testTciConnection = async (host: string, port: number): Promise<{ ok: boolean; message: string }> => {
    try {
      const res = await fetch('/api/tci/test-connection', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ host, port }),
      });
      const data = await res.json();
      return data.ok
        ? { ok: true, message: `TCI server responding at ${host}:${port}` }
        : { ok: false, message: data.error || 'No response from the TCI server.' };
    } catch {
      return { ok: false, message: 'Could not reach the backend to run the test.' };
    }
  };

  const handleTestTci = async () => {
    const { host, port } = tciSettings;
    if (!host || !port) return;
    setIsTestingTci(true);
    setTciTestResult(null);
    setTciTestResult(await testTciConnection(host, port));
    setIsTestingTci(false);
  };

  // Add or save a TCI rig. Probes host:port first (unless the operator chose
  // "Add anyway" after a failed probe) so a wrong port can't silently create a
  // rig that never connects.
  const handleSaveTci = async (skipProbe = false) => {
    const host = tciSettings.host;
    const port = tciSettings.port;
    if (!host || !port) return;

    if (!skipProbe) {
      setIsTestingTci(true);
      setTciTestResult(null);
      const result = await testTciConnection(host, port);
      setIsTestingTci(false);
      setTciTestResult(result);
      if (!result.ok) return; // stay on the form; the "Add anyway" button appears
    }

    try {
      // Editing to a different host:port changes the rig id — drop the old entry.
      if (editingRadioId) {
        const newId = `tci-${host}:${port}`;
        if (editingRadioId !== newId) await deleteTciConfig(editingRadioId);
      }
      await saveTciConfig(host, port, tciSettings.name || undefined);
    } catch (error) {
      console.error('Failed to save TCI config:', error);
    }

    setShowTciForm(false);
    setEditingRadioId(null);
    setTciTestResult(null);
  };

  const handleEditTci = (radio: { id: string; ipAddress: string; port?: number; model?: string; nickname?: string }) => {
    // The rig's name lives in nickname (if set) or model; blank the
    // auto-generated "TCI (host)" default so the placeholder shows instead.
    const autoName = `TCI (${radio.ipAddress})`;
    const current = radio.nickname || radio.model || '';
    updateTciSettings({
      host: radio.ipAddress,
      port: radio.port ?? 50001,
      name: current === autoName ? '' : current,
    });
    setEditingRadioId(radio.id);
    setTciTestResult(null);
    setShowTciForm(true);
  };

  const closeTciForm = () => {
    setShowTciForm(false);
    setEditingRadioId(null);
    setTciTestResult(null);
  };

  // Save flrig config — flips FlrigService's poll state on the backend
  // via SaveFlrigConfig hub method (writes settings.Radio.Flrig).
  const handleSaveFlrig = async () => {
    const { host, port, digitalMode, rttyMode } = flrigSettings;
    if (!host || !port) return;
    setShowFlrigForm(false);
    try {
      // Toggling Enabled here — Save always writes the current form values.
      // The Enable pill in the form is the source of truth for the toggle.
      await saveFlrigConfig(host, port, flrigSettings.enabled,
        digitalMode || undefined, rttyMode || undefined);
    } catch (error) {
      console.error('Failed to save flrig config:', error);
    }
  };

  const updateHamlibConfig = (updates: Partial<HamlibRigConfigDto>) => {
    setHamlibConfig(prev => ({ ...prev, ...updates }));
  };

  const handleRigSelect = (rig: HamlibRigModelInfo) => {
    updateHamlibConfig({
      modelId: rig.modelId,
      modelName: rig.displayName,
    });
    setRigSearch(rig.displayName);
    setShowRigDropdown(false);
    setPickedPopularId(null);
  };

  // Shortcut list, narrowed to what the installed Hamlib actually offers.
  const clashingPorts = useMemo(() => duplicatePorts(portDetails), [portDetails]);
  const popularRigs = useMemo(() => resolvePopularRigs(hamlibRigs), [hamlibRigs]);

  // An already-configured rig that isn't on the shortcut list has to stay visible —
  // otherwise loading that config would show a shortcut grid with nothing selected on it.
  useEffect(() => {
    if (hamlibConfig.modelId <= 0 || popularRigs.length === 0) return;
    const onShortlist = popularRigs.some((r) => r.modelId === hamlibConfig.modelId);
    if (!onShortlist) setShowAllRigs(true);
  }, [hamlibConfig.modelId, popularRigs]);

  const handlePopularSelect = (rig: ResolvedPopularRig) => {
    // Fill the whole serial setup, not just the model — picking a radio by its front-panel
    // name should leave nothing but the port to choose.
    updateHamlibConfig({ modelId: rig.modelId, modelName: rig.modelName, ...rig.defaults });
    setRigSearch(rig.modelName);
    setShowRigDropdown(false);
    setPickedPopularId(rig.id);
  };





  // Radio discovery + connection setup. Rendered inside Settings > Station, so
  // no panel chrome of its own.
  return (
    <div>
      <div className="space-y-4">
        {/* Radio Type Selection */}
        <div>
          <div className="text-xs text-dark-300 uppercase tracking-wider mb-2 font-ui">
            Radio Type
          </div>
          <div className="grid grid-cols-4 gap-2">
            <button
              onClick={() => {
                setShowTciForm(!showTciForm);
                setShowHamlibForm(false);
                setShowFlrigForm(false);
                setShowFlexForm(false);
              }}
              className={`px-3 py-3 rounded-lg text-sm font-medium font-ui transition-all border ${
                showTciForm
                  ? "bg-accent-secondary/20 text-accent-secondary border-accent-secondary/30"
                  : "bg-dark-700 text-dark-200 hover:bg-dark-600 border-glass-100"
              }`}
            >
              <div className="flex flex-col items-center gap-1">
                <Radio className="w-5 h-5" />
                <span>TCI</span>
              </div>
            </button>
            <button
              onClick={() => {
                setShowHamlibForm(!showHamlibForm);
                setShowTciForm(false);
                setShowFlrigForm(false);
                setShowFlexForm(false);
              }}
              className={`px-3 py-3 rounded-lg text-sm font-medium font-ui transition-all border ${
                showHamlibForm
                  ? "bg-accent-primary/20 text-accent-primary border-accent-primary/30"
                  : "bg-dark-700 text-dark-200 hover:bg-dark-600 border-glass-100"
              }`}
            >
              <div className="flex flex-col items-center gap-1">
                <Settings className="w-5 h-5" />
                <span>Hamlib</span>
              </div>
            </button>
            <button
              onClick={() => {
                setShowFlrigForm(!showFlrigForm);
                setShowTciForm(false);
                setShowHamlibForm(false);
                setShowFlexForm(false);
              }}
              className={`px-3 py-3 rounded-lg text-sm font-medium font-ui transition-all border ${
                showFlrigForm
                  ? "bg-amber-500/20 text-amber-400 border-amber-400/30"
                  : "bg-dark-700 text-dark-200 hover:bg-dark-600 border-glass-100"
              }`}
              title="W1HKJ flrig XML-RPC bridge"
            >
              <div className="flex flex-col items-center gap-1">
                <Radio className="w-5 h-5" />
                <span>flrig</span>
              </div>
            </button>
            <button
              onClick={() => {
                setShowFlexForm(!showFlexForm);
                setShowTciForm(false);
                setShowHamlibForm(false);
                setShowFlrigForm(false);
              }}
              className={`px-3 py-3 rounded-lg text-sm font-medium font-ui transition-all border ${
                showFlexForm
                  ? "bg-emerald-500/20 text-emerald-400 border-emerald-400/30"
                  : "bg-dark-700 text-dark-200 hover:bg-dark-600 border-glass-100"
              }`}
              title="Native FlexRadio 6000 (SmartSDR) — auto-discovered on the LAN"
            >
              <div className="flex flex-col items-center gap-1">
                <Radio className="w-5 h-5" />
                <span>FlexRadio</span>
              </div>
            </button>
          </div>
        </div>

        {/* FlexRadio info — the native SmartSDR backend needs no manual config: it
            listens for the radio's UDP discovery broadcast and lists any Flex 6000 on
            the LAN below. Connecting rides ALONGSIDE SmartSDR (the API is multi-client). */}
        {showFlexForm && (
          <div className="bg-dark-700/50 rounded-lg p-4 border border-emerald-400/30 space-y-2">
            <div className="text-xs text-emerald-400 uppercase tracking-wider font-ui">
              FlexRadio 6000 (SmartSDR)
            </div>
            <p className="text-sm text-dark-200">
              Flex radios are <strong className="text-white">auto-discovered</strong> on your
              local network — no host or port to enter. Any powered-on 6000-series radio
              appears in the list below; click it to connect. SDRLogger+ connects to the
              radio's control API <strong className="text-white">alongside SmartSDR</strong> (or
              Aether), so you don't have to close anything.
            </p>
            <p className="text-xs text-amber-400/90">
              ⚠ New in this build and not yet bench-verified against hardware — please report
              any issues.
            </p>
          </div>
        )}

        {/* flrig Configuration Form — v1.x SDRLogger+ port. flrig is a
            desktop bridge that talks to the physical rig over CAT/USB and
            exposes the connection as XML-RPC on port 12345 by default.
            SDRLoggerPlus polls flrig at 1.5 s cadence when Enabled. */}
        {showFlrigForm && (
          <div className="bg-dark-700/50 rounded-lg p-4 border border-amber-400/30 space-y-3">
            <div className="text-xs text-amber-400 uppercase tracking-wider mb-2 font-ui">
              flrig XML-RPC Connection
            </div>

            <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
              <input
                type="checkbox"
                checked={flrigSettings.enabled}
                onChange={(e) => updateFlrigSettings({ enabled: e.target.checked })}
                className="w-4 h-4 rounded bg-dark-800 border-glass-100 text-amber-400 focus:ring-amber-400/50"
              />
              <span className="text-dark-200 font-ui">Enable flrig polling</span>
            </label>

            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs text-dark-300 mb-1 font-ui">Host</label>
                <input
                  type="text"
                  value={flrigSettings.host}
                  onChange={(e) => updateFlrigSettings({ host: e.target.value })}
                  placeholder="127.0.0.1"
                  className="w-full px-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-amber-400/50"
                />
              </div>
              <div>
                <label className="block text-xs text-dark-300 mb-1 font-ui">Port</label>
                <input
                  type="number"
                  value={flrigSettings.port}
                  onChange={(e) => updateFlrigSettings({ port: parseInt(e.target.value) || 12345 })}
                  placeholder="12345"
                  className="w-full px-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-amber-400/50"
                />
              </div>
            </div>

            <details className="text-xs">
              <summary className="cursor-pointer text-dark-300 hover:text-dark-200 font-ui">
                Rig-specific mode overrides (advanced)
              </summary>
              <div className="mt-2 space-y-3">
                <div>
                  <label className="block text-[10px] text-dark-300 mb-1 font-ui uppercase tracking-wider">
                    Digital passthrough mode override
                  </label>
                  <input
                    type="text"
                    value={flrigSettings.digitalMode}
                    onChange={(e) => updateFlrigSettings({ digitalMode: e.target.value.trim().toUpperCase() })}
                    placeholder="Auto-detect (leave blank)"
                    className="w-full px-3 py-1.5 bg-dark-800 border border-glass-100 rounded text-xs text-dark-200 font-mono focus:outline-none focus:border-amber-400/50"
                  />
                  <p className="text-[10px] text-dark-400 mt-1">
                    Common: USB-D (Icom), DATA-U (Kenwood/Yaesu), PKT-U, DIGU. Auto-detected on connect if blank.
                  </p>
                </div>
                <div>
                  <label className="block text-[10px] text-dark-300 mb-1 font-ui uppercase tracking-wider">
                    RTTY mode override
                  </label>
                  <input
                    type="text"
                    value={flrigSettings.rttyMode}
                    onChange={(e) => updateFlrigSettings({ rttyMode: e.target.value.trim().toUpperCase() })}
                    placeholder="RTTY (leave blank for native)"
                    className="w-full px-3 py-1.5 bg-dark-800 border border-glass-100 rounded text-xs text-dark-200 font-mono focus:outline-none focus:border-amber-400/50"
                  />
                  <p className="text-[10px] text-dark-400 mt-1">
                    Blank = native RTTY. Set USB-D / DATA-U for AFSK RTTY via fldigi.
                  </p>
                </div>
              </div>
            </details>

            <div className="flex gap-2 pt-2">
              <button
                onClick={handleSaveFlrig}
                disabled={!flrigSettings.host}
                className="flex-1 px-4 py-2 text-sm font-medium font-ui flex items-center justify-center gap-2 bg-amber-500/20 text-amber-400 rounded-lg hover:bg-amber-500/30 transition-all disabled:opacity-50"
              >
                <Plus className="w-4 h-4" />
                Save
              </button>
              <button
                onClick={() => setShowFlrigForm(false)}
                className="px-4 py-2 text-sm font-medium font-ui bg-dark-700 text-dark-300 rounded-lg hover:bg-dark-600 transition-all"
              >
                Cancel
              </button>
            </div>
            <p className="text-[10px] text-dark-400">
              Requires flrig running on the given host/port. flrig connects to the physical rig via CAT; SDRLoggerPlus reads freq/mode from flrig at 1.5 s.
            </p>
          </div>
        )}

        {/* Hamlib Configuration Form */}
        {showHamlibForm && (
          <div className="bg-dark-700/50 rounded-lg p-4 border border-accent-primary/30 space-y-4">
            <div className="text-xs text-accent-primary uppercase tracking-wider font-ui">
              Hamlib Rig Configuration
            </div>

            {/* No models at all — say why, and how to fix it, rather than showing an
                empty picker the operator can only read as "this app is broken". */}
            {hamlibRigs.length === 0 && rigListError && (
              <div className="rounded-lg border border-red-500/30 bg-red-500/5 p-3">
                <div className="text-xs uppercase tracking-wider text-red-300 font-ui mb-1">
                  No radio models available
                </div>
                <p className="text-xs text-dark-200">{rigListError}</p>
                <a
                  href="https://hamlib.github.io/"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="mt-2 inline-flex items-center gap-1 text-xs text-accent-primary hover:underline font-ui"
                >
                  Get Hamlib <ExternalLink className="w-3 h-3" />
                </a>
              </div>
            )}

            {/* Popular radios — pick by the name on the front panel. Everything else
                (baud, bits, PTT) is filled in, leaving only the port to choose. */}
            {popularRigs.length > 0 && !showAllRigs && (
              <div>
                <label className="block text-xs text-dark-300 mb-1 font-ui">Popular Radios</label>
                <div className="flex flex-wrap gap-1.5">
                  {popularRigs.map((rig) => (
                    <button
                      key={rig.id}
                      onClick={() => handlePopularSelect(rig)}
                      title={`${rig.manufacturer} ${rig.label} — ${rig.modelName}`}
                      className={`px-2.5 py-1.5 rounded-lg text-xs font-medium font-ui border transition-all ${
                        pickedPopularId === rig.id
                          ? "bg-accent-primary/20 text-accent-primary border-accent-primary/30"
                          : "bg-dark-800 text-dark-200 border-glass-100 hover:bg-dark-700"
                      }`}
                    >
                      {rig.label}
                    </button>
                  ))}
                </div>
                <button
                  onClick={() => setShowAllRigs(true)}
                  className="mt-2 text-xs text-accent-primary hover:underline font-ui"
                >
                  My radio isn't listed — show all {hamlibRigs.length} models
                </button>
                <p className="mt-1 text-[11px] text-dark-300">
                  A shortcut, not a compatibility list — Hamlib drives many more radios than these.
                </p>
              </div>
            )}

            {/* Radio-side setup: the menu item on the rig that has to agree. This is what
                most "it won't connect" reports turn out to be, and we can't detect it. */}
            {pickedPopularId && (
              <div className="rounded-lg border border-accent-secondary/30 bg-accent-secondary/5 p-2.5">
                <div className="text-[11px] uppercase tracking-wider text-accent-secondary font-ui mb-1">
                  Check on the radio
                </div>
                <p className="text-xs text-dark-200">
                  {popularRigs.find((r) => r.id === pickedPopularId)?.setupHint}
                </p>
                {/* Say plainly which defaults are still documentation rather than bench —
                    the same honesty the FlexRadio backend ships with. */}
                {!popularRigs.find((r) => r.id === pickedPopularId)?.verified && (
                  <p className="mt-1.5 text-[11px] text-dark-300">
                    🧪 These starting values come from the radio's documentation and haven't been confirmed
                    on this model yet. If it connects — or if you had to change something — please let us know
                    so we can mark it verified.
                  </p>
                )}
              </div>
            )}

            {/* What's actually selected, while the full list is collapsed. */}
            {!showAllRigs && popularRigs.length > 0 && hamlibConfig.modelId > 0 && (
              <div className="text-xs text-dark-300 font-ui">
                Selected: <span className="text-dark-100 font-mono">{hamlibConfig.modelName}</span>
              </div>
            )}

            {/* Rig Model Selector — the full searchable Hamlib list. */}
            <div className={`relative ${popularRigs.length > 0 && !showAllRigs ? 'hidden' : ''}`}>
              <label className="block text-xs text-dark-300 mb-1 font-ui">Rig Model</label>
              <input
                type="text"
                value={rigSearch}
                onChange={(e) => {
                  setRigSearch(e.target.value);
                  setShowRigDropdown(true);
                }}
                onFocus={() => setShowRigDropdown(true)}
                placeholder="Search for rig model..."
                className="w-full px-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-primary/50"
              />
              {showRigDropdown && filteredRigs.length > 0 && (
                <div className="absolute z-50 w-full mt-1 max-h-48 overflow-y-auto bg-dark-800 border border-glass-100 rounded-lg shadow-lg">
                  {filteredRigs.map((rig) => (
                    <button
                      key={rig.modelId}
                      onClick={() => handleRigSelect(rig)}
                      className="w-full px-3 py-2 text-left text-sm text-dark-200 hover:bg-dark-700 transition-colors"
                    >
                      <div className="font-medium font-ui">{rig.displayName}</div>
                      <div className="text-xs text-dark-300 font-mono">{rig.manufacturer} - {rig.model}</div>
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Connection Type Toggle - only show options that are supported */}
            <div>
              <label className="block text-xs text-dark-300 mb-1 font-ui">Connection Type</label>
              <div className="flex gap-2">
                {/* Only show Serial option if supported */}
                {(!hamlibCaps || hamlibCaps.supportsSerial) && (
                  <button
                    onClick={() => updateHamlibConfig({ connectionType: "Serial" })}
                    className={`flex-1 px-3 py-2 rounded-lg text-sm font-medium font-ui transition-all ${
                      hamlibConfig.connectionType === "Serial"
                        ? "bg-accent-primary/20 text-accent-primary border border-accent-primary/30"
                        : "bg-dark-800 text-dark-300 border border-glass-100 hover:bg-dark-700"
                    }`}
                  >
                    Serial
                  </button>
                )}
                {/* Only show Network option if supported */}
                {(!hamlibCaps || hamlibCaps.supportsNetwork) && (
                  <button
                    onClick={() => updateHamlibConfig({ connectionType: "Network", hostname: hamlibConfig.hostname || "localhost" })}
                    className={`flex-1 px-3 py-2 rounded-lg text-sm font-medium font-ui transition-all ${
                      hamlibConfig.connectionType === "Network"
                        ? "bg-accent-primary/20 text-accent-primary border border-accent-primary/30"
                        : "bg-dark-800 text-dark-300 border border-glass-100 hover:bg-dark-700"
                    }`}
                  >
                    Network
                  </button>
                )}
              </div>
            </div>

            {/* Serial Settings */}
            {hamlibConfig.connectionType === "Serial" && (
              <div className="space-y-3">
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block text-xs text-dark-300 mb-1 font-ui">Serial Port</label>
                    <select
                      value={hamlibConfig.serialPort || ""}
                      onChange={(e) => updateHamlibConfig({ serialPort: e.target.value })}
                      className="w-full px-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-primary/50"
                    >
                      <option value="">Select port...</option>
                      {/* Keyed by port AND device: two drivers really can claim the same
                          COM number — Windows hands Bluetooth an unused number, but
                          virtual-port software often doesn't register with the name
                          arbiter, so both end up as "COM5". The port alone is not unique. */}
                      {portDetails.map((p, i) => (
                        <option key={`${p.port}|${p.description ?? ''}|${i}`} value={p.port}>{portLabel(p)}</option>
                      ))}
                    </select>
                    {/* A collision is silent otherwise: the port opens, the wrong driver
                        answers, and it reads as the radio refusing to connect. */}
                    {clashingPorts.size > 0 && (
                      <p className="mt-1 text-[11px] text-accent-warning">
                        ⚠ {[...clashingPorts].join(', ')} {clashingPorts.size === 1 ? 'is claimed' : 'are claimed'} by more than
                        one device — usually virtual-port software sitting on a number Windows then gave to
                        something else. Renumber one of them in Device Manager (Port Settings → Advanced),
                        or connecting may reach the wrong device.
                      </p>
                    )}
                  </div>
                  <div>
                    <label className="block text-xs text-dark-300 mb-1 font-ui">Baud Rate</label>
                    <select
                      value={hamlibConfig.baudRate}
                      onChange={(e) => updateHamlibConfig({ baudRate: parseInt(e.target.value) })}
                      className="w-full px-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-primary/50"
                    >
                      {HAMLIB_BAUD_RATES.map((rate) => (
                        <option key={rate} value={rate}>{rate}</option>
                      ))}
                    </select>
                  </div>
                </div>

                <div className="grid grid-cols-4 gap-2">
                  <div>
                    <label className="block text-xs text-dark-300 mb-1 font-ui">Data Bits</label>
                    <select
                      value={hamlibConfig.dataBits}
                      onChange={(e) => updateHamlibConfig({ dataBits: parseInt(e.target.value) as HamlibDataBits })}
                      className="w-full px-2 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-primary/50"
                    >
                      <option value={5}>5</option>
                      <option value={6}>6</option>
                      <option value={7}>7</option>
                      <option value={8}>8</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs text-dark-300 mb-1 font-ui">Stop Bits</label>
                    <select
                      value={hamlibConfig.stopBits}
                      onChange={(e) => updateHamlibConfig({ stopBits: parseInt(e.target.value) as HamlibStopBits })}
                      className="w-full px-2 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-primary/50"
                    >
                      <option value={1}>1</option>
                      <option value={2}>2</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs text-dark-300 mb-1 font-ui">Flow</label>
                    <select
                      value={hamlibConfig.flowControl}
                      onChange={(e) => updateHamlibConfig({ flowControl: e.target.value as HamlibFlowControl })}
                      className="w-full px-2 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-primary/50"
                    >
                      <option value="None">None</option>
                      <option value="Hardware">HW</option>
                      <option value="Software">SW</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs text-dark-300 mb-1 font-ui">Parity</label>
                    <select
                      value={hamlibConfig.parity}
                      onChange={(e) => updateHamlibConfig({ parity: e.target.value as HamlibParity })}
                      className="w-full px-2 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-primary/50"
                    >
                      <option value="None">None</option>
                      <option value="Even">Even</option>
                      <option value="Odd">Odd</option>
                      <option value="Mark">Mark</option>
                      <option value="Space">Space</option>
                    </select>
                  </div>
                </div>

              </div>
            )}

            {/* Network Settings */}
            {hamlibConfig.connectionType === "Network" && (
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-xs text-dark-300 mb-1 font-ui">Hostname</label>
                  <input
                    type="text"
                    value={hamlibConfig.hostname || ""}
                    onChange={(e) => updateHamlibConfig({ hostname: e.target.value })}
                    placeholder="localhost"
                    className="w-full px-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-primary/50"
                  />
                </div>
                <div>
                  <label className="block text-xs text-dark-300 mb-1 font-ui">Port</label>
                  <input
                    type="number"
                    value={hamlibConfig.networkPort}
                    onChange={(e) => updateHamlibConfig({ networkPort: parseInt(e.target.value) || 4532 })}
                    placeholder="4532"
                    className="w-full px-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-primary/50"
                  />
                </div>
              </div>
            )}

            {/* Advanced Options Toggle */}
            <button
              onClick={() => setShowAdvanced(!showAdvanced)}
              className="flex items-center gap-1 text-xs text-dark-300 hover:text-dark-200 transition-colors font-ui"
            >
              {showAdvanced ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
              Advanced Options
            </button>

            {/* Advanced Options */}
            {showAdvanced && (
              <div className="space-y-3 pt-2 border-t border-glass-100">
                {/* PTT: read-only for us. Lives here, beside the "Get PTT" toggle that
                    governs it, rather than in the main form where it looked like
                    something you had to set up before the rig would work. */}
                <div>
                  <label className="block text-xs text-dark-300 mb-1 font-ui">PTT Reporting</label>
                  <select
                    value={hamlibConfig.pttType === "Rig" ? "Rig" : "None"}
                    onChange={(e) => updateHamlibConfig({ pttType: e.target.value as HamlibPttType, pttPort: undefined })}
                    className="w-full px-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-primary/50"
                  >
                    <option value="Rig">Read over CAT</option>
                    <option value="None">Don't read</option>
                  </select>
                  <p className="mt-1 text-[11px] text-dark-300">
                    SDRLogger+ only <em>reads</em> PTT, to show whether you're transmitting — it never keys your radio.
                    That's why there's no DTR/RTS option here: those line-based methods gain you nothing when nothing
                    transmits, and they're what many interfaces key the rig from. Turn this off with <span className="text-dark-100">Get PTT</span> below.
                  </p>
                </div>

                <div className="text-xs text-dark-300 mb-2 font-ui">Feature Toggles</div>
                <div className="grid grid-cols-2 gap-2">
                  <FeatureToggle
                    label="Get Frequency"
                    checked={hamlibConfig.getFrequency}
                    onChange={(v) => updateHamlibConfig({ getFrequency: v })}
                    disabled={hamlibCaps ? !hamlibCaps.canGetFreq : false}
                  />
                  <FeatureToggle
                    label="Get Mode"
                    checked={hamlibConfig.getMode}
                    onChange={(v) => updateHamlibConfig({ getMode: v })}
                    disabled={hamlibCaps ? !hamlibCaps.canGetMode : false}
                  />
                  <FeatureToggle
                    label="Get VFO"
                    checked={hamlibConfig.getVfo}
                    onChange={(v) => updateHamlibConfig({ getVfo: v })}
                    disabled={hamlibCaps ? !hamlibCaps.canGetVfo : false}
                  />
                  <FeatureToggle
                    label="Get PTT"
                    checked={hamlibConfig.getPtt}
                    onChange={(v) => updateHamlibConfig({ getPtt: v })}
                    disabled={hamlibCaps ? !hamlibCaps.canGetPtt : false}
                  />
                  <FeatureToggle
                    label="Get Power"
                    checked={hamlibConfig.getPower}
                    onChange={(v) => updateHamlibConfig({ getPower: v })}
                    disabled={hamlibCaps ? !hamlibCaps.canGetPower : false}
                  />
                  <FeatureToggle
                    label="Get RIT"
                    checked={hamlibConfig.getRit}
                    onChange={(v) => updateHamlibConfig({ getRit: v })}
                    disabled={hamlibCaps ? !hamlibCaps.canGetRit : false}
                  />
                  <FeatureToggle
                    label="Get XIT"
                    checked={hamlibConfig.getXit}
                    onChange={(v) => updateHamlibConfig({ getXit: v })}
                    disabled={hamlibCaps ? !hamlibCaps.canGetXit : false}
                  />
                  <FeatureToggle
                    label="Get Key Speed"
                    checked={hamlibConfig.getKeySpeed}
                    onChange={(v) => updateHamlibConfig({ getKeySpeed: v })}
                    disabled={hamlibCaps ? !hamlibCaps.canGetKeySpeed : false}
                  />
                </div>

                <div>
                  <label className="block text-xs text-dark-300 mb-1 font-ui">Poll Interval (ms)</label>
                  <input
                    type="number"
                    value={hamlibConfig.pollIntervalMs}
                    onChange={(e) => updateHamlibConfig({ pollIntervalMs: parseInt(e.target.value) || 250 })}
                    min={50}
                    max={5000}
                    className="w-32 px-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-primary/50"
                  />
                </div>
              </div>
            )}

            {/* Action Buttons */}
            <div className="flex gap-2 pt-2">
              <button
                onClick={handleAddHamlib}
                disabled={!hamlibConfig.modelId || (hamlibConfig.connectionType === "Serial" && !hamlibConfig.serialPort) || (hamlibConfig.connectionType === "Network" && !hamlibConfig.hostname)}
                className="flex-1 px-4 py-2 text-sm font-medium font-ui flex items-center justify-center gap-2 bg-accent-primary/20 text-accent-primary rounded-lg hover:bg-accent-primary/30 transition-all disabled:opacity-50"
              >
                <Plus className="w-4 h-4" />
                Add
              </button>
              <button
                onClick={() => setShowHamlibForm(false)}
                className="px-4 py-2 text-sm font-medium font-ui bg-dark-700 text-dark-300 rounded-lg hover:bg-dark-600 transition-all"
              >
                Cancel
              </button>
            </div>
          </div>
        )}

        {/* TCI Connection Form */}
        {showTciForm && (
          <div className="bg-dark-700/50 rounded-lg p-4 border border-accent-secondary/30 space-y-3">
            <div className="text-xs text-accent-secondary uppercase tracking-wider mb-2 font-ui">
              {editingRadioId ? 'Edit TCI Rig' : 'Connect to TCI Server'}
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs text-dark-300 mb-1 font-ui">Host</label>
                <input
                  type="text"
                  value={tciSettings.host}
                  onChange={(e) => updateTciSettings({ host: e.target.value })}
                  placeholder="localhost"
                  className="w-full px-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-secondary/50"
                />
              </div>
              <div>
                <label className="block text-xs text-dark-300 mb-1 font-ui">Port</label>
                <input
                  type="number"
                  value={tciSettings.port}
                  onChange={(e) => updateTciSettings({ port: parseInt(e.target.value) || 50001 })}
                  placeholder="50001"
                  className="w-full px-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-secondary/50"
                />
              </div>
            </div>
            <div>
              <label className="block text-xs text-dark-300 mb-1 font-ui">Name (optional)</label>
              <input
                type="text"
                value={tciSettings.name}
                onChange={(e) => updateTciSettings({ name: e.target.value })}
                placeholder="My TCI Radio"
                className="w-full px-3 py-2 bg-dark-800 border border-glass-100 rounded-lg text-sm text-dark-200 font-mono focus:outline-none focus:border-accent-secondary/50"
              />
            </div>
            {tciTestResult && (
              <div className={`text-xs rounded-lg px-3 py-2 border ${tciTestResult.ok ? 'bg-accent-primary/10 border-accent-primary/30 text-accent-primary' : 'bg-accent-warning/10 border-accent-warning/40 text-accent-warning'}`}>
                {tciTestResult.ok ? '✓ ' : '⚠ '}{tciTestResult.message}
              </div>
            )}
            <div className="flex gap-2 pt-2">
              <button
                onClick={handleTestTci}
                disabled={!tciSettings.host || isTestingTci}
                title="Check that a TCI server is reachable at this host:port"
                className="px-4 py-2 text-sm font-medium font-ui flex items-center justify-center gap-2 bg-dark-700 text-dark-200 rounded-lg hover:bg-dark-600 transition-all disabled:opacity-50"
              >
                <Wifi className={`w-4 h-4 ${isTestingTci ? 'animate-pulse' : ''}`} />
                Test
              </button>
              <button
                onClick={() => handleSaveTci()}
                disabled={!tciSettings.host || isTestingTci}
                className="flex-1 px-4 py-2 text-sm font-medium font-ui flex items-center justify-center gap-2 bg-accent-secondary/20 text-accent-secondary rounded-lg hover:bg-accent-secondary/30 transition-all disabled:opacity-50"
              >
                <Plus className="w-4 h-4" />
                {editingRadioId ? 'Save' : 'Add'}
              </button>
              <button
                onClick={closeTciForm}
                className="px-4 py-2 text-sm font-medium font-ui bg-dark-700 text-dark-300 rounded-lg hover:bg-dark-600 transition-all"
              >
                Cancel
              </button>
            </div>
            {tciTestResult && !tciTestResult.ok && !isTestingTci && (
              <button
                onClick={() => handleSaveTci(true)}
                className="w-full px-4 py-2 text-xs font-medium font-ui bg-accent-warning/15 text-accent-warning rounded-lg hover:bg-accent-warning/25 transition-all"
              >
                {editingRadioId ? 'Save anyway' : 'Add anyway'} (skip the check)
              </button>
            )}
          </div>
        )}

        {/* Startup reconnect setting */}
        {!showTciForm && !showHamlibForm && (
          <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
            <input
              type="checkbox"
              checked={reconnectLastOnStartup}
              onChange={(e) => {
                updateRadioSettings({ reconnectLastOnStartup: e.target.checked });
                saveSettings();
              }}
              className="w-4 h-4 rounded bg-dark-800 border-glass-100 text-accent-primary focus:ring-accent-primary/50"
            />
            <span className="text-dark-300 font-ui">Reconnect last radio on startup</span>
          </label>
        )}

        {/* Saved Rig - only show if we have a configured rig and not showing forms */}
        {radios.length > 0 && !showTciForm && !showHamlibForm ? (
          <div>
            <div className="text-xs text-dark-300 uppercase tracking-wider mb-2 font-ui">
              Saved Rig
            </div>
            <div className="space-y-2">
              {radios.map((radio) => {
                const connectionState = radioConnectionStates.get(radio.id);
                const isConnecting = connectionState === "Connecting";

                return (
                  <div
                    key={radio.id}
                    className="flex items-center justify-between p-3 bg-dark-700/50 rounded-lg border border-glass-100"
                  >
                    <div className="flex items-center gap-3">
                      <div
                        className={`p-2 rounded-lg ${
                          radio.type === "Hamlib"
                            ? "bg-accent-primary/20"
                            : "bg-accent-secondary/20"
                        }`}
                      >
                        {radio.type === "Hamlib" ? (
                          <Settings className="w-4 h-4 text-accent-primary" />
                        ) : (
                          <Radio className="w-4 h-4 text-accent-secondary" />
                        )}
                      </div>
                      <div>
                        <div className="text-sm font-medium text-dark-200 font-ui flex items-center gap-1.5">
                          {radio.nickname || radio.model}
                          {autoReconnect && autoConnectRigId === radio.id && (
                            <span title="Auto-connect enabled">
                              <RefreshCw className="w-3 h-3 text-accent-primary" />
                            </span>
                          )}
                        </div>
                        <div className="text-xs text-dark-300 font-mono">
                          {radio.ipAddress}{radio.port ? `:${radio.port}` : ""}
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <button
                        onClick={() => handleToggleAutoReconnectForRig(radio.id)}
                        title={autoReconnect && autoConnectRigId === radio.id ? "Disable auto-reconnect" : "Enable auto-reconnect"}
                        className={`p-1.5 rounded transition-all ${
                          autoReconnect && autoConnectRigId === radio.id
                            ? "bg-accent-primary/20 text-accent-primary"
                            : "bg-dark-700 text-dark-300 hover:text-dark-200"
                        }`}
                      >
                        <RefreshCw className={`w-3.5 h-3.5 ${autoReconnect && autoConnectRigId === radio.id ? "" : "opacity-50"}`} />
                      </button>
                      {radio.type === 'Tci' && (
                        <button
                          onClick={() => handleEditTci(radio)}
                          title="Edit rig (name / host / port)"
                          className="px-3 py-1.5 text-xs font-medium font-ui flex items-center gap-1.5 bg-dark-700 text-dark-300 rounded-lg hover:text-dark-200 transition-all"
                        >
                          <Pencil className="w-3.5 h-3.5" />
                          Edit
                        </button>
                      )}
                      <button
                        onClick={() => handleConnect(radio.id)}
                        disabled={isConnecting}
                        className="px-3 py-1.5 text-xs font-medium font-ui flex items-center gap-1.5 bg-accent-primary/20 text-accent-primary rounded-lg hover:bg-accent-primary/30 transition-all disabled:opacity-50"
                      >
                        {isConnecting ? (
                          <>
                            <Wifi className="w-3.5 h-3.5 animate-pulse" />
                            Connecting...
                          </>
                        ) : (
                          <>
                            <Power className="w-3.5 h-3.5" />
                            Connect
                          </>
                        )}
                      </button>
                      <button
                        onClick={() => handleRemoveRig(radio.id)}
                        className="px-3 py-1.5 text-xs font-medium font-ui flex items-center gap-1.5 bg-accent-danger/20 text-accent-danger rounded-lg hover:bg-accent-danger/30 transition-all"
                        title="Remove saved rig"
                      >
                        <PowerOff className="w-3.5 h-3.5" />
                        Remove
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ) : !showTciForm && !showHamlibForm ? (
          <div className="flex flex-col items-center justify-center py-8 text-dark-300">
            <WifiOff className="w-8 h-8 mb-3 opacity-50" />
            <p className="text-sm text-center font-ui">No rig configured</p>
            <p className="text-xs text-dark-400 mt-1 text-center font-ui">
              Select a radio type above to configure
            </p>
          </div>
        ) : null}
      </div>
    </div>
  );
}

interface FeatureToggleProps {
  label: string;
  checked: boolean;
  onChange: (value: boolean) => void;
  disabled?: boolean;
}

function FeatureToggle({ label, checked, onChange, disabled }: FeatureToggleProps) {
  return (
    <label className={`flex items-center gap-2 text-sm ${disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}`}>
      <input
        type="checkbox"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
        disabled={disabled}
        className="w-4 h-4 rounded bg-dark-800 border-glass-100 text-accent-primary focus:ring-accent-primary/50"
      />
      <span className="text-dark-200 font-ui">{label}</span>
    </label>
  );
}


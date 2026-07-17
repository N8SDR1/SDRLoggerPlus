import { useEffect, useRef, useCallback, useState } from 'react';
import { Layout, Model, TabNode, TabSetNode, BorderNode, ITabSetRenderValues, Actions, DockLocation } from 'flexlayout-react';
import { X, LayoutGrid, Plus, Search, NotebookPen, ScrollText, RadioTower, Navigation2, Earth, RadioReceiver, ContactRound, Trophy, PanelTop, Satellite, Plane, BotMessageSquare, TentTree, Signal, AudioWaveform, Activity, TrendingUp, Gauge, Map, Swords, Grid3x3 } from 'lucide-react';
import { StatusBar } from './components/StatusBar';
import { WeatherAlertBanner } from './components/WeatherAlertBanner';
import { Toasts } from './components/Toasts';
import { SettingsPanel } from './components/SettingsPanel';
import { ConnectionOverlay } from './components/ConnectionOverlay';
import { SetupWizard } from './components/SetupWizard';
import { PluginErrorBoundary } from './components/PluginErrorBoundary';
import { useSignalRConnection } from './hooks/useSignalR';
import { LogEntryPlugin, LogHistoryPlugin, ClusterPlugin, RotatorPlugin, GlobePlugin, RigPlugin, QrzProfilePlugin, ContestsPlugin, ContestEntryPlugin, MultNeededPlugin, ContestBandmapPlugin, HeaderPlugin, DXpeditionsPlugin, ChatAiPlugin, POTAPlugin, PropagationPanelPlugin, CwKeyerPlugin, PanadapterPlugin, StatisticsPlugin, SatPlugin, MeterPlugin, MapPlugin } from './plugins';
import { useLayoutStore, defaultLayout } from './store/layoutStore';
import { useSettingsStore } from './store/settingsStore';
import { useSetupStore } from './store/setupStore';
import { useAppStore } from './store/appStore';
import { useTheme } from './hooks/useTheme';
import { useOutOfBandAlert } from './hooks/useOutOfBandAlert';

import 'flexlayout-react/style/dark.css';

// Plugin registry
type PluginCategory = 'Logging' | 'Maps & Navigation' | 'Radio & Equipment' | 'Information' | 'Display';

const CATEGORY_ORDER: PluginCategory[] = ['Logging', 'Maps & Navigation', 'Radio & Equipment', 'Information', 'Display'];

interface PluginDef {
  name: string;
  icon: React.ReactNode;
  component: React.ComponentType;
  category: PluginCategory;
  tags?: string[];
}

const PLUGINS: Record<string, PluginDef> = {
  'log-entry': {
    name: 'Log Entry',
    icon: <NotebookPen className="w-4 h-4" />,
    component: LogEntryPlugin,
    category: 'Logging',
  },
  'log-history': {
    name: 'Log History',
    icon: <ScrollText className="w-4 h-4" />,
    component: LogHistoryPlugin,
    category: 'Logging',
  },
  'cluster': {
    name: 'DX Cluster',
    icon: <RadioTower className="w-4 h-4" />,
    component: ClusterPlugin,
    category: 'Information',
    tags: ['spots', 'dx'],
  },
  'rotator': {
    name: 'Rotator',
    icon: <Navigation2 className="w-4 h-4" />,
    component: RotatorPlugin,
    category: 'Radio & Equipment',
    tags: ['antenna', 'bearing'],
  },
  'globe-3d': {
    name: '3D Globe',
    icon: <Earth className="w-4 h-4" />,
    component: GlobePlugin,
    category: 'Maps & Navigation',
    tags: ['map', 'earth'],
  },
  'rig': {
    name: 'Rig',
    icon: <RadioReceiver className="w-4 h-4" />,
    component: RigPlugin,
    category: 'Radio & Equipment',
    tags: ['transceiver', 'radio'],
  },
  'qrz-profile': {
    name: 'QRZ Profile',
    icon: <ContactRound className="w-4 h-4" />,
    component: QrzProfilePlugin,
    category: 'Information',
    tags: ['callsign', 'lookup'],
  },
  'contests': {
    name: 'Contests',
    icon: <Trophy className="w-4 h-4" />,
    component: ContestsPlugin,
    category: 'Information',
  },
  'contest-entry': {
    name: 'Contest Entry',
    icon: <Swords className="w-4 h-4" />,
    component: ContestEntryPlugin,
    category: 'Logging',
    tags: ['contest', 'dupe', 'serial', 'score', 'exchange'],
  },
  'contest-mults': {
    name: 'Multipliers',
    icon: <Grid3x3 className="w-4 h-4" />,
    component: MultNeededPlugin,
    category: 'Logging',
    tags: ['contest', 'multipliers', 'zones', 'mults'],
  },
  'contest-bandmap': {
    name: 'Bandmap',
    icon: <Map className="w-4 h-4" />,
    component: ContestBandmapPlugin,
    category: 'Logging',
    tags: ['contest', 'bandmap', 'spots', 'dupe', 'mult'],
  },
  'header-bar': {
    name: 'Header Bar',
    icon: <PanelTop className="w-4 h-4" />,
    component: HeaderPlugin,
    category: 'Display',
    tags: ['time', 'utc', 'callsign'],
  },
  'sat-controller': {
    name: 'S.A.T. Controller',
    icon: <Satellite className="w-4 h-4" />,
    component: SatPlugin,
    category: 'Radio & Equipment',
    tags: ['satellite', 'sat', 'csn'],
  },
  'dxpeditions': {
    name: 'DXpeditions',
    icon: <Plane className="w-4 h-4" />,
    component: DXpeditionsPlugin,
    category: 'Information',
    tags: ['dx', 'expedition'],
  },
  'chat-ai': {
    name: 'Chat AI',
    icon: <BotMessageSquare className="w-4 h-4" />,
    component: ChatAiPlugin,
    category: 'Information',
    tags: ['assistant', 'ai'],
  },
  'pota': {
    name: 'POTA',
    icon: <TentTree className="w-4 h-4" />,
    component: POTAPlugin,
    category: 'Maps & Navigation',
    tags: ['parks', 'activations'],
  },
  'propagation': {
    name: 'Propagation',
    icon: <Signal className="w-4 h-4" />,
    component: PropagationPanelPlugin,
    category: 'Information',
    tags: ['bands', 'muf', 'hf'],
  },
  'cw-keyer': {
    name: 'CW Keyer',
    icon: <AudioWaveform className="w-4 h-4" />,
    component: CwKeyerPlugin,
    category: 'Radio & Equipment',
    tags: ['morse', 'cw'],
  },
  'meters': {
    name: 'Meters',
    icon: <Gauge className="w-4 h-4" />,
    component: MeterPlugin,
    category: 'Radio & Equipment',
    tags: ['meter', 'smeter', 'swr', 'power', 'tci'],
  },
  'panadapter': {
    name: 'Panadapter',
    icon: <Activity className="w-4 h-4" />,
    component: PanadapterPlugin,
    category: 'Radio & Equipment',
    tags: ['spectrum', 'waterfall', 'fft', 'sdr', 'panadapter'],
  },
  'statistics': {
    name: 'Statistics',
    icon: <TrendingUp className="w-4 h-4" />,
    component: StatisticsPlugin,
    category: 'Logging',
    tags: ['dxcc', 'awards', 'stats', 'countries', 'bands'],
  },
  'map': {
    name: '2D Map',
    icon: <Map className="w-4 h-4" />,
    component: MapPlugin,
    category: 'Maps & Navigation',
    tags: ['map', '2d', 'leaflet', 'spots'],
  },
};


/**
 * Drops layout tabs whose component no longer exists in PLUGINS (panels removed
 * from the app) so saved layouts don't render "Unknown component" husks.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function sanitizeLayout(json: any): any {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const prune = (node: any): any => {
    if (!node || typeof node !== 'object') return node;
    if (node.type === 'tab' && node.component && !PLUGINS[node.component]) return null;
    if (Array.isArray(node.children)) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const children = node.children.map(prune).filter((c: any) => c !== null);
      return { ...node, children };
    }
    return node;
  };
  return { ...json, layout: prune(json.layout) };
}

export function App() {
  const layoutRef = useRef<Layout>(null);
  const { layout, setLayout, resetLayout: resetLayoutStore, loadFromBackend: loadLayout, syncToBackendSync } = useLayoutStore();
  const { loadSettings, openSettings, settings } = useSettingsStore();
  const { fetchStatus, status: setupStatus, isLoading: setupLoading } = useSetupStore();
  const { setStationInfo, setDatabaseConnected } = useAppStore();
  const [model, setModel] = useState<Model>(() => Model.fromJson(sanitizeLayout(layout)));
  const [showPanelPicker, setShowPanelPicker] = useState(false);
  const [targetTabSetId, setTargetTabSetId] = useState<string | null>(null);
  const [panelFilter, setPanelFilter] = useState('');

  // Apply theme from settings (dark/light/system)
  useTheme();

  // Out-of-band VFO warnings (ITU band plan for the configured region)
  useOutOfBandAlert();

  // Check setup status on mount (for status display, not blocking)
  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  // Poll database connection status periodically (every 10 seconds)
  useEffect(() => {
    const checkDbHealth = async () => {
      try {
        const response = await fetch('/api/health');
        if (response.ok) {
          const data = await response.json();
          setDatabaseConnected(data.databaseConnected ?? false);
        }
      } catch (error) {
        console.error('Failed to check database health:', error);
      }
    };

    // Initial check
    checkDbHealth();

    // Poll every 10 seconds
    const interval = setInterval(checkDbHealth, 10000);

    return () => clearInterval(interval);
  }, [setDatabaseConnected]);

  // Initialize SignalR connection (only called here, not in plugins)
  useSignalRConnection();

  // Load settings and layout from the backend on mount (will gracefully fail if not connected)
  useEffect(() => {
    loadSettings();
    loadLayout();
  }, [loadSettings, loadLayout]);

  // Sync station info to app store whenever settings change
  // This ensures map/globe components have access to station coordinates
  // even when the settings panel is not open
  useEffect(() => {
    if (settings.station.callsign || settings.station.gridSquare) {
      setStationInfo(settings.station.callsign, settings.station.gridSquare);
    }
  }, [settings.station.callsign, settings.station.gridSquare, setStationInfo]);

  // Listen for Electron menu commands (Settings via Cmd+,)
  useEffect(() => {
    // Check if running in Electron with IPC available
    if (window.electronAPI) {
      window.electronAPI.onOpenSettings(() => {
        openSettings();
      });

      return () => {
        window.electronAPI?.removeOpenSettingsListener();
      };
    }
  }, [openSettings]);

  // Update model when layout store changes (e.g., from backend load)
  useEffect(() => {
    setModel(Model.fromJson(sanitizeLayout(layout)));
  }, [layout]);

  // Get list of existing plugin components in the layout
  const getExistingPlugins = useCallback((): Set<string> => {
    const existing = new Set<string>();
    model.visitNodes((node) => {
      if (node.getType() === 'tab') {
        const tabNode = node as TabNode;
        const component = tabNode.getComponent();
        if (component) {
          existing.add(component);
        }
      }
    });
    return existing;
  }, [model]);

  // Debounced save to the backend
  const saveTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pendingLayoutRef = useRef<Model | null>(null);

  // Save layout immediately (synchronous, for shutdown/unload)
  const saveLayoutImmediately = useCallback(() => {
    if (pendingLayoutRef.current) {
      const layoutJson = pendingLayoutRef.current.toJson();
      syncToBackendSync(layoutJson);
      pendingLayoutRef.current = null;
      if (saveTimeoutRef.current) {
        clearTimeout(saveTimeoutRef.current);
        saveTimeoutRef.current = null;
      }
    }
  }, [syncToBackendSync]);

  // Save layout changes (debounced).
  // pendingLayoutRef is left set to the LATEST model even after the debounced
  // async sync fires — that's what makes shutdown-save reliable. The async
  // syncToBackend from setLayout is fire-and-forget, so if the user closes
  // the browser within the fetch window (a few hundred ms typically), the
  // response never lands. Keeping pendingLayoutRef non-null means the
  // beforeunload handler can always fall back to a synchronous save via
  // saveLayoutImmediately → syncToBackendSync (sendBeacon / sync XHR),
  // which IS guaranteed to reach the backend before the tab dies.
  const handleModelChange = useCallback((newModel: Model) => {
    setModel(newModel);
    pendingLayoutRef.current = newModel;

    // Debounce the save - wait 1 second after last change
    if (saveTimeoutRef.current) {
      clearTimeout(saveTimeoutRef.current);
    }
    saveTimeoutRef.current = setTimeout(() => {
      setLayout(newModel.toJson());
      // NOTE: intentionally DO NOT clear pendingLayoutRef.current here.
      // A redundant sync on beforeunload costs one small sendBeacon call
      // but eliminates the "closed the tab before the async fetch landed"
      // data-loss window that used to lose panel drags made in the last
      // ~second before shutdown.
    }, 1000);
  }, [setLayout]);

  // Save layout before app closes (browser/Electron window close)
  useEffect(() => {
    const handleBeforeUnload = () => {
      saveLayoutImmediately();
    };

    window.addEventListener('beforeunload', handleBeforeUnload);
    return () => {
      window.removeEventListener('beforeunload', handleBeforeUnload);
      // Also save on cleanup
      saveLayoutImmediately();
    };
  }, [saveLayoutImmediately]);

  // Factory function to render components
  const factory = useCallback((node: TabNode) => {
    const component = node.getComponent();
    const plugin = PLUGINS[component || ''];

    if (plugin) {
      const Component = plugin.component;
      return (
        <PluginErrorBoundary pluginId={component || 'unknown'}>
          <Component />
        </PluginErrorBoundary>
      );
    }

    return (
      <div className="flex items-center justify-center h-full text-gray-500">
        Unknown component: {component}
      </div>
    );
  }, []);

  // Add a new panel to a specific tabset
  const handleAddPanel = useCallback((pluginId: string) => {
    const plugin = PLUGINS[pluginId];
    if (!plugin || !targetTabSetId) return;

    model.doAction(
      Actions.addNode(
        {
          type: 'tab',
          name: plugin.name,
          component: pluginId,
        },
        targetTabSetId,
        DockLocation.CENTER,
        -1,
        true
      )
    );

    setShowPanelPicker(false);
    setTargetTabSetId(null);
    setPanelFilter('');
  }, [model, targetTabSetId]);

  // Custom tab rendering
  const onRenderTab = useCallback((node: TabNode, renderValues: { leading: React.ReactNode; content: React.ReactNode }) => {
    const component = node.getComponent();
    const plugin = PLUGINS[component || ''];

    if (plugin) {
      renderValues.leading = (
        <span className="mr-2 text-accent-secondary">{plugin.icon}</span>
      );
    }
  }, []);

  // Custom tabset rendering - add + button to each tabset
  const onRenderTabSet = useCallback((node: TabSetNode | BorderNode, renderValues: ITabSetRenderValues) => {
    if (node instanceof TabSetNode) {
      renderValues.stickyButtons.push(
        <button
          key="add-panel"
          title="Add panel to this tabset"
          className="flexlayout__tab_toolbar_button"
          onClick={() => {
            setTargetTabSetId(node.getId());
            setShowPanelPicker(true);
          }}
        >
          <Plus className="w-3.5 h-3.5" />
        </button>
      );
    }
  }, []);

  // Reset layout to default
  const handleResetLayout = useCallback(() => {
    setModel(Model.fromJson(defaultLayout));
    resetLayoutStore();
  }, [resetLayoutStore]);

  const showSetupWizard = !setupLoading && setupStatus !== null && !setupStatus.isConfigured;

  return (
    <div className="h-screen flex flex-col bg-dark-900 text-gray-100 crt-scanlines relative">
      {/* Setup Wizard - blocks UI when not configured */}
      {showSetupWizard && (
        <SetupWizard onComplete={() => fetchStatus()} />
      )}

      {/* Weather alert strip pinned to the very top — v1.x parity. Sits
          above the header so a real lightning / high-wind alert is the
          first thing the operator sees, and pushes the rest of the app
          down (rather than overlapping) so nothing behind it is hidden. */}
      <WeatherAlertBanner />

      <main className="flex-1 relative overflow-hidden">
        <Layout
          ref={layoutRef}
          model={model}
          factory={factory}
          onModelChange={handleModelChange}
          onRenderTab={onRenderTab}
          onRenderTabSet={onRenderTabSet}
          classNameMapper={(className) => {
            return className;
          }}
        />

        {/* Panel Picker Modal */}
        {showPanelPicker && (
          <div className="absolute inset-0 bg-dark-900/85 backdrop-blur-sm flex items-center justify-center z-50">
            <div className="glass-panel w-[32rem] max-h-[80vh] flex flex-col animate-fade-in">
              <div className="flex items-center justify-between px-4 py-3 border-b border-glass-100">
                <div className="flex items-center gap-2">
                  <LayoutGrid className="w-5 h-5 text-accent-secondary" />
                  <h3 className="font-semibold text-accent-success font-ui text-sm tracking-wide uppercase">Add Panel</h3>
                </div>
                <button
                  onClick={() => {
                    setShowPanelPicker(false);
                    setTargetTabSetId(null);
                    setPanelFilter('');
                  }}
                  className="p-1 hover:bg-dark-600 rounded transition-colors text-dark-300 hover:text-accent-danger"
                >
                  <X className="w-4 h-4" />
                </button>
              </div>

              <div className="px-4 pt-3 pb-2">
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-dark-300" />
                  <input
                    type="text"
                    placeholder="Filter panels..."
                    value={panelFilter}
                    onChange={(e) => setPanelFilter(e.target.value)}
                    autoFocus
                    className="w-full pl-9 pr-3 py-2 bg-dark-700/50 border border-glass-100 rounded text-sm font-ui text-gray-100 placeholder-dark-300 focus:outline-none focus:border-accent-secondary/50"
                  />
                </div>
              </div>

              <div className="flex-1 overflow-y-auto px-4 pb-4">
                {(() => {
                  const existingPlugins = getExistingPlugins();
                  const filterLower = panelFilter.toLowerCase();
                  const availablePlugins = Object.entries(PLUGINS).filter(([id, plugin]) => {
                    if (existingPlugins.has(id)) return false;
                    if (!filterLower) return true;
                    return (
                      plugin.name.toLowerCase().includes(filterLower) ||
                      id.toLowerCase().includes(filterLower) ||
                      plugin.category.toLowerCase().includes(filterLower) ||
                      (plugin.tags?.some(tag => tag.toLowerCase().includes(filterLower)) ?? false)
                    );
                  });

                  if (availablePlugins.length === 0) {
                    return (
                      <div className="text-center py-6 text-dark-300">
                        {panelFilter ? 'No panels match your filter' : 'All panels have been added to the layout'}
                      </div>
                    );
                  }

                  // Group by category
                  const grouped: Partial<Record<PluginCategory, [string, PluginDef][]>> = {};
                  for (const entry of availablePlugins) {
                    const cat = entry[1].category;
                    if (!grouped[cat]) grouped[cat] = [];
                    grouped[cat]!.push(entry);
                  }

                  return CATEGORY_ORDER
                    .filter(cat => grouped[cat])
                    .map(cat => (
                      <div key={cat} className="mt-3 first:mt-0">
                        <h4 className="text-xs font-ui font-semibold text-dark-300 uppercase tracking-wider mb-2">{cat}</h4>
                        <div className="grid grid-cols-3 gap-2">
                          {grouped[cat]!.map(([id, plugin]) => (
                            <button
                              key={id}
                              onClick={() => handleAddPanel(id)}
                              className="glass-button flex flex-col items-center gap-2 p-3 hover:border-accent-secondary/40"
                            >
                              <span className="text-accent-secondary">{plugin.icon}</span>
                              <span className="text-xs font-ui">{plugin.name}</span>
                            </button>
                          ))}
                        </div>
                      </div>
                    ));
                })()}
              </div>

              <div className="px-4 py-3 border-t border-glass-100">
                <button
                  onClick={handleResetLayout}
                  className="text-sm text-dark-300 hover:text-accent-danger transition-colors font-ui"
                >
                  Reset to default layout
                </button>
              </div>
            </div>
          </div>
        )}
      </main>

      <StatusBar />

      {/* Settings Panel (Modal) */}
      <SettingsPanel />

      {/* Connection Overlay - blocks UI when disconnected */}
      <ConnectionOverlay />

      {/* Transient notifications (OOB warnings, band openings, …) */}
      <Toasts />
    </div>
  );
}

export default App;

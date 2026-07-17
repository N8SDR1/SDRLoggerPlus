import { Radio, MapPin, Clock, Settings, ChevronDown, Power } from 'lucide-react';
import { useAppStore } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import { useRigConnection } from '../hooks/useRigConnection';
import { useEffect, useRef, useState } from 'react';
import { APP_VERSION } from '../version';
import { AboutDialog, type TabId } from './AboutDialog';

export function StatusBar() {
  const { stationCallsign, stationGrid, rigStatus } = useAppStore();
  const { openSettings } = useSettingsStore();
  const { rigs, switchTo, disconnect, pillRigName, pillConnected } = useRigConnection();
  const [currentTime, setCurrentTime] = useState(new Date());
  const [showAbout, setShowAbout] = useState(false);
  const [aboutTab, setAboutTab] = useState<TabId>('about');
  const [rigMenuOpen, setRigMenuOpen] = useState(false);
  const rigMenuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const timer = setInterval(() => setCurrentTime(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);

  // Close the rig switcher on any outside click.
  useEffect(() => {
    if (!rigMenuOpen) return;
    const onDown = (e: MouseEvent) => {
      if (rigMenuRef.current && !rigMenuRef.current.contains(e.target as Node)) {
        setRigMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', onDown);
    return () => document.removeEventListener('mousedown', onDown);
  }, [rigMenuOpen]);

  useEffect(() => {
    if (window.electronAPI?.onOpenAbout) {
      window.electronAPI.onOpenAbout(() => setShowAbout(true));
      return () => window.electronAPI?.removeOpenAboutListener?.();
    }
  }, []);

  // Settings → About → "Open the User Guide" fires this window event; open the
  // About dialog straight on the Help tab.
  useEffect(() => {
    const openHelp = () => { setAboutTab('help'); setShowAbout(true); };
    window.addEventListener('open-help-guide', openHelp);
    return () => window.removeEventListener('open-help-guide', openHelp);
  }, []);

  const formatUtcTime = (date: Date) => {
    return date.toISOString().slice(11, 19);
  };

  const formatFrequency = (freq: number) => {
    return (freq / 1000000).toFixed(3);
  };

  // Rig connection state + display name come from the shared useRigConnection
  // hook — the same rig (and name) the popover lists, so the pill and menu match.
  const rigConnected = pillConnected;
  const rigName = pillRigName || 'Rig';

  return (
    <>
    <div className="h-8 bg-dark-800/90 backdrop-blur-sm border-t border-glass-100 flex items-center justify-between px-4 text-sm font-ui">
      {/* Left side - Station info */}
      <div className="flex items-center gap-6">
        <button
          onClick={() => setShowAbout(true)}
          className="flex items-center gap-1.5 hover:bg-dark-600 rounded px-1.5 py-0.5 transition-colors group"
          title="About SDRLoggerPlus"
        >
          <img src="./sdrloggerplus-icon.png" alt="SDRLoggerPlus" className="w-4 h-4" />
          <span className="text-xs font-display text-dark-300 group-hover:text-accent-primary transition-colors">SDRLoggerPlus</span>
          <span className="text-[10px] font-mono text-dark-500">v{APP_VERSION}</span>
        </button>
        <div className="flex items-center gap-2">
          <Radio className="w-4 h-4 text-accent-primary" />
          <span className="font-mono font-bold text-accent-primary">{stationCallsign}</span>
        </div>

        <div className="flex items-center gap-2 text-dark-300">
          <MapPin className="w-3 h-3" />
          <span className="font-mono">{stationGrid}</span>
        </div>

        {rigStatus && (
          <div className="flex items-center gap-2">
            <span className="frequency-display text-accent-secondary">
              {formatFrequency(rigStatus.frequency)} MHz
            </span>
            <span className={`badge ${rigStatus.mode === 'CW' ? 'badge-cw' : rigStatus.mode === 'SSB' ? 'badge-ssb' : 'badge-ft8'}`}>
              {rigStatus.mode}
            </span>
            {rigStatus.isTransmitting && (
              <span className="px-2 py-0.5 bg-accent-danger/20 text-accent-danger rounded text-xs font-bold animate-pulse font-mono">
                TX
              </span>
            )}
          </div>
        )}
      </div>

      {/* Right side - Time and connection */}
      <div className="flex items-center gap-6">
        <div className="flex items-center gap-2 text-dark-300">
          <Clock className="w-3 h-3" />
          <span className="font-mono">{formatUtcTime(currentTime)} UTC</span>
        </div>

        <button
          onClick={openSettings}
          className="p-1 hover:bg-dark-600 rounded transition-colors text-gray-400 hover:text-accent-secondary"
          title="Settings"
        >
          <Settings className="w-4 h-4" />
        </button>

        <div className="relative" ref={rigMenuRef}>
          <button
            onClick={() => setRigMenuOpen((o) => !o)}
            className="flex items-center gap-2 hover:bg-dark-600 rounded px-1.5 py-0.5 transition-colors"
            title="Choose radio"
          >
            <Radio className={`w-4 h-4 ${rigConnected ? 'text-accent-success' : 'text-accent-danger'}`} />
            <span className={`text-xs font-mono ${rigConnected ? 'text-accent-success' : 'text-accent-danger'}`}>
              {rigName} {rigConnected ? 'Connected' : 'Disconnected'}
            </span>
            <ChevronDown className={`w-3 h-3 text-dark-400 transition-transform ${rigMenuOpen ? 'rotate-180' : ''}`} />
          </button>

          {rigMenuOpen && (
            <div className="absolute right-0 bottom-full mb-2 w-72 glass-panel p-2 z-[1001] shadow-xl">
              <div className="px-1.5 pb-1.5 mb-1 border-b border-glass-100/60 text-[10px] uppercase tracking-wider text-dark-400 font-ui">
                Radios
              </div>

              {rigs.length === 0 && (
                <div className="px-1.5 py-2 text-xs text-dark-300 leading-relaxed">
                  No rigs configured. Add one in the <span className="text-dark-200">RIG</span> panel.
                </div>
              )}

              <div className="space-y-0.5">
                {rigs.map((r) => (
                  <div
                    key={r.id}
                    className={`flex items-center gap-2.5 px-2 py-2 rounded-lg transition-colors group ${
                      r.connected ? 'bg-accent-success/10' : 'hover:bg-dark-600/60'
                    }`}
                  >
                    <span
                      className={`w-2 h-2 rounded-full flex-shrink-0 ${
                        r.connected
                          ? 'bg-accent-success ring-2 ring-accent-success/30'
                          : r.connecting
                          ? 'bg-accent-warning animate-pulse'
                          : 'bg-dark-500'
                      }`}
                    />
                    <button
                      onClick={() => {
                        if (!r.connected && !r.connecting) switchTo(r.id);
                        setRigMenuOpen(false);
                      }}
                      disabled={r.connecting}
                      className="flex-1 min-w-0 text-left disabled:cursor-default"
                      title={r.connected ? `${r.name} (connected)` : `Connect ${r.name}`}
                    >
                      <div className={`text-xs font-medium truncate ${r.connected ? 'text-accent-success' : 'text-dark-100'}`}>
                        {r.name}
                      </div>
                      <div className="mt-0.5 text-[10px] text-dark-400 font-mono truncate">
                        {r.detail ? (
                          <>
                            <span className={r.connected ? 'text-ham-cw' : undefined}>{r.detail}</span> · {r.type.toUpperCase()}
                          </>
                        ) : (
                          r.type.toUpperCase()
                        )}
                      </div>
                    </button>
                    {r.connected ? (
                      <button
                        onClick={() => disconnect(r.id)}
                        title="Disconnect"
                        className="flex-shrink-0 flex items-center gap-1 text-[9px] uppercase tracking-wide font-semibold text-accent-success/80 hover:text-accent-danger transition-colors"
                      >
                        <span className="group-hover:hidden">Connected</span>
                        <span className="hidden group-hover:inline-flex items-center gap-1">
                          <Power className="w-3 h-3" /> Disconnect
                        </span>
                      </button>
                    ) : (
                      <span
                        className={`flex-shrink-0 text-[9px] uppercase tracking-wide font-semibold ${
                          r.connecting ? 'text-accent-warning' : 'text-dark-500'
                        }`}
                      >
                        {r.connecting ? 'Connecting…' : 'Disconnected'}
                      </span>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
    <AboutDialog
      isOpen={showAbout}
      initialTab={aboutTab}
      onClose={() => { setShowAbout(false); setAboutTab('about'); }}
    />
    </>
  );
}

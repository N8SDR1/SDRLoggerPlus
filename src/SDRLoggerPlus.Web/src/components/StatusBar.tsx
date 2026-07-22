import { Radio, MapPin, Settings, ChevronDown, Power, Volume2, VolumeX, Waves } from 'lucide-react';
import { useAppStore } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import { useRigConnection } from '../hooks/useRigConnection';
import { useWsjtxLink } from '../hooks/useWsjtxLink';
import type { WsjtxLinkState } from '../utils/wsjtxLink';
import { useEffect, useRef, useState } from 'react';
import { APP_VERSION } from '../version';
import { AboutDialog, type TabId } from './AboutDialog';

// Announcement volume to restore when unmuting.
const VOLUME_BEFORE_MUTE = 'sdrl_volume_before_mute';

// Colour per decoder-link state. 'off' never reaches here — the pill unmounts.
const WSJTX_LINK_COLOR: Record<WsjtxLinkState, string> = {
  off: '',
  listening: 'text-dark-400',
  alive: 'text-accent-success',
  stale: 'text-accent-warning',
  error: 'text-accent-danger',
};

export function StatusBar() {
  const { stationCallsign, stationGrid, rigStatus } = useAppStore();
  const { openSettings, setActiveSection, updateVoiceSettings, saveSettings } = useSettingsStore();
  const voiceVolume = useSettingsStore((s) => s.settings.voice.volume);
  const muted = voiceVolume === 0;
  const { rigs, switchTo, disconnect, pillRigName, pillConnected } = useRigConnection();
  const [showAbout, setShowAbout] = useState(false);
  const [aboutTab, setAboutTab] = useState<TabId>('about');
  const [rigMenuOpen, setRigMenuOpen] = useState(false);
  const rigMenuRef = useRef<HTMLDivElement>(null);
  const wsjtxLink = useWsjtxLink();

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

  const formatFrequency = (freq: number) => {
    return (freq / 1000000).toFixed(3);
  };

  // Rig connection state + display name come from the shared useRigConnection
  // hook — the same rig (and name) the popover lists, so the pill and menu match.
  const rigConnected = pillConnected;
  const rigName = pillRigName || 'Rig';

  // Mute by zeroing the shared announcement volume — the same thing as dragging
  // the Settings > Voice slider to 0, so every spoken alert (including the Test
  // buttons) goes quiet with no separate mute flag to keep in sync. The pre-mute
  // level is remembered so unmuting restores it rather than guessing.
  const toggleMute = () => {
    if (muted) {
      const prev = Number(localStorage.getItem(VOLUME_BEFORE_MUTE) ?? '');
      updateVoiceSettings({ volume: prev > 0 ? prev : 0.8 });
    } else {
      localStorage.setItem(VOLUME_BEFORE_MUTE, String(voiceVolume));
      updateVoiceSettings({ volume: 0 });
      // Volume is baked into an utterance when it's created, so zeroing it only
      // silences future announcements — whatever is mid-sentence keeps talking.
      // Kill the in-flight utterance and anything queued behind it so mute means
      // "quiet now", not "quiet after this one finishes".
      if (typeof speechSynthesis !== 'undefined') speechSynthesis.cancel();
    }
    // update* only touches local state; persist so the toggle survives a restart.
    saveSettings().catch((e) => console.warn('[status-bar] mute save failed', e));
  };

  // Radio setup lives in Settings > Station (it used to be its own Rig panel).
  const openRigSettings = () => {
    setRigMenuOpen(false);
    setActiveSection('station');
    openSettings();
  };

  const openWsjtxSettings = () => {
    setActiveSection('wsjtx');
    openSettings();
  };

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
        {/* Decoder link. Hidden unless a WSJT-X source is enabled, so operators
            who never run digital modes lose no status-bar space. The dot tracks
            heartbeat freshness — a bound socket only proves we're listening. */}
        {wsjtxLink.state !== 'off' && (
          <button
            onClick={openWsjtxSettings}
            className={`flex items-center gap-1.5 hover:bg-dark-600 rounded px-1.5 py-0.5 transition-colors ${WSJTX_LINK_COLOR[wsjtxLink.state]}`}
            title={`${wsjtxLink.title} · Click for decoder settings`}
          >
            <Waves className="w-3.5 h-3.5" />
            <span className="text-xs font-mono">{wsjtxLink.label}</span>
            <span
              className={`w-1.5 h-1.5 rounded-full bg-current ${
                wsjtxLink.state === 'stale' || wsjtxLink.state === 'error' ? 'animate-pulse' : ''
              }`}
            />
          </button>
        )}

        {/* Mute = announcement volume 0, exactly as if the Settings > Voice
            volume slider were dragged to zero. Everything spoken already honours
            that volume, so there's nothing else to gate. */}
        <button
          onClick={toggleMute}
          className={`p-1 rounded transition-colors hover:bg-dark-600 ${
            muted ? 'text-accent-danger' : 'text-accent-success'
          }`}
          title={muted ? 'Announcements muted — click to unmute' : 'Mute announcements'}
          aria-label={muted ? 'Unmute announcements' : 'Mute announcements'}
          aria-pressed={muted}
        >
          {muted ? <VolumeX className="w-4 h-4" /> : <Volume2 className="w-4 h-4" />}
        </button>

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
            onContextMenu={(e) => { e.preventDefault(); openRigSettings(); }}
            className="flex items-center gap-2 hover:bg-dark-600 rounded px-1.5 py-0.5 transition-colors"
            title="Left-click: choose radio · Right-click: radio setup in Settings > Station"
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
                  No rigs configured. Add one with the button below.
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

              <button
                onClick={openRigSettings}
                className="mt-1.5 w-full flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-lg border border-glass-100/60 text-[11px] font-ui text-dark-200 hover:text-accent-primary hover:border-accent-primary/50 transition-colors"
                title="Open Settings > Station to add, edit or remove radios"
              >
                <Settings className="w-3 h-3" /> Add / manage radios
              </button>
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

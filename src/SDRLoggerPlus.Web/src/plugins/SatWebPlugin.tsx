import { useMemo, useState } from 'react';
import { Satellite, ExternalLink, RefreshCw } from 'lucide-react';
import { GlassPanel } from '../components/GlassPanel';
import { useSettingsStore } from '../store/settingsStore';

/**
 * Embeds the CSN S.A.T. controller's own web interface as a dockable panel.
 *
 * The controller serves a complete UI at its IP — next passes, click-to-track sat
 * selection, TLE / freq-DB updates, rotator, pass log, polar + map. Rather than
 * reimplementing (and re-deriving) all of that against an undocumented protocol, we
 * surface the real thing: it always shows the operator's own data and keeps working
 * across CSN firmware updates. The controller sets no X-Frame-Options/CSP and
 * advertises Access-Control-Allow-Private-Network, so it is embeddable by design.
 *
 * Uses the controller IP already configured in Settings → S.A.T.
 */
export function SatWebPlugin() {
  const { settings, openSettings, setActiveSection } = useSettingsStore();
  const ip = settings.sat?.controllerIp?.trim();
  // Bumping this remounts the iframe — a cross-origin frame can't be reloaded directly.
  const [reloadKey, setReloadKey] = useState(0);

  const url = useMemo(() => (ip ? `http://${ip}/` : ''), [ip]);

  const openExternal = () => {
    if (!url) return;
    if (window.electronAPI && 'openExternal' in window.electronAPI) {
      window.electronAPI.openExternal(url);
    } else {
      window.open(url, '_blank', 'noopener,noreferrer');
    }
  };

  return (
    <GlassPanel
      title="S.A.T. Web"
      icon={<Satellite className="w-5 h-5" />}
      actions={
        ip ? (
          <>
            <button
              onClick={() => setReloadKey((n) => n + 1)}
              title="Reload the controller page"
              className="p-1.5 rounded bg-dark-700 text-dark-300 hover:text-dark-100 transition-colors"
            >
              <RefreshCw className="w-3.5 h-3.5" />
            </button>
            <button
              onClick={openExternal}
              title="Open the controller in your browser"
              className="p-1.5 rounded bg-dark-700 text-dark-300 hover:text-dark-100 transition-colors"
            >
              <ExternalLink className="w-3.5 h-3.5" />
            </button>
          </>
        ) : undefined
      }
    >
      {!ip ? (
        <div className="h-full flex flex-col items-center justify-center text-center gap-2 p-6">
          <Satellite className="w-8 h-8 text-dark-500" />
          <p className="text-sm text-dark-300">No S.A.T. controller address set</p>
          <p className="text-xs text-dark-400 max-w-xs">
            Enter your CSN S.A.T. controller's IP in Settings → S.A.T., and its full
            interface — next passes, tracking, TLE &amp; frequency database — appears here.
          </p>
          <button
            onClick={() => { setActiveSection('sat'); openSettings(); }}
            className="mt-1 px-3 py-1.5 text-xs rounded-lg bg-accent-primary/20 border border-accent-primary/40 text-accent-primary hover:bg-accent-primary/30"
          >
            Open S.A.T. settings
          </button>
        </div>
      ) : (
        <iframe
          key={reloadKey}
          src={url}
          title="CSN S.A.T. Controller"
          className="w-full h-full border-0 rounded-lg"
        />
      )}
    </GlassPanel>
  );
}

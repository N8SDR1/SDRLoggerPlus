import { useEffect, useState } from 'react';
import { Zap, Wind, X } from 'lucide-react';
import { api, LightningStatus, WindStatus } from '../api/client';
import { useSettingsStore } from '../store/settingsStore';
import { useWeatherPreviewStore } from '../store/weatherPreviewStore';

const SEVERITY_STYLES: Record<string, string> = {
  elevated: 'bg-yellow-900/60 border-yellow-600/60 text-yellow-200',
  high: 'bg-orange-900/60 border-orange-600/60 text-orange-200',
  extreme: 'bg-red-900/70 border-red-600/70 text-red-200',
};

/**
 * Slim alert strip for station-protection weather alerts (SDRLogger+ port).
 * Polls the backend; a dismiss hides the banner until the status changes.
 */
export function WeatherAlertBanner() {
  const weatherEnabled = useSettingsStore(
    state => state.settings.weather.lightning.enabled || state.settings.weather.wind.enabled);
  const [lightning, setLightning] = useState<LightningStatus | null>(null);
  const [wind, setWind] = useState<WindStatus | null>(null);
  const [dismissedKey, setDismissedKey] = useState('');
  // Preview override — Settings → Weather can inject a fake status for
  // the operator to see what the banner looks like without waiting for
  // real weather. When active, it takes precedence over the polled state.
  const preview = useWeatherPreviewStore();

  useEffect(() => {
    if (!weatherEnabled) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const [l, w] = await Promise.all([api.getLightningStatus(), api.getWindStatus()]);
        if (!cancelled) { setLightning(l); setWind(w); }
      } catch {
        // Backend unavailable — keep last state
      }
    };
    poll();
    const timer = setInterval(poll, 60_000);
    return () => { cancelled = true; clearInterval(timer); };
  }, [weatherEnabled]);

  const previewActive = preview.expiresAt > Date.now();
  // Preview overrides the enable check too — operator wants to see the
  // banner even if weather alerts are disabled overall.
  if (!weatherEnabled && !previewActive) return null;

  const effectiveLightning = previewActive ? preview.lightning : lightning;
  const effectiveWind = previewActive ? preview.wind : wind;

  const lightningActive = effectiveLightning?.active === true;
  const windActive = effectiveWind?.active === true && !!effectiveWind.severity;
  if (!lightningActive && !windActive) return null;

  // Key changes whenever the alert content changes, un-hiding a dismissed banner
  const key = `${lightningActive ? `L${effectiveLightning?.strikesLastHour}${effectiveLightning?.closestKm}` : ''}|${windActive ? `W${effectiveWind?.severity}${effectiveWind?.gustMph}` : ''}`;
  if (key === dismissedKey) return null;

  const useKph = effectiveWind?.unit === 'kph';
  // Local aliases the JSX still references — kept as `lightning` and
  // `wind` so the rendering block below doesn't need touching.
  const l = effectiveLightning;
  const w = effectiveWind;
  const windText = windActive
    ? (useKph
        ? `G${Math.round(w!.gustKph ?? 0)}kph ${Math.round(w!.sustainedKph ?? 0)}kph ${w!.direction}`
        : `G${Math.round(w!.gustMph ?? 0)} ${Math.round(w!.sustainedMph ?? 0)}mph ${w!.direction}`)
    : '';

  const severityStyle = SEVERITY_STYLES[w?.severity ?? ''] ?? SEVERITY_STYLES.high;
  const style = lightningActive ? SEVERITY_STYLES.extreme : severityStyle;

  return (
    <div className={`flex items-center gap-3 px-3 py-1 border-b text-xs font-medium ${style}${previewActive ? ' animate-pulse' : ''}`}>
      {previewActive && (
        <span className="text-[10px] font-bold uppercase tracking-widest opacity-80" title="Preview only — not a real alert">
          Preview
        </span>
      )}
      {lightningActive && (
        <span className="flex items-center gap-1.5" title={l?.nwsWarning ?? 'Lightning detected'}>
          <Zap className="w-3.5 h-3.5" />
          Lightning{l?.closestMi != null && ` ${l.closestMi} mi ${l.direction}`}
          {l != null && l.strikesLastHour > 0 && ` · ${l.strikesLastHour}/hr`}
          {l?.nwsWarning && ` · ${l.nwsWarning}`}
        </span>
      )}
      {windActive && (
        <span className="flex items-center gap-1.5" title={w?.nwsAlert ?? 'High wind'}>
          <Wind className="w-3.5 h-3.5" />
          {w?.severity === 'extreme' ? 'EXTREME WIND' : w?.severity === 'high' ? 'High wind' : 'Wind'}
          {' '}{windText}
          {w?.nwsAlert && ` · ${w.nwsAlert}`}
        </span>
      )}
      <button
        onClick={() => setDismissedKey(key)}
        className="ml-auto p-0.5 hover:opacity-70"
        title="Dismiss (reappears if conditions change)"
      >
        <X className="w-3.5 h-3.5" />
      </button>
    </div>
  );
}

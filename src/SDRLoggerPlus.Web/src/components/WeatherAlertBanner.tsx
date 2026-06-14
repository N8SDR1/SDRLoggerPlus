import { useEffect, useState } from 'react';
import { Zap, Wind, X } from 'lucide-react';
import { api, LightningStatus, WindStatus } from '../api/client';
import { useSettingsStore } from '../store/settingsStore';

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

  if (!weatherEnabled) return null;

  const lightningActive = lightning?.active === true;
  const windActive = wind?.active === true && !!wind.severity;
  if (!lightningActive && !windActive) return null;

  // Key changes whenever the alert content changes, un-hiding a dismissed banner
  const key = `${lightningActive ? `L${lightning?.strikesLastHour}${lightning?.closestKm}` : ''}|${windActive ? `W${wind?.severity}${wind?.gustMph}` : ''}`;
  if (key === dismissedKey) return null;

  const useKph = wind?.unit === 'kph';
  const windText = windActive
    ? (useKph
        ? `G${Math.round(wind!.gustKph ?? 0)}kph ${Math.round(wind!.sustainedKph ?? 0)}kph ${wind!.direction}`
        : `G${Math.round(wind!.gustMph ?? 0)} ${Math.round(wind!.sustainedMph ?? 0)}mph ${wind!.direction}`)
    : '';

  const severityStyle = SEVERITY_STYLES[wind?.severity ?? ''] ?? SEVERITY_STYLES.high;
  const style = lightningActive ? SEVERITY_STYLES.extreme : severityStyle;

  return (
    <div className={`flex items-center gap-3 px-3 py-1 border-b text-xs font-medium ${style}`}>
      {lightningActive && (
        <span className="flex items-center gap-1.5" title={lightning?.nwsWarning ?? 'Lightning detected'}>
          <Zap className="w-3.5 h-3.5" />
          Lightning{lightning?.closestMi != null && ` ${lightning.closestMi} mi ${lightning.direction}`}
          {lightning != null && lightning.strikesLastHour > 0 && ` · ${lightning.strikesLastHour}/hr`}
          {lightning?.nwsWarning && ` · ${lightning.nwsWarning}`}
        </span>
      )}
      {windActive && (
        <span className="flex items-center gap-1.5" title={wind?.nwsAlert ?? 'High wind'}>
          <Wind className="w-3.5 h-3.5" />
          {wind?.severity === 'extreme' ? 'EXTREME WIND' : wind?.severity === 'high' ? 'High wind' : 'Wind'}
          {' '}{windText}
          {wind?.nwsAlert && ` · ${wind.nwsAlert}`}
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

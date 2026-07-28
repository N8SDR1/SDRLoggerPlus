import { useEffect, useMemo, useRef, useState } from 'react';
import { useSettingsStore } from '../store/settingsStore';
import { useAppStore } from '../store/appStore';
import { api, SpaceWeatherData, type NtpOffset } from '../api/client';
import { APP_VERSION } from '../version';

// ── Band activity heat map (SDRLogger+ port) ────────────────────────────────
// Counts cluster spots per band over the trailing 30 minutes; five heat tiers
// from cold blue (quiet) to red (hot).
const HM_BANDS = ['160m', '80m', '40m', '30m', '20m', '17m', '15m', '12m', '10m', '6m'];
const HM_RANGES: Record<string, [number, number]> = {
  '160m': [1800, 2000], '80m': [3500, 4000], '40m': [7000, 7300],
  '30m': [10100, 10150], '20m': [14000, 14350], '17m': [18068, 18168],
  '15m': [21000, 21450], '12m': [24890, 24990], '10m': [28000, 29700],
  '6m': [50000, 54000],
};

function hmBandFor(freqKhz: number): string | null {
  for (const [band, [lo, hi]] of Object.entries(HM_RANGES)) {
    if (freqKhz >= lo && freqKhz <= hi) return band;
  }
  return null;
}

function hmTier(n: number): { background: string; color: string } {
  if (n === 0) return { background: '#1a2535', color: '#4a5568' };
  if (n <= 3) return { background: '#1a3a5c', color: '#7cb8e0' };
  if (n <= 8) return { background: '#1d5e8a', color: '#a8d4f0' };
  if (n <= 15) return { background: '#b45309', color: '#fde68a' };
  return { background: '#dc2626', color: '#ffffff' };
}

interface WeatherData {
  temperature: number;
  temperatureC: number;
  windSpeed: number;
  weatherCode: number;
}

const getWeatherIcon = (code: number) => {
  if (code === 0) return '\u2600\uFE0F';
  if (code <= 3) return '\u26C5';
  if (code <= 48) return '\u2601\uFE0F';
  if (code <= 67) return '\uD83C\uDF27\uFE0F';
  if (code <= 77) return '\uD83C\uDF28\uFE0F';
  if (code <= 82) return '\uD83C\uDF27\uFE0F';
  if (code <= 86) return '\uD83C\uDF28\uFE0F';
  if (code <= 99) return '\u26C8\uFE0F';
  return '\u2601\uFE0F';
};

export function HeaderPlugin() {
  const { settings } = useSettingsStore();
  const [currentTime, setCurrentTime] = useState(new Date());
  const [spaceWeather, setSpaceWeather] = useState<SpaceWeatherData | null>(null);
  const [weather, setWeather] = useState<WeatherData | null>(null);

  const containerRef = useRef<HTMLDivElement>(null);
  const fitRef = useRef<() => void>(() => {});
  const { callsign } = settings.station;
  const { showWeather } = settings.header;

  // Band activity counts — recompute when spots change or once a minute
  // (the trailing window must decay even when no new spots arrive).
  const spots = useAppStore((s) => s.dxClusterSpots);
  const minuteKey = Math.floor(currentTime.getTime() / 60_000);
  const bandCounts = useMemo(() => {
    const cutoff = Date.now() - 30 * 60 * 1000;
    const counts: Record<string, number> = {};
    HM_BANDS.forEach((b) => (counts[b] = 0));
    for (const spot of spots) {
      if (new Date(spot.timestamp).getTime() < cutoff) continue;
      const band = hmBandFor(spot.frequency);
      if (band) counts[band]++;
    }
    return counts;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [spots, minuteKey]);

  // Binary search for largest --ts where content fits without overflow
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const fit = () => {
      if (!el.clientHeight) return;
      let lo = 8, hi = el.clientHeight;
      while (hi - lo > 1) {
        const mid = Math.floor((lo + hi) / 2);
        el.style.setProperty('--ts', `${mid}`);
        if (el.scrollHeight > el.clientHeight || el.scrollWidth > el.clientWidth) {
          hi = mid;
        } else {
          lo = mid;
        }
      }
      el.style.setProperty('--ts', `${lo}`);
    };
    fitRef.current = fit;
    const observer = new ResizeObserver(() => fit());
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const timer = setInterval(() => setCurrentTime(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    const fetchSpaceWeather = async () => {
      try {
        const data = await api.getSpaceWeather();
        setSpaceWeather(data);
      } catch (error) {
        console.error('Failed to fetch space weather:', error);
      }
    };
    fetchSpaceWeather();
    const interval = setInterval(fetchSpaceWeather, 15 * 60 * 1000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    if (!showWeather || !settings.station.latitude || !settings.station.longitude) return;
    const fetchWeather = async () => {
      try {
        const { latitude, longitude } = settings.station;
        const url = `https://api.open-meteo.com/v1/forecast?latitude=${latitude}&longitude=${longitude}&current=temperature_2m,wind_speed_10m,weather_code&temperature_unit=fahrenheit&wind_speed_unit=mph`;
        const response = await fetch(url);
        const data = await response.json();
        if (data.current) {
          setWeather({
            temperature: Math.round(data.current.temperature_2m),
            temperatureC: Math.round((data.current.temperature_2m - 32) * 5 / 9),
            windSpeed: Math.round(data.current.wind_speed_10m),
            weatherCode: data.current.weather_code,
          });
        }
      } catch (error) {
        console.error('Failed to fetch weather:', error);
      }
    };
    fetchWeather();
    const interval = setInterval(fetchWeather, 30 * 60 * 1000);
    return () => clearInterval(interval);
  }, [showWeather, settings.station]);

  // Re-fit when content changes (data loads in)
  useEffect(() => { fitRef.current(); }, [spaceWeather, weather]);

  const pad = (n: number) => n.toString().padStart(2, '0');

  const [ntpPos, setNtpPos] = useState<{ x: number; y: number } | null>(null);
  const utcHours = currentTime.getUTCHours();
  const utcMinutes = currentTime.getUTCMinutes();
  const utcSeconds = currentTime.getUTCSeconds();
  const utcYear = currentTime.getUTCFullYear();
  const utcMonth = pad(currentTime.getUTCMonth() + 1);
  const utcDay = pad(currentTime.getUTCDate());

  const localHours = currentTime.getHours();
  const localMinutes = currentTime.getMinutes();
  const localSeconds = currentTime.getSeconds();

  // Local time honors the header time-format setting (UTC stays 24h by convention).
  const use12h = settings.header.timeFormat === '12h';
  const localHour12 = localHours % 12 === 0 ? 12 : localHours % 12;
  const localTimeStr = use12h
    ? `${localHour12}:${pad(localMinutes)}:${pad(localSeconds)} ${localHours < 12 ? 'AM' : 'PM'}`
    : `${pad(localHours)}:${pad(localMinutes)}:${pad(localSeconds)}`;

  const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const localDateStr = `${days[currentTime.getDay()]}, ${months[currentTime.getMonth()]} ${currentTime.getDate()}`;

  return (
    <div ref={containerRef} className="header-plugin bg-dark-900">
      {/* Brand mark + operator callsign kept as one visual unit so the
          top-bar space-between distribution doesn't push the callsign
          halfway across the header. Matches the v1.x layout where
          icon → SDRLOGGER+ → callsign all hugged the left edge. */}
      <div className="header-plugin__brand-block">
        <div className="header-plugin__group header-plugin__brand">
          <img
            src="./sdrloggerplus-icon.png"
            alt="SDRLoggerPlus"
            className="header-plugin__logo rounded"
          />
          <span className="header-plugin__wordmark font-display">SDRLOGGER<span className="text-accent-primary">+</span></span>
        </div>

        <div className="header-plugin__group">
          <span className="header-plugin__callsign font-display text-accent-primary">{callsign || 'N0CALL'}</span>
          <span className="header-plugin__version font-mono text-dark-300">v{APP_VERSION}</span>
        </div>
      </div>

      {/* Separator */}
      <div className="header-plugin__sep border-glass-100" />

      {/* UTC Time — right-click to sync the PC clock from NTP */}
      <div
        className="header-plugin__group cursor-context-menu"
        title="Right-click to sync your PC clock from NTP"
        onContextMenu={(e) => { e.preventDefault(); e.stopPropagation(); setNtpPos({ x: e.clientX, y: e.clientY }); }}
      >
        <span className="header-plugin__label font-ui text-accent-success">UTC</span>
        <span className="header-plugin__time font-display text-white">
          {pad(utcHours)}:{pad(utcMinutes)}:{pad(utcSeconds)}
        </span>
        <span className="header-plugin__date font-mono text-dark-300">{utcYear}-{utcMonth}-{utcDay}</span>
      </div>

      {/* Separator */}
      <div className="header-plugin__sep border-glass-100" />

      {/* Local Time — right-click to sync the PC clock from NTP */}
      <div
        className="header-plugin__group cursor-context-menu"
        title="Right-click to sync your PC clock from NTP"
        onContextMenu={(e) => { e.preventDefault(); e.stopPropagation(); setNtpPos({ x: e.clientX, y: e.clientY }); }}
      >
        <span className="header-plugin__label font-ui text-accent-success">LOCAL</span>
        <span className="header-plugin__time font-display text-accent-primary">
          {localTimeStr}
        </span>
        <span className="header-plugin__date font-mono text-dark-300">{localDateStr}</span>
      </div>

      {/* NTP sync popover — fixed to the viewport at the click point so the header panel's
          overflow can never clip it. */}
      {ntpPos && <NtpSyncPopover pos={ntpPos} onClose={() => setNtpPos(null)} />}

      {/* Separator */}
      <div className="header-plugin__sep border-glass-100" />

      {/* Weather */}
      {showWeather && weather && (
        <>
          <div className="header-plugin__group">
            <span className="header-plugin__weather-icon">{getWeatherIcon(weather.weatherCode)}</span>
            <span className="header-plugin__weather-temp font-mono text-accent-secondary">
              {weather.temperature}&deg;F/{weather.temperatureC}&deg;C
            </span>
          </div>
          <div className="header-plugin__sep border-glass-100" />
        </>
      )}

      {/* Band activity heat map — spots per band, trailing 30 min */}
      <div className="header-plugin__group" style={{ display: 'flex', gap: 3, alignItems: 'center' }}>
        {HM_BANDS.map((band) => {
          const n = bandCounts[band];
          const tier = hmTier(n);
          return (
            <div
              key={band}
              title={`${band}: ${n} spot${n === 1 ? '' : 's'} in the last 30 min`}
              style={{
                ...tier,
                display: 'inline-flex',
                flexDirection: 'column',
                alignItems: 'center',
                borderRadius: 3,
                padding: '2px 6px',
                minWidth: 27,
                lineHeight: 1.25,
                transition: 'background .3s',
              }}
              className="font-mono"
            >
              <span style={{ fontSize: 13, fontWeight: 700 }}>{n > 0 ? n : '·'}</span>
              <span style={{ fontSize: 9, opacity: 0.85 }}>{band.replace('m', '')}</span>
            </div>
          );
        })}
      </div>

      {/* Separator */}
      <div className="header-plugin__sep border-glass-100" />

      {/* Space Weather Indices */}
      {spaceWeather && (
        <div className="header-plugin__group header-plugin__indices">
          <span className="header-plugin__label font-ui text-accent-success">SFI</span>
          <span className="header-plugin__value font-display text-accent-primary">{spaceWeather.solarFluxIndex}</span>
          <span className="header-plugin__label font-ui text-accent-success" style={{ marginLeft: 12 }}>K</span>
          <span className={`header-plugin__value font-display${spaceWeather.kIndex >= 4 ? ' header-plugin__value--danger text-accent-danger' : ' text-accent-primary'}`}>
            {spaceWeather.kIndex}
          </span>
          <span className="header-plugin__label font-ui text-accent-success" style={{ marginLeft: 12 }}>SSN</span>
          <span className="header-plugin__value font-display text-white">{spaceWeather.sunspotNumber}</span>
        </div>
      )}
    </div>
  );
}

/**
 * Right-click-the-clock NTP sync. Queries public NTP for this PC's offset and, on Windows, offers a
 * one-click "Sync now" that sets the system clock via an elevated helper (a UAC prompt). Read-only
 * elsewhere. See NtpService / TimeController.
 */
function NtpSyncPopover({ pos, onClose }: { pos: { x: number; y: number }; onClose: () => void }) {
  const [offset, setOffset] = useState<NtpOffset | null>(null);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  const load = async () => {
    setLoading(true); setMessage(null);
    try { setOffset(await api.getNtpOffset()); }
    catch { setOffset({ reachable: false, serverUtc: '', offsetMs: 0, server: 'NTP', error: 'Request failed' }); }
    finally { setLoading(false); }
  };
  useEffect(() => { load(); }, []);

  const sync = async () => {
    setSyncing(true); setMessage(null);
    try {
      const r = await api.ntpResync();
      setMessage(r.detail);
      if (r.ok) await load();
    } catch (e) {
      setMessage(e instanceof Error ? e.message : 'Sync failed.');
    } finally {
      setSyncing(false);
    }
  };

  const describe = (ms: number) => {
    const s = ms / 1000;
    const abs = Math.abs(s);
    const word = s > 0 ? 'behind' : 'ahead of';
    if (abs < 0.5) return 'PC clock is in sync with NTP.';
    return `PC clock is ${abs.toFixed(1)} s ${word} NTP.`;
  };

  // Clamp to the viewport so a click near the right/bottom edge stays fully visible.
  const width = 256;
  const left = Math.min(pos.x, window.innerWidth - width - 8);
  const top = Math.min(pos.y + 8, window.innerHeight - 180);

  return (
    <>
      {/* click-away backdrop */}
      <div className="fixed inset-0 z-[999]" onClick={onClose} onContextMenu={(e) => { e.preventDefault(); onClose(); }} />
      <div
        className="fixed z-[1000] w-64 rounded-lg border border-glass-200 bg-dark-800 shadow-xl p-3 text-xs"
        style={{ left: Math.max(8, left), top: Math.max(8, top) }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between mb-2">
          <span className="font-ui font-semibold text-white">Clock sync (NTP)</span>
          <button onClick={onClose} className="text-dark-400 hover:text-white" title="Close">✕</button>
        </div>

        {loading ? (
          <div className="text-dark-300">Checking NTP…</div>
        ) : offset && !offset.reachable ? (
          <div className="text-amber-300">Couldn’t reach NTP{offset.error ? `: ${offset.error}` : '.'}</div>
        ) : offset ? (
          <>
            <div className="text-dark-200">{describe(offset.offsetMs)}</div>
            <div className="text-[10px] text-dark-400 mt-0.5 font-mono">{offset.server}</div>
          </>
        ) : null}

        {message && <div className="mt-2 text-accent-primary">{message}</div>}

        <div className="flex items-center gap-2 mt-3">
          <button
            onClick={sync}
            disabled={syncing || loading}
            className="px-2.5 py-1.5 rounded-md bg-accent-secondary text-black font-medium hover:bg-accent-secondary/90 disabled:opacity-50"
            title="Windows: prompts for administrator approval to set the clock"
          >
            {syncing ? 'Syncing…' : 'Sync now'}
          </button>
          <button onClick={load} disabled={loading || syncing}
            className="px-2.5 py-1.5 rounded-md border border-glass-200 text-gray-200 hover:bg-glass-100 disabled:opacity-50">
            Re-check
          </button>
        </div>
        <p className="text-[10px] text-dark-400 mt-2 leading-snug">
          “Sync now” sets your PC clock from NTP. On Windows it asks for administrator approval.
        </p>
      </div>
    </>
  );
}

import { useEffect, useMemo, useState, useRef } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Map as MapIcon } from 'lucide-react';
import { api, BatchCheckEntry } from '../api/client';
import { useAppStore } from '../store/appStore';
import { useSignalR } from '../hooks/useSignalR';
import { GlassPanel } from '../components/GlassPanel';

// Per-band frequency window (kHz) for the vertical scale.
const BAND_RANGES: Record<string, [number, number]> = {
  '160m': [1800, 2000],
  '80m': [3500, 4000],
  '40m': [7000, 7300],
  '20m': [14000, 14350],
  '15m': [21000, 21450],
  '10m': [28000, 28700],
  '6m': [50000, 50400],
};
const BANDS = Object.keys(BAND_RANGES);

const bandFromKhz = (khz: number): string | null => {
  for (const [b, [lo, hi]] of Object.entries(BAND_RANGES)) {
    if (khz >= lo && khz <= hi) return b;
  }
  return null;
};

const STALE_MS = 15 * 60 * 1000;

export function ContestBandmapPlugin() {
  const spots = useAppStore((s) => s.dxClusterSpots);
  const rigStatus = useAppStore((s) => s.rigStatus);
  const contestState = useAppStore((s) => s.contestState);
  const setContestState = useAppStore((s) => s.setContestState);
  const setContestSpotCall = useAppStore((s) => s.setContestSpotCall);
  const { selectSpot } = useSignalR();

  // Learn about an already-active session on mount (state otherwise only arrives
  // via SignalR after the next contest QSO).
  useEffect(() => {
    if (!contestState) api.getContestState().then((s) => setContestState(s)).catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const [band, setBand] = useState('20m');
  const followRig = useRef(true);
  const [, tick] = useState(0);

  // Re-render every 30s so staleness fades update.
  useEffect(() => {
    const t = setInterval(() => tick((n) => n + 1), 30000);
    return () => clearInterval(t);
  }, []);

  // Follow the rig band unless the operator picked one manually.
  useEffect(() => {
    if (!followRig.current || !rigStatus) return;
    const b = bandFromKhz(rigStatus.frequency / 1000);
    if (b) setBand(b);
  }, [rigStatus]);

  const [lo, hi] = BAND_RANGES[band];

  // Spots on this band, most-recent first, deduped by call (keep freshest).
  const bandSpots = useMemo(() => {
    const now = Date.now();
    const seen = new Set<string>();
    return spots
      .filter((s) => {
        const khz = s.frequency;
        return khz >= lo && khz <= hi && now - new Date(s.timestamp).getTime() < STALE_MS;
      })
      .filter((s) => {
        const c = s.dxCall.toUpperCase();
        if (seen.has(c)) return false;
        seen.add(c);
        return true;
      })
      .sort((a, b) => a.frequency - b.frequency);
  }, [spots, lo, hi]);

  // Batch dupe/mult check for the visible spots; refresh on spots/state change.
  const callsKey = bandSpots.map((s) => s.dxCall).join(',');
  const { data: checks } = useQuery({
    queryKey: ['contest-bandmap-check', band, callsKey, contestState?.qsos, contestState?.multipliers],
    queryFn: () =>
      api.checkContestBatch(bandSpots.map((s) => ({ call: s.dxCall, band, mode: 'CW' }))),
    enabled: !!contestState && bandSpots.length > 0,
  });
  const checkByCall = useMemo(() => {
    const m = new Map<string, BatchCheckEntry>();
    checks?.forEach((c) => m.set(c.call.toUpperCase(), c));
    return m;
  }, [checks]);

  const onSpotClick = (call: string, freqKhz: number, mode?: string) => {
    setContestSpotCall(call);          // fill the contest entry window
    void selectSpot(call, freqKhz, mode); // QSY the rig
  };

  return (
    <GlassPanel
      title="Bandmap"
      icon={<MapIcon className="w-5 h-5" />}
      actions={
        <select
          value={band}
          onChange={(e) => { followRig.current = false; setBand(e.target.value); }}
          className="glass-input text-xs px-2 py-1"
        >
          {BANDS.map((b) => <option key={b} value={b}>{b}</option>)}
        </select>
      }
    >
      <div className="flex flex-col h-full p-2">
        {!contestState && (
          <div className="text-xs text-amber-400/80 px-2 pb-1">
            No active contest — spots shown without dupe/mult coloring.
          </div>
        )}
        <div className="flex-1 overflow-y-auto">
          {bandSpots.length === 0 ? (
            <div className="flex items-center justify-center h-full text-sm text-gray-500 text-center px-4">
              No spots on {band}. Spots from the DX cluster / RBN appear here.
            </div>
          ) : (
            <div className="space-y-0.5">
              {bandSpots.map((s) => {
                const chk = checkByCall.get(s.dxCall.toUpperCase());
                const ageMin = Math.floor((Date.now() - new Date(s.timestamp).getTime()) / 60000);
                const color = chk?.isDupe
                  ? 'text-red-400/80 line-through'
                  : chk?.isNewMult
                  ? 'text-emerald-300'
                  : 'text-gray-200';
                const border = chk?.isDupe
                  ? 'border-red-500/30'
                  : chk?.isNewMult
                  ? 'border-emerald-500/40'
                  : 'border-glass-100';
                return (
                  <button
                    key={s.id}
                    onClick={() => onSpotClick(s.dxCall, s.frequency, s.mode)}
                    style={{ opacity: Math.max(0.4, 1 - ageMin / 15) }}
                    className={`w-full flex items-center gap-2 px-2 py-1 rounded border ${border} bg-dark-700/40 hover:bg-dark-600/60 text-left transition-colors`}
                    title={`${s.dxCall} — ${(s.frequency / 1000).toFixed(3)} MHz${chk?.isDupe ? ' (DUPE)' : chk?.isNewMult ? ' (new mult)' : ''}`}
                  >
                    <span className="font-mono text-xs text-gray-500 w-16 shrink-0">
                      {s.frequency.toFixed(1)}
                    </span>
                    <span className={`font-mono text-sm font-medium flex-1 truncate ${color}`}>
                      {s.dxCall}
                    </span>
                    {chk?.isNewMult && <span className="text-[10px] text-emerald-400 shrink-0">MULT</span>}
                    {chk?.isDupe && <span className="text-[10px] text-red-400 shrink-0">DUPE</span>}
                    <span className="text-[10px] text-gray-600 shrink-0">{ageMin}m</span>
                  </button>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </GlassPanel>
  );
}

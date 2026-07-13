import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Target } from 'lucide-react';
import { GlassPanel } from '../components/GlassPanel';
import { useAppStore, Spot } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import { useSignalR } from '../hooks/useSignalR';
import { getBandFromFrequency, bandClassForFrequency } from '../utils/spotBands';
import { gridToLatLon } from '../utils/maidenhead';
import { assessPropagation, PropAssessment, BandOpenState } from '../utils/propagationGate';
import { api } from '../api/client';

/**
 * DX Coach — Phase 1 (deterministic award engine) + Phase 2 (propagation gate).
 *
 * Phase 1: scan live DX-cluster spots, keep the ones that fill an award gap
 * (per-spot newDxcc / newBand status the backend already computes), rank them.
 *
 * Phase 2: annotate each opportunity with a *soft* propagation read — a coarse
 * open / marginal / closed band score plus imminent gray-line timing — computed
 * client-side from SFI/K (backend space weather) + the operator QTH + the DX
 * centroid the backend now stamps on every spot. The prop read nudges ranking
 * within an award tier but never outranks the award itself, and every claim is
 * timed/geometric — no forecasting. See docs/design/ai-dx-coach.md.
 */
interface Opportunity {
  spot: Spot;
  band: string;
  kind: 'newDxcc' | 'newBand';
  onRigBand: boolean;
  prop: PropAssessment;
  score: number;
  /** How many live spots collapsed into this one opportunity (call+band). */
  count: number;
}

const OPEN_META: Record<Exclude<BandOpenState, 'unknown'>, { dot: string; label: string; text: string }> = {
  open: { dot: 'bg-accent-success', label: 'Open', text: 'text-accent-success' },
  marginal: { dot: 'bg-accent-warning', label: 'Marginal', text: 'text-accent-warning' },
  closed: { dot: 'bg-dark-400', label: 'Closed', text: 'text-dark-400' },
};

export function DxCoachPlugin() {
  const spots = useAppStore((s) => s.dxClusterSpots);
  const selectedRadioId = useAppStore((s) => s.selectedRadioId);
  const radioStates = useAppStore((s) => s.radioStates);
  const station = useSettingsStore((s) => s.settings.station);
  const coach = useSettingsStore((s) => s.settings.dxCoach);
  const { selectSpot } = useSignalR();

  // Space weather (SFI/K) drives the coarse band-open score. Cheap, shared,
  // refreshed on the backend's 15-min cadence.
  const { data: space } = useQuery({
    queryKey: ['space-weather'],
    queryFn: () => api.getSpaceWeather(),
    refetchInterval: 15 * 60 * 1000,
    staleTime: 10 * 60 * 1000,
  });

  // Operator QTH: explicit lat/lon wins, else derive from the grid square.
  const qth = useMemo(() => {
    if (station.latitude != null && station.longitude != null) {
      return { lat: station.latitude, lon: station.longitude };
    }
    if (station.gridSquare) {
      const g = gridToLatLon(station.gridSquare);
      if (g) return { lat: g.lat, lon: g.lon };
    }
    return null;
  }, [station.latitude, station.longitude, station.gridSquare]);

  // What band is the rig sitting on right now? Opportunities there are the
  // easiest to jump on (no band change), so they rank a little higher.
  const rigBand = useMemo(() => {
    const rig = selectedRadioId ? radioStates.get(selectedRadioId) : undefined;
    return rig?.frequencyHz != null ? getBandFromFrequency(rig.frequencyHz / 1000) : null;
  }, [selectedRadioId, radioStates]);

  const opportunities = useMemo<Opportunity[]>(() => {
    const sfi = space?.solarFluxIndex ?? null;
    const kIndex = space?.kIndex ?? null;
    // Collapse duplicate spots of the same station on the same band (multiple
    // spotters / repeats) into one opportunity, tracking how many landed.
    const byKey = new Map<string, Opportunity>();
    for (const s of spots) {
      // Only spots that fill a gap — a worked/unknown spot isn't an opportunity.
      if (s.status !== 'newDxcc' && s.status !== 'newBand') continue;

      // Band-class filter — an HF-only op has no use for 2m/70cm opportunities.
      const cls = bandClassForFrequency(s.frequency);
      if (cls === 'MF' && !coach.showLowBand) continue;
      if (cls === 'HF' && !coach.showHf) continue;
      if (cls === '6M' && !coach.show6m) continue;
      if (cls === 'VHF' && !coach.showVhf) continue;
      if (cls === 'UHF' && !coach.showUhf) continue;

      const band = getBandFromFrequency(s.frequency);
      const key = `${s.dxCall.toUpperCase()}|${band}`;
      const existing = byKey.get(key);
      if (existing) {
        // Same call+band already surfaced — merge: bump the count and keep the
        // most recent spot as the representative row.
        existing.count += 1;
        if (s.timestamp > existing.spot.timestamp) existing.spot = s;
        continue;
      }

      const onRigBand = rigBand != null && rigBand !== '?' && band === rigBand;

      const prop = assessPropagation({
        freqMHz: s.frequency / 1000,
        deLat: qth?.lat ?? null,
        deLon: qth?.lon ?? null,
        dxLat: s.dxStation?.lat ?? null,
        dxLon: s.dxStation?.lon ?? null,
        sfi,
        kIndex,
      });

      // Threshold filter: hide low-odds paths so the panel stays "right and
      // rare". Only judges spots we can actually score — a spot with no prop
      // data (no QTH, or no DX centroid) is always kept.
      if (prop.reliability != null && prop.reliability < coach.minReliability) continue;

      // Award tier dominates (100 vs 50 — a whole entity beats a band-slot).
      // Everything below is a *soft* re-order within a tier.
      let score = s.status === 'newDxcc' ? 100 : 50;
      if (onRigBand) score += 15;
      if (prop.open === 'open') score += 12;
      else if (prop.open === 'marginal') score += 4;
      else if (prop.open === 'closed') score -= 8;
      if (prop.grayLine) score += prop.grayLine.active ? 10 : 6;

      byKey.set(key, { spot: s, band, kind: s.status, onRigBand, prop, score, count: 1 });
    }
    const out = [...byKey.values()];
    out.sort((a, b) => b.score - a.score || (b.spot.timestamp > a.spot.timestamp ? 1 : -1));
    return out;
  }, [spots, rigBand, qth, space, coach]);

  const anyStatus = spots.some((s) => s.status);
  const propReady = qth != null && space != null;

  return (
    <GlassPanel
      title="DX Coach"
      icon={<Target className="w-5 h-5" />}
      actions={
        <span className="text-sm text-dark-300">
          {opportunities.length} opportunit{opportunities.length === 1 ? 'y' : 'ies'}
        </span>
      }
    >
      <div className="flex flex-col h-full">
        <div className="px-4 pt-3 pb-2 text-xs text-dark-400 border-b border-glass-100">
          Live spots that would fill an award gap — <span className="text-accent-primary">new DXCC</span> or a{' '}
          <span className="text-accent-secondary">new band-slot</span> — with a coarse propagation read. Click one to tune.{' '}
          {!propReady && (
            <span className="opacity-70">
              (Set your QTH grid in Settings → Station for the propagation gate.)
            </span>
          )}
        </div>
        <div className="flex-1 overflow-auto px-3 py-3 space-y-2 min-h-0">
          {opportunities.length === 0 ? (
            <div className="text-center py-10 text-dark-300 text-sm px-4">
              {anyStatus
                ? 'No award opportunities in the current spots — as spots arrive that fill a DXCC or band gap, they show up here, ranked.'
                : 'Waiting for spots. Connect a DX cluster (and make sure DX-cluster spot status is enabled) so the Coach can see what you still need.'}
            </div>
          ) : (
            opportunities.map((o) => {
              const openMeta = o.prop.open !== 'unknown' ? OPEN_META[o.prop.open] : null;
              // Activity tier for the spot-count chip: neutral (2), amber (3–5),
              // red (>5 — a pileup worth noticing).
              const countClass =
                o.count > 5
                  ? 'bg-red-500/20 text-red-300 border-red-500/50'
                  : o.count >= 3
                  ? 'bg-amber-400/20 text-amber-300 border-amber-400/50'
                  : 'bg-dark-600 text-dark-100 border-dark-400';
              return (
                <button
                  key={o.spot.id}
                  onClick={() => selectSpot(o.spot.dxCall, o.spot.frequency, o.spot.mode)}
                  title={`Tune the radio to this spot — ${o.prop.summary}`}
                  className="w-full text-left rounded-lg border border-glass-100 bg-dark-700/50 hover:bg-dark-700 hover:border-accent-primary/40 transition-colors p-3"
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="flex items-center gap-2 min-w-0">
                      <span
                        className={`text-xs font-ui px-2 py-0.5 rounded-full whitespace-nowrap ${
                          o.kind === 'newDxcc'
                            ? 'bg-accent-primary/20 text-accent-primary'
                            : 'bg-accent-secondary/20 text-accent-secondary'
                        }`}
                      >
                        {o.kind === 'newDxcc' ? '🌍 New DXCC' : '📻 New band'}
                      </span>
                      <span className="font-mono font-bold text-dark-100 truncate">{o.spot.dxCall}</span>
                      {o.count > 1 && (
                        <span
                          title={
                            o.count > 5
                              ? `Very active — ${o.count} spots on this band`
                              : `${o.count} spots on this band`
                          }
                          className={`inline-flex items-center justify-center text-xs font-ui font-bold px-2.5 py-0.5 min-w-[2.75rem] rounded-full border whitespace-nowrap ${countClass}`}
                        >
                          {o.count} ×
                        </span>
                      )}
                      {o.onRigBand && (
                        <span className="text-[10px] font-ui px-1.5 py-0.5 rounded bg-accent-success/20 text-accent-success whitespace-nowrap">
                          on your band
                        </span>
                      )}
                    </div>
                    <span className="text-xs font-mono text-dark-300 whitespace-nowrap">
                      {(o.spot.frequency / 1000).toFixed(3)}
                    </span>
                  </div>
                  <div className="mt-1.5 text-xs text-dark-300 flex items-center gap-2 flex-wrap">
                    <span className="text-dark-200">{o.spot.dxStation?.country || o.spot.country || 'Unknown entity'}</span>
                    <span className="opacity-50">·</span>
                    <span>{o.band}</span>
                    {o.spot.mode && (
                      <>
                        <span className="opacity-50">·</span>
                        <span>{o.spot.mode}</span>
                      </>
                    )}
                    <span className="opacity-50">·</span>
                    <span className="text-dark-400">
                      {o.kind === 'newDxcc' ? 'New country for DXCC' : `New band-slot on ${o.band}`}
                    </span>
                  </div>
                  {(openMeta || o.prop.grayLine) && (
                    <div className="mt-2 flex items-center gap-2 flex-wrap">
                      {openMeta && (
                        <span className={`inline-flex items-center gap-1.5 text-[11px] font-ui ${openMeta.text}`}>
                          <span className={`w-2 h-2 rounded-full ${openMeta.dot}`} />
                          {openMeta.label}
                          {o.prop.reliability != null && (
                            <span className="text-dark-400">{o.prop.reliability}%</span>
                          )}
                        </span>
                      )}
                      {o.prop.grayLine && (
                        <span
                          className={`text-[11px] font-ui px-1.5 py-0.5 rounded whitespace-nowrap ${
                            o.prop.grayLine.active
                              ? 'bg-amber-400/20 text-amber-300'
                              : 'bg-amber-400/10 text-amber-200/80'
                          }`}
                        >
                          🌅 {o.prop.grayLine.label}
                        </span>
                      )}
                    </div>
                  )}
                </button>
              );
            })
          )}
        </div>
      </div>
    </GlassPanel>
  );
}

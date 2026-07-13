import { useMemo } from 'react';
import { Target } from 'lucide-react';
import { GlassPanel } from '../components/GlassPanel';
import { useAppStore, Spot } from '../store/appStore';
import { useSignalR } from '../hooks/useSignalR';
import { getBandFromFrequency } from '../utils/spotBands';

/**
 * DX Coach — Phase 1 (deterministic, no AI).
 *
 * The "Opportunity Engine": scans the live DX-cluster spots and surfaces the
 * ones that would fill an award gap, using the per-spot status the backend
 * SpotStatusService already computes (`newDxcc` / `newBand` at DXCC + band
 * granularity). Ranked, with the reason spelled out, boosted when the spot is
 * on the band the rig is already parked on. State / zone gaps and the LLM
 * "coach voice" are later phases (see docs/design/ai-dx-coach.md).
 */
interface Opportunity {
  spot: Spot;
  band: string;
  kind: 'newDxcc' | 'newBand';
  onRigBand: boolean;
  score: number;
}

export function DxCoachPlugin() {
  const spots = useAppStore((s) => s.dxClusterSpots);
  const selectedRadioId = useAppStore((s) => s.selectedRadioId);
  const radioStates = useAppStore((s) => s.radioStates);
  const { selectSpot } = useSignalR();

  // What band is the rig sitting on right now? Opportunities there are the
  // easiest to jump on (no band change), so they rank a little higher.
  const rigBand = useMemo(() => {
    const rig = selectedRadioId ? radioStates.get(selectedRadioId) : undefined;
    return rig?.frequencyHz != null ? getBandFromFrequency(rig.frequencyHz / 1000) : null;
  }, [selectedRadioId, radioStates]);

  const opportunities = useMemo<Opportunity[]>(() => {
    const out: Opportunity[] = [];
    for (const s of spots) {
      // Only spots that fill a gap — a worked/unknown spot isn't an opportunity.
      if (s.status !== 'newDxcc' && s.status !== 'newBand') continue;
      const band = getBandFromFrequency(s.frequency);
      const onRigBand = rigBand != null && rigBand !== '?' && band === rigBand;
      const score = (s.status === 'newDxcc' ? 100 : 50) + (onRigBand ? 15 : 0);
      out.push({ spot: s, band, kind: s.status, onRigBand, score });
    }
    // Rank: a whole new entity beats a new band-slot; on-rig-band breaks near
    // ties; then most recent first.
    out.sort((a, b) => b.score - a.score || (b.spot.timestamp > a.spot.timestamp ? 1 : -1));
    return out;
  }, [spots, rigBand]);

  const anyStatus = spots.some((s) => s.status);

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
          <span className="text-accent-secondary">new band-slot</span>. Click one to tune.{' '}
          <span className="opacity-70">(Phase 1 — reasons from your logbook; state / zone coaching &amp; AI nudges to come.)</span>
        </div>
        <div className="flex-1 overflow-auto px-3 py-3 space-y-2 min-h-0">
          {opportunities.length === 0 ? (
            <div className="text-center py-10 text-dark-300 text-sm px-4">
              {anyStatus
                ? 'No award opportunities in the current spots — as spots arrive that fill a DXCC or band gap, they show up here, ranked.'
                : 'Waiting for spots. Connect a DX cluster (and make sure DX-cluster spot status is enabled) so the Coach can see what you still need.'}
            </div>
          ) : (
            opportunities.map((o) => (
              <button
                key={o.spot.id}
                onClick={() => selectSpot(o.spot.dxCall, o.spot.frequency, o.spot.mode)}
                title="Tune the radio to this spot"
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
              </button>
            ))
          )}
        </div>
      </div>
    </GlassPanel>
  );
}

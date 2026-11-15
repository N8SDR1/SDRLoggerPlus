import { useMemo, useEffect, useRef } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Target } from 'lucide-react';
import { GlassPanel } from '../components/GlassPanel';
import { useAppStore } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import { useSignalR } from '../hooks/useSignalR';
import { getBandFromFrequency } from '../utils/spotBands';
import { gridToLatLon } from '../utils/maidenhead';
import { buildOpportunities, type Confidence, type ReasonKind } from '../utils/dxCoach';
import { useMyReceptionReports } from '../hooks/useMyReceptionReports';
import { spellCallsign } from '../utils/hotSpotAnnouncer';
import { applyAnnouncementVoice } from '../utils/announcementVoice';
import { api } from '../api/client';

/**
 * DX Coach — live spots that fill an award gap (DXCC / WAZ), ranked by the best
 * evidence that you can work them. The engine lives in utils/dxCoach.ts; this is
 * the panel. See docs/design/ai-dx-coach.md.
 */
const REASON_CLASS: Record<ReasonKind, string> = {
  newDxcc: 'bg-accent-primary/20 text-accent-primary',
  newZone: 'bg-violet-500/25 text-violet-200',
  newBand: 'bg-accent-secondary/20 text-accent-secondary',
  newZoneBand: 'bg-violet-500/15 text-violet-200/90',
};

// Evidence colours: green = someone actually heard it (you, near you, your
// region); amber/grey = forecast only.
const CONFIDENCE_META: Record<Confidence, { dot: string; text: string }> = {
  heardMe: { dot: 'bg-accent-success', text: 'text-accent-success font-semibold' },
  heardNear: { dot: 'bg-accent-success', text: 'text-accent-success font-semibold' },
  heardRegion: { dot: 'bg-accent-success/70', text: 'text-accent-success/90' },
  likely: { dot: 'bg-sky-400', text: 'text-sky-300' },
  possible: { dot: 'bg-accent-warning', text: 'text-accent-warning' },
  unlikely: { dot: 'bg-dark-400', text: 'text-dark-300' },
  unknown: { dot: 'bg-dark-500', text: 'text-dark-400' },
};

/** Speak a high-value opportunity aloud, using the shared announcement voice. */
function speakOpportunity(
  dxCall: string,
  band: string,
  kind: ReasonKind,
  zone: number | undefined,
  country: string | undefined,
): void {
  if (typeof speechSynthesis === 'undefined') return;
  const bandSpoken = band.replace('cm', ' centimeters').replace('m', ' meters');
  const lead =
    kind === 'newDxcc'
      ? `New D X C C. ${spellCallsign(dxCall)}.${country ? ` ${country}.` : ''}`
      : `New zone ${zone ?? ''}. ${spellCallsign(dxCall)}.`;
  const u = new SpeechSynthesisUtterance(`${lead} ${bandSpoken}.`);
  applyAnnouncementVoice(u); // shared voice / accent / rate / volume
  // No cancel() — queue so several fresh opportunities don't cut each other off.
  speechSynthesis.speak(u);
}

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

  // Who has heard YOUR signal lately — the Coach's strongest evidence.
  const heardMe = useMyReceptionReports(station.callsign);

  // Live ionosondes correct the forecast's level (server caches 15 min).
  const { data: iono } = useQuery({
    queryKey: ['ionosondes'],
    queryFn: () => api.getIonosondes(),
    refetchInterval: 15 * 60 * 1000,
    staleTime: 10 * 60 * 1000,
  });
  const ionosondes = iono?.readings;

  const { opportunities, hiddenUnlikely } = useMemo(() => {
    const ctx = { qth, sfi: space?.solarFluxIndex ?? null, kIndex: space?.kIndex ?? null, rigBand, heardMe, ionosondes };
    const shown = buildOpportunities(spots, coach, ctx);
    // How many "unlikely" ones the optional filter is holding back, so a short
    // or empty list says why instead of looking broken.
    const all = coach.hideUnlikely ? buildOpportunities(spots, { ...coach, hideUnlikely: false }, ctx).length : shown.length;
    return { opportunities: shown, hiddenUnlikely: all - shown.length };
  }, [spots, rigBand, qth, space, coach, heardMe, ionosondes]);

  // Voice: announce newly-arriving high-value opportunities (new DXCC / new
  // zone) aloud, throttled. Enabling only speaks opportunities that appear
  // *after* it's turned on (existing ones are seeded silently), and a per-key
  // cooldown + freshness gate keep a pileup from spamming.
  const announcedRef = useRef<Map<string, number>>(new Map());
  const voiceWasOn = useRef(false);
  useEffect(() => {
    if (!coach.voice) {
      voiceWasOn.current = false;
      return;
    }
    const now = Date.now();
    const seedOnly = !voiceWasOn.current;
    voiceWasOn.current = true;
    const COOLDOWN = 10 * 60 * 1000;
    const FRESH = 3 * 60 * 1000;
    let spoken = 0;
    for (const o of opportunities) {
      const top = o.reasons[0];
      if (top.kind !== 'newDxcc' && top.kind !== 'newZone') continue; // rare/valuable only
      const key = `${o.spot.dxCall.toUpperCase()}|${o.band}|${top.kind}`;
      if (now - (announcedRef.current.get(key) ?? 0) < COOLDOWN) continue;
      announcedRef.current.set(key, now);
      if (seedOnly) continue; // silently seed what's already on screen
      const age = now - Date.parse(o.spot.timestamp);
      if (isNaN(age) || age > FRESH) continue; // don't replay stale spots
      if (spoken >= 3) continue; // cap a burst
      spoken++;
      speakOpportunity(o.spot.dxCall, o.band, top.kind, o.spot.cqZone, o.spot.dxStation?.country || o.spot.country);
    }
  }, [opportunities, coach.voice]);

  const anyStatus = spots.some((s) => s.status || s.zoneStatus);
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
        <div className="px-4 pt-3 pb-2 text-sm text-dark-300 border-b border-glass-100">
          Live spots that fill an award gap — <span className="text-accent-primary">new DXCC</span>,{' '}
          <span className="text-violet-300">new CQ zone</span>, or a new band-slot — best chances first. Click one to tune.
          {!propReady && (
            <span className="block mt-1 text-accent-warning">
              Set your grid square in Settings → Station so the Coach can tell which spotters are near you.
            </span>
          )}
          {ionosondes && ionosondes.length > 0 && (
            <span className="block mt-1 text-xs text-dark-400">
              Forecasts use {ionosondes.length} live ionosonde readings. {iono?.attribution}
            </span>
          )}
          {hiddenUnlikely > 0 && (
            <span className="block mt-1 text-dark-400">
              {hiddenUnlikely} unlikely path{hiddenUnlikely === 1 ? '' : 's'} hidden (Settings → DX Coach).
            </span>
          )}
        </div>
        <div className="flex-1 overflow-auto px-3 py-3 space-y-2 min-h-0">
          {opportunities.length === 0 ? (
            <div className="text-center py-10 text-dark-300 text-sm px-4">
              {!anyStatus
                ? 'Waiting for spots. Connect a DX cluster (and make sure DX-cluster spot status is enabled) so the Coach can see what you still need.'
                : hiddenUnlikely > 0
                ? `Only unlikely paths right now, and "Hide unlikely paths" is on (${hiddenUnlikely} hidden). Turn it off in Settings → DX Coach to see them.`
                : 'No award opportunities in the current spots — as spots arrive that fill a DXCC, zone, or band gap, they show up here, ranked.'}
            </div>
          ) : (
            opportunities.map((o) => {
              const ev = CONFIDENCE_META[o.evidence.confidence];
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
                  title={`Tune the radio to this spot — ${o.evidence.detail}`}
                  className="w-full text-left rounded-lg border border-glass-100 bg-dark-700/50 hover:bg-dark-700 hover:border-accent-primary/40 transition-colors p-3"
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="flex items-center gap-2 min-w-0 flex-wrap">
                      {o.reasons.map((r) => (
                        <span
                          key={r.kind}
                          className={`text-xs font-ui px-2 py-0.5 rounded-full whitespace-nowrap ${REASON_CLASS[r.kind]}`}
                        >
                          {r.chip}
                        </span>
                      ))}
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
                    {o.spot.cqZone != null && (
                      <>
                        <span className="opacity-50">·</span>
                        <span>Zone {o.spot.cqZone}</span>
                      </>
                    )}
                    <span className="opacity-50">·</span>
                    <span>{o.band}</span>
                    {o.spot.mode && (
                      <>
                        <span className="opacity-50">·</span>
                        <span>{o.spot.mode}</span>
                      </>
                    )}
                    <span className="opacity-50">·</span>
                    <span className="text-dark-400">{o.reasons.map((r) => r.detail).join(' + ')}</span>
                  </div>
                  <div className="mt-2 flex items-start gap-2 flex-wrap text-sm">
                    <span className={`inline-flex items-center gap-1.5 whitespace-nowrap ${ev.text}`}>
                      <span className={`w-2.5 h-2.5 rounded-full ${ev.dot}`} />
                      {o.evidence.label}
                    </span>
                    <span className="text-dark-300 min-w-0">{o.evidence.detail}</span>
                    {o.prop.grayLine && (
                      <span
                        className={`text-xs font-ui px-1.5 py-0.5 rounded whitespace-nowrap ${
                          o.prop.grayLine.active
                            ? 'bg-amber-400/20 text-amber-300'
                            : 'bg-amber-400/10 text-amber-200/80'
                        }`}
                      >
                        🌅 {o.prop.grayLine.label}
                      </span>
                    )}
                  </div>
                </button>
              );
            })
          )}
        </div>
      </div>
    </GlassPanel>
  );
}

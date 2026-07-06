import { useEffect, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Satellite, Power } from 'lucide-react';
import { api, SatState } from '../api/client';
import { GlassPanel } from '../components/GlassPanel';
import { signalRService } from '../api/signalr';
import { useSettingsStore } from '../store/settingsStore';

const STATUS_LABELS: Record<string, { text: string; cls: string }> = {
  idle: { text: 'Idle', cls: 'text-dark-200' },
  tracking: { text: 'Tracking', cls: 'text-accent-secondary' },
  aos: { text: '● PASS IN PROGRESS', cls: 'text-accent-success' },
  los: { text: 'LOS', cls: 'text-orange-400' },
};

// mm:ss formatter for ttaos / ttlos — matches v1.x "AOS in 13:08" style.
const fmtSecs = (s: number): string => {
  const total = Math.max(0, Math.round(s));
  const m = Math.floor(total / 60);
  const sec = total % 60;
  return `${m}:${String(sec).padStart(2, '0')}`;
};

// Small labelled row used in the tracking-info grid — label stays dimmer
// than the value, but both are pushed up from the near-invisible dark-400
// the panel used to have so the text actually reads on the glass background.
const StatRow = ({ label, value, valueClass }: { label: string; value: React.ReactNode; valueClass?: string }) => (
  <div className="flex items-baseline justify-between gap-2">
    <span className="text-xs text-dark-200 font-ui">{label}</span>
    <span className={`text-sm font-mono text-gray-100 tabular-nums ${valueClass ?? ''}`}>{value}</span>
  </div>
);

/**
 * CSN Technologies S.A.T. controller panel: live pass status, transponder
 * info, auto-logged pass QSOs, and the controller event log. The Activate
 * toggle binds/releases the UDP listeners so the ports stay free for other
 * tools while inactive.
 */
export function SatPlugin() {
  const satEnabled = useSettingsStore(state => state.settings.sat.enabled);
  const queryClient = useQueryClient();
  const [liveState, setLiveState] = useState<SatState | null>(null);

  const { data: polled } = useQuery({
    queryKey: ['sat-status'],
    queryFn: () => api.getSatStatus(),
    refetchInterval: 5000,
    enabled: satEnabled,
  });

  useEffect(() => {
    signalRService.setHandlers({ onSatState: (state) => setLiveState(state) });
    return () => signalRService.setHandlers({ onSatState: undefined });
  }, []);

  const state = liveState ?? polled ?? null;
  const status = STATUS_LABELS[state?.status ?? 'idle'] ?? STATUS_LABELS.idle;

  const setActive = async (active: boolean) => {
    try {
      await api.setSatActive(active);
      queryClient.invalidateQueries({ queryKey: ['sat-status'] });
    } catch {
      // Backend unavailable
    }
  };

  const elapsed = state?.aosTimeUtc
    ? Math.max(0, Math.floor((Date.now() - new Date(state.aosTimeUtc).getTime()) / 1000))
    : null;

  return (
    <GlassPanel
      title="S.A.T. Controller"
      icon={<Satellite className="w-5 h-5" />}
      actions={
        satEnabled && (
          <button
            onClick={() => setActive(!(state?.active ?? false))}
            className={`flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium transition-colors ${
              state?.active
                ? 'bg-accent-success/20 text-accent-success'
                : 'bg-dark-600 text-dark-300 hover:text-gray-200'
            }`}
            title={state?.active ? 'Deactivate (releases UDP ports)' : 'Activate SAT mode'}
          >
            <Power className="w-3 h-3" />
            {state?.active ? 'Active' : 'Activate'}
          </button>
        )
      }
    >
      <div className="h-full overflow-y-auto p-4 space-y-3 text-sm">
        {!satEnabled ? (
          <p className="text-dark-200 text-xs">
            S.A.T. integration is disabled. Enable it and set the controller IP in Settings → S.A.T.
          </p>
        ) : !state?.active ? (
          <p className="text-dark-200 text-xs">
            Inactive. Click Activate to bind the S.A.T. UDP listeners and start polling the controller.
          </p>
        ) : (
          <>
            <div className="flex items-center justify-between">
              <span className={`font-semibold ${status.cls}`}>{status.text}</span>
              {elapsed != null && (
                <span className="text-xs font-mono text-dark-100">
                  {Math.floor(elapsed / 60)}:{String(elapsed % 60).padStart(2, '0')} since AOS
                </span>
              )}
            </div>

            {state.error && <p className="text-xs text-red-400">{state.error}</p>}

            {state.satellite && (
              <div className="p-3 bg-dark-700 rounded-lg space-y-2">
                {/* Header: satellite + catalog # + tracking status pill */}
                <div className="flex items-center justify-between">
                  <p className="text-gray-100 font-bold font-mono text-base">
                    🛰 {state.satellite}
                    {state.catalogNumber && (
                      <span className="text-dark-100 font-normal ml-2">#{state.catalogNumber}</span>
                    )}
                  </p>
                </div>

                {/* v1.x parity rows — Satellite Status panel from SDRLogger+ v1
                    surfaces these six numbers front-and-centre. Only rows with
                    live data are shown so an idle pre-AOS state doesn't render
                    a wall of dashes. */}
                <div className="grid grid-cols-2 gap-x-4 gap-y-1 pt-1">
                  {(state.azDeg != null || state.elDeg != null) && (
                    <StatRow
                      label="AZ / EL"
                      value={
                        <>
                          <span className="text-accent-secondary">{state.azDeg?.toFixed(1) ?? '—'}°</span>
                          <span className="text-dark-100 mx-1">/</span>
                          <span className={state.elDeg != null && state.elDeg > 0 ? 'text-accent-success' : 'text-dark-100'}>
                            {state.elDeg?.toFixed(1) ?? '—'}°
                          </span>
                        </>
                      }
                    />
                  )}
                  {state.maxElDeg != null && (
                    <StatRow label="Max EL" value={`${state.maxElDeg.toFixed(1)}°`} />
                  )}
                  {state.rangeKm != null && (
                    <StatRow
                      label="Range"
                      value={`${state.rangeKm.toLocaleString(undefined, { maximumFractionDigits: 1 })} km`}
                    />
                  )}
                  {state.aosAzimuth && <StatRow label="AOS Az" value={`${state.aosAzimuth}°`} />}
                  {state.losAzimuth && <StatRow label="LOS Az" value={`${state.losAzimuth}°`} />}
                  {state.timeToLosSec != null && state.timeToLosSec > 0 && (
                    <StatRow
                      label="Time to LOS"
                      value={fmtSecs(state.timeToLosSec)}
                      valueClass="text-accent-primary"
                    />
                  )}
                  {state.timeToAosSec != null && state.timeToAosSec > 0 && (
                    <StatRow
                      label="AOS in"
                      value={fmtSecs(state.timeToAosSec)}
                      valueClass="text-accent-primary"
                    />
                  )}
                  {elapsed != null && (
                    <StatRow label="Elapsed" value={fmtSecs(elapsed)} />
                  )}
                </div>

                {/* Transponder subsection — matches v1.x "TRANSPONDER" block */}
                {state.transponder && (
                  <div className="pt-2 border-t border-glass-100 space-y-1">
                    <p className="text-[10px] font-ui text-dark-100 tracking-wider uppercase">Transponder</p>
                    <p className="text-sm font-mono text-gray-100">{state.transponder}</p>
                    <div className="text-xs font-mono text-dark-100 flex gap-3">
                      {state.uplinkFreq && (
                        <span>↑ <span className="text-gray-100">{state.uplinkFreq}</span> {state.uplinkMode}</span>
                      )}
                      {state.downlinkFreq && (
                        <span>↓ <span className="text-gray-100">{state.downlinkFreq}</span> {state.downlinkMode}</span>
                      )}
                    </div>
                  </div>
                )}

                {state.map && (
                  <p className="text-xs text-dark-100 pt-1">
                    Sub-point {state.map.lat.toFixed(1)}°, {state.map.lon.toFixed(1)}° ·
                    alt {Math.round(state.map.altKm)} km · footprint r={Math.round(state.map.footprintRadiusKm)} km
                  </p>
                )}
              </div>
            )}

            {state.passQsos.length > 0 && (
              <div>
                <p className="text-xs text-dark-100 mb-1 font-ui">Pass QSOs ({state.passQsos.length})</p>
                <div className="space-y-0.5">
                  {state.passQsos.map((q, i) => (
                    <p key={i} className="text-xs font-mono text-gray-100">
                      {q.callsign} <span className="text-dark-100">{q.grid} {q.mode} on {q.satName}</span>
                    </p>
                  ))}
                </div>
              </div>
            )}

            {state.events.length > 0 && (
              <div>
                <p className="text-xs text-dark-100 mb-1 font-ui">Events</p>
                <div className="space-y-0.5 max-h-40 overflow-y-auto">
                  {[...state.events].reverse().map((e, i) => (
                    <p key={i} className="text-xs text-dark-200">
                      <span className="font-mono text-dark-100">
                        {new Date(e.timeUtc).toLocaleTimeString()}
                      </span>{' '}
                      {e.detail}
                    </p>
                  ))}
                </div>
              </div>
            )}

            {state.lastHeardUtc && (
              <p className="text-xs text-dark-100">
                Last heard: {new Date(state.lastHeardUtc).toLocaleTimeString()}
              </p>
            )}
          </>
        )}
      </div>
    </GlassPanel>
  );
}

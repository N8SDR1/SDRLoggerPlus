import { useEffect, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Satellite, Power } from 'lucide-react';
import { api, SatState } from '../api/client';
import { GlassPanel } from '../components/GlassPanel';
import { signalRService } from '../api/signalr';
import { useSettingsStore } from '../store/settingsStore';

const STATUS_LABELS: Record<string, { text: string; cls: string }> = {
  idle: { text: 'Idle', cls: 'text-dark-400' },
  tracking: { text: 'Tracking', cls: 'text-accent-secondary' },
  aos: { text: '● PASS IN PROGRESS', cls: 'text-accent-success' },
  los: { text: 'LOS', cls: 'text-orange-400' },
};

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
          <p className="text-dark-400 text-xs">
            S.A.T. integration is disabled. Enable it and set the controller IP in Settings → S.A.T.
          </p>
        ) : !state?.active ? (
          <p className="text-dark-400 text-xs">
            Inactive. Click Activate to bind the S.A.T. UDP listeners and start polling the controller.
          </p>
        ) : (
          <>
            <div className="flex items-center justify-between">
              <span className={`font-semibold ${status.cls}`}>{status.text}</span>
              {elapsed != null && (
                <span className="text-xs font-mono text-dark-300">
                  {Math.floor(elapsed / 60)}:{String(elapsed % 60).padStart(2, '0')} since AOS
                </span>
              )}
            </div>

            {state.error && <p className="text-xs text-red-400">{state.error}</p>}

            {state.satellite && (
              <div className="p-2 bg-dark-700 rounded-lg space-y-1 text-xs">
                <p className="text-dark-200 font-semibold font-mono text-sm">
                  🛰 {state.satellite} {state.catalogNumber && <span className="text-dark-400">#{state.catalogNumber}</span>}
                </p>
                {(state.aosAzimuth || state.losAzimuth) && (
                  <p className="text-dark-300">
                    {state.aosAzimuth && `AOS ${state.aosAzimuth}°`}
                    {state.aosAzimuth && state.losAzimuth && ' · '}
                    {state.losAzimuth && `LOS ${state.losAzimuth}°`}
                  </p>
                )}
                {state.transponder && (
                  <p className="text-dark-300">
                    {state.transponder}: ↑{state.uplinkFreq} {state.uplinkMode} ↓{state.downlinkFreq} {state.downlinkMode}
                  </p>
                )}
                {state.map && (
                  <p className="text-dark-400">
                    Sub-point {state.map.lat.toFixed(1)}°, {state.map.lon.toFixed(1)}° ·
                    alt {Math.round(state.map.altKm)} km · footprint r={Math.round(state.map.footprintRadiusKm)} km
                  </p>
                )}
              </div>
            )}

            {state.passQsos.length > 0 && (
              <div>
                <p className="text-xs text-dark-400 mb-1">Pass QSOs ({state.passQsos.length})</p>
                <div className="space-y-0.5">
                  {state.passQsos.map((q, i) => (
                    <p key={i} className="text-xs font-mono text-dark-200">
                      {q.callsign} <span className="text-dark-400">{q.grid} {q.mode} on {q.satName}</span>
                    </p>
                  ))}
                </div>
              </div>
            )}

            {state.events.length > 0 && (
              <div>
                <p className="text-xs text-dark-400 mb-1">Events</p>
                <div className="space-y-0.5 max-h-40 overflow-y-auto">
                  {[...state.events].reverse().map((e, i) => (
                    <p key={i} className="text-xs text-dark-300">
                      <span className="font-mono text-dark-400">
                        {new Date(e.timeUtc).toLocaleTimeString()}
                      </span>{' '}
                      {e.detail}
                    </p>
                  ))}
                </div>
              </div>
            )}

            {state.lastHeardUtc && (
              <p className="text-xs text-dark-400">
                Last heard: {new Date(state.lastHeardUtc).toLocaleTimeString()}
              </p>
            )}
          </>
        )}
      </div>
    </GlassPanel>
  );
}

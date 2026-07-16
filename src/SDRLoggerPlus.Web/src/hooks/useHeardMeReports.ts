import { useEffect, useState } from 'react';
import { api, type PskReceptionReport, type RbnHeardMeReport } from '../api/client';
import { useSettingsStore } from '../store/settingsStore';
import { useAppStore } from '../store/appStore';

const clamp = (n: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, n));

/** Polls PSK + RBN "who heard me" per the enabled Heard-Me layers. */
export function useHeardMeReports() {
  const { settings } = useSettingsStore();
  const { radioStates, selectedRadioId } = useAppStore();
  const map = settings.map;
  const call = (settings.station.callsign || '').trim();

  // Active band: 'all' means no band filter (overrides rig-follow); otherwise the
  // live rig band when connected, else the manual fallback.
  const rigBand = selectedRadioId ? radioStates.get(selectedRadioId)?.band ?? null : null;
  const activeBand = map.heardMeBand === 'all' ? null : (rigBand || map.heardMeBand || null);

  const [psk, setPsk] = useState<PskReceptionReport[]>([]);
  const [rbn, setRbn] = useState<RbnHeardMeReport[]>([]);

  const pskWindow = clamp(map.heardMePskWindowMinutes ?? 60, 5, 60);
  const rbnWindow = clamp(map.heardMeRbnWindowMinutes ?? 15, 5, 15);

  // PSK: 5-min cadence (etiquette + backend cache).
  useEffect(() => {
    if (!map.showGlobeHeardMePsk || !call) { setPsk([]); return; }
    let cancelled = false;
    const run = async () => {
      try { const r = await api.getPskReports(call, pskWindow); if (!cancelled) setPsk(r); } catch { /* keep last */ }
    };
    run();
    const id = setInterval(run, 5 * 60 * 1000);
    return () => { cancelled = true; clearInterval(id); };
  }, [map.showGlobeHeardMePsk, call, pskWindow]);

  // RBN: ~45s cadence.
  useEffect(() => {
    if (!map.showGlobeHeardMeRbn || !call) { setRbn([]); return; }
    let cancelled = false;
    const run = async () => {
      try { const r = await api.getRbnHeardMe(call, activeBand, rbnWindow); if (!cancelled) setRbn(r); } catch { /* keep last */ }
    };
    run();
    const id = setInterval(run, 45 * 1000);
    return () => { cancelled = true; clearInterval(id); };
  }, [map.showGlobeHeardMeRbn, call, activeBand, rbnWindow]);

  return { psk, rbn, activeBand };
}

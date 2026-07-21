import { useEffect, useState } from 'react';
import { api, type WsjtxStatus } from '../api/client';
import { useSettingsStore } from '../store/settingsStore';
import { summarizeWsjtxLink, type WsjtxLink } from '../utils/wsjtxLink';

/** Matches the existing Settings > WSJT-X poll, and re-renders often enough
 *  that the pill goes stale within a few seconds of the heartbeat lapsing. */
const POLL_MS = 5_000;

/**
 * Live health of the WSJT-X/JTDX decoder link for the status bar.
 *
 * Polling is gated on the settings toggles: an operator who never runs digital
 * modes should not generate a request every five seconds forever. When both
 * sources are off the hook reports 'off' without touching the network.
 */
export function useWsjtxLink(): WsjtxLink {
  const primaryEnabled = useSettingsStore((s) => s.settings.wsjtx.enabled);
  const secondaryEnabled = useSettingsStore((s) => s.settings.wsjtx.source2.enabled);
  const anyEnabled = primaryEnabled || secondaryEnabled;

  const [statuses, setStatuses] = useState<WsjtxStatus[] | null>(null);
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    if (!anyEnabled) {
      setStatuses(null);
      return;
    }
    let cancelled = false;
    const poll = async () => {
      try {
        const st = await api.getWsjtxStatus();
        if (!cancelled) {
          setStatuses(st);
          setNow(Date.now());
        }
      } catch {
        // A failed status fetch says nothing about the decoder — it means the
        // backend is unreachable, which the connection overlay already reports.
        if (!cancelled) setStatuses(null);
      }
    };
    poll();
    const timer = setInterval(poll, POLL_MS);
    return () => { cancelled = true; clearInterval(timer); };
  }, [anyEnabled]);

  return summarizeWsjtxLink(statuses, now);
}

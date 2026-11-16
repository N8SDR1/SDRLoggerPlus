import { useCallback, useEffect, useState } from 'react';
import { Users } from 'lucide-react';
import { GlassPanel } from '../components/GlassPanel';
import { useSettingsStore } from '../store/settingsStore';

interface QrvRosterRow {
  call: string;
  app: string;
  band: string;
  mode: string;
  lastSeen?: string | null;
  hz?: number | null;
}

interface QrvStatus {
  enabled: boolean;
  onAir: boolean;
  phase: 'off' | 'waiting' | 'on-air' | 'error' | string;
  call?: string | null;
  band?: string | null;
  mode?: string | null;
  error?: string | null;
  detail?: string | null;
  installKeyPrefix?: string | null;
  roster: QrvRosterRow[];
}

const EMPTY: QrvStatus = {
  enabled: false,
  onAir: false,
  phase: 'off',
  detail: 'Presence is off.',
  roster: [],
};

async function readStatus(watch: boolean): Promise<QrvStatus> {
  const response = await fetch(watch ? '/api/qrv?watch=true' : '/api/qrv');
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const body = (await response.json()) as QrvStatus;
  return { ...EMPTY, ...body, roster: Array.isArray(body.roster) ? body.roster : [] };
}

/**
 * QRV board roster. Presence only — the server publishes call, band, and mode
 * when the operator opts in. This panel does not send Hz, notes, or the log.
 */
export function QrvPlugin() {
  const qrv = useSettingsStore((s) => s.settings.qrv);
  const enabled = qrv?.enabled ?? false;
  const [status, setStatus] = useState<QrvStatus>({ ...EMPTY, enabled });
  const [loadError, setLoadError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const next = await readStatus(true);
      setStatus(next);
      setLoadError(null);
    } catch {
      setLoadError('QRV status unavailable');
    }
  }, []);

  useEffect(() => {
    let stopped = false;
    const pull = () => {
      if (!stopped) void refresh();
    };
    pull();
    const timer = window.setInterval(pull, 5000);
    return () => {
      stopped = true;
      window.clearInterval(timer);
      fetch('/api/qrv/watch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ watching: false }),
      }).catch(() => {});
    };
  }, [refresh]);

  const toggle = async () => {
    const next = !enabled;
    setSaving(true);
    useSettingsStore.setState((state) => ({
      settings: {
        ...state.settings,
        qrv: { ...(state.settings.qrv ?? { enabled: false, baseUrl: 'https://qrvsdr.cc/v1' }), enabled: next },
      },
    }));
    try {
      const response = await fetch('/api/qrv/settings', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled: next }),
      });
      if (response.ok) {
        const body = (await response.json()) as QrvStatus;
        setStatus({ ...EMPTY, ...body, roster: Array.isArray(body.roster) ? body.roster : status.roster });
        setLoadError(null);
      } else {
        setLoadError('Could not save QRV');
      }
    } catch {
      setLoadError('Could not save QRV');
    } finally {
      setSaving(false);
    }
  };

  const phase = loadError ? 'error' : status.phase;
  const roster = status.roster ?? [];
  const showHz = roster.some((row) => row.hz != null && row.hz > 0);
  const publishing = status.call && status.band && status.mode
    ? `${status.call}  ${status.band}  ${status.mode}`
    : null;

  return (
    <GlassPanel title="QRV" icon={<Users className="w-4 h-4" />}>
      <div className="flex flex-col h-full min-h-0 p-3 gap-3">
        <div className="flex items-center justify-between gap-3">
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              <StatusPill phase={phase} />
              {publishing && (
                <span className="text-xs font-mono text-dark-200 truncate">{publishing}</span>
              )}
            </div>
            <p className="text-xs text-dark-400 mt-1">
              {loadError ?? status.error ?? status.detail ?? 'Presence is off.'}
              {status.installKeyPrefix ? ` · key ${status.installKeyPrefix}` : ''}
            </p>
          </div>
          <button
            type="button"
            onClick={() => { void toggle(); }}
            disabled={saving}
            aria-pressed={enabled}
            aria-label="Share QRV presence"
            className={`shrink-0 px-3 py-1.5 rounded-md text-xs font-ui font-medium border transition-colors ${
              enabled
                ? 'bg-accent-success/15 border-accent-success/40 text-accent-success'
                : 'bg-dark-700 border-glass-200 text-dark-200 hover:text-dark-100'
            }`}
          >
            {enabled ? 'QRV on' : 'QRV off'}
          </button>
        </div>

        <p className="text-[11px] text-dark-400">
          Presence only. Band and mode are published while a radio is connected. Exact frequency is not sent.
        </p>

        <div className="flex-1 min-h-0 overflow-auto border border-glass-100 rounded-md">
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-dark-800 text-dark-400">
              <tr>
                <th className="text-left font-medium px-2 py-1.5">Call</th>
                <th className="text-left font-medium px-2 py-1.5">App</th>
                <th className="text-left font-medium px-2 py-1.5">Band</th>
                <th className="text-left font-medium px-2 py-1.5">Mode</th>
                {showHz && <th className="text-right font-medium px-2 py-1.5">Hz</th>}
                <th className="text-left font-medium px-2 py-1.5">Seen</th>
              </tr>
            </thead>
            <tbody>
              {roster.length === 0 && (
                <tr>
                  <td colSpan={showHz ? 6 : 5} className="px-2 py-4 text-dark-400">
                    No stations on the board.
                  </td>
                </tr>
              )}
              {roster.map((row, index) => (
                <tr key={`${row.call}-${row.app}-${index}`} className="border-t border-glass-100 text-dark-100">
                  <td className="px-2 py-1.5 font-mono">{row.call}</td>
                  <td className="px-2 py-1.5">{row.app}</td>
                  <td className="px-2 py-1.5 font-mono">{row.band}</td>
                  <td className="px-2 py-1.5 font-mono">{row.mode}</td>
                  {showHz && <td className="px-2 py-1.5 text-right font-mono">{row.hz ?? ''}</td>}
                  <td className="px-2 py-1.5 text-dark-300">{formatSeen(row.lastSeen)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </GlassPanel>
  );
}

function StatusPill({ phase }: { phase: string }) {
  const label = phase === 'on-air' ? 'On the air' : phase === 'waiting' ? 'Waiting' : phase === 'error' ? 'Error' : 'Off';
  const color = phase === 'on-air'
    ? 'bg-accent-success/15 text-accent-success'
    : phase === 'error'
      ? 'bg-red-500/15 text-red-300'
      : phase === 'waiting'
        ? 'bg-amber-500/15 text-amber-200'
        : 'bg-dark-600 text-dark-300';
  return <span className={`px-2 py-0.5 rounded text-[11px] font-medium ${color}`}>{label}</span>;
}

function formatSeen(value?: string | null): string {
  if (!value) return '';
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) return value;
  return value.replace('T', ' ').replace('Z', 'Z');
}

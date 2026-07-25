import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { RefreshCw, AlertTriangle } from 'lucide-react';
import { api, FfmaGridStatus } from '../api/client';

// FFMA is confirmation-driven and all-or-nothing at 488, so the useful view is a
// checklist — what's confirmed, what's worked-but-unconfirmed, and what's still
// needed — not just a progress number. Grids are grouped by Maidenhead field.

type StatusFilter = 'all' | 'needed' | 'worked' | 'confirmed';

const STATUS_TABS: { value: StatusFilter; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'needed', label: 'Needed' },
  { value: 'worked', label: 'Worked, not confirmed' },
  { value: 'confirmed', label: 'Confirmed' },
];

const CELL_STYLE: Record<FfmaGridStatus['status'], string> = {
  confirmed: 'bg-accent-success/20 text-accent-success border-accent-success/30',
  worked: 'bg-accent-warning/20 text-accent-warning border-accent-warning/30',
  needed: 'bg-dark-700/60 text-dark-400 border-glass-100',
};

export function FfmaStatisticsTab() {
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all');

  const { data, isLoading, error, refetch } = useQuery({
    // Nested under 'statistics' so logging a QSO invalidates it (prefix match).
    queryKey: ['statistics', 'ffma'],
    queryFn: () => api.getFfmaStatistics(),
    staleTime: 60_000,
  });

  const byField = useMemo(() => {
    if (!data) return [];
    const shown = data.grids.filter((g) => statusFilter === 'all' || g.status === statusFilter);
    const map = new Map<string, FfmaGridStatus[]>();
    for (const g of shown) {
      const field = g.grid.slice(0, 2);
      (map.get(field) ?? map.set(field, []).get(field)!).push(g);
    }
    return [...map.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  }, [data, statusFilter]);

  if (isLoading) return <div className="p-4 text-sm text-dark-300">Loading FFMA progress…</div>;
  if (error) return <div className="p-4 text-sm text-accent-danger">Failed to load FFMA statistics.</div>;
  if (!data) return null;

  const remaining = data.totalRequired - data.confirmed;
  const pct = data.totalRequired > 0 ? Math.round((data.confirmed / data.totalRequired) * 100) : 0;
  const workedPct = data.totalRequired > 0 ? Math.round((data.worked / data.totalRequired) * 100) : 0;

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="flex-shrink-0 px-4 py-3 border-b border-glass-100">
        <div className="flex items-center justify-between mb-2">
          <div>
            <h3 className="text-sm font-semibold font-ui text-dark-100">Fred Fish Memorial Award</h3>
            <p className="text-xs text-dark-400">All 488 grids of the lower 48 · 6 m · confirmed by LoTW or QSL</p>
          </div>
          <button
            onClick={() => refetch()}
            className="p-1.5 rounded text-dark-300 hover:text-accent-primary hover:bg-dark-700 transition-colors"
            title="Refresh"
          >
            <RefreshCw className="w-4 h-4" />
          </button>
        </div>

        {!data.listComplete && (
          <div className="flex items-start gap-2 mb-2 px-2.5 py-2 rounded border border-accent-warning/30 bg-accent-warning/5 text-xs text-accent-warning">
            <AlertTriangle className="w-3.5 h-3.5 shrink-0 mt-0.5" />
            <span>The grid roster loaded isn't the full 488 ({data.totalRequired} loaded) — progress shown is against a partial list.</span>
          </div>
        )}

        {/* Confirmed (green) over worked (amber) over the 488 track. */}
        <div className="bg-dark-700 rounded-full h-2 relative mb-1">
          <div className="bg-accent-warning h-2 rounded-full absolute top-0 left-0 transition-all" style={{ width: `${workedPct}%` }} />
          <div className="bg-accent-success h-2 rounded-full absolute top-0 left-0 transition-all" style={{ width: `${pct}%` }} />
        </div>
        <div className="flex items-center justify-between text-xs">
          <span className="text-accent-success font-semibold">{data.confirmed} confirmed</span>
          <span className="text-accent-warning">{data.worked - data.confirmed} worked, unconfirmed</span>
          <span className="text-dark-300">{remaining} to go</span>
          <span className="text-gray-300 font-mono">{data.confirmed} / {data.totalRequired}</span>
        </div>
      </div>

      <div className="flex-shrink-0 px-4 pt-2 pb-2 border-b border-glass-100 flex gap-1">
        {STATUS_TABS.map((t) => (
          <button
            key={t.value}
            onClick={() => setStatusFilter(t.value)}
            className={`px-2.5 py-1 text-xs font-ui rounded transition-colors ${
              statusFilter === t.value
                ? 'bg-accent-secondary/20 text-accent-secondary font-semibold'
                : 'text-dark-300 hover:text-gray-300 hover:bg-dark-700/50'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-y-auto px-4 py-3">
        {byField.length === 0 ? (
          <p className="text-xs text-dark-400 italic">
            {statusFilter === 'needed' ? '🎉 No grids needed with this filter.' : 'Nothing to show.'}
          </p>
        ) : (
          <div className="space-y-3">
            {byField.map(([field, grids]) => (
              <div key={field}>
                <div className="text-[11px] uppercase tracking-wider text-dark-400 font-ui mb-1">{field} · {grids.length}</div>
                <div className="flex flex-wrap gap-1">
                  {grids.map((g) => (
                    <span
                      key={g.grid}
                      title={g.status === 'needed' ? 'Needed' : `${g.status} · ${g.qsoCount} QSO${g.qsoCount === 1 ? '' : 's'}`}
                      className={`px-1.5 py-0.5 rounded border text-[11px] font-mono ${CELL_STYLE[g.status]}`}
                    >
                      {g.grid}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, AwardFilters } from '../api/client';
import { ALL_BANDS as BANDS } from '../utils/spotBands';

/** Worked/confirmed progress within one bar: confirmed fills over worked. */
function ProgressBar({ worked, confirmed, target }: { worked: number; confirmed: number; target: number }) {
  const pct = (n: number) => (target > 0 ? Math.min(100, (n / target) * 100) : 0);
  return (
    <div className="relative h-1.5 w-full rounded-sm bg-glass-100 overflow-hidden">
      <div className="absolute inset-y-0 left-0 bg-accent-warning" style={{ width: `${pct(worked)}%` }} />
      <div className="absolute inset-y-0 left-0 bg-accent-success" style={{ width: `${pct(confirmed)}%` }} />
    </div>
  );
}

function Stat({ label, value, hint }: { label: string; value: number | string; hint?: string }) {
  return (
    <div className="px-3 py-2 rounded bg-glass-100" title={hint}>
      <div className="text-[10px] uppercase tracking-wide text-dark-200">{label}</div>
      <div className="text-lg font-semibold tabular-nums">{value}</div>
    </div>
  );
}

const day = (iso: string | null) => (iso ? iso.slice(0, 10) : '—');

export function SatStatisticsTab() {
  const [filters, setFilters] = useState<AwardFilters>({});

  const { data, isLoading, error } = useQuery({
    // Nested under 'statistics' so QSO create/edit/delete/import invalidation
    // (which targets ['statistics']) reaches this tab. React Query matches by
    // key prefix — a standalone key silently never refreshes.
    queryKey: ['statistics', 'satellites', filters],
    queryFn: () => api.getSatelliteStatistics(filters),
    staleTime: 60_000,
  });

  const vuccPct = data && data.vuccThreshold > 0
    ? ((data.uniqueGrids / data.vuccThreshold) * 100).toFixed(0) : '0';

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="flex-shrink-0 px-4 py-2 border-b border-glass-100 flex flex-wrap gap-2 items-center">
        <select
          value={filters.band ?? ''}
          onChange={e => setFilters(prev => ({ ...prev, band: e.target.value || undefined }))}
          className="glass-input text-xs px-2 py-1"
        >
          <option value="">All bands</option>
          {BANDS.map(b => <option key={b} value={b}>{b}</option>)}
        </select>
        <input
          type="text"
          value={filters.mode ?? ''}
          onChange={e => setFilters(prev => ({ ...prev, mode: e.target.value || undefined }))}
          className="glass-input text-xs px-2 py-1 w-24"
          placeholder="Mode"
        />
        {data && (
          <span className="text-xs text-dark-100 ml-auto">
            <span className="text-accent-warning font-semibold">{data.totalQsos}</span> QSOs
            {' · '}
            <span className="font-semibold">{data.totalSatellites}</span> satellite{data.totalSatellites === 1 ? '' : 's'}
          </span>
        )}
      </div>

      <div className="flex-1 overflow-auto px-4 py-3">
        {isLoading && <p className="text-xs text-dark-100">Loading…</p>}
        {error != null && <p className="text-xs text-red-400">Failed to load satellite statistics</p>}

        {data && data.totalQsos === 0 && data.uniqueGrids === 0 && (
          <p className="text-xs text-dark-200">
            No satellite QSOs found. A QSO counts here when it carries ADIF
            PROP_MODE=SAT or names a satellite in SAT_NAME.
          </p>
        )}

        {data && (data.totalQsos > 0 || data.uniqueGrids > 0) && (
          <>
            <div className="grid gap-2 mb-3 [grid-template-columns:repeat(auto-fit,minmax(110px,1fr))]">
              <Stat label="Grids" value={data.uniqueGrids} hint="Distinct 4-character grid squares worked via satellite" />
              <Stat label="Confirmed" value={data.confirmedGrids} hint="Grids with at least one confirmed satellite QSO" />
              <Stat label="States" value={data.uniqueStates} hint="US states worked via satellite" />
              <Stat label="DXCC" value={data.uniqueEntities} hint="DXCC entities worked via satellite" />
            </div>

            <div className="mb-4">
              <div className="flex items-baseline justify-between text-xs mb-1">
                <span className="text-dark-100">VUCC Satellite</span>
                <span className="tabular-nums">
                  <span className="text-accent-warning font-semibold">{data.uniqueGrids}</span>
                  {' / '}{data.vuccThreshold} grids ({vuccPct}%)
                </span>
              </div>
              <ProgressBar worked={data.uniqueGrids} confirmed={data.confirmedGrids} target={data.vuccThreshold} />
            </div>

            <table className="w-full text-xs">
              <thead>
                <tr className="text-dark-200 border-b border-glass-100">
                  <th className="text-left py-1">Satellite</th>
                  <th className="text-right px-2 py-1">QSOs</th>
                  <th className="text-right px-2 py-1">Confirmed</th>
                  <th className="text-right px-2 py-1">Grids</th>
                  <th className="text-right px-2 py-1">First</th>
                  <th className="text-right px-2 py-1">Last</th>
                </tr>
              </thead>
              <tbody>
                {data.satellites.map(s => (
                  <tr key={s.satellite} className="border-b border-glass-100/50">
                    <td className="py-1 font-medium">{s.satellite}</td>
                    <td className="text-right px-2 py-1 tabular-nums">{s.qsoCount}</td>
                    <td className={`text-right px-2 py-1 tabular-nums ${s.confirmedQsos > 0 ? 'text-accent-success' : 'text-dark-200'}`}>
                      {s.confirmedQsos}
                    </td>
                    <td className="text-right px-2 py-1 tabular-nums">{s.uniqueGrids}</td>
                    <td className="text-right px-2 py-1 tabular-nums text-dark-100">{day(s.firstWorked)}</td>
                    <td className="text-right px-2 py-1 tabular-nums text-dark-100">{day(s.lastWorked)}</td>
                  </tr>
                ))}
              </tbody>
            </table>

            <p className="text-[10px] text-dark-200 mt-3">
              Grids, states and DXCC count satellite QSOs only. A satellite QSO
              logged without a SAT_NAME still counts toward those totals but
              appears in no row above.
            </p>
          </>
        )}
      </div>
    </div>
  );
}

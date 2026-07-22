import { Fragment, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, AwardFilters } from '../api/client';
import { BAND_RANGES } from '../utils/spotBands';

// Derived from the shared band table rather than a local copy, so a band can
// never be missing from the filter while QSOs on it sit in the log. The old
// hard-coded list omitted 30m, 60m, 2m and 70cm — 30m alone hides ~740 QSOs.
const BANDS = Object.keys(BAND_RANGES);

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

function StateDetail({ state, filters }: { state: string; filters: AwardFilters }) {
  const { data, isLoading, error } = useQuery({
    // Nested under 'statistics' so the app-wide invalidation on QSO
    // create/edit/delete/import (which targets ['statistics']) refreshes this
    // too. A standalone key silently missed those and the tab went stale.
    queryKey: ['statistics', 'county-details', state, filters],
    queryFn: () => api.getCountyDetails(state, filters),
    staleTime: 60_000,
  });

  if (isLoading) return <p className="text-xs text-dark-100 px-2 py-1">Loading counties…</p>;
  if (error != null) return <p className="text-xs text-red-400 px-2 py-1">Failed to load counties for {state}</p>;
  if (!data?.length) return <p className="text-xs text-dark-200 px-2 py-1">No counties on file for {state}</p>;

  return (
    <div className="px-2 py-2 grid gap-x-4 gap-y-0.5 [grid-template-columns:repeat(auto-fill,minmax(190px,1fr))]">
      {data.map(c => (
        <div
          key={c.county}
          className={`flex items-baseline justify-between gap-2 ${c.qsoCount > 0 ? '' : 'opacity-70'}`}
          title={c.qsoCount > 0
            ? `${c.qsoCount} QSO${c.qsoCount === 1 ? '' : 's'}${c.firstWorked ? ` · first ${c.firstWorked.slice(0, 10)}` : ''}${c.confirmed ? ' · confirmed' : ' · not confirmed'}`
            : 'Not worked'}
        >
          <span className={
            c.confirmed ? 'text-accent-success' : c.qsoCount > 0 ? 'text-accent-warning' : 'text-dark-200'
          }>
            {c.confirmed ? '●' : c.qsoCount > 0 ? '○' : '·'} {c.county}
          </span>
          {c.qsoCount > 0 && <span className="text-dark-100 font-mono text-[10px]">{c.qsoCount}</span>}
        </div>
      ))}
    </div>
  );
}

export function CountiesStatisticsTab() {
  const [filters, setFilters] = useState<AwardFilters>({});
  const [openState, setOpenState] = useState<string | null>(null);

  const { data, isLoading, error } = useQuery({
    // See county-details note: nested under 'statistics' so QSO mutations
    // invalidate this tab.
    queryKey: ['statistics', 'counties', filters],
    queryFn: () => api.getCountiesStatistics(filters),
    staleTime: 60_000,
  });

  const workedPct = data && data.totalTarget > 0
    ? ((data.totalWorked / data.totalTarget) * 100).toFixed(1) : '0.0';

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
            <span className="text-accent-warning font-semibold">{data.totalWorked}</span> worked
            {' · '}
            <span className="text-accent-success font-semibold">{data.totalConfirmed}</span> confirmed
            {' / '}{data.totalTarget} counties ({workedPct}%)
          </span>
        )}
      </div>

      <div className="flex-1 overflow-auto px-4 py-2">
        {isLoading && <p className="text-xs text-dark-100">Loading…</p>}
        {error != null && <p className="text-xs text-red-400">Failed to load counties statistics</p>}
        {data && (
          <>
            <table className="w-full text-xs">
              <thead className="sticky top-0 bg-dark-800">
                <tr className="text-dark-100 font-semibold">
                  <th className="text-left px-1 py-1">State</th>
                  <th className="text-right px-1 py-1">Worked</th>
                  <th className="text-right px-1 py-1">Confirmed</th>
                  <th className="text-right px-1 py-1">Target</th>
                  <th className="text-right px-1 py-1">Left</th>
                  <th className="text-left px-2 py-1 w-1/3">Progress</th>
                  <th className="text-right px-1 py-1">QSOs</th>
                </tr>
              </thead>
              <tbody>
                {data.states.map(s => {
                  const isOpen = openState === s.state;
                  return (
                    // The Fragment is the list child, so the key belongs here —
                    // on the <tr>s it satisfies nothing and React warns.
                    <Fragment key={s.state}>
                      <tr
                        onClick={() => setOpenState(isOpen ? null : s.state)}
                        className={`border-t border-glass-100 cursor-pointer hover:bg-glass-100 ${s.worked ? '' : 'opacity-70'}`}
                      >
                        <td className={`px-1 py-1 font-mono ${s.worked ? 'text-accent-secondary font-semibold' : 'text-dark-200'}`}>
                          {isOpen ? '▾' : '▸'} {s.state}
                        </td>
                        <td className="text-right px-1 py-1 font-mono text-accent-warning">{s.worked || ''}</td>
                        <td className="text-right px-1 py-1 font-mono text-accent-success">{s.confirmed || ''}</td>
                        <td className="text-right px-1 py-1 font-mono text-dark-100">{s.target}</td>
                        <td className="text-right px-1 py-1 font-mono text-dark-200">{s.target - s.worked}</td>
                        <td className="px-2 py-1">
                          <ProgressBar worked={s.worked} confirmed={s.confirmed} target={s.target} />
                        </td>
                        <td className="text-right px-1 py-1 text-dark-100 font-mono">{s.qsoCount || ''}</td>
                      </tr>
                      {isOpen && (
                        <tr className="border-t border-glass-100">
                          <td colSpan={7} className="bg-glass-100/40">
                            <StateDetail state={s.state} filters={filters} />
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
            <p className="text-[10px] text-dark-200 mt-3">
              Counties from the US Census list (~{data.totalTarget}). MARAC's official USA-CA list is
              ~3,077 and differs slightly over Virginia's independent cities and Alaska's boroughs —
              treat this as progress tracking, not an award submission. Confirmed counts QSL cards and
              eQSL only; LoTW does not carry county reliably.
            </p>
          </>
        )}
      </div>
    </div>
  );
}

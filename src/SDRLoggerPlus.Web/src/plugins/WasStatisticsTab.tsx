import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, AwardFilters } from '../api/client';
import { ALL_BANDS as BANDS } from '../utils/spotBands';

const ALL_STATES = [
  'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
  'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
  'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
  'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
  'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY',
];

export function WasStatisticsTab() {
  const [filters, setFilters] = useState<AwardFilters>({});

  const { data, isLoading, error } = useQuery({
    queryKey: ['was-statistics', filters],
    queryFn: () => api.getWasStatistics(filters),
    staleTime: 60_000,
  });

  const stateMap = new Map(data?.states.map(s => [s.state, s]) ?? []);
  const bandsInData = new Set(data?.states.flatMap(s => Object.keys(s.bands)) ?? []);
  const visibleBands = BANDS.filter(b => bandsInData.has(b));

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
            <span className="text-accent-secondary font-semibold">{data.totalWorked}</span> / {data.totalNeeded} states
          </span>
        )}
      </div>

      <div className="flex-1 overflow-auto px-4 py-2">
        {isLoading && <p className="text-xs text-dark-100">Loading…</p>}
        {error != null && <p className="text-xs text-red-400">Failed to load WAS statistics</p>}
        {data && (
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-dark-800">
              <tr className="text-dark-100 font-semibold">
                <th className="text-left px-1 py-1">State</th>
                {visibleBands.map(b => <th key={b} className="text-center px-1 py-1">{b}</th>)}
                <th className="text-right px-1 py-1">QSOs</th>
              </tr>
            </thead>
            <tbody>
              {ALL_STATES.map(st => {
                const worked = stateMap.get(st);
                return (
                  // Un-worked rows: keep them noticeably dimmer than worked
                  // rows (opacity-60 + dark-200) but readable — the old
                  // opacity-40 + dark-400/500 combo was nearly invisible on
                  // the glass background.
                  <tr key={st} className={`border-t border-glass-100 ${worked ? '' : 'opacity-70'}`}>
                    <td className={`px-1 py-1 font-mono ${worked ? 'text-accent-secondary font-semibold' : 'text-dark-200'}`}>{st}</td>
                    {visibleBands.map(b => (
                      <td key={b} className="text-center px-1 py-1">
                        {worked?.bands[b] ? (
                          <span
                            title={worked.bands[b].join(', ')}
                            className="inline-block w-3 h-3 rounded-sm bg-accent-success"
                          />
                        ) : (
                          <span className="text-dark-300">-</span>
                        )}
                      </td>
                    ))}
                    <td className="text-right px-1 py-1 text-dark-100 font-mono">{worked?.qsoCount ?? ''}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

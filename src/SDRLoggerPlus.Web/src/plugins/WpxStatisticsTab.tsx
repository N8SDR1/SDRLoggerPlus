import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, AwardFilters } from '../api/client';

const BANDS = ['160m', '80m', '40m', '20m', '17m', '15m', '12m', '10m', '6m'];

export function WpxStatisticsTab() {
  const [filters, setFilters] = useState<AwardFilters>({});
  const [sortBy, setSortBy] = useState<'prefix' | 'bands'>('prefix');

  const { data, isLoading, error } = useQuery({
    queryKey: ['wpx-statistics', filters],
    queryFn: () => api.getWpxStatistics(filters),
    staleTime: 60_000,
  });

  const sorted = useMemo(() => {
    if (!data) return [];
    return [...data.prefixes].sort((a, b) =>
      sortBy === 'bands'
        ? b.bandCount - a.bandCount || a.prefix.localeCompare(b.prefix)
        : a.prefix.localeCompare(b.prefix));
  }, [data, sortBy]);

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
        <select
          value={sortBy}
          onChange={e => setSortBy(e.target.value as 'prefix' | 'bands')}
          className="glass-input text-xs px-2 py-1"
        >
          <option value="prefix">Sort: Prefix</option>
          <option value="bands">Sort: Bands</option>
        </select>
        {data && (
          <span className="text-xs text-dark-300 ml-auto">
            <span className="text-accent-secondary font-semibold">{data.totalWorked}</span> prefixes worked
          </span>
        )}
      </div>

      <div className="flex-1 overflow-auto px-4 py-2">
        {isLoading && <p className="text-xs text-dark-300">Loading…</p>}
        {error != null && <p className="text-xs text-red-400">Failed to load WPX statistics</p>}
        {data && (
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-dark-800">
              <tr className="text-dark-300">
                <th className="text-left px-1 py-1">Prefix</th>
                <th className="text-left px-1 py-1">Bands</th>
                <th className="text-left px-1 py-1">Sample Calls</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map(p => (
                <tr key={p.prefix} className="border-t border-glass-100">
                  <td className="px-1 py-1 font-mono text-accent-secondary">{p.prefix}</td>
                  <td className="px-1 py-1 text-dark-300">
                    {Object.keys(p.bands).join(', ')}
                  </td>
                  <td className="px-1 py-1 text-dark-300 font-mono">{p.calls.join(', ')}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

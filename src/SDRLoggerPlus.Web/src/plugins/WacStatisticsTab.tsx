import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, AwardFilters, WacContinentStatus } from '../api/client';
import { ALL_BANDS as BANDS } from '../utils/spotBands';

const BASE_ORDER = ['NA', 'SA', 'EU', 'AS', 'AF', 'OC'];

function ContinentCard({ status, code, name }: { status?: WacContinentStatus; code: string; name: string }) {
  const worked = !!status;
  return (
    <div className={`p-3 rounded-lg border ${
      worked ? 'bg-accent-success/10 border-accent-success/40' : 'bg-dark-700 border-glass-100 opacity-60'
    }`}>
      <div className="flex items-center justify-between">
        <span className={`text-sm font-semibold ${worked ? 'text-accent-success' : 'text-dark-300'}`}>{name}</span>
        <span className="text-xs font-mono text-dark-400">{code}</span>
      </div>
      {status && (
        <div className="mt-1.5 text-xs text-dark-300 space-y-0.5">
          <p>Bands: {Object.keys(status.bands).join(', ')}</p>
          <p className="truncate" title={status.entities.join(', ')}>e.g. {status.entities.slice(0, 3).join(', ')}</p>
        </div>
      )}
      {!status && <p className="mt-1.5 text-xs text-dark-400">Not worked</p>}
    </div>
  );
}

export function WacStatisticsTab() {
  const [filters, setFilters] = useState<AwardFilters>({});

  const { data, isLoading, error } = useQuery({
    queryKey: ['wac-statistics', filters],
    queryFn: () => api.getWacStatistics(filters),
    staleTime: 60_000,
  });

  const byCode = new Map(data?.continents.map(c => [c.code, c]) ?? []);
  const names: Record<string, string> = {
    NA: 'North America', SA: 'South America', EU: 'Europe',
    AS: 'Asia', AF: 'Africa', OC: 'Oceania', AN: 'Antarctica',
  };

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
          <span className="text-xs text-dark-300 ml-auto">
            <span className="text-accent-secondary font-semibold">{data.baseWorked}</span> / 6 continents
          </span>
        )}
      </div>

      <div className="flex-1 overflow-auto px-4 py-3 space-y-4">
        {isLoading && <p className="text-xs text-dark-300">Loading…</p>}
        {error != null && <p className="text-xs text-red-400">Failed to load WAC statistics</p>}
        {data && (
          <>
            {data.achieved && (
              <div className="p-2 rounded-lg bg-accent-success/20 border border-accent-success/50 text-center text-sm text-accent-success font-semibold">
                🏆 Worked All Continents achieved!
              </div>
            )}
            <div className="grid grid-cols-2 lg:grid-cols-3 gap-3">
              {BASE_ORDER.map(code => (
                <ContinentCard key={code} code={code} name={names[code]} status={byCode.get(code)} />
              ))}
            </div>
            <div>
              <p className="text-xs text-dark-400 mb-2">Endorsement</p>
              <div className="grid grid-cols-2 lg:grid-cols-3 gap-3">
                <ContinentCard code="AN" name={names.AN} status={byCode.get('AN')} />
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

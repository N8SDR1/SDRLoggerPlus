import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, AwardFilters } from '../api/client';
import { ALL_BANDS as BANDS } from '../utils/spotBands';

export function WazStatisticsTab() {
  const [filters, setFilters] = useState<AwardFilters>({});
  const [selectedZone, setSelectedZone] = useState<number | null>(null);

  const { data, isLoading, error } = useQuery({
    queryKey: ['waz-statistics', filters],
    queryFn: () => api.getWazStatistics(filters),
    staleTime: 60_000,
  });

  const zoneMap = new Map(data?.zones.map(z => [z.zone, z]) ?? []);
  const selected = selectedZone != null ? zoneMap.get(selectedZone) : undefined;

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
            <span className="text-accent-secondary font-semibold">{data.totalWorked}</span> / {data.totalNeeded} zones
          </span>
        )}
      </div>

      <div className="flex-1 overflow-auto px-4 py-3 space-y-4">
        {isLoading && <p className="text-xs text-dark-300">Loading…</p>}
        {error != null && <p className="text-xs text-red-400">Failed to load WAZ statistics</p>}
        {data && (
          <>
            <div className="grid grid-cols-10 gap-1.5">
              {Array.from({ length: 40 }, (_, i) => i + 1).map(zone => {
                const worked = zoneMap.has(zone);
                return (
                  <button
                    key={zone}
                    onClick={() => setSelectedZone(worked ? zone : null)}
                    title={worked ? `Zone ${zone} — click for detail` : `Zone ${zone} — not worked`}
                    className={`h-8 rounded text-xs font-mono transition-colors ${
                      worked
                        ? 'bg-accent-success/30 text-accent-success border border-accent-success/50 hover:bg-accent-success/50'
                        : 'bg-dark-700 text-dark-400 border border-glass-100'
                    } ${selectedZone === zone ? 'ring-1 ring-accent-secondary' : ''}`}
                  >
                    {zone}
                  </button>
                );
              })}
            </div>

            {selected && (
              <div className="p-3 bg-dark-700 rounded-lg border border-glass-100 text-xs space-y-1">
                <p className="text-dark-200 font-semibold">Zone {selected.zone}</p>
                <p className="text-dark-300">
                  Bands: {Object.entries(selected.bands).map(([b, modes]) => `${b} (${modes.join(', ')})`).join(' · ')}
                </p>
                <p className="text-dark-300">Entities: {selected.entities.join(', ')}</p>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, FiveBandStatistics } from '../api/client';

/**
 * Shared renderer for the two five-band awards (5BWAS: 50 states per band,
 * 5BDXCC: 100 entities per band) on 80/40/20/15/10.
 */
function FiveBandView({ title, itemNoun, queryKey, fetcher }: {
  title: string;
  itemNoun: string;
  queryKey: string;
  fetcher: (mode?: string) => Promise<FiveBandStatistics>;
}) {
  const [mode, setMode] = useState('');
  const [expanded, setExpanded] = useState<string | null>(null);

  const { data, isLoading, error } = useQuery({
    queryKey: [queryKey, mode],
    queryFn: () => fetcher(mode || undefined),
    staleTime: 60_000,
  });

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="flex-shrink-0 px-4 py-2 border-b border-glass-100 flex flex-wrap gap-2 items-center">
        <input
          type="text"
          value={mode}
          onChange={e => setMode(e.target.value)}
          className="glass-input text-xs px-2 py-1 w-24"
          placeholder="Mode"
        />
        {data && (
          <span className="text-xs text-dark-300 ml-auto">
            {data.unionCount} unique {itemNoun} across all bands
          </span>
        )}
      </div>

      <div className="flex-1 overflow-auto px-4 py-3 space-y-3">
        {isLoading && <p className="text-xs text-dark-300">Loading…</p>}
        {error != null && <p className="text-xs text-red-400">Failed to load {title} statistics</p>}
        {data && (
          <>
            {data.achieved && (
              <div className="p-2 rounded-lg bg-accent-warning/20 border border-accent-warning/50 text-center text-sm text-accent-warning font-semibold">
                🏆 {title} achieved — all five bands complete!
              </div>
            )}
            {data.bands.map(b => {
              const pct = Math.min(100, Math.round((b.count / b.threshold) * 100));
              return (
                <div key={b.band}>
                  <button
                    onClick={() => setExpanded(expanded === b.band ? null : b.band)}
                    className="w-full text-left"
                    title={`Click to ${expanded === b.band ? 'collapse' : 'expand'} worked ${itemNoun}`}
                  >
                    <div className="flex items-center gap-2 text-xs">
                      <span className={`w-10 shrink-0 font-mono ${b.achieved ? 'text-accent-success font-semibold' : 'text-dark-300'}`}>
                        {b.band}
                      </span>
                      <div className="flex-1 bg-dark-700 rounded-full h-2">
                        <div
                          className={`h-2 rounded-full transition-all ${b.achieved ? 'bg-accent-success' : 'bg-accent-secondary'}`}
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                      <span className="text-gray-300 w-20 text-right">{b.count} / {b.threshold}</span>
                    </div>
                  </button>
                  {expanded === b.band && b.items.length > 0 && (
                    <p className="mt-1 ml-12 text-xs text-dark-300 font-mono break-words">
                      {b.items.join(', ')}
                    </p>
                  )}
                </div>
              );
            })}
          </>
        )}
      </div>
    </div>
  );
}

export function FiveBandWasTab() {
  return (
    <FiveBandView
      title="5-Band WAS"
      itemNoun="states"
      queryKey="5bwas-statistics"
      fetcher={(mode) => api.get5BWasStatistics(mode)}
    />
  );
}

export function FiveBandDxccTab() {
  return (
    <FiveBandView
      title="5-Band DXCC"
      itemNoun="entities"
      queryKey="5bdxcc-statistics"
      fetcher={(mode) => api.get5BDxccStatistics(mode)}
    />
  );
}

import { useEffect, useMemo } from 'react';
import { Grid3x3 } from 'lucide-react';
import { api } from '../api/client';
import { useAppStore } from '../store/appStore';
import { GlassPanel } from '../components/GlassPanel';
import { STATE_PROV_UNIVERSE } from '../contest/locations';

const BAND_ORDER = ['160m', '80m', '40m', '30m', '20m', '17m', '15m', '10m', '6m', '2m'];

// Human labels for the engine's MultSource enum names.
const SOURCE_LABELS: Record<string, string> = {
  CqZone: 'CQ Zones',
  ItuZone: 'ITU Zones',
  Dxcc: 'Countries',
  State: 'States / Provinces',
  Section: 'Sections',
  WpxPrefix: 'Prefixes',
  Grid: 'Grids',
  Continent: 'Continents',
};

// A worked mult value is stored as "value", optionally suffixed with "@BAND"
// (per-band rules) and/or "+MODE" (per-mode rules): e.g. "OH@20m", "14+CW",
// "OH@20m+CW". Parse all three parts so a per-mode contest doesn't render a
// literal "+CW" as part of the value.
export function parseEntry(entry: string): { value: string; band: string | null; mode: string | null } {
  let rest = entry;
  let mode: string | null = null;
  const plus = rest.indexOf('+');
  if (plus >= 0) {
    mode = rest.slice(plus + 1).toUpperCase();
    rest = rest.slice(0, plus);
  }
  let band: string | null = null;
  const at = rest.indexOf('@');
  if (at >= 0) {
    band = rest.slice(at + 1).toLowerCase();
    rest = rest.slice(0, at);
  }
  return { value: rest, band, mode };
}

// A worked-entry column is a band, a mode, or a band+mode pair (or "—" when the
// rule is neither per-band nor per-mode).
function columnKey(band: string | null, mode: string | null): string {
  if (band && mode) return `${band}·${mode}`;
  return band ?? mode ?? '—';
}

// The fixed "universe" of possible values for a source, so unworked entries read
// as gaps rather than being invisible. Only sources with a knowable, finite,
// contest-independent universe qualify. (Section is intentionally omitted until an
// authoritative ARRL/RAC section table is wired in from the backend.)
function universeFor(source: string): string[] | null {
  if (source === 'CqZone') return Array.from({ length: 40 }, (_, i) => String(i + 1));
  if (source === 'State') return STATE_PROV_UNIVERSE;
  return null;
}

// Natural sort so zone "9" precedes "10" and prefixes stay alphabetical.
function naturalCompare(a: string, b: string): number {
  const na = Number(a);
  const nb = Number(b);
  if (!Number.isNaN(na) && !Number.isNaN(nb)) return na - nb;
  return a.localeCompare(b);
}

export function MultNeededPlugin() {
  const contestState = useAppStore((s) => s.contestState);
  const setContestState = useAppStore((s) => s.setContestState);

  useEffect(() => {
    if (!contestState) api.getContestState().then((s) => setContestState(s)).catch(() => {});
  }, [contestState, setContestState]);

  if (!contestState) {
    return (
      <GlassPanel title="Multipliers" icon={<Grid3x3 className="w-5 h-5" />}>
        <div className="flex items-center justify-center h-full text-sm text-gray-500 p-6 text-center">
          Start a contest session to track multipliers.
        </div>
      </GlassPanel>
    );
  }

  return (
    <GlassPanel
      title="Multipliers"
      icon={<Grid3x3 className="w-5 h-5" />}
      actions={<span className="text-xs text-gray-400">{contestState.multipliers} total</span>}
    >
      <div className="flex flex-col h-full overflow-y-auto p-3 gap-4">
        {Object.keys(contestState.multsBySource).length === 0 ? (
          <div className="text-center text-sm text-gray-500 py-6">No multipliers worked yet.</div>
        ) : (
          Object.entries(contestState.multsBySource).map(([source, entries]) => (
            <MultSourceMatrix key={source} source={source} entries={entries} />
          ))
        )}
      </div>
    </GlassPanel>
  );
}

function MultSourceMatrix({ source, entries }: { source: string; entries: string[] }) {
  const { rows, cols, worked, gridded, workedCount } = useMemo(() => {
    const parsed = entries.map(parseEntry);
    // A grid (rows × columns) makes sense only when the rule splits by band/mode.
    const gridded = parsed.some((p) => p.band !== null || p.mode !== null);
    const colSet = new Set<string>();
    const workedSet = new Set<string>(); // "value|col"
    const valueSet = new Set<string>();
    for (const p of parsed) {
      valueSet.add(p.value);
      const col = columnKey(p.band, p.mode);
      colSet.add(col);
      workedSet.add(`${p.value}|${col}`);
    }
    // Sort columns by band order (band-only columns) then lexically for the rest.
    const cols = [...colSet].sort((a, b) => {
      const ia = BAND_ORDER.indexOf(a);
      const ib = BAND_ORDER.indexOf(b);
      if (ia !== -1 || ib !== -1) return (ia + 100) - (ib + 100);
      return a.localeCompare(b);
    });
    const universe = universeFor(source);
    const rows = universe ?? [...valueSet].sort(naturalCompare);
    return { rows, cols, worked: workedSet, gridded, workedCount: valueSet.size };
  }, [source, entries]);

  const label = SOURCE_LABELS[source] ?? source;
  const universe = universeFor(source);
  // "worked / total" when the universe is known, else just the worked count.
  const countLabel = universe ? `${workedCount} / ${universe.length}` : `${workedCount} worked`;

  return (
    <div>
      <div className="flex items-center justify-between mb-1.5">
        <h4 className="text-xs font-semibold text-gray-300 uppercase tracking-wider">{label}</h4>
        <span className="text-xs text-gray-500">{countLabel}</span>
      </div>

      {gridded ? (
        <div className="overflow-x-auto">
          <table className="text-xs border-collapse">
            <thead>
              <tr>
                <th className="px-1.5 py-0.5 text-left text-gray-500 font-medium sticky left-0 bg-dark-800/80"></th>
                {cols.map((c) => (
                  <th key={c} className="px-1.5 py-0.5 text-gray-500 font-medium">{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((value) => (
                <tr key={value}>
                  <td className="px-1.5 py-0.5 font-mono text-gray-300 sticky left-0 bg-dark-800/80">{value}</td>
                  {cols.map((c) => {
                    const hit = worked.has(`${value}|${c}`);
                    return (
                      <td key={c} className="px-1.5 py-0.5 text-center">
                        <span
                          className={`inline-block w-3 h-3 rounded-sm ${
                            hit ? 'bg-emerald-500' : 'bg-dark-600'
                          }`}
                          title={hit ? `${value} worked on ${c}` : `${value} needed on ${c}`}
                        />
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : universe ? (
        // Known universe, single dimension: show every possible value so unworked
        // ones read as "needed" (dim) — the way CQ zones show all of 1–40.
        <div className="flex flex-wrap gap-1">
          {rows.map((value) => {
            const hit = worked.has(`${value}|—`);
            return (
              <span
                key={value}
                className={`px-1.5 py-0.5 rounded text-xs font-mono border ${
                  hit
                    ? 'bg-emerald-500/15 border-emerald-500/30 text-emerald-300'
                    : 'bg-dark-700/40 border-glass-100 text-gray-600'
                }`}
                title={hit ? `${value} worked` : `${value} needed`}
              >
                {value}
              </span>
            );
          })}
        </div>
      ) : (
        // Unbounded source (countries, prefixes, grids): just the worked chips.
        <div className="flex flex-wrap gap-1">
          {rows.map((value) => (
            <span
              key={value}
              className="px-1.5 py-0.5 rounded bg-emerald-500/15 border border-emerald-500/30 text-emerald-300 text-xs font-mono"
            >
              {value}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

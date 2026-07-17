import { useEffect, useMemo } from 'react';
import { Grid3x3 } from 'lucide-react';
import { api } from '../api/client';
import { useAppStore } from '../store/appStore';
import { GlassPanel } from '../components/GlassPanel';

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

// A worked mult value is stored as "value" or "value@BAND" (per-band rules).
function parseEntry(entry: string): { value: string; band: string | null } {
  const at = entry.indexOf('@');
  if (at < 0) return { value: entry, band: null };
  return { value: entry.slice(0, at), band: entry.slice(at + 1).toLowerCase() };
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
  const { rows, bands, worked, perBand } = useMemo(() => {
    const parsed = entries.map(parseEntry);
    const perBand = parsed.some((p) => p.band !== null);
    const bandSet = new Set<string>();
    const workedSet = new Set<string>(); // "value|band"
    const valueSet = new Set<string>();
    for (const p of parsed) {
      valueSet.add(p.value);
      const band = p.band ?? '—';
      bandSet.add(band);
      workedSet.add(`${p.value}|${band}`);
    }
    const bands = [...bandSet].sort(
      (a, b) => (BAND_ORDER.indexOf(a) + 100) - (BAND_ORDER.indexOf(b) + 100)
    );
    let rows = [...valueSet].sort(naturalCompare);
    // CQ zones have a fixed 1–40 universe — show them all so gaps read as "needed".
    if (source === 'CqZone') {
      rows = Array.from({ length: 40 }, (_, i) => String(i + 1));
    }
    return { rows, bands, worked: workedSet, perBand };
  }, [source, entries]);

  const label = SOURCE_LABELS[source] ?? source;
  const distinct = new Set(entries.map((e) => parseEntry(e).value)).size;

  return (
    <div>
      <div className="flex items-center justify-between mb-1.5">
        <h4 className="text-xs font-semibold text-gray-300 uppercase tracking-wider">{label}</h4>
        <span className="text-xs text-gray-500">{distinct} worked</span>
      </div>

      {perBand ? (
        <div className="overflow-x-auto">
          <table className="text-xs border-collapse">
            <thead>
              <tr>
                <th className="px-1.5 py-0.5 text-left text-gray-500 font-medium sticky left-0 bg-dark-800/80"></th>
                {bands.map((b) => (
                  <th key={b} className="px-1.5 py-0.5 text-gray-500 font-medium">{b}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((value) => (
                <tr key={value}>
                  <td className="px-1.5 py-0.5 font-mono text-gray-300 sticky left-0 bg-dark-800/80">{value}</td>
                  {bands.map((b) => {
                    const hit = worked.has(`${value}|${b}`);
                    return (
                      <td key={b} className="px-1.5 py-0.5 text-center">
                        <span
                          className={`inline-block w-3 h-3 rounded-sm ${
                            hit ? 'bg-emerald-500' : 'bg-dark-600'
                          }`}
                          title={hit ? `${value} worked on ${b}` : `${value} needed on ${b}`}
                        />
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        // Non-per-band sources: just worked chips.
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

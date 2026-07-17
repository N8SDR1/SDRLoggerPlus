import { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Swords, Play, Square, Search, AlertTriangle, Sparkles, Download } from 'lucide-react';
import {
  api,
  ContestDefinition,
  ContestMyExchange,
  ContestCheckResponse,
} from '../api/client';
import { useAppStore } from '../store/appStore';
import { GlassPanel } from '../components/GlassPanel';

const BANDS = ['160m', '80m', '40m', '20m', '15m', '10m', '6m', '2m'];
const MODES = ['CW', 'SSB', 'FT8', 'FT4', 'RTTY'];

// Server enums serialize PascalCase ("Rst") — compare case-insensitively.
const isType = (fieldType: string, t: string) => fieldType.toLowerCase() === t;

// Band from rig frequency (Hz) — same table LogEntryPlugin uses.
const bandFromHz = (hz: number): string | null => {
  const k = hz / 1000;
  if (k >= 1800 && k <= 2000) return '160m';
  if (k >= 3500 && k <= 4000) return '80m';
  if (k >= 7000 && k <= 7300) return '40m';
  if (k >= 14000 && k <= 14350) return '20m';
  if (k >= 21000 && k <= 21450) return '15m';
  if (k >= 28000 && k <= 29700) return '10m';
  if (k >= 50000 && k <= 54000) return '6m';
  if (k >= 144000 && k <= 148000) return '2m';
  return null;
};

// Collapse rig modes to contest modes (USB/LSB → SSB, CWU/CWL → CW).
const contestModeFromRig = (mode: string): string => {
  const m = (mode || '').toUpperCase();
  if (m === 'USB' || m === 'LSB' || m === 'SSB') return 'SSB';
  if (m.startsWith('CW')) return 'CW';
  if (m === 'FT8' || m === 'FT4' || m === 'RTTY') return m;
  return 'CW';
};

export function ContestEntryPlugin() {
  const contestState = useAppStore((s) => s.contestState);
  const setContestState = useAppStore((s) => s.setContestState);

  // Seed state from the API on mount (covers reload while a session is live).
  useEffect(() => {
    api.getContestState().then((s) => setContestState(s)).catch(() => {});
  }, [setContestState]);

  return (
    <GlassPanel title="Contest" icon={<Swords className="w-5 h-5" />}>
      {contestState ? <EntryView /> : <SetupView />}
    </GlassPanel>
  );
}

// ---------------------------------------------------------------------------
// Setup: search + pick a contest definition, my exchange, start session
// ---------------------------------------------------------------------------

function SetupView() {
  const setContestState = useAppStore((s) => s.setContestState);
  const [search, setSearch] = useState('');
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [label, setLabel] = useState('');
  const [myEx, setMyEx] = useState<ContestMyExchange>({});
  const [error, setError] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);

  const { data: definitions } = useQuery({
    queryKey: ['contest-definitions'],
    queryFn: () => api.getContestDefinitions(),
  });

  const filtered = useMemo(() => {
    if (!definitions) return [];
    const q = search.trim().toLowerCase();
    if (!q) return definitions;
    return definitions.filter(
      (d) =>
        d.name.toLowerCase().includes(q) ||
        d.cabrilloName.toLowerCase().includes(q) ||
        d.modes.some((m) => m.toLowerCase().includes(q))
    );
  }, [definitions, search]);

  const selected = definitions?.find((d) => d.id === selectedId) ?? null;

  const start = async () => {
    if (!selected) return;
    setStarting(true);
    setError(null);
    try {
      await api.startContestSession({
        definitionId: selected.id,
        myExchange: myEx,
        label: label || undefined,
      });
      const state = await api.getContestState();
      setContestState(state);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to start session');
    } finally {
      setStarting(false);
    }
  };

  return (
    <div className="flex flex-col h-full p-4 gap-3 overflow-y-auto">
      {/* Search */}
      <div className="relative">
        <Search className="w-4 h-4 absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-500" />
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search contests…"
          className="glass-input w-full text-sm pl-8 pr-2 py-1.5"
        />
      </div>

      {/* Definition list */}
      <div className="flex-1 min-h-[8rem] overflow-y-auto space-y-1">
        {filtered.map((d) => (
          <button
            key={d.id}
            onClick={() => setSelectedId(d.id)}
            className={`w-full text-left px-3 py-2 rounded-lg border text-sm transition-colors ${
              selectedId === d.id
                ? 'bg-accent-primary/20 border-accent-primary/50 text-gray-100'
                : 'bg-dark-700/50 border-glass-100 text-gray-300 hover:bg-dark-600/50'
            }`}
          >
            <div className="flex items-center justify-between">
              <span className="font-medium truncate">{d.name}</span>
              <span className="text-xs text-gray-500 ml-2 shrink-0">
                {d.builtin ? d.modes.join('/') : 'custom'}
              </span>
            </div>
            <div className="text-xs text-gray-500 mt-0.5">
              Exchange: {d.rcvdExchange.map((f) => f.label).join(' + ') || '—'}
            </div>
          </button>
        ))}
        {filtered.length === 0 && (
          <div className="text-center text-sm text-gray-500 py-6">No contests match</div>
        )}
      </div>

      {/* Session setup for the selected contest */}
      {selected && (
        <div className="space-y-2 border-t border-glass-100 pt-3">
          <input
            type="text"
            value={label}
            onChange={(e) => setLabel(e.target.value)}
            placeholder={`${selected.name} ${new Date().getUTCFullYear()}`}
            className="glass-input w-full text-sm px-2 py-1.5"
          />
          {/* My-exchange fields relevant to the sent exchange */}
          <div className="grid grid-cols-2 gap-2">
            {selected.sentExchange.some((f) => f.type === 'zone') && (
              <input type="text" placeholder="My CQ zone" className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => setMyEx((p) => ({ ...p, cqZone: parseInt(e.target.value) || undefined }))} />
            )}
            {selected.sentExchange.some((f) => f.type === 'state') && (
              <input type="text" placeholder="My state" className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => setMyEx((p) => ({ ...p, state: e.target.value.toUpperCase() || undefined }))} />
            )}
            {selected.sentExchange.some((f) => f.type === 'section') && (
              <input type="text" placeholder="My section" className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => setMyEx((p) => ({ ...p, section: e.target.value.toUpperCase() || undefined }))} />
            )}
            {selected.sentExchange.some((f) => f.type === 'name') && (
              <input type="text" placeholder="My name" className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => setMyEx((p) => ({ ...p, name: e.target.value || undefined }))} />
            )}
            {selected.sentExchange.some((f) => f.type === 'grid') && (
              <input type="text" placeholder="My grid" className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => setMyEx((p) => ({ ...p, grid: e.target.value.toUpperCase() || undefined }))} />
            )}
            <input type="text" placeholder="My continent (NA/EU/…)" className="glass-input text-sm px-2 py-1.5"
              onChange={(e) => setMyEx((p) => ({ ...p, continent: e.target.value.toUpperCase() || undefined }))} />
          </div>
          {error && <div className="text-xs text-red-400">{error}</div>}
          <button
            onClick={start}
            disabled={starting}
            className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg bg-accent-primary/20 border border-accent-primary/50 text-accent-primary hover:bg-accent-primary/30 transition-colors text-sm font-medium disabled:opacity-50"
          >
            <Play className="w-4 h-4" />
            Start {selected.name}
          </button>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Entry: the live operating window
// ---------------------------------------------------------------------------

function EntryView() {
  const contestState = useAppStore((s) => s.contestState)!;
  const setContestState = useAppStore((s) => s.setContestState);
  const rigStatus = useAppStore((s) => s.rigStatus);
  const queryClient = useQueryClient();

  const { data: definitions } = useQuery({
    queryKey: ['contest-definitions'],
    queryFn: () => api.getContestDefinitions(),
  });
  const definition: ContestDefinition | undefined = definitions?.find(
    (d) => d.id === contestState.definitionId
  );

  const [call, setCall] = useState('');
  const [exchange, setExchange] = useState<Record<string, string>>({});
  const [band, setBand] = useState('20m');
  const [mode, setMode] = useState('CW');
  const [check, setCheck] = useState<ContestCheckResponse | null>(null);
  const [lastLog, setLastLog] = useState<string | null>(null);
  const [logging, setLogging] = useState(false);
  const callRef = useRef<HTMLInputElement>(null);
  const followRig = useRef(true);

  // Follow the rig band/mode while the operator hasn't overridden manually.
  useEffect(() => {
    if (!followRig.current || !rigStatus) return;
    const b = bandFromHz(rigStatus.frequency);
    if (b) setBand(b);
    setMode(contestModeFromRig(rigStatus.mode));
  }, [rigStatus]);

  // Debounced dupe/mult check while typing the call.
  useEffect(() => {
    if (call.trim().length < 3) {
      setCheck(null);
      return;
    }
    const t = setTimeout(() => {
      api.checkContestCall(call.trim(), band, mode).then(setCheck).catch(() => setCheck(null));
    }, 250);
    return () => clearTimeout(t);
  }, [call, band, mode]);

  const rstDefault = mode === 'CW' ? '599' : '59';
  const hasRstField = definition?.rcvdExchange.some((f) => isType(f.type, 'rst')) ?? false;

  const wipe = useCallback(() => {
    setCall('');
    setExchange(hasRstField ? { rst: rstDefault } : {});
    setCheck(null);
    callRef.current?.focus();
  }, [hasRstField, rstDefault]);

  // Prefill RST once the definition (or mode) is known, without clobbering a
  // value the operator already typed.
  useEffect(() => {
    if (hasRstField) setExchange((p) => ({ ...p, rst: p.rst || rstDefault }));
  }, [hasRstField, rstDefault]);

  const logQso = useCallback(async () => {
    if (!call.trim() || logging) return;
    setLogging(true);
    try {
      const result = await api.logContestQso({
        callsign: call.trim(),
        band,
        mode,
        // Qso.Frequency is stored in kHz (ADIF export divides by 1000 → MHz).
        frequency: rigStatus ? rigStatus.frequency / 1000 : undefined,
        rstSent: rstDefault,
        exchange,
      });
      setContestState(result.state);
      setLastLog(
        `${call.trim().toUpperCase()} — ${result.isDupe ? 'DUPE' : `${result.points} pts`}${
          result.newMults.length > 0 ? ` +${result.newMults.length} mult` : ''
        }`
      );
      queryClient.invalidateQueries({ queryKey: ['qsos'] });
      wipe();
    } catch {
      setLastLog('Log failed — check backend');
    } finally {
      setLogging(false);
    }
  }, [call, band, mode, exchange, rigStatus, rstDefault, logging, setContestState, queryClient, wipe]);

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      logQso();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      wipe();
    }
  };

  const stopSession = async () => {
    await api.stopContestSession(contestState.sessionId);
    setContestState(null);
  };

  const exportCabrillo = async () => {
    try {
      const { blob, fileName } = await api.downloadCabrillo(contestState.sessionId);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = fileName;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      setLastLog(e instanceof Error ? e.message : 'Cabrillo export failed');
    }
  };

  const isDupe = check?.isDupe ?? false;
  const newMults = check?.newMults?.length ?? 0;

  return (
    <div className="flex flex-col h-full">
      {/* Session header */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-glass-100">
        <div className="min-w-0">
          <div className="text-sm font-medium text-gray-200 truncate">{contestState.label}</div>
          <div className="text-xs text-gray-500">{contestState.definitionName}</div>
        </div>
        <div className="flex items-center gap-1.5">
          <button
            onClick={exportCabrillo}
            title="Export Cabrillo log"
            className="flex items-center gap-1.5 px-2 py-1 rounded text-xs text-gray-400 hover:text-accent-primary border border-glass-100 hover:border-accent-primary/40 transition-colors"
          >
            <Download className="w-3 h-3" /> Cabrillo
          </button>
          <button
            onClick={stopSession}
            title="Stop contest session"
            className="flex items-center gap-1.5 px-2 py-1 rounded text-xs text-gray-400 hover:text-red-400 border border-glass-100 hover:border-red-500/40 transition-colors"
          >
            <Square className="w-3 h-3" /> Stop
          </button>
        </div>
      </div>

      {/* Entry row */}
      <div className="p-4 space-y-3" onKeyDown={onKeyDown}>
        <div className="flex gap-2">
          <select value={band} onChange={(e) => { followRig.current = false; setBand(e.target.value); }}
            className="glass-input text-sm px-2 py-1.5 w-20">
            {BANDS.map((b) => <option key={b} value={b}>{b}</option>)}
          </select>
          <select value={mode} onChange={(e) => { followRig.current = false; setMode(e.target.value); }}
            className="glass-input text-sm px-2 py-1.5 w-20">
            {MODES.map((m) => <option key={m} value={m}>{m}</option>)}
          </select>
          {contestState.serialInUse && (
            <div className="flex items-center px-2 text-xs text-gray-400 whitespace-nowrap">
              NR <span className="text-accent-primary font-mono font-semibold ml-1.5">
                {String(contestState.nextSerial).padStart(3, '0')}
              </span>
            </div>
          )}
        </div>

        <div className="flex gap-2 items-start">
          {/* Callsign */}
          <div className="flex-1 min-w-[8rem]">
            <input
              ref={callRef}
              type="text"
              value={call}
              onChange={(e) => setCall(e.target.value.toUpperCase())}
              placeholder="CALL"
              autoFocus
              spellCheck={false}
              className={`glass-input w-full font-mono text-lg tracking-wider px-3 py-2 uppercase ${
                isDupe
                  ? 'border-red-500/60 text-red-400'
                  : newMults > 0
                  ? 'border-emerald-500/60 text-emerald-300'
                  : ''
              }`}
            />
            <div className="h-4 mt-1 text-xs">
              {isDupe && (
                <span className="text-red-400 flex items-center gap-1">
                  <AlertTriangle className="w-3 h-3" /> DUPE (worked {check!.workedCount}×)
                </span>
              )}
              {!isDupe && newMults > 0 && (
                <span className="text-emerald-400 flex items-center gap-1">
                  <Sparkles className="w-3 h-3" /> New mult ×{newMults}
                </span>
              )}
            </div>
          </div>

          {/* Dynamic exchange fields from the definition (RST included, prefilled) */}
          {definition?.rcvdExchange
            .map((f) => (
              <div key={f.key} style={{ width: `${Math.max(f.width, 4)}rem` }}>
                <input
                  type="text"
                  value={exchange[f.key] ?? ''}
                  onChange={(e) => setExchange((p) => ({ ...p, [f.key]: e.target.value }))}
                  placeholder={f.label}
                  spellCheck={false}
                  className="glass-input w-full font-mono text-lg px-2 py-2 uppercase"
                />
                <div className="h-4" />
              </div>
            ))}
        </div>

        {lastLog && <div className="text-xs text-gray-400">Last: {lastLog}</div>}
      </div>

      {/* Score strip */}
      <div className="mt-auto grid grid-cols-5 gap-px bg-glass-100 border-t border-glass-100 text-center">
        <ScoreCell label="QSOs" value={contestState.qsos} />
        <ScoreCell label="Points" value={contestState.points} />
        <ScoreCell label="Mults" value={contestState.multipliers} />
        <ScoreCell label="Score" value={contestState.score.toLocaleString()} accent />
        <ScoreCell label="Rate/hr" value={contestState.rateLastHour} />
      </div>
    </div>
  );
}

function ScoreCell({ label, value, accent }: { label: string; value: number | string; accent?: boolean }) {
  return (
    <div className="bg-dark-800/80 px-2 py-2">
      <div className={`text-base font-semibold font-mono ${accent ? 'text-accent-primary' : 'text-gray-200'}`}>
        {value}
      </div>
      <div className="text-[10px] uppercase tracking-wider text-gray-500">{label}</div>
    </div>
  );
}

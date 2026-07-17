import { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Swords, Play, Square, Search, AlertTriangle, Sparkles, Download, Plus, Copy, Pencil, Trash2, ChevronDown, ChevronRight } from 'lucide-react';
import {
  api,
  ContestDefinition,
  ContestMyExchange,
  ContestCheckResponse,
} from '../api/client';
import { useAppStore } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import { GlassPanel } from '../components/GlassPanel';
import { ContestEditor } from '../components/ContestEditor';
import { isValidStateProv, isKnownCounty, matchCounties, type County } from '../contest/locations';

// Only used before the active definition has loaded; the live dropdowns come from
// the contest definition's own bands/modes so we never offer a band or mode the
// contest forbids (e.g. FT8 in a QSO party, or WARC bands anywhere).
const FALLBACK_BANDS = ['160m', '80m', '40m', '20m', '15m', '10m', '6m', '2m'];
const FALLBACK_MODES = ['CW', 'SSB', 'FT8', 'FT4', 'RTTY'];

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
  const queryClient = useQueryClient();
  const [search, setSearch] = useState('');
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [label, setLabel] = useState('');
  const [myEx, setMyEx] = useState<ContestMyExchange>({});
  const [error, setError] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);
  // { open } drives the editor modal; initial is the draft to edit (null = new).
  const [editor, setEditor] = useState<{ initial: ContestDefinition | null } | null>(null);

  const { data: definitions } = useQuery({
    queryKey: ['contest-definitions'],
    queryFn: () => api.getContestDefinitions(),
  });

  const refresh = () => queryClient.invalidateQueries({ queryKey: ['contest-definitions'] });

  const clone = async (id: string) => {
    setError(null);
    try {
      const draft = await api.cloneContestDefinition(id, `${definitions?.find((d) => d.id === id)?.name ?? 'Contest'} copy`);
      setEditor({ initial: draft });
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Clone failed');
    }
  };

  const remove = async (id: string, name: string) => {
    if (!window.confirm(`Delete your contest "${name}"? This cannot be undone.`)) return;
    setError(null);
    try {
      await api.deleteContestDefinition(id);
      if (selectedId === id) setSelectedId(null);
      refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Delete failed');
    }
  };

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
    if (myEx.state && !isValidStateProv(myEx.state)) {
      setError(`"${myEx.state}" isn't a valid state/province`);
      return;
    }
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
      {/* Search + new */}
      <div className="flex gap-2">
        <div className="relative flex-1">
          <Search className="w-4 h-4 absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-500" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search contests…"
            className="glass-input w-full text-sm pl-8 pr-2 py-1.5"
          />
        </div>
        <button
          onClick={() => setEditor({ initial: null })}
          title="Create a custom contest"
          className="flex items-center gap-1 px-2.5 py-1.5 rounded-lg text-sm bg-dark-700/50 border border-glass-100 text-gray-300 hover:bg-dark-600/50 whitespace-nowrap"
        >
          <Plus className="w-4 h-4" /> New
        </button>
      </div>

      {/* Definition list */}
      <div className="flex-1 min-h-[8rem] overflow-y-auto space-y-1">
        {filtered.map((d) => (
          <div
            key={d.id}
            className={`flex items-stretch rounded-lg border text-sm transition-colors ${
              selectedId === d.id
                ? 'bg-accent-primary/20 border-accent-primary/50'
                : 'bg-dark-700/50 border-glass-100 hover:bg-dark-600/50'
            }`}
          >
            <button
              onClick={() => setSelectedId(d.id)}
              className="flex-1 text-left px-3 py-2 min-w-0"
            >
              <div className="flex items-center justify-between">
                <span className={`font-medium truncate ${selectedId === d.id ? 'text-gray-100' : 'text-gray-300'}`}>{d.name}</span>
                <span className="text-xs text-gray-500 ml-2 shrink-0">
                  {d.builtin ? d.modes.join('/') : 'custom'}
                </span>
              </div>
              <div className="text-xs text-gray-500 mt-0.5">
                Exchange: {d.rcvdExchange.map((f) => f.label).join(' + ') || '—'}
              </div>
            </button>
            <div className="flex items-center gap-0.5 pr-1.5 shrink-0">
              {d.builtin ? (
                <IconBtn title="Clone this contest" onClick={() => clone(d.id)}><Copy className="w-3.5 h-3.5" /></IconBtn>
              ) : (
                <>
                  <IconBtn title="Edit this contest" onClick={() => setEditor({ initial: d })}><Pencil className="w-3.5 h-3.5" /></IconBtn>
                  <IconBtn title="Delete this contest" danger onClick={() => remove(d.id, d.name)}><Trash2 className="w-3.5 h-3.5" /></IconBtn>
                </>
              )}
            </div>
          </div>
        ))}
        {filtered.length === 0 && (
          <div className="text-center text-sm text-gray-500 py-6">No contests match</div>
        )}
      </div>

      <InteropConfig />

      {editor && (
        <ContestEditor
          initial={editor.initial}
          onClose={() => setEditor(null)}
          onSaved={() => { setEditor(null); refresh(); }}
        />
      )}

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
          {/* Role-split (QSO party) contests: the operator declares where they're
              operating from. State decides in-state vs out-of-state; county is the
              in-area sent exchange. */}
          {selected.homeArea?.kind === 'StateCounty' && (
            <div className="grid grid-cols-2 gap-2">
              <input type="text" placeholder="My state" className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => setMyEx((p) => ({ ...p, state: e.target.value.toUpperCase() || undefined }))} />
              <input type="text" placeholder="My county" className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => setMyEx((p) => ({ ...p, county: e.target.value.toUpperCase() || undefined }))} />
            </div>
          )}
          {/* Power class — drives the contest's final-score power multiplier. */}
          {selected.powerMultipliers && (
            <select
              className="glass-input w-full text-sm px-2 py-1.5"
              value={myEx.power ?? ''}
              onChange={(e) => setMyEx((p) => ({ ...p, power: e.target.value || undefined }))}
            >
              <option value="">Power class…</option>
              {Object.entries(selected.powerMultipliers).map(([cls, mult]) => (
                <option key={cls} value={cls}>
                  {cls === 'QRP' ? 'QRP' : cls === 'LOW' ? 'Low' : cls === 'HIGH' ? 'High' : cls}
                  {mult !== 1 ? ` (×${mult})` : ''}
                </option>
              ))}
            </select>
          )}
          {/* My-exchange fields relevant to the sent exchange */}
          <div className="grid grid-cols-2 gap-2">
            {selected.sentExchange.some((f) => f.type === 'zone') && (
              <input type="text" placeholder="My CQ zone" className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => setMyEx((p) => ({ ...p, cqZone: parseInt(e.target.value) || undefined }))} />
            )}
            {selected.sentExchange.some((f) => f.type === 'state') && selected.homeArea?.kind !== 'StateCounty' && (
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

  // The bands/modes the operator may pick come from the contest itself — never a
  // hardcoded list — so a contest that forbids FT8 or WARC bands simply won't
  // offer them. Fall back to a generic set only until the definition loads.
  // Bands are normalized to the app's lowercase convention ("20m") so they match
  // the rig-follow (bandFromHz) and the logged band; modes are uppercased ("SSB").
  const bands = useMemo(
    () => (definition?.bands?.length ? definition.bands : FALLBACK_BANDS).map((b) => b.toLowerCase()),
    [definition]
  );
  const modes = useMemo(
    () => (definition?.modes?.length ? definition.modes : FALLBACK_MODES).map((m) => m.toUpperCase()),
    [definition]
  );
  // Home state(s) of a role-split QSO party, for county autocomplete/validation.
  const homeStates = useMemo(() => definition?.homeArea?.states ?? [], [definition]);

  // Super Check Partial: load the call set once, match locally as we type.
  const { data: scpCalls } = useQuery({
    queryKey: ['contest-scp'],
    queryFn: () => api.getScpCalls(),
    staleTime: 10 * 60 * 1000,
  });

  const [call, setCall] = useState('');
  const [exchange, setExchange] = useState<Record<string, string>>({});
  const [band, setBand] = useState('20m');
  const [mode, setMode] = useState('CW');
  const [check, setCheck] = useState<ContestCheckResponse | null>(null);
  const [lastLog, setLastLog] = useState<string | null>(null);
  const [logging, setLogging] = useState(false);
  const callRef = useRef<HTMLInputElement>(null);
  const followRig = useRef(true);

  // Follow the rig band/mode while the operator hasn't overridden manually, but
  // only when the rig's band/mode is actually valid for this contest (the rig may
  // sit on a WARC band while tuning around during a non-WARC contest).
  useEffect(() => {
    if (!followRig.current || !rigStatus) return;
    const b = bandFromHz(rigStatus.frequency);
    if (b && bands.includes(b)) setBand(b);
    const m = contestModeFromRig(rigStatus.mode);
    if (modes.includes(m)) setMode(m);
  }, [rigStatus, bands, modes]);

  // Keep the selection inside the contest's allowed set — resets a stale band/mode
  // carried over from a prior contest to the first the current contest allows.
  useEffect(() => {
    if (!bands.includes(band)) setBand(bands[0]);
  }, [bands, band]);
  useEffect(() => {
    if (!modes.includes(mode)) setMode(modes[0]);
  }, [modes, mode]);

  // A click on the contest bandmap fills the call here.
  const contestSpotCall = useAppStore((s) => s.contestSpotCall);
  useEffect(() => {
    if (contestSpotCall?.call) {
      setCall(contestSpotCall.call.toUpperCase());
      callRef.current?.focus();
    }
  }, [contestSpotCall]);

  // Super Check Partial matches for the current partial call (local, instant).
  const scpMatches = useMemo(() => {
    const q = call.trim().toUpperCase();
    if (q.length < 2 || !scpCalls) return [];
    return scpCalls.filter((c) => c.includes(q) && c !== q).slice(0, 10);
  }, [call, scpCalls]);

  // Debounced dupe/mult check + exchange prefill while typing the call.
  useEffect(() => {
    if (call.trim().length < 3) {
      setCheck(null);
      return;
    }
    const t = setTimeout(() => {
      api.checkContestCall(call.trim(), band, mode)
        .then((res) => {
          setCheck(res);
          // Prefill received-exchange fields we don't already have a value for.
          if (res.prefill) {
            setExchange((prev) => {
              const next = { ...prev };
              for (const [key, value] of Object.entries(res.prefill!)) {
                if (!next[key]) next[key] = value;
              }
              return next;
            });
          }
        })
        .catch(() => setCheck(null));
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

    // Strict state/province check: a 2-char location must be a real S/P (or DX).
    // A 3+ char county is only assisted (warned in the field), never blocked.
    const locField = definition?.rcvdExchange.find((f) => isType(f.type, 'state'));
    if (locField) {
      const v = (exchange[locField.key] ?? '').trim().toUpperCase();
      if (v.length > 0 && v.length <= 2 && !isValidStateProv(v)) {
        setLastLog(`"${v}" isn't a valid state/province — fix before logging`);
        return;
      }
    }

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
  }, [call, band, mode, exchange, definition, rigStatus, rstDefault, logging, setContestState, queryClient, wipe]);

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
          <div className="flex items-center gap-1.5">
            <span className="text-xs text-gray-500 truncate">{contestState.definitionName}</span>
            <RoleBadge role={contestState.role} />
          </div>
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
            {bands.map((b) => <option key={b} value={b}>{b}</option>)}
          </select>
          <select value={mode} onChange={(e) => { followRig.current = false; setMode(e.target.value); }}
            className="glass-input text-sm px-2 py-1.5 w-20">
            {modes.map((m) => <option key={m} value={m}>{m}</option>)}
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

          {/* Dynamic exchange fields from the definition (RST included, prefilled).
              The location (state-typed) field gets S/P validation + county
              autocomplete for QSO parties. */}
          {definition?.rcvdExchange.map((f) =>
            isType(f.type, 'state') ? (
              <LocationField
                key={f.key}
                label={f.label}
                width={f.width}
                homeStates={homeStates}
                value={exchange[f.key] ?? ''}
                onChange={(v) => setExchange((p) => ({ ...p, [f.key]: v }))}
              />
            ) : (
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
            )
          )}
        </div>

        {/* Super Check Partial — click a match to fill the call */}
        {scpMatches.length > 0 && !isDupe && (
          <div className="flex flex-wrap gap-1">
            {scpMatches.map((c) => (
              <button
                key={c}
                onClick={() => { setCall(c); callRef.current?.focus(); }}
                className="px-1.5 py-0.5 rounded bg-dark-700/70 border border-glass-100 text-xs font-mono text-gray-300 hover:border-accent-primary/50 hover:text-accent-primary transition-colors"
              >
                {c}
              </button>
            ))}
          </div>
        )}

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

// Collapsible config for N1MM UDP broadcast + online score reporting. Persists
// through the shared settings store; the server broadcasts when enabled.
function InteropConfig() {
  const settings = useSettingsStore((s) => s.settings.contest);
  const update = useSettingsStore((s) => s.updateContestSettings);
  const save = useSettingsStore((s) => s.saveSettings);
  const [open, setOpen] = useState(false);

  const commit = (patch: Parameters<typeof update>[0]) => { update(patch); void save(); };

  return (
    <div className="border-t border-glass-100 pt-2">
      <button onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-1.5 text-xs text-gray-400 hover:text-gray-200">
        {open ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
        Broadcast / score reporting
      </button>
      {open && (
        <div className="mt-2 space-y-2 text-xs text-gray-400">
          <label className="flex items-center gap-2">
            <input type="checkbox" checked={settings.n1mmUdpEnabled}
              onChange={(e) => commit({ n1mmUdpEnabled: e.target.checked })} />
            N1MM UDP broadcast
          </label>
          {settings.n1mmUdpEnabled && (
            <div className="flex gap-2 pl-6">
              <input className="glass-input text-xs px-2 py-1 flex-1" value={settings.n1mmUdpHost}
                onChange={(e) => commit({ n1mmUdpHost: e.target.value })} placeholder="host" />
              <input className="glass-input text-xs px-2 py-1 w-20" type="number" value={settings.n1mmUdpPort}
                onChange={(e) => commit({ n1mmUdpPort: Number(e.target.value) || 0 })} placeholder="port" />
            </div>
          )}
          <label className="flex items-center gap-2">
            <input type="checkbox" checked={settings.onlineScoreEnabled}
              onChange={(e) => commit({ onlineScoreEnabled: e.target.checked })} />
            Online score reporting
          </label>
          {settings.onlineScoreEnabled && (
            <input className="glass-input text-xs px-2 py-1 w-full ml-0" value={settings.onlineScoreUrl}
              onChange={(e) => commit({ onlineScoreUrl: e.target.value })} placeholder="score post URL" />
          )}
        </div>
      )}
    </div>
  );
}

// The received S/P/C field: a 2-letter state/province (validated strictly, red on
// bad input) or a home-state county code (autocompleted, amber when unknown but
// still loggable). For non-QSO-party contests homeStates is empty → plain S/P box.
function LocationField({
  value, onChange, label, width, homeStates,
}: {
  value: string;
  onChange: (v: string) => void;
  label: string;
  width: number;
  homeStates: string[];
}) {
  const [focused, setFocused] = useState(false);
  const v = value.trim().toUpperCase();
  const matches = useMemo(
    () => (homeStates.length ? matchCounties(v, homeStates) : []),
    [v, homeStates]
  );
  const badStateProv = v.length > 0 && v.length <= 2 && !isValidStateProv(v);
  const unknownCounty = v.length >= 3 && homeStates.length > 0 && !isKnownCounty(v, homeStates);
  const showMenu = focused && matches.length > 0 && v.length >= 1;

  const pick = (c: County) => { onChange(c.code); setFocused(false); };

  return (
    <div className="relative" style={{ width: `${Math.max(width, 4)}rem` }}>
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onFocus={() => setFocused(true)}
        onBlur={() => setTimeout(() => setFocused(false), 120)}
        placeholder={label}
        spellCheck={false}
        title={
          badStateProv ? 'Not a valid state/province'
            : unknownCounty ? 'Unknown county code for this contest — check it'
            : undefined
        }
        className={`glass-input w-full font-mono text-lg px-2 py-2 uppercase ${
          badStateProv ? 'border-red-500/70 text-red-400'
            : unknownCounty ? 'border-amber-500/70 text-amber-300'
            : ''
        }`}
      />
      <div className="h-4" />
      {showMenu && (
        <div className="absolute z-20 top-full left-0 mt-0.5 w-48 max-h-48 overflow-y-auto rounded-lg bg-dark-800 border border-glass-100 shadow-lg">
          {matches.map((c) => (
            <button
              key={c.code}
              onMouseDown={(e) => { e.preventDefault(); pick(c); }}
              className="flex w-full items-center justify-between px-2 py-1 text-left text-xs hover:bg-dark-600/60"
            >
              <span className="font-mono text-accent-primary">{c.code}</span>
              <span className="text-gray-400 truncate ml-2">{c.name}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

// Shows how the engine classified the operator for role-split contests (QSO
// parties, ARRL DX). Hidden for 'All' (global contests with no location split).
function RoleBadge({ role }: { role: string }) {
  const map: Record<string, { label: string; cls: string }> = {
    InArea: { label: 'In-State', cls: 'bg-emerald-500/15 text-emerald-300 border-emerald-500/40' },
    OutArea: { label: 'Out-of-State', cls: 'bg-sky-500/15 text-sky-300 border-sky-500/40' },
    Dx: { label: 'DX', cls: 'bg-amber-500/15 text-amber-300 border-amber-500/40' },
  };
  const m = map[role];
  if (!m) return null;
  return (
    <span className={`shrink-0 px-1.5 py-0.5 rounded text-[10px] font-medium uppercase tracking-wide border ${m.cls}`}>
      {m.label}
    </span>
  );
}

function IconBtn({ title, onClick, danger, children }: { title: string; onClick: () => void; danger?: boolean; children: React.ReactNode }) {
  return (
    <button
      title={title}
      onClick={onClick}
      className={`p-1.5 rounded text-gray-500 transition-colors ${danger ? 'hover:text-red-400' : 'hover:text-accent-primary'}`}
    >
      {children}
    </button>
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

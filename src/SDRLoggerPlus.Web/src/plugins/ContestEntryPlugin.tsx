import { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Swords, Play, Square, Search, AlertTriangle, Sparkles, Download, Plus, Copy, Pencil, Trash2, ChevronDown, ChevronRight, EyeOff, Eye, X, Settings2 } from 'lucide-react';
import {
  api,
  ContestDefinition,
  ContestMyExchange,
  ContestCheckResponse,
  ContestQso,
  ContestSession,
  ContestStateEvent,
} from '../api/client';
import { useAppStore } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import { GlassPanel } from '../components/GlassPanel';
import { ContestEditor } from '../components/ContestEditor';
import {
  isValidStateProv, isKnownCounty, matchCounties, hasOfficialCounties,
  countiesForContest, type County,
} from '../contest/locations';

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
  // A stale active session (idle a long time — e.g. left over from a past
  // contest) is NOT auto-opened. We keep it in the store (so SignalR updates
  // still land) but gate the entry view on staleness + an explicit resume, so
  // last year's calls don't silently reappear. Deriving the gate from
  // contestState.isStale (rather than nulling the store) means a broadcast
  // can't race past the guard.
  const [resumed, setResumed] = useState(false);

  // Seed state from the API on mount (covers reload while a session is live).
  useEffect(() => {
    api.getContestState().then(setContestState).catch(() => {});
  }, [setContestState]);

  const showStale = !!contestState?.isStale && !resumed;

  // End the leftover session server-side so it stops prompting, then clear it.
  const endStale = async () => {
    if (!contestState) return;
    try {
      await api.stopContestSession(contestState.sessionId);
    } catch {
      /* ignore — worst case it prompts again next open */
    }
    setResumed(false);
    setContestState(null);
  };

  return (
    <GlassPanel title="Contest" icon={<Swords className="w-5 h-5" />}>
      {contestState && !showStale ? (
        <EntryView />
      ) : (
        <SetupView
          staleSession={showStale ? contestState : null}
          onResumeStale={() => setResumed(true)}
          onEndStale={endStale}
        />
      )}
    </GlassPanel>
  );
}

// ---------------------------------------------------------------------------
// Setup: search + pick a contest definition, my exchange, start session
// ---------------------------------------------------------------------------

function SetupView({
  staleSession,
  onResumeStale,
  onEndStale,
}: {
  staleSession?: ContestStateEvent | null;
  onResumeStale?: () => void;
  onEndStale?: () => Promise<void> | void;
}) {
  const setContestState = useAppStore((s) => s.setContestState);
  const queryClient = useQueryClient();
  const [endingStale, setEndingStale] = useState(false);

  const endStale = async () => {
    setEndingStale(true);
    try {
      await onEndStale?.();
    } finally {
      setEndingStale(false);
    }
  };
  const [search, setSearch] = useState('');
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [label, setLabel] = useState('');
  const [myEx, setMyEx] = useState<ContestMyExchange>({});
  const [error, setError] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);
  // { open } drives the editor modal; initial is the draft to edit (null = new).
  const [editor, setEditor] = useState<{ initial: ContestDefinition | null } | null>(null);
  const [managing, setManaging] = useState(false);

  // Contests the operator has removed from the picker (persisted). Built-ins get
  // re-seeded on startup, so this is a reversible hide rather than a real delete.
  const contestSettings = useSettingsStore((s) => s.settings.contest);
  const updateContestSettings = useSettingsStore((s) => s.updateContestSettings);
  const saveSettings = useSettingsStore((s) => s.saveSettings);
  const hidden = contestSettings.hiddenContestIds ?? [];

  const commitHidden = (ids: string[]) => {
    updateContestSettings({ hiddenContestIds: ids });
    void saveSettings();
    if (selectedId && ids.includes(selectedId)) setSelectedId(null);
  };
  const hideContest = (id: string) => commitHidden(Array.from(new Set([...hidden, id])));
  const showContest = (id: string) => commitHidden(hidden.filter((h) => h !== id));

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
    const visible = definitions.filter((d) => !hidden.includes(d.id));
    const q = search.trim().toLowerCase();
    if (!q) return visible;
    return visible.filter(
      (d) =>
        d.name.toLowerCase().includes(q) ||
        d.cabrilloName.toLowerCase().includes(q) ||
        d.modes.some((m) => m.toLowerCase().includes(q))
    );
  }, [definitions, search, hidden]);

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
      {/* Resume prompt for a stale leftover session (idle a long time). Shown
          instead of auto-opening it, so last year's calls don't reappear. */}
      {staleSession && (
        <div className="flex flex-col gap-2 p-3 rounded-lg bg-amber-500/10 border border-amber-500/30">
          <div className="flex items-start gap-2">
            <AlertTriangle className="w-4 h-4 text-amber-400 mt-0.5 shrink-0" />
            <div className="text-sm text-gray-200 leading-snug">
              You have an unfinished session:{' '}
              <span className="font-medium">{staleSession.label}</span>
              <div className="text-xs text-gray-400 mt-0.5">
                {staleSession.qsos} QSO{staleSession.qsos === 1 ? '' : 's'} · started{' '}
                {new Date(staleSession.startedAt).toLocaleDateString()}. Resume it, or
                start a new contest below.
              </div>
            </div>
          </div>
          <div className="flex gap-2">
            <button
              onClick={() => onResumeStale?.()}
              className="flex items-center gap-1 px-2.5 py-1 rounded-md text-sm bg-amber-500/20 border border-amber-500/40 text-amber-200 hover:bg-amber-500/30"
            >
              <Play className="w-3.5 h-3.5" /> Resume
            </button>
            <button
              onClick={endStale}
              disabled={endingStale}
              className="flex items-center gap-1 px-2.5 py-1 rounded-md text-sm bg-dark-700/50 border border-glass-100 text-gray-300 hover:bg-dark-600/50 disabled:opacity-50"
            >
              <X className="w-3.5 h-3.5" /> {endingStale ? 'Ending…' : 'End it'}
            </button>
          </div>
        </div>
      )}
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
        <button
          onClick={() => setManaging(true)}
          title="Manage / restore removed contests"
          className="relative flex items-center px-2.5 py-1.5 rounded-lg text-sm bg-dark-700/50 border border-glass-100 text-gray-300 hover:bg-dark-600/50 whitespace-nowrap"
        >
          <Settings2 className="w-4 h-4" />
          {hidden.length > 0 && (
            <span className="absolute -top-1.5 -right-1.5 min-w-[1rem] px-1 rounded-full bg-accent-primary/80 text-[10px] leading-4 text-white text-center">
              {hidden.length}
            </span>
          )}
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
                <>
                  <IconBtn title="Clone this contest" onClick={() => clone(d.id)}><Copy className="w-3.5 h-3.5" /></IconBtn>
                  <IconBtn title="Remove from list" onClick={() => hideContest(d.id)}><EyeOff className="w-3.5 h-3.5" /></IconBtn>
                </>
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

      {managing && definitions && (
        <ManageContestsModal
          definitions={definitions}
          hidden={hidden}
          onHide={hideContest}
          onShow={showContest}
          onSetHidden={commitHidden}
          onClose={() => setManaging(false)}
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
          {/* Role-split contests (QSO parties, ARRL DX): the operator declares
              whether they're operating in- or out-of-area. This drives which
              exchange is sent and the whole scoring role — chosen explicitly rather
              than only inferred from the typed state. */}
          {roleLabels(selected) && (
            <div className="space-y-1">
              <div className="text-[10px] uppercase tracking-wider text-gray-500">Operating as</div>
              <RoleSelector def={selected}
                value={myEx.roleOverride ?? derivedRole(selected, myEx.state)}
                onChange={(r) => setMyEx((p) => ({ ...p, roleOverride: r }))} />
            </div>
          )}
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
  // Set while correcting an already-logged QSO (busted call); Enter saves instead
  // of logging a new QSO.
  const [editingId, setEditingId] = useState<string | null>(null);
  const callRef = useRef<HTMLInputElement>(null);
  const followRig = useRef(true);

  // Recent QSOs of this session for the edit strip; refreshed after each log/edit.
  const { data: recentQsos } = useQuery({
    queryKey: ['contest-qsos', contestState.sessionId],
    queryFn: () => api.getContestQsos(6),
  });

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

  // Debounced dupe/mult check + exchange prefill while typing the call. Skipped
  // while editing an existing QSO (it's already in the log, so it'd read as a dupe).
  useEffect(() => {
    if (editingId || call.trim().length < 3) {
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
  }, [call, band, mode, editingId]);

  const rstDefault = mode === 'CW' ? '599' : '59';
  const hasRstField = definition?.rcvdExchange.some((f) => isType(f.type, 'rst')) ?? false;

  const wipe = useCallback(() => {
    setCall('');
    setExchange(hasRstField ? { rst: rstDefault } : {});
    setCheck(null);
    setEditingId(null);
    callRef.current?.focus();
  }, [hasRstField, rstDefault]);

  // Load a logged QSO back into the fields to correct it.
  const startEdit = useCallback((q: ContestQso) => {
    setEditingId(q.id);
    setCall(q.callsign);
    setExchange(q.exchange ?? {});
    setCheck(null);
    callRef.current?.focus();
  }, []);

  // Remove a busted QSO from the log; the server recomputes and rebroadcasts state.
  const removeQso = useCallback(async (q: ContestQso) => {
    if (!window.confirm(`Delete ${q.callsign} from the log?`)) return;
    try {
      const state = await api.deleteContestQso(q.id);
      if (state) setContestState(state);
      if (editingId === q.id) wipe();
      queryClient.invalidateQueries({ queryKey: ['qsos'] });
      queryClient.invalidateQueries({ queryKey: ['contest-qsos'] });
      setLastLog(`Deleted ${q.callsign}`);
    } catch {
      setLastLog('Delete failed — check backend');
    }
  }, [editingId, wipe, setContestState, queryClient]);

  // The active session (for the mid-contest My-exchange / class editor).
  const { data: activeSession } = useQuery({
    queryKey: ['contest-active-session', contestState.sessionId],
    queryFn: () => api.getActiveContestSession(),
  });

  // Prefill RST once the definition (or mode) is known, without clobbering a
  // value the operator already typed.
  useEffect(() => {
    if (hasRstField) setExchange((p) => ({ ...p, rst: p.rst || rstDefault }));
  }, [hasRstField, rstDefault]);

  const logQso = useCallback(async () => {
    if (!call.trim() || logging) return;

    // Location check: a 2-char value must be a real state/province (or DX). A 3+
    // char county is blocked only when the contest has an official county table
    // (an unknown code is then genuinely wrong); with the generic fallback it's
    // assisted, not blocked.
    const locField = definition?.rcvdExchange.find((f) => isType(f.type, 'state'));
    if (locField && !editingId) {
      const v = (exchange[locField.key] ?? '').trim().toUpperCase();
      if (v.length > 0 && v.length <= 2 && !isValidStateProv(v)) {
        setLastLog(`"${v}" isn't a valid state/province — fix before logging`);
        return;
      }
      if (v.length >= 3 && hasOfficialCounties(definition?.id) && !isKnownCounty(v, definition?.id)) {
        setLastLog(`"${v}" isn't a valid county code for this contest — fix before logging`);
        return;
      }
    }

    setLogging(true);
    try {
      if (editingId) {
        // Correcting a logged QSO — save the fix and take the recomputed state.
        const state = await api.updateContestQso(editingId, { callsign: call.trim(), exchange });
        setContestState(state);
        setLastLog(`Edited ${call.trim().toUpperCase()}`);
      } else {
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
      }
      queryClient.invalidateQueries({ queryKey: ['qsos'] });
      queryClient.invalidateQueries({ queryKey: ['contest-qsos'] });
      wipe();
    } catch {
      setLastLog(editingId ? 'Edit failed — check backend' : 'Log failed — check backend');
    } finally {
      setLogging(false);
    }
  }, [call, band, mode, exchange, editingId, definition, rigStatus, rstDefault, logging, setContestState, queryClient, wipe]);

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
                defId={definition?.id}
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

        {editingId ? (
          <div className="flex items-center justify-between text-xs text-amber-300">
            <span className="flex items-center gap-1"><Pencil className="w-3 h-3" /> Editing — Enter to save, Esc to cancel</span>
            <button onClick={wipe} className="text-gray-400 hover:text-gray-200">Cancel</button>
          </div>
        ) : (
          lastLog && <div className="text-xs text-gray-400">Last: {lastLog}</div>
        )}

        {/* Recent QSOs — click the call to correct a busted entry, × to delete. */}
        {recentQsos && recentQsos.length > 0 && (
          <div className="flex flex-wrap gap-1">
            {recentQsos.map((q) => (
              <div
                key={q.id}
                className={`flex items-stretch rounded border text-xs font-mono overflow-hidden transition-colors ${
                  editingId === q.id
                    ? 'bg-amber-500/20 border-amber-500/50'
                    : 'bg-dark-700/70 border-glass-100 hover:border-amber-500/50'
                }`}
              >
                <button
                  onClick={() => startEdit(q)}
                  title={`Edit — ${q.band} ${q.mode}${q.isDupe ? ' (dupe)' : ` · ${q.points} pts`}`}
                  className={`px-1.5 py-0.5 ${
                    editingId === q.id ? 'text-amber-300'
                      : q.isDupe ? 'text-red-400/80 hover:text-amber-300'
                      : 'text-gray-300 hover:text-amber-300'
                  }`}
                >
                  {q.callsign}
                </button>
                <button
                  onClick={() => removeQso(q)}
                  title={`Delete ${q.callsign}`}
                  className="px-1 border-l border-glass-100 text-gray-500 hover:text-red-400 hover:bg-red-500/10"
                >
                  <X className="w-3 h-3" />
                </button>
              </div>
            ))}
          </div>
        )}

        {/* Mid-contest config: change your county / power class / state without
            stopping the session (recomputes role + score on save). */}
        {definition && activeSession && (
          <SessionConfig definition={definition} session={activeSession} />
        )}
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

// In/out-of-area labels for a role-split contest, or null when the contest has no
// split (global contests — no selector shown).
function roleLabels(def: ContestDefinition): { inArea: string; outArea: string } | null {
  const kind = def.homeArea?.kind;
  if (!kind || kind === 'None') return null;
  return kind === 'StateCounty'
    ? { inArea: 'In-state', outArea: 'Out-of-state' }
    : { inArea: 'In-area', outArea: 'Out-of-area' };
}

// Frontend guess of the role from the typed state, matching the backend's
// location-based derivation for StateCounty parties (WVE defaults to in-area).
function derivedRole(def: ContestDefinition, state: string | undefined): string {
  if (def.homeArea?.kind === 'StateCounty') {
    const st = (state ?? '').trim().toUpperCase();
    return st && def.homeArea.states?.some((s) => s.toUpperCase() === st) ? 'InArea' : 'OutArea';
  }
  return 'InArea';
}

// Segmented control letting the operator declare in/out-of-area explicitly rather
// than relying on the state-based guess. Returns null for non-split contests.
function RoleSelector({ def, value, onChange }: {
  def: ContestDefinition; value: string; onChange: (role: string) => void;
}) {
  const labels = roleLabels(def);
  if (!labels) return null;
  return (
    <div className="flex rounded-lg overflow-hidden border border-glass-100 text-sm">
      {(['InArea', 'OutArea'] as const).map((r) => (
        <button
          key={r}
          type="button"
          onClick={() => onChange(r)}
          className={`flex-1 px-2 py-1.5 transition-colors ${
            value === r
              ? 'bg-accent-primary/25 text-accent-primary font-medium'
              : 'bg-dark-700/40 text-gray-400 hover:text-gray-200'
          }`}
        >
          {r === 'InArea' ? labels.inArea : labels.outArea}
        </button>
      ))}
    </div>
  );
}

// Mid-contest editor for the operator's own exchange (county, power class, state,
// …). Saving re-derives the in/out-of-area role and recomputes the score, so a
// wrong county or power class can be corrected without restarting the session.
function SessionConfig({ definition, session }: { definition: ContestDefinition; session: ContestSession }) {
  const setContestState = useAppStore((s) => s.setContestState);
  const currentRole = useAppStore((s) => s.contestState?.role);
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [ex, setEx] = useState<ContestMyExchange>(session.myExchange ?? {});
  const [saving, setSaving] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const labels = roleLabels(definition);

  // Re-seed local edits if the stored exchange changes underneath us.
  useEffect(() => { setEx(session.myExchange ?? {}); }, [session.myExchange]);

  const set = (patch: Partial<ContestMyExchange>) => { setEx((p) => ({ ...p, ...patch })); setStatus(null); };
  const homeCounty = definition.homeArea?.kind === 'StateCounty';
  const has = (t: string) => definition.sentExchange.some((f) => isType(f.type, t));

  const save = async () => {
    if (ex.state && !isValidStateProv(ex.state)) { setStatus(`"${ex.state}" isn't a valid state/province`); return; }
    setSaving(true);
    try {
      const state = await api.updateContestExchange(session.id, ex);
      if (state) setContestState(state);
      queryClient.invalidateQueries({ queryKey: ['contest-active-session'] });
      queryClient.invalidateQueries({ queryKey: ['contest-qsos'] });
      setStatus('Saved');
    } catch {
      setStatus('Save failed — check backend');
    } finally { setSaving(false); }
  };

  return (
    <div className="border-t border-glass-100 pt-2">
      <button onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-1.5 text-xs text-gray-400 hover:text-gray-200">
        {open ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
        <Settings2 className="w-3 h-3" /> My exchange / class
      </button>
      {open && (
        <div className="mt-2 space-y-2">
          {labels && (
            <div className="space-y-1">
              <div className="text-[10px] uppercase tracking-wider text-gray-500">Operating as</div>
              <RoleSelector def={definition}
                value={ex.roleOverride ?? currentRole ?? derivedRole(definition, ex.state)}
                onChange={(r) => set({ roleOverride: r })} />
            </div>
          )}
          {homeCounty && (
            <div className="grid grid-cols-2 gap-2">
              <input type="text" placeholder="My state" value={ex.state ?? ''} className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => set({ state: e.target.value.toUpperCase() || undefined })} />
              <input type="text" placeholder="My county" value={ex.county ?? ''} className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => set({ county: e.target.value.toUpperCase() || undefined })} />
            </div>
          )}
          {definition.powerMultipliers && (
            <select className="glass-input w-full text-sm px-2 py-1.5" value={ex.power ?? ''}
              onChange={(e) => set({ power: e.target.value || undefined })}>
              <option value="">Power class…</option>
              {Object.entries(definition.powerMultipliers).map(([cls, mult]) => (
                <option key={cls} value={cls}>
                  {cls === 'QRP' ? 'QRP' : cls === 'LOW' ? 'Low' : cls === 'HIGH' ? 'High' : cls}
                  {mult !== 1 ? ` (×${mult})` : ''}
                </option>
              ))}
            </select>
          )}
          <div className="grid grid-cols-2 gap-2">
            {has('zone') && (
              <input type="text" placeholder="My CQ zone" value={ex.cqZone ?? ''} className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => set({ cqZone: parseInt(e.target.value) || undefined })} />
            )}
            {has('state') && !homeCounty && (
              <input type="text" placeholder="My state" value={ex.state ?? ''} className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => set({ state: e.target.value.toUpperCase() || undefined })} />
            )}
            {has('section') && (
              <input type="text" placeholder="My section" value={ex.section ?? ''} className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => set({ section: e.target.value.toUpperCase() || undefined })} />
            )}
            {has('name') && (
              <input type="text" placeholder="My name" value={ex.name ?? ''} className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => set({ name: e.target.value || undefined })} />
            )}
            {has('grid') && (
              <input type="text" placeholder="My grid" value={ex.grid ?? ''} className="glass-input text-sm px-2 py-1.5"
                onChange={(e) => set({ grid: e.target.value.toUpperCase() || undefined })} />
            )}
          </div>
          <div className="flex items-center justify-between">
            <span className={`text-xs ${status === 'Saved' ? 'text-emerald-400' : status ? 'text-red-400' : 'text-gray-600'}`}>
              {status ?? 'Applies to the whole session'}
            </span>
            <button onClick={save} disabled={saving}
              className="px-3 py-1 rounded text-xs bg-accent-primary/20 border border-accent-primary/50 text-accent-primary hover:bg-accent-primary/30 disabled:opacity-50">
              Save
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// A selectable list of every contest — uncheck to remove it from the picker,
// re-check to restore. The removal is persisted (hiddenContestIds) so it survives
// the built-in re-seed on every startup.
function ManageContestsModal({
  definitions, hidden, onHide, onShow, onSetHidden, onClose,
}: {
  definitions: ContestDefinition[];
  hidden: string[];
  onHide: (id: string) => void;
  onShow: (id: string) => void;
  onSetHidden: (ids: string[]) => void;
  onClose: () => void;
}) {
  const [q, setQ] = useState('');
  const list = useMemo(() => {
    const s = q.trim().toLowerCase();
    return definitions.filter((d) => !s || d.name.toLowerCase().includes(s));
  }, [definitions, q]);

  const allShown = hidden.length === 0;
  const allHidden = hidden.length >= definitions.length;

  return createPortal(
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4" onClick={onClose}>
      <div className="w-full max-w-md max-h-[85vh] flex flex-col rounded-xl bg-dark-800 border border-glass-100 shadow-xl"
        onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center justify-between px-4 py-3 border-b border-glass-100 shrink-0">
          <div className="text-sm font-medium text-gray-200">Manage contests</div>
          <button onClick={onClose} title="Close" className="text-gray-400 hover:text-gray-200"><X className="w-4 h-4" /></button>
        </div>
        <div className="p-3 border-b border-glass-100 shrink-0">
          <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search…"
            className="glass-input w-full text-sm px-2 py-1.5" />
          <div className="flex items-center gap-2 mt-2">
            <button
              onClick={() => onSetHidden([])}
              disabled={allShown}
              className="flex items-center gap-1 px-2 py-1 rounded-md text-xs bg-dark-700/50 border border-glass-100 text-gray-300 hover:bg-dark-600/50 disabled:opacity-40 disabled:cursor-default"
            >
              <Eye className="w-3.5 h-3.5" /> Enable all
            </button>
            <button
              onClick={() => onSetHidden(definitions.map((d) => d.id))}
              disabled={allHidden}
              className="flex items-center gap-1 px-2 py-1 rounded-md text-xs bg-dark-700/50 border border-glass-100 text-gray-300 hover:bg-dark-600/50 disabled:opacity-40 disabled:cursor-default"
            >
              <EyeOff className="w-3.5 h-3.5" /> Disable all
            </button>
            {hidden.length > 0 && (
              <span className="text-xs text-gray-500 ml-auto">{hidden.length} removed</span>
            )}
          </div>
          <div className="text-xs text-gray-500 mt-2">
            Uncheck to remove a contest from the picker; re-check to restore.
          </div>
        </div>
        <div className="flex-1 min-h-0 overflow-y-auto p-2 space-y-0.5">
          {list.map((d) => {
            const shown = !hidden.includes(d.id);
            return (
              <label key={d.id}
                className="flex items-center gap-2 px-2 py-1.5 rounded hover:bg-dark-700/50 cursor-pointer">
                <input type="checkbox" checked={shown}
                  onChange={() => (shown ? onHide(d.id) : onShow(d.id))} />
                {shown ? <Eye className="w-3.5 h-3.5 text-gray-500" /> : <EyeOff className="w-3.5 h-3.5 text-gray-600" />}
                <span className={`text-sm truncate ${shown ? 'text-gray-200' : 'text-gray-500 line-through'}`}>{d.name}</span>
                {!d.builtin && <span className="text-[10px] text-gray-500 ml-auto shrink-0">custom</span>}
              </label>
            );
          })}
          {list.length === 0 && <div className="text-center text-sm text-gray-500 py-6">No contests match</div>}
        </div>
      </div>
    </div>,
    document.body
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
  value, onChange, label, width, defId,
}: {
  value: string;
  onChange: (v: string) => void;
  label: string;
  width: number;
  defId: string | undefined;
}) {
  const [focused, setFocused] = useState(false);
  const v = value.trim().toUpperCase();
  const matches = useMemo(() => matchCounties(v, defId), [v, defId]);
  const hasCounties = countiesForContest(defId).length > 0;
  const official = hasOfficialCounties(defId);
  const badStateProv = v.length > 0 && v.length <= 2 && !isValidStateProv(v);
  // Unknown 3+ char code: an error for an official table, just a warning otherwise.
  const unknownCounty = v.length >= 3 && hasCounties && !isKnownCounty(v, defId);
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
            : unknownCounty ? (official ? 'Not a valid county code for this contest' : 'Unknown county code — check it')
            : undefined
        }
        className={`glass-input w-full font-mono text-lg px-2 py-2 uppercase ${
          badStateProv || (unknownCounty && official) ? 'border-red-500/70 text-red-400'
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

import { useState, useCallback, useEffect, useRef } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Send, Search, User, MapPin, NotebookPen, Link, Unlink, Clock, Lock, LockOpen, Loader2, X, ChevronDown, ExternalLink, Trees, Satellite, Swords, Radio as RadioIcon, Pencil, Megaphone, ArrowUp, ArrowDown, AlertTriangle } from 'lucide-react';
import { api, CreateQsoRequest, QsoResponse, SatState } from '../api/client';
import { formatDupeWarning, MIN_CALLSIGN_LENGTH } from '../utils/dupeWarning';
import { signalRService, setTciMetersCallback, clearTciMetersCallback, addSatStateCallback, removeSatStateCallback, type TciMetersEvent } from '../api/signalr';
import { S9_DBM, DB_PER_S_UNIT } from '../utils/smeter';
import { spotKhzToMhzString, spotKhzToHz, formMhzToStoredKhz, rigHzToStoredKhz } from '../utils/frequency';
import { useSignalR } from '../hooks/useSignalR';
import { useAppStore } from '../store/appStore';
import { activeLayoutRef, applyLayoutRef, layoutRefLabel, liveLayoutDiffersFromActive, saveLiveToActiveSlot } from '../layouts/applyLayout';
import { useSettingsStore } from '../store/settingsStore';
import { GlassPanel } from '../components/GlassPanel';
import { getCountryFlag } from '../core/countryFlags';
import { useRegisterShortcuts } from '../hooks/useRegisterShortcuts';
import type { Shortcut } from '../utils/shortcuts';

// Enter is the form's implicit submit; Esc is the onKeyDown handler on <form>.
const SHORTCUTS: Shortcut[] = [
  { keys: 'Enter', label: 'Log the QSO' },
  { keys: 'Esc', label: 'Clear the entry form' },
];

// v1.x-style log-entry mode switcher — General, POTA, and SAT are all
// fully wired now. General is the default free-form logger, POTA layers
// on my_pota_ref / pota_ref (P2P) tagging, and SAT auto-populates the
// satellite name + uplink/downlink freq+mode from the live S.A.T.
// controller state and writes ADIF-standard sat_name / prop_mode=SAT /
// freq_rx / down_mode so LoTW satellite credit survives ADIF export.
//
// Contest is intentionally NOT a mode here — it sits beside these tabs as a
// jump to the Contest Entry panel, which owns serials, dupe checking and
// scoring. Adding it to this union would persist an empty mode to localStorage.
type LogMode = 'general' | 'pota' | 'sat';

// Modes typical for satellite passes — SSB birds use USB, FM/CW birds
// their respective mode. Kept short so the SAT-panel dropdowns aren't a
// wall of digital modes that don't apply to a satellite QSO.
const SAT_MODES = ['USB', 'LSB', 'FM', 'CWU', 'CWL'];

// Helper to format date for input
const formatDateForInput = (date: Date): string => {
  return date.toISOString().slice(0, 10);
};

// Helper to format time for input (UTC)
const formatTimeForInput = (date: Date): string => {
  return date.toISOString().slice(11, 16);
};

const BANDS = ['160m', '80m', '40m', '30m', '20m', '17m', '15m', '12m', '10m', '6m', '2m', '1.25m', '70cm', '33cm', '23cm', '13cm', '9cm', '6cm', '3cm'];

// Mode list matches v1.x SDRLogger+ — USB/LSB are separate (so the rig
// actually gets USB or LSB, not a collapsed "SSB"), and DIGU/DIGL are the
// digital-passthrough entries that flrig / Hamlib / TCI translate to the
// rig's specific digital mode name (USB-D on Icom, DATA-U on Kenwood/Yaesu,
// etc.) via the per-rig ModeOut mapping in each service.
const MODES = [
  'USB', 'LSB',
  'CWU', 'CWL',
  'AM', 'SAM', 'FM', 'NFM',
  'DIGU', 'DIGL',
  'FT8', 'FT4', 'JS8', 'RTTY', 'PSK31', 'WSPR', 'JT65', 'JT9', 'DIGI',
];

// Grouped mode structure powering the <optgroup> layout on the Log Entry
// mode dropdown — matches the v1.x /templates/index.html grouping so
// operators moving between versions see the same organization.
const MODE_GROUPS: { label: string; modes: string[] }[] = [
  { label: 'SSB',            modes: ['USB', 'LSB'] },
  { label: 'CW',             modes: ['CWU', 'CWL'] },
  { label: 'AM / FM',        modes: ['AM', 'SAM', 'FM', 'NFM'] },
  { label: 'Digital (TCI)',  modes: ['DIGU', 'DIGL'] },
  { label: 'Digital (Log)',  modes: ['FT8', 'FT4', 'JS8', 'RTTY', 'PSK31', 'WSPR', 'JT65', 'JT9', 'DIGI'] },
];

// Treat CWU/CWL as "CW" for RST/RSTr defaulting and dB-enhancement checks
// that used to hard-code just `mode === 'CW'`.
const isCwMode = (m: string) => m === 'CW' || m === 'CWU' || m === 'CWL';

// Common RST values for phone modes (SSB, AM, FM)
const RST_PHONE = ['59', '58', '57', '56', '55', '54', '53', '52', '51'];
// Common RST values for CW and digital modes
const RST_CW_DIGITAL = ['599', '589', '579', '569', '559', '549', '539', '529', '519'];

// Combo auto RST-Rcvd (Lyra link): map a received-signal dBm to the "S" digit
// of an RST report (1..9). HF convention (S9 = -73 dBm, 6 dB/unit — the same
// scale the analog meter uses); clamped 1..9 (RST strength caps at 9, and
// anything over S9 reports "9"). R stays 5 and T stays 9 by convention — only
// S is measurable. VHF/UHF uses a -93 dBm S9; we keep the HF scale to match the
// meter, so a VHF report reads a few S-units low (operator can override).
const dbmToSDigit = (dbm: number): number =>
  Math.max(1, Math.min(9, Math.round((dbm - S9_DBM) / DB_PER_S_UNIT) + 9));
// Only auto-suggest the received S when the signal is clearly above the noise
// floor; below this the S-meter is just reading noise and would report a
// phantom S-unit. Lyra supplies the SNR (lyra_snr); this is the trust gate.
const AUTO_RST_SNR_GATE_DB = 3;

// Get default RST based on mode. CW-family and DIGI/RTTY use 3-digit RST;
// phone modes use 2-digit.
const getDefaultRst = (mode: string): string => {
  return isCwMode(mode) ? '599' : '59';
};

// CW doesn't use +dB enhancement
const supportsDbEnhancement = (mode: string): boolean => {
  return !isCwMode(mode);
};

function RstCombobox({ value, onChange, options, className }: {
  value: string;
  onChange: (v: string) => void;
  options: string[];
  className?: string;
}) {
  const [isOpen, setIsOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!isOpen) return;
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [isOpen]);

  return (
    <div ref={containerRef} className="relative">
      <div className="flex">
        <input
          type="text"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className={`${className} !rounded-r-none !border-r-0`}
        />
        <button
          type="button"
          onClick={() => setIsOpen(!isOpen)}
          className="glass-input !rounded-l-none !border-l-0 px-1 hover:bg-dark-600 flex items-center"
          tabIndex={-1}
        >
          <ChevronDown className={`w-3 h-3 text-dark-300 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
        </button>
      </div>
      {isOpen && (
        <div className="absolute z-50 top-full left-0 right-0 mt-0.5 bg-dark-700 border border-glass-200 rounded shadow-lg max-h-48 overflow-y-auto">
          {options.map(opt => (
            <button
              key={opt}
              type="button"
              onClick={() => { onChange(opt); setIsOpen(false); }}
              className={`w-full px-2 py-1 text-left text-sm font-mono hover:bg-dark-600 transition-colors ${
                opt === value ? 'text-accent-primary bg-dark-600/50' : 'text-gray-200'
              }`}
            >
              {opt}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

export function LogEntryPlugin() {
  useRegisterShortcuts('Log Entry', SHORTCUTS);

  const queryClient = useQueryClient();
  const { focusCallsign, persistCallsignMapImage, setRadioMode, tuneToBand, sendDxSpot } = useSignalR();
  const { focusedCallsignInfo, radioStates, selectedRadioId, isLookingUpCallsign, setFocusedCallsign, setFocusedCallsignInfo, setLogHistoryCallsignFilter, clearCallsignFromAllControls, selectedSpot, setSelectedSpot, addCallsignMapImage, comboLinked } = useAppStore();
  const { settings, updateRadioSettings } = useSettingsStore();
  const followRadio = settings.radio.followRadio;

  // Log-mode switcher — persists per session; General + POTA fully
  // wired here, SAT still placeholder until commit #3.
  const [logMode, setLogMode] = useState<LogMode>(() => {
    try {
      const saved = localStorage.getItem('sdrl_log_mode');
      if (saved === 'general' || saved === 'pota' || saved === 'sat') return saved;
    } catch { /* localStorage disabled — default fine */ }
    return 'general';
  });
  useEffect(() => {
    try { localStorage.setItem('sdrl_log_mode', logMode); } catch { /* no-op */ }
  }, [logMode]);

  // Per-mode "home" layout: when the operator switches modes and that mode has a
  // bound layout that isn't already on screen, offer to load it — always a prompt,
  // never a silent swap. `canSave` is only true when there are unsaved changes AND
  // the active layout is one of the operator's own slots (a read-only starter has
  // nothing of theirs to save — decision B).
  const modeLayouts = settings.modeLayouts;
  const [layoutPrompt, setLayoutPrompt] = useState<
    { mode: string; ref: string; canSave: boolean } | null
  >(null);
  const offerModeLayout = useCallback(async (mode: keyof typeof modeLayouts) => {
    const ref = modeLayouts?.[mode];
    if (!ref || activeLayoutRef() === ref) return;
    const dirty = await liveLayoutDiffersFromActive();
    const activeIsSaved = activeLayoutRef()?.startsWith('saved:') ?? false;
    setLayoutPrompt({ mode, ref, canSave: dirty && activeIsSaved });
  }, [modeLayouts]);

  // POTA "activating" park — the park YOU'RE at (my_pota_ref on the QSO).
  // Persisted across sessions so a multi-hour activation doesn't need
  // re-entry every reload.
  const [activatingPark, setActivatingPark] = useState<string>(() => {
    try { return localStorage.getItem('sdrl_activating_park') || ''; } catch { return ''; }
  });
  const [editingPark, setEditingPark] = useState(false);
  const [parkDraft, setParkDraft] = useState('');
  useEffect(() => {
    try { localStorage.setItem('sdrl_activating_park', activatingPark); } catch { /* no-op */ }
  }, [activatingPark]);

  const [formData, setFormData] = useState({
    callsign: '',
    band: '20m',
    mode: 'USB',
    rstSent: '59',
    rstSentPlus: '',
    rstRcvd: '59',
    rstRcvdPlus: '',
    frequency: '',
    name: '',
    qth: '',
    county: '',
    grid: '',
    contest: '',
    remarks: '',
    // POTA park-to-park — the WORKED station's park (pota_ref on the QSO).
    p2pPark: '',
    // SAT fields — auto-populate from S.A.T. controller when tracking,
    // editable otherwise. Freqs in MHz (v1.x + ADIF convention). satGrid
    // is the WORKED station's grid; separate from `grid` so the standard
    // grid field can still come from QRZ if we want.
    satellite: '',
    uplinkFreq: '',
    downlinkFreq: '',
    upMode: 'USB',
    downMode: 'USB',
    satGrid: '',
  });

  // Live S.A.T. controller state — poll (5 s) + SignalR push. Only enabled
  // when SAT integration is on in Settings so we don't hammer the backend
  // for operators who never use satellites.
  const satEnabled = settings.sat.enabled;
  const [liveSatState, setLiveSatState] = useState<SatState | null>(null);
  const { data: polledSatState } = useQuery({
    queryKey: ['sat-status'],
    queryFn: () => api.getSatStatus(),
    refetchInterval: 5000,
    // Poll whenever SAT is enabled (any log mode) — NOT only in SAT mode. The
    // auto-switch below must detect active↔inactive while you're still in General,
    // otherwise it could never switch you INTO SAT mode (chicken-and-egg).
    enabled: satEnabled,
  });
  const lastPushAtRef = useRef(0);
  useEffect(() => {
    // Subscribe whenever SAT is enabled, regardless of the current log mode, so the
    // auto-switch/rig-lock react to the controller from General mode too.
    if (!satEnabled) return;
    const cb = (s: SatState) => {
      lastPushAtRef.current = Date.now();
      setLiveSatState(s);
    };
    addSatStateCallback(cb);
    return () => removeSatStateCallback(cb);
  }, [satEnabled]);
  // Pushes win while they're flowing, but they must not win forever: the backend only
  // broadcasts on a state CHANGE, so a restarted (or reconnected) server never announces
  // the state it came up in, and the last push would otherwise shadow the poll for good.
  // Once pushes go quiet, let the poll take over.
  useEffect(() => {
    if (!polledSatState) return;
    if (Date.now() - lastPushAtRef.current > 7000) setLiveSatState(polledSatState);
  }, [polledSatState]);
  const satState = liveSatState ?? polledSatState ?? null;
  const satTracking = !!(satState?.active && satState?.satellite);
  // While the CSN S.A.T. controller is active it owns the radio — the backend
  // suppresses all outbound rig control (see LogHub.SatOwnsRig). Mirror that in the
  // UI: show a "rig control paused" indicator and disable the tune-the-rig dropdowns.
  const rigControlLocked = !!satState?.active;

  // Auto-switch the Log Entry to SAT mode when the S.A.T. controller goes active,
  // and back to General when it stops — a pass drops you straight into satellite
  // logging and returns you afterward. Edge-triggered (only on the active↔inactive
  // transition) so you can still change tabs manually during a pass if you want.
  const prevSatActiveRef = useRef(false);
  // Whether the current SAT tab was entered BY the auto-switch rather than by the
  // operator. Persisted, because the distinction has to survive a page reload — see
  // the reconcile below.
  const autoEnteredSatRef = useRef(
    (() => { try { return localStorage.getItem('sdrl_log_mode_auto') === '1'; } catch { return false; } })()
  );
  const markAutoEnteredSat = useCallback((auto: boolean) => {
    autoEnteredSatRef.current = auto;
    try { localStorage.setItem('sdrl_log_mode_auto', auto ? '1' : '0'); } catch { /* no-op */ }
  }, []);
  useEffect(() => {
    const active = !!satState?.active;
    if (active && !prevSatActiveRef.current) {
      setLogMode('sat');
      markAutoEnteredSat(true);
    } else if (!active && prevSatActiveRef.current) {
      setLogMode('general');
      markAutoEnteredSat(false);
    }
    prevSatActiveRef.current = active;
  }, [satState?.active, markAutoEnteredSat]);

  // Reconcile once, on mount. The auto-switch above is edge-triggered, so a page load
  // that restores a persisted 'sat' tab has no transition to correct it and would sit in
  // SAT mode with no pass running. Only undo what WE set — a deliberately chosen SAT tab
  // stays put.
  const satReconciledRef = useRef(false);
  useEffect(() => {
    if (satReconciledRef.current || !satEnabled || !satState) return;
    satReconciledRef.current = true;
    if (!satState.active && autoEnteredSatRef.current) {
      setLogMode((m) => (m === 'sat' ? 'general' : m));
      markAutoEnteredSat(false);
    }
  }, [satEnabled, satState, markAutoEnteredSat]);

  // Timestamp state - locked means it follows system time
  const [timeLocked, setTimeLocked] = useState(true);
  const [qsoDate, setQsoDate] = useState(() => formatDateForInput(new Date()));
  const [qsoTime, setQsoTime] = useState(() => formatTimeForInput(new Date()));
  const timeIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Name state - locked means it auto-fills from QRZ
  const [nameLocked, setNameLocked] = useState(true);

  // Combo auto RST-Rcvd (Stage B+): when linked to Lyra, suggest the received
  // "S" from the shared, calibrated S-meter — peak-held over the exchange and
  // SNR-gated so noise never reads as signal. The toggle persists; the field
  // re-arms to AUTO each new QSO and latches to MANUAL the instant the operator
  // edits it. Not offered in SAT mode (its reports are hand-entered).
  const [autoRstEnabled, setAutoRstEnabled] = useState<boolean>(() => {
    try { return localStorage.getItem('sdrl_auto_rst_s') === '1'; } catch { return false; }
  });
  useEffect(() => {
    try { localStorage.setItem('sdrl_auto_rst_s', autoRstEnabled ? '1' : '0'); } catch { /* no-op */ }
  }, [autoRstEnabled]);
  const [rstRcvdAuto, setRstRcvdAuto] = useState(true);  // field is AUTO vs operator-edited

  // Super Check Partial: the merged call set (master.scp ∪ your logged calls), fetched once
  // and matched locally as you type. Same source the Contest Entry uses. The worked set flags
  // + ranks calls you've worked before ahead of master-only calls.
  const [scpAll, setScpAll] = useState<string[]>([]);
  const [workedSet, setWorkedSet] = useState<Set<string>>(new Set());
  const [scpMatches, setScpMatches] = useState<{ call: string; worked: boolean }[]>([]);
  const [scpOpen, setScpOpen] = useState(false);
  const [scpIndex, setScpIndex] = useState(0);
  // Enter still logs the QSO by default; it only accepts a suggestion once the operator has
  // arrowed into the list (scpActive). Preserves the "type call, Enter = log" muscle memory.
  const [scpActive, setScpActive] = useState(false);
  // Last-worked band/date for a highlighted worked call — fetched on demand, deduped by ref.
  const [lastWorked, setLastWorked] = useState<Record<string, { band?: string; date?: string } | null>>({});
  const lwRequested = useRef<Set<string>>(new Set());
  useEffect(() => {
    api.getScpCalls().then(setScpAll).catch(() => setScpAll([]));
    api.getWorkedCalls().then(w => setWorkedSet(new Set(w))).catch(() => setWorkedSet(new Set()));
  }, []);

  // Call-history prefill: fill empty name/grid from the most recent QSO with this call.
  const applyCallHistory = useCallback(async (call: string) => {
    const c = call.trim().toUpperCase();
    if (c.length < 3) return;
    try {
      const qso = await api.getRecentByCallsign(c);
      if (!qso) return;
      const priorName = qso.station?.name ?? qso.name ?? '';
      const priorGrid = qso.station?.grid ?? qso.grid ?? '';
      const priorQth = qso.qth ?? '';
      const priorCounty = qso.station?.county ?? '';
      // Prefill EMPTY fields only — anything the operator has already typed (e.g. a rover's
      // new grid/QTH) always wins and is never overwritten.
      setFormData(prev => ({
        ...prev,
        name: prev.name || priorName,
        grid: prev.grid || priorGrid,
        qth: prev.qth || priorQth,
        county: prev.county || priorCounty,
      }));
    } catch { /* best-effort prefill */ }
  }, []);
  const peakRxDbmRef = useRef<number | null>(null);      // peak-hold of received dBm, this QSO

  // Update time every second when locked
  useEffect(() => {
    if (timeLocked) {
      const updateTime = () => {
        const now = new Date();
        setQsoDate(formatDateForInput(now));
        setQsoTime(formatTimeForInput(now));
      };
      updateTime();
      timeIntervalRef.current = setInterval(updateTime, 1000);
      return () => {
        if (timeIntervalRef.current) {
          clearInterval(timeIntervalRef.current);
        }
      };
    }
  }, [timeLocked]);

  // Get current radio state
  const currentRadioState = selectedRadioId ? radioStates.get(selectedRadioId) : null;

  // Auto-populate from radio state when followRadio is enabled. The form
  // DISPLAYS MHz (v1.x parity); it is converted to kHz on submit, which is
  // how Qso.Frequency is stored. Six decimals preserve sub-Hz rig precision.
  useEffect(() => {
    if (followRadio && currentRadioState) {
      const frequencyMhz = (currentRadioState.frequencyHz / 1_000_000).toFixed(6);
      setFormData(prev => ({
        ...prev,
        frequency: frequencyMhz,
        band: currentRadioState.band || prev.band,
        mode: normalizeMode(currentRadioState.mode) || prev.mode,
      }));
    }
  }, [followRadio, currentRadioState]);

  // Auto-populate SAT fields from the S.A.T. controller when we're in SAT
  // mode and a pass is being tracked. Empty values from the controller are
  // preserved as empty (operator can hand-fill); non-empty values overwrite
  // the form (matches v1.x behaviour where the SAT panel is the source of
  // truth during a pass). We only auto-fill when the operator hasn't
  // manually edited the satellite name for THIS pass (via a simple check
  // against the tracking satellite string).
  // Nominal (Doppler-free) transponder freqs in MHz, stashed while tracking so a
  // logged QSO records the clean transponder freq even though the fields below
  // follow the live Doppler-corrected value.
  const satNominalRef = useRef<{ up?: number; down?: number }>({});
  useEffect(() => {
    if (logMode !== 'sat' || !satState?.satellite) return;
    // The S.A.T. controller reports uplink/downlink in Hz (e.g. "432150000"),
    // but the SAT form fields and the submit path are MHz. Convert defensively:
    // a ham freq value ≥ 100k is Hz, anything smaller is already MHz.
    const toMhz = (raw?: string | null): number | null => {
      const n = parseFloat(raw ?? '');
      if (Number.isNaN(n) || n <= 0) return null;
      return n >= 100_000 ? n / 1_000_000 : n;
    };
    const fmt = (mhz: number) => String(parseFloat(mhz.toFixed(6)));
    // Display follows the live Doppler-corrected freq (matches the controller);
    // logging uses the nominal transponder freq (stashed in satNominalRef).
    const upLive = toMhz(satState.uplinkFreqLive ?? satState.uplinkFreq);
    const dnLive = toMhz(satState.downlinkFreqLive ?? satState.downlinkFreq);
    satNominalRef.current = {
      up: toMhz(satState.uplinkFreq) ?? undefined,
      down: toMhz(satState.downlinkFreq) ?? undefined,
    };
    setFormData(prev => {
      const next = {
        ...prev,
        satellite: satState.satellite ?? prev.satellite,
        uplinkFreq: upLive != null ? fmt(upLive) : prev.uplinkFreq,
        downlinkFreq: dnLive != null ? fmt(dnLive) : prev.downlinkFreq,
        upMode: satState.uplinkMode ?? prev.upMode,
        downMode: satState.downlinkMode ?? prev.downMode,
      };
      // The QSO's primary Freq / Band / Mode follow the UPLINK (TX) leg — ADIF
      // FREQ = transmit — so a contact logged straight from the controller is
      // self-consistent with no rig tuned to the pass. The downlink stays in its
      // own fields (freq_rx / down_mode) server-side.
      if (upLive != null) {
        next.frequency = fmt(upLive);
        const b = getBandFromFrequency(upLive * 1_000_000);
        if (b) next.band = b;
      }
      if (satState.uplinkMode) next.mode = normalizeMode(satState.uplinkMode);
      return next;
    });
  }, [logMode, satState?.satellite, satState?.uplinkFreq, satState?.downlinkFreq, satState?.uplinkFreqLive, satState?.downlinkFreqLive, satState?.uplinkMode, satState?.downlinkMode]);

  // Auto-populate name from QRZ when nameLocked is true
  useEffect(() => {
    if (nameLocked && focusedCallsignInfo?.name) {
      setFormData(prev => ({
        ...prev,
        name: focusedCallsignInfo.name || '',
      }));
    }
  }, [nameLocked, focusedCallsignInfo?.name]);

  // Auto-populate QTH ("City, State") and grid from the callbook. Unlike the
  // name field there's no lock toggle here, so this is keyed on the looked-up
  // callsign: it fires once when a lookup lands and never again for that call,
  // leaving any manual correction the operator types afterwards intact.
  //
  // The submit path already falls back to focusedCallsignInfo.grid, so this
  // doesn't change what gets logged — it makes the value visible in the form so
  // it can be checked or corrected before the QSO goes in.
  const filledFor = useRef<string | null>(null);
  useEffect(() => {
    const info = focusedCallsignInfo;
    if (!info?.callsign) { filledFor.current = null; return; }
    if (filledFor.current === info.callsign) return;
    const qth = [info.city, info.state].filter(Boolean).join(', ');
    const grid = (info.grid ?? '').toUpperCase();
    const county = info.county ?? '';
    if (!qth && !grid && !county) return;
    filledFor.current = info.callsign;
    setFormData(prev => ({
      ...prev,
      ...(qth ? { qth } : {}),
      ...(grid ? { grid } : {}),
      ...(county ? { county } : {}),
    }));
  }, [focusedCallsignInfo]);

  // Auto-populate from DX cluster spot selection. selectedSpot.frequency
  // arrives in kHz (the app-wide spot unit — see utils/frequency.ts); we
  // display MHz throughout the form (v1.x parity) and band-match in Hz.
  useEffect(() => {
    if (selectedSpot) {
      const frequencyMhz = spotKhzToMhzString(selectedSpot.frequency);
      const band = getBandFromFrequency(spotKhzToHz(selectedSpot.frequency));
      const mode = selectedSpot.mode ? normalizeMode(selectedSpot.mode) : formData.mode;

      setFormData(prev => ({
        ...prev,
        callsign: selectedSpot.dxCall,
        frequency: frequencyMhz,
        band: band || prev.band,
        mode: mode,
      }));

      // Clear the selected spot after processing to allow re-selection of same spot
      setSelectedSpot(null);
    }
  }, [selectedSpot, setSelectedSpot]);

  // Update RST defaults when mode changes
  useEffect(() => {
    // eslint-disable-next-line react-hooks/exhaustive-deps
    const defaultRst = getDefaultRst(formData.mode);
    setFormData(prev => ({
      ...prev,
      rstSent: defaultRst,
      rstRcvd: defaultRst,
    }));
  }, [formData.mode]);



  // Helper to determine band from frequency in Hz
  const getBandFromFrequency = (freqHz: number): string | null => {
    const freqKhz = freqHz / 1000;
    if (freqKhz >= 1800 && freqKhz <= 2000) return '160m';
    if (freqKhz >= 3500 && freqKhz <= 4000) return '80m';
    if (freqKhz >= 7000 && freqKhz <= 7300) return '40m';
    if (freqKhz >= 10100 && freqKhz <= 10150) return '30m';
    if (freqKhz >= 14000 && freqKhz <= 14350) return '20m';
    if (freqKhz >= 18068 && freqKhz <= 18168) return '17m';
    if (freqKhz >= 21000 && freqKhz <= 21450) return '15m';
    if (freqKhz >= 24890 && freqKhz <= 24990) return '12m';
    if (freqKhz >= 28000 && freqKhz <= 29700) return '10m';
    if (freqKhz >= 50000 && freqKhz <= 54000) return '6m';
    if (freqKhz >= 144000 && freqKhz <= 148000) return '2m';
    if (freqKhz >= 420000 && freqKhz <= 450000) return '70cm';
    return null;
  };

  // Normalize mode names from radio to match our MODES list. Preserves the
  // USB/LSB distinction (v1.x behavior) so the rig actually gets back USB
  // vs LSB and not a collapsed "SSB", and maps the various rig-specific
  // digital passthrough names (DATA-U, PKT-U, USB-D, USBD) → DIGU / DIGL.
  const normalizeMode = (mode: string): string => {
    const upperMode = mode?.toUpperCase() || '';
    // Exact match to our list first (covers USB, LSB, CWU, CWL, FT8, ...).
    if (MODES.includes(upperMode)) return upperMode;
    // Digital passthrough — rig-specific names → DIGU / DIGL.
    if (['DATA-U', 'PKT-U', 'USB-D', 'USBD'].includes(upperMode)) return 'DIGU';
    if (['DATA-L', 'PKT-L', 'LSB-D', 'LSBD'].includes(upperMode)) return 'DIGL';
    // CW-R / CWR from flrig → CWL (lower sideband CW).
    if (upperMode === 'CW-R' || upperMode === 'CWR') return 'CWL';
    // Bare "CW" from a rig with no side distinction → CWU as a sane default.
    if (upperMode === 'CW') return 'CWU';
    // Bare "SSB" — no side info; leave existing choice or default to USB.
    if (upperMode === 'SSB') return 'USB';
    // FSK variants → RTTY.
    if (upperMode.includes('RTTY') || upperMode.includes('FSK')) return 'RTTY';
    // Wildcard matches for FT8 / FT4 / JS8 / WSPR / JT65 / JT9 / PSK.
    if (upperMode.includes('FT8')) return 'FT8';
    if (upperMode.includes('FT4')) return 'FT4';
    if (upperMode.includes('JS8')) return 'JS8';
    if (upperMode.includes('WSPR')) return 'WSPR';
    if (upperMode.includes('JT65')) return 'JT65';
    if (upperMode.includes('JT9'))  return 'JT9';
    if (upperMode.includes('PSK'))  return 'PSK31';
    // AM/FM/NFM/SAM already handled by exact match; fall through here means
    // some obscure rig mode we don't have a bucket for — default USB.
    return 'USB';
  };

  // Get RST options based on mode. CW-family gets 3-digit; digital modes
  // that use dB reports (FT8/FT4/JS8/WSPR/JT65/JT9) still fall under
  // supportsDbEnhancement elsewhere; the phone-vs-CW split here is for the
  // basic RST-sent/received dropdown defaults.
  const getRstOptions = (mode: string): string[] => {
    return isCwMode(mode) ? RST_CW_DIGITAL : RST_PHONE;
  };

  const toggleFollowRadio = () => {
    updateRadioSettings({ followRadio: !followRadio });
  };

  const createQso = useMutation({
    mutationFn: (data: CreateQsoRequest) => api.createQso(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['qsos'] });
      queryClient.invalidateQueries({ queryKey: ['statistics'] });
      // Save focused callsign to map overlay BEFORE clearing (only logged QSOs persist on map)
      const info = useAppStore.getState().focusedCallsignInfo;
      if (info?.latitude != null && info?.longitude != null) {
        const mapImage = {
          callsign: info.callsign,
          imageUrl: info.imageUrl ?? undefined,
          latitude: info.latitude,
          longitude: info.longitude,
          name: info.name ?? undefined,
          country: info.country ?? undefined,
          grid: info.grid ?? undefined,
          savedAt: new Date().toISOString(),
        };
        addCallsignMapImage(mapImage);
        persistCallsignMapImage(mapImage).catch(() => {});
      }
      // Clear callsign from all controls (QRZ profile, rotator, log history filter, etc.)
      clearCallsignFromAllControls();
      // Clear form. Note we intentionally KEEP activatingPark (it's a
      // session-level setting, not per-QSO) but clear p2pPark since
      // that's specific to the QSO we just logged.
      setFormData({
        ...formData,
        callsign: '',
        name: '',
        qth: '',
        county: '',
        grid: '',
        contest: '',
        remarks: '',
        frequency: '',
        rstSentPlus: '',
        rstRcvdPlus: '',
        p2pPark: '',
        // Clear per-QSO SAT worked-station fields but KEEP the satellite
        // name + freq/mode — they belong to the pass, not the QSO, and
        // the auto-fill effect will re-apply them from the S.A.T.
        // controller anyway.
        satGrid: '',
      });
    },
  });

  const handleCallsignChange = useCallback(async (value: string) => {
    const callsign = value.toUpperCase();
    setFormData(prev => ({ ...prev, callsign }));

    // Update log history filter to show matching entries
    setLogHistoryCallsignFilter(callsign.length > 0 ? callsign : null);

    // Super Check Partial: local match, ranked worked-before first, then prefix before contains.
    if (callsign.length >= 2 && scpAll.length > 0) {
      const wp: { call: string; worked: boolean }[] = [], wc: { call: string; worked: boolean }[] = [];
      const mp: { call: string; worked: boolean }[] = [], mc: { call: string; worked: boolean }[] = [];
      for (const c of scpAll) {
        if (c === callsign) continue;
        const worked = workedSet.has(c);
        if (c.startsWith(callsign)) {
          if (worked) { if (wp.length < 8) wp.push({ call: c, worked }); }
          else if (mp.length < 8) mp.push({ call: c, worked });
        } else if (c.includes(callsign)) {
          if (worked) { if (wc.length < 8) wc.push({ call: c, worked }); }
          else if (mc.length < 8) mc.push({ call: c, worked });
        }
        if (wp.length >= 8 && wc.length >= 8 && mp.length >= 8 && mc.length >= 8) break;
      }
      const m = [...wp, ...wc, ...mp, ...mc].slice(0, 8);
      setScpMatches(m);
      setScpOpen(m.length > 0);
      setScpIndex(0);
      setScpActive(false);   // fresh typing → Enter still logs until the operator arrows in
    } else {
      setScpMatches([]);
      setScpOpen(false);
    }

    if (callsign.length >= 3) {
      await focusCallsign(callsign, 'log-entry');
    } else {
      // Clear the focused callsign info when callsign is cleared or too short
      setFocusedCallsign(null);
      setFocusedCallsignInfo(null);
    }
  }, [focusCallsign, setFocusedCallsign, setFocusedCallsignInfo, setLogHistoryCallsignFilter, scpAll]);

  // Accept an SCP suggestion: complete the call, close the list, run callbook + call-history prefill.
  const acceptSuggestion = useCallback((call: string) => {
    setScpOpen(false);
    setScpMatches([]);
    setFormData(prev => ({ ...prev, callsign: call }));
    setLogHistoryCallsignFilter(call);
    focusCallsign(call, 'log-entry');
    applyCallHistory(call);
  }, [focusCallsign, setLogHistoryCallsignFilter, applyCallHistory]);

  // Fetch last-worked band/date for the highlighted worked call (once each) for the dropdown hint.
  useEffect(() => {
    const m = scpMatches[scpIndex];
    if (!scpOpen || !m || !m.worked || lwRequested.current.has(m.call)) return;
    lwRequested.current.add(m.call);
    let cancelled = false;
    api.getRecentByCallsign(m.call).then(qso => {
      if (cancelled) return;
      const date = qso?.qsoDate
        ? new Date(qso.qsoDate).toLocaleDateString(undefined, { month: 'short', year: '2-digit' })
        : undefined;
      setLastWorked(prev => ({ ...prev, [m.call]: qso ? { band: qso.band, date } : null }));
    }).catch(() => { lwRequested.current.delete(m.call); });
    return () => { cancelled = true; };
  }, [scpIndex, scpMatches, scpOpen]);

  const handleClear = useCallback(() => {
    setFormData({
      callsign: '',
      band: formData.band,
      mode: formData.mode,
      rstSent: '59',
      rstSentPlus: '',
      rstRcvd: '59',
      rstRcvdPlus: '',
      frequency: followRadio && currentRadioState ? formData.frequency : '',
      name: '',
      qth: '',
      county: '',
      grid: '',
      contest: '',
      remarks: '',
      p2pPark: '',
      // handleClear is the Escape/Clear-button path — a full reset.
      // Wipe SAT fields too; the auto-fill effect will re-populate them
      // from the S.A.T. controller state on the next tick if a pass is
      // still active.
      satellite: '',
      uplinkFreq: '',
      downlinkFreq: '',
      upMode: 'USB',
      downMode: 'USB',
      satGrid: '',
    });
    setTimeLocked(true);
    setNameLocked(true);
    clearCallsignFromAllControls();
  }, [formData.band, formData.mode, formData.frequency, followRadio, currentRadioState, clearCallsignFromAllControls]);

  // Build + submit the current QSO. `overrides` lets the Combo {LOG} path
  // (Stage B) stamp the on-air RST / mode / frequency Lyra reports at macro
  // send time, while the populated form supplies call / name / grid / etc.
  // Called with no args by the "Log QSO" button (handleSubmit).
  const submitQso = (overrides?: {
    rstSent?: string | null;
    rstRcvd?: string | null;
    mode?: string | null;
    frequencyHz?: number | null;
  }) => {
    if (!formData.callsign) return;

    // Format RST with plus values if present (form path); the Combo path
    // passes explicit sent/rcvd strings that win when provided.
    const rstSent = overrides?.rstSent
      ?? (formData.rstSentPlus ? `${formData.rstSent}+${formData.rstSentPlus}` : formData.rstSent);
    const rstRcvd = overrides?.rstRcvd
      ?? (formData.rstRcvdPlus ? `${formData.rstRcvd}+${formData.rstRcvdPlus}` : formData.rstRcvd);
    // Combo sends Lyra's app mode (CWU/CWL/USB/…); normalizeMode maps it into
    // our canonical set. Absent override → the form's mode (already synced).
    const mode = overrides?.mode ? normalizeMode(overrides.mode) : formData.mode;
    // Combo carries Hz and the form displays MHz (v1.x parity), but backend
    // Qso.Frequency is stored in kHz — ADIF export divides by 1000 to get MHz.
    // Convert at this boundary only; the form stays MHz.
    const frequency = (overrides?.frequencyHz && overrides.frequencyHz > 0)
      ? rigHzToStoredKhz(overrides.frequencyHz)
      : (formData.frequency ? formMhzToStoredKhz(parseFloat(formData.frequency)) : undefined);

    // Use the timestamp from state (either live or manual)
    const qsoDateTime = new Date(`${qsoDate}T${qsoTime}:00.000Z`);
    createQso.mutate({
      callsign: formData.callsign,
      qsoDate: qsoDateTime.toISOString(),
      timeOn: qsoTime.replace(':', '') + '00',
      band: formData.band,
      mode,
      frequency,
      rstSent,
      rstRcvd,
      name: formData.name || focusedCallsignInfo?.name,
      qth: formData.qth || undefined,
      // State rides from the lookup like country does; county prefers the
      // form value so an operator correction wins over the callbook.
      state: focusedCallsignInfo?.state,
      county: formData.county || focusedCallsignInfo?.county,
      // In SAT mode the operator hand-enters the worked station's grid
      // (satGrid); otherwise fall back to the general grid field then QRZ.
      grid: (logMode === 'sat' && formData.satGrid)
        ? formData.satGrid
        : (formData.grid || focusedCallsignInfo?.grid),
      country: focusedCallsignInfo?.country,
      contest: formData.contest || undefined,
      // v1.x has a single "Remarks" field; the backend still has both
      // comment + notes columns. Map Remarks → comment (the primary,
      // ADIF-exported field). Notes stays available for the POTA/SAT
      // commits or a future secondary-notes field if we want one.
      comment: formData.remarks || undefined,
      // POTA — only send the park fields when actually in POTA mode
      // so a General QSO doesn't accidentally get tagged with a
      // leftover activatingPark value from a prior session.
      myPotaRef: logMode === 'pota' && activatingPark ? activatingPark : undefined,
      potaRef: logMode === 'pota' && formData.p2pPark ? formData.p2pPark : undefined,
      // SAT — only send SAT fields when actually in SAT mode. Uplink
      // freq/mode become the QSO's top-level Frequency/Mode server-side;
      // satellite name + downlink freq/mode land in AdifExtra. satGrid
      // (worked station's grid) rides on the standard `grid` field
      // above — no separate wire field needed.
      satellite: logMode === 'sat' && formData.satellite ? formData.satellite : undefined,
      // While tracking, log the NOMINAL transponder freq (the fields show the
      // live Doppler-corrected value, which we don't want in the log). When not
      // tracking, log whatever the operator hand-entered in the field.
      uplinkFreq: logMode === 'sat'
        ? (satTracking && satNominalRef.current.up != null
            ? satNominalRef.current.up
            : (formData.uplinkFreq ? parseFloat(formData.uplinkFreq) : undefined))
        : undefined,
      downlinkFreq: logMode === 'sat'
        ? (satTracking && satNominalRef.current.down != null
            ? satNominalRef.current.down
            : (formData.downlinkFreq ? parseFloat(formData.downlinkFreq) : undefined))
        : undefined,
      upMode: logMode === 'sat' && formData.upMode ? formData.upMode : undefined,
      downMode: logMode === 'sat' && formData.downMode ? formData.downMode : undefined,
    });
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    submitQso();
  };

  // Stage B: a {LOG}-tagged Lyra CW macro logs the current QSO. Register a
  // stable handler ONCE that calls the latest submitQso via a ref, so it never
  // captures a stale formData snapshot. setHandlers merges, so this leaves the
  // other combo / SAT handlers untouched. No-ops if the form has no callsign.
  const submitQsoRef = useRef(submitQso);
  submitQsoRef.current = submitQso;
  useEffect(() => {
    signalRService.setHandlers({
      onComboLogRequested: (evt) => submitQsoRef.current({
        rstSent: evt.rstSent,
        rstRcvd: evt.rstRcvd,
        mode: evt.mode,
        frequencyHz: evt.frequencyHz,
      }),
    });
    return () => signalRService.setHandlers({ onComboLogRequested: undefined });
  }, []);

  // Combo auto RST-Rcvd — drive the "S" digit from the live S-meter stream.
  // Registered ONCE; the ref always holds the latest closure so it reads
  // current formData/flags without re-subscribing on every (≤12 Hz) meter tick.
  // Uses a SET-backed callback (see signalr.ts) so it coexists with the Meter
  // panel's own subscription.
  const onMeterForRstRef = useRef<(evt: TciMetersEvent) => void>(() => {});
  onMeterForRstRef.current = (evt: TciMetersEvent) => {
    if (!autoRstEnabled || !comboLinked || logMode === 'sat' || !rstRcvdAuto) return;
    if (!formData.callsign) return;              // nothing being worked yet
    if (evt.isTransmitting) return;              // only sample while receiving them
    const dbm = evt.rxSignalDbm;
    const snr = evt.rxSnrDb;
    if (dbm == null || snr == null || snr < AUTO_RST_SNR_GATE_DB) return;
    // Peak-hold: only climbs, so the field settles on their strongest reading
    // over the exchange and doesn't flicker with QSB.
    if (peakRxDbmRef.current !== null && dbm <= peakRxDbmRef.current) return;
    peakRxDbmRef.current = dbm;
    const s = dbmToSDigit(dbm);
    const rst = isCwMode(formData.mode) ? `5${s}9` : `5${s}`;
    // At/over S9 the S digit caps at 9, so report the excess in the "+dB over S9"
    // field (nearest 5 dB) — the same field the operator can type into. Only for
    // modes that use it (same gate as the field's visibility), and cleared below
    // S9. Rides the peak-hold above, so it settles on the strongest reading of the
    // exchange just like the S digit.
    const overDb = Math.round((dbm - S9_DBM) / 5) * 5;
    const plus = (supportsDbEnhancement(formData.mode) && s === 9 && overDb >= 5)
      ? String(overDb)
      : '';
    setFormData(prev =>
      (prev.rstRcvd === rst && prev.rstRcvdPlus === plus)
        ? prev
        : { ...prev, rstRcvd: rst, rstRcvdPlus: plus });
  };
  useEffect(() => {
    const handler = (evt: TciMetersEvent) => onMeterForRstRef.current(evt);
    setTciMetersCallback(handler);
    return () => clearTciMetersCallback(handler);
  }, []);

  // New QSO (call changed or cleared) → reset the peak-hold and re-arm AUTO, so
  // each contact starts fresh and a prior manual override never sticks to the
  // next station.
  useEffect(() => {
    peakRxDbmRef.current = null;
    setRstRcvdAuto(true);
  }, [formData.callsign]);

  // Probable-dupe check (U-4): same call + band + mode logged within the
  // backend's 30-min window → advisory warning strip. Never blocks logging.
  // Debounced per keystroke; the sequence counter discards out-of-order
  // responses (same pattern as the callsign-info card). Logging a QSO clears
  // the callsign, which clears the warning through this same effect.
  const [dupe, setDupe] = useState<QsoResponse | null>(null);
  const dupeSeq = useRef(0);
  useEffect(() => {
    const call = formData.callsign.trim();
    if (call.length < MIN_CALLSIGN_LENGTH) {
      setDupe(null);
      return;
    }
    const seq = ++dupeSeq.current;
    const timer = setTimeout(async () => {
      try {
        const result = await api.checkQsoDupe(call, formData.band, formData.mode);
        if (dupeSeq.current === seq) setDupe(result);
      } catch {
        // Best-effort advisory — an unreachable backend must not disturb entry.
        if (dupeSeq.current === seq) setDupe(null);
      }
    }, 400);
    return () => clearTimeout(timer);
  }, [formData.callsign, formData.band, formData.mode]);

  // v1.x-style mode switcher tabs. Each mode has its own accent color
  // matching v1.x: General=cyan, POTA=green, SAT=goldish yellow.
  // POTA + SAT are clickable placeholders — the tab switches but the
  // fields don't change yet (bespoke fields + separate-database
  // routing + S.A.T. controller sync land in follow-up commits).
  const MODE_STYLES: Record<LogMode, { active: string; inactiveHover: string }> = {
    general: {
      active: 'border-cyan-400 text-cyan-300 bg-cyan-500/10',
      inactiveHover: 'hover:text-cyan-300/70',
    },
    pota: {
      active: 'border-green-400 text-green-300 bg-green-500/10',
      inactiveHover: 'hover:text-green-300/70',
    },
    sat: {
      active: 'border-amber-400 text-amber-300 bg-amber-500/10',
      inactiveHover: 'hover:text-amber-300/70',
    },
  };

  const ModeTab = ({ id, label, icon, disabledTitle }: { id: LogMode; label: string; icon: React.ReactNode; disabledTitle?: string }) => {
    const style = MODE_STYLES[id];
    const isActive = logMode === id;
    return (
      <button
        type="button"
        onClick={() => { setLogMode(id); markAutoEnteredSat(false); void offerModeLayout(id); }}
        title={disabledTitle}
        className={`flex items-center gap-1.5 px-3 py-1.5 rounded-t border-b-2 font-ui text-xs font-semibold transition-colors ${
          isActive
            ? style.active
            : `border-transparent text-dark-300 ${style.inactiveHover} hover:bg-dark-700/20`
        }`}
      >
        {icon}
        <span>{label}</span>
        {disabledTitle && (
          <span className="ml-1 px-1 rounded bg-dark-600 text-[9px] text-dark-300 tracking-wider">soon</span>
        )}
      </button>
    );
  };

  const closeLayoutPrompt = () => setLayoutPrompt(null);
  const applyPromptLayout = async (save: boolean) => {
    const p = layoutPrompt;
    setLayoutPrompt(null);
    if (!p) return;
    if (save) { try { await saveLiveToActiveSlot(); } catch { /* best effort */ } }
    await applyLayoutRef(p.ref);
  };

  return (
    <>
    {layoutPrompt && (
      <div className="fixed inset-0 z-[200] flex items-center justify-center bg-black/60" onClick={closeLayoutPrompt}>
        <div className="glass-panel border border-glass-200 rounded-xl p-5 w-full max-w-sm mx-4" onClick={(e) => e.stopPropagation()}>
          <h3 className="text-sm font-semibold font-ui text-dark-100 mb-1">
            Switch to your “{layoutRefLabel(layoutPrompt.ref)}” layout?
          </h3>
          <p className="text-xs text-dark-300 mb-4">
            {layoutPrompt.canSave
              ? 'You have unsaved changes to the current layout. Save them first, or discard them and switch.'
              : 'This replaces your current panel arrangement.'}
          </p>
          <div className="flex items-center justify-end gap-2">
            <button
              onClick={closeLayoutPrompt}
              className="px-3 py-1.5 rounded text-xs font-ui border border-glass-100 text-dark-300 hover:bg-dark-600/50 transition-colors"
            >
              Stay
            </button>
            {layoutPrompt.canSave && (
              <button
                onClick={() => void applyPromptLayout(true)}
                className="px-3 py-1.5 rounded text-xs font-ui border border-accent-success/40 text-accent-success hover:bg-accent-success/10 transition-colors"
              >
                Save &amp; switch
              </button>
            )}
            <button
              onClick={() => void applyPromptLayout(false)}
              className="px-3 py-1.5 rounded text-xs font-ui border border-accent-primary/40 text-accent-primary hover:bg-accent-primary/10 transition-colors"
            >
              {layoutPrompt.canSave ? 'Discard & switch' : 'Switch'}
            </button>
          </div>
        </div>
      </div>
    )}
    <GlassPanel
      title="Log Entry"
      icon={<NotebookPen className="w-5 h-5" />}
      actions={
        <div className="flex items-center gap-2">
          {/* Lyra ↔ SDRLogger+ Combo link indicator (read-only; Lyra owns the
              toggle). Shows only while a Combo-enabled Lyra is connected. */}
          {comboLinked && (
            <span
              className="inline-flex items-center gap-1 px-2 py-1 rounded text-[10px] font-ui bg-accent-primary/15 border border-accent-primary/30 text-accent-primary"
              title="Lyra Combo linked — grabbed CW callsigns populate here; a {LOG} macro logs the QSO"
            >
              <span className="w-1.5 h-1.5 rounded-full bg-accent-primary" />
              Lyra Combo
            </span>
          )}
          {rigControlLocked ? (
            <span
              className="flex items-center gap-1.5 px-2 py-1 text-xs font-ui rounded bg-accent-primary/20 text-accent-primary border border-accent-primary/40"
              title="The CSN S.A.T. controller is driving the radio. SDRLogger+ rig control is paused until tracking stops."
            >
              <span>🛰</span>
              <span>S.A.T. controlling radio — rig control paused</span>
            </span>
          ) : (
            <button
              type="button"
              onClick={toggleFollowRadio}
              className={`flex items-center gap-1.5 px-2 py-1 text-xs font-ui rounded transition-all ${
                followRadio
                  ? 'bg-accent-success/20 text-accent-success hover:bg-accent-success/30'
                  : 'bg-dark-600 text-dark-300 hover:bg-dark-500'
              }`}
              title={followRadio ? 'Following radio frequency' : 'Not following radio'}
            >
              {followRadio ? (
                <>
                  <Link className="w-3.5 h-3.5" />
                  <span>Following</span>
                </>
              ) : (
                <>
                  <Unlink className="w-3.5 h-3.5" />
                  <span>Manual</span>
                </>
              )}
            </button>
          )}
        </div>
      }
    >
      {/* Mode switcher row — "Log Mode:" label + General / POTA / SAT
          tabs. Label makes the tabs' purpose obvious for a new operator
          who hasn't seen v1.x's General/POTA/SAT split before. */}
      <div className="flex items-center gap-2 px-3 pt-2 border-b border-glass-100">
        <span className="text-[10px] font-ui text-dark-300 font-semibold tracking-wider uppercase pr-1">
          Log&nbsp;Mode
        </span>
        <ModeTab id="general" label="General" icon={<RadioIcon className="w-3.5 h-3.5" />} />
        <ModeTab id="pota"    label="POTA"    icon={<Trees className="w-3.5 h-3.5" />} />
        <ModeTab id="sat"     label="SAT"     icon={<Satellite className="w-3.5 h-3.5" />} />
        {/* Contest is a jump, not a log mode: contest QSOs need serials, dupe
            checking and live scoring, which all live in the Contest Entry panel.
            Deliberately does NOT set logMode — otherwise the empty "contest"
            mode would persist to localStorage and reopen on restart. */}
        <button
          type="button"
          onClick={() => { window.dispatchEvent(new CustomEvent('open-panel', { detail: 'contest-entry' })); void offerModeLayout('contest'); }}
          title="Open the Contest Entry panel — contest QSOs are logged there, with serials, dupe checking and scoring"
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-t border-b-2 border-transparent font-ui text-xs font-semibold text-dark-300 transition-colors hover:text-accent-primary hover:bg-dark-700/20"
        >
          <Swords className="w-3.5 h-3.5" />
          <span>Contest</span>
          <ExternalLink className="w-2.5 h-2.5 opacity-60" />
        </button>
      </div>

      <form onSubmit={handleSubmit} onKeyDown={(e) => { if (e.key === 'Escape') { e.preventDefault(); handleClear(); } }} className="p-3 space-y-3">
        {/* POTA — ACTIVATING chip. Shows the park the operator is
            currently activating; persists across reloads. Empty state
            surfaces an "Add park ref" prompt so the intent is obvious
            for someone new to POTA mode. Spot Myself is a placeholder
            for the POTA-spot API integration (needs a POTA account
            token — deferred). */}
        {logMode === 'pota' && (
          <div className="flex items-center gap-2 p-2 rounded-lg bg-green-500/10 border border-green-500/30">
            <Trees className="w-4 h-4 text-green-400 flex-shrink-0" />
            {editingPark ? (
              <>
                <input
                  type="text"
                  autoFocus
                  value={parkDraft}
                  onChange={(e) => setParkDraft(e.target.value.toUpperCase())}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') { e.preventDefault(); setActivatingPark(parkDraft.trim()); setEditingPark(false); }
                    if (e.key === 'Escape') { e.preventDefault(); setEditingPark(false); }
                  }}
                  placeholder="K-1234"
                  className="glass-input flex-1 font-mono text-sm py-1"
                />
                <button
                  type="button"
                  onClick={() => { setActivatingPark(parkDraft.trim()); setEditingPark(false); }}
                  className="px-2 py-1 rounded bg-green-500/20 border border-green-500/40 text-green-300 text-xs font-ui hover:bg-green-500/30"
                  tabIndex={-1}
                >
                  Set
                </button>
                <button
                  type="button"
                  onClick={() => setEditingPark(false)}
                  className="px-2 py-1 rounded bg-dark-600 border border-dark-500 text-dark-300 text-xs font-ui hover:bg-dark-500"
                  tabIndex={-1}
                >
                  Cancel
                </button>
              </>
            ) : activatingPark ? (
              <>
                <span className="text-[10px] font-ui text-green-400 tracking-wider uppercase">Activating</span>
                <span className="font-mono font-bold text-green-300 text-sm">{activatingPark}</span>
                <button
                  type="button"
                  onClick={() => { setParkDraft(activatingPark); setEditingPark(true); }}
                  className="ml-1 p-1 rounded text-green-300/70 hover:text-green-300 hover:bg-green-500/10"
                  title="Edit park reference"
                  tabIndex={-1}
                >
                  <Pencil className="w-3 h-3" />
                </button>
                <button
                  type="button"
                  onClick={async () => {
                    // Self-spot the current activation on POTA. Freq comes
                    // from the form (MHz → kHz). Empty freq / mode means
                    // the operator hasn't tuned yet; disabled catches
                    // that case so we never send garbage to POTA.
                    const call = settings.station.callsign?.trim().toUpperCase() || '';
                    const freqMhz = parseFloat(formData.frequency);
                    if (!call || !activatingPark || !freqMhz) return;
                    try {
                      const resp = await fetch('/api/pota/spot', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                          callsign: call,
                          reference: activatingPark,
                          frequencyKhz: freqMhz * 1000,
                          mode: formData.mode,
                          comment: formData.remarks || undefined,
                        }),
                      });
                      const data = await resp.json();
                      alert(data.message ?? (data.success ? 'Spotted on POTA' : 'POTA self-spot failed'));
                    } catch (e) {
                      alert(`POTA self-spot failed: ${e instanceof Error ? e.message : String(e)}`);
                    }
                  }}
                  disabled={!formData.frequency || !activatingPark}
                  className="ml-auto px-2 py-1 rounded bg-green-500/20 border border-green-500/40 text-green-300 text-xs font-ui hover:bg-green-500/30 disabled:opacity-40 disabled:cursor-not-allowed flex items-center gap-1"
                  title={
                    !activatingPark ? 'Set your activating park first'
                    : !formData.frequency ? 'Set the frequency you are operating on'
                    : `Self-spot ${activatingPark} on POTA.app (uses POTA username/password from Settings)`
                  }
                >
                  <Megaphone className="w-3 h-3" />
                  Spot Myself
                </button>
              </>
            ) : (
              <>
                <span className="text-[10px] font-ui text-green-400 tracking-wider uppercase">POTA activation</span>
                <span className="text-xs text-green-300/70">No park set —</span>
                <button
                  type="button"
                  onClick={() => { setParkDraft(''); setEditingPark(true); }}
                  className="px-2 py-0.5 rounded bg-green-500/20 border border-green-500/40 text-green-300 text-xs font-ui hover:bg-green-500/30 flex items-center gap-1"
                  tabIndex={-1}
                >
                  <Pencil className="w-3 h-3" />
                  Add park ref
                </button>
              </>
            )}
          </div>
        )}

        {/* SAT — pass status chip + auto-populated uplink/downlink fields.
            When the S.A.T. controller is tracking, the satellite/freqs/modes
            update live; the operator can still hand-edit any field. When
            no controller is active (or SAT integration is off), everything
            is hand-enterable — you can log SAT QSOs without a CSN box. */}
        {logMode === 'sat' && (
          <div className="p-2 rounded-lg bg-amber-500/10 border border-amber-500/30 space-y-2">
            <div className="flex items-center gap-2">
              <Satellite className="w-4 h-4 text-amber-400 flex-shrink-0" />
              <span className="text-[10px] font-ui text-amber-400 tracking-wider uppercase">Satellite</span>
              <input
                type="text"
                value={formData.satellite}
                onChange={(e) => setFormData(prev => ({ ...prev, satellite: e.target.value.toUpperCase() }))}
                placeholder="SO-50, AO-91, ..."
                className="glass-input flex-1 font-mono font-bold text-sm py-1 uppercase"
              />
              {satTracking && (
                <span
                  className="px-2 py-0.5 rounded bg-amber-500/20 border border-amber-500/40 text-amber-300 text-[10px] font-ui tracking-wider whitespace-nowrap"
                  title="Following the CSN S.A.T. controller for this pass"
                >
                  TRACKING · CSN S.A.T.
                </span>
              )}
            </div>

            <div className="flex gap-2">
              <div className="flex-1">
                <label className="text-xs font-ui text-dark-200 mb-1 flex items-center gap-1">
                  <ArrowUp className="w-3 h-3 text-amber-400" />
                  Uplink (MHz)
                </label>
                <input
                  type="text"
                  value={formData.uplinkFreq}
                  onChange={(e) => setFormData(prev => ({ ...prev, uplinkFreq: e.target.value }))}
                  placeholder="145.850"
                  className="glass-input w-full font-mono text-sm"
                />
              </div>
              <div className="w-24">
                <label className="text-xs font-ui text-dark-200 mb-1 block">Up Mode</label>
                <select
                  value={formData.upMode}
                  onChange={(e) => setFormData(prev => ({ ...prev, upMode: e.target.value }))}
                  className="glass-input w-full text-sm font-mono"
                >
                  {SAT_MODES.map(m => <option key={m} value={m}>{m}</option>)}
                </select>
              </div>
            </div>

            <div className="flex gap-2">
              <div className="flex-1">
                <label className="text-xs font-ui text-dark-200 mb-1 flex items-center gap-1">
                  <ArrowDown className="w-3 h-3 text-amber-400" />
                  Downlink (MHz)
                </label>
                <input
                  type="text"
                  value={formData.downlinkFreq}
                  onChange={(e) => setFormData(prev => ({ ...prev, downlinkFreq: e.target.value }))}
                  placeholder="436.795"
                  className="glass-input w-full font-mono text-sm"
                />
              </div>
              <div className="w-24">
                <label className="text-xs font-ui text-dark-200 mb-1 block">Down Mode</label>
                <select
                  value={formData.downMode}
                  onChange={(e) => setFormData(prev => ({ ...prev, downMode: e.target.value }))}
                  className="glass-input w-full text-sm font-mono"
                >
                  {SAT_MODES.map(m => <option key={m} value={m}>{m}</option>)}
                </select>
              </div>
            </div>

            <div>
              <label className="text-xs font-ui text-dark-200 mb-1 flex items-center gap-1">
                <MapPin className="w-3 h-3 text-amber-400" />
                Their Grid
                <span className="text-[10px] text-dark-400 font-normal ml-1">(worked station's grid — required for SAT credit)</span>
              </label>
              <input
                type="text"
                value={formData.satGrid}
                onChange={(e) => setFormData(prev => ({ ...prev, satGrid: e.target.value.toUpperCase() }))}
                placeholder="EM79"
                className="glass-input w-full font-mono text-sm uppercase"
              />
            </div>
          </div>
        )}

        {/* Callsign, Band, Mode on one line */}
        <div className="flex gap-2 items-end">
          <div className="flex-1 relative">
            <label className="text-xs font-ui text-dark-200 flex items-center gap-1 mb-1">
              {isLookingUpCallsign ? (
                <Loader2 className="w-3 h-3 animate-spin text-accent-primary" />
              ) : (
                <Search className="w-3 h-3" />
              )}
              Callsign
              {isLookingUpCallsign && (
                <span className="text-accent-primary text-[10px]">Looking up...</span>
              )}
            </label>
            <input
              type="text"
              value={formData.callsign}
              onChange={(e) => handleCallsignChange(e.target.value)}
              onKeyDown={(e) => {
                if (!scpOpen || scpMatches.length === 0) return;   // dropdown closed → Enter logs, Esc clears (form handles)
                if (e.key === 'ArrowDown') { e.preventDefault(); setScpActive(true); setScpIndex(i => Math.min(i + 1, scpMatches.length - 1)); }
                else if (e.key === 'ArrowUp') { e.preventDefault(); setScpActive(true); setScpIndex(i => Math.max(i - 1, 0)); }
                else if (e.key === 'Tab') { e.preventDefault(); acceptSuggestion(scpMatches[scpIndex].call); }
                else if (e.key === 'Enter' && scpActive) { e.preventDefault(); e.stopPropagation(); acceptSuggestion(scpMatches[scpIndex].call); }
                else if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); setScpOpen(false); }
              }}
              onBlur={() => { applyCallHistory(formData.callsign); window.setTimeout(() => setScpOpen(false), 120); }}
              placeholder="Callsign"
              className="glass-input w-full font-mono font-bold tracking-wider uppercase"
              autoFocus
            />
            {scpOpen && scpMatches.length > 0 && (
              <ul className="absolute z-50 left-0 right-0 mt-1 max-h-56 overflow-auto rounded-md border border-glass-200 bg-dark-800/98 shadow-xl">
                {scpMatches.map((m, i) => {
                  const lw = m.worked ? lastWorked[m.call] : undefined;
                  const hint = lw ? [lw.band, lw.date].filter(Boolean).join(' · ') : '';
                  return (
                    <li
                      key={m.call}
                      onMouseDown={(e) => { e.preventDefault(); acceptSuggestion(m.call); }}
                      onMouseEnter={() => setScpIndex(i)}
                      className={`px-2 py-1 font-mono text-sm cursor-pointer flex items-center justify-between gap-2 ${i === scpIndex ? 'bg-accent-primary/20 text-white' : 'text-dark-100 hover:bg-dark-700'}`}
                    >
                      <span className="flex items-center gap-1.5">
                        <span
                          className={`w-1.5 h-1.5 rounded-full ${m.worked ? 'bg-accent-success' : 'bg-transparent'}`}
                          title={m.worked ? 'Worked before' : undefined}
                        />
                        {m.call}
                      </span>
                      {hint && <span className="text-[10px] text-dark-300 font-ui whitespace-nowrap">{hint}</span>}
                    </li>
                  );
                })}
              </ul>
            )}
          </div>
          <div className="w-28">
            <label className="text-xs font-ui text-dark-200 mb-1 flex items-center gap-1">
              Band
              {followRadio && currentRadioState && (
                <span className="w-1.5 h-1.5 rounded-full bg-accent-success" title="From radio" />
              )}
            </label>
            <select
              value={formData.band}
              disabled={rigControlLocked}
              title={rigControlLocked ? 'S.A.T. is controlling the radio — rig control paused' : undefined}
              onChange={(e) => {
                const newBand = e.target.value;
                setFormData(prev => ({ ...prev, band: newBand }));
                // When following a rig, also tune it to a sensible default
                // frequency in the new band. Without this, the follow-radio
                // effect would snap the dropdown back to the rig's current
                // band on the next poll and the user would be unable to
                // change the band from the Log Entry form. Skipped while the
                // S.A.T. controller owns the radio.
                if (!rigControlLocked && followRadio && currentRadioState) {
                  tuneToBand(newBand, formData.mode).catch(() => {});
                }
              }}
              className={`glass-input w-full text-sm font-mono ${
                rigControlLocked ? 'opacity-50 cursor-not-allowed' : followRadio && currentRadioState ? 'border-accent-success/30' : ''
              }`}
            >
              {BANDS.map(band => (
                <option key={band} value={band}>{band}</option>
              ))}
            </select>
          </div>
          <div className="w-28">
            <label className="text-xs font-ui text-dark-200 mb-1 flex items-center gap-1">
              Mode
              {followRadio && currentRadioState && (
                <span className="w-1.5 h-1.5 rounded-full bg-accent-success" title="From radio" />
              )}
            </label>
            <select
              value={formData.mode}
              disabled={rigControlLocked}
              title={rigControlLocked ? 'S.A.T. is controlling the radio — rig control paused' : undefined}
              onChange={(e) => {
                const newMode = e.target.value;
                setFormData(prev => ({ ...prev, mode: newMode }));
                // Same rationale as the band handler above — push the mode
                // change back to whichever rig is active so the follow-radio
                // effect doesn't fight the user's selection every 1.5 s poll.
                // Skipped while the S.A.T. controller owns the radio.
                if (!rigControlLocked && followRadio && currentRadioState) {
                  setRadioMode(newMode).catch(() => {});
                }
              }}
              className={`glass-input w-full text-sm font-mono ${
                rigControlLocked ? 'opacity-50 cursor-not-allowed' : followRadio && currentRadioState ? 'border-accent-success/30' : ''
              }`}
            >
              {MODE_GROUPS.map(group => (
                <optgroup key={group.label} label={group.label}>
                  {group.modes.map(mode => (
                    <option key={mode} value={mode}>{mode}</option>
                  ))}
                </optgroup>
              ))}
            </select>
          </div>
        </div>

        {/* Probable dupe — advisory only; the Log button stays enabled. */}
        {dupe && (
          <div className="flex items-center gap-2 px-2.5 py-1.5 rounded-lg border border-accent-warning/40 bg-accent-warning/10 text-accent-warning text-xs font-ui animate-fade-in">
            <AlertTriangle className="w-3.5 h-3.5 flex-shrink-0" />
            <span>Possible dupe — {formatDupeWarning(dupe, Date.now())}</span>
          </div>
        )}

        {/* Callsign Info Card - Loading State */}
        {isLookingUpCallsign && formData.callsign && (
          <div className="bg-dark-700/50 rounded-lg p-2 border border-glass-100 animate-fade-in">
            <div className="flex items-center gap-2">
              <div className="w-10 h-10 rounded-lg bg-dark-600 flex items-center justify-center">
                <Loader2 className="w-5 h-5 text-accent-primary animate-spin" />
              </div>
              <div className="flex-1 min-w-0">
                <p className="font-medium font-ui text-dark-300 text-sm">Looking up callsign...</p>
              </div>
            </div>
          </div>
        )}

        {/* Callsign Info Card - Data (only show if callsign matches to avoid stale data from out-of-order responses) */}
        {!isLookingUpCallsign && focusedCallsignInfo && formData.callsign &&
         focusedCallsignInfo.callsign?.toUpperCase() === formData.callsign.toUpperCase() && (
          <div className="bg-dark-700/50 rounded-lg p-2 border border-glass-100 animate-fade-in">
            <div className="flex items-center gap-2">
              {focusedCallsignInfo.imageUrl ? (
                <img
                  src={focusedCallsignInfo.imageUrl}
                  alt={focusedCallsignInfo.callsign}
                  className="w-10 h-10 rounded-lg object-cover"
                />
              ) : (
                <div className="w-10 h-10 rounded-lg bg-dark-600 flex items-center justify-center">
                  <User className="w-5 h-5 text-dark-300" />
                </div>
              )}
              <div className="flex-1 min-w-0">
                <p className="font-medium text-gray-100 text-sm truncate">
                  {focusedCallsignInfo.name || 'No name on file'}
                </p>
                <div className="flex items-center gap-2 text-xs text-dark-300">
                  <span>{getCountryFlag(focusedCallsignInfo.country) || ''}</span>
                  <span className="truncate">{focusedCallsignInfo.country || 'Unknown'}</span>
                  {focusedCallsignInfo.grid && (
                    <>
                      <MapPin className="w-3 h-3" />
                      <span className="font-mono">{focusedCallsignInfo.grid}</span>
                    </>
                  )}
                </div>
              </div>
              <div className="text-3xl" title={focusedCallsignInfo.country || 'Unknown country'}>
                {getCountryFlag(focusedCallsignInfo.country, '')}
              </div>
              <button
                type="button"
                onClick={() => window.open(`https://www.qrz.com/db/${focusedCallsignInfo.callsign}`, '_blank', 'noopener,noreferrer')}
                className="p-1.5 rounded transition-colors text-accent-primary hover:bg-dark-600"
                title="QRZ.com"
              >
                <ExternalLink className="w-4 h-4" />
              </button>
            </div>
          </div>
        )}

        {/* Name field with lock pattern */}
        <div>
          <label className="text-xs font-ui text-dark-200 mb-1 flex items-center gap-1">
            <User className="w-3 h-3" />
            Name
            {nameLocked && focusedCallsignInfo?.name && (
              <span className="w-1.5 h-1.5 rounded-full bg-accent-primary" title="From QRZ" />
            )}
          </label>
          <div className="flex items-center gap-2">
            <input
              type="text"
              value={formData.name}
              onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
              placeholder={nameLocked && focusedCallsignInfo?.name ? focusedCallsignInfo.name : 'Operator name...'}
              className={`glass-input flex-1 text-sm ${
                nameLocked && focusedCallsignInfo?.name ? 'border-accent-primary/30' : ''
              }`}
              readOnly={nameLocked && !!focusedCallsignInfo?.name}
            />
            <button
              type="button"
              onClick={() => setNameLocked(!nameLocked)}
              className={`p-1.5 rounded transition-colors ${
                nameLocked
                  ? 'text-dark-300 hover:text-dark-200'
                  : 'text-accent-primary hover:text-accent-primary/80'
              }`}
              title={nameLocked ? 'Unlock to edit name' : 'Lock to auto-fill from QRZ'}
              tabIndex={-1}
            >
              {nameLocked ? <LockOpen className="w-3.5 h-3.5" /> : <Lock className="w-3.5 h-3.5" />}
            </button>
          </div>
        </div>

        {/* QTH/Location + county + grid — worked-station location.
            Both auto-fill from the callbook lookup. The grid was always logged
            (the submit path falls back to the lookup value) but had no box, so
            it couldn't be seen or corrected before logging. */}
        <div className="flex gap-2">
          <div className="flex-1 min-w-0">
            <label className="text-xs font-ui text-dark-200 mb-1 block flex items-center gap-1">
              <MapPin className="w-3 h-3" />
              QTH / Location
            </label>
            <input
              type="text"
              value={formData.qth}
              onChange={(e) => setFormData(prev => ({ ...prev, qth: e.target.value }))}
              placeholder="City, State"
              className="glass-input w-full text-sm"
            />
          </div>
          <div className="w-32 shrink-0">
            <label className="text-xs font-ui text-dark-200 mb-1 block">County</label>
            <input
              type="text"
              value={formData.county}
              onChange={(e) => setFormData(prev => ({ ...prev, county: e.target.value }))}
              placeholder="County"
              className="glass-input w-full text-sm"
            />
          </div>
          <div className="w-28 shrink-0">
            <label className="text-xs font-ui text-dark-200 mb-1 block">Grid</label>
            <input
              type="text"
              value={formData.grid}
              onChange={(e) => setFormData(prev => ({ ...prev, grid: e.target.value.toUpperCase() }))}
              placeholder="EN54"
              spellCheck={false}
              className="glass-input w-full text-sm uppercase"
            />
          </div>
        </div>

        {/* Frequency, RST Sent, RST Rcvd on one line */}
        <div className="flex gap-3 items-end">
          <div className="w-32">
            <label className="text-xs font-ui text-dark-200 mb-1 flex items-center gap-1">
              Frequency (MHz)
              {followRadio && currentRadioState && (
                <span className="w-1.5 h-1.5 rounded-full bg-accent-success" title="From radio" />
              )}
            </label>
            <input
              type="text"
              value={formData.frequency}
              onChange={(e) => {
                const frequency = e.target.value;
                const freqMhz = parseFloat(frequency);
                // Recompute the band from the entered MHz value so the
                // Band dropdown stays consistent with the frequency.
                const newBand = !isNaN(freqMhz) ? getBandFromFrequency(freqMhz * 1_000_000) : null;
                setFormData(prev => ({
                  ...prev,
                  frequency,
                  band: newBand || prev.band,
                }));
              }}
              placeholder="14.250"
              className={`glass-input w-full font-mono text-sm ${
                followRadio && currentRadioState ? 'border-accent-success/30' : ''
              }`}
              readOnly={followRadio && !!currentRadioState}
            />
          </div>

          {/* My RST Sent — v1.x label wording */}
          <div>
            <label className="text-xs font-ui text-dark-200 mb-1 flex items-center gap-1">
              <span className="text-accent-success">My</span> RST Sent
            </label>
            <div className="flex items-center gap-1">
              <RstCombobox
                value={formData.rstSent}
                onChange={(v) => setFormData(prev => ({ ...prev, rstSent: v }))}
                options={getRstOptions(formData.mode)}
                className="glass-input w-16 font-mono text-sm"
              />
              {supportsDbEnhancement(formData.mode) && formData.rstSent.endsWith('9') && (
                <>
                  <span className="text-dark-300">+</span>
                  <select
                    value={formData.rstSentPlus}
                    onChange={(e) => setFormData(prev => ({ ...prev, rstSentPlus: e.target.value }))}
                    className="glass-input w-20 font-mono text-sm"
                  >
                    <option value="">--</option>
                    <option value="10">10</option>
                    <option value="20">20</option>
                    <option value="30">30</option>
                    <option value="40">40</option>
                  </select>
                </>
              )}
            </div>
          </div>

          {/* Separator */}
          <div className="flex items-end pb-2">
            <span className="text-dark-600 text-lg">/</span>
          </div>

          {/* Their RST Rcvd — v1.x label wording. Combo (Lyra link) adds an
              "S-auto" toggle that fills the S digit from the signal meter, with
              an AUTO/MANUAL badge; hidden in SAT mode + when not linked. */}
          <div>
            <label className="text-xs font-ui text-dark-200 mb-1 flex items-center gap-1">
              <span className="text-accent-secondary">Their</span> RST Rcvd
              {comboLinked && logMode !== 'sat' && (
                <>
                  <button
                    type="button"
                    tabIndex={-1}
                    onClick={() => setAutoRstEnabled(v => !v)}
                    title={autoRstEnabled
                      ? 'Auto received-S from Lyra’s signal meter is ON — click to turn off'
                      : 'Auto-fill the received S from Lyra’s signal meter (Combo)'}
                    className={`ml-1 rounded px-1 leading-tight text-[9px] uppercase tracking-wide border ${
                      autoRstEnabled
                        ? 'border-accent-secondary/50 text-accent-secondary bg-accent-secondary/10'
                        : 'border-dark-600 text-dark-400 hover:text-dark-200'
                    }`}
                  >
                    S-auto
                  </button>
                  {autoRstEnabled && (
                    <span
                      className={`text-[9px] uppercase tracking-wide ${rstRcvdAuto ? 'text-accent-success' : 'text-dark-400'}`}
                      title={rstRcvdAuto
                        ? 'Tracking the signal meter — fills from peak strength'
                        : 'You edited it — auto is paused until the next QSO'}
                    >
                      {rstRcvdAuto ? 'auto' : 'manual'}
                    </span>
                  )}
                </>
              )}
            </label>
            <div className="flex items-center gap-1">
              <RstCombobox
                value={formData.rstRcvd}
                onChange={(v) => { setFormData(prev => ({ ...prev, rstRcvd: v })); setRstRcvdAuto(false); }}
                options={getRstOptions(formData.mode)}
                className="glass-input w-16 font-mono text-sm"
              />
              {supportsDbEnhancement(formData.mode) && formData.rstRcvd.endsWith('9') && (
                <>
                  <span className="text-dark-300">+</span>
                  <select
                    value={formData.rstRcvdPlus}
                    onChange={(e) => { setFormData(prev => ({ ...prev, rstRcvdPlus: e.target.value })); setRstRcvdAuto(false); }}
                    className="glass-input w-20 font-mono text-sm"
                  >
                    <option value="">--</option>
                    <option value="10">10</option>
                    <option value="20">20</option>
                    <option value="30">30</option>
                    <option value="40">40</option>
                  </select>
                </>
              )}
            </div>
          </div>
        </div>

        {/* POTA — P2P Park Ref (worked station's park for park-to-park
            contacts). Optional; when set, backend stores as `pota_ref`
            in AdifExtra so PotaStatistics picks it up as a hunt. */}
        {logMode === 'pota' && (
          <div>
            <label className="text-xs font-ui text-dark-200 mb-1 block flex items-center gap-1">
              <Trees className="w-3 h-3 text-green-400" />
              P2P Park Ref
              <span className="text-[10px] text-dark-400 font-normal ml-1">(optional — their park if P2P contact)</span>
            </label>
            <input
              type="text"
              value={formData.p2pPark}
              onChange={(e) => setFormData(prev => ({ ...prev, p2pPark: e.target.value.toUpperCase() }))}
              placeholder="K-5678"
              className="glass-input w-full font-mono text-sm"
            />
          </div>
        )}

        {/* Contest / Event / Park — v1.x General field, free-text
            contest name / event / park reference. Backend maps into
            Qso.Contest.ContestId. */}
        <div>
          <label className="text-xs font-ui text-dark-200 mb-1 block">Contest / Event / Park</label>
          <input
            type="text"
            value={formData.contest}
            onChange={(e) => setFormData(prev => ({ ...prev, contest: e.target.value }))}
            placeholder="Optional"
            className="glass-input w-full text-sm"
          />
        </div>

        {/* Remarks — v1.x consolidates Comment + Notes into one field.
            Stored server-side in Qso.Comment (the ADIF-exported field). */}
        <div>
          <label className="text-xs font-ui text-dark-200 mb-1 block">Remarks</label>
          <input
            type="text"
            value={formData.remarks}
            onChange={(e) => setFormData(prev => ({ ...prev, remarks: e.target.value }))}
            placeholder="Notes, contest exchange, antenna used..."
            className="glass-input w-full text-sm"
          />
        </div>

        {/* Timestamp - compact display with optional edit */}
        <div className="flex items-center gap-2 text-xs text-dark-300">
          <Clock className="w-3 h-3" />
          {timeLocked ? (
            <>
              <span className="font-mono">{qsoDate} {qsoTime} UTC</span>
              <button
                type="button"
                onClick={() => setTimeLocked(false)}
                className="text-dark-300 hover:text-dark-200 transition-colors"
                title="Edit timestamp"
                tabIndex={-1}
              >
                <LockOpen className="w-3 h-3" />
              </button>
            </>
          ) : (
            <>
              <input
                type="date"
                value={qsoDate}
                onChange={(e) => setQsoDate(e.target.value)}
                className="glass-input px-1 py-0.5 font-mono text-xs w-28"
              />
              <input
                type="time"
                value={qsoTime}
                onChange={(e) => setQsoTime(e.target.value)}
                className="glass-input px-1 py-0.5 font-mono text-xs w-20"
              />
              <span>UTC</span>
              <button
                type="button"
                onClick={() => setTimeLocked(true)}
                className="text-accent-primary hover:text-accent-primary/80 transition-colors"
                title="Lock to system time"
                tabIndex={-1}
              >
                <Lock className="w-3 h-3" />
              </button>
            </>
          )}
        </div>

        {/* Submit / Clear / Spot — v1.x General button row minus QRZ
            (the callsign info card already exposes a QRZ.com link via the
            ExternalLink icon, so a second button here would be redundant).
            Spot is a placeholder until the "send-a-spot-to-the-cluster"
            hub method lands (v2 currently only RECEIVES spots). */}
        <div className="flex gap-2">
          <button
            type="submit"
            disabled={!formData.callsign || createQso.isPending}
            className="glass-button-success flex-1 flex items-center justify-center gap-2 py-2 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <Send className="w-4 h-4" />
            {createQso.isPending ? 'Logging...' : 'Log QSO'}
          </button>
          <button
            type="button"
            onClick={handleClear}
            disabled={createQso.isPending}
            className="glass-button flex items-center justify-center gap-1.5 py-2 px-3 disabled:opacity-50 disabled:cursor-not-allowed"
            title="Clear QSO details"
          >
            <X className="w-4 h-4" />
            Clear
          </button>
          <button
            type="button"
            onClick={async () => {
              // Broadcast to every connected cluster. Freq is stored as
              // MHz in the form; the cluster wire format is kHz so we
              // convert. Callsign and freq are required — Spot is
              // useless without either.
              const freqMhz = parseFloat(formData.frequency);
              if (!formData.callsign || !freqMhz) return;
              try {
                const r = await sendDxSpot(formData.callsign, freqMhz * 1000, formData.remarks || undefined);
                alert(r.sent
                  ? `Spotted ${formData.callsign} on ${r.detail}`
                  : r.detail);
              } catch (e) {
                alert(`Spot failed: ${e instanceof Error ? e.message : String(e)}`);
              }
            }}
            disabled={!formData.callsign || !formData.frequency}
            className="glass-button flex items-center justify-center gap-1.5 py-2 px-3 disabled:opacity-40 disabled:cursor-not-allowed"
            title="Send this QSO's callsign+frequency as a spot to every connected DX cluster"
          >
            <Send className="w-4 h-4" />
            Spot
          </button>
        </div>

        {/* v1.x-style "mode active" indicator at the bottom of the form —
            tells the operator at a glance which database bucket their
            QSOs are landing in. v2 uses one collection with POTA tagging
            via AdifExtra rather than a separate .db file, but the
            behavioral outcome (queried out via PotaStatistics) matches. */}
        {logMode === 'pota' && (
          <div className="flex items-center justify-center gap-2 px-3 py-1.5 rounded bg-green-500/10 border border-green-500/30 text-[11px] font-ui text-green-300">
            <Trees className="w-3 h-3" />
            POTA mode active — QSOs tagged as POTA
            {activatingPark && <span className="font-mono font-bold">({activatingPark})</span>}
          </div>
        )}
        {logMode === 'sat' && (
          <div className="flex items-center justify-center gap-2 px-3 py-1.5 rounded bg-amber-500/10 border border-amber-500/30 text-[11px] font-ui text-amber-300">
            <Satellite className="w-3 h-3" />
            SAT mode active — QSOs tagged as PROP_MODE=SAT for LoTW satellite credit
            {formData.satellite && <span className="font-mono font-bold">({formData.satellite})</span>}
          </div>
        )}
      </form>
    </GlassPanel>
    </>
  );
}

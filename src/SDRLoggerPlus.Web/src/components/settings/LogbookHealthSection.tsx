import { useCallback, useEffect, useState } from 'react';
import { Stethoscope, Clock, CheckCircle2, AlertTriangle, Wrench, RefreshCw, ShieldCheck, Layers, Trash2, Globe } from 'lucide-react';
import {
  api,
  type QsoTimeAuditResult, type QsoTimeRepairResult,
  type QsoDuplicateScanResult, type QsoDuplicateRemoveResult,
  type CountryNameAuditResult, type CountryNameNormalizeResult,
} from '../../api/client';

/**
 * Settings → Logbook Health → Verify QSO times.
 * Read-only scan of the local log for QSOs whose date lost its time to the old edit-modal bug,
 * with an opt-in, backup-first, upload-safe repair. See docs/design/timezone-architecture.md §5a.
 */
export function LogbookHealthSection() {
  const [scan, setScan] = useState<QsoTimeAuditResult | null>(null);
  const [scanning, setScanning] = useState(false);
  const [repairing, setRepairing] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [result, setResult] = useState<QsoTimeRepairResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const runScan = useCallback(async () => {
    setScanning(true); setError(null); setResult(null);
    try { setScan(await api.scanQsoTimes()); }
    catch (e) { setError(e instanceof Error ? e.message : 'Scan failed.'); }
    finally { setScanning(false); }
  }, []);

  useEffect(() => { runScan(); }, [runScan]);

  // ── Find duplicates ──
  const [dups, setDups] = useState<QsoDuplicateScanResult | null>(null);
  const [dupScanning, setDupScanning] = useState(false);
  const [dupRemoving, setDupRemoving] = useState(false);
  const [dupConfirming, setDupConfirming] = useState(false);
  const [dupResult, setDupResult] = useState<QsoDuplicateRemoveResult | null>(null);
  const [dupError, setDupError] = useState<string | null>(null);
  // Which QSO ids the operator has marked for removal. Seeded from the scan's defaults
  // (auto-keeper's twins in same-mode groups; nothing pre-selected in mode-mismatch groups).
  const [removeIds, setRemoveIds] = useState<Set<string>>(new Set());

  const scanDups = async () => {
    setDupScanning(true); setDupError(null); setDupResult(null);
    try {
      const r = await api.scanDuplicates();
      setDups(r);
      setRemoveIds(new Set(r.groups.flatMap(g => g.members.filter(m => !m.keep).map(m => m.id))));
    }
    catch (e) { setDupError(e instanceof Error ? e.message : 'Duplicate scan failed.'); }
    finally { setDupScanning(false); }
  };

  const toggleRemove = (id: string) => {
    setRemoveIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };

  const removeDups = async () => {
    if (!dups || removeIds.size === 0) return;
    setDupRemoving(true); setDupError(null);
    try {
      const r = await api.removeDuplicates([...removeIds]);
      setDupResult(r);
      setDupConfirming(false);
      await scanDups();
    } catch (e) {
      setDupError(e instanceof Error ? e.message : 'Duplicate removal failed.');
    } finally {
      setDupRemoving(false);
    }
  };

  // ── Standardize country names ──
  const [cn, setCn] = useState<CountryNameAuditResult | null>(null);
  const [cnScanning, setCnScanning] = useState(false);
  const [cnRunning, setCnRunning] = useState(false);
  const [cnConfirming, setCnConfirming] = useState(false);
  const [cnResult, setCnResult] = useState<CountryNameNormalizeResult | null>(null);
  const [cnError, setCnError] = useState<string | null>(null);
  const [cnSelected, setCnSelected] = useState<Set<number>>(new Set());

  const scanCountry = async () => {
    setCnScanning(true); setCnError(null); setCnResult(null);
    try {
      const r = await api.scanCountryNames();
      setCn(r);
      setCnSelected(new Set(r.groups.map(g => g.dxcc))); // default: normalize all proposed
    }
    catch (e) { setCnError(e instanceof Error ? e.message : 'Country scan failed.'); }
    finally { setCnScanning(false); }
  };

  const toggleCountry = (dxcc: number) => {
    setCnSelected(prev => {
      const next = new Set(prev);
      if (next.has(dxcc)) next.delete(dxcc); else next.add(dxcc);
      return next;
    });
  };

  const cnChangeCount = cn ? cn.groups.filter(g => cnSelected.has(g.dxcc)).reduce((n, g) => n + g.changeCount, 0) : 0;

  const normalizeCountry = async () => {
    if (!cn || cnSelected.size === 0) return;
    setCnRunning(true); setCnError(null);
    try {
      const r = await api.normalizeCountryNames([...cnSelected]);
      setCnResult(r);
      setCnConfirming(false);
      await scanCountry();
    } catch (e) {
      setCnError(e instanceof Error ? e.message : 'Country normalization failed.');
    } finally {
      setCnRunning(false);
    }
  };

  const doRepair = async () => {
    if (!scan) return;
    setRepairing(true); setError(null);
    try {
      const ids = scan.fixableSamples.map(s => s.id);
      const r = await api.repairQsoTimes(ids);
      setResult(r);
      setConfirming(false);
      await runScan();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Repair failed.');
    } finally {
      setRepairing(false);
    }
  };

  const utc = (iso: string) =>
    new Date(iso).toLocaleString('en-US', { timeZone: 'UTC', hour12: false });

  return (
    <div className="space-y-6 max-w-2xl">
      <div>
        <h2 className="text-lg font-semibold font-ui text-white flex items-center gap-2">
          <Stethoscope className="w-5 h-5 text-accent-secondary" /> Logbook Health
        </h2>
        <p className="text-sm text-dark-300 mt-1">
          Opt-in maintenance for your own log. Nothing is ever changed without your confirmation, and a
          full backup is taken automatically before any repair.
        </p>
      </div>

      {/* Verify QSO times */}
      <div className="rounded-lg border border-glass-100 bg-glass-50 p-4 space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-gray-200 font-medium">
            <Clock className="w-4 h-4 text-accent-primary" /> Verify QSO times
          </div>
          <button onClick={runScan} disabled={scanning}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-glass-200 text-sm text-gray-200 hover:bg-glass-100 disabled:opacity-50">
            <RefreshCw className={`w-3.5 h-3.5 ${scanning ? 'animate-spin' : ''}`} />
            {scanning ? 'Scanning…' : 'Re-scan'}
          </button>
        </div>

        <p className="text-xs text-dark-300">
          Finds QSOs whose stored date lost its time-of-day to an old bug (the time still survives in the
          ADIF <span className="font-mono">TIME_ON</span> field). It reconstructs only those; anything it
          can’t be sure about is reported for you to review, never guessed.
        </p>

        {error && (
          <div className="text-sm text-red-400 flex items-center gap-1.5">
            <AlertTriangle className="w-4 h-4" /> {error}
          </div>
        )}

        {scan && (
          <>
            {/* Counts */}
            <div className="grid grid-cols-3 gap-2 text-center">
              <Stat label="Consistent" value={scan.consistent} tone="ok" />
              <Stat label="Fixable" value={scan.fixableLostTime} tone={scan.fixableLostTime > 0 ? 'warn' : 'ok'} />
              <Stat label="Ambiguous" value={scan.ambiguous} tone={scan.ambiguous > 0 ? 'muted' : 'ok'} />
            </div>
            <div className="text-[11px] text-dark-400 text-center">{scan.total.toLocaleString()} QSOs scanned</div>

            {/* All clear */}
            {scan.fixableLostTime === 0 && scan.ambiguous === 0 && (
              <div className="text-sm text-green-400 flex items-center gap-1.5">
                <CheckCircle2 className="w-4 h-4" /> Every QSO time is consistent — nothing to repair.
              </div>
            )}

            {/* Fixable review + repair */}
            {scan.fixableLostTime > 0 && (
              <div className="space-y-2">
                <div className="text-sm text-gray-200 font-medium">
                  {scan.fixableLostTime.toLocaleString()} QSO(s) with a recoverable time
                </div>
                <div className="max-h-52 overflow-y-auto rounded border border-glass-100 divide-y divide-glass-100">
                  {scan.fixableSamples.map(s => (
                    <div key={s.id} className="flex items-center justify-between gap-3 px-3 py-1.5 text-xs">
                      <span className="font-mono text-accent-primary font-bold">{s.callsign}</span>
                      <span className="text-dark-300 font-mono">
                        {utc(s.qsoDate)} → <span className="text-green-300">{s.proposedQsoDate ? utc(s.proposedQsoDate) : '?'}</span> UTC
                      </span>
                    </div>
                  ))}
                  {scan.fixableLostTime > scan.fixableSamples.length && (
                    <div className="px-3 py-1.5 text-[11px] text-dark-400">
                      …and {(scan.fixableLostTime - scan.fixableSamples.length).toLocaleString()} more (all will be repaired)
                    </div>
                  )}
                </div>

                {!confirming ? (
                  <button onClick={() => setConfirming(true)}
                    className="flex items-center gap-1.5 px-3 py-2 rounded-lg bg-accent-secondary text-black font-medium text-sm hover:bg-accent-secondary/90">
                    <Wrench className="w-4 h-4" /> Repair {scan.fixableLostTime.toLocaleString()} time(s)
                  </button>
                ) : (
                  <div className="rounded-lg border border-amber-400/30 bg-amber-400/5 p-3 space-y-2">
                    <p className="text-xs text-amber-200">
                      This takes a <strong>full backup first</strong>, then reconstructs the times. It changes
                      only your local log — it will <strong>not</strong> re-upload anything to QRZ / LoTW / eQSL.
                    </p>
                    <div className="flex gap-2">
                      <button onClick={doRepair} disabled={repairing}
                        className="px-3 py-1.5 rounded-lg bg-accent-secondary text-black font-medium text-sm hover:bg-accent-secondary/90 disabled:opacity-50">
                        {repairing ? 'Repairing…' : 'Back up & repair'}
                      </button>
                      <button onClick={() => setConfirming(false)} disabled={repairing}
                        className="px-3 py-1.5 rounded-lg border border-glass-200 text-sm text-gray-200 hover:bg-glass-100 disabled:opacity-50">
                        Cancel
                      </button>
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Ambiguous — report only */}
            {scan.ambiguous > 0 && (
              <div className="space-y-2">
                <div className="text-sm text-dark-200 font-medium flex items-center gap-1.5">
                  <AlertTriangle className="w-4 h-4 text-amber-400" />
                  {scan.ambiguous.toLocaleString()} QSO(s) to review manually
                </div>
                <p className="text-[11px] text-dark-400">
                  These have a time (or possibly a date) mismatch with no safe way to recover the original —
                  a wrong date can’t be reconstructed from the record, so they’re reported, not changed.
                </p>
                <div className="max-h-40 overflow-y-auto rounded border border-glass-100 divide-y divide-glass-100">
                  {scan.ambiguousSamples.map(s => (
                    <div key={s.id} className="flex items-center justify-between gap-3 px-3 py-1.5 text-xs">
                      <span className="font-mono text-dark-200">{s.callsign}</span>
                      <span className="text-dark-400 font-mono">
                        {utc(s.qsoDate)} UTC · TIME_ON {s.timeOn ?? '—'}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </>
        )}

        {result && (
          <div className="text-sm text-green-400 flex items-center gap-1.5">
            <CheckCircle2 className="w-4 h-4" /> Repaired {result.repaired.toLocaleString()} QSO time(s).
            {result.skipped > 0 && <span className="text-dark-400"> ({result.skipped} skipped)</span>}
          </div>
        )}

        <div className="flex items-start gap-2 text-[11px] text-dark-400 border-t border-glass-100 pt-3">
          <ShieldCheck className="w-3.5 h-3.5 mt-0.5 shrink-0 text-dark-300" />
          <span>
            Fixes your <strong>local</strong> log only. Anything already uploaded to QRZ / LoTW keeps its
            current date — a re-upload is never triggered.
          </span>
        </div>
      </div>

      {/* Find duplicates */}
      <div className="rounded-lg border border-glass-100 bg-glass-50 p-4 space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-gray-200 font-medium">
            <Layers className="w-4 h-4 text-accent-primary" /> Find duplicates
          </div>
          <button onClick={scanDups} disabled={dupScanning}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-glass-200 text-sm text-gray-200 hover:bg-glass-100 disabled:opacity-50">
            <RefreshCw className={`w-3.5 h-3.5 ${dupScanning ? 'animate-spin' : ''}`} />
            {dupScanning ? 'Scanning…' : dups ? 'Re-scan' : 'Scan for duplicates'}
          </button>
        </div>

        <p className="text-xs text-dark-300">
          Finds the same station worked on the same band at the same minute. It <strong className="text-gray-200">keeps one copy</strong> of each
          same-mode set (preferring one already synced to QRZ/LoTW, then the most complete) and pre-checks the
          rest for removal. When the copies have <strong className="text-gray-200">different modes</strong> it leaves them alone and asks you to
          decide — tick exactly the rows you want gone.
        </p>

        {dupError && (
          <div className="text-sm text-red-400 flex items-center gap-1.5">
            <AlertTriangle className="w-4 h-4" /> {dupError}
          </div>
        )}

        {dups && (
          <>
            <div className="grid grid-cols-2 gap-2 text-center">
              <Stat label="Duplicate sets" value={dups.groupCount} tone={dups.groupCount > 0 ? 'warn' : 'ok'} />
              <Stat label="Selected to remove" value={removeIds.size} tone={removeIds.size > 0 ? 'warn' : 'ok'} />
            </div>
            <div className="text-[11px] text-dark-400 text-center">{dups.total.toLocaleString()} QSOs scanned</div>

            {dups.groupCount === 0 && (
              <div className="text-sm text-green-400 flex items-center gap-1.5">
                <CheckCircle2 className="w-4 h-4" /> No duplicates found.
              </div>
            )}

            {dups.groupCount > 0 && (
              <div className="space-y-2">
                <div className="max-h-72 overflow-y-auto rounded border border-glass-100 divide-y divide-glass-100">
                  {dups.groups.map(g => (
                    <div key={g.key} className={`px-3 py-2 ${g.modeMismatch ? 'bg-amber-400/5' : ''}`}>
                      {g.modeMismatch && (
                        <div className="flex items-center gap-1 text-[10px] uppercase tracking-wide text-amber-300 mb-1">
                          <AlertTriangle className="w-3 h-3" /> Different modes — you choose
                        </div>
                      )}
                      {g.members.map(m => {
                        const marked = removeIds.has(m.id);
                        return (
                          <label key={m.id} className="flex items-center justify-between gap-3 text-xs py-0.5 cursor-pointer">
                            <span className="flex items-center gap-2 min-w-0">
                              <input type="checkbox" checked={marked} onChange={() => toggleRemove(m.id)}
                                className="accent-red-500 shrink-0" />
                              <span className="font-mono font-bold text-accent-primary">{m.callsign}</span>
                              <span className="text-dark-400 font-mono truncate">{utc(m.qsoDate)} · {m.band} · <span className={g.modeMismatch ? 'text-amber-300' : ''}>{m.mode}</span></span>
                            </span>
                            <span className="text-[10px] uppercase tracking-wide shrink-0">
                              {marked ? (
                                <span className="text-red-400 flex items-center gap-1"><Trash2 className="w-3 h-3" /> remove</span>
                              ) : (
                                <span className="text-green-400" title={m.keepReason ?? ''}>keep{m.synced ? ' ✓synced' : ''}</span>
                              )}
                            </span>
                          </label>
                        );
                      })}
                    </div>
                  ))}
                </div>

                {!dupConfirming ? (
                  <button onClick={() => setDupConfirming(true)} disabled={removeIds.size === 0}
                    className="flex items-center gap-1.5 px-3 py-2 rounded-lg bg-accent-secondary text-black font-medium text-sm hover:bg-accent-secondary/90 disabled:opacity-40">
                    <Trash2 className="w-4 h-4" /> Remove {removeIds.size.toLocaleString()} selected
                  </button>
                ) : (
                  <div className="rounded-lg border border-amber-400/30 bg-amber-400/5 p-3 space-y-2">
                    <p className="text-xs text-amber-200">
                      This takes a <strong>full backup first</strong>, then deletes the <strong>{removeIds.size}</strong> ticked
                      row(s) from your <strong>local</strong> log only. It will <strong>not</strong> remove them from
                      QRZ / LoTW / eQSL. At least one QSO in every set is always kept.
                    </p>
                    <div className="flex gap-2">
                      <button onClick={removeDups} disabled={dupRemoving}
                        className="px-3 py-1.5 rounded-lg bg-accent-secondary text-black font-medium text-sm hover:bg-accent-secondary/90 disabled:opacity-50">
                        {dupRemoving ? 'Removing…' : 'Back up & remove'}
                      </button>
                      <button onClick={() => setDupConfirming(false)} disabled={dupRemoving}
                        className="px-3 py-1.5 rounded-lg border border-glass-200 text-sm text-gray-200 hover:bg-glass-100 disabled:opacity-50">
                        Cancel
                      </button>
                    </div>
                  </div>
                )}
              </div>
            )}
          </>
        )}

        {dupResult && (
          <div className="text-sm text-green-400 flex items-center gap-1.5">
            <CheckCircle2 className="w-4 h-4" /> Removed {dupResult.deleted.toLocaleString()} duplicate(s).
            {dupResult.skipped > 0 && <span className="text-dark-400"> ({dupResult.skipped} skipped)</span>}
          </div>
        )}

        <div className="flex items-start gap-2 text-[11px] text-dark-400 border-t border-glass-100 pt-3">
          <ShieldCheck className="w-3.5 h-3.5 mt-0.5 shrink-0 text-dark-300" />
          <span>Removes rows from your <strong>local</strong> log only — never from QRZ / LoTW / eQSL.</span>
        </div>
      </div>

      {/* Standardize country names */}
      <div className="rounded-lg border border-glass-100 bg-glass-50 p-4 space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-gray-200 font-medium">
            <Globe className="w-4 h-4 text-accent-primary" /> Standardize country names
          </div>
          <button onClick={scanCountry} disabled={cnScanning}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-glass-200 text-sm text-gray-200 hover:bg-glass-100 disabled:opacity-50">
            <RefreshCw className={`w-3.5 h-3.5 ${cnScanning ? 'animate-spin' : ''}`} />
            {cnScanning ? 'Scanning…' : cn ? 'Re-scan' : 'Scan country names'}
          </button>
        </div>

        <p className="text-xs text-dark-300">
          Imported logs keep whatever the source file wrote for the country (LoTW uses
          <span className="font-mono"> "UNITED STATES OF AMERICA"</span>, others use <span className="font-mono">"USA"</span>…), so one
          country can appear under several spellings. This finds entities logged under more than one name — grouped by
          <strong className="text-gray-200"> DXCC number</strong>, so they're provably the same country — and can set them all to one
          standard name. Tick which entities to fix.
        </p>

        {cnError && (
          <div className="text-sm text-red-400 flex items-center gap-1.5">
            <AlertTriangle className="w-4 h-4" /> {cnError}
          </div>
        )}

        {cn && (
          <>
            <div className="grid grid-cols-2 gap-2 text-center">
              <Stat label="Entities to fix" value={cn.groupCount} tone={cn.groupCount > 0 ? 'warn' : 'ok'} />
              <Stat label="QSOs to update" value={cnChangeCount} tone={cnChangeCount > 0 ? 'warn' : 'ok'} />
            </div>
            <div className="text-[11px] text-dark-400 text-center">
              {cn.totalQsos.toLocaleString()} QSOs scanned
              {cn.withoutDxcc > 0 && <span> · {cn.withoutDxcc.toLocaleString()} without a DXCC number skipped</span>}
            </div>

            {cn.groupCount === 0 && (
              <div className="text-sm text-green-400 flex items-center gap-1.5">
                <CheckCircle2 className="w-4 h-4" /> Country names are already consistent.
              </div>
            )}

            {cn.groupCount > 0 && (
              <div className="space-y-2">
                <div className="max-h-72 overflow-y-auto rounded border border-glass-100 divide-y divide-glass-100">
                  {cn.groups.map(g => (
                    <label key={g.dxcc} className="flex items-start gap-2 px-3 py-2 cursor-pointer">
                      <input type="checkbox" checked={cnSelected.has(g.dxcc)} onChange={() => toggleCountry(g.dxcc)}
                        className="accent-accent-primary mt-0.5 shrink-0" />
                      <div className="min-w-0 text-xs">
                        <div className="text-gray-200">
                          → <span className="font-semibold text-accent-primary">{g.canonical}</span>
                          <span className="text-dark-400"> ({g.changeCount.toLocaleString()} to change)</span>
                        </div>
                        <div className="text-dark-400 truncate">
                          {g.variants.map(v => `${v.country} (${v.count})`).join('  ·  ')}
                        </div>
                      </div>
                    </label>
                  ))}
                </div>

                {!cnConfirming ? (
                  <button onClick={() => setCnConfirming(true)} disabled={cnSelected.size === 0 || cnChangeCount === 0}
                    className="flex items-center gap-1.5 px-3 py-2 rounded-lg bg-accent-secondary text-black font-medium text-sm hover:bg-accent-secondary/90 disabled:opacity-40">
                    <Globe className="w-4 h-4" /> Standardize {cnChangeCount.toLocaleString()} QSO(s)
                  </button>
                ) : (
                  <div className="rounded-lg border border-amber-400/30 bg-amber-400/5 p-3 space-y-2">
                    <p className="text-xs text-amber-200">
                      This takes a <strong>full backup first</strong>, then renames the country on <strong>{cnChangeCount.toLocaleString()}</strong> QSO(s)
                      in your <strong>local</strong> log only. It does <strong>not</strong> re-upload to or change QRZ / LoTW / eQSL.
                    </p>
                    <div className="flex gap-2">
                      <button onClick={normalizeCountry} disabled={cnRunning}
                        className="px-3 py-1.5 rounded-lg bg-accent-secondary text-black font-medium text-sm hover:bg-accent-secondary/90 disabled:opacity-50">
                        {cnRunning ? 'Updating…' : 'Back up & standardize'}
                      </button>
                      <button onClick={() => setCnConfirming(false)} disabled={cnRunning}
                        className="px-3 py-1.5 rounded-lg border border-glass-200 text-sm text-gray-200 hover:bg-glass-100 disabled:opacity-50">
                        Cancel
                      </button>
                    </div>
                  </div>
                )}
              </div>
            )}
          </>
        )}

        {cnResult && (
          <div className="text-sm text-green-400 flex items-center gap-1.5">
            <CheckCircle2 className="w-4 h-4" /> Standardized {cnResult.changed.toLocaleString()} QSO(s).
            {cnResult.skipped > 0 && <span className="text-dark-400"> ({cnResult.skipped} skipped)</span>}
          </div>
        )}

        <div className="flex items-start gap-2 text-[11px] text-dark-400 border-t border-glass-100 pt-3">
          <ShieldCheck className="w-3.5 h-3.5 mt-0.5 shrink-0 text-dark-300" />
          <span>Renames the country on your <strong>local</strong> QSOs only — sync flags are untouched, so nothing is re-uploaded to QRZ / LoTW / eQSL.</span>
        </div>
      </div>
    </div>
  );
}

function Stat({ label, value, tone }: { label: string; value: number; tone: 'ok' | 'warn' | 'muted' }) {
  const color = tone === 'warn' ? 'text-amber-300' : tone === 'muted' ? 'text-dark-200' : 'text-green-400';
  return (
    <div className="rounded-lg bg-glass-100/40 py-2">
      <div className={`text-xl font-bold font-mono ${color}`}>{value.toLocaleString()}</div>
      <div className="text-[10px] uppercase tracking-wide text-dark-400">{label}</div>
    </div>
  );
}

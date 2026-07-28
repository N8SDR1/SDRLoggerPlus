import { useCallback, useEffect, useState } from 'react';
import { Stethoscope, Clock, CheckCircle2, AlertTriangle, Wrench, RefreshCw, ShieldCheck } from 'lucide-react';
import { api, type QsoTimeAuditResult, type QsoTimeRepairResult } from '../../api/client';

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

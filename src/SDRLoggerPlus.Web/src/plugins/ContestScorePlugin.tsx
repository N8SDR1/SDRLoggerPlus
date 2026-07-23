import { useEffect, useState } from 'react';
import { Trophy } from 'lucide-react';
import { api } from '../api/client';
import { useAppStore } from '../store/appStore';
import { GlassPanel } from '../components/GlassPanel';

// Elapsed session time as H:MM:SS (or M:SS under an hour). Pure for testing.
export function formatElapsed(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) ms = 0;
  const total = Math.floor(ms / 1000);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  const mm = String(m).padStart(2, '0');
  const ss = String(s).padStart(2, '0');
  return h > 0 ? `${h}:${mm}:${ss}` : `${m}:${ss}`;
}

// Average QSOs/hour over the whole session. 0 until at least a few seconds in, to
// avoid a meaningless spike on the first QSO. Pure for testing.
export function averageRate(qsos: number, startedAtIso: string, now: number): number {
  const started = Date.parse(startedAtIso);
  if (!Number.isFinite(started)) return 0;
  const hours = (now - started) / 3_600_000;
  if (hours <= 1 / 120) return 0; // < 30s elapsed
  return Math.round(qsos / hours);
}

export function ContestScorePlugin() {
  const contestState = useAppStore((s) => s.contestState);
  const setContestState = useAppStore((s) => s.setContestState);
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    if (!contestState) api.getContestState().then((s) => setContestState(s)).catch(() => {});
  }, [contestState, setContestState]);

  // Tick the elapsed clock every second while a session is active.
  useEffect(() => {
    if (!contestState) return;
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, [contestState]);

  if (!contestState) {
    return (
      <GlassPanel title="Score" icon={<Trophy className="w-5 h-5" />}>
        <div className="flex items-center justify-center h-full text-sm text-gray-500 p-6 text-center">
          Start a contest session to see your score.
        </div>
      </GlassPanel>
    );
  }

  const elapsed = formatElapsed(now - Date.parse(contestState.startedAt));
  const avg = averageRate(contestState.qsos, contestState.startedAt, now);

  return (
    <GlassPanel
      title="Score"
      icon={<Trophy className="w-5 h-5" />}
      actions={<span className="text-xs text-gray-400 truncate max-w-[10rem]">{contestState.definitionName}</span>}
    >
      <div className="flex flex-col h-full p-4 gap-4">
        {/* Headline score */}
        <div className="text-center">
          <div className="text-5xl font-bold font-mono text-accent-primary leading-none">
            {contestState.score.toLocaleString()}
          </div>
          <div className="text-[11px] uppercase tracking-widest text-gray-500 mt-1">Claimed score</div>
        </div>

        {/* QSOs × Mults breakdown */}
        <div className="grid grid-cols-3 gap-px bg-glass-100 border border-glass-100 rounded-lg overflow-hidden text-center">
          <Cell label="QSOs" value={contestState.qsos} />
          <Cell label="Points" value={contestState.points} />
          <Cell label="Mults" value={contestState.multipliers} />
        </div>

        {/* Self-declared objective/bonus points (e.g. Winter Field Day), folded
            into the claimed score above. */}
        {contestState.bonusPoints > 0 && (
          <div className="flex items-center justify-between text-xs px-1 -mt-1">
            <span className="text-gray-500">Bonus points</span>
            <span className="font-mono text-emerald-300">+{contestState.bonusPoints.toLocaleString()}</span>
          </div>
        )}

        {/* Rate + session stats */}
        <div className="grid grid-cols-2 gap-px bg-glass-100 border border-glass-100 rounded-lg overflow-hidden text-center">
          <Cell label="Rate / hr" value={contestState.rateLastHour} />
          <Cell label="Rate (last 10)" value={contestState.rateLast10} />
          <Cell label="Avg / hr" value={avg} />
          <Cell label="Dupes" value={contestState.dupes} />
        </div>

        <div className="mt-auto flex items-center justify-between text-xs text-gray-500">
          <span>Elapsed</span>
          <span className="font-mono text-gray-300">{elapsed}</span>
        </div>
      </div>
    </GlassPanel>
  );
}

function Cell({ label, value }: { label: string; value: number | string }) {
  return (
    <div className="bg-dark-800/80 px-2 py-2.5">
      <div className="text-lg font-semibold font-mono text-gray-200">{value}</div>
      <div className="text-[10px] uppercase tracking-wider text-gray-500">{label}</div>
    </div>
  );
}

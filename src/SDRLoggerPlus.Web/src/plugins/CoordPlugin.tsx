import { useEffect, useMemo, useRef, useState } from 'react';
import { Radio, AlertTriangle, Send, Users, MessageSquare } from 'lucide-react';
import { api } from '../api/client';
import { useMultiOpStore } from '../store/multiOpStore';
import { useSettingsStore } from '../store/settingsStore';

const BANDS = ['160m', '80m', '60m', '40m', '30m', '20m', '17m', '15m', '12m', '10m', '6m', '2m', '70cm'];
const MODES = ['CW', 'USB', 'LSB', 'FM', 'RTTY', 'FT8', 'FT4', 'PSK'];

/** A stable per-machine id so a station's presence updates in place across reloads. */
function useStationId(): string {
  return useMemo(() => {
    let id = localStorage.getItem('multiop.stationId');
    if (!id) {
      id = (crypto.randomUUID?.() ?? `st-${Date.now()}-${Math.floor(Math.random() * 1e6)}`);
      localStorage.setItem('multiop.stationId', id);
    }
    return id;
  }, []);
}

/**
 * Multi-op coordination panel (S-COORD): declare the band/mode you're on, see the "who's on what" board
 * with RF-collision warnings (a nearby rig on your band can desense your RX), and chat with the other
 * operators. Presence + messages ride the shared event bus through the host.
 */
export function CoordPlugin() {
  const stationId = useStationId();
  const myCall = useSettingsStore((s) => s.settings.station.callsign);
  const { presence, messages, setPresence } = useMultiOpStore();

  const [band, setBand] = useState<string>('');
  const [mode, setMode] = useState<string>('');
  const [draft, setDraft] = useState('');
  const msgEndRef = useRef<HTMLDivElement>(null);

  // Seed the board once, then live updates arrive via SignalR into the store.
  useEffect(() => { api.getPresenceBoard().then(setPresence).catch(() => {}); }, [setPresence]);

  // Announce our band/mode whenever it changes.
  useEffect(() => {
    if (!band && !mode) return;
    api.reportPresence(stationId, myCall || undefined, band || undefined, mode || undefined)
      .then(setPresence).catch(() => {});
  }, [band, mode, stationId, myCall, setPresence]);

  useEffect(() => { msgEndRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages]);

  const others = presence.filter((p) => p.stationId !== stationId);
  const sameBandMode = others.filter((p) => band && p.band === band && p.mode === mode);
  const sameBand = others.filter((p) => band && p.band === band && p.mode !== mode);

  const send = async () => {
    const text = draft.trim();
    if (!text) return;
    setDraft('');
    await api.sendCoordMessage(stationId, myCall || undefined, text).catch(() => {});
  };

  return (
    <div className="flex flex-col h-full p-3 gap-3 text-sm">
      <div className="flex items-center gap-2 text-white font-semibold">
        <Users className="w-4 h-4 text-accent-secondary" /> Multi-op Coordination
      </div>

      {/* Your band/mode */}
      <div className="flex items-center gap-2">
        <Radio className="w-4 h-4 text-dark-300 shrink-0" />
        <span className="text-dark-300 text-xs">You're on</span>
        <select value={band} onChange={(e) => setBand(e.target.value)} className="glass-input text-xs px-2 py-1">
          <option value="">band…</option>
          {BANDS.map((b) => <option key={b} value={b}>{b}</option>)}
        </select>
        <select value={mode} onChange={(e) => setMode(e.target.value)} className="glass-input text-xs px-2 py-1">
          <option value="">mode…</option>
          {MODES.map((m) => <option key={m} value={m}>{m}</option>)}
        </select>
      </div>

      {/* RF-collision warning */}
      {sameBandMode.length > 0 && (
        <div className="p-2 rounded-lg bg-red-500/15 border border-red-500/40 text-red-200 text-xs flex items-start gap-2">
          <AlertTriangle className="w-4 h-4 mt-0.5 shrink-0" />
          <span><strong>Watch out</strong> — {sameBandMode.map((p) => p.operator || p.stationId).join(', ')} {sameBandMode.length === 1 ? 'is' : 'are'} also on <strong>{band} {mode}</strong>. Same band + mode — high desense risk.</span>
        </div>
      )}
      {sameBand.length > 0 && (
        <div className="p-2 rounded-lg bg-amber-500/10 border border-amber-500/30 text-amber-200 text-xs flex items-start gap-2">
          <AlertTriangle className="w-4 h-4 mt-0.5 shrink-0" />
          <span><strong>Take care</strong> — {sameBand.map((p) => `${p.operator || p.stationId} (${p.mode})`).join(', ')} on <strong>{band}</strong> too. Different mode, but same band.</span>
        </div>
      )}

      {/* Who's on what */}
      <div>
        <div className="text-[10px] uppercase tracking-wider text-dark-400 mb-1">Stations on the air</div>
        <div className="space-y-1">
          {others.length === 0 && <div className="text-dark-400 text-xs">No other stations reporting.</div>}
          {others.map((p) => {
            const clash = band && p.band === band;
            return (
              <div key={p.stationId}
                className={`flex items-center justify-between px-2 py-1 rounded text-xs ${
                  clash && p.mode === mode ? 'bg-red-500/10' : clash ? 'bg-amber-500/10' : 'bg-glass-50'}`}>
                <span className="text-gray-200">{p.operator || p.stationId}</span>
                <span className="text-dark-300">{p.band || '—'} {p.mode || ''}</span>
              </div>
            );
          })}
        </div>
      </div>

      {/* Chat */}
      <div className="flex flex-col flex-1 min-h-0">
        <div className="text-[10px] uppercase tracking-wider text-dark-400 mb-1 flex items-center gap-1">
          <MessageSquare className="w-3 h-3" /> Messages
        </div>
        <div className="flex-1 min-h-0 overflow-y-auto space-y-1 pr-1">
          {messages.length === 0 && <div className="text-dark-400 text-xs">No messages yet.</div>}
          {messages.map((m, i) => (
            <div key={i} className="text-xs">
              <span className="text-accent-secondary font-medium">{m.operator || m.stationId}</span>
              <span className="text-dark-500 ml-1">{new Date(m.sentUtc).toLocaleTimeString()}</span>
              <div className="text-gray-200">{m.text}</div>
            </div>
          ))}
          <div ref={msgEndRef} />
        </div>
        <div className="flex gap-2 mt-2">
          <input value={draft} onChange={(e) => setDraft(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter') send(); }}
            placeholder="Message the other ops…" className="glass-input flex-1 text-xs px-2 py-1.5" />
          <button onClick={send} className="px-2 py-1.5 rounded bg-accent-secondary/20 text-accent-secondary border border-accent-secondary/40 hover:bg-accent-secondary/30">
            <Send className="w-4 h-4" />
          </button>
        </div>
      </div>
    </div>
  );
}

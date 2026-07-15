import { useEffect, useMemo, useState } from 'react';
import { Radio, X } from 'lucide-react';
import { api } from '../api/client';
import type { WsjtxDecodeEvent } from '../api/signalr';
import { GlassPanel } from '../components/GlassPanel';
import { useWsjtxDecodeStore } from '../store/wsjtxDecodeStore';
import { useSettingsStore } from '../store/settingsStore';
import { useToastStore } from '../store/toastStore';

/**
 * Live WSJT-X / JTDX / MSHV decode list (Phase 1 of the decode-alerts feature).
 * Shows every decoded FT8/FT4 transmission as it arrives over the WSJT-X UDP
 * protocol, coloured by the operator's needed-status (new DXCC / band / zone),
 * reusing the same verdict the DX cluster panel uses. Decodes stream live via
 * SignalR (onWsjtxDecode → wsjtxDecodeStore); the panel seeds recent decodes on
 * open from GET /api/wsjtx/decodes.
 *
 * Read-only for now — alert rules, click-to-call and the grid map come in later
 * phases (see docs/design/wsjtx-decode-alerts-design.md).
 */
export function DecodesPlugin() {
  const decodes = useWsjtxDecodeStore((s) => s.decodes);
  const seed = useWsjtxDecodeStore((s) => s.seed);
  const clear = useWsjtxDecodeStore((s) => s.clear);
  const colors = useSettingsStore((s) => s.settings.spotStatus.colors);

  const [neededOnly, setNeededOnly] = useState(false);
  const [cqOnly, setCqOnly] = useState(false);

  // Backfill recent decodes when the panel mounts.
  useEffect(() => {
    let active = true;
    api.getWsjtxDecodes()
      .then((list) => { if (active && list.length) seed(list); })
      .catch(() => { /* backend not ready — live stream will fill it in */ });
    return () => { active = false; };
  }, [seed]);

  const filtered = useMemo(() => {
    return decodes.filter((d) => {
      if (cqOnly && !d.isCq) return false;
      if (neededOnly && !isNeeded(d)) return false;
      return true;
    });
  }, [decodes, cqOnly, neededOnly]);

  const neededCount = useMemo(() => decodes.filter(isNeeded).length, [decodes]);

  // Double-click a decode → tell the decoder (WSJT-X/JTDX/MSHV) to answer that CQ.
  const callStation = async (d: WsjtxDecodeEvent) => {
    const toast = useToastStore.getState();
    try {
      const r = await api.sendWsjtxReply(d);
      if (r.sent) toast.push(`📞 Calling ${d.callsign} — answer set up in your decoder`, 'success');
      else toast.push(`Couldn't reach the decoder to call ${d.callsign}`, 'error');
    } catch {
      toast.push(`Call request failed for ${d.callsign}`, 'error');
    }
  };

  return (
    <GlassPanel
      title="Digital Decodes"
      icon={<Radio className="w-5 h-5" />}
      actions={
        <span className="text-xs text-dark-300 font-mono">
          {decodes.length}{neededCount > 0 && <span className="text-accent-primary"> · {neededCount} needed</span>}
        </span>
      }
    >
      {/* Filter strip */}
      <div className="px-3 py-2 border-b border-glass-100 flex items-center gap-2 flex-wrap text-xs">
        <FilterPill active={neededOnly} onClick={() => setNeededOnly((v) => !v)} title="Show only decodes that fill an award need (new DXCC / band / zone)">
          Needed only
        </FilterPill>
        <FilterPill active={cqOnly} onClick={() => setCqOnly((v) => !v)} title="Show only stations calling CQ (available to work)">
          CQ only
        </FilterPill>
        <span className="flex-1" />
        {decodes.length > 0 && (
          <button
            onClick={clear}
            className="px-2 py-1 rounded border border-dark-500 text-dark-200 hover:bg-dark-700 font-mono text-[10px] flex items-center gap-1"
            title="Clear the decode list"
          >
            <X className="w-3 h-3" /> CLEAR
          </button>
        )}
      </div>

      <div className="h-full overflow-y-auto">
        {filtered.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 text-gray-500 text-center px-6">
            <Radio className="w-12 h-12 mb-3 opacity-40" />
            <p>{decodes.length === 0 ? 'No decodes yet' : 'No decodes match the filters'}</p>
            {decodes.length === 0 && (
              <p className="text-xs mt-1 max-w-xs">
                Enable <span className="font-mono">WSJT-X Source 1</span> in Settings and turn on
                “Enable Decoded Text” in WSJT-X / JTDX / MSHV.
              </p>
            )}
          </div>
        ) : (
          <table className="w-full text-sm border-collapse">
            <thead className="sticky top-0 bg-dark-800/95 backdrop-blur text-[11px] uppercase tracking-wider text-dark-400">
              <tr>
                <th className="text-left font-medium px-2 py-1.5">Time</th>
                <th className="text-left font-medium px-2 py-1.5">Call</th>
                <th className="text-right font-medium px-2 py-1.5">dB</th>
                <th className="text-right font-medium px-2 py-1.5">Freq</th>
                <th className="text-left font-medium px-2 py-1.5">Grid</th>
                <th className="text-left font-medium px-2 py-1.5">Country</th>
                <th className="text-left font-medium px-2 py-1.5">Status</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((d, i) => (
                <DecodeRow key={`${d.decodedAtUtc}-${d.callsign}-${i}`} d={d} colors={colors} onCall={callStation} />
              ))}
            </tbody>
          </table>
        )}
      </div>
    </GlassPanel>
  );
}

function DecodeRow({ d, colors, onCall }: {
  d: WsjtxDecodeEvent;
  colors: { newDxcc: string; newBand: string; worked: string };
  onCall: (d: WsjtxDecodeEvent) => void;
}) {
  const status = decodeStatus(d, colors);
  return (
    <tr
      className="border-b border-glass-100/40 hover:bg-white/[0.03] cursor-pointer"
      title={`Double-click to call ${d.callsign} (answer this CQ in your decoder)`}
      onDoubleClick={() => onCall(d)}
      style={status.needed ? { borderLeft: `3px solid ${status.color}`, background: `${status.color}12` } : undefined}
    >
      <td className="px-2 py-1 font-mono text-[11px] text-dark-400 whitespace-nowrap">{fmtTime(d.decodedAtUtc)}</td>
      <td className="px-2 py-1 font-mono font-bold whitespace-nowrap" style={status.color ? { color: status.color } : undefined}>
        {d.isCq && <span className="text-[9px] text-accent-primary/80 mr-1 align-middle">CQ</span>}
        {d.callsign}
      </td>
      <td className="px-2 py-1 text-right font-mono text-dark-300 tabular-nums">{d.snr > 0 ? `+${d.snr}` : d.snr}</td>
      <td className="px-2 py-1 text-right font-mono text-dark-300 whitespace-nowrap tabular-nums">
        {d.frequencyHz > 0
          ? <span title={`${(d.frequencyHz / 1e6).toFixed(6)} MHz`}>{d.band ?? `${(d.frequencyHz / 1e6).toFixed(3)}`}</span>
          : <span className="text-dark-500">—</span>}
      </td>
      <td className="px-2 py-1 font-mono text-dark-300">{d.grid ?? ''}</td>
      <td className="px-2 py-1 text-dark-300 truncate max-w-[140px]" title={d.country ?? ''}>{d.country ?? ''}</td>
      <td className="px-2 py-1">
        {status.label && (
          <span
            className="text-[10px] font-bold px-1.5 py-0.5 rounded whitespace-nowrap"
            style={{ color: status.color, backgroundColor: `${status.color}22` }}
          >
            {status.label}
          </span>
        )}
      </td>
    </tr>
  );
}

function FilterPill({ active, onClick, title, children }: { active: boolean; onClick: () => void; title: string; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      title={title}
      className={`px-2 py-1 rounded border font-mono text-[10px] tracking-wider transition-colors ${
        active
          ? 'bg-accent-primary/20 border-accent-primary text-accent-primary'
          : 'bg-dark-700 border-dark-500 text-dark-300 hover:bg-dark-600'
      }`}
    >
      {children}
    </button>
  );
}

/** Whether a decode fills an award need (drives the "Needed only" filter + highlight). */
function isNeeded(d: WsjtxDecodeEvent): boolean {
  return d.spotStatus === 'newDxcc' || d.spotStatus === 'newBand'
    || d.zoneStatus === 'newZone' || d.zoneStatus === 'newZoneBand'
    || d.gridStatus === 'newGrid' || d.gridStatus === 'newGridBand';
}

const ZONE_COLOR = '#00e5ff';
const GRID_COLOR = '#ff8c3b';

function decodeStatus(d: WsjtxDecodeEvent, colors: { newDxcc: string; newBand: string; worked: string }):
  { label: string; color?: string; needed: boolean } {
  // Most-valuable award wins the badge: DXCC → band → zone → grid.
  if (d.spotStatus === 'newDxcc') return { label: 'New DXCC', color: colors.newDxcc, needed: true };
  if (d.spotStatus === 'newBand') return { label: 'New Band', color: colors.newBand, needed: true };
  if (d.zoneStatus === 'newZone') return { label: 'New Zone', color: ZONE_COLOR, needed: true };
  if (d.zoneStatus === 'newZoneBand') return { label: 'Zone+Band', color: ZONE_COLOR, needed: true };
  if (d.gridStatus === 'newGrid') return { label: 'New Grid', color: GRID_COLOR, needed: true };
  if (d.gridStatus === 'newGridBand') return { label: 'Grid+Band', color: GRID_COLOR, needed: true };
  if (d.spotStatus === 'worked') return { label: 'Worked', color: colors.worked, needed: false };
  return { label: '', needed: false };
}

function fmtTime(iso: string): string {
  const dt = new Date(iso);
  if (isNaN(dt.getTime())) return '';
  return dt.toISOString().slice(11, 19); // HH:MM:SS UTC
}

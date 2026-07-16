import { useMemo, useRef, useState, useCallback } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Grid3x3, Plus, Minus, Locate } from 'lucide-react';
import { api } from '../api/client';
import { GlassPanel } from '../components/GlassPanel';
import { useSettingsStore } from '../store/settingsStore';
import { useWsjtxDecodeStore } from '../store/wsjtxDecodeStore';
import worldRaw from '../geo/world-outline.json';

/**
 * Grid Tracker — a dedicated Maidenhead grid-square chart (Phase 4). Plots the
 * operator's worked grids on an equirectangular projection, colored worked
 * (solid green) / confirmed (…same, outline = worked-but-unconfirmed) / needed
 * (absent). Live FT8/FT4 decodes overlay: a station active in a grid RIGHT NOW
 * gets a cyan triangle; a station in a NEEDED grid pulses red+cyan ("chase this
 * cycle"). Not a slippy map — a purpose-built, high-contrast grid view that
 * aggregates instead of piling up. Base coordinate space is equirectangular:
 * x = lon+180 (0..360), y = 90-lat (0..180, north on top); each 4-char grid is
 * a 2°×1° rect. Pan by dragging, zoom with the wheel or +/- buttons.
 */

const GREEN = '#2f9e44';       // confirmed
const GREEN_DIM = '#2e6b34';   // worked, not confirmed
const CYAN = '#22d3ee';
const RED = '#e03131';

const BANDS = ['All', '160m', '80m', '40m', '30m', '20m', '17m', '15m', '12m', '10m', '6m', '2m', '70cm'];
const MODES = ['All', 'FT8', 'FT4', 'FT8/FT4', 'SSB', 'CW'];
const VUCC_THRESHOLD: Record<string, number> = { '6m': 100, '2m': 100, '70cm': 50, '23cm': 50 };

interface Box { x: number; y: number; w: number; h: number; }

function gridCorner(grid: string): { lon: number; lat: number } | null {
  const g = grid.toUpperCase();
  if (g.length < 4) return null;
  const c0 = g.charCodeAt(0) - 65, c1 = g.charCodeAt(1) - 65;
  const d2 = g.charCodeAt(2) - 48, d3 = g.charCodeAt(3) - 48;
  if (c0 < 0 || c0 > 17 || c1 < 0 || c1 > 17 || d2 < 0 || d2 > 9 || d3 < 0 || d3 > 9) return null;
  return { lon: c0 * 20 - 180 + d2 * 2, lat: c1 * 10 - 90 + d3 };
}

// Precompute country/coastline outline paths once, projected into the same
// equirectangular base space as the grids (x = lon+180, y = 90-lat). Gives the
// chart geographic context so it reads as a map, not floating squares.
const WORLD_PATHS: string[] = (() => {
  const feats = (worldRaw as { f: { t: string; c: number[][][] | number[][][][] }[] }).f;
  const ringPath = (ring: number[][]) => {
    let d = '';
    for (let i = 0; i < ring.length; i++) d += `${i ? 'L' : 'M'}${(ring[i][0] + 180).toFixed(1)},${(90 - ring[i][1]).toFixed(1)}`;
    return d + 'Z';
  };
  const paths: string[] = [];
  for (const f of feats) {
    let d = '';
    if (f.t === 'Polygon') for (const r of f.c as number[][][]) d += ringPath(r);
    else for (const poly of f.c as number[][][][]) for (const r of poly) d += ringPath(r);
    if (d) paths.push(d);
  }
  return paths;
})();

function lonLatToGrid(lon: number, lat: number): string | null {
  const nl = ((lon + 180) % 360 + 360) % 360;
  if (lat < -90 || lat >= 90) return null;
  const c0 = Math.floor(nl / 20), c1 = Math.floor((lat + 90) / 10);
  const d2 = Math.floor((nl % 20) / 2), d3 = Math.floor((lat + 90) % 10);
  return String.fromCharCode(65 + c0) + String.fromCharCode(65 + c1) + d2 + d3;
}

export function GridTrackerPlugin() {
  const myGrid = useSettingsStore((s) => s.settings.station.gridSquare) || 'EM79';
  const decodes = useWsjtxDecodeStore((s) => s.decodes);
  const [band, setBand] = useState('All');
  const [mode, setMode] = useState('All');
  const [showNeeded, setShowNeeded] = useState(false);

  const { data, isLoading } = useQuery({
    queryKey: ['gridmap', band, mode],
    queryFn: () => api.getGridMap(band === 'All' ? undefined : band, mode === 'All' ? undefined : mode),
    refetchInterval: 5 * 60 * 1000,
  });

  const worked = useMemo(() => {
    const m = new Map<string, boolean>(); // grid -> confirmed
    for (const g of data?.grids ?? []) m.set(g.grid, g.confirmed);
    return m;
  }, [data]);

  // Live grids from the decode stream: grid -> needed? (unworked & flagged)
  const live = useMemo(() => {
    const m = new Map<string, boolean>();
    for (const d of decodes) {
      if (!d.grid) continue;
      const g = d.grid.slice(0, 4).toUpperCase();
      // "needed" for the live overlay = not worked for the CURRENT filter — the
      // same test that colours the map. This keeps red strictly = "you don't
      // have this grid" and never turns a green (worked/confirmed) cell red.
      const needed = !worked.has(g);
      m.set(g, m.get(g) || needed);
    }
    return m;
  }, [decodes, worked]);

  // Default view centered on the operator's grid.
  const home = gridCorner(myGrid.slice(0, 4)) ?? { lon: -86, lat: 39 };
  const defaultBox: Box = { x: home.lon + 180 + 1 - 50, y: 90 - home.lat - 0.5 - 25, w: 100, h: 50 };
  const [box, setBox] = useState<Box>(defaultBox);

  const svgRef = useRef<SVGSVGElement>(null);
  const drag = useRef<{ px: number; py: number; bx: number; by: number } | null>(null);
  const [hover, setHover] = useState<{ grid: string; cx: number; cy: number } | null>(null);

  const onDown = (e: React.MouseEvent) => { drag.current = { px: e.clientX, py: e.clientY, bx: box.x, by: box.y }; };
  const onUp = () => { drag.current = null; };
  const onMove = (e: React.MouseEvent) => {
    const r = svgRef.current?.getBoundingClientRect();
    if (!r) return;
    // preserveAspectRatio="meet" scales the viewBox uniformly and centers it,
    // so there's a letterbox on one axis. Use the uniform scale + offsets for
    // both panning and the pointer→grid lookup, or the two disagree with the map.
    const scale = Math.min(r.width / box.w, r.height / box.h);
    if (drag.current) {
      setBox((b) => ({ ...b, x: drag.current!.bx - (e.clientX - drag.current!.px) / scale, y: drag.current!.by - (e.clientY - drag.current!.py) / scale }));
      setHover(null);
      return;
    }
    const offX = (r.width - box.w * scale) / 2, offY = (r.height - box.h * scale) / 2;
    const cx = e.clientX - r.left - offX, cy = e.clientY - r.top - offY;
    if (cx < 0 || cy < 0 || cx > box.w * scale || cy > box.h * scale) { setHover(null); return; }
    const g = lonLatToGrid(box.x + cx / scale - 180, 90 - (box.y + cy / scale));
    if (g) setHover({ grid: g, cx: e.clientX - r.left, cy: e.clientY - r.top });
  };

  const zoom = useCallback((factor: number) => {
    setBox((b) => {
      const w = Math.max(6, Math.min(360, b.w * factor));
      const h = Math.max(3, Math.min(180, b.h * factor));
      return { x: b.x + (b.w - w) / 2, y: b.y + (b.h - h) / 2, w, h };
    });
  }, []);
  const onWheel = (e: React.WheelEvent) => zoom(e.deltaY > 0 ? 1.15 : 0.87);

  const rects = useMemo(() => {
    const out: JSX.Element[] = [];
    worked.forEach((confirmed, g) => {
      const c = gridCorner(g);
      if (!c) return;
      out.push(
        <rect key={g} x={c.lon + 180} y={90 - c.lat - 1} width={2} height={1}
          fill={confirmed ? GREEN : GREEN_DIM} />,
      );
    });
    return out;
  }, [worked]);

  const liveRects = useMemo(() => {
    const out: JSX.Element[] = [];
    live.forEach((needed, g) => {
      const c = gridCorner(g);
      if (!c) return;
      const x = c.lon + 180, y = 90 - c.lat - 1;
      if (needed) {
        // Needed + active = "chase now": a solid red cell with a pulsing cyan ring.
        out.push(<rect key={`nf${g}`} x={x} y={y} width={2} height={1} fill={RED} />);
        out.push(<rect key={`nr${g}`} x={x} y={y} width={2} height={1} fill="none" stroke={CYAN} strokeWidth={2.5} vectorEffect="non-scaling-stroke" className="gt-pulse" />);
      } else {
        // Worked + active: cyan corner + a crisp full-cell cyan ring.
        out.push(<polygon key={`t${g}`} points={`${x + 2},${y} ${x + 2},${y + 1} ${x},${y + 1}`} fill={CYAN} opacity={0.85} />);
        out.push(<rect key={`wr${g}`} x={x} y={y} width={2} height={1} fill="none" stroke={CYAN} strokeWidth={1.6} vectorEffect="non-scaling-stroke" />);
      }
    });
    return out;
  }, [live]);

  // Faint 2°×1° cell divisions — shown only in Needed mode so the red land
  // reads as griddable squares instead of a solid wash. Worked mode stays clean.
  const cellLines = useMemo(() => {
    const out: JSX.Element[] = [];
    for (let lon = 0; lon <= 360; lon += 2) out.push(<line key={`cv${lon}`} x1={lon} y1={0} x2={lon} y2={180} stroke="#0a1119" strokeOpacity={0.85} strokeWidth={0.7} vectorEffect="non-scaling-stroke" />);
    for (let lat = 0; lat <= 180; lat += 1) out.push(<line key={`ch${lat}`} x1={0} y1={lat} x2={360} y2={lat} stroke="#0a1119" strokeOpacity={0.85} strokeWidth={0.7} vectorEffect="non-scaling-stroke" />);
    return out;
  }, []);

  const threshold = VUCC_THRESHOLD[band];
  const total = data?.totalGrids ?? 0;
  const confirmed = data?.confirmedGrids ?? 0;

  return (
    <GlassPanel title="Grid Tracker" icon={<Grid3x3 className="w-5 h-5" />}
      actions={<span className="text-xs text-dark-300 font-mono">{total} grids · {confirmed} conf</span>}>
      <div className="h-full flex flex-col">
        <style>{`@keyframes gtpulse{0%,100%{opacity:.4}50%{opacity:1}}.gt-pulse{animation:gtpulse 1.1s ease-in-out infinite}`}</style>
        <div className="px-3 py-2 border-b border-glass-100 flex items-center gap-2 flex-wrap text-xs">
          <select value={band} onChange={(e) => setBand(e.target.value)} className="glass-input py-1 px-2 text-xs">
            {BANDS.map((b) => <option key={b} value={b}>{b === 'All' ? 'All bands' : b}</option>)}
          </select>
          <select value={mode} onChange={(e) => setMode(e.target.value)} className="glass-input py-1 px-2 text-xs">
            {MODES.map((m) => <option key={m} value={m}>{m === 'All' ? 'All modes' : m}</option>)}
          </select>
          <button onClick={() => setShowNeeded((v) => !v)} title="Tint un-worked land red (VUCC needed view)"
            className={`px-2 py-1 rounded border text-xs font-medium ${showNeeded ? 'bg-red-500/20 border-red-400 text-red-300' : 'bg-dark-700 border-dark-500 text-dark-300 hover:bg-dark-600'}`}>
            Needed
          </button>
          <span className="flex-1" />
          <span className="flex items-center gap-2 text-[11px] text-dark-300">
            <span><span style={{ background: GREEN }} className="inline-block w-2.5 h-2.5 rounded-sm align-middle" /> worked/conf</span>
            <span><span style={{ background: RED }} className="inline-block w-2.5 h-2.5 rounded-sm align-middle" /> needed·live</span>
            <span><span style={{ background: CYAN }} className="inline-block w-2.5 h-2.5 rounded-sm align-middle" /> active</span>
          </span>
          <button onClick={() => zoom(0.8)} className="glass-button p-1" title="Zoom in"><Plus className="w-3.5 h-3.5" /></button>
          <button onClick={() => zoom(1.25)} className="glass-button p-1" title="Zoom out"><Minus className="w-3.5 h-3.5" /></button>
          <button onClick={() => setBox(defaultBox)} className="glass-button p-1" title="Recenter on my grid"><Locate className="w-3.5 h-3.5" /></button>
        </div>

        <div className="relative flex-1 min-h-0" style={{ background: '#0a1119' }}>
          {isLoading && <div className="absolute inset-0 flex items-center justify-center text-dark-400 text-sm">Loading grids…</div>}
          <svg ref={svgRef} viewBox={`${box.x} ${box.y} ${box.w} ${box.h}`} preserveAspectRatio="xMidYMid meet"
            width="100%" height="100%" style={{ cursor: drag.current ? 'grabbing' : 'crosshair', display: 'block' }}
            onMouseDown={onDown} onMouseUp={onUp} onMouseLeave={() => { onUp(); setHover(null); }} onMouseMove={onMove} onWheel={onWheel}>
            <g fill={showNeeded ? '#e0313126' : '#12283b'} stroke={showNeeded ? '#8a4a4a' : '#345a78'} strokeOpacity={0.75}>
              {WORLD_PATHS.map((d, i) => <path key={i} d={d} vectorEffect="non-scaling-stroke" strokeWidth={0.6} fillRule="evenodd" />)}
            </g>
            {rects}
            {showNeeded && cellLines}
            {liveRects}
            {(() => { const c = gridCorner(myGrid.slice(0, 4)); return c ? <rect x={c.lon + 180} y={90 - c.lat - 1} width={2} height={1} fill="none" stroke="#e0a030" strokeWidth={0.3} /> : null; })()}
            {hover && (() => { const c = gridCorner(hover.grid); return c ? <rect x={c.lon + 180} y={90 - c.lat - 1} width={2} height={1} fill="none" stroke="#ffffff" strokeWidth={1.6} vectorEffect="non-scaling-stroke" pointerEvents="none" /> : null; })()}
          </svg>
          {hover && (
            <div className="absolute pointer-events-none px-2 py-1 rounded bg-dark-900/95 border border-glass-100 text-xs font-mono text-dark-100"
              style={{ left: Math.min(hover.cx + 12, 9999), top: hover.cy + 12 }}>
              {hover.grid} — {worked.has(hover.grid) ? (worked.get(hover.grid) ? 'confirmed' : 'worked') : 'needed'}
              {live.has(hover.grid) && <span className="text-cyan-300"> · active now</span>}
            </div>
          )}
        </div>

        <div className="px-3 py-1.5 border-t border-glass-100 flex items-center gap-4 text-xs text-dark-300">
          <span>Worked <span className="text-dark-100 font-medium">{total}</span></span>
          <span>Confirmed <span className="text-green-400 font-medium">{confirmed}</span></span>
          {threshold && <span>VUCC {threshold}: <span className="text-amber-400 font-medium">{Math.max(0, threshold - confirmed)}</span> to go</span>}
          <span className="flex-1" />
          <span className="text-dark-500">drag to pan · scroll to zoom</span>
        </div>
      </div>
    </GlassPanel>
  );
}

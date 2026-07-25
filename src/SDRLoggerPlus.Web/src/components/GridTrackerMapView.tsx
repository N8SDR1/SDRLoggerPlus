import { useEffect, useRef, useState, useCallback } from 'react';
import { MapContainer, TileLayer, useMap } from 'react-leaflet';
import L from 'leaflet';
import { Maximize2, Search } from 'lucide-react';
import { TILE_LAYERS, type TileLayerKey } from '../geo/tileLayers';

const GREEN = '#2f9e44';      // confirmed
const GREEN_DIM = '#2e6b34';  // worked, not confirmed
const CYAN = '#22d3ee';
const RED = '#e03131';

const LABEL_ZOOM = 5;   // draw grid designators at/above this zoom
const CHASE_ZOOM = 4;   // draw the unworked graticule at/above this zoom
const MAX_CHASE = 2500; // safety cap on empty cells drawn per redraw

export interface GridLiveInfo {
  needed: boolean;
  calls: string[];
}

interface GridTrackerMapViewProps {
  /** grid (4-char, upper) -> confirmed? */
  worked: Map<string, boolean>;
  /** grid (4-char, upper) -> { needed this cycle, callsigns decoded there } */
  liveInfo: Map<string, GridLiveInfo>;
  /** Operator's own grid, for the home marker + initial center. */
  myGrid: string;
  basemap: TileLayerKey;
}

/** SW corner (lon/lat) of a 4-char Maidenhead grid, or null if malformed. */
function gridCorner(grid: string): { lon: number; lat: number } | null {
  const g = grid.toUpperCase();
  if (g.length < 4) return null;
  const c0 = g.charCodeAt(0) - 65, c1 = g.charCodeAt(1) - 65;
  const d2 = g.charCodeAt(2) - 48, d3 = g.charCodeAt(3) - 48;
  if (c0 < 0 || c0 > 17 || c1 < 0 || c1 > 17 || d2 < 0 || d2 > 9 || d3 < 0 || d3 > 9) return null;
  return { lon: c0 * 20 - 180 + d2 * 2, lat: c1 * 10 - 90 + d3 };
}

function boundsOf(grid: string): L.LatLngBoundsExpression | null {
  const c = gridCorner(grid);
  if (!c) return null;
  return [[c.lat, c.lon], [c.lat + 1, c.lon + 2]];
}

function centerOf(grid: string): { lat: number; lon: number } | null {
  const c = gridCorner(grid);
  return c ? { lat: c.lat + 0.5, lon: c.lon + 1 } : null;
}

/** 4-char grid containing a lon/lat point. */
function gridAt(lon: number, lat: number): string {
  const nl = ((lon + 180) % 360 + 360) % 360;
  const c0 = Math.floor(nl / 20), c1 = Math.floor((lat + 90) / 10);
  const d2 = Math.floor((nl % 20) / 2), d3 = Math.floor((lat + 90) % 10);
  return String.fromCharCode(65 + c0) + String.fromCharCode(65 + c1) + d2 + d3;
}

function labelIcon(text: string, dim = false): L.DivIcon {
  return L.divIcon({
    className: `gt-grid-label${dim ? ' gt-grid-label-dim' : ''}`,
    html: text,
    iconSize: [34, 12],
    iconAnchor: [17, 6],
  });
}

/**
 * Static worked/confirmed + live-decode cells + home marker. Rebuilt only when the data
 * changes (not on pan/zoom). Worked = translucent green/dim-green; a grid active in the live
 * decode stream gets a cyan ring (worked) or red fill + pulsing cyan ring (needed). Tooltips
 * name the grid and, for live cells, the callsign(s) heard there.
 */
function StaticCells({ worked, liveInfo, myGrid }: Omit<GridTrackerMapViewProps, 'basemap'>) {
  const map = useMap();
  const groupRef = useRef<L.LayerGroup | null>(null);

  useEffect(() => {
    const group = L.layerGroup().addTo(map);
    groupRef.current = group;
    // The map prefers canvas for speed, but the pulsing ring animates via a CSS class, which
    // only works on an SVG DOM node — give that one layer its own SVG renderer.
    const svgRenderer = L.svg({ padding: 0.5 });

    worked.forEach((confirmed, g) => {
      const b = boundsOf(g);
      if (!b) return;
      const color = confirmed ? GREEN : GREEN_DIM;
      L.rectangle(b, { color, weight: 1, opacity: 0.7, fillColor: color, fillOpacity: 0.35, interactive: true })
        .bindTooltip(`${g} — ${confirmed ? 'confirmed' : 'worked'}`, { sticky: true, direction: 'top' })
        .addTo(group);
    });

    liveInfo.forEach((info, g) => {
      const b = boundsOf(g);
      if (!b) return;
      const who = info.calls.length ? ` · ${info.calls.slice(0, 4).join(', ')}${info.calls.length > 4 ? '…' : ''}` : '';
      if (info.needed) {
        L.rectangle(b, { color: RED, weight: 1, opacity: 0.9, fillColor: RED, fillOpacity: 0.4, interactive: true })
          .bindTooltip(`${g} — needed · active now${who}`, { sticky: true, direction: 'top' })
          .addTo(group);
        L.rectangle(b, { color: CYAN, weight: 2.5, opacity: 1, fill: false, className: 'gt-pulse', interactive: false, renderer: svgRenderer })
          .addTo(group);
      } else {
        L.rectangle(b, { color: CYAN, weight: 1.8, opacity: 1, fill: false, interactive: true })
          .bindTooltip(`${g} — active now${who}`, { sticky: true, direction: 'top' })
          .addTo(group);
      }
    });

    const home = boundsOf(myGrid.slice(0, 4));
    if (home) L.rectangle(home, { color: '#e0a030', weight: 2, opacity: 0.9, fill: false, interactive: false }).addTo(group);

    return () => { group.remove(); groupRef.current = null; };
  }, [map, worked, liveInfo, myGrid]);

  return null;
}

/**
 * Zoom/viewport-dependent layers: grid-designator labels on worked cells (at LABEL_ZOOM+),
 * and — when "chase" is on — a faint outline of the UNWORKED grids in the current view so
 * you can hunt needed squares on the map. Redrawn on pan/zoom, capped for safety.
 */
function DynamicLayer({ worked, showChase }: { worked: Map<string, boolean>; showChase: boolean }) {
  const map = useMap();

  useEffect(() => {
    const group = L.layerGroup().addTo(map);

    const redraw = () => {
      group.clearLayers();
      const z = map.getZoom();
      const b = map.getBounds();

      // Labels on worked cells in view.
      if (z >= LABEL_ZOOM) {
        worked.forEach((_confirmed, g) => {
          const c = centerOf(g);
          if (!c || !b.contains([c.lat, c.lon])) return;
          L.marker([c.lat, c.lon], { icon: labelIcon(g), interactive: false, keyboard: false }).addTo(group);
        });
      }

      // Chase: outline unworked cells across the current viewport.
      if (showChase && z >= CHASE_ZOOM) {
        const west = Math.floor(b.getWest() / 2) * 2;
        const east = Math.ceil(b.getEast() / 2) * 2;
        const south = Math.floor(b.getSouth());
        const north = Math.ceil(b.getNorth());
        let count = 0;
        for (let lon = west; lon < east && count < MAX_CHASE; lon += 2) {
          for (let lat = south; lat < north && count < MAX_CHASE; lat += 1) {
            if (lat < -90 || lat >= 90) continue;
            const g = gridAt(lon + 1, lat + 0.5);
            if (worked.has(g)) continue;
            L.rectangle([[lat, lon], [lat + 1, lon + 2]], {
              color: '#5a6b7a', weight: 0.6, opacity: 0.5, fill: false, interactive: false,
            }).addTo(group);
            if (z >= LABEL_ZOOM) {
              L.marker([lat + 0.5, lon + 1], { icon: labelIcon(g, true), interactive: false, keyboard: false }).addTo(group);
            }
            count++;
          }
        }
      }
    };

    redraw();
    map.on('moveend zoomend', redraw);
    return () => { map.off('moveend zoomend', redraw); group.remove(); };
  }, [map, worked, showChase]);

  return null;
}

/** Lifts the Leaflet map instance up to the wrapper so overlay buttons can drive it. */
function MapReady({ onReady }: { onReady: (m: L.Map) => void }) {
  const map = useMap();
  useEffect(() => { onReady(map); }, [map, onReady]);
  return null;
}

export function GridTrackerMapView({ worked, liveInfo, myGrid, basemap }: GridTrackerMapViewProps) {
  const c = gridCorner(myGrid.slice(0, 4)) ?? { lon: -86, lat: 39 };
  const center: L.LatLngExpression = [c.lat + 0.5, c.lon + 1];
  const tiles = TILE_LAYERS[basemap] ?? TILE_LAYERS.dark;

  const [mapObj, setMapObj] = useState<L.Map | null>(null);
  const [showChase, setShowChase] = useState(false);

  const fitToWorked = useCallback(() => {
    if (!mapObj || worked.size === 0) return;
    const pts: L.LatLngExpression[] = [];
    worked.forEach((_v, g) => {
      const cc = gridCorner(g);
      if (cc) { pts.push([cc.lat, cc.lon]); pts.push([cc.lat + 1, cc.lon + 2]); }
    });
    if (pts.length) mapObj.fitBounds(L.latLngBounds(pts as L.LatLngBoundsLiteral), { padding: [20, 20] });
  }, [mapObj, worked]);

  return (
    <div className="relative w-full h-full">
      <style>{`
        .gt-grid-label{color:#cfe8ff;font:10px/12px ui-monospace,SFMono-Regular,Menlo,monospace;text-shadow:0 0 2px #000,0 0 2px #000;text-align:center;background:none;border:none;white-space:nowrap;pointer-events:none;}
        .gt-grid-label-dim{color:#7f93a6;}
      `}</style>
      <MapContainer
        center={center}
        zoom={4}
        worldCopyJump
        style={{ height: '100%', width: '100%', background: '#0a1119' }}
        preferCanvas
      >
        <TileLayer key={basemap} url={tiles.url} attribution={tiles.attribution} />
        <MapReady onReady={setMapObj} />
        <StaticCells worked={worked} liveInfo={liveInfo} myGrid={myGrid} />
        <DynamicLayer worked={worked} showChase={showChase} />
      </MapContainer>

      {/* Overlay controls (sit above the map, top-right) */}
      <div className="absolute top-2 right-2 z-[500] flex flex-col gap-1">
        <button onClick={fitToWorked} title="Zoom to fit all worked grids"
          className="glass-button p-1.5 bg-dark-800/90 hover:bg-dark-700">
          <Maximize2 className="w-4 h-4" />
        </button>
        <button onClick={() => setShowChase((v) => !v)} title="Show unworked grids in view (chase needed squares)"
          className={`glass-button p-1.5 bg-dark-800/90 hover:bg-dark-700 ${showChase ? 'text-amber-300 ring-1 ring-amber-400/60' : ''}`}>
          <Search className="w-4 h-4" />
        </button>
      </div>
    </div>
  );
}

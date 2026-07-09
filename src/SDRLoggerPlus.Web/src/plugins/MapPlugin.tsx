import React, { useEffect, useRef, useCallback, useState, useMemo } from 'react';
import { Map as MapIcon, Target, Maximize2, ZoomIn, ZoomOut, Layers, Satellite, Radio, Sun } from 'lucide-react';
import { MapContainer, TileLayer, Marker, Popup, useMapEvents, useMap, Circle, Polyline, CircleMarker, Tooltip } from 'react-leaflet';
import L from 'leaflet';
import { useAppStore, Spot } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import { useSignalR } from '../hooks/useSignalR';
import { GlassPanel } from '../components/GlassPanel';
import { DXNewsTicker } from '../components/DXNewsTicker';
import { DayNightOverlay } from '../components/DayNightOverlay';
import { GrayLineOverlay } from '../components/GrayLineOverlay';
import { AuroraOverlay } from '../components/AuroraOverlay';
import { PskReporterOverlay } from '../components/PskReporterOverlay';
import { gridToLatLon, calculateDistance, calculateBearing, getAnimationDuration } from '../utils/maidenhead';
import { fetchTLEData, calculateSatellitePosition, calculateOrbitTrack, type SatellitePosition, type SatelliteTLE } from '../utils/satellite';
import { api, type RbnSpot } from '../api/client';
import { GlobeCore } from './GlobePlugin';
import { RotatorControls } from './RotatorPlugin';

import 'leaflet/dist/leaflet.css';

// Fix for default marker icons in webpack/vite
delete (L.Icon.Default.prototype as unknown as { _getIconUrl?: unknown })._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
});

// Default station location (IO52RN - Limerick area)
const DEFAULT_LAT = 52.6667;
const DEFAULT_LON = -8.6333;

// Custom station marker
const stationIcon = new L.DivIcon({
  className: 'custom-station-marker',
  html: `
    <div style="
      width: 24px;
      height: 24px;
      background: linear-gradient(135deg, #00ddff, #00bbdd);
      border: 3px solid #fff;
      border-radius: 50%;
      box-shadow: 0 0 10px rgba(0, 221, 255, 0.6);
    "></div>
  `,
  iconSize: [24, 24],
  iconAnchor: [12, 12],
});

// Custom target marker
const targetIcon = new L.DivIcon({
  className: 'custom-target-marker',
  html: `
    <div style="
      width: 20px;
      height: 20px;
      background: linear-gradient(135deg, #ffb432, #ff4466);
      border: 2px solid #fff;
      border-radius: 50%;
      box-shadow: 0 0 8px rgba(255, 180, 50, 0.6);
    "></div>
  `,
  iconSize: [20, 20],
  iconAnchor: [10, 10],
});

// Custom POTA marker (pine tree)
const potaIcon = new L.DivIcon({
  className: 'custom-pota-marker',
  html: `
    <div style="filter: drop-shadow(0 0 4px rgba(16, 185, 129, 0.5));">
      <svg width="20" height="24" viewBox="0 0 20 24" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M10 1L3 10h3L2 16h5v6h6v-6h5l-4-6h3L10 1z" fill="#10b981" stroke="#065f46" stroke-width="0.75"/>
        <rect x="8" y="16" width="4" height="6" fill="#7c5e3c" stroke="#5c4033" stroke-width="0.5"/>
      </svg>
    </div>
  `,
  iconSize: [20, 24],
  iconAnchor: [10, 24],
});

// Custom satellite marker (purple/magenta diamond)
const satelliteIcon = new L.DivIcon({
  className: 'custom-satellite-marker',
  html: `
    <div style="
      width: 18px;
      height: 18px;
      background: linear-gradient(135deg, #a855f7, #ec4899);
      border: 2px solid #fff;
      transform: rotate(45deg);
      box-shadow: 0 0 8px rgba(168, 85, 247, 0.6);
    "></div>
  `,
  iconSize: [18, 18],
  iconAnchor: [9, 9],
});

// Create callsign image marker icon — a map pin (teardrop) whose circular
// hole holds the operator's QRZ photo; the pin's point anchors on the
// station coordinate, callsign label sits just below.
function createCallsignImageIcon(imageUrl: string | undefined | null, callsign: string, scale: '1x' | '2x') {
  const pinW = scale === '2x' ? 52 : 42;   // pin width in px
  const pinH = Math.round(pinW * 1.32);    // teardrop taller than wide
  // Panel-name green (accent-secondary), via the CSS var so it tracks themes.
  const bodyColor = 'rgb(var(--accent-secondary))';
  const fontSize = scale === '2x' ? 12 : 10;
  const emojiPx = Math.round(pinW * 0.42);
  // Escape HTML special chars in callsign to prevent XSS
  const safeCallsign = callsign.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  // Photo (or 📻 fallback) rides in a foreignObject over the pin's hole, so
  // the browser's <img> onerror fallback still works inside the SVG.
  const holeContent = imageUrl
    ? `<img src="${imageUrl}" alt="${safeCallsign}"
            style="width:100%;height:100%;object-fit:cover;display:block;"
            onerror="this.parentElement.innerHTML='<div style=\\'display:flex;align-items:center;justify-content:center;width:100%;height:100%;font-size:${emojiPx}px\\'>📻</div>'" />`
    : `<div style="display:flex;align-items:center;justify-content:center;width:100%;height:100%;font-size:${emojiPx}px">📻</div>`;

  // Geometry in a 40×52 viewBox: circular bulge centered (20,18) r18,
  // tapering to the point at (20,50). Hole = circle (20,18) r14.5 — the
  // photo nearly fills the bulge, leaving a thin coloured rim.
  const pin = `
    <svg width="${pinW}" height="${pinH}" viewBox="0 0 40 52" xmlns="http://www.w3.org/2000/svg"
         style="display:block;filter:drop-shadow(0 2px 3px rgba(0,0,0,0.55));">
      <path d="M20 50 C 9 33, 2 27, 2 18 A 18 18 0 1 1 38 18 C 38 27, 31 33, 20 50 Z"
            style="fill:${bodyColor}" stroke="#0a0e14" stroke-width="2.5" stroke-linejoin="round" />
      <foreignObject x="5.5" y="3.5" width="29" height="29">
        <div xmlns="http://www.w3.org/1999/xhtml"
             style="width:29px;height:29px;border-radius:50%;overflow:hidden;background:#1a1e26;">
          ${holeContent}
        </div>
      </foreignObject>
    </svg>`;

  return new L.DivIcon({
    className: 'custom-callsign-image-marker',
    // translate so the pin's point (bottom-center of the SVG) sits on the
    // geographic coordinate; the label hangs just beneath it.
    html: `
      <div style="
        display: flex;
        flex-direction: column;
        align-items: center;
        transform: translate(-50%, -${pinH}px);
        pointer-events: auto;
      ">
        ${pin}
        <span style="
          font-family: monospace;
          font-size: ${fontSize}px;
          font-weight: bold;
          color: ${bodyColor};
          text-shadow: 0 0 4px rgba(0,0,0,0.85), 0 1px 2px rgba(0,0,0,0.9);
          white-space: nowrap;
          line-height: 1;
          margin-top: 1px;
        ">${safeCallsign}</span>
      </div>
    `,
    iconSize: [0, 0],
    iconAnchor: [0, 0],
  });
}

// Tile layer options for different map styles
const TILE_LAYERS = {
  osm: {
    name: 'OpenStreetMap',
    url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
  },
  dark: {
    name: 'Dark',
    url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>',
  },
  satellite: {
    name: 'Satellite',
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    attribution: '&copy; Esri',
  },
  terrain: {
    name: 'Terrain',
    url: 'https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',
    attribution: '&copy; <a href="https://opentopomap.org">OpenTopoMap</a>',
  },
};

type TileLayerKey = keyof typeof TILE_LAYERS;

// Map click handler component
function MapClickHandler({
  stationLat,
  stationLon,
  onBearingClick
}: {
  stationLat: number;
  stationLon: number;
  onBearingClick: (azimuth: number) => void;
}) {
  useMapEvents({
    click(e) {
      const { lat, lng } = e.latlng;
      const azimuth = calculateAzimuth(stationLat, stationLon, lat, lng);
      onBearingClick(azimuth);
    },
  });
  return null;
}

// Calculate azimuth between two points
function calculateAzimuth(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const toRad = Math.PI / 180;
  const toDeg = 180 / Math.PI;

  const dLon = (lon2 - lon1) * toRad;
  const lat1Rad = lat1 * toRad;
  const lat2Rad = lat2 * toRad;

  const y = Math.sin(dLon) * Math.cos(lat2Rad);
  const x = Math.cos(lat1Rad) * Math.sin(lat2Rad) -
      Math.sin(lat1Rad) * Math.cos(lat2Rad) * Math.cos(dLon);

  let azimuth = Math.atan2(y, x) * toDeg;
  azimuth = (azimuth + 360) % 360;

  return Math.round(azimuth);
}

// Calculate destination point from start, azimuth, and distance
function getDestinationPoint(lat: number, lon: number, azimuth: number, distanceKm: number): [number, number] {
  const R = 6371;
  const toRad = Math.PI / 180;
  const toDeg = 180 / Math.PI;

  const lat1Rad = lat * toRad;
  const lon1Rad = lon * toRad;
  const azimuthRad = azimuth * toRad;
  const angularDistance = distanceKm / R;

  const lat2Rad = Math.asin(
    Math.sin(lat1Rad) * Math.cos(angularDistance) +
    Math.cos(lat1Rad) * Math.sin(angularDistance) * Math.cos(azimuthRad)
  );

  const lon2Rad = lon1Rad + Math.atan2(
    Math.sin(azimuthRad) * Math.sin(angularDistance) * Math.cos(lat1Rad),
    Math.cos(angularDistance) - Math.sin(lat1Rad) * Math.sin(lat2Rad)
  );

  return [lat2Rad * toDeg, ((lon2Rad * toDeg + 540) % 360) - 180];
}


// Band colors for DX cluster spot paths (matching typical ham radio conventions)
const BAND_COLORS: Record<string, string> = {
  '160m': '#8B0000', // dark red
  '80m': '#DC143C',  // crimson
  '60m': '#FF6347',  // tomato
  '40m': '#FF8C00',  // dark orange
  '30m': '#FFD700',  // gold
  '20m': '#32CD32',  // lime green
  '17m': '#00CED1',  // dark turquoise
  '15m': '#00BFFF',  // deep sky blue
  '12m': '#4169E1',  // royal blue
  '10m': '#8A2BE2',  // blue violet
  '6m': '#FF00FF',   // magenta
};

const BAND_RANGES: Record<string, [number, number]> = {
  '160m': [1800, 2000],
  '80m': [3500, 4000],
  '60m': [5330, 5410],
  '40m': [7000, 7300],
  '30m': [10100, 10150],
  '20m': [14000, 14350],
  '17m': [18068, 18168],
  '15m': [21000, 21450],
  '12m': [24890, 24990],
  '10m': [28000, 29700],
  '6m': [50000, 54000],
};

function getBandFromFrequency(freq: number): string {
  for (const [band, [min, max]] of Object.entries(BAND_RANGES)) {
    if (freq >= min && freq <= max) return band;
  }
  return '?';
}

// Generate intermediate great-circle path points between two coordinates
// Returns array of path segments to handle antimeridian crossing
function generateGreatCirclePoints(
  lat1: number, lon1: number, lat2: number, lon2: number, numPoints = 50
): [number, number][][] {
  const points: [number, number][] = [];
  const toRad = Math.PI / 180;
  const toDeg = 180 / Math.PI;

  const lat1Rad = lat1 * toRad;
  const lon1Rad = lon1 * toRad;
  const lat2Rad = lat2 * toRad;
  const lon2Rad = lon2 * toRad;

  // Angular distance between the two points
  const d = 2 * Math.asin(Math.sqrt(
    Math.pow(Math.sin((lat1Rad - lat2Rad) / 2), 2) +
    Math.cos(lat1Rad) * Math.cos(lat2Rad) * Math.pow(Math.sin((lon1Rad - lon2Rad) / 2), 2)
  ));

  if (d === 0) return [[[lat1, lon1]]];

  for (let i = 0; i <= numPoints; i++) {
    const f = i / numPoints;
    const A = Math.sin((1 - f) * d) / Math.sin(d);
    const B = Math.sin(f * d) / Math.sin(d);

    const x = A * Math.cos(lat1Rad) * Math.cos(lon1Rad) + B * Math.cos(lat2Rad) * Math.cos(lon2Rad);
    const y = A * Math.cos(lat1Rad) * Math.sin(lon1Rad) + B * Math.cos(lat2Rad) * Math.sin(lon2Rad);
    const z = A * Math.sin(lat1Rad) + B * Math.sin(lat2Rad);

    const lat = Math.atan2(z, Math.sqrt(x * x + y * y)) * toDeg;
    const lon = Math.atan2(y, x) * toDeg;
    points.push([lat, lon]);
  }

  // Split path segments at antimeridian (±180° longitude) crossings
  const segments: [number, number][][] = [];
  let currentSegment: [number, number][] = [points[0]];

  for (let i = 1; i < points.length; i++) {
    const prevLon = points[i - 1][1];
    const currLon = points[i][1];
    const lonDiff = Math.abs(currLon - prevLon);

    // Detect antimeridian crossing (longitude jump > 180°)
    if (lonDiff > 180) {
      // Finish current segment
      segments.push(currentSegment);
      // Start new segment
      currentSegment = [points[i]];
    } else {
      currentSegment.push(points[i]);
    }
  }

  // Add the last segment
  if (currentSegment.length > 0) {
    segments.push(currentSegment);
  }

  return segments;
}

/**
 * Animated great-circle path drawn as a traveling sine wave. Each base point
 * of the segment is offset perpendicular to the local path direction by a
 * sine whose phase advances every frame (so the wave travels from the station
 * toward the DX). A sin() envelope tapers the amplitude to zero at both ends,
 * keeping the wave anchored to the two markers. Rendered as a direct Leaflet
 * layer (updated in a rAF loop, no React re-render per frame); colour + the
 * gentle opacity breath come from the shared .dx-target-path CSS class.
 */
function SineWavePath({ segment }: { segment: [number, number][] }) {
  const map = useMap();
  useEffect(() => {
    if (!segment || segment.length < 3) return;
    const toRad = Math.PI / 180, toDeg = 180 / Math.PI, R = 6371;
    // Cumulative along-path distance (km) of the coarse base points.
    const baseCum = [0];
    for (let i = 1; i < segment.length; i++) {
      baseCum[i] = baseCum[i - 1] + calculateDistance(segment[i - 1][0], segment[i - 1][1], segment[i][0], segment[i][1]);
    }
    const total = baseCum[segment.length - 1];
    if (total <= 0) return;

    // Wave geometry proportional to path length so density/height read the
    // same for a 500 km hop or a 15 000 km path.
    const waves = Math.max(4, Math.min(22, Math.round(total / 900)));
    const wavelengthKm = total / waves;
    const amplitudeKm = Math.max(25, Math.min(450, wavelengthKm * 0.28));

    // Resample the coarse great-circle path to many evenly-spaced points —
    // ~28 per wave — so each oscillation is a smooth curve, not a few straight
    // segments. Linear interpolation between adjacent base points is fine at
    // this spacing (they're only tens of km apart).
    const N = Math.max(160, Math.min(900, waves * 28));
    const dense: { lat: number; lon: number; d: number }[] = [];
    let j = 0;
    for (let k = 0; k <= N; k++) {
      const target = (total * k) / N;
      while (j < segment.length - 2 && baseCum[j + 1] < target) j++;
      const segLen = baseCum[j + 1] - baseCum[j];
      const t = segLen > 0 ? (target - baseCum[j]) / segLen : 0;
      dense.push({
        lat: segment[j][0] + (segment[j + 1][0] - segment[j][0]) * t,
        lon: segment[j][1] + (segment[j + 1][1] - segment[j][1]) * t,
        d: target,
      });
    }

    // Perpendicular bearing at each dense point (path bearing + 90°).
    const perp = dense.map((_, i) => {
      const a = dense[Math.max(0, i - 1)];
      const b = dense[Math.min(dense.length - 1, i + 1)];
      return (calculateBearing(a.lat, a.lon, b.lat, b.lon) + 90) % 360;
    });

    // Offset a lat/lon by a signed distance (km) along a bearing.
    const offset = (lat: number, lon: number, brg: number, distKm: number): [number, number] => {
      let b = brg, d = distKm;
      if (d < 0) { b = (brg + 180) % 360; d = -d; }
      const delta = d / R, theta = b * toRad, phi1 = lat * toRad, lam1 = lon * toRad;
      const phi2 = Math.asin(Math.sin(phi1) * Math.cos(delta) + Math.cos(phi1) * Math.sin(delta) * Math.cos(theta));
      const lam2 = lam1 + Math.atan2(Math.sin(theta) * Math.sin(delta) * Math.cos(phi1), Math.cos(delta) - Math.sin(phi1) * Math.sin(phi2));
      return [phi2 * toDeg, lam2 * toDeg];
    };

    // Wave points for a given phase.
    const waveAt = (phase: number): [number, number][] =>
      dense.map((p, i) => {
        const envelope = Math.sin(Math.PI * p.d / total); // 0 at ends, 1 mid
        const off = amplitudeKm * envelope * Math.sin(2 * Math.PI * p.d / wavelengthKm - phase);
        return offset(p.lat, p.lon, perp[i], off);
      });

    // Draw the first frame synchronously so the smooth wave shows immediately
    // (independent of the rAF loop, which only drives the travel animation).
    // smoothFactor: 0 disables Leaflet's Douglas-Peucker simplification —
    // otherwise it decimates our dense wave points back into jagged segments.
    const poly = L.polyline(waveAt(0), { weight: 2, smoothFactor: 0, className: 'dx-target-path' }).addTo(map);
    let raf = 0;
    let phase = 0;
    const frame = () => {
      phase += 0.12; // travel speed (rad/frame)
      poly.setLatLngs(waveAt(phase));
      raf = requestAnimationFrame(frame);
    };
    raf = requestAnimationFrame(frame);

    return () => {
      cancelAnimationFrame(raf);
      map.removeLayer(poly);
    };
  }, [segment, map]);
  return null;
}

// Custom DX spot marker (small diamond)
function createSpotIcon(color: string, isHighlighted: boolean) {
  const size = isHighlighted ? 10 : 6;
  return new L.DivIcon({
    className: 'custom-spot-marker',
    html: `<div style="
      width: ${size}px;
      height: ${size}px;
      background: ${color};
      border: 1px solid rgba(255,255,255,${isHighlighted ? '0.9' : '0.5'});
      border-radius: 50%;
      box-shadow: 0 0 ${isHighlighted ? '8' : '3'}px ${color};
    "></div>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
}

// Smaller icon for spotter locations
function createSpotterIcon(color: string, isHighlighted: boolean) {
  const size = isHighlighted ? 6 : 4;
  return new L.DivIcon({
    className: 'custom-spotter-marker',
    html: `<div style="
      width: ${size}px;
      height: ${size}px;
      background: ${color};
      border: 1px solid rgba(255,255,255,${isHighlighted ? '0.7' : '0.3'});
      border-radius: 50%;
      opacity: 0.7;
    "></div>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
}

// Compute spot path data (memoizable)
interface SpotPathData {
  spot: Spot;
  band: string;
  color: string;
  targetLat: number;
  targetLon: number;
  pathPoints: [number, number][][]; // Array of path segments to handle antimeridian crossing
  spotterLat?: number;
  spotterLon?: number;
  spotterPathPoints?: [number, number][][]; // Path from spotter to DX station
}

/** Guard against NaN / undefined / null coordinates that crash Leaflet */
function isValidCoord(v: number | null | undefined): v is number {
  return typeof v === 'number' && Number.isFinite(v);
}

export function MapCore({ children, flyToOffsetX = 0 }: { children?: React.ReactNode; flyToOffsetX?: number }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const lastTargetCoordsRef = useRef<{ lat: number; lon: number } | null>(null);
  const { stationGrid, rotatorPosition, focusedCallsignInfo, potaSpots, dxClusterMapEnabled, hoveredSpotId } = useAppStore();
  const { settings, updateMapSettings, saveSettings } = useSettingsStore();
  const { commandRotator, selectSpot } = useSignalR();

  // DX cluster spots from ephemeral in-memory store (populated via SignalR)
  const spots = useAppStore((state) => state.dxClusterSpots);
  const [currentAzimuth, setCurrentAzimuth] = useState(0);
  const [showLayerPicker, setShowLayerPicker] = useState(false);
  const [satellitePositions, setSatellitePositions] = useState<Map<string, SatellitePosition>>(new Map());
  const [satelliteTLEs, setSatelliteTLEs] = useState<Map<string, SatelliteTLE>>(new Map());
  const [satelliteOrbits, setSatelliteOrbits] = useState<Map<string, Array<{ lat: number; lon: number }>>>(new Map());
  const [rbnSpots, setRbnSpots] = useState<RbnSpot[]>([]);
  const [showRbnPanel, setShowRbnPanel] = useState(false);
  const [showOverlayPanel, setShowOverlayPanel] = useState(false);

  // Get tile layer and RBN settings from persisted settings
  const tileLayer = settings.map.tileLayer;
  const rbnSettings = settings.map.rbn;

  // Rotator is enabled in settings
  const rotatorEnabled = settings.rotator.enabled;

  // Station coordinates - prioritize lat/lon from settings, fall back to grid square, then defaults
  let stationLat = DEFAULT_LAT;
  let stationLon = DEFAULT_LON;

  // First priority: explicit lat/lon in settings
  if (settings.station.latitude != null && settings.station.longitude != null) {
    stationLat = settings.station.latitude;
    stationLon = settings.station.longitude;
  }
  // Second priority: convert grid square to coordinates
  else if (settings.station.gridSquare) {
    const coords = gridToLatLon(settings.station.gridSquare);
    if (coords) {
      stationLat = coords.lat;
      stationLon = coords.lon;
    }
  }
  // Third priority: use stationGrid from appStore (legacy support)
  else if (stationGrid) {
    const coords = gridToLatLon(stationGrid);
    if (coords) {
      stationLat = coords.lat;
      stationLon = coords.lon;
    }
  }

  // Compute DX cluster spot paths for map overlay
  const spotPaths = useMemo<SpotPathData[]>(() => {
    if (!dxClusterMapEnabled || !spots?.length) return [];

    const paths: SpotPathData[] = [];

    for (const spot of spots) {
      if (!spot.dxStation?.grid) continue;

      const coords = gridToLatLon(spot.dxStation.grid);
      if (!coords) continue;

      const band = getBandFromFrequency(spot.frequency);
      const color = BAND_COLORS[band] || '#888888';

      // Path from station to DX
      const pathPoints = generateGreatCirclePoints(
        stationLat, stationLon, coords.lat, coords.lon
      );

      // If spotter location is available, calculate path from spotter to DX
      let spotterLat: number | undefined;
      let spotterLon: number | undefined;
      let spotterPathPoints: [number, number][][] | undefined;

      if (spot.spotterStation?.grid) {
        const spotterCoords = gridToLatLon(spot.spotterStation.grid);
        if (spotterCoords) {
          spotterLat = spotterCoords.lat;
          spotterLon = spotterCoords.lon;
          spotterPathPoints = generateGreatCirclePoints(
            spotterCoords.lat, spotterCoords.lon, coords.lat, coords.lon
          );
        }
      }

      paths.push({
        spot,
        band,
        color,
        targetLat: coords.lat,
        targetLon: coords.lon,
        pathPoints,
        spotterLat,
        spotterLon,
        spotterPathPoints
      });
    }

    return paths;
  }, [dxClusterMapEnabled, spots, stationLat, stationLon]);

  // Active bands for the legend (only bands that have spots visible)
  const activeBands = useMemo(() => {
    const bands = new Set(spotPaths.map(sp => sp.band));
    return Object.entries(BAND_COLORS).filter(([band]) => bands.has(band));
  }, [spotPaths]);

  // Update azimuth from rotator position only
  useEffect(() => {
    if (rotatorPosition?.currentAzimuth !== undefined) {
      setCurrentAzimuth(rotatorPosition.currentAzimuth);
    }
  }, [rotatorPosition]);

  // Update map center when station coordinates change
  useEffect(() => {
    if (mapRef.current) {
      mapRef.current.setView([stationLat, stationLon], mapRef.current.getZoom());
    }
  }, [stationLat, stationLon]);

  // Fly to target when focused callsign changes with distance-based animation
  useEffect(() => {
    if (!mapRef.current) return;

    const targetLat = focusedCallsignInfo?.latitude;
    const targetLon = focusedCallsignInfo?.longitude;

    // Only fly if we have valid target coordinates (guards against NaN from POTA spots etc.)
    if (!isValidCoord(targetLat) || !isValidCoord(targetLon)) return;

    // Calculate distance for animation duration
    let duration = 2; // Default duration
    const lastCoords = lastTargetCoordsRef.current;

    if (lastCoords) {
      // Calculate distance from previous target
      const distance = calculateDistance(lastCoords.lat, lastCoords.lon, targetLat, targetLon);
      duration = getAnimationDuration(distance);
    } else {
      // First target - calculate distance from station
      const distance = calculateDistance(stationLat, stationLon, targetLat, targetLon);
      duration = getAnimationDuration(distance);
    }

    // Update last coordinates ref
    lastTargetCoordsRef.current = { lat: targetLat, lon: targetLon };

    // Fly to target with calculated duration. When something overlays the
    // left side of the map (the 2D Map panel's globe circle), shift the map
    // center left by flyToOffsetX px so the target lands in the open area
    // to the right of the overlay instead of underneath it.
    let center: L.LatLngExpression = [targetLat, targetLon];
    if (flyToOffsetX > 0) {
      // Clamp so the target never lands off the right edge when the overlay
      // is nearly as wide as (or wider than) the map itself.
      const halfWidth = mapRef.current.getSize().x / 2;
      const offsetX = Math.min(flyToOffsetX, Math.max(0, halfWidth - 48));
      const targetPoint = mapRef.current.project([targetLat, targetLon], 5);
      center = mapRef.current.unproject(targetPoint.subtract(L.point(offsetX, 0)), 5);
    }
    mapRef.current.flyTo(center, 5, {
      duration: duration,
      easeLinearity: 0.1, // Smooth curved motion
    });
  }, [focusedCallsignInfo?.latitude, focusedCallsignInfo?.longitude, stationLat, stationLon, flyToOffsetX]);

  // Invalidate map size when container is resized (e.g., FlexLayout panel drag)
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const observer = new ResizeObserver(() => {
      requestAnimationFrame(() => {
        mapRef.current?.invalidateSize();
      });
    });

    observer.observe(container);
    return () => observer.disconnect();
  }, []);

  // Fetch TLE data and initialize satellite tracking
  useEffect(() => {
    if (!settings.map.showSatellites || settings.map.selectedSatellites.length === 0) {
      setSatelliteTLEs(new Map());
      setSatellitePositions(new Map());
      setSatelliteOrbits(new Map());
      return;
    }

    const loadTLEData = async () => {
      try {
        const tleData = await fetchTLEData(settings.map.selectedSatellites);
        setSatelliteTLEs(tleData);
      } catch (error) {
        console.error('Failed to load TLE data:', error);
      }
    };

    loadTLEData();
  }, [settings.map.showSatellites, settings.map.selectedSatellites]);

  // Update satellite positions every 5 seconds
  useEffect(() => {
    if (satelliteTLEs.size === 0) return;

    const updateSatellitePositions = () => {
      const now = new Date();
      const newPositions = new Map<string, SatellitePosition>();
      const newOrbits = new Map<string, Array<{ lat: number; lon: number }>>();

      satelliteTLEs.forEach((tle, name) => {
        // Calculate current position
        const position = calculateSatellitePosition(tle, now, stationLat, stationLon, 0);
        if (position) {
          newPositions.set(name, position);
        }

        // Calculate orbital track (next 90 minutes)
        const orbitTrack = calculateOrbitTrack(tle, now, 90, 2);
        newOrbits.set(name, orbitTrack);
      });

      setSatellitePositions(newPositions);
      setSatelliteOrbits(newOrbits);
    };

    // Initial update
    updateSatellitePositions();

    // Update every 5 seconds
    const interval = setInterval(updateSatellitePositions, 5000);
    return () => clearInterval(interval);
  }, [satelliteTLEs, stationLat, stationLon]);

  // Handle click on map to set bearing (only when rotator enabled)
  const handleBearingClick = useCallback((azimuth: number) => {
    if (!rotatorEnabled) return; // Ignore clicks when rotator disabled
    setCurrentAzimuth(azimuth);
    commandRotator(azimuth, 'map');
  }, [commandRotator, rotatorEnabled]);

  // Toggle satellite overlay
  const toggleSatellites = useCallback(() => {
    updateMapSettings({ showSatellites: !settings.map.showSatellites });
    saveSettings();
  }, [settings.map.showSatellites, updateMapSettings, saveSettings]);

  // Generate rotator beam visualization line (cyan)
  const beamLinePoints: [number, number][] = [];
  const beamDistance = 5000; // 5000km beam visualization
  for (let d = 0; d <= beamDistance; d += 100) {
    beamLinePoints.push(getDestinationPoint(stationLat, stationLon, currentAzimuth, d));
  }

  // Target location from focused callsign
  const targetLat = focusedCallsignInfo?.latitude;
  const targetLon = focusedCallsignInfo?.longitude;

  // Generate great circle path from station to focused callsign (amber)
  const targetPathSegments = useMemo<[number, number][][]>(() => {
    if (!isValidCoord(targetLat) || !isValidCoord(targetLon)) return [];
    return generateGreatCirclePoints(stationLat, stationLon, targetLat, targetLon);
  }, [stationLat, stationLon, targetLat, targetLon]);

  // Fetch RBN spots periodically when enabled
  useEffect(() => {
    if (!rbnSettings.enabled) {
      setRbnSpots([]);
      return;
    }

    const fetchRbnSpots = async () => {
      try {
        const data = await api.getRbnSpots(rbnSettings.timeWindowMinutes);
        if (data && data.spots) {
          // Filter for my callsign
          const myCallsign = settings.station.callsign;
          if (myCallsign) {
            const mySpots = data.spots.filter(s =>
              s.dx.toUpperCase() === myCallsign.toUpperCase()
            );
            setRbnSpots(mySpots);
          } else {
            setRbnSpots([]);
          }
        }
      } catch (error) {
        console.error('Failed to fetch RBN spots:', error);
      }
    };

    fetchRbnSpots();
    const interval = setInterval(fetchRbnSpots, 60000); // Update every minute

    return () => clearInterval(interval);
  }, [rbnSettings.enabled, rbnSettings.timeWindowMinutes, settings.station.callsign]);

  // Helper function to get SNR color
  const getSNRColor = (snr: number | undefined): string => {
    if (snr === null || snr === undefined) return '#888888';
    if (snr < 0) return '#ef4444';      // Red: Weak
    if (snr < 10) return '#f97316';     // Orange: Fair
    if (snr < 20) return '#fbbf24';     // Yellow: Good
    if (snr < 30) return '#84cc16';     // Light green: Very good
    return '#22c55e';                   // Bright green: Excellent
  };

  return (
    <div ref={containerRef} className="relative w-full h-full">
        <MapContainer
          center={[stationLat, stationLon]}
          zoom={5}
          className="w-full h-full"
          style={{ background: '#0a0e14' }}
          ref={(map) => { mapRef.current = map ?? null; }}
          zoomControl={false}
          whenReady={() => requestAnimationFrame(() => mapRef.current?.invalidateSize())}
        >
          <TileLayer
            url={TILE_LAYERS[tileLayer].url}
            attribution={TILE_LAYERS[tileLayer].attribution}
          />

          {/* Day/Night Overlay */}
          {settings.map.showDayNightOverlay && (
            <DayNightOverlay
              opacity={settings.map.dayNightOpacity}
              showSunMarker={settings.map.showSunMarker}
              showMoonMarker={settings.map.showMoonMarker}
            />
          )}

          {/* Gray Line Overlay */}
          {settings.map.showGrayLine && (
            <GrayLineOverlay
              opacity={settings.map.grayLineOpacity}
            />
          )}

          {/* Aurora oval (NOAA OVATION) */}
          {settings.map.showAuroraOverlay && <AuroraOverlay />}

          {/* PSK Reporter reception paths */}
          {settings.map.showPskOverlay && (
            <PskReporterOverlay
              callsign={settings.map.pskCallsign || settings.station.callsign || ''}
            />
          )}

          {/* Click handler */}
          <MapClickHandler
            stationLat={stationLat}
            stationLon={stationLon}
            onBearingClick={handleBearingClick}
          />

          {/* Station marker */}
          <Marker position={[stationLat, stationLon]} icon={stationIcon}>
            <Popup>
              <div className="text-center">
                <div style={{ fontSize: '15px', fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", color: '#00ddff', marginBottom: 2 }}>{stationGrid || 'Station'}</div>
                <div style={{ fontSize: '11px', color: '#a5b4c8' }}>Your Location</div>
              </div>
            </Popup>
          </Marker>

          {/* Station range circle */}
          <Circle
            center={[stationLat, stationLon]}
            radius={500000}
            pathOptions={{
              color: '#00ddff',
              fillColor: '#00ddff',
              fillOpacity: 0.05,
              weight: 1,
              dashArray: '5, 5',
            }}
          />

          {/* Rotator beam direction line - only show when rotator enabled (cyan) */}
          {rotatorEnabled && (
            <Polyline
              positions={beamLinePoints}
              pathOptions={{
                color: '#00ddff',
                weight: 3,
                opacity: 0.7,
                dashArray: '10, 5',
              }}
            />
          )}

          {/* Great circle path to focused callsign — an animated traveling
              sine wave in the pin green (accent-secondary), so pin + line read
              as one. Short segments (< 3 pts, e.g. antimeridian slivers) fall
              back to a plain line. */}
          {targetPathSegments.map((segment, segmentIndex) => (
            segment.length >= 3 ? (
              <SineWavePath key={`target-wave-${segmentIndex}`} segment={segment} />
            ) : (
              <Polyline
                key={`target-path-${segmentIndex}`}
                positions={segment}
                pathOptions={{ weight: 2, className: 'dx-target-path' }}
              />
            )
          ))}

          {/* Target marker - show callsign image icon (2x) with image or placeholder, or standard dot if callsign images disabled */}
          {isValidCoord(targetLat) && isValidCoord(targetLon) && (
            settings.map.showCallsignImages ? (
              <Marker
                position={[targetLat, targetLon]}
                icon={createCallsignImageIcon(focusedCallsignInfo?.imageUrl, focusedCallsignInfo?.callsign ?? '', '2x')}
              >
                <Popup>
                  <div className="text-center">
                    <div style={{ fontSize: '15px', fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", color: '#ffb432', marginBottom: 2 }}>{focusedCallsignInfo?.callsign}</div>
                    {focusedCallsignInfo?.name && (
                      <div style={{ fontSize: '12px', color: '#cdd7e4', marginBottom: 2 }}>{focusedCallsignInfo.name}</div>
                    )}
                    {focusedCallsignInfo?.grid && (
                      <div style={{ fontSize: '11px', fontFamily: "'JetBrains Mono', monospace", color: '#a5b4c8' }}>{focusedCallsignInfo.grid}</div>
                    )}
                    {focusedCallsignInfo?.bearing != null && (
                      <div style={{ fontSize: '11px', fontFamily: "'JetBrains Mono', monospace", color: '#00ddff', marginTop: 2 }}>
                        {focusedCallsignInfo.bearing.toFixed(0)}° / {Math.round(focusedCallsignInfo.distance ?? 0).toLocaleString()} km
                      </div>
                    )}
                    <div style={{ display: 'flex', gap: '4px', marginTop: '4px' }}>
                      {rotatorEnabled && focusedCallsignInfo?.bearing != null && (
                        <button
                          onClick={(e) => { e.stopPropagation(); commandRotator(focusedCallsignInfo.bearing!, 'map'); }}
                          className="map-popup-qrz-btn"
                          style={{ flex: 1, cursor: 'pointer', background: 'rgba(0, 221, 255, 0.15)', borderColor: 'rgba(0, 221, 255, 0.3)', color: '#00ddff' }}
                        >
                          Rotate {focusedCallsignInfo.bearing.toFixed(0)}°
                        </button>
                      )}
                      <a
                        href={`https://www.qrz.com/db/${focusedCallsignInfo?.callsign}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="map-popup-qrz-btn"
                        style={{ flex: 1 }}
                      >
                        QRZ.com ↗
                      </a>
                    </div>
                  </div>
                </Popup>
              </Marker>
            ) : (
              <Marker position={[targetLat, targetLon]} icon={targetIcon}>
                <Popup>
                  <div className="text-center">
                    <div style={{ fontSize: '15px', fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", color: '#ffb432', marginBottom: 2 }}>{focusedCallsignInfo?.callsign}</div>
                    {focusedCallsignInfo?.grid && (
                      <div style={{ fontSize: '11px', fontFamily: "'JetBrains Mono', monospace", color: '#a5b4c8' }}>{focusedCallsignInfo.grid}</div>
                    )}
                    {focusedCallsignInfo?.bearing != null && (
                      <div style={{ fontSize: '11px', fontFamily: "'JetBrains Mono', monospace", color: '#00ddff', marginTop: 2 }}>
                        {focusedCallsignInfo.bearing.toFixed(0)}° / {Math.round(focusedCallsignInfo.distance ?? 0).toLocaleString()} km
                      </div>
                    )}
                    <div style={{ display: 'flex', gap: '4px', marginTop: '4px' }}>
                      {rotatorEnabled && focusedCallsignInfo?.bearing != null && (
                        <button
                          onClick={(e) => { e.stopPropagation(); commandRotator(focusedCallsignInfo.bearing!, 'map'); }}
                          className="map-popup-qrz-btn"
                          style={{ flex: 1, cursor: 'pointer', background: 'rgba(0, 221, 255, 0.15)', borderColor: 'rgba(0, 221, 255, 0.3)', color: '#00ddff' }}
                        >
                          Rotate {focusedCallsignInfo.bearing.toFixed(0)}°
                        </button>
                      )}
                      <a
                        href={`https://www.qrz.com/db/${focusedCallsignInfo?.callsign}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="map-popup-qrz-btn"
                        style={{ flex: 1 }}
                      >
                        QRZ.com ↗
                      </a>
                    </div>
                  </div>
                </Popup>
              </Marker>
            )
          )}


          {/* RBN Spots Overlay */}
          {rbnSettings.enabled && rbnSpots.map((spot, idx) => {
            if (!spot.skimmerLat || !spot.skimmerLon) return null;

            // Apply band filter
            if (rbnSettings.bands.length > 0 && !rbnSettings.bands.includes('all') && !rbnSettings.bands.includes(spot.band)) {
              return null;
            }

            // Apply mode filter
            if (rbnSettings.modes.length > 0 && !rbnSettings.modes.includes(spot.mode)) {
              return null;
            }

            // Apply SNR filter
            if (spot.snr !== undefined && spot.snr < rbnSettings.minSnr) {
              return null;
            }

            const color = getSNRColor(spot.snr);
            const size = spot.snr !== undefined && spot.snr >= 20 ? 8 : 6;

            return (
              <div key={`rbn-${idx}`}>
                {/* Path line from station to skimmer */}
                {rbnSettings.showPaths && (
                  <Polyline
                    positions={[
                      [stationLat, stationLon],
                      [spot.skimmerLat, spot.skimmerLon]
                    ]}
                    pathOptions={{
                      color: color,
                      weight: 2,
                      opacity: rbnSettings.opacity * 0.6,
                      dashArray: '5, 5',
                    }}
                  />
                )}

                {/* Skimmer marker */}
                <CircleMarker
                  center={[spot.skimmerLat, spot.skimmerLon]}
                  radius={size}
                  pathOptions={{
                    fillColor: color,
                    color: '#ffffff',
                    weight: 2,
                    opacity: rbnSettings.opacity,
                    fillOpacity: rbnSettings.opacity * 0.8,
                  }}
                >
                  <Popup>
                    <div style={{ fontFamily: 'monospace' }}>
                      <strong>{spot.callsign}</strong><br />
                      Heard: <strong>{spot.dx}</strong><br />
                      SNR: <strong>{spot.snr ?? '?'} dB</strong><br />
                      Band: <strong>{spot.band}</strong><br />
                      Freq: <strong>{(spot.frequency/1000).toFixed(1)} kHz</strong><br />
                      {spot.grid && <>Grid: {spot.grid}<br /></>}
                      {spot.speed && <>Speed: {spot.speed} WPM<br /></>}
                      Time: {new Date(spot.timestamp).toLocaleTimeString()}
                    </div>
                  </Popup>
                </CircleMarker>
              </div>
            );
          })}

          {/* POTA markers - show when enabled and we have spot data with coordinates */}
          {settings.map.showPotaOverlay && potaSpots.map((spot) => {
            if (!isValidCoord(spot.latitude) || !isValidCoord(spot.longitude)) return null;
            const spBearing = calculateBearing(stationLat, stationLon, spot.latitude, spot.longitude);
            const lpBearing = (spBearing + 180) % 360;
            const dist = calculateDistance(stationLat, stationLon, spot.latitude, spot.longitude);
            return (
              <Marker
                key={spot.spotId}
                position={[spot.latitude, spot.longitude]}
                icon={potaIcon}
              >
                <Popup>
                  <div className="text-center">
                    <div style={{ fontSize: '15px', fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", color: '#10b981', marginBottom: 2 }}>{spot.activator}</div>
                    <div style={{ fontSize: '12px', fontFamily: "'JetBrains Mono', monospace", color: '#34d399' }}>{spot.reference}</div>
                    {spot.parkName && (
                      <div style={{ fontSize: '11px', color: '#cdd7e4', marginTop: 1 }}>{spot.parkName}</div>
                    )}
                    <div style={{ fontSize: '11px', fontFamily: "'JetBrains Mono', monospace", color: '#a5b4c8', marginTop: 2 }}>
                      {(parseFloat(spot.frequency) / 1000).toFixed(3)} MHz &bull; {spot.mode}
                    </div>
                    {spot.locationDesc && (
                      <div style={{ fontSize: '10px', color: '#7a8a9e', marginTop: 1 }}>{spot.locationDesc}</div>
                    )}
                    {spot.grid6 && (
                      <div style={{ fontSize: '11px', fontFamily: "'JetBrains Mono', monospace", color: '#a5b4c8', marginTop: 1 }}>{spot.grid6}</div>
                    )}
                    <div style={{ fontSize: '11px', fontFamily: "'JetBrains Mono', monospace", color: '#00ddff', marginTop: 3 }}>
                      {spBearing.toFixed(0)}&deg; / {Math.round(dist).toLocaleString()} km
                    </div>
                    {/* Rotator buttons: SP and LP */}
                    {rotatorEnabled && (
                      <div style={{ display: 'flex', gap: '4px', marginTop: '4px' }}>
                        <button
                          onClick={(e) => { e.stopPropagation(); commandRotator(spBearing, 'map'); }}
                          className="map-popup-qrz-btn"
                          style={{ flex: 1, cursor: 'pointer', background: 'rgba(0, 221, 255, 0.15)', borderColor: 'rgba(0, 221, 255, 0.3)', color: '#00ddff' }}
                        >
                          SP {spBearing.toFixed(0)}&deg;
                        </button>
                        <button
                          onClick={(e) => { e.stopPropagation(); commandRotator(lpBearing, 'map'); }}
                          className="map-popup-qrz-btn"
                          style={{ flex: 1, cursor: 'pointer', background: 'rgba(0, 221, 255, 0.15)', borderColor: 'rgba(0, 221, 255, 0.3)', color: '#00ddff' }}
                        >
                          LP {lpBearing.toFixed(0)}&deg;
                        </button>
                      </div>
                    )}
                    {/* QRZ + Work buttons */}
                    <div style={{ display: 'flex', gap: '4px', marginTop: '4px' }}>
                      <button
                        onClick={(e) => { e.stopPropagation(); selectSpot(spot.activator, parseFloat(spot.frequency), spot.mode); }}
                        className="map-popup-qrz-btn"
                        style={{ flex: 1, cursor: 'pointer', background: 'rgba(16, 185, 129, 0.15)', borderColor: 'rgba(16, 185, 129, 0.3)', color: '#10b981' }}
                      >
                        Work
                      </button>
                      <a
                        href={`https://www.qrz.com/db/${spot.activator}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="map-popup-qrz-btn"
                        style={{ flex: 1 }}
                      >
                        QRZ.com &#8599;
                      </a>
                    </div>
                  </div>
                </Popup>
              </Marker>
            );
          })}

          {/* Satellite markers and orbital tracks */}
          {settings.map.showSatellites && Array.from(satellitePositions.entries()).map(([name, position]) => {
            const orbit = satelliteOrbits.get(name) || [];
            const orbitPolyline: [number, number][] = orbit.map(p => [p.lat, p.lon]);

            return (
              <div key={name}>
                {/* Orbital track line */}
                {orbitPolyline.length > 0 && (
                  <Polyline
                    positions={orbitPolyline}
                    pathOptions={{
                      color: '#a855f7',
                      weight: 2,
                      opacity: 0.6,
                      dashArray: '3, 6',
                    }}
                  />
                )}

                {/* Footprint circle */}
                <Circle
                  center={[position.latitude, position.longitude]}
                  radius={position.footprintRadius * 1000}
                  pathOptions={{
                    color: '#ec4899',
                    fillColor: '#ec4899',
                    fillOpacity: 0.08,
                    weight: 1,
                    opacity: 0.5,
                    dashArray: '5, 5',
                  }}
                />

                {/* Satellite marker */}
                <Marker
                  position={[position.latitude, position.longitude]}
                  icon={satelliteIcon}
                >
                  <Popup>
                    <div className="text-center">
                      <strong className="font-mono text-accent-secondary">{name}</strong>
                      <br />
                      <span className="text-xs font-mono text-dark-300">
                        Alt: {Math.round(position.altitude)} km
                      </span>
                      <br />
                      <span className="text-xs font-mono text-dark-300">
                        Vel: {position.velocity.toFixed(1)} km/s
                      </span>
                      {position.elevation !== undefined && position.elevation > 0 && (
                        <>
                          <br />
                          <span className="text-xs font-mono text-accent-primary">
                            Az: {position.azimuth?.toFixed(0)}° El: {position.elevation.toFixed(0)}°
                          </span>
                          <br />
                          <span className="text-xs font-mono text-accent-success">
                            VISIBLE
                          </span>
                        </>
                      )}
                      {position.eclipsed && (
                        <>
                          <br />
                          <span className="text-xs text-gray-500">Eclipsed</span>
                        </>
                      )}
                    </div>
                  </Popup>
                </Marker>
              </div>
            );
          })}

          {/* DX Cluster spot paths overlay */}
          {dxClusterMapEnabled && spotPaths.map((sp) => {
            const isHovered = hoveredSpotId === sp.spot.id;
            return (
              <React.Fragment key={`path-${sp.spot.id}`}>
                {/* Path from station to DX */}
                {sp.pathPoints.map((segment, segmentIndex) => (
                  <Polyline
                    key={`path-${sp.spot.id}-${segmentIndex}`}
                    positions={segment}
                    pathOptions={{
                      color: sp.color,
                      weight: isHovered ? 3 : 1.5,
                      opacity: isHovered ? 1 : 0.5,
                    }}
                  />
                ))}
                {/* Path from spotter to DX (if spotter location is available) */}
                {sp.spotterPathPoints && sp.spotterPathPoints.map((segment, segmentIndex) => (
                  <Polyline
                    key={`spotter-path-${sp.spot.id}-${segmentIndex}`}
                    positions={segment}
                    pathOptions={{
                      color: sp.color,
                      weight: isHovered ? 2 : 1,
                      opacity: isHovered ? 0.8 : 0.3,
                      dashArray: '5, 10', // Dashed line to distinguish spotter path
                    }}
                  />
                ))}
              </React.Fragment>
            );
          })}

          {/* DX Cluster spot endpoint markers */}
          {dxClusterMapEnabled && spotPaths.map((sp) => {
            const isHovered = hoveredSpotId === sp.spot.id;
            return (
              <Marker
                key={`spot-${sp.spot.id}`}
                position={[sp.targetLat, sp.targetLon]}
                icon={createSpotIcon(sp.color, isHovered)}
              >
                <Tooltip
                  direction="top"
                  offset={[0, -6]}
                  permanent={isHovered}
                  className="dx-spot-tooltip"
                >
                  <span style={{ color: sp.color, fontFamily: 'monospace', fontWeight: 'bold', fontSize: '11px' }}>
                    {sp.spot.dxCall}
                  </span>
                  <span style={{ color: '#999', fontSize: '10px', marginLeft: '4px' }}>
                    {(sp.spot.frequency / 1000).toFixed(1)}
                  </span>
                </Tooltip>
              </Marker>
            );
          })}

          {/* DX Cluster spotter markers (if location is available) */}
          {dxClusterMapEnabled && spotPaths.map((sp) => {
            if (!sp.spotterLat || !sp.spotterLon) return null;
            const isHovered = hoveredSpotId === sp.spot.id;
            return (
              <Marker
                key={`spotter-${sp.spot.id}`}
                position={[sp.spotterLat, sp.spotterLon]}
                icon={createSpotterIcon(sp.color, isHovered)}
              >
                <Tooltip
                  direction="top"
                  offset={[0, -6]}
                  permanent={isHovered}
                  className="dx-spot-tooltip"
                >
                  <span style={{ color: sp.color, fontFamily: 'monospace', fontSize: '10px' }}>
                    DE: {sp.spot.spotter}
                  </span>
                </Tooltip>
              </Marker>
            );
          })}
        </MapContainer>

        {/* Map controls overlay - positioned outside MapContainer to avoid event conflicts */}
        <div className="absolute top-2 right-2 z-[1000] flex flex-col gap-1">
          <button
            onClick={(e) => {
              e.stopPropagation();
              mapRef.current?.zoomIn();
            }}
            className="glass-button p-2"
            title="Zoom In"
          >
            <ZoomIn className="w-4 h-4" />
          </button>
          <button
            onClick={(e) => {
              e.stopPropagation();
              mapRef.current?.zoomOut();
            }}
            className="glass-button p-2"
            title="Zoom Out"
          >
            <ZoomOut className="w-4 h-4" />
          </button>
          <button
            onClick={(e) => {
              e.stopPropagation();
              toggleSatellites();
            }}
            className={`glass-button p-2 ${settings.map.showSatellites ? 'bg-accent-primary/20' : ''}`}
            title={settings.map.showSatellites ? "Hide Satellites" : "Show Satellites"}
          >
            <Satellite className="w-4 h-4" />
          </button>
          <div className="relative">
            <button
              onClick={(e) => {
                e.stopPropagation();
                setShowLayerPicker(!showLayerPicker);
                setShowOverlayPanel(false);
              }}
              className="glass-button p-2"
              title="Map Layers"
            >
              <Layers className="w-4 h-4" />
            </button>
            {showLayerPicker && (
              <div className="absolute right-full mr-2 top-0 glass-panel p-2 min-w-[180px]">
                <div className="text-xs text-gray-400 mb-2 px-2">Base Layers</div>
                {Object.entries(TILE_LAYERS).map(([key, layer]) => (
                  <button
                    key={key}
                    onClick={(e) => {
                      e.stopPropagation();
                      updateMapSettings({ tileLayer: key as TileLayerKey });
                      saveSettings();
                    }}
                    className={`w-full text-left px-2 py-1 text-sm font-ui rounded hover:bg-dark-600 ${
                      tileLayer === key ? 'text-accent-primary' : 'text-dark-200'
                    }`}
                  >
                    {layer.name}
                  </button>
                ))}
                <div className="border-t border-gray-600 my-2"></div>
                <div className="text-xs text-gray-400 mb-2 px-2">Overlays</div>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    updateMapSettings({
                      rbn: { ...rbnSettings, enabled: !rbnSettings.enabled }
                    });
                    saveSettings();
                  }}
                  className="w-full text-left px-2 py-1 text-sm rounded hover:bg-dark-600 flex items-center justify-between"
                >
                  <span className={rbnSettings.enabled ? 'text-accent-primary' : 'text-gray-300'}>
                    RBN Layer
                  </span>
                  {rbnSettings.enabled && <Radio className="w-3 h-3" />}
                </button>
                {rbnSettings.enabled && (
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      setShowRbnPanel(!showRbnPanel);
                      setShowLayerPicker(false);
                    }}
                    className="w-full text-left px-2 py-1 text-xs text-gray-400 hover:text-gray-300"
                  >
                    ⚙️ RBN Settings
                  </button>
                )}
              </div>
            )}
          </div>
          <div className="relative">
            <button
              onClick={(e) => {
                e.stopPropagation();
                setShowOverlayPanel(!showOverlayPanel);
                setShowLayerPicker(false);
              }}
              className={`glass-button p-2 ${(settings.map.showDayNightOverlay || settings.map.showGrayLine) ? 'text-accent-primary' : ''}`}
              title="Day/Night & Gray Line"
            >
              <Sun className="w-4 h-4" />
            </button>
            {showOverlayPanel && (
              <div className="absolute right-full mr-2 top-0 glass-panel p-3 min-w-[220px]">
                <div className="text-xs font-ui text-dark-200 mb-2 font-semibold">Solar Overlays</div>

                {/* Day/Night Overlay Toggle */}
                <label className="flex items-center gap-2 mb-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={settings.map.showDayNightOverlay}
                    onChange={(e) => {
                      e.stopPropagation();
                      updateMapSettings({ showDayNightOverlay: e.target.checked });
                      saveSettings();
                    }}
                    className="w-4 h-4"
                  />
                  <span className="text-sm font-ui text-dark-200">Day/Night</span>
                </label>

                {/* Day/Night Opacity */}
                {settings.map.showDayNightOverlay && (
                  <div className="ml-6 mb-2">
                    <label className="flex items-center gap-2 text-xs text-dark-300">
                      <span>Opacity:</span>
                      <input
                        type="range"
                        min="0"
                        max="1"
                        step="0.1"
                        value={settings.map.dayNightOpacity}
                        onChange={(e) => {
                          updateMapSettings({ dayNightOpacity: parseFloat(e.target.value) });
                        }}
                        onMouseUp={() => saveSettings()}
                        className="flex-1"
                      />
                      <span>{Math.round(settings.map.dayNightOpacity * 100)}%</span>
                    </label>
                  </div>
                )}

                {/* Sun Marker Toggle */}
                {settings.map.showDayNightOverlay && (
                  <label className="flex items-center gap-2 mb-2 ml-6 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={settings.map.showSunMarker}
                      onChange={(e) => {
                        e.stopPropagation();
                        updateMapSettings({ showSunMarker: e.target.checked });
                        saveSettings();
                      }}
                      className="w-3 h-3"
                    />
                    <span className="text-xs font-ui text-dark-300">☀️ Sun</span>
                  </label>
                )}

                {/* Moon Marker Toggle */}
                {settings.map.showDayNightOverlay && (
                  <label className="flex items-center gap-2 mb-3 ml-6 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={settings.map.showMoonMarker}
                      onChange={(e) => {
                        e.stopPropagation();
                        updateMapSettings({ showMoonMarker: e.target.checked });
                        saveSettings();
                      }}
                      className="w-3 h-3"
                    />
                    <span className="text-xs font-ui text-dark-300">🌙 Moon</span>
                  </label>
                )}

                {/* Gray Line Toggle */}
                <label className="flex items-center gap-2 mb-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={settings.map.showGrayLine}
                    onChange={(e) => {
                      e.stopPropagation();
                      updateMapSettings({ showGrayLine: e.target.checked });
                      saveSettings();
                    }}
                    className="w-4 h-4"
                  />
                  <span className="text-sm font-ui text-dark-200">Gray Line</span>
                </label>

                {/* Gray Line Opacity */}
                {settings.map.showGrayLine && (
                  <div className="ml-6">
                    <label className="flex items-center gap-2 text-xs text-dark-300">
                      <span>Opacity:</span>
                      <input
                        type="range"
                        min="0"
                        max="1"
                        step="0.1"
                        value={settings.map.grayLineOpacity}
                        onChange={(e) => {
                          updateMapSettings({ grayLineOpacity: parseFloat(e.target.value) });
                        }}
                        onMouseUp={() => saveSettings()}
                        className="flex-1"
                      />
                      <span>{Math.round(settings.map.grayLineOpacity * 100)}%</span>
                    </label>
                  </div>
                )}

                <div className="mt-3 pt-2 border-t border-dark-600 text-xs text-dark-400">
                  Updates every 60 seconds
                </div>

                {/* Propagation overlays (aurora + PSK Reporter) */}
                <div className="mt-3 pt-2 border-t border-dark-600">
                  <div className="text-xs font-ui text-dark-200 mb-2 font-semibold">Propagation</div>

                  {/* Aurora Oval Toggle */}
                  <label className="flex items-center gap-2 mb-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={settings.map.showAuroraOverlay}
                      onChange={(e) => {
                        e.stopPropagation();
                        updateMapSettings({ showAuroraOverlay: e.target.checked });
                        saveSettings();
                      }}
                      className="w-4 h-4"
                    />
                    <span className="text-sm font-ui text-dark-200">🌌 Aurora Oval</span>
                  </label>

                  {/* PSK Reporter Toggle */}
                  <label className="flex items-center gap-2 mb-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={settings.map.showPskOverlay}
                      onChange={(e) => {
                        e.stopPropagation();
                        updateMapSettings({ showPskOverlay: e.target.checked });
                        saveSettings();
                      }}
                      className="w-4 h-4"
                    />
                    <span className="text-sm font-ui text-dark-200">📡 PSK Reporter</span>
                  </label>

                  {settings.map.showPskOverlay && (
                    <div className="ml-6">
                      <label className="flex flex-col gap-1 text-xs text-dark-300">
                        <span>Callsign (who's hearing it):</span>
                        <input
                          type="text"
                          value={settings.map.pskCallsign}
                          placeholder={settings.station.callsign || 'e.g. W1AW'}
                          onChange={(e) => {
                            updateMapSettings({ pskCallsign: e.target.value.toUpperCase() });
                          }}
                          onBlur={() => saveSettings()}
                          className="glass-input px-2 py-1 text-sm font-mono uppercase"
                        />
                      </label>
                      <div className="mt-1 text-[10px] text-dark-400">
                        Blank = your station callsign · refreshes every 5 min
                      </div>
                    </div>
                  )}
                </div>

                {/* Callsign image — QRZ photo icon for the currently worked
                    callsign only (historical/worked markers were removed by
                    user decision: the map shows just the active QSO). */}
                <div className="mt-3 pt-2 border-t border-dark-600">
                  <div className="text-xs font-ui text-dark-200 mb-2 font-semibold">Callsign Image</div>

                  <label className="flex items-center gap-2 mb-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={settings.map.showCallsignImages}
                      onChange={(e) => {
                        e.stopPropagation();
                        updateMapSettings({ showCallsignImages: e.target.checked });
                        saveSettings();
                      }}
                      className="w-4 h-4"
                    />
                    <span className="text-sm font-ui text-dark-200">Show photo for current callsign</span>
                  </label>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Instructions overlay */}
        <div className="absolute bottom-12 right-4 glass-panel px-3 py-2 z-[1000] text-xs font-ui text-dark-300">
          {rotatorEnabled ? 'Click on map to set bearing' : 'Rotator disabled'}
        </div>

        {/* RBN Settings Panel */}
        {showRbnPanel && (
          <div className="absolute top-20 right-4 glass-panel p-4 z-[1001] min-w-[300px]">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-bold text-accent-primary flex items-center gap-2">
                <Radio className="w-4 h-4" />
                RBN Settings
              </h3>
              <button
                onClick={() => setShowRbnPanel(false)}
                className="text-gray-400 hover:text-white"
              >
                ✕
              </button>
            </div>

            <div className="space-y-3 text-sm">
              {/* Spots count */}
              <div className="text-xs text-gray-400">
                Showing {rbnSpots.length} spot{rbnSpots.length !== 1 ? 's' : ''} for {settings.station.callsign || 'your callsign'}
              </div>

              {/* Opacity */}
              <div>
                <label className="block mb-1 text-gray-300">
                  Opacity: {Math.round(rbnSettings.opacity * 100)}%
                </label>
                <input
                  type="range"
                  min="0"
                  max="1"
                  step="0.1"
                  value={rbnSettings.opacity}
                  onChange={(e) => {
                    updateMapSettings({
                      rbn: { ...rbnSettings, opacity: parseFloat(e.target.value) }
                    });
                  }}
                  onMouseUp={() => saveSettings()}
                  className="w-full"
                />
              </div>

              {/* Show Paths */}
              <label className="flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={rbnSettings.showPaths}
                  onChange={(e) => {
                    updateMapSettings({
                      rbn: { ...rbnSettings, showPaths: e.target.checked }
                    });
                    saveSettings();
                  }}
                  className="rounded"
                />
                <span className="text-gray-300">Show signal paths</span>
              </label>

              {/* Time Window */}
              <div>
                <label className="block mb-1 text-gray-300">
                  Time Window: {rbnSettings.timeWindowMinutes} min
                </label>
                <input
                  type="range"
                  min="1"
                  max="15"
                  step="1"
                  value={rbnSettings.timeWindowMinutes}
                  onChange={(e) => {
                    updateMapSettings({
                      rbn: { ...rbnSettings, timeWindowMinutes: parseInt(e.target.value) }
                    });
                  }}
                  onMouseUp={() => saveSettings()}
                  className="w-full"
                />
              </div>

              {/* Min SNR */}
              <div>
                <label className="block mb-1 text-gray-300">
                  Min SNR: {rbnSettings.minSnr} dB
                </label>
                <input
                  type="range"
                  min="-30"
                  max="30"
                  step="5"
                  value={rbnSettings.minSnr}
                  onChange={(e) => {
                    updateMapSettings({
                      rbn: { ...rbnSettings, minSnr: parseInt(e.target.value) }
                    });
                  }}
                  onMouseUp={() => saveSettings()}
                  className="w-full"
                />
              </div>

              <div className="pt-2 border-t border-gray-600 text-xs text-gray-500">
                Data from reversebeacon.net
              </div>
            </div>
          </div>
        )}

        {/* Band color legend - shown when DX cluster map overlay is active */}
        {dxClusterMapEnabled && activeBands.length > 0 && (
          <div className="absolute top-4 right-14 glass-panel px-2 py-1.5 z-[1000]">
            <div className="flex flex-wrap gap-x-3 gap-y-0.5">
              {activeBands.map(([band, color]) => (
                <div key={band} className="flex items-center gap-1">
                  <div
                    className="w-3 h-1 rounded-sm"
                    style={{ backgroundColor: color }}
                  />
                  <span className="text-[10px] font-mono text-dark-300">{band}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* DX News Ticker */}
        {settings.map.showDxNewsTicker && <DXNewsTicker />}
        
        {children}
    </div>
  );
}

export function MapPlugin() {
  const { settings } = useSettingsStore();
  const [isFullscreen, setIsFullscreen] = useState(false);
  const { rotatorPosition, selectedRadioId, radioStates, focusedCallsignInfo } = useAppStore();
  const [containerSize, setContainerSize] = useState({ width: 0, height: 0 });
  const containerRef = useRef<HTMLDivElement>(null);

  // Track container size: hides top/bottom sidebar content when short, and
  // drives the globe-circle diameter so the cockpit scales with the panel.
  // Observe via ref, not getElementById — the id can transiently match a
  // stale duplicate (maximize/remount), leaving the size state frozen.
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const observer = new ResizeObserver((entries) => {
      const rect = entries[0].contentRect;
      setContainerSize({ width: rect.width, height: rect.height });
    });

    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  // Determine visibility based on container height
  const containerHeight = containerSize.height;
  const showTopContent = containerHeight >= 740;

  // Globe-circle geometry. The original cockpit was drawn for a fixed
  // 700px-diameter circle shifted 100px off the left edge, with the bulge
  // backdrop and arc border clipped at the sidebar's x=198 border. Scale
  // that whole construction from the panel size instead: fill the height,
  // but never take more than ~60% of the width so the flat map stays usable.
  const globeSize = Math.max(320, Math.min(700, containerHeight, containerSize.width * 0.6));
  const globeOffset = -globeSize / 7;               // was -100 at 700
  const sidebarEdge = 198;                          // sidebar border x
  const bulgeWidth = Math.max(0, globeOffset + globeSize - sidebarEdge);

  // The cockpit frame (sidebar column + bulge + arc) only earns its space
  // when the panel is tall enough for the sidebar to show its content.
  // Below that the sidebar is a dead black band (user feedback), so the map
  // runs full-bleed and the globe floats as a plain bordered circle.
  const showCockpit = showTopContent;

  // Get active radio state if available
  const radioState = selectedRadioId ? radioStates.get(selectedRadioId) : null;
  const frequency = radioState?.frequencyHz ? (radioState.frequencyHz / 1000000).toFixed(3) : null;

  const toggleFullscreen = useCallback(() => {
    const el = document.getElementById('map-plugin-container');
    if (!el) return;

    if (!isFullscreen) {
      el.requestFullscreen?.();
    } else {
      document.exitFullscreen?.();
    }
    setIsFullscreen(!isFullscreen);
  }, [isFullscreen]);

  return (
    <GlassPanel
      title="2D Map"
      icon={<MapIcon className="w-5 h-5" />}
      actions={
        <div className="flex items-center gap-4">
          {settings.rotator.enabled && (
             <RotatorControls />
          )}
          <button
            onClick={toggleFullscreen}
            className="glass-button p-1.5"
            title="Fullscreen"
          >
            <Maximize2 className="w-4 h-4" />
          </button>
        </div>
      }
    >
      <div id="map-plugin-container" ref={containerRef} className="relative w-full h-full min-h-[500px] bg-dark-900 overflow-hidden font-ui">
        
        {/* Background Map - Full Screen */}
        <div className="absolute inset-0 z-0">
          {/* Fly-to targets land centered in the open area right of the globe
              circle (offset = half the circle's right edge) instead of
              underneath it */}
          <MapCore flyToOffsetX={(globeOffset + globeSize) / 2} />
        </div>
        
        {/* Z-10: Unified Cockpit Background Shapes (casts the single master shadow) */}
        {showCockpit && (
          <div className="absolute top-0 bottom-0 left-0 z-10 pointer-events-none drop-shadow-[15px_0_30px_rgba(0,0,0,0.85)]">

            {/* Main Sidebar Base */}
            <div className="absolute top-0 bottom-0 left-0 w-[200px] bg-[#0a0e14] border-r-[2px] border-[#334155] pointer-events-auto" />

            {/* Bulge Base (clipped to only show exactly to the right of the sidebar border) */}
            <div
              className="absolute top-1/2 -translate-y-1/2 overflow-hidden pointer-events-none"
              style={{ left: sidebarEdge, width: bulgeWidth, height: globeSize }}
            >
               {/* The circle's arc intersects the sidebar's straight border line */}
               <div
                 className="absolute top-1/2 -translate-y-1/2 rounded-full bg-[#0a0e14] border-[2px] border-[#334155] pointer-events-auto"
                 style={{ left: globeOffset - sidebarEdge, width: globeSize, height: globeSize }}
               />
            </div>

          </div>
        )}

        {/* Z-20: Interactive Content Layer */}
        <div className="absolute inset-0 z-20 pointer-events-none">
          
          {/* Globe Component (touching left, shifted 1/7 of its diameter off-edge) */}
          {/* Since it sits at z-20, it perfectly covers the straight sidebar background border behind it,
              preventing the straight line from drawing "through" the globe. Without the cockpit frame
              it floats directly on the map, so it carries its own border ring. */}
          <div
            className={`absolute top-1/2 -translate-y-1/2 pointer-events-auto rounded-full overflow-hidden bg-[#020304] ${showCockpit ? '' : 'border-[2px] border-[#334155] drop-shadow-[0_0_20px_rgba(0,0,0,0.85)]'}`}
            style={{ left: globeOffset, width: globeSize, height: globeSize }}
          >
            <GlobeCore hideOverlays={true} />
          </div>

          {/* Sidebar Content (Rendered after Globe to stay on top if screen is very short) */}
          {showCockpit && (
          <div className="absolute top-0 bottom-0 left-0 w-[200px] py-8 z-30">
            {/* Station info + rotor heading, all above the globe (user call:
                no grid; rotor position replaces it). z-30: the globe circle
                is vertically centered and can reach up under this block, and
                its WebGL canvas paints over plain siblings — so the text
                must explicitly stack above it. */}
            <div className={`px-6 pointer-events-auto transition-opacity duration-300 ${showTopContent ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}>
               <div className="flex items-center gap-2 mb-4 text-accent-primary font-display font-bold text-sm tracking-wider">
                 <Radio className="w-4 h-4" />
                 <span>STATION</span>
               </div>
               <div className="space-y-2 font-mono text-xs text-dark-300">
                 <div className="flex justify-between items-center border-b border-glass-100/10 pb-2">
                   <span>Call:</span>
                   <span className="text-gray-100 font-bold">{settings.station.callsign || 'N/A'}</span>
                 </div>

                 {/* Rig Info */}
                 {frequency && (
                   <div className="flex justify-between items-center border-b border-glass-100/10 pb-2 pt-1">
                     <span>Rig:</span>
                     <span className="text-accent-info font-bold">{frequency} MHz {radioState?.mode}</span>
                   </div>
                 )}
               </div>

            </div>
          </div>
          )}

        </div>

        {/* Target callsign chip — centered over the open map area (right of
            the globe circle) so the rotor chip can never cover it. Lives at
            panel level (not inside MapCore) because MapCore's z-0 stacking
            context caps its overlays below the panel-level chips. */}
        {focusedCallsignInfo && (
          <div
            className="absolute top-4 glass-panel px-3 py-2 z-30 -translate-x-1/2"
            style={{ left: `calc((100% + ${globeOffset + globeSize}px) / 2)` }}
          >
            <div className="flex items-center gap-2">
              <Target className="w-4 h-4 text-accent-primary" />
              <div>
                <p className="font-mono font-bold text-accent-primary">
                  {focusedCallsignInfo.callsign}
                </p>
                {focusedCallsignInfo.grid && (
                  <p className="text-xs font-mono text-dark-300">{focusedCallsignInfo.grid}</p>
                )}
                {focusedCallsignInfo.bearing != null && (
                  <p className="text-xs font-mono text-accent-secondary">
                    {focusedCallsignInfo.bearing.toFixed(0)}°
                    {focusedCallsignInfo.distance != null && ` / ${Math.round(focusedCallsignInfo.distance)} km`}
                  </p>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Rotor heading chip — floats just above the globe circle (user call:
            rotor position above the globe, grid dropped). Independent of the
            cockpit frame so it is visible at ANY panel size; when the circle
            touches the panel top it tucks inside onto the starfield, which is
            always dark enough to read against. */}
        {settings.rotator.enabled && (
          <div
            className="absolute z-30 flex items-baseline gap-2 px-3 py-1.5 rounded-full bg-[#0a0e14]/80 border border-glass-100/20 backdrop-blur-sm pointer-events-none"
            style={{
              left: globeOffset + globeSize / 2,
              top: Math.max((containerHeight - globeSize) / 2 - 44, 8),
              transform: 'translateX(-50%)',
            }}
          >
            <span className="text-[10px] text-dark-400 font-ui uppercase tracking-widest">Rotor</span>
            <span className="text-2xl font-display font-bold text-accent-primary drop-shadow-[0_2px_10px_rgba(255,180,50,0.3)]">
              {rotatorPosition?.currentAzimuth?.toFixed(0) || 0}&deg;
            </span>
          </div>
        )}

        {/* Z-30: Globe Border Overlay */}
        {/* Because the Globe at z-20 covered the background circle's right border,
            we redraw just the protruding arc border over the globe here. */}
        {showCockpit && (
          <div
            className="absolute top-1/2 -translate-y-1/2 overflow-hidden z-30 pointer-events-none"
            style={{ left: sidebarEdge, width: bulgeWidth, height: globeSize }}
          >
             <div
               className="absolute top-1/2 -translate-y-1/2 rounded-full border-[2px] border-[#334155]"
               style={{ left: globeOffset - sidebarEdge, width: globeSize, height: globeSize }}
             />
          </div>
        )}

      </div>
    </GlassPanel>
  );
}

import { useEffect, useRef, useCallback, useState } from 'react';
import { Globe as GlobeIcon, Navigation, Target, Maximize2, RadioTower, Pause, Play, Zap, SunMoon } from 'lucide-react';
import { useAppStore } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import { useSignalR } from '../hooks/useSignalR';
import { GlassPanel } from '../components/GlassPanel';
import { gridToLatLon, calculateDistance, calculateBearing, getAnimationDuration } from '../utils/maidenhead';
import { RotatorControls } from './RotatorPlugin';
import { api } from '../api/client';
import { rigModeToSpotModes } from '../utils/rigTracking';
import type { CallsignLookedUpEvent } from '../api/signalr';
import { StrikeStore, type Strike } from '../utils/lightningStrikes';
import { setLightningStrikesCallback, clearLightningStrikesCallback } from '../api/signalr';
import { createDayNightShell, type DayNightShell } from '../utils/dayNightShell';
import { createIonosphereShells, type IonosphereShells } from '../utils/ionosphereShells';
import { getSunPosition } from '../utils/solarCalculations';
// Globe is dynamically imported to catch WebGL errors at load time

// Default station location (can be overridden by store)
const DEFAULT_LAT = 52.6667; // IO52RN - Limerick
const DEFAULT_LON = -8.6333;

// Spherical linear interpolation (SLERP) along a great circle between two lat/lon points.
// t=0 returns start, t=1 returns end.
function interpolateGreatCircle(
  lat1: number, lon1: number,
  lat2: number, lon2: number,
  t: number
): { lat: number; lng: number } {
  const toRad = Math.PI / 180;
  const toDeg = 180 / Math.PI;

  const lat1Rad = lat1 * toRad;
  const lon1Rad = lon1 * toRad;
  const lat2Rad = lat2 * toRad;
  const lon2Rad = lon2 * toRad;

  const x1 = Math.cos(lat1Rad) * Math.cos(lon1Rad);
  const y1 = Math.cos(lat1Rad) * Math.sin(lon1Rad);
  const z1 = Math.sin(lat1Rad);

  const x2 = Math.cos(lat2Rad) * Math.cos(lon2Rad);
  const y2 = Math.cos(lat2Rad) * Math.sin(lon2Rad);
  const z2 = Math.sin(lat2Rad);

  const dot = x1 * x2 + y1 * y2 + z1 * z2;
  const omega = Math.acos(Math.max(-1, Math.min(1, dot)));

  if (Math.abs(omega) < 1e-10) {
    return { lat: lat1, lng: lon1 };
  }

  const sinOmega = Math.sin(omega);
  const a = Math.sin((1 - t) * omega) / sinOmega;
  const b = Math.sin(t * omega) / sinOmega;

  const x = a * x1 + b * x2;
  const y = a * y1 + b * y2;
  const z = a * z1 + b * z2;

  return {
    lat: Math.atan2(z, Math.sqrt(x * x + y * y)) * toDeg,
    lng: Math.atan2(y, x) * toDeg,
  };
}

/**
 * Great-circle Slerp along the LONG path — the reflex-angle arc that goes
 * the OTHER way around the globe from station to target. Uses the same
 * math as the short-path helper above but with the central angle set to
 * (2π - ω) instead of ω. sin() naturally flips sign in the third quadrant,
 * which is exactly what puts the interpolated points on the opposite
 * hemisphere from the short path.
 *
 * Used by the DE→DX Long-Path visualization on the 3D globe.
 */
function interpolateGreatCircleLongPath(
  lat1: number, lon1: number,
  lat2: number, lon2: number,
  t: number
): { lat: number; lng: number } {
  const toRad = Math.PI / 180;
  const toDeg = 180 / Math.PI;

  const lat1Rad = lat1 * toRad;
  const lon1Rad = lon1 * toRad;
  const lat2Rad = lat2 * toRad;
  const lon2Rad = lon2 * toRad;

  const x1 = Math.cos(lat1Rad) * Math.cos(lon1Rad);
  const y1 = Math.cos(lat1Rad) * Math.sin(lon1Rad);
  const z1 = Math.sin(lat1Rad);

  const x2 = Math.cos(lat2Rad) * Math.cos(lon2Rad);
  const y2 = Math.cos(lat2Rad) * Math.sin(lon2Rad);
  const z2 = Math.sin(lat2Rad);

  const dot = x1 * x2 + y1 * y2 + z1 * z2;
  const omega = Math.acos(Math.max(-1, Math.min(1, dot)));
  const omegaLong = 2 * Math.PI - omega;

  const sinLong = Math.sin(omegaLong);
  if (Math.abs(sinLong) < 1e-10) {
    // Antipodal endpoints — great-circle plane is undefined.
    return { lat: lat1, lng: lon1 };
  }

  const a = Math.sin((1 - t) * omegaLong) / sinLong;
  const b = Math.sin(t * omegaLong) / sinLong;

  const x = a * x1 + b * x2;
  const y = a * y1 + b * y2;
  const z = a * z1 + b * z2;

  return {
    lat: Math.atan2(z, Math.sqrt(x * x + y * y)) * toDeg,
    lng: Math.atan2(y, x) * toDeg,
  };
}

/**
 * Rough estimate of the number of ionospheric hops for a path, from distance,
 * band, and whether the path is in daylight. Not a propagation prediction —
 * just a plausible hop count for the visual. The max single-hop ground
 * distance grows with the reflecting layer's virtual height: at night the F2
 * layer sits high (~long hops), by day lower E/F1 layers shorten them; higher
 * bands work lower takeoff angles so reach a bit farther per hop.
 */
function estimateHopCount(distKm: number, freqMHz: number, daytime: boolean): number {
  let maxHopKm = daytime ? 2800 : 4000;
  if (freqMHz >= 21) maxHopKm += 400;        // 15/12/10 m
  else if (freqMHz >= 14) maxHopKm += 200;   // 20/17 m
  else if (freqMHz > 0 && freqMHz < 7) maxHopKm -= 500; // 160/80 m
  maxHopKm = Math.max(1800, Math.min(4200, maxHopKm));
  return Math.max(1, Math.min(12, Math.ceil(distKm / maxHopKm)));
}

// Marker data structure for globe points
interface GlobeMarkerData {
  lat: number;
  lng: number;
  label: string;
  color: string;
  size: number;
  type: 'station' | 'target' | 'pota' | 'dx';
  /** Extra tooltip lines for POTA markers (reference, park, freq/mode). */
  potaDetail?: string[];
  /** DX cluster spot callsign (clicked to add to the log entry panel). */
  dxCall?: string;
  /** Extra tooltip lines for DX markers (band, freq/mode, spotter). */
  dxDetail?: string[];
  /** DX spot frequency (kHz) + mode — used to prefill the log entry on click. */
  frequency?: number;
  mode?: string;
}

/** Animated great-circle arc from a spotter to the DX station it spotted. */
interface GlobeArcData {
  startLat: number;
  startLng: number;
  endLat: number;
  endLng: number;
  color: string;
  dxCall: string;
  frequency: number;
  mode?: string;
  label: string;
}

/** Always-visible callsign label at a spotter or DX endpoint. */
interface GlobeLabelData {
  lat: number;
  lng: number;
  text: string;
  color: string;
  type: 'dx' | 'spotter' | 'pota';
  dxCall?: string;
  activator?: string;
  frequency?: number;
  mode?: string;
}

// Band coloring for DX cluster markers — mirrors MapPlugin's 2D scheme so the
// globe and the flat map stay visually consistent.
const GLOBE_BAND_COLORS: Record<string, string> = {
  '160m': '#8B0000', '80m': '#DC143C', '60m': '#FF6347', '40m': '#FF8C00',
  '30m': '#FFD700', '20m': '#32CD32', '17m': '#00CED1', '15m': '#00BFFF',
  '12m': '#4169E1', '10m': '#8A2BE2', '6m': '#FF00FF',
};
const GLOBE_BAND_RANGES: Record<string, [number, number]> = {
  '160m': [1800, 2000], '80m': [3500, 4000], '60m': [5330, 5410],
  '40m': [7000, 7300], '30m': [10100, 10150], '20m': [14000, 14350],
  '17m': [18068, 18168], '15m': [21000, 21450], '12m': [24890, 24990],
  '10m': [28000, 29700], '6m': [50000, 54000],
};
function globeBandFromFrequency(freq: number): string {
  for (const [band, [min, max]] of Object.entries(GLOBE_BAND_RANGES)) {
    if (freq >= min && freq <= max) return band;
  }
  return '?';
}

interface GlobeInstance {
  (element: HTMLElement): GlobeInstance;
  globeImageUrl(url: string): GlobeInstance;
  globeTileEngineUrl(fn: (x: number, y: number, l: number) => string): GlobeInstance;
  bumpImageUrl(url: string): GlobeInstance;
  backgroundImageUrl(url: string): GlobeInstance;
  showAtmosphere(show: boolean): GlobeInstance;
  atmosphereColor(color: string): GlobeInstance;
  atmosphereAltitude(alt: number): GlobeInstance;
  pointOfView(pov: { lat: number; lng: number; altitude: number }): GlobeInstance;
  pointOfView(): { lat: number; lng: number; altitude: number };
  enablePointerInteraction(enable: boolean): GlobeInstance;
  pointsData(data: unknown[]): GlobeInstance;
  pointLat(accessor: string): GlobeInstance;
  pointLng(accessor: string): GlobeInstance;
  pointColor(accessor: string | ((d: unknown) => string)): GlobeInstance;
  pointAltitude(alt: number | ((d: unknown) => number)): GlobeInstance;
  pointRadius(accessor: string | ((d: unknown) => number)): GlobeInstance;
  pointResolution(res: number): GlobeInstance;
  pointLabel(fn: (d: unknown) => string): GlobeInstance;
  pathsData(data: unknown[]): GlobeInstance;
  pathPoints(accessor: string): GlobeInstance;
  pathPointAlt(accessor: number | ((p: unknown) => number)): GlobeInstance;
  pathColor(accessor: string): GlobeInstance;
  pathStroke(accessor: string): GlobeInstance;
  pathDashLength(len: number | ((d: unknown) => number)): GlobeInstance;
  pathDashGap(gap: number | ((d: unknown) => number)): GlobeInstance;
  pathDashAnimateTime(time: number): GlobeInstance;
  pathTransitionDuration(duration: number): GlobeInstance;
  polygonsData(data: unknown[]): GlobeInstance;
  polygonCapColor(fn: string | ((d: unknown) => string)): GlobeInstance;
  polygonSideColor(fn: string | ((d: unknown) => string)): GlobeInstance;
  polygonAltitude(alt: number | ((d: unknown) => number)): GlobeInstance;
  polygonsTransitionDuration(duration: number): GlobeInstance;
  ringsData(data: unknown[]): GlobeInstance;
  ringColor(accessor: (d: unknown) => (t: number) => string): GlobeInstance;
  ringMaxRadius(accessor: number | ((d: unknown) => number)): GlobeInstance;
  ringPropagationSpeed(accessor: number | ((d: unknown) => number)): GlobeInstance;
  ringRepeatPeriod(accessor: number | ((d: unknown) => number)): GlobeInstance;
  ringAltitude(accessor: number | ((d: unknown) => number)): GlobeInstance;
  ringResolution(res: number): GlobeInstance;
  customLayerData(data: unknown[]): GlobeInstance;
  customThreeObject(fn: (d: unknown) => object): GlobeInstance;
  customThreeObjectUpdate(fn: (obj: object, d: unknown) => void): GlobeInstance;
  arcsData(data: unknown[]): GlobeInstance;
  arcStartLat(accessor: (d: unknown) => number): GlobeInstance;
  arcStartLng(accessor: (d: unknown) => number): GlobeInstance;
  arcEndLat(accessor: (d: unknown) => number): GlobeInstance;
  arcEndLng(accessor: (d: unknown) => number): GlobeInstance;
  arcColor(accessor: (d: unknown) => string | string[]): GlobeInstance;
  arcStroke(accessor: number | ((d: unknown) => number)): GlobeInstance;
  arcAltitudeAutoScale(scale: number): GlobeInstance;
  arcDashLength(len: number): GlobeInstance;
  arcDashGap(gap: number): GlobeInstance;
  arcDashInitialGap(accessor: number | ((d: unknown) => number)): GlobeInstance;
  arcDashAnimateTime(time: number): GlobeInstance;
  arcsTransitionDuration(duration: number): GlobeInstance;
  arcLabel(fn: (d: unknown) => string): GlobeInstance;
  onArcClick(fn: (arc: unknown, event: MouseEvent) => void): GlobeInstance;
  getScreenCoords(lat: number, lng: number, altitude?: number): { x: number; y: number } | undefined;
  getCoords(lat: number, lng: number, altitude?: number): { x: number; y: number; z: number };
  camera(): { position: { x: number; y: number; z: number } };
  // Three.js scene graph + renderer — used to raise texture anisotropy on the
  // globe surface and streamed map tiles so the sphere stays crisp at oblique
  // viewing angles (default anisotropy of 1 reads as "grainy").
  scene(): {
    add(obj: object): void;
    remove(obj: object): void;
    traverse(cb: (obj: unknown) => void): void;
  };
  renderer(): { capabilities: { getMaxAnisotropy(): number } };
  onGlobeClick(fn: (coords: { lat: number; lng: number }) => void): GlobeInstance;
  onPointClick(fn: (point: unknown, event: MouseEvent, coords: { lat: number; lng: number; altitude: number }) => void): GlobeInstance;
  onZoom(fn: (pov: { lat: number; lng: number; altitude: number }) => void): GlobeInstance;
  // OrbitControls (globe.gl >= 2.x). Exposes autoRotate for the slow spin.
  controls(): { autoRotate: boolean; autoRotateSpeed: number };
  width(w: number): GlobeInstance;
  height(h: number): GlobeInstance;
  globeMaterial(): { opacity: number };
}

export function GlobeCore({ hideOverlays }: { hideOverlays?: boolean } = {}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const globeRef = useRef<GlobeInstance | null>(null);
  const animationRef = useRef<number | null>(null);
  const cameraAnimationRef = useRef<number | null>(null);
  const lastTargetCoordsRef = useRef<{ lat: number; lng: number } | null>(null);
  const lastBeamPolyKeyRef = useRef<string | null>(null);

  const { stationGrid, rotatorPosition, focusedCallsignInfo, radioStates, selectedRadioId, potaSpots, dxClusterSpots: spots, dxClusterMapEnabled } = useAppStore();
  const { settings, updateMapSettings, saveSettings } = useSettingsStore();
  const { commandRotator, focusCallsign, selectSpot } = useSignalR();

  const [currentAzimuth, setCurrentAzimuth] = useState(0);
  const [webglError, setWebglError] = useState<string | null>(null);
  const [containerHeight, setContainerHeight] = useState(0);
  // Slow auto-rotation of the globe (on by default); pausable via the overlay button.
  const [isRotating, setIsRotating] = useState(false); // paused on startup; resume via the Play button
  const isRotatingRef = useRef(isRotating);
  isRotatingRef.current = isRotating;
  // Flips true once the globe instance is created, so rotation/marker effects can run.
  const [globeReady, setGlobeReady] = useState(false);
  // Latest focusCallsign handler for the onPointClick closure (avoids stale refs).
  const focusCallsignRef = useRef(focusCallsign);
  focusCallsignRef.current = focusCallsign;
  const selectSpotRef = useRef(selectSpot);
  selectSpotRef.current = selectSpot;
  const strikeStoreRef = useRef(new StrikeStore());
  const dayNightShellRef = useRef<DayNightShell | null>(null);
  const ionoShellsRef = useRef<IonosphereShells | null>(null);
  // Throttle timestamp for the texture-anisotropy sweep (see the label tick).
  const lastAnisoSweepRef = useRef(0);

  // When a radio is connected, the globe shows only DX spots on the radio's
  // current band + mode (so the display follows the rig). No radio → show all.
  const rigState = selectedRadioId ? radioStates.get(selectedRadioId) : undefined;
  const rigFreqHz = rigState?.frequencyHz;
  const rigMode = rigState?.mode;

  // Cache of callsign → resolved coordinates, filled by QRZ lookups for spots
  // the cluster didn't geolocate. `null` is a negative cache (lookup failed) so
  // we don't retry the same callsign every render.
  const gridCacheRef = useRef<Map<string, { lat: number; lon: number } | null>>(new Map());
  const gridInFlightRef = useRef<Set<string>>(new Set());
  const [gridTick, setGridTick] = useState(0);

  // Spotter/DX callsign labels rendered as our own HTML overlay (black boxes),
  // positioned each frame from the globe's projection. Kept out of globe.gl's
  // CSS3D layer, which interferes with the WebGL arc rendering.
  const [globeLabels, setGlobeLabels] = useState<GlobeLabelData[]>([]);
  const globeLabelsRef = useRef<GlobeLabelData[]>([]);
  globeLabelsRef.current = globeLabels;
  const labelElsRef = useRef<(HTMLDivElement | null)[]>([]);
  // The pulsing radio-tower icon we drop over the focused-callsign QTH.
  // Positioned every frame from the same projection tick that drives the
  // callsign labels (see below). Hidden when no callsign is focused OR
  // when the point rotates behind the globe.
  const targetIconRef = useRef<HTMLDivElement | null>(null);
  const focusedInfoLatestRef = useRef<CallsignLookedUpEvent | null>(null);
  focusedInfoLatestRef.current = focusedCallsignInfo;

  // Rotator is enabled in settings
  const rotatorEnabled = settings.rotator.enabled;

  // Get station coordinates from settings - prioritize lat/lon, fall back to grid square, then defaults
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

  // Calculate azimuth between two points
  const calculateAzimuth = useCallback((lat1: number, lon1: number, lat2: number, lon2: number) => {
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
  }, []);

  // Calculate destination point from start point, azimuth and distance
  const getDestinationPoint = useCallback((lat: number, lon: number, azimuth: number, distance: number = 10000) => {
    const R = 6371;
    const toRad = Math.PI / 180;
    const toDeg = 180 / Math.PI;

    const lat1Rad = lat * toRad;
    const lon1Rad = lon * toRad;
    const azimuthRad = azimuth * toRad;
    const angularDistance = distance / R;

    const lat2Rad = Math.asin(
      Math.sin(lat1Rad) * Math.cos(angularDistance) +
      Math.cos(lat1Rad) * Math.sin(angularDistance) * Math.cos(azimuthRad)
    );

    const lon2Rad = lon1Rad + Math.atan2(
      Math.sin(azimuthRad) * Math.sin(angularDistance) * Math.cos(lat1Rad),
      Math.cos(angularDistance) - Math.sin(lat1Rad) * Math.sin(lat2Rad)
    );

    return {
      lat: lat2Rad * toDeg,
      lng: ((lon2Rad * toDeg + 540) % 360) - 180
    };
  }, []);

  // Track target DX station coordinates for the DE→DX path line
  // approximate=true means lat/lng came from the cty.dat country centroid
  // fallback rather than a QRZ/HamQTH lookup — the great-circle to it is
  // drawn dashed + dimmer so the operator can tell at a glance.
  const targetCoordsRef = useRef<{ lat: number; lng: number; approximate: boolean } | null>(null);

  // Track focused callsign info for label callback
  const focusedCallsignInfoRef = useRef<CallsignLookedUpEvent | null>(null);
  focusedCallsignInfoRef.current = focusedCallsignInfo;

  // Render the beam visualization (rotator beam + DE→DX line)
  // Long-path visibility — read once per render outside the animation loop.
  const showLongPathRef = useRef<boolean>(settings.map.showLongPath !== false);
  showLongPathRef.current = settings.map.showLongPath !== false;
  const showIonoHopsRef = useRef<boolean>(!!settings.map.showIonosphereHops);
  showIonoHopsRef.current = !!settings.map.showIonosphereHops;
  const rigFreqRef = useRef<number | undefined>(rigFreqHz);
  rigFreqRef.current = rigFreqHz;

  const renderBeam = useCallback((azimuth: number, isConnected: boolean) => {
    if (!globeRef.current) return;

    // dashLength / dashGap = 0 → solid line (real QRZ/HamQTH coords).
    // dashLength / dashGap > 0 → dashed line (cty.dat centroid fallback,
    // "approximately in that country" — visual cue for the operator).
    const pathsData: { path: [number, number, number][]; color: string; stroke: number; dashLength: number; dashGap: number }[] = [];
    const numSegments = 50;
    const BEAM_WIDTH_DEG = 45;          // total azimuthal spread of the beam triangle
    const DEFAULT_BEAM_KM = 5000;       // length when no DX target is focused
    const TARGET_OVERSHOOT = 1.1;       // extend a little past the DX spot

    // Beam length: just past the focused DX target, or a modest default
    const target = targetCoordsRef.current;
    const beamLength = target
      ? calculateDistance(stationLat, stationLon, target.lat, target.lng) * TARGET_OVERSHOOT
      : DEFAULT_BEAM_KM;

    // Rotator beam: a green shaded triangle (apex at the station, edges at
    // constant ±22.5°), built from small quad polygons — small quads
    // triangulate safely at any azimuth (one huge polygon breaks across the
    // antimeridian). Meshes only rebuild when azimuth or length changes.
    const polyKey = isConnected ? `${Math.round(azimuth * 10)}:${Math.round(beamLength)}` : 'off';
    if (polyKey !== lastBeamPolyKeyRef.current) {
      lastBeamPolyKeyRef.current = polyKey;

      const polygons: { geometry: { type: 'Polygon'; coordinates: number[][][] } }[] = [];
      if (isConnected) {
        // Subdivide the wedge in BOTH directions: large curved quads render
        // with flat interior triangles that sag below the globe surface and
        // get occluded — visible as triangle-shaped holes, worse at long
        // beam lengths. Small quads (≤ ~5.6° wide, ≤ ~500 km tall) hug the
        // sphere and shade solidly.
        const azSteps = 8;
        const radialSteps = Math.min(30, Math.max(8, Math.ceil(beamLength / 500)));
        const grid: [number, number][][] = [];
        for (let r = 0; r <= radialSteps; r++) {
          const distance = (beamLength / radialSteps) * r;
          const row: [number, number][] = [];
          for (let c = 0; c <= azSteps; c++) {
            const az = (azimuth - BEAM_WIDTH_DEG / 2 + (BEAM_WIDTH_DEG * c) / azSteps + 360) % 360;
            const p = getDestinationPoint(stationLat, stationLon, az, distance);
            row.push([p.lng, p.lat]);
          }
          grid.push(row);
        }
        for (let r = 0; r < radialSteps; r++) {
          for (let c = 0; c < azSteps; c++) {
            polygons.push({
              geometry: {
                type: 'Polygon',
                coordinates: [[grid[r][c], grid[r + 1][c], grid[r + 1][c + 1], grid[r][c + 1], grid[r][c]]],
              },
            });
          }
        }
      }
      globeRef.current.polygonsData(polygons);
    }

    // Render great-circle paths from DE station to DX target as elevated
    // arcs with a whole-line alpha pulse ("heartbeat") — the arcs remain
    // solid solid lines that gently brighten and dim in unison rather than
    // dashes flowing along them. Reads as "beam is aimed here" without
    // demanding the operator's attention.
    //
    // globe.gl's built-in arcsData always draws the SHORT path so we can't
    // use it for LP. Instead we build both as pathsData polylines with a
    // per-point altitude bulge (sin(π·t) · peakAlt) so each arcs off the
    // sphere. renderBeam runs every animation frame, so recomputing the
    // pulse alpha per frame is free — we just modulate the color string.
    const targetCoords = targetCoordsRef.current;
    if (targetCoords !== null) {
      const isApprox = targetCoords.approximate;

      // Pulse factor — 0..1, sinusoidal, ~3.5s per full breath. Slow enough
      // to feel calm and background-y, fast enough to feel alive. Both
      // arcs pulse in unison so the visual reads as a single station-scale
      // heartbeat, not two independent lines.
      const PULSE_PERIOD_MS = 3500;
      const pulseFactor = 0.5 + 0.5 * Math.sin(performance.now() * (2 * Math.PI) / PULSE_PERIOD_MS);

      // Base alpha (when the pulse is at its peak) and floor alpha (trough).
      // Approx (cty.dat centroid) sits dimmer overall so the "these coords
      // are a guess" hint stays visible under the alpha modulation.
      const spAlphaPeak = isApprox ? 0.6  : 0.95;
      const spAlphaFloor = spAlphaPeak * 0.35;
      const lpAlphaPeak = isApprox ? 0.6  : 0.95;
      const lpAlphaFloor = lpAlphaPeak * 0.35;
      const spAlpha = (spAlphaFloor + (spAlphaPeak - spAlphaFloor) * pulseFactor).toFixed(3);
      const lpAlpha = (lpAlphaFloor + (lpAlphaPeak - lpAlphaFloor) * pulseFactor).toFixed(3);

      const SP_COLOR = `rgba(255, 68, 102, ${spAlpha})`;   // red-orange
      const LP_COLOR = `rgba(163, 230, 53, ${lpAlpha})`;   // lime green

      // Peak altitude of the LP arc bulge above the surface. LP is much
      // longer and gets a higher bulge — reinforces visually that it's the
      // "long way around" while keeping both curves clearly separated in 3D
      // so they never overlap or merge as the operator rotates the globe.
      const LP_PEAK_ALT = 0.22;

      // ── Short path — ionospheric skip ───────────────────────────────
      // The great-circle line dips to the ground and arcs up to the
      // ionosphere `hops` times (altitude = |sin(hops·π·t)|), so it reads
      // as the signal bouncing its way to the DX rather than a single bulge.
      // Hop count scales with distance (~one hop per 3000 km, like real HF).
      // A bright pulse then travels the hops from the station toward the DX.
      const SP_PEAK_ALT = 0.28;  // tall, dramatic hop height (fraction of radius)
      const R_KM = 6371;
      const toRadHop = Math.PI / 180;
      const dLat = (targetCoords.lat - stationLat) * toRadHop;
      const dLon = (targetCoords.lng - stationLon) * toRadHop;
      const hav = Math.sin(dLat / 2) ** 2 +
        Math.cos(stationLat * toRadHop) * Math.cos(targetCoords.lat * toRadHop) * Math.sin(dLon / 2) ** 2;
      const distKm = 2 * R_KM * Math.asin(Math.min(1, Math.sqrt(hav)));
      // Hop count estimated from distance, band, and day/night at the path
      // midpoint (falls back to 20 m if no rig frequency is known).
      const midHop = interpolateGreatCircle(stationLat, stationLon, targetCoords.lat, targetCoords.lng, 0.5);
      const sunHop = getSunPosition(new Date());
      const daytimeMid = calculateDistance(midHop.lat, midHop.lng, sunHop.lat, sunHop.lon) < 10000;
      const freqMHz = (rigFreqRef.current ?? 0) / 1e6 || 14;
      const hops = estimateHopCount(distKm, freqMHz, daytimeMid);
      const SP_SEGMENTS = hops * 40; // enough points to keep the peaks sharp

      const ionoHops = showIonoHopsRef.current;
      const targetPath: [number, number, number][] = [];
      for (let i = 0; i <= SP_SEGMENTS; i++) {
        const t = i / SP_SEGMENTS;
        const point = interpolateGreatCircle(stationLat, stationLon, targetCoords.lat, targetCoords.lng, t);
        // Ionospheric skip on: sharp triangle wave (ground → ionosphere →
        // ground per hop). Off: a single smooth arc bulge.
        const alt = ionoHops
          ? (((hops * t) % 1) < 0.5 ? ((hops * t) % 1) * 2 : (1 - ((hops * t) % 1)) * 2) * SP_PEAK_ALT
          : Math.sin(Math.PI * t) * 0.12;
        targetPath.push([point.lat, point.lng, alt]);
      }
      // Steady (gently breathing) base line showing the whole hop zigzag.
      pathsData.push({ path: targetPath, color: SP_COLOR, stroke: 3, dashLength: 0, dashGap: 0 });

      // Bright pulse travelling station → DX along the hops (~2.2 s per pass).
      const PULSE_TRAVEL_MS = 2200;
      const pulsePos = (performance.now() % PULSE_TRAVEL_MS) / PULSE_TRAVEL_MS;
      const pulseHalf = 0.05; // pulse covers ~10% of the path
      const pulsePath = targetPath.filter((_, i) => {
        const t = i / SP_SEGMENTS;
        return t >= pulsePos - pulseHalf && t <= pulsePos + pulseHalf;
      });
      if (pulsePath.length >= 2) {
        pathsData.push({ path: pulsePath, color: 'rgba(255, 235, 225, 1)', stroke: 5, dashLength: 0, dashGap: 0 });
      }

      // ── Long path (lime green, higher bulge) ────────────────────────
      // The reflex-angle arc going the other way around the globe. Same
      // start/end points, opposite hemisphere in between. ~3× longer than
      // SP so uses double the segments to keep the curve smooth.
      // Off by user preference (Settings → Map → Show Long Path) → skip.
      if (showLongPathRef.current) {
        const LP_SEGMENTS = numSegments * 2;
        const longPath: [number, number, number][] = [];
        for (let i = 0; i <= LP_SEGMENTS; i++) {
          const t = i / LP_SEGMENTS;
          const point = interpolateGreatCircleLongPath(stationLat, stationLon, targetCoords.lat, targetCoords.lng, t);
          const alt = Math.sin(Math.PI * t) * LP_PEAK_ALT;
          longPath.push([point.lat, point.lng, alt]);
        }
        pathsData.push({
          path: longPath,
          color: LP_COLOR,
          stroke: 2.5,
          dashLength: 0,
          dashGap: 0,
        });
      }
    }

    globeRef.current
      .pathsData(pathsData)
      .pathPoints('path')
      // Use each point's 3rd element as altitude — WITHOUT this globe.gl
      // defaults to a fixed near-zero altitude and draws every path flat on
      // the surface, silently discarding the hop/bulge heights.
      .pathPointAlt((p: unknown) => (p as number[])[2])
      .pathColor('color')
      .pathStroke('stroke')
      .pathDashLength((d: unknown) => (d as { dashLength: number }).dashLength)
      .pathDashGap((d: unknown) => (d as { dashGap: number }).dashGap)
      // No dash flow — the arcs pulse in unison via the alpha modulation
      // computed inside renderBeam above. Setting animate-time to 0 keeps
      // globe.gl's dash-motion machinery idle.
      .pathDashAnimateTime(0)
      .pathTransitionDuration(0);
  }, [stationLat, stationLon, getDestinationPoint]);

  // Animation loop - use ref to avoid dependency on currentAzimuth
  const currentAzimuthRef = useRef(currentAzimuth);
  currentAzimuthRef.current = currentAzimuth;

  // Track rotator enabled status in ref for animation loop
  const rotatorEnabledRef = useRef(rotatorEnabled);
  rotatorEnabledRef.current = rotatorEnabled;

  // Store callbacks in refs to avoid re-initializing globe
  const commandRotatorRef = useRef(commandRotator);
  commandRotatorRef.current = commandRotator;
  const calculateAzimuthRef = useRef(calculateAzimuth);
  calculateAzimuthRef.current = calculateAzimuth;

  // Track last command time and commanded azimuth to avoid rotator position overwriting clicked value
  const lastCommandTimeRef = useRef<number>(0);
  const commandedAzimuthRef = useRef<number | null>(null);
  const displayedAzimuthRef = useRef<number>(0);

  const animateBeam = useCallback(() => {
    renderBeam(currentAzimuthRef.current, rotatorEnabledRef.current);
    animationRef.current = requestAnimationFrame(animateBeam);
  }, [renderBeam]);

  // Initialize globe - only runs once on mount
  useEffect(() => {
    if (!containerRef.current) return;

    // Quick WebGL availability check — only tests for context existence.
    // The shader-compilation pre-check that was here produced false negatives on
    // Windows (ANGLE backend) and Linux (Mesa) even when WebGL was fully functional,
    // preventing the globe from loading on non-macOS platforms.
    // globe.gl's own Three.js initialization handles deeper WebGL failures via the
    // try/catch below.
    try {
      const testCanvas = document.createElement('canvas');
      const gl = testCanvas.getContext('webgl2') ?? testCanvas.getContext('webgl');
      if (!gl) {
        setWebglError('WebGL is not supported in your browser.');
        return;
      }
    } catch (e) {
      setWebglError('WebGL is not available in this environment.');
      return;
    }

    // Variables for cleanup
    let resizeObserver: ResizeObserver | null = null;
    let resizeTimeout: ReturnType<typeof setTimeout> | null = null;
    let debouncedResizeFn: (() => void) | null = null;
    let isCancelled = false;

    // Dynamically import globe.gl to catch WebGL errors at load time
    const initGlobe = async () => {
      // Wait for container to have valid dimensions
      const waitForDimensions = async (): Promise<boolean> => {
        for (let i = 0; i < 20; i++) { // Try for up to 2 seconds
          if (!containerRef.current || isCancelled) return false;
          const rect = containerRef.current.getBoundingClientRect();
          if (rect.width > 0 && rect.height > 0) {
            return true;
          }
          await new Promise(resolve => setTimeout(resolve, 100));
        }
        return false;
      };

      const hasDimensions = await waitForDimensions();
      if (!hasDimensions) {
        if (!isCancelled) {
          setWebglError('Globe container failed to initialize. Please try resizing the window.');
        }
        return;
      }

      let Globe;
      let THREE: typeof import('three');
      try {
        THREE = await import('three');
        const module = await import('globe.gl');
        Globe = module.default;
      } catch (e) {
        if (!isCancelled) {
          setWebglError('Failed to load globe.gl');
        }
        return;
      }

      if (isCancelled || !containerRef.current) return;

      let globe: GlobeInstance | null = null;

      try {
        // @ts-expect-error - globe.gl typings issue with dynamic import
        globe = Globe() as GlobeInstance;
      } catch (e) {
        if (!isCancelled) {
          setWebglError('Failed to initialize 3D Globe.');
        }
        return;
      }

      if (isCancelled || !containerRef.current) return;

      try {
        globe(containerRef.current)
        // Use Google satellite tiles with labels - shows countries, cities as you zoom.
        // Online enhancement only; the bundled base textures below render without internet.
        .globeTileEngineUrl((x, y, l) => `https://mt1.google.com/vt/lyrs=y&x=${x}&y=${y}&z=${l}`)
        // Bundled locally (public/textures) so the globe works offline and does not depend
        // on the unpkg CDN, which was leaving the globe blank when unreachable.
        .globeImageUrl('./textures/earth-blue-marble.jpg')
        .bumpImageUrl('./textures/earth-topology.png')
        .backgroundImageUrl('./textures/night-sky.png')
        .showAtmosphere(true)
        .atmosphereColor('rgba(10, 14, 20, 0.4)')
        .atmosphereAltitude(0.25)
        .pointOfView({ lat: stationLat, lng: stationLon, altitude: 1.7 })
        .enablePointerInteraction(true);

      // Rotator beam wedge: green shaded fill (no side walls, no per-quad stroke)
      lastBeamPolyKeyRef.current = null; // force re-push after (re)init
      globe
        .polygonsData([])
        .polygonCapColor(() => 'rgba(58, 168, 85, 0.35)') // natural green shade
        .polygonSideColor(() => 'rgba(0, 0, 0, 0)')
        .polygonAltitude(0.012) // enough clearance that quad interiors never dip below the surface
        .polygonsTransitionDuration(0);

      // Configure marker appearance (data will be set by effect)
      globe
        .pointsData([]) // Initial empty - will be populated by effect
        .pointLat('lat')
        .pointLng('lng')
        .pointColor('color')
        .pointAltitude((d: unknown) => {
          const data = d as GlobeMarkerData;
          return data.type === 'target' ? 0.015 : 0.01;
        })
        .pointRadius('size')
        .pointResolution(12)
        .pointLabel((d: unknown) => {
          const data = d as GlobeMarkerData;
          if (data.type === 'station') {
            return `<div style="text-align: center; padding: 5px; background: rgba(10, 14, 20, 0.9); border-radius: 3px; color: #8899aa; border: 1px solid rgba(255, 180, 50, 0.12);">
              <div style="font-weight: bold; color: #ffb432;">${data.label}</div>
              <div style="font-size: 0.8em;">Station Location</div>
            </div>`;
          } else if (data.type === 'pota') {
            const detail = (data.potaDetail ?? [])
              .map((line) => `<div style="font-size: 0.8em;">${line}</div>`)
              .join('');
            return `<div style="text-align: center; padding: 5px; background: rgba(10, 14, 20, 0.9); border-radius: 3px; color: #8899aa; border: 1px solid rgba(16, 185, 129, 0.25);">
              <div style="font-weight: bold; color: #10b981;">${data.label}</div>
              ${detail}
            </div>`;
          } else if (data.type === 'dx') {
            const detail = (data.dxDetail ?? [])
              .map((line) => `<div style="font-size: 0.8em;">${line}</div>`)
              .join('');
            return `<div style="text-align: center; padding: 5px; background: rgba(10, 14, 20, 0.9); border-radius: 3px; color: #8899aa; border: 1px solid ${data.color}55;">
              <div style="font-weight: bold; color: ${data.color};">${data.label}</div>
              ${detail}
              <div style="font-size: 0.7em; color: #667; margin-top: 2px;">click to add to log</div>
            </div>`;
          } else {
            // Target marker - use ref to access latest focusedCallsignInfo
            const info = focusedCallsignInfoRef.current;
            return `<div style="text-align: center; padding: 5px; background: rgba(10, 14, 20, 0.9); border-radius: 3px; color: #8899aa; border: 1px solid rgba(255, 180, 50, 0.12);">
              <div style="font-weight: bold; color: #ff4466;">${data.label}</div>
              ${info?.grid ? `<div style="font-size: 0.8em; color: #8899aa;">${info.grid}</div>` : ''}
              ${info?.bearing != null ? `<div style="font-size: 0.8em; color: #00ddff;">
                ${info.bearing.toFixed(0)}° / ${Math.round(info.distance ?? 0)} km
              </div>` : ''}
            </div>`;
          }
        });

      // Handle globe clicks to set azimuth - use refs to avoid stale closures
      globe.onGlobeClick((coords) => {
        if (!rotatorEnabledRef.current) return; // Ignore clicks when rotator disabled
        if (coords && coords.lat !== undefined && coords.lng !== undefined) {
          const azimuth = calculateAzimuthRef.current(stationLat, stationLon, coords.lat, coords.lng);
          lastCommandTimeRef.current = Date.now();
          commandedAzimuthRef.current = azimuth;
          displayedAzimuthRef.current = azimuth;
          setCurrentAzimuth(azimuth);
          commandRotatorRef.current(azimuth, 'globe');
        }
      });

      // DX cluster arcs — animated great-circle paths from the spotter to the DX
      // station, colored by band. The dash animation flows start→end (spotter→DX).
      // Data is set by the marker/arc effect below.
      globe
        .arcsData([])
        .arcStartLat((d: unknown) => (d as GlobeArcData).startLat)
        .arcStartLng((d: unknown) => (d as GlobeArcData).startLng)
        .arcEndLat((d: unknown) => (d as GlobeArcData).endLat)
        .arcEndLng((d: unknown) => (d as GlobeArcData).endLng)
        .arcColor((d: unknown) => (d as GlobeArcData).color)
        .arcStroke(0.5)
        .arcAltitudeAutoScale(0.4)
        .arcDashLength(0.4)
        .arcDashGap(0.2)
        .arcDashInitialGap(() => Math.random())
        .arcDashAnimateTime(1500)
        .arcsTransitionDuration(0)
        .arcLabel((d: unknown) => {
          const a = d as GlobeArcData;
          return `<div style="text-align:center;padding:4px 6px;background:rgba(10,14,20,0.9);border-radius:3px;color:#8899aa;border:1px solid ${a.color}55;">
            <div style="font-weight:bold;color:${a.color};">${a.label}</div>
            <div style="font-size:0.7em;color:#667;margin-top:2px;">click to add to log</div>
          </div>`;
        })
        .onArcClick((arc: unknown) => {
          const a = arc as GlobeArcData;
          if (a?.dxCall) selectSpotRef.current(a.dxCall, a.frequency, a.mode);
        });

      // Callsign labels are drawn as an HTML overlay (see the positioning effect
      // and JSX below), not via globe.gl's label/HTML layers.

      // Clicking a DX cluster marker adds that spot to the Log Entry panel (point
      // clicks fire here, not onGlobeClick, so they don't disturb the rotator beam).
      globe.onPointClick((point) => {
        const data = point as GlobeMarkerData;
        if (data?.type === 'dx' && data.dxCall) {
          selectSpotRef.current(data.dxCall, data.frequency ?? 0, data.mode);
        }
      });

      // Slow auto-spin via OrbitControls. The effect below keeps it in sync with
      // the pause/play state; this just sets the speed and the initial value.
      const controls = globe.controls();
      controls.autoRotateSpeed = 0.35; // gentle; ~5 min per revolution
      controls.autoRotate = isRotatingRef.current;

      // Set material opacity
      const material = globe.globeMaterial();
      material.opacity = 0.95;

      // Lightning strike rings — the strike layer owns ringsData (see the strike
      // effect + the removed per-frame clear in renderBeam). Pure white (keeps
      // clear of the DX-spot/POTA palette); intensity fades with strike age,
      // local strikes render brighter than distant ones.
      globe
        .ringColor((d: unknown) => {
          const s = d as { local: boolean; ts: number };
          return (t: number) => {
            const ageMin = (Date.now() - s.ts) / 60_000;
            const ageFactor = ageMin < 1 ? 1 : ageMin < 5 ? 0.7 : 0.45;
            const alpha = Math.max(0, 1 - t) * ageFactor * (s.local ? 1 : 0.6);
            return `rgba(255, 255, 255, ${alpha.toFixed(3)})`;
          };
        })
        .ringMaxRadius((d: unknown) => ((d as { local: boolean }).local ? 3.5 : 2.5))
        .ringPropagationSpeed(2)
        .ringRepeatPeriod(2000)   // re-ripple while the strike is retained (5–10 min)
        .ringAltitude(0.006)
        .ringResolution(32)   // halved — ring count is capped, keep per-ring cost low
        .ringsData([]);

      // Strike-center lightning bolts (custom layer): a red bolt with a white
      // outline marks each strike's exact spot for 60 s after live arrival, then
      // vanishes (the strike effect filters). One shared canvas texture;
      // billboard sprites stay cheap even at a few hundred live strikes.
      const boltCanvas = document.createElement('canvas');
      boltCanvas.width = 64;
      boltCanvas.height = 64;
      const boltCtx = boltCanvas.getContext('2d');
      if (boltCtx) {
        // Bolt inset from the 64px edges so the white outline never clips.
        boltCtx.beginPath();
        boltCtx.moveTo(40, 8);
        boltCtx.lineTo(16, 36);
        boltCtx.lineTo(30, 36);
        boltCtx.lineTo(24, 56);
        boltCtx.lineTo(48, 26);
        boltCtx.lineTo(34, 26);
        boltCtx.closePath();
        // White outline first (drawn wide, so the red fill covers its inner half).
        boltCtx.lineJoin = 'round';
        boltCtx.strokeStyle = '#ffffff';
        boltCtx.lineWidth = 6;
        boltCtx.stroke();
        // Red fill on top.
        boltCtx.fillStyle = '#ff2a2a';
        boltCtx.fill();
      }
      const boltTexture = new THREE.CanvasTexture(boltCanvas);
      globe
        .customThreeObject(() => {
          const sprite = new THREE.Sprite(new THREE.SpriteMaterial({
            map: boltTexture,
            transparent: true,
            depthWrite: false,
          }));
          sprite.scale.set(3, 3, 1);
          return sprite;
        })
        .customThreeObjectUpdate((obj: object, d: unknown) => {
          const s = d as { lat: number; lng: number };
          const p = globe!.getCoords(s.lat, s.lng, 0.012);
          (obj as { position: { set: (x: number, y: number, z: number) => void } }).position.set(p.x, p.y, p.z);
        })
        .customLayerData([]);

      // Day/night terminator shell — a plain scene child (no globe.gl data
      // layer, so it cannot collide with beam/lightning/marker layers).
      // Hidden until the settings effect enables a layer. globe.gl's globe
      // radius is 100 world units.
      const dayNightShell = createDayNightShell(THREE, 100);
      globe.scene().add(dayNightShell.mesh);
      dayNightShellRef.current = dayNightShell;

      // Ionosphere D/E/F glow shells (hidden until the hops layer is enabled).
      const ionoShells = createIonosphereShells(THREE, 100);
      for (const mesh of ionoShells.meshes) globe.scene().add(mesh);
      ionoShellsRef.current = ionoShells;

      globeRef.current = globe;
      setGlobeReady(true);

      // Handle resize
      let lastWidth = 0;
      let lastHeight = 0;
      let isResizing = false;

      const handleResize = () => {
        if (isResizing || !containerRef.current || !globeRef.current) return;

        const rect = containerRef.current.getBoundingClientRect();
        const width = Math.floor(rect.width);
        const height = Math.floor(rect.height);

        // Update container height state for conditional overlays
        setContainerHeight(height);

        const widthDiff = Math.abs(width - lastWidth);
        const heightDiff = Math.abs(height - lastHeight);

        if (width > 0 && height > 0 && (widthDiff > 5 || heightDiff > 5)) {
          isResizing = true;
          lastWidth = width;
          lastHeight = height;
          globeRef.current.width(width).height(height);
          setTimeout(() => { isResizing = false; }, 100);
        }
      };

      const debouncedResize = () => {
        if (resizeTimeout) {
          clearTimeout(resizeTimeout);
        }
        resizeTimeout = setTimeout(handleResize, 300);
      };

      debouncedResizeFn = debouncedResize;
      resizeObserver = new ResizeObserver(debouncedResize);
      resizeObserver.observe(containerRef.current);
      window.addEventListener('resize', debouncedResize);
      setTimeout(handleResize, 100);
      animationRef.current = requestAnimationFrame(animateBeam);

      } catch (e) {
        if (!isCancelled) {
          setWebglError('Failed to initialize 3D Globe.');
        }
      }
    };

    // Start async initialization
    initGlobe();

    // Cleanup function
    return () => {
      isCancelled = true;
      if (resizeObserver) {
        resizeObserver.disconnect();
      }
      if (debouncedResizeFn) {
        window.removeEventListener('resize', debouncedResizeFn);
      }
      if (resizeTimeout) {
        clearTimeout(resizeTimeout);
      }
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
      }
      dayNightShellRef.current?.dispose();
      dayNightShellRef.current = null;
      ionoShellsRef.current?.dispose();
      ionoShellsRef.current = null;
      setGlobeReady(false);
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stationLat, stationLon, stationGrid]);

  // Update beam when rotator position changes
  useEffect(() => {
    if (rotatorPosition?.currentAzimuth != null && typeof rotatorPosition.currentAzimuth === 'number') {
      const newPosition = rotatorPosition.currentAzimuth;
      const currentDisplay = displayedAzimuthRef.current;
      const isNearZero = currentDisplay <= 30 || currentDisplay >= 330;
      const isSuspiciousZero = newPosition === 0 && !isNearZero;
      if (isSuspiciousZero) return;
      const timeSinceCommand = Date.now() - lastCommandTimeRef.current;
      const commanded = commandedAzimuthRef.current;
      if (timeSinceCommand < 1000 && commanded !== null) {
        const diff = Math.abs(newPosition - commanded);
        const wrappedDiff = Math.min(diff, 360 - diff);
        if (wrappedDiff <= 15) {
          displayedAzimuthRef.current = newPosition;
          setCurrentAzimuth(newPosition);
        }
      } else {
        commandedAzimuthRef.current = null;
        displayedAzimuthRef.current = newPosition;
        setCurrentAzimuth(newPosition);
      }
    }
  }, [rotatorPosition]);

  // Update target DX coordinates when focused callsign changes
  useEffect(() => {
    if (focusedCallsignInfo?.latitude != null && focusedCallsignInfo?.longitude != null) {
      targetCoordsRef.current = {
        lat: focusedCallsignInfo.latitude,
        lng: focusedCallsignInfo.longitude,
        approximate: focusedCallsignInfo.latLonIsApproximate === true,
      };
    } else {
      targetCoordsRef.current = null;
    }
  }, [focusedCallsignInfo]);

  // Update globe markers when focused callsign changes
  useEffect(() => {
    if (!globeRef.current) return;

    const markerData: GlobeMarkerData[] = [{
      lat: stationLat,
      lng: stationLon,
      label: stationGrid || 'Station',
      color: '#ffb432',
      size: 0.05,
      type: 'station',
    }];

    // On the clean 2D-map globe (hideOverlays) skip the red target point — the
    // radio-tower icon marks the DX QTH instead of a reddish blob.
    if (!hideOverlays && focusedCallsignInfo?.latitude != null && focusedCallsignInfo?.longitude != null) {
      markerData.push({
        lat: focusedCallsignInfo.latitude,
        lng: focusedCallsignInfo.longitude,
        label: focusedCallsignInfo.callsign,
        color: '#ff4466',
        size: 0.04,
        type: 'target',
      });
    }

    // POTA overlay (same setting the 2D map used) — emerald park markers
    if (settings.map.showPotaOverlay) {
      for (const spot of potaSpots) {
        if (spot.latitude == null || spot.longitude == null ||
            !isFinite(spot.latitude) || !isFinite(spot.longitude)) continue;
        markerData.push({
          lat: spot.latitude,
          lng: spot.longitude,
          label: spot.activator,
          color: '#10b981',
          size: 0.025,
          type: 'pota',
          potaDetail: [
            spot.reference,
            spot.parkName || '',
            `${(parseFloat(spot.frequency) / 1000).toFixed(3)} MHz ${spot.mode || ''}`.trim(),
          ].filter(Boolean),
        });
      }
    }

    // DX cluster spots — band-colored markers at the DX station plus animated
    // great-circle arcs from the spotter to the DX. Gated by the same toggle as
    // the 2D map. Clicking a marker or arc adds the spot to the Log Entry panel.
    const arcData: GlobeArcData[] = [];
    const labelData: GlobeLabelData[] = [];
    const labeledCalls = new Set<string>();

    // Follow the connected radio: when a rig is connected, only show spots on its
    // current band and mode. No rig connected → show all spots (all bands).
    const rigBand = rigFreqHz != null ? globeBandFromFrequency(rigFreqHz / 1000) : null;
    const rigModes = rigMode ? rigModeToSpotModes(rigMode) : null;

    // Resolve a spot endpoint's coordinates: prefer the grid the cluster supplied,
    // otherwise fall back to a QRZ lookup cached by callsign (see the effect below).
    const resolveCoords = (grid: string | undefined, call: string | undefined): { lat: number; lon: number } | null => {
      if (grid) {
        const c = gridToLatLon(grid);
        if (c) return { lat: c.lat, lon: c.lon };
      }
      if (call) {
        const cached = gridCacheRef.current.get(call);
        if (cached) return cached;
      }
      return null;
    };

    if (dxClusterMapEnabled) {
      for (const spot of spots) {
        // Band/mode filter — follow the rig.
        if (rigBand && rigBand !== '?' && globeBandFromFrequency(spot.frequency) !== rigBand) continue;
        if (rigModes) {
          let m = spot.mode?.toUpperCase();
          if (m === 'USB' || m === 'LSB') m = 'SSB';
          if (m && !rigModes.includes(m)) continue;
        }

        const coords = resolveCoords(spot.dxStation?.grid, spot.dxCall);
        if (!coords) continue;
        const band = globeBandFromFrequency(spot.frequency);
        const color = GLOBE_BAND_COLORS[band] || '#888888';

        // Always-visible DX label (click to add to the log).
        if (spot.dxCall && !labeledCalls.has(spot.dxCall)) {
          labeledCalls.add(spot.dxCall);
          labelData.push({
            lat: coords.lat,
            lng: coords.lon,
            text: `${spot.dxCall} ${(spot.frequency / 1000).toFixed(3)}`,
            color,
            type: 'dx',
            dxCall: spot.dxCall,
            frequency: spot.frequency,
            mode: spot.mode,
          });
        }

        // Arc from spotter → DX plus the spotter's label (when located).
        const spotterCoords = resolveCoords(spot.spotterStation?.grid, spot.spotter);
        if (spotterCoords) {
          arcData.push({
            startLat: spotterCoords.lat,
            startLng: spotterCoords.lon,
            endLat: coords.lat,
            endLng: coords.lon,
            color,
            dxCall: spot.dxCall,
            frequency: spot.frequency,
            mode: spot.mode,
            label: `${spot.dxCall} ${(spot.frequency / 1000).toFixed(3)}`,
          });
          if (spot.spotter && !labeledCalls.has(spot.spotter)) {
            labeledCalls.add(spot.spotter);
            labelData.push({
              lat: spotterCoords.lat,
              lng: spotterCoords.lon,
              text: spot.spotter,
              color,
              type: 'spotter',
            });
          }
        }
      }
    }

    // POTA labels — always-visible like the DX labels (click to add to the log).
    if (settings.map.showPotaOverlay) {
      for (const spot of potaSpots) {
        if (spot.latitude == null || spot.longitude == null ||
            !isFinite(spot.latitude) || !isFinite(spot.longitude)) continue;
        const freqKhz = parseFloat(spot.frequency);
        labelData.push({
          lat: spot.latitude,
          lng: spot.longitude,
          text: `${spot.reference} ${isFinite(freqKhz) ? (freqKhz / 1000).toFixed(3) : ''}`.trim(),
          color: '#10b981',
          type: 'pota',
          activator: spot.activator,
          frequency: isFinite(freqKhz) ? freqKhz : undefined,
          mode: spot.mode ?? undefined,
        });
      }
    }

    globeRef.current.pointsData(markerData);
    globeRef.current.arcsData(arcData);
    setGlobeLabels(labelData);
  }, [focusedCallsignInfo, stationLat, stationLon, stationGrid, potaSpots, settings.map.showPotaOverlay, spots, dxClusterMapEnabled, globeReady, gridTick, rigFreqHz, rigMode, hideOverlays]);

  // Fill in missing spot locations via QRZ. For any spot whose DX or spotter
  // callsign has no grid from the cluster, look it up on QRZ (once, cached) so
  // the globe can place the marker and draw the spotter→DX arc. Lookups are
  // sequential to stay friendly to QRZ's rate limits; failures are negatively
  // cached so we don't retry them.
  useEffect(() => {
    if (!dxClusterMapEnabled) return;
    let cancelled = false;

    // Only resolve spots that match the rig's band/mode (when following a rig),
    // so we don't spend QRZ lookups on spots the globe won't show.
    const rigBand = rigFreqHz != null ? globeBandFromFrequency(rigFreqHz / 1000) : null;
    const rigModes = rigMode ? rigModeToSpotModes(rigMode) : null;
    const matchesRig = (spot: typeof spots[number]) => {
      if (rigBand && rigBand !== '?' && globeBandFromFrequency(spot.frequency) !== rigBand) return false;
      if (rigModes) {
        let m = spot.mode?.toUpperCase();
        if (m === 'USB' || m === 'LSB') m = 'SSB';
        if (m && !rigModes.includes(m)) return false;
      }
      return true;
    };

    const needed: string[] = [];
    const seen = new Set<string>();
    const consider = (call: string | undefined, hasGrid: boolean) => {
      if (!call || hasGrid) return;
      if (gridCacheRef.current.has(call) || gridInFlightRef.current.has(call) || seen.has(call)) return;
      seen.add(call);
      needed.push(call);
    };
    for (const spot of spots) {
      if (!matchesRig(spot)) continue;
      consider(spot.dxCall, !!spot.dxStation?.grid);
      consider(spot.spotter, !!spot.spotterStation?.grid);
    }
    if (needed.length === 0) return;

    (async () => {
      for (const call of needed) {
        if (cancelled) break;
        gridInFlightRef.current.add(call);
        try {
          const info = await api.lookupCallsignQrz(call);
          let coords: { lat: number; lon: number } | null = null;
          if (info?.latitude != null && info?.longitude != null) {
            coords = { lat: info.latitude, lon: info.longitude };
          } else if (info?.grid) {
            const c = gridToLatLon(info.grid);
            if (c) coords = { lat: c.lat, lon: c.lon };
          }
          gridCacheRef.current.set(call, coords);
        } catch {
          gridCacheRef.current.set(call, null); // negative cache
        } finally {
          gridInFlightRef.current.delete(call);
        }
        if (!cancelled) setGridTick((t) => t + 1);
      }
    })();

    return () => { cancelled = true; };
  }, [spots, dxClusterMapEnabled, rigFreqHz, rigMode]);

  // Position the HTML callsign labels over the globe each frame so they follow
  // the rotation, hiding any that are on the far (back) side of the globe.
  useEffect(() => {
    if (!globeReady) return;
    let raf = 0;
    const tick = () => {
      const globe = globeRef.current;
      const labels = globeLabelsRef.current;
      const els = labelElsRef.current;
      if (globe) {
        const cam = globe.camera().position;
        const camLen = Math.hypot(cam.x, cam.y, cam.z) || 1;

        // Anisotropic filtering sweep. The globe base texture and the Google
        // map tiles both load with three.js's default anisotropy of 1, which
        // leaves the sphere looking grainy/smeared toward its curved edges.
        // Bump every texture to the GPU's max anisotropy for a crisp surface.
        // Map tiles stream in continuously as the operator zooms/rotates, so we
        // re-sweep on a throttle and skip textures already at max.
        const nowMs = performance.now();
        if (nowMs - lastAnisoSweepRef.current > 750) {
          lastAnisoSweepRef.current = nowMs;
          try {
            const maxAniso = globe.renderer().capabilities.getMaxAnisotropy();
            if (maxAniso > 1) {
              globe.scene().traverse((obj) => {
                const mat = (obj as { material?: unknown }).material;
                const mats = Array.isArray(mat) ? mat : mat ? [mat] : [];
                for (const m of mats) {
                  const tex = (m as { map?: { anisotropy: number; needsUpdate: boolean } }).map;
                  if (tex && tex.anisotropy !== maxAniso) {
                    tex.anisotropy = maxAniso;
                    tex.needsUpdate = true;
                  }
                }
              });
            }
          } catch { /* best-effort — never break the render loop */ }
        }

        for (let i = 0; i < labels.length; i++) {
          const el = els[i];
          if (!el) continue;
          const lb = labels[i];
          const sc = globe.getScreenCoords(lb.lat, lb.lng, 0.02);
          const p = globe.getCoords(lb.lat, lb.lng, 0.02);
          if (!sc || !p) { el.style.display = 'none'; continue; }
          const pLen = Math.hypot(p.x, p.y, p.z) || 1;
          const cosAngle = (p.x * cam.x + p.y * cam.y + p.z * cam.z) / (pLen * camLen);
          // Visible when within the globe's horizon cap (cos > R/d).
          if (cosAngle > pLen / camLen) {
            el.style.display = '';
            el.style.transform = `translate(${sc.x}px, ${sc.y}px) translate(-50%, -130%)`;
          } else {
            el.style.display = 'none';
          }
        }

        // ── Focused-callsign DX tower icon ──────────────────────────────
        // Positioned every frame by the same projection math the labels
        // use. Hidden when there's no focused callsign or when the target
        // point rotates past the globe's horizon.
        const iconEl = targetIconRef.current;
        const info = focusedInfoLatestRef.current;
        if (iconEl) {
          if (info?.latitude != null && info?.longitude != null) {
            const sc = globe.getScreenCoords(info.latitude, info.longitude, 0.02);
            const p = globe.getCoords(info.latitude, info.longitude, 0.02);
            if (sc && p) {
              const pLen = Math.hypot(p.x, p.y, p.z) || 1;
              const cosAngle = (p.x * cam.x + p.y * cam.y + p.z * cam.z) / (pLen * camLen);
              if (cosAngle > pLen / camLen) {
                iconEl.style.display = '';
                iconEl.style.transform = `translate(${sc.x}px, ${sc.y}px) translate(-50%, -100%)`;
              } else {
                iconEl.style.display = 'none';
              }
            } else {
              iconEl.style.display = 'none';
            }
          } else {
            iconEl.style.display = 'none';
          }
        }
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [globeReady]);

  // Keep the auto-rotation in sync with the pause/play toggle.
  useEffect(() => {
    if (!globeReady || !globeRef.current) return;
    globeRef.current.controls().autoRotate = isRotating;
  }, [isRotating, globeReady]);

  // Lightning strikes → globe.gl ringsData. Backfill via REST, then live via SignalR.
  useEffect(() => {
    if (!globeReady || !globeRef.current) return;
    if (!settings.map.showLightning) {
      globeRef.current.ringsData([]);
      globeRef.current.customLayerData([]);
      return;
    }
    const store = strikeStoreRef.current;
    let cancelled = false;

    // Stable ring datum per strike — globe.gl diffs ringsData by object
    // identity, so fresh literals each sweep would rebuild every ring mesh.
    // Arrival time (for the 60 s strike-bolt window) lives in the persistent
    // StrikeStore keyed by strike-key — NOT here — so it survives dedupe and
    // this effect being torn down/recreated on toggle. Bolts key on arrival,
    // not strike time, because the feed publishes ~1–2 min behind real time.
    type RingDatum = { lat: number; lng: number; local: boolean; ts: number };
    const ringCache = new WeakMap<Strike, RingDatum>();
    // Render caps — the store retains thousands of strikes across the global
    // 10-min window, but every rendered ring is a continuously-rippling mesh
    // and every bolt an unbatched sprite draw call. Thousands of them stall the
    // GPU (freeze/crash on weaker hardware), so bound what actually draws: keep
    // all local strikes, then the newest, up to these limits.
    const MAX_RINGS = 400;
    const MAX_BOLTS = 120;
    const render = () => {
      if (cancelled || !globeRef.current) return;
      const now = Date.now();
      const prioritized = store.active(now)
        .map(s => {
          let r = ringCache.get(s);
          if (!r) { r = { lat: s.lat, lng: s.lon, local: s.local, ts: Date.parse(s.timestampUtc) }; ringCache.set(s, r); }
          const arrived = store.arrivedAt(s);
          return { r, live: arrived !== undefined && now - arrived <= 60_000 };
        })
        .sort((a, b) => (Number(b.r.local) - Number(a.r.local)) || (b.r.ts - a.r.ts)); // local first, then newest
      const rings = prioritized.slice(0, MAX_RINGS).map(x => x.r);
      const dots = prioritized.filter(x => x.live).slice(0, MAX_BOLTS).map(x => x.r);
      globeRef.current.ringsData(rings);
      globeRef.current.customLayerData(dots);
    };

    // Initial backfill (no arrival time → history draws rings only, no bolt).
    api.getLightningStrikes().then(list => {
      if (cancelled) return;
      store.merge(list as Strike[]);
      render();
    }).catch(() => { /* best-effort */ });

    // Live updates — record arrival so these strikes get a 60 s bolt.
    const cb = (evt: { strikes: Strike[] }) => {
      store.merge(evt.strikes, Date.now());
      render();
    };
    setLightningStrikesCallback(cb);

    // Age-out sweep so rings fade even without new strikes arriving.
    const interval = setInterval(render, 2000);

    return () => {
      cancelled = true;
      clearInterval(interval);
      clearLightningStrikesCallback(cb);
      if (globeRef.current) {
        globeRef.current.ringsData([]);
        globeRef.current.customLayerData([]);
      }
    };
  }, [globeReady, settings.map.showLightning]);

  // Day/night terminator → shader shell uniforms. The shell is created at
  // globe init; this effect drives it from settings and refreshes the sun
  // direction (terminator moves 0.25°/min — 60 s is plenty). The shell's
  // second opacity (grey-line band) is unused: the band didn't look good on
  // the satellite tiles, so it stays at 0.
  useEffect(() => {
    if (!globeReady || !globeRef.current) return;
    const shell = dayNightShellRef.current;
    if (!shell) return;

    const night = settings.map.showDayNightOverlay ? settings.map.dayNightOpacity : 0;
    shell.setOpacities(night, 0);
    if (night <= 0) return;

    const updateSun = () => {
      if (!globeRef.current) return;
      const sun = getSunPosition(new Date());
      const p = globeRef.current.getCoords(sun.lat, sun.lon, 0);
      shell.setSunDirection(p.x, p.y, p.z);
    };
    updateSun();
    const interval = setInterval(updateSun, 60_000);
    return () => clearInterval(interval);
  }, [globeReady, settings.map.showDayNightOverlay, settings.map.dayNightOpacity]);

  // Ionosphere glow — show the D/E/F layer shells while the ionospheric-hops
  // layer is on (warm→cool, brighter at the outer edge); hidden otherwise.
  useEffect(() => {
    if (!globeReady) return;
    ionoShellsRef.current?.setVisible(!!settings.map.showIonosphereHops);
  }, [globeReady, settings.map.showIonosphereHops]);

  // Fly to target when focused callsign changes
  useEffect(() => {
    if (!globeRef.current) return;

    const targetLat = focusedCallsignInfo?.latitude;
    const targetLon = focusedCallsignInfo?.longitude;
    if (targetLat == null || targetLon == null) return;

    if (cameraAnimationRef.current) {
      cancelAnimationFrame(cameraAnimationRef.current);
      cameraAnimationRef.current = null;
    }

    let duration = 2;
    const lastCoords = lastTargetCoordsRef.current;
    if (lastCoords) {
      const distance = calculateDistance(lastCoords.lat, lastCoords.lng, targetLat, targetLon);
      duration = getAnimationDuration(distance);
    } else {
      const distance = calculateDistance(stationLat, stationLon, targetLat, targetLon);
      duration = getAnimationDuration(distance);
    }

    lastTargetCoordsRef.current = { lat: targetLat, lng: targetLon };

    const startPov = globeRef.current.pointOfView();
    let targetPov: { lat: number; lng: number; altitude: number };
    if (showIonoHopsRef.current) {
      // Ionospheric-hops view: aim at a point offset PERPENDICULAR to the
      // short path from its midpoint, so we look at the hop zigzag obliquely
      // (arcs rising off the globe) instead of straight down. Pull back to
      // frame the whole arc — farther DX → higher altitude.
      const spMid = interpolateGreatCircle(stationLat, stationLon, targetLat, targetLon, 0.5);
      const spDistKm = calculateDistance(stationLat, stationLon, targetLat, targetLon);
      const framedAlt = Math.max(1.6, Math.min(3.2, 1.2 + spDistKm / 7000));
      const bearing = calculateBearing(stationLat, stationLon, targetLat, targetLon);
      // Larger perpendicular offset → more oblique (more tilt) so the hops
      // are seen rising off the globe rather than close to straight down.
      const aim = getDestinationPoint(spMid.lat, spMid.lng, (bearing + 90) % 360, 5200);
      targetPov = { lat: aim.lat, lng: aim.lng, altitude: framedAlt };
    } else {
      // Standard view: fly to the DX location.
      targetPov = { lat: targetLat, lng: targetLon, altitude: 1.7 };
    }

    const startTime = performance.now();
    const durationMs = duration * 1000;

    const easeInOutCubic = (t: number): number => {
      return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
    };

    let startLng = startPov.lng;
    let endLng = targetPov.lng;
    const lngDiff = endLng - startLng;
    if (lngDiff > 180) startLng += 360;
    else if (lngDiff < -180) endLng += 360;

    const animateCamera = (currentTime: number) => {
      const elapsed = currentTime - startTime;
      const progress = Math.min(elapsed / durationMs, 1);
      const easedProgress = easeInOutCubic(progress);

      const currentLat = startPov.lat + (targetPov.lat - startPov.lat) * easedProgress;
      let currentLng = startLng + (endLng - startLng) * easedProgress;
      currentLng = ((currentLng + 540) % 360) - 180;
      const currentAlt = startPov.altitude + (targetPov.altitude - startPov.altitude) * easedProgress;

      if (globeRef.current) {
        globeRef.current.pointOfView({ lat: currentLat, lng: currentLng, altitude: currentAlt });
      }

      if (progress < 1) {
        cameraAnimationRef.current = requestAnimationFrame(animateCamera);
      } else {
        cameraAnimationRef.current = null;
      }
    };

    cameraAnimationRef.current = requestAnimationFrame(animateCamera);

    return () => {
      if (cameraAnimationRef.current) {
        cancelAnimationFrame(cameraAnimationRef.current);
        cameraAnimationRef.current = null;
      }
    };
  }, [focusedCallsignInfo?.latitude, focusedCallsignInfo?.longitude, stationLat, stationLon]);

  return (
      <div className="relative w-full h-full">
        {/* WebGL Error Message */}
        {webglError && (
          <div className="absolute inset-0 flex items-center justify-center bg-dark-800/95 backdrop-blur-sm z-50">
            <div className="glass-panel p-6 max-w-md text-center">
              <GlobeIcon className="w-12 h-12 mx-auto mb-4 text-dark-300" />
              <h3 className="text-lg font-semibold mb-2 font-ui text-dark-200">WebGL Not Available</h3>
              <p className="text-sm text-dark-300 mb-4">{webglError}</p>
            </div>
          </div>
        )}

        {/* Globe container */}
        <div
          ref={containerRef}
          className="w-full h-full"
          style={{ cursor: rotatorEnabled ? 'crosshair' : 'default' }}
        />

        {/* Focused-callsign radio-tower icon — sits over the DX QTH with a
            gentle wave pulse to reinforce "beam pointing here." Positioned
            every frame by the same projection tick that drives the labels.
            Hidden CSS-wise; the tick makes it visible + sets its transform. */}
        {/* Rendered on the full globe AND the 2D-map's globe (hideOverlays) —
            the DX tower marker is wanted in both; the per-frame tick positions
            it and keeps it display:none until a callsign is focused. */}
        <div
            ref={targetIconRef}
            className="absolute pointer-events-none"
            style={{
              top: 0,
              left: 0,
              display: 'none',
              // Amber/gold — reads as "beacon broadcasting" and stands
              // clear of the red SP and lime LP arc colors that terminate
              // at the same point. Also ties to the app's ☕ donate button
              // for a small warm-color throughline across the UI.
              color: '#fbbf24',
              filter: 'drop-shadow(0 0 6px rgba(0, 0, 0, 0.85))',
            }}
          >
            <div className="relative flex items-center justify-center">
              {/* Concentric expanding rings behind the tower — the "radio
                  waves broadcasting" effect. Two rings staggered half a
                  cycle apart so waves are always emitting. The rings are
                  drawn brighter than the tower itself (lighter yellow
                  #fde047 vs amber #fbbf24, higher alpha, plus a soft glow
                  via box-shadow) so the eye reads them as light being
                  emitted BY the tower, not shadows around it. */}
              <span
                className="absolute rounded-full"
                style={{
                  width: '38px',
                  height: '38px',
                  border: '2.5px solid rgba(253, 224, 71, 0.95)',
                  boxShadow: '0 0 8px rgba(253, 224, 71, 0.6)',
                  animation: 'sdrGlobeTowerPulse 2.2s ease-out infinite',
                }}
              />
              <span
                className="absolute rounded-full"
                style={{
                  width: '38px',
                  height: '38px',
                  border: '2.5px solid rgba(253, 224, 71, 0.95)',
                  boxShadow: '0 0 8px rgba(253, 224, 71, 0.6)',
                  animation: 'sdrGlobeTowerPulse 2.2s ease-out infinite',
                  animationDelay: '1.1s',
                }}
              />
              <RadioTower className="w-6 h-6 relative" strokeWidth={2.4} />
            </div>
          </div>

        {/* Callsign label overlay — black boxes positioned over the globe each
            frame by the effect above (independent of globe.gl's label layers). */}
        {!hideOverlays && (
          <div className="absolute inset-0 overflow-hidden pointer-events-none">
            {globeLabels.map((lb, i) => (
              <div
                key={`${lb.type}-${i}-${lb.text}`}
                ref={(el) => { labelElsRef.current[i] = el; }}
                onClick={
                  lb.type === 'dx' && lb.dxCall
                    ? () => selectSpotRef.current(lb.dxCall!, lb.frequency ?? 0, lb.mode)
                    : lb.type === 'pota' && lb.activator
                    ? () => selectSpotRef.current(lb.activator!, lb.frequency ?? 0, lb.mode)
                    : undefined
                }
                title={lb.type === 'dx' || lb.type === 'pota' ? 'Click to add to log' : undefined}
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  display: 'none',
                  padding: '1px 5px',
                  borderRadius: '3px',
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: '11px',
                  fontWeight: 700,
                  lineHeight: 1.2,
                  whiteSpace: 'nowrap',
                  background: 'rgba(8, 11, 18, 0.85)',
                  border: `1px solid ${lb.color}`,
                  color: lb.color,
                  userSelect: 'none',
                  pointerEvents: lb.type === 'dx' || lb.type === 'pota' ? 'auto' : 'none',
                  cursor: lb.type === 'dx' || lb.type === 'pota' ? 'pointer' : 'default',
                }}
              >
                {lb.text}
              </div>
            ))}
          </div>
        )}

        {/* Auto-rotation pause/play (Top Right) */}
        {!hideOverlays && (
          <button
            onClick={() => setIsRotating((r) => !r)}
            className="glass-button absolute top-4 right-4 p-2 z-10"
            title={isRotating ? 'Pause rotation' : 'Resume rotation'}
            aria-label={isRotating ? 'Pause globe rotation' : 'Resume globe rotation'}
          >
            {isRotating ? <Pause className="w-4 h-4" /> : <Play className="w-4 h-4" />}
          </button>
        )}

        {/* Lightning strikes overlay toggle (Top Right, left of rotation button) */}
        {!hideOverlays && (
          <button
            onClick={() => updateMapSettings({ showLightning: !settings.map.showLightning })}
            className={`glass-button absolute top-4 right-16 p-2 z-10 ${settings.map.showLightning ? 'text-cyan-300' : ''}`}
            title={settings.map.showLightning ? 'Hide lightning strikes' : 'Show lightning strikes'}
            aria-label={settings.map.showLightning ? 'Hide lightning strikes' : 'Show lightning strikes'}
          >
            <Zap className="w-4 h-4" />
          </button>
        )}

        {/* Day/night terminator toggle (Top Right, left of lightning) */}
        {!hideOverlays && (
          <button
            onClick={() => { updateMapSettings({ showDayNightOverlay: !settings.map.showDayNightOverlay }); saveSettings(); }}
            className={`glass-button absolute top-4 right-28 p-2 z-10 ${settings.map.showDayNightOverlay ? 'text-amber-300' : ''}`}
            title={settings.map.showDayNightOverlay ? 'Hide day/night shading' : 'Show day/night shading'}
            aria-label={settings.map.showDayNightOverlay ? 'Hide day/night shading' : 'Show day/night shading'}
          >
            <SunMoon className="w-4 h-4" />
          </button>
        )}

        {/* Beam heading — under the play button (Top Right). */}
        {!hideOverlays && rotatorEnabled && (
          <div className="absolute top-16 right-4 text-right pointer-events-none">
            <div className="text-[1.5rem] font-display font-bold text-accent-primary drop-shadow-glow leading-none">
              {currentAzimuth}°
            </div>
            <div className="text-[10px] font-ui font-bold uppercase tracking-[0.2em] text-accent-primary/60 mt-1">
              Beam Heading
            </div>
          </div>
        )}

        {/* Station and Rig Info Overlay (Top Left) */}
        {!hideOverlays && (
          <div className="absolute top-4 left-4 flex flex-col gap-2 pointer-events-none">
            {/* Focused DX ("their callsign") card — stays visible at any panel size. */}
            {focusedCallsignInfo && (
              <div className="glass-panel px-3 py-2 border-l-4 border-accent-danger animate-fade-in pointer-events-auto">
                <div className="flex items-center gap-2">
                  <Target className="w-4 h-4 text-accent-secondary" />
                  <div>
                    <p className="font-mono font-bold text-accent-danger flex items-center gap-1.5">
                      {focusedCallsignInfo.callsign}
                      {focusedCallsignInfo.latLonIsApproximate && (
                        <span
                          className="px-1 py-[1px] rounded text-[8px] font-normal tracking-wider bg-dark-600 text-dark-300 border border-glass-100"
                          title="Approximate location — cty.dat country centroid (QRZ/HamQTH lookup unavailable)"
                        >
                          APPROX
                        </span>
                      )}
                    </p>
                    {focusedCallsignInfo.grid && (
                      <p className="text-[10px] font-mono text-dark-300">{focusedCallsignInfo.grid}</p>
                    )}
                    {focusedCallsignInfo.bearing != null && (
                      <p className="text-[10px] font-mono text-accent-info">
                        <span className="text-accent-danger" title="Short path">SP</span>{' '}
                        {focusedCallsignInfo.bearing.toFixed(0)}°
                        {focusedCallsignInfo.distance != null && ` / ${Math.round(focusedCallsignInfo.distance)}km`}
                      </p>
                    )}
                    {focusedCallsignInfo.bearing != null && settings.map.showLongPath !== false && (
                      // Long-path readout — reciprocal bearing (SP + 180°) and
                      // the LP distance = 40030 - SP distance (great-circle
                      // circumference at Earth's mean radius). Cyan matches
                      // the LP line drawn on the globe. Hidden when the
                      // Show Long Path toggle is off (Settings → Map).
                      <p className="text-[10px] font-mono" style={{ color: 'rgba(163, 230, 53, 0.95)' }}>
                        <span title="Long path">LP</span>{' '}
                        {((focusedCallsignInfo.bearing + 180) % 360).toFixed(0)}°
                        {focusedCallsignInfo.distance != null && ` / ${Math.round(40030 - focusedCallsignInfo.distance)}km`}
                      </p>
                    )}
                  </div>
                  {rotatorEnabled && focusedCallsignInfo.bearing != null && (
                    <button
                      onClick={() => {
                        const bearing = focusedCallsignInfo.bearing!;
                        lastCommandTimeRef.current = Date.now();
                        commandedAzimuthRef.current = bearing;
                        displayedAzimuthRef.current = bearing;
                        setCurrentAzimuth(bearing);
                        commandRotator(bearing, 'globe');
                      }}
                      className="rounded-lg px-2 py-1 flex items-center gap-1 text-[10px] ml-2 font-bold transition-all duration-200 hover:brightness-125 active:scale-95"
                      style={{
                        // High-contrast HUD pill: bright green on a near-opaque
                        // dark backdrop so the bearing stays legible over the
                        // globe. The old glass-button-success was green text on
                        // a translucent green fill, which blended into the map.
                        background: 'rgba(8, 11, 18, 0.9)',
                        border: '1px solid rgba(74, 222, 128, 0.85)',
                        color: '#6ee7a0',
                        boxShadow: '0 0 8px rgba(74, 222, 128, 0.35)',
                      }}
                      title={`Rotate to ${focusedCallsignInfo.callsign}`}
                    >
                      <Navigation className="w-2.5 h-2.5" />
                      <span className="font-mono">{focusedCallsignInfo.bearing.toFixed(0)}°</span>
                    </button>
                  )}
                </div>
              </div>
            )}

          </div>
        )}

        {/* Instructions (Bottom Left) - Hide if height too small */}
        {!hideOverlays && containerHeight > 400 && (
          <div className="absolute bottom-4 left-4 glass-panel px-3 py-2 text-[10px] pointer-events-none opacity-60">
            <div className="flex items-center gap-2 font-ui text-dark-300">
              <Navigation className="w-3 h-3" />
              <span>{rotatorEnabled ? 'Click globe to set beam' : 'Rotator disabled'}</span>
            </div>
          </div>
        )}

      </div>
  );
}

export function GlobePlugin() {
  const { settings } = useSettingsStore();
  const [isFullscreen, setIsFullscreen] = useState(false);
  const { rotatorPosition } = useAppStore();
  const currentAzimuth = rotatorPosition?.currentAzimuth ?? 0;

  const toggleFullscreen = useCallback(() => {
    const el = document.getElementById('globe-plugin-container');
    if (!el) return;
    if (!isFullscreen) el.requestFullscreen?.();
    else document.exitFullscreen?.();
    setIsFullscreen(!isFullscreen);
  }, [isFullscreen]);

  return (
    <GlassPanel
      title="3D Globe"
      icon={<GlobeIcon className="w-5 h-5" />}
      actions={
        <div className="flex items-center gap-4">
          {settings.rotator.enabled && <RotatorControls />}
          <div className="flex items-center gap-2">
            {settings.rotator.enabled ? (
              <span className="text-sm font-mono font-bold text-accent-primary min-w-[3ch]">{currentAzimuth}°</span>
            ) : (
              <span className="text-xs font-ui text-dark-400">No Rotator</span>
            )}
            <button
              onClick={toggleFullscreen}
              className="glass-button p-1.5"
              title="Fullscreen"
            >
              <Maximize2 className="w-4 h-4" />
            </button>
          </div>
        </div>
      }
    >
      <div id="globe-plugin-container" className="w-full h-full">
         <GlobeCore />
      </div>
    </GlassPanel>
  );
}

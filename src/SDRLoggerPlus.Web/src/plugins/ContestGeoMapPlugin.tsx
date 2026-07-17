import { useEffect, useMemo, useRef, useState } from 'react';
import { MapContainer, GeoJSON } from 'react-leaflet';
import L from 'leaflet';
import type { Feature, FeatureCollection, Geometry } from 'geojson';
import { MapPinned, Tag, ZoomIn, ZoomOut, Locate, Trophy, Grid3x3, ChevronLeft } from 'lucide-react';
import { useAppStore } from '../store/appStore';
import { api } from '../api/client';
import {
  COUNTRY_COLORS,
  COUNTRY_NAMES,
  getPrefix,
  getShortName,
  type CountryCode,
} from '../geo/contestGeo/entityPrefixes';
import { loadCountyData, loadCountyGeo, stateAbbrForContest, type County } from '../geo/contestGeo/counties';

import 'leaflet/dist/leaflet.css';

interface StateProps { name: string; country: CountryCode; }
interface CountyProps { fips: string; name: string; code: string; }

// North-America-fitting view.
const NA_CENTER: [number, number] = [44, -96];
const NA_ZOOM = 3;

const WORKED_COLOR = '#10b981'; // emerald
const CURRENT_COLOR = '#ffb432'; // amber
const COUNTY_COLOR = '#00ddff'; // cyan (all counties of one state)

type StyleState = 'base' | 'worked' | 'current';

function styleFor(state: StyleState, baseColor: string): L.PathOptions {
  switch (state) {
    case 'current':
      return { color: CURRENT_COLOR, weight: 3, opacity: 1, fillColor: CURRENT_COLOR, fillOpacity: 0.55 };
    case 'worked':
      return { color: WORKED_COLOR, weight: 1.5, opacity: 0.95, fillColor: WORKED_COLOR, fillOpacity: 0.45 };
    default:
      return { color: baseColor, weight: 1, opacity: 0.75, fillColor: baseColor, fillOpacity: 0.1 };
  }
}

interface Selected {
  key: string;   // prefix (state) or county code
  name: string;
  color: string;
  country?: CountryCode; // set in states mode; drives the "drill" affordance
}

interface VectorConfig<P> {
  keyOf: (p: P) => string;
  labelOf: (p: P) => string;
  colorOf: (p: P) => string;
  nameOf: (p: P) => string;
  countryOf?: (p: P) => CountryCode | undefined;
}

function VectorLayers<P>({
  data,
  dataId,
  showLabels,
  config,
  workedRef,
  currentRef,
  registry,
  onSelect,
  onPick,
}: {
  data: FeatureCollection<Geometry, P>;
  dataId: string;
  showLabels: boolean;
  config: VectorConfig<P>;
  workedRef: React.MutableRefObject<Set<string>>;
  currentRef: React.MutableRefObject<string | null>;
  registry: React.MutableRefObject<Map<string, { layer: L.Path; color: string }>>;
  onSelect: (s: Selected | null) => void;
  onPick: (key: string) => void;
}) {
  // Rebuild (and re-style) when the dataset, labels, or worked set change. The
  // `current` highlight is applied imperatively (parent) so typing a location
  // doesn't rebuild every polygon.
  const workedSig = [...workedRef.current].sort().join(',');
  const layerKey = `${dataId}|${showLabels ? 'L' : 'l'}|${workedSig}`;

  const stateOf = (key: string): StyleState =>
    key && key === currentRef.current ? 'current'
      : key && workedRef.current.has(key) ? 'worked'
        : 'base';

  const onEachFeature = (feature: Feature<Geometry, P>, layer: L.Layer) => {
    const p = feature.properties;
    const key = config.keyOf(p);
    const color = config.colorOf(p);
    const path = layer as L.Path;
    if (key) registry.current.set(key, { layer: path, color });

    const label = config.labelOf(p);
    if (showLabels && label) {
      layer.bindTooltip(label, { permanent: true, direction: 'center', className: 'contest-geo-label', opacity: 1 });
    }

    layer.on({
      mouseover: () => {
        path.setStyle({ weight: 2.5, opacity: 1, fillOpacity: 0.35 });
        (path as unknown as { bringToFront?: () => void }).bringToFront?.();
        onSelect({ key, name: config.nameOf(p), color, country: config.countryOf?.(p) });
      },
      mouseout: () => {
        path.setStyle(styleFor(stateOf(key), color));
      },
      click: () => {
        onSelect({ key, name: config.nameOf(p), color, country: config.countryOf?.(p) });
        if (key) onPick(key);
      },
    });
  };

  return (
    <GeoJSON
      key={layerKey}
      data={data}
      style={(f) => {
        const p = (f as Feature<Geometry, P>).properties;
        return styleFor(stateOf(config.keyOf(p)), config.colorOf(p));
      }}
      onEachFeature={onEachFeature as (f: Feature, l: L.Layer) => void}
    />
  );
}

export function ContestGeoMapPlugin() {
  const [naData, setNaData] = useState<FeatureCollection<Geometry, StateProps> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showLabels, setShowLabels] = useState(true);
  const [selected, setSelected] = useState<Selected | null>(null);
  const mapRef = useRef<L.Map | null>(null);

  // County drill-down.
  const [drillAbbr, setDrillAbbr] = useState<string | null>(null);
  const [countyGeo, setCountyGeo] = useState<FeatureCollection<Geometry, CountyProps> | null>(null);
  const [countyList, setCountyList] = useState<County[] | null>(null);
  const autoDrilledRef = useRef<string | null>(null); // sessionId we auto-drilled for

  const contestState = useAppStore((s) => s.contestState);
  const setContestState = useAppStore((s) => s.setContestState);
  const contestCurrentLocation = useAppStore((s) => s.contestCurrentLocation);
  const setContestFillLocation = useAppStore((s) => s.setContestFillLocation);

  useEffect(() => {
    if (!contestState) api.getContestState().then((s) => setContestState(s)).catch(() => {});
  }, [contestState, setContestState]);

  // Worked location codes from the session's State multipliers ("VALUE" or
  // "VALUE@BAND"): 2-letter S/P in the states view, county codes in a drill.
  const workedPrefixes = useMemo(() => {
    const set = new Set<string>();
    const entries = contestState?.multsBySource?.State;
    if (entries) {
      for (const e of entries) {
        const at = e.indexOf('@');
        const v = (at < 0 ? e : e.slice(0, at)).toUpperCase();
        if (v) set.add(v);
      }
    }
    return set;
  }, [contestState]);

  const currentPrefix = contestCurrentLocation ? contestCurrentLocation.toUpperCase() : null;

  const workedRef = useRef(workedPrefixes);
  const currentRef = useRef(currentPrefix);
  const registry = useRef<Map<string, { layer: L.Path; color: string }>>(new Map());
  workedRef.current = workedPrefixes;

  // Move the "current" highlight without rebuilding the layer.
  useEffect(() => {
    const prev = currentRef.current;
    currentRef.current = currentPrefix;
    const restyle = (key: string | null) => {
      if (!key) return;
      const hit = registry.current.get(key);
      if (!hit) return;
      const s: StyleState =
        key === currentRef.current ? 'current'
          : workedRef.current.has(key) ? 'worked' : 'base';
      hit.layer.setStyle(styleFor(s, hit.color));
    };
    if (prev && prev !== currentPrefix) restyle(prev);
    restyle(currentPrefix);
  }, [currentPrefix]);

  // Lazy-load the NA states boundary data.
  useEffect(() => {
    let cancelled = false;
    import('../geo/contestGeo/naAdmin1.geo.json')
      .then((mod) => {
        if (!cancelled) setNaData((mod.default ?? mod) as unknown as FeatureCollection<Geometry, StateProps>);
      })
      .catch((e) => { if (!cancelled) setError(String(e)); });
    return () => { cancelled = true; };
  }, []);

  // Auto-drill into the county map of the active state QSO party (once/session).
  useEffect(() => {
    const sid = contestState?.sessionId;
    if (!sid || autoDrilledRef.current === sid) return;
    loadCountyData().then((data) => {
      const abbr = stateAbbrForContest(contestState?.definitionName, (a) => a in data);
      if (abbr) { autoDrilledRef.current = sid; setDrillAbbr(abbr); }
    }).catch(() => {});
  }, [contestState?.sessionId, contestState?.definitionName]);

  // Load county geometry + names when drilled.
  useEffect(() => {
    if (!drillAbbr) { setCountyGeo(null); setCountyList(null); return; }
    let cancelled = false;
    registry.current.clear();
    Promise.all([loadCountyGeo(drillAbbr), loadCountyData()])
      .then(([geo, data]) => {
        if (cancelled) return;
        setCountyGeo(geo as FeatureCollection<Geometry, CountyProps>);
        setCountyList(data[drillAbbr] ?? []);
        setSelected(null);
      })
      .catch(() => { if (!cancelled) setDrillAbbr(null); }); // no county file → stay on states
    return () => { cancelled = true; };
  }, [drillAbbr]);

  // Fit the map to the drilled state, or back to North America.
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    if (countyGeo) {
      const b = L.geoJSON(countyGeo).getBounds();
      if (b.isValid()) map.fitBounds(b, { padding: [24, 24] });
    } else {
      map.setView(NA_CENTER, NA_ZOOM);
    }
  }, [countyGeo]);

  const counts = useMemo(() => {
    const c: Record<CountryCode, number> = { US: 0, CA: 0, MX: 0 };
    naData?.features.forEach((f) => { const cc = f.properties.country; if (cc in c) c[cc] += 1; });
    return c;
  }, [naData]);

  const inCounties = !!countyGeo;

  // Worked count that maps to a visible polygon (states or counties).
  const workedOnMap = useMemo(() => {
    let n = 0;
    if (inCounties && countyList) {
      const codes = new Set(countyList.map((c) => c.code));
      workedPrefixes.forEach((p) => { if (codes.has(p)) n += 1; });
    } else if (naData) {
      workedPrefixes.forEach((p) => {
        if (naData.features.some((f) => getPrefix(f.properties.name) === p)) n += 1;
      });
    }
    return n;
  }, [inCounties, countyList, naData, workedPrefixes]);

  const sessionActive = !!contestState;
  const drillName = drillAbbr ?? '';

  const stateConfig: VectorConfig<StateProps> = {
    keyOf: (p) => getPrefix(p.name),
    labelOf: (p) => getPrefix(p.name),
    colorOf: (p) => COUNTRY_COLORS[p.country] ?? '#8aa',
    nameOf: (p) => getShortName(p.name),
    countryOf: (p) => p.country,
  };
  const countyConfig: VectorConfig<CountyProps> = {
    keyOf: (p) => p.code,
    labelOf: (p) => p.code,
    colorOf: () => COUNTY_COLOR,
    nameOf: (p) => p.name,
  };

  return (
    <div className="relative w-full h-full" style={{ background: '#0a0e14' }}>
      <style>{`
        .contest-geo-label {
          background: transparent !important; border: none !important; box-shadow: none !important;
          color: #e6eef7; font-family: 'JetBrains Mono', monospace; font-size: 10px; font-weight: 700;
          text-shadow: 0 0 3px rgba(0,0,0,0.95), 0 1px 2px rgba(0,0,0,0.9); padding: 0; white-space: nowrap;
        }
        .contest-geo-label::before { display: none !important; }
      `}</style>

      {!naData && !error && (
        <div className="absolute inset-0 flex items-center justify-center text-dark-300 text-sm font-ui z-[500]">Loading map…</div>
      )}
      {error && (
        <div className="absolute inset-0 flex items-center justify-center text-red-400 text-sm font-ui z-[500] px-4 text-center">Failed to load map data: {error}</div>
      )}

      <MapContainer
        center={NA_CENTER}
        zoom={NA_ZOOM}
        minZoom={2}
        maxZoom={9}
        className="w-full h-full"
        style={{ background: '#0a0e14' }}
        zoomControl={false}
        attributionControl={false}
        ref={(m) => { mapRef.current = m ?? null; }}
        whenReady={() => requestAnimationFrame(() => mapRef.current?.invalidateSize())}
      >
        {inCounties && countyGeo ? (
          <VectorLayers
            data={countyGeo}
            dataId={`cty-${drillName}`}
            showLabels={showLabels}
            config={countyConfig}
            workedRef={workedRef}
            currentRef={currentRef}
            registry={registry}
            onSelect={setSelected}
            onPick={setContestFillLocation}
          />
        ) : naData ? (
          <VectorLayers
            data={naData}
            dataId="na"
            showLabels={showLabels}
            config={stateConfig}
            workedRef={workedRef}
            currentRef={currentRef}
            registry={registry}
            onSelect={setSelected}
            onPick={setContestFillLocation}
          />
        ) : null}
      </MapContainer>

      {/* Title + legend */}
      <div className="absolute top-2 left-2 z-[1000] glass-panel px-3 py-2 pointer-events-none">
        <div className="flex items-center gap-2 text-sm font-ui font-semibold text-dark-100">
          <MapPinned className="w-4 h-4 text-accent-primary" />
          {inCounties ? `${drillName} Counties` : 'QSO-Party Map'}
        </div>
        {inCounties ? (
          <div className="mt-1.5 flex items-center gap-2 text-[11px] font-ui">
            <span className="text-dark-300">{countyList?.length ?? 0} counties</span>
            {sessionActive && (
              <><Trophy className="w-3 h-3 text-emerald-400" /><span className="text-emerald-300">{workedOnMap} worked</span></>
            )}
          </div>
        ) : sessionActive ? (
          <div className="mt-1.5 flex items-center gap-2 text-[11px] font-ui">
            <Trophy className="w-3 h-3 text-emerald-400" />
            <span className="text-emerald-300">{workedOnMap} worked</span>
            <span className="text-dark-500 truncate max-w-[10rem]">{contestState!.definitionName}</span>
          </div>
        ) : (
          <div className="mt-1.5 flex flex-col gap-0.5">
            {(Object.keys(COUNTRY_COLORS) as CountryCode[]).map((cc) => (
              <div key={cc} className="flex items-center gap-2 text-[11px] font-ui text-dark-300">
                <span className="inline-block w-3 h-3 rounded-sm" style={{ background: COUNTRY_COLORS[cc], opacity: 0.6, border: `1px solid ${COUNTRY_COLORS[cc]}` }} />
                <span>{COUNTRY_NAMES[cc]}</span>
                <span className="text-dark-400">({counts[cc]})</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Controls */}
      <div className="absolute top-2 right-2 z-[1000] flex flex-col gap-1">
        {inCounties && (
          <button
            onClick={(e) => { e.stopPropagation(); setDrillAbbr(null); }}
            className="glass-button p-2 text-accent-primary"
            title="Back to states"
          >
            <ChevronLeft className="w-4 h-4" />
          </button>
        )}
        <button onClick={(e) => { e.stopPropagation(); mapRef.current?.zoomIn(); }} className="glass-button p-2" title="Zoom In"><ZoomIn className="w-4 h-4" /></button>
        <button onClick={(e) => { e.stopPropagation(); mapRef.current?.zoomOut(); }} className="glass-button p-2" title="Zoom Out"><ZoomOut className="w-4 h-4" /></button>
        <button
          onClick={(e) => { e.stopPropagation(); if (inCounties && countyGeo) { const b = L.geoJSON(countyGeo).getBounds(); if (b.isValid()) mapRef.current?.fitBounds(b, { padding: [24, 24] }); } else { mapRef.current?.setView(NA_CENTER, NA_ZOOM); } }}
          className="glass-button p-2"
          title={inCounties ? 'Fit state' : 'Fit North America'}
        >
          <Locate className="w-4 h-4" />
        </button>
        <button onClick={(e) => { e.stopPropagation(); setShowLabels((v) => !v); }} className={`glass-button p-2 ${showLabels ? 'text-accent-primary' : ''}`} title={showLabels ? 'Hide labels' : 'Show labels'}><Tag className="w-4 h-4" /></button>
      </div>

      {/* Selected / hovered readout */}
      {selected && (
        <div className="absolute bottom-2 left-2 z-[1000] glass-panel px-3 py-2">
          <div className="flex items-baseline gap-2">
            <span className="font-mono font-bold text-base" style={{ color: selected.color }}>{selected.key || '—'}</span>
            <span className="text-sm font-ui text-dark-100">{selected.name}</span>
            {!inCounties && selected.country && <span className="text-[11px] font-ui text-dark-400">{COUNTRY_NAMES[selected.country]}</span>}
            {selected.key && workedPrefixes.has(selected.key) && <span className="text-[11px] font-ui text-emerald-400">worked</span>}
          </div>
          {!inCounties && selected.country === 'US' && (
            <button
              onClick={() => setDrillAbbr(selected.key)}
              className="mt-1.5 flex items-center gap-1 text-[11px] font-ui text-accent-primary hover:underline"
            >
              <Grid3x3 className="w-3 h-3" /> View {selected.name} counties
            </button>
          )}
        </div>
      )}
    </div>
  );
}

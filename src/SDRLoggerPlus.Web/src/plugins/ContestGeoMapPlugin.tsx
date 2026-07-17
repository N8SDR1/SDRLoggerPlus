import { useEffect, useMemo, useRef, useState } from 'react';
import { MapContainer, GeoJSON } from 'react-leaflet';
import L from 'leaflet';
import type { Feature, FeatureCollection, Geometry } from 'geojson';
import { MapPinned, Tag, ZoomIn, ZoomOut, Locate, Trophy } from 'lucide-react';
import { useAppStore } from '../store/appStore';
import { api } from '../api/client';
import {
  COUNTRY_COLORS,
  COUNTRY_NAMES,
  getPrefix,
  getShortName,
  type CountryCode,
} from '../geo/contestGeo/entityPrefixes';

import 'leaflet/dist/leaflet.css';

interface EntityProps {
  name: string;
  country: CountryCode;
}

type EntityFeature = Feature<Geometry, EntityProps>;

// North-America-fitting view.
const NA_CENTER: [number, number] = [44, -96];
const NA_ZOOM = 3;

const WORKED_COLOR = '#10b981'; // emerald
const CURRENT_COLOR = '#ffb432'; // amber

type StyleState = 'base' | 'worked' | 'current';

function styleFor(state: StyleState, country: CountryCode): L.PathOptions {
  const color = COUNTRY_COLORS[country] ?? '#8aa';
  switch (state) {
    case 'current':
      return { color: CURRENT_COLOR, weight: 3, opacity: 1, fillColor: CURRENT_COLOR, fillOpacity: 0.55 };
    case 'worked':
      return { color: WORKED_COLOR, weight: 1.5, opacity: 0.95, fillColor: WORKED_COLOR, fillOpacity: 0.45 };
    default:
      return { color, weight: 1, opacity: 0.75, fillColor: color, fillOpacity: 0.1 };
  }
}

interface Selected {
  name: string;
  prefix: string;
  country: CountryCode;
}

function GeoLayers({
  data,
  showLabels,
  workedRef,
  currentRef,
  registry,
  onSelect,
  onPick,
}: {
  data: FeatureCollection<Geometry, EntityProps>;
  showLabels: boolean;
  workedRef: React.MutableRefObject<Set<string>>;
  currentRef: React.MutableRefObject<string | null>;
  registry: React.MutableRefObject<Map<string, { layer: L.Path; country: CountryCode }>>;
  onSelect: (s: Selected | null) => void;
  onPick: (prefix: string) => void;
}) {
  // Re-key so the layer rebuilds (and re-styles) when labels or the worked set
  // change. The `current` highlight is applied imperatively (see parent) so
  // typing a location doesn't rebuild all 97 polygons.
  const workedSig = [...workedRef.current].sort().join(',');
  const layerKey = `${showLabels ? 'L' : 'l'}|${workedSig}`;

  const styleState = (prefix: string): StyleState =>
    prefix && prefix === currentRef.current ? 'current'
      : prefix && workedRef.current.has(prefix) ? 'worked'
        : 'base';

  const onEachFeature = (feature: EntityFeature, layer: L.Layer) => {
    const { name, country } = feature.properties;
    const prefix = getPrefix(name);
    const path = layer as L.Path;
    if (prefix) registry.current.set(prefix, { layer: path, country });

    if (showLabels && prefix) {
      layer.bindTooltip(prefix, {
        permanent: true,
        direction: 'center',
        className: 'contest-geo-label',
        opacity: 1,
      });
    }

    layer.on({
      mouseover: () => {
        path.setStyle({ weight: 2.5, opacity: 1, fillOpacity: 0.35 });
        (path as unknown as { bringToFront?: () => void }).bringToFront?.();
        onSelect({ name: getShortName(name), prefix, country });
      },
      mouseout: () => {
        path.setStyle(styleFor(styleState(prefix), country));
      },
      click: () => {
        onSelect({ name: getShortName(name), prefix, country });
        if (prefix) onPick(prefix);
      },
    });
  };

  return (
    <GeoJSON
      key={layerKey}
      data={data}
      style={(f) => {
        const p = (f as EntityFeature).properties;
        return styleFor(styleState(getPrefix(p.name)), p.country);
      }}
      onEachFeature={onEachFeature as (f: Feature, l: L.Layer) => void}
    />
  );
}

export function ContestGeoMapPlugin() {
  const [data, setData] = useState<FeatureCollection<Geometry, EntityProps> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showLabels, setShowLabels] = useState(true);
  const [selected, setSelected] = useState<Selected | null>(null);
  const mapRef = useRef<L.Map | null>(null);

  const contestState = useAppStore((s) => s.contestState);
  const setContestState = useAppStore((s) => s.setContestState);
  const contestCurrentLocation = useAppStore((s) => s.contestCurrentLocation);
  const setContestFillLocation = useAppStore((s) => s.setContestFillLocation);

  // Seed contest state on mount so a live session lights up after a reload.
  useEffect(() => {
    if (!contestState) api.getContestState().then((s) => setContestState(s)).catch(() => {});
  }, [contestState, setContestState]);

  // Worked state/province prefixes from the active session's State multipliers.
  // Entries are "VALUE" or "VALUE@BAND"; county codes simply won't match a
  // state polygon (that's the per-state county map, a later piece).
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

  // Refs the layer styling reads so handlers always see fresh values.
  const workedRef = useRef(workedPrefixes);
  const currentRef = useRef(currentPrefix);
  const registry = useRef<Map<string, { layer: L.Path; country: CountryCode }>>(new Map());
  workedRef.current = workedPrefixes;

  // Imperatively move the "current" highlight without rebuilding the layer.
  useEffect(() => {
    const prev = currentRef.current;
    currentRef.current = currentPrefix;
    const restyle = (prefix: string | null) => {
      if (!prefix) return;
      const hit = registry.current.get(prefix);
      if (!hit) return;
      const state: StyleState =
        prefix === currentRef.current ? 'current'
          : workedRef.current.has(prefix) ? 'worked' : 'base';
      hit.layer.setStyle(styleFor(state, hit.country));
    };
    if (prev && prev !== currentPrefix) restyle(prev);
    restyle(currentPrefix);
  }, [currentPrefix]);

  // Lazy-load the (131 KB) boundary data so it stays out of the main bundle.
  useEffect(() => {
    let cancelled = false;
    import('../geo/contestGeo/naAdmin1.geo.json')
      .then((mod) => {
        if (!cancelled) {
          setData((mod.default ?? mod) as unknown as FeatureCollection<Geometry, EntityProps>);
        }
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const counts = useMemo(() => {
    const c: Record<CountryCode, number> = { US: 0, CA: 0, MX: 0 };
    data?.features.forEach((f) => {
      const cc = f.properties.country;
      if (cc in c) c[cc] += 1;
    });
    return c;
  }, [data]);

  // How many worked prefixes actually correspond to a mapped state/province.
  const workedOnMap = useMemo(() => {
    if (!data) return 0;
    let n = 0;
    workedPrefixes.forEach((p) => {
      if (data.features.some((f) => getPrefix(f.properties.name) === p)) n += 1;
    });
    return n;
  }, [data, workedPrefixes]);

  const sessionActive = !!contestState;

  return (
    <div className="relative w-full h-full" style={{ background: '#0a0e14' }}>
      <style>{`
        .contest-geo-label {
          background: transparent !important;
          border: none !important;
          box-shadow: none !important;
          color: #e6eef7;
          font-family: 'JetBrains Mono', monospace;
          font-size: 10px;
          font-weight: 700;
          text-shadow: 0 0 3px rgba(0,0,0,0.95), 0 1px 2px rgba(0,0,0,0.9);
          padding: 0;
          white-space: nowrap;
        }
        .contest-geo-label::before { display: none !important; }
      `}</style>

      {!data && !error && (
        <div className="absolute inset-0 flex items-center justify-center text-dark-300 text-sm font-ui z-[500]">
          Loading map…
        </div>
      )}
      {error && (
        <div className="absolute inset-0 flex items-center justify-center text-red-400 text-sm font-ui z-[500] px-4 text-center">
          Failed to load map data: {error}
        </div>
      )}

      <MapContainer
        center={NA_CENTER}
        zoom={NA_ZOOM}
        minZoom={2}
        maxZoom={7}
        className="w-full h-full"
        style={{ background: '#0a0e14' }}
        zoomControl={false}
        attributionControl={false}
        ref={(m) => { mapRef.current = m ?? null; }}
        whenReady={() => requestAnimationFrame(() => mapRef.current?.invalidateSize())}
      >
        {data && (
          <GeoLayers
            data={data}
            showLabels={showLabels}
            workedRef={workedRef}
            currentRef={currentRef}
            registry={registry}
            onSelect={setSelected}
            onPick={setContestFillLocation}
          />
        )}
      </MapContainer>

      {/* Title + legend */}
      <div className="absolute top-2 left-2 z-[1000] glass-panel px-3 py-2 pointer-events-none">
        <div className="flex items-center gap-2 text-sm font-ui font-semibold text-dark-100">
          <MapPinned className="w-4 h-4 text-accent-primary" />
          QSO-Party Map
        </div>
        {sessionActive ? (
          <div className="mt-1.5 flex items-center gap-2 text-[11px] font-ui">
            <Trophy className="w-3 h-3 text-emerald-400" />
            <span className="text-emerald-300">{workedOnMap} worked</span>
            <span className="text-dark-500 truncate max-w-[10rem]">{contestState!.definitionName}</span>
          </div>
        ) : (
          <div className="mt-1.5 flex flex-col gap-0.5">
            {(Object.keys(COUNTRY_COLORS) as CountryCode[]).map((cc) => (
              <div key={cc} className="flex items-center gap-2 text-[11px] font-ui text-dark-300">
                <span
                  className="inline-block w-3 h-3 rounded-sm"
                  style={{ background: COUNTRY_COLORS[cc], opacity: 0.6, border: `1px solid ${COUNTRY_COLORS[cc]}` }}
                />
                <span>{COUNTRY_NAMES[cc]}</span>
                <span className="text-dark-400">({counts[cc]})</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Controls */}
      <div className="absolute top-2 right-2 z-[1000] flex flex-col gap-1">
        <button
          onClick={(e) => { e.stopPropagation(); mapRef.current?.zoomIn(); }}
          className="glass-button p-2"
          title="Zoom In"
        >
          <ZoomIn className="w-4 h-4" />
        </button>
        <button
          onClick={(e) => { e.stopPropagation(); mapRef.current?.zoomOut(); }}
          className="glass-button p-2"
          title="Zoom Out"
        >
          <ZoomOut className="w-4 h-4" />
        </button>
        <button
          onClick={(e) => { e.stopPropagation(); mapRef.current?.setView(NA_CENTER, NA_ZOOM); }}
          className="glass-button p-2"
          title="Fit North America"
        >
          <Locate className="w-4 h-4" />
        </button>
        <button
          onClick={(e) => { e.stopPropagation(); setShowLabels((v) => !v); }}
          className={`glass-button p-2 ${showLabels ? 'text-accent-primary' : ''}`}
          title={showLabels ? 'Hide prefixes' : 'Show prefixes'}
        >
          <Tag className="w-4 h-4" />
        </button>
      </div>

      {/* Selected / hovered readout */}
      {selected && (
        <div className="absolute bottom-2 left-2 z-[1000] glass-panel px-3 py-2">
          <div className="flex items-baseline gap-2">
            <span
              className="font-mono font-bold text-base"
              style={{ color: COUNTRY_COLORS[selected.country] }}
            >
              {selected.prefix || '—'}
            </span>
            <span className="text-sm font-ui text-dark-100">{selected.name}</span>
            <span className="text-[11px] font-ui text-dark-400">{COUNTRY_NAMES[selected.country]}</span>
            {selected.prefix && workedPrefixes.has(selected.prefix) && (
              <span className="text-[11px] font-ui text-emerald-400">worked</span>
            )}
          </div>
          {sessionActive && selected.prefix && (
            <div className="mt-1 text-[10px] font-ui text-dark-400">Click a state to fill the entry location.</div>
          )}
        </div>
      )}
    </div>
  );
}

import { useEffect, useRef } from 'react';
import { useMap } from 'react-leaflet';
import { useQuery } from '@tanstack/react-query';
import L from 'leaflet';
import { api } from '../api/client';
import { useConfirmationRuleStore } from '../store/confirmationRuleStore';

interface GridSquareOverlayProps {
  /** Band to filter worked grids to (e.g. '20m'); 'all' or undefined = every band. */
  band?: string;
  /** Mode to filter worked grids to (e.g. 'FT8'); 'all' or undefined = every mode. */
  mode?: string;
}

// Match the Grid Tracker chart's palette so the two views read the same.
const GREEN = '#2f9e44';      // confirmed
const GREEN_DIM = '#2e6b34';  // worked, not confirmed

/**
 * Maidenhead worked-grid overlay for the 2D map — the GridTracker "grids painted over
 * geography" look. Draws each worked 4-char grid as a translucent rectangle on the slippy
 * map: solid-ish green = confirmed, dim green = worked-but-unconfirmed. Reuses the same
 * /statistics/gridmap data the dedicated Grid Tracker chart uses, so worked/confirmed
 * status stays consistent between the two. Rendered imperatively into a single Leaflet
 * layer group (one vector layer, cleaned up on unmount) rather than as thousands of React
 * components.
 */
export function GridSquareOverlay({ band, mode }: GridSquareOverlayProps) {
  const map = useMap();
  const layerRef = useRef<L.LayerGroup | null>(null);
  const bandFilter = band && band.toLowerCase() !== 'all' ? band : undefined;
  const modeFilter = mode && mode.toLowerCase() !== 'all' ? mode : undefined;

  // Same shared rule as the Grid Tracker panel, so the overlay and the chart
  // can never disagree about which grids are confirmed (#46).
  const confirmationRule = useConfirmationRuleStore((s) => s.rule);

  const { data } = useQuery({
    queryKey: ['gridmap', 'map-overlay', bandFilter ?? 'all', modeFilter ?? 'all', confirmationRule],
    queryFn: () => api.getGridMap(bandFilter, modeFilter, confirmationRule),
    staleTime: 60_000,
  });

  useEffect(() => {
    const group = L.layerGroup().addTo(map);
    layerRef.current = group;

    for (const g of data?.grids ?? []) {
      const corner = gridCorner(g.grid);
      if (!corner) continue;
      // 4-char grid = 2° lon × 1° lat; corner is the SW corner.
      const bounds: L.LatLngBoundsExpression = [
        [corner.lat, corner.lon],
        [corner.lat + 1, corner.lon + 2],
      ];
      const color = g.confirmed ? GREEN : GREEN_DIM;
      L.rectangle(bounds, {
        color,
        weight: 1,
        opacity: 0.7,
        fillColor: color,
        fillOpacity: 0.35,
        interactive: true,
      })
        .bindTooltip(`${g.grid.toUpperCase()} — ${g.confirmed ? 'confirmed' : 'worked'}`, {
          sticky: true,
          direction: 'top',
        })
        .addTo(group);
    }

    return () => {
      group.remove();
      layerRef.current = null;
    };
  }, [map, data]);

  return null;
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

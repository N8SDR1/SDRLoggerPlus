import { useEffect, useRef } from 'react';
import { useMap } from 'react-leaflet';
import L from 'leaflet';
import { api } from '../api/client';

interface AuroraOverlayProps {
  /** 0..1 multiplier on cell fill opacity. */
  opacity?: number;
}

// Green → yellow → orange → red as auroral probability climbs.
function auroraColor(prob: number): string {
  if (prob >= 80) return '#ff2d2d';
  if (prob >= 50) return '#ff8c00';
  if (prob >= 25) return '#e6e600';
  return '#33ff66';
}

/**
 * NOAA OVATION auroral-oval forecast overlay. Each model cell (~1°×1°) is drawn as a
 * canvas-rendered rectangle tinted by probability. Refreshes every 5 minutes to match the
 * server-side cache; the backend already drops near-zero cells to keep the payload small.
 */
export function AuroraOverlay({ opacity = 1 }: AuroraOverlayProps) {
  const map = useMap();
  const layerGroupRef = useRef<L.LayerGroup | null>(null);

  useEffect(() => {
    // Canvas renderer keeps thousands of cells smooth versus SVG paths.
    const renderer = L.canvas({ padding: 0.5 });
    const layerGroup = L.layerGroup().addTo(map);
    layerGroupRef.current = layerGroup;
    let cancelled = false;

    async function updateOverlay() {
      try {
        const forecast = await api.getAuroraOvation();
        if (cancelled) return;
        layerGroup.clearLayers();

        for (const p of forecast.points) {
          const color = auroraColor(p.aurora);
          // Probability drives fill strength so the oval reads as a heat gradient.
          const fillOpacity = Math.min(0.6, 0.12 + (p.aurora / 100) * 0.5) * opacity;
          for (const offset of [-360, 0, 360]) {
            L.rectangle(
              [
                [p.lat - 0.5, p.lon - 0.5 + offset],
                [p.lat + 0.5, p.lon + 0.5 + offset],
              ],
              {
                renderer,
                stroke: false,
                fill: true,
                fillColor: color,
                fillOpacity,
                interactive: false,
              }
            ).addTo(layerGroup);
          }
        }
      } catch {
        // Transient failures just leave the previous frame in place.
      }
    }

    updateOverlay();
    const interval = setInterval(updateOverlay, 5 * 60 * 1000);

    return () => {
      cancelled = true;
      clearInterval(interval);
      if (layerGroupRef.current) {
        map.removeLayer(layerGroupRef.current);
      }
    };
  }, [map, opacity]);

  return null;
}

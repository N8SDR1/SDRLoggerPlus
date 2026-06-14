import { useEffect, useRef } from 'react';
import { useMap } from 'react-leaflet';
import L from 'leaflet';
import { api } from '../api/client';
import { gridToLatLon } from '../utils/maidenhead';

interface PskReporterOverlayProps {
  /** Callsign whose reception reports (who is hearing it) are drawn. */
  callsign: string;
}

// Band coloring mirrors the DX-cluster scheme used elsewhere on the map.
const BAND_COLORS: Record<string, string> = {
  '160m': '#8B0000', '80m': '#DC143C', '60m': '#FF6347', '40m': '#FF8C00',
  '30m': '#FFD700', '20m': '#32CD32', '17m': '#00CED1', '15m': '#00BFFF',
  '12m': '#4169E1', '10m': '#8A2BE2', '6m': '#FF00FF',
};
const BAND_RANGES: Record<string, [number, number]> = {
  '160m': [1800, 2000], '80m': [3500, 4000], '60m': [5330, 5410],
  '40m': [7000, 7300], '30m': [10100, 10150], '20m': [14000, 14350],
  '17m': [18068, 18168], '15m': [21000, 21450], '12m': [24890, 24990],
  '10m': [28000, 29700], '6m': [50000, 54000],
};
function bandFromKHz(khz: number): string {
  for (const [band, [min, max]] of Object.entries(BAND_RANGES)) {
    if (khz >= min && khz <= max) return band;
  }
  return '?';
}

/**
 * PSK Reporter overlay: draws a line from the looked-up callsign to every station that
 * recently heard it, colored by band. Data is fetched through the cached backend proxy and
 * refreshed every 5 minutes (PSK Reporter's polling etiquette).
 */
export function PskReporterOverlay({ callsign }: PskReporterOverlayProps) {
  const map = useMap();
  const layerGroupRef = useRef<L.LayerGroup | null>(null);

  useEffect(() => {
    const call = callsign.trim().toUpperCase();
    const layerGroup = L.layerGroup().addTo(map);
    layerGroupRef.current = layerGroup;
    let cancelled = false;

    if (!call) return undefined;

    async function updateOverlay() {
      try {
        const reports = await api.getPskReports(call);
        if (cancelled) return;
        layerGroup.clearLayers();

        for (const r of reports) {
          const from = gridToLatLon(r.senderLocator);
          const to = gridToLatLon(r.receiverLocator);
          if (!from || !to) continue;

          const khz = r.frequencyHz / 1000;
          const band = bandFromKHz(khz);
          const color = BAND_COLORS[band] || '#888888';

          const tooltip =
            `<b>${r.receiverCallsign}</b> heard <b>${call}</b><br/>` +
            `${khz.toFixed(1)} kHz ${r.mode}${band !== '?' ? ` (${band})` : ''}<br/>` +
            `SNR ${r.snr} dB`;

          L.polyline(
            [
              [from.lat, from.lon],
              [to.lat, to.lon],
            ],
            { color, weight: 1.5, opacity: 0.6 }
          )
            .bindTooltip(tooltip, { sticky: true })
            .addTo(layerGroup);

          // Small marker at the receiving station.
          L.circleMarker([to.lat, to.lon], {
            radius: 3,
            color,
            fillColor: color,
            fillOpacity: 0.9,
            weight: 1,
          })
            .bindTooltip(tooltip, { sticky: true })
            .addTo(layerGroup);
        }
      } catch {
        // Leave the previous frame on transient errors.
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
  }, [map, callsign]);

  return null;
}

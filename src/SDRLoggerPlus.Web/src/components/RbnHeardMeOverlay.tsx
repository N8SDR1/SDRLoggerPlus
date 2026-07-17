import { useEffect, useRef } from 'react';
import { useMap } from 'react-leaflet';
import L from 'leaflet';
import { api } from '../api/client';
import { heardMeBandColor } from '../utils/heardMe';

interface RbnHeardMeOverlayProps {
  /** Operator callsign — RBN skimmers that spotted this call are drawn. */
  callsign: string;
  /** Station origin (arc start). */
  stationLat: number;
  stationLon: number;
  /** Band to filter to (e.g. '20m'); null or 'all' = every band. */
  band?: string | null;
  /** Look-back window in minutes (RBN buffer retains ~15 min). */
  minutes?: number;
}

function ageLabel(seconds: number): string {
  const mins = Math.max(0, Math.round(seconds / 60));
  return mins < 60 ? `${mins}m ago` : `${Math.floor(mins / 60)}h${mins % 60}m ago`;
}

/**
 * RBN "who heard me" overlay for the 2D map: draws a line from the station to every RBN
 * skimmer (CW/RTTY) that recently spotted the operator's callsign, with a clickable dot
 * showing the reception report. Distinct from the general "RBN Layer" (the full cluster
 * feed). Polls every ~45 s; the backend resolves skimmer locations (QRZ → cty.dat).
 */
export function RbnHeardMeOverlay({ callsign, stationLat, stationLon, band = null, minutes = 15 }: RbnHeardMeOverlayProps) {
  const map = useMap();
  const layerGroupRef = useRef<L.LayerGroup | null>(null);

  useEffect(() => {
    const call = callsign.trim().toUpperCase();
    const bandFilter = band && band !== 'all' ? band : null;
    const layerGroup = L.layerGroup().addTo(map);
    layerGroupRef.current = layerGroup;
    let cancelled = false;

    if (!call) return undefined;

    async function updateOverlay() {
      try {
        const reports = await api.getRbnHeardMe(call, bandFilter, minutes);
        if (cancelled) return;
        layerGroup.clearLayers();

        for (const r of reports) {
          const color = heardMeBandColor(r.band);

          const popup =
            `<b>${r.skimmer}</b> (RBN) heard <b>${call}</b><br/>` +
            `${r.freqKhz.toFixed(1)} kHz ${r.mode}${r.band ? ` (${r.band})` : ''}<br/>` +
            `SNR ${r.snr} dB · ${ageLabel(r.ageSeconds)}`;

          L.polyline(
            [
              [stationLat, stationLon],
              [r.lat, r.lon],
            ],
            { color, weight: 1.5, opacity: 0.6, dashArray: '4 4' }
          ).addTo(layerGroup);

          L.circleMarker([r.lat, r.lon], {
            radius: 4,
            color,
            fillColor: color,
            fillOpacity: 0.9,
            weight: 1,
          })
            .bindPopup(popup)
            .bindTooltip(`${r.skimmer} · ${r.snr} dB`, { direction: 'top' })
            .addTo(layerGroup);
        }
      } catch {
        // Leave the previous frame on transient errors.
      }
    }

    updateOverlay();
    const interval = setInterval(updateOverlay, 45 * 1000);

    return () => {
      cancelled = true;
      clearInterval(interval);
      if (layerGroupRef.current) {
        map.removeLayer(layerGroupRef.current);
      }
    };
  }, [map, callsign, stationLat, stationLon, band, minutes]);

  return null;
}

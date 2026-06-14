import { useEffect, useRef } from 'react';
import { useAppStore } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import { useToastStore } from '../store/toastStore';
import { checkOutOfBand, type ItuRegion } from '../core/bandPlan';

/**
 * Watches the selected radio's VFO and warns (once per band@kHz) when it sits
 * outside the strict ITU allocation for the configured region (SDRLogger+ port).
 * The warning re-arms when the VFO returns in-band or moves to a new frequency.
 */
export function useOutOfBandAlert() {
  const { radioStates, selectedRadioId } = useAppStore();
  const region = useSettingsStore((s) => s.settings.station.ituRegion);
  const push = useToastStore((s) => s.push);
  const lastOobKeyRef = useRef<string | null>(null);

  const frequencyHz = selectedRadioId ? radioStates.get(selectedRadioId)?.frequencyHz : undefined;

  useEffect(() => {
    if (frequencyHz == null || frequencyHz <= 0) return;
    const mhz = frequencyHz / 1_000_000;
    const oob = checkOutOfBand(mhz, (region || 2) as ItuRegion);
    if (oob) {
      const key = `${oob.band}@${Math.round(mhz * 1000)}`;
      if (lastOobKeyRef.current !== key) {
        lastOobKeyRef.current = key;
        push(
          `VFO ${mhz.toFixed(4)} MHz is outside the ${oob.band} ITU Region ${region || 2} allocation (${oob.lo}–${oob.hi} MHz)`,
          'warn',
          12000
        );
      }
    } else {
      lastOobKeyRef.current = null;
    }
  }, [frequencyHz, region, push]);
}

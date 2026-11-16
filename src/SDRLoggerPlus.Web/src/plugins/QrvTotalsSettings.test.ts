import { describe, expect, it } from 'vitest';
import { useSettingsStore } from '../store/settingsStore';

describe('QRV QSO totals setting', () => {
  it('stays off until that switch is set, and does not turn presence on', () => {
    useSettingsStore.setState((state) => ({
      settings: {
        ...state.settings,
        qrv: { enabled: false, baseUrl: 'https://qrvsdr.cc/v1', qsoTotalsEnabled: false },
      },
      isDirty: false,
    }));

    expect(useSettingsStore.getState().settings.qrv.qsoTotalsEnabled).toBe(false);
    expect(useSettingsStore.getState().settings.qrv.enabled).toBe(false);

    useSettingsStore.getState().updateQrvSettings({ qsoTotalsEnabled: true });

    expect(useSettingsStore.getState().settings.qrv.qsoTotalsEnabled).toBe(true);
    expect(useSettingsStore.getState().settings.qrv.enabled).toBe(false);
    expect(useSettingsStore.getState().isDirty).toBe(true);
  });
});

import { createElement } from 'react';
import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { SettingsPanel } from '../components/SettingsPanel';
import { useSettingsStore } from '../store/settingsStore';

describe('QRV QSO totals setting', () => {
  it('tells the operator that contacts already in the log count, including an ADIF import', () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('offline')));
    useSettingsStore.setState({ isOpen: true, activeSection: 'qrv' });

    render(createElement(SettingsPanel));

    expect(screen.getByText(/Every non-contest contact already in the log counts/)).toBeInTheDocument();
    expect(screen.getByText(/ADIF monitor/)).toBeInTheDocument();
    expect(screen.getByText(/ADIF-over-UDP/)).toBeInTheDocument();
    expect(screen.getByText(/manual ADIF import all count/)).toBeInTheDocument();
    expect(screen.queryByText(/ADIF import stays out/)).not.toBeInTheDocument();
  });

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

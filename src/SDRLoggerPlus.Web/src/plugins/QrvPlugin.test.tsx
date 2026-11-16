import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { QrvPlugin } from './QrvPlugin';
import { useSettingsStore } from '../store/settingsStore';

describe('QrvPlugin', () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({
        enabled: false,
        onAir: false,
        phase: 'off',
        detail: 'Presence is off.',
        error: null,
        roster: [],
      }),
    });
    useSettingsStore.setState((state) => ({
      settings: {
        ...state.settings,
        qrv: { enabled: false, baseUrl: 'https://qrvsdr.cc/v1' },
      },
    }));
  });

  it('renders off by default and does not enable until the control is used', async () => {
    render(<QrvPlugin />);

    expect(await screen.findByText('Off')).toBeTruthy();
    expect(screen.getByText('Presence is off.')).toBeTruthy();
    const toggle = screen.getByRole('button', { name: 'Share QRV presence' });
    expect(toggle.getAttribute('aria-pressed')).toBe('false');
    expect(screen.getByText('QRV off')).toBeTruthy();

    const statusCalls = fetchMock.mock.calls.filter((call) => String(call[0]).includes('/api/qrv'));
    expect(statusCalls.some((call) => String(call[0]).includes('watch=true'))).toBe(true);
    expect(statusCalls.some((call) => call[1]?.method === 'PUT')).toBe(false);
  });

  it('turns presence on through the settings endpoint and shows an error without dropping the roster', async () => {
    render(<QrvPlugin />);
    await screen.findByText('Off');

    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
      if (init?.method === 'PUT') {
        return {
          ok: true,
          json: async () => ({
            enabled: true,
            onAir: false,
            phase: 'error',
            error: 'invalid_fields',
            detail: 'QRV rejected the last update. Change mode, or turn QRV off and on, to try again.',
            call: 'N8SDR',
            band: '20m',
            mode: 'FT8',
            roster: [{ call: 'KC8TYK', app: 'Lyra', band: '40m', mode: 'LSB', lastSeen: '2026-11-15T23:03:44Z' }],
          }),
        };
      }
      return {
        ok: true,
        json: async () => ({ enabled: false, onAir: false, phase: 'off', detail: 'Presence is off.', roster: [] }),
      };
    });

    fireEvent.click(screen.getByRole('button', { name: 'Share QRV presence' }));

    expect(await screen.findByText('Error')).toBeTruthy();
    expect(screen.getByText('invalid_fields')).toBeTruthy();
    expect(screen.getByText('KC8TYK')).toBeTruthy();
    expect(screen.getByText(/N8SDR\s+20m\s+FT8/)).toBeTruthy();
    await waitFor(() => {
      expect(useSettingsStore.getState().settings.qrv.enabled).toBe(true);
    });
    const put = fetchMock.mock.calls.find((call) => call[1]?.method === 'PUT');
    expect(String(put?.[0])).toContain('/api/qrv/settings');
    expect(JSON.parse(String(put?.[1].body))).toEqual({ enabled: true });
  });
});

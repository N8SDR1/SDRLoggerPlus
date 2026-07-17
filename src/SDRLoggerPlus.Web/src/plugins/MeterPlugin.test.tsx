import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, act, fireEvent } from '@testing-library/react';
import { MeterPlugin } from './MeterPlugin';
import type { TciMetersEvent } from '../api/signalr';

// Capture the callback MeterPlugin registers so tests can push synthetic
// meter events without a SignalR connection.
let registeredCallback: ((evt: TciMetersEvent) => void) | null = null;

vi.mock('../api/signalr', () => ({
  setTciMetersCallback: (cb: ((evt: TciMetersEvent) => void) | null) => {
    registeredCallback = cb;
  },
  clearTciMetersCallback: (cb: (evt: TciMetersEvent) => void) => {
    if (registeredCallback === cb) registeredCallback = null;
  },
  setSpectrumDataCallback: vi.fn(),
  clearSpectrumDataCallback: vi.fn(),
}));

function makeEvent(overrides: Partial<TciMetersEvent> = {}): TciMetersEvent {
  return {
    radioId: 'radio1',
    rxSignalDbm: -97.4,
    rxAvgSignalDbm: -99.1,
    txMicDbm: null,
    txPowerWatts: 4.8,
    txPeakPowerWatts: 5.0,
    txSwr: 1.42,
    isTransmitting: false,
    timestampUtc: new Date().toISOString(),
    ...overrides,
  };
}

describe('MeterPlugin', () => {
  beforeEach(() => {
    registeredCallback = null;
    localStorage.clear();
  });

  it('shows the empty state before any meter data arrives', () => {
    render(<MeterPlugin />);
    expect(screen.getByText(/No meter data/i)).toBeInTheDocument();
  });

  it('renders the meter content after a meter event arrives', () => {
    render(<MeterPlugin />);
    expect(registeredCallback).not.toBeNull();

    act(() => registeredCallback!(makeEvent()));

    // The empty state is replaced by the S-meter + freq/mode line. (Power/SWR
    // tiles are TX-side and now live in Lyra, so they're no longer rendered.)
    expect(screen.queryByText(/No meter data/i)).toBeNull();
    expect(screen.getByTestId('meter-content')).toBeInTheDocument();
    expect(screen.getByTestId('meter-freq-line')).toBeInTheDocument();
  });

  it('keeps the analog view and exposes the round meter view', () => {
    render(<MeterPlugin />);
    act(() => registeredCallback!(makeEvent()));

    expect(screen.getByTestId('meter-content')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Round' }));

    expect(screen.getByTestId('round-meter-canvas')).toBeInTheDocument();
    // Meter config now lives behind the settings gear.
    fireEvent.click(screen.getByRole('button', { name: 'Meter settings' }));
    expect(screen.getByLabelText('Peak')).toBeChecked();
    expect(screen.getByLabelText('Spectrum')).toBeChecked();
  });

  it('shows a dash for frequency when the radio reports none', () => {
    render(<MeterPlugin />);

    // A bare meter event carries no VFO frequency (that comes from the TCI
    // radio state), so the freq tile shows the placeholder dash.
    act(() => registeredCallback!(makeEvent()));

    expect(screen.getByTestId('meter-freq').textContent).toBe('—');
  });

  it('unregisters the callback on unmount', () => {
    const { unmount } = render(<MeterPlugin />);
    expect(registeredCallback).not.toBeNull();
    unmount();
    expect(registeredCallback).toBeNull();
  });

  it('ignores events from a second radio while the first is live', () => {
    render(<MeterPlugin />);

    // The meter locks onto the first radio it hears and ignores others until
    // that source goes stale. radio2's TX event must not flip the meter to TX.
    act(() => registeredCallback!(makeEvent({ radioId: 'radio1', isTransmitting: false })));
    act(() => registeredCallback!(makeEvent({ radioId: 'radio2', isTransmitting: true })));

    expect(screen.queryByText('TX')).toBeNull();
    expect(screen.getByTestId('meter-freq-line').className).toContain('text-white');
  });

  it('accepts non-finite values without crashing the meter', () => {
    render(<MeterPlugin />);

    // The callback defensively normalizes NaN/Infinity to null before the
    // canvas math, so a garbage event must still render cleanly.
    act(() => registeredCallback!(makeEvent({ rxSignalDbm: Number.NaN, rxAvgSignalDbm: Infinity })));

    expect(screen.getByTestId('meter-content')).toBeInTheDocument();
  });

  it('dims the tiles once data goes stale', () => {
    vi.useFakeTimers({
      toFake: ['setInterval', 'clearInterval', 'performance', 'requestAnimationFrame', 'cancelAnimationFrame'],
    });
    try {
      render(<MeterPlugin />);
      act(() => registeredCallback!(makeEvent()));

      const content = screen.getByTestId('meter-content');
      expect(content.style.opacity).toBe('');

      // STALE_AFTER_MS is 2000 and the staleness poll runs every 500 ms.
      act(() => {
        vi.advanceTimersByTime(2600);
      });

      expect(screen.getByTestId('meter-content').style.opacity).toBe('0.45');
    } finally {
      vi.useRealTimers();
    }
  });
});

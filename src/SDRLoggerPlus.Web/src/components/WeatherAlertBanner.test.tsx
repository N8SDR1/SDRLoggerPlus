import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { WeatherAlertBanner } from './WeatherAlertBanner';
import { api, LightningStatus, WindStatus } from '../api/client';
import { useSettingsStore } from '../store/settingsStore';
import { useWeatherPreviewStore } from '../store/weatherPreviewStore';

const quietWind: WindStatus = {
  active: false,
  severity: '',
  sustainedMph: null,
  gustMph: null,
  sustainedKph: null,
  gustKph: null,
  direction: '',
  sources: [],
  nwsAlert: null,
  lastUpdateUtc: null,
  unit: 'mph',
  threshSustMph: 30,
  threshGustMph: 45,
};

const activeLightning = (ageMs: number): LightningStatus => ({
  active: true,
  closestKm: 12.3,
  closestMi: 7.6,
  direction: 'NE',
  strikeCount: 4,
  sources: ['blitzortung'],
  nwsWarning: null,
  lastUpdateUtc: new Date(Date.now() - ageMs).toISOString(),
});

function setLightningEnabled(enabled: boolean) {
  useSettingsStore.setState((s) => ({
    settings: {
      ...s.settings,
      weather: {
        ...s.settings.weather,
        lightning: { ...s.settings.weather.lightning, enabled },
      },
    },
  }));
}

describe('WeatherAlertBanner staleness hint', () => {
  beforeEach(() => {
    vi.spyOn(api, 'getWindStatus').mockResolvedValue(quietWind);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    setLightningEnabled(false);
    useWeatherPreviewStore.setState({ lightning: null, wind: null, expiresAt: 0 });
  });

  it('renders an active alert without the hint while data is fresh', async () => {
    setLightningEnabled(true);
    vi.spyOn(api, 'getLightningStatus').mockResolvedValue(activeLightning(0));

    render(<WeatherAlertBanner />);

    await waitFor(() => expect(screen.getByTitle('Lightning detected')).toBeInTheDocument());
    expect(screen.queryByText(/data delayed/)).toBeNull();
  });

  it('does not show the hint at a healthy-but-slow age (2 min)', async () => {
    setLightningEnabled(true);
    vi.spyOn(api, 'getLightningStatus').mockResolvedValue(activeLightning(2 * 60_000));

    render(<WeatherAlertBanner />);

    await waitFor(() => expect(screen.getByTitle('Lightning detected')).toBeInTheDocument());
    expect(screen.queryByText(/data delayed/)).toBeNull();
  });

  it('shows "data delayed" once the status is older than 3 minutes', async () => {
    setLightningEnabled(true);
    vi.spyOn(api, 'getLightningStatus').mockResolvedValue(activeLightning(4 * 60_000));

    render(<WeatherAlertBanner />);

    await waitFor(() => expect(screen.getByText(/data delayed/)).toBeInTheDocument());
  });

  it('never shows the hint for a preview, even with an old timestamp', async () => {
    // Weather stays disabled: the preview alone should drive the banner.
    useWeatherPreviewStore.setState({
      lightning: activeLightning(10 * 60_000),
      wind: null,
      expiresAt: Date.now() + 60_000,
    });

    render(<WeatherAlertBanner />);

    await waitFor(() => expect(screen.getByTitle('Lightning detected')).toBeInTheDocument());
    expect(screen.queryByText(/data delayed/)).toBeNull();
    expect(screen.getByText('Preview')).toBeInTheDocument();
  });
});

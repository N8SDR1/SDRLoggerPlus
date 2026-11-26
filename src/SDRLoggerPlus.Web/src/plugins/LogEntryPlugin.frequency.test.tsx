// Field report (2026-11-26): while following the rig, a QSO logged after the
// previous one carried no frequency (or an old one). Logging reset the form's
// frequency copy to '' — the box then showed a grey "14.250" placeholder — and
// nothing refilled it until the rig next changed frequency. Log Entry now reads
// the rig's live frequency at log time; these tests log back-to-back QSOs
// without retuning and check every one carries the rig's frequency.
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useAppStore } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import type { CreateQsoRequest } from '../api/client';

const createQso = vi.fn(async (q: CreateQsoRequest) => ({ id: 'x', ...q }));

vi.mock('../api/client', async (orig) => {
  const actual = await orig<typeof import('../api/client')>();
  return {
    ...actual,
    api: {
      ...actual.api,
      createQso: (q: CreateQsoRequest) => createQso(q),
      checkQsoDupe: async () => null,
      getDxccEntityGlance: async () => null,
      getRecentByCallsign: async () => null,
      getSatStatus: async () => null,
      getScpCalls: async () => [],
      getWorkedCalls: async () => [],
    },
  };
});

vi.mock('../hooks/useSignalR', () => ({
  useSignalR: () => ({
    focusCallsign: vi.fn(),
    persistCallsignMapImage: vi.fn(async () => {}),
    setRadioMode: vi.fn(),
    tuneToBand: vi.fn(),
    sendDxSpot: vi.fn(),
  }),
}));

const { LogEntryPlugin } = await import('./LogEntryPlugin');

const RADIO = 'lyra-1';
function setRig(frequencyHz: number) {
  act(() => {
    useAppStore.getState().setRadioState({
      radioId: RADIO, frequencyHz, mode: 'USB', isTransmitting: false, band: '20m',
    } as Parameters<ReturnType<typeof useAppStore.getState>['setRadioState']>[0]);
  });
}

function renderPanel() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={client}><LogEntryPlugin /></QueryClientProvider>);
}

async function logContact(user: ReturnType<typeof userEvent.setup>, call: string) {
  const box = screen.getByPlaceholderText(/callsign/i);
  await user.clear(box);
  await user.type(box, call);
  await user.click(screen.getByRole('button', { name: /log qso/i }));
  await waitFor(() => expect(createQso).toHaveBeenLastCalledWith(expect.objectContaining({ callsign: call })));
  // Let the post-save reset run.
  await waitFor(() => expect(box).toHaveValue(''));
}

describe('Log Entry frequency while following the rig', () => {
  beforeEach(() => {
    createQso.mockClear();
    useAppStore.setState({ radioStates: new Map(), selectedRadioId: RADIO, selectedSpot: null });
    const s = useSettingsStore.getState();
    useSettingsStore.setState({ settings: { ...s.settings, radio: { ...s.settings.radio, followRadio: true } } });
  });

  it('logs the rig frequency on back-to-back QSOs without retuning', async () => {
    const user = userEvent.setup();
    renderPanel();
    setRig(14_237_500);

    await logContact(user, 'W1AW');
    await logContact(user, 'K1ABC');   // same frequency, rig sent nothing new

    expect(createQso.mock.calls.map(c => c[0].frequency)).toEqual([14237.5, 14237.5]);
    expect(screen.getByDisplayValue('14.237500')).toBeInTheDocument(); // box still shows it
  });

  it('logs where the rig is now, not where a previous QSO was', async () => {
    const user = userEvent.setup();
    renderPanel();
    setRig(14_237_500);
    await logContact(user, 'W1AW');

    setRig(14_195_000);
    await logContact(user, 'K1ABC');

    expect(createQso.mock.calls.map(c => c[0].frequency)).toEqual([14237.5, 14195]);
  });
});

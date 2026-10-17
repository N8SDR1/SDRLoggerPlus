import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QrzProfilePlugin } from './QrzProfilePlugin';
import { useAppStore } from '../store/appStore';

const focusCallsign = vi.fn();

vi.mock('../hooks/useSignalR', () => ({
  useSignalR: () => ({ focusCallsign }),
}));

describe('QRZ profile callsign box', () => {
  beforeEach(() => {
    focusCallsign.mockClear();
    useAppStore.setState({
      focusedCallsign: null,
      focusedCallsignInfo: null,
      isLookingUpCallsign: false,
      profileCallRequest: null,
    });
  });

  it('looks up a callsign typed in the profile and asks Log Entry to use it', async () => {
    const user = userEvent.setup();
    render(<QrzProfilePlugin />);

    await user.type(screen.getByLabelText('Callsign'), 'w1aw');

    expect(focusCallsign).toHaveBeenCalledWith('W1AW', 'qrz-profile');
    expect(useAppStore.getState().profileCallRequest?.call).toBe('W1AW');
  });

  it('shows the callsign Log Entry already focused', () => {
    useAppStore.setState({ focusedCallsign: 'K1ABC' });
    render(<QrzProfilePlugin />);
    expect(screen.getByLabelText('Callsign')).toHaveValue('K1ABC');
  });
});

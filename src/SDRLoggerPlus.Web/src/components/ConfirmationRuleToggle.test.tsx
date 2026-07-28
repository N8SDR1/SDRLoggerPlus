import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ConfirmationRuleToggle } from './ConfirmationRuleToggle';
import { useConfirmationRuleStore } from '../store/confirmationRuleStore';

// Issue #46: the grid views must agree on what "confirmed" means. The store
// defaults to ARRL counting and every grid surface reads the same value.
describe('ConfirmationRuleToggle', () => {
  beforeEach(() => {
    localStorage.removeItem('awards.confirmationRule');
    useConfirmationRuleStore.setState({ rule: 'awardRules' });
  });

  it('defaults to ARRL counting (LoTW + card)', () => {
    expect(useConfirmationRuleStore.getState().rule).toBe('awardRules');
  });

  it('switches the shared rule and persists it', () => {
    render(<ConfirmationRuleToggle />);

    fireEvent.click(screen.getByText('All conf'));

    expect(useConfirmationRuleStore.getState().rule).toBe('any');
    expect(localStorage.getItem('awards.confirmationRule')).toBe('any');

    fireEvent.click(screen.getByText('LoTW+Card'));

    expect(useConfirmationRuleStore.getState().rule).toBe('awardRules');
    expect(localStorage.getItem('awards.confirmationRule')).toBe('awardRules');
  });
});

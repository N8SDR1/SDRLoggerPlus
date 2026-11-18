import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { DxccEntityGlanceGrid } from './DxccEntityGlanceGrid';
import type { DxccEntityGlance } from '../api/client';

function glance(overrides?: Partial<DxccEntityGlance>): DxccEntityGlance {
  return {
    entityName: 'Hawaii',
    primaryPrefix: 'KH6',
    bands: ['40m', '20m'],
    rows: [
      {
        modeGroup: 'phone',
        cells: [
          { worked: true, confirmed: true, qsoCount: 2, callsignWorked: true },
          { worked: false, confirmed: false, qsoCount: 0, callsignWorked: false },
        ],
      },
      {
        modeGroup: 'cw',
        cells: [
          { worked: true, confirmed: false, qsoCount: 1, callsignWorked: false },
          { worked: false, confirmed: false, qsoCount: 0, callsignWorked: false },
        ],
      },
      {
        modeGroup: 'digital',
        cells: [
          { worked: false, confirmed: false, qsoCount: 0, callsignWorked: false },
          { worked: false, confirmed: false, qsoCount: 0, callsignWorked: false },
        ],
      },
    ],
    ...overrides,
  };
}

describe('DxccEntityGlanceGrid', () => {
  it('draws the entity, a dash, confirmed green, and worked amber', () => {
    render(<DxccEntityGlanceGrid glance={glance()} />);

    expect(screen.getByText('Hawaii')).toBeInTheDocument();
    expect(screen.getByText('KH6')).toBeInTheDocument();
    expect(screen.getByText('Phone')).toBeInTheDocument();
    expect(screen.getByText('CW')).toBeInTheDocument();
    expect(screen.getByText('Digital')).toBeInTheDocument();
    expect(screen.getAllByText('-')).toHaveLength(4);

    expect(screen.getByTitle('2 QSOs · Confirmed')).toHaveClass('bg-accent-success');
    expect(screen.getByTitle('1 QSO')).toHaveClass('bg-accent-warning');
    expect(screen.getByText('Confirmed')).toBeInTheDocument();
    expect(screen.getByText('Worked')).toBeInTheDocument();
    expect(screen.getByText('Not worked')).toBeInTheDocument();
  });

  it('ignores the callsign mark in stage 1', () => {
    const { container } = render(<DxccEntityGlanceGrid glance={glance()} />);
    expect(container.querySelector('.bg-accent-info')).toBeNull();
    expect(screen.getByTitle('2 QSOs · Confirmed')).toHaveClass('bg-accent-success');
  });
});

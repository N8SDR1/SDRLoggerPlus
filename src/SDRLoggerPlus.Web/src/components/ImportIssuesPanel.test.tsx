import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { ImportIssuesPanel } from './ImportIssuesPanel';
import type { AdifImportIssue } from '../api/client';

const issue = (over: Partial<AdifImportIssue>): AdifImportIssue => ({
  field: 'mode',
  originalValue: 'FT2',
  storedValue: 'FT2',
  action: 'Flagged',
  count: 1,
  note: '',
  ...over,
});

describe('ImportIssuesPanel', () => {
  it('renders nothing at all for a clean import', () => {
    // A clean import must not show an empty panel or a reassuring "0 issues" box.
    const { container } = render(<ImportIssuesPanel issues={[]} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('renders nothing when the server did not send the field', () => {
    const { container } = render(<ImportIssuesPanel issues={undefined} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('shows an unrecognised value with its record count and says it was kept', () => {
    render(<ImportIssuesPanel issues={[issue({ originalValue: 'FT2', count: 114 })]} />);

    expect(screen.getByText('FT2')).toBeInTheDocument();
    expect(screen.getByText('114')).toBeInTheDocument();
    expect(screen.getByText('kept as-is')).toBeInTheDocument();
    expect(screen.getByText(/Not recognised/)).toBeInTheDocument();
  });

  it('shows a correction as a rewrite, not as something needing attention', () => {
    render(<ImportIssuesPanel issues={[
      issue({ field: 'band', originalValue: '40M', storedValue: '40m', action: 'Corrected', count: 850 }),
    ]} />);

    expect(screen.getByText('40M')).toBeInTheDocument();
    expect(screen.getByText(/40m/)).toBeInTheDocument();
    expect(screen.getByText(/Normalised/)).toBeInTheDocument();
    expect(screen.queryByText(/Not recognised/)).not.toBeInTheDocument();
  });

  it('totals records rather than distinct values in each heading', () => {
    render(<ImportIssuesPanel issues={[
      issue({ originalValue: 'FT2', count: 114 }),
      issue({ originalValue: '29', count: 49 }),
      issue({ field: 'band', originalValue: '40M', storedValue: '40m', action: 'Corrected', count: 850 }),
      issue({ field: 'band', originalValue: '80M', storedValue: '80m', action: 'Corrected', count: 664 }),
    ]} />);

    expect(screen.getByText(/Not recognised — 163 records/)).toBeInTheDocument();
    expect(screen.getByText(/Normalised — 1,514 records/)).toBeInTheDocument();
  });

  it('puts the values needing a decision before the ones already handled', () => {
    const { container } = render(<ImportIssuesPanel issues={[
      issue({ field: 'band', originalValue: '40M', storedValue: '40m', action: 'Corrected', count: 850 }),
      issue({ originalValue: 'FT2', count: 114 }),
    ]} />);

    const text = container.textContent ?? '';
    expect(text.indexOf('Not recognised')).toBeLessThan(text.indexOf('Normalised'));
  });

  it('uses singular wording for a single record', () => {
    render(<ImportIssuesPanel issues={[issue({ count: 1 })]} />);
    expect(screen.getByText(/1 record$/)).toBeInTheDocument();
  });
});

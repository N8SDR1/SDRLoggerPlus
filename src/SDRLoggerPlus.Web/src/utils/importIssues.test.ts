import { describe, it, expect } from 'vitest';
import { summarizeImportIssues } from './importIssues';
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

describe('summarizeImportIssues', () => {
  it('separates the values needing a decision from the ones already handled', () => {
    const { corrected, flagged } = summarizeImportIssues([
      issue({ field: 'band', originalValue: '40M', storedValue: '40m', action: 'Corrected', count: 850 }),
      issue({ originalValue: 'FT2', action: 'Flagged', count: 114 }),
    ]);

    expect(corrected).toHaveLength(1);
    expect(flagged).toHaveLength(1);
    expect(flagged[0].originalValue).toBe('FT2');
  });

  it('totals records rather than counting distinct values', () => {
    // The number that matters is how much of the log is affected — three distinct bad
    // modes across 1,130 records is a very different problem from three records.
    const { correctedRecords, flaggedRecords } = summarizeImportIssues([
      issue({ field: 'band', action: 'Corrected', count: 850 }),
      issue({ field: 'band', action: 'Corrected', count: 664 }),
      issue({ action: 'Flagged', count: 114 }),
      issue({ action: 'Flagged', count: 49 }),
    ]);

    expect(correctedRecords).toBe(1514);
    expect(flaggedRecords).toBe(163);
  });

  it('reports a clean import as empty so no panel is rendered', () => {
    expect(summarizeImportIssues([]).isEmpty).toBe(true);
    expect(summarizeImportIssues(undefined).isEmpty).toBe(true);
  });

  it('treats an unknown action as needing attention rather than silently hiding it', () => {
    // Forward compatibility: a newer server could add an action this build has never
    // heard of. Defaulting it to "flagged" surfaces it; defaulting to "corrected" would
    // quietly bury it, which is the exact failure this whole feature exists to fix.
    const { flagged, corrected } = summarizeImportIssues([issue({ action: 'SomethingNew' })]);

    expect(flagged).toHaveLength(1);
    expect(corrected).toHaveLength(0);
  });

  it('does not treat a missing issues list as an error', () => {
    // Older servers do not send the field at all.
    const s = summarizeImportIssues(undefined);
    expect(s.corrected).toEqual([]);
    expect(s.flagged).toEqual([]);
    expect(s.correctedRecords).toBe(0);
  });
});

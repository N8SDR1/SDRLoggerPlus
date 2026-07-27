import type { AdifImportIssue } from '../api/client';

/**
 * Split the importer's findings into the two groups a person actually acts on
 * differently, and total the records each affected.
 *
 * "Corrected" needs no decision — the value was rewritten with nothing lost (band case).
 * "Flagged" does — the importer did not recognise the value, kept it exactly as it
 * arrived, and is telling you to go look at the source log. Showing them in one
 * undifferentiated list buries the half that matters.
 */
export function summarizeImportIssues(issues: AdifImportIssue[] | undefined) {
  const all = issues ?? [];
  const corrected = all.filter(i => i.action === 'Corrected');
  const flagged = all.filter(i => i.action !== 'Corrected');

  const records = (list: AdifImportIssue[]) => list.reduce((sum, i) => sum + i.count, 0);

  return {
    corrected,
    flagged,
    correctedRecords: records(corrected),
    flaggedRecords: records(flagged),
    // Nothing to show at all — a clean import must not render an empty panel.
    isEmpty: all.length === 0,
  };
}

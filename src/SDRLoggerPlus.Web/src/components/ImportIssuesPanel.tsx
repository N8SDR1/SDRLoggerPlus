import { AlertTriangle } from 'lucide-react';
import type { AdifImportIssue } from '../api/client';
import { summarizeImportIssues } from '../utils/importIssues';

/**
 * The part of the import result that says what actually happened to the data.
 *
 * Without it the dialog reports four integers, so a file full of malformed modes looks
 * exactly like a clean one — which is how 1,130 records carrying modes the ADIF
 * enumeration does not define reached a 24.5k log unnoticed.
 *
 * Flagged is listed first and given the warning colour: those are the values the importer
 * could not understand and stored verbatim, and they are the only ones needing a decision.
 * Corrections are shown for transparency but need nothing from the operator.
 */
export function ImportIssuesPanel({ issues }: { issues?: AdifImportIssue[] }) {
  const { corrected, flagged, correctedRecords, flaggedRecords, isEmpty } =
    summarizeImportIssues(issues);

  if (isEmpty) return null;

  const row = (issue: AdifImportIssue, isFlagged: boolean) => (
    <div
      key={`${issue.field}-${issue.originalValue}-${issue.action}`}
      className="flex items-baseline gap-2 py-1 text-sm border-b border-glass-100 last:border-0"
    >
      <span className="text-xs uppercase text-dark-300 font-ui w-10 flex-shrink-0">
        {issue.field}
      </span>
      <span className="font-mono text-white">{issue.originalValue}</span>
      {isFlagged ? (
        <span className="text-xs text-dark-300 font-ui">kept as-is</span>
      ) : (
        <span className="font-mono text-dark-200">&rarr; {issue.storedValue}</span>
      )}
      <span className="ml-auto font-mono text-dark-200 tabular-nums flex-shrink-0">
        {issue.count.toLocaleString()}
      </span>
    </div>
  );

  return (
    <div className="mb-4 space-y-3" data-testid="import-issues">
      {flagged.length > 0 && (
        <div>
          <p className="text-sm font-ui text-accent-primary flex items-center gap-2 mb-1">
            <AlertTriangle className="w-4 h-4 flex-shrink-0" />
            Not recognised &mdash; {flaggedRecords.toLocaleString()} record
            {flaggedRecords === 1 ? '' : 's'}
          </p>
          <p className="text-xs text-dark-300 font-ui mb-2">
            Stored exactly as they arrived, nothing guessed. Worth checking the source log
            &mdash; these are often truncated fields.
          </p>
          <div className="max-h-40 overflow-y-auto bg-dark-700/50 rounded px-2">
            {flagged.map(i => row(i, true))}
          </div>
        </div>
      )}

      {corrected.length > 0 && (
        <div>
          <p className="text-sm font-ui text-dark-200 mb-1">
            Normalised &mdash; {correctedRecords.toLocaleString()} record
            {correctedRecords === 1 ? '' : 's'}
          </p>
          <p className="text-xs text-dark-300 font-ui mb-2">
            Rewritten to the standard spelling. No information lost.
          </p>
          <div className="max-h-32 overflow-y-auto bg-dark-700/50 rounded px-2">
            {corrected.map(i => row(i, false))}
          </div>
        </div>
      )}
    </div>
  );
}

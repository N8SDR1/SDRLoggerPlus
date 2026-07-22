import type { QslServiceSync, QslSync } from '../api/client';

/**
 * Presentation state for one QSL service in the Log History "Sync" column.
 *
 * `untracked` is a first-class state, not a variant of `pending`. Roughly
 * 24,000 QSOs were logged before the ledger existed and their true upload
 * state is unknowable — showing them as "not sent" would be a claim we cannot
 * support, and would invite a bulk re-send that gets the operator's IP banned
 * by Club Log.
 */
export type QslBadgeState = 'synced' | 'retryable' | 'blocked' | 'untracked';

export interface QslBadge {
  key: string;
  letter: string;
  service: string;
  state: QslBadgeState;
  title: string;
}

const SERVICES: Array<{ key: keyof QslSync; letter: string; name: string }> = [
  { key: 'clubLog', letter: 'C', name: 'Club Log' },
  { key: 'hrdLog', letter: 'H', name: 'HRDLog' },
  { key: 'eqsl', letter: 'E', name: 'eQSL' },
];

/** Sort weight: worse states sort first so problems surface at the top. */
const WEIGHT: Record<QslBadgeState, number> = {
  blocked: 3,
  retryable: 2,
  synced: 1,
  untracked: 0,
};

function formatWhen(iso?: string): string {
  if (!iso) return '';
  const parsed = new Date(iso.endsWith('Z') || /[+-]\d{2}:?\d{2}$/.test(iso) ? iso : `${iso}Z`);
  return Number.isNaN(parsed.getTime()) ? '' : parsed.toISOString().slice(0, 16).replace('T', ' ');
}

function describe(name: string, entry: QslServiceSync): { state: QslBadgeState; title: string } {
  if (entry.status === 'Synced') {
    const when = formatWhen(entry.syncedAt);
    return { state: 'synced', title: `${name}: uploaded${when ? ` ${when} UTC` : ''}` };
  }

  const attempts = entry.attempts > 1 ? ` after ${entry.attempts} attempts` : '';
  const reason = entry.lastError ? ` — ${entry.lastError}` : '';

  if (entry.retryable) {
    return { state: 'retryable', title: `${name}: upload failed${attempts}, will retry${reason}` };
  }

  // Auth and per-QSO rejections need a human. Retrying an auth failure against
  // Club Log is what triggers its IP firewall, so this must not look like
  // something that resolves itself.
  const needs = entry.failureKind === 'Auth' ? 'check credentials' : 'needs attention';
  return { state: 'blocked', title: `${name}: upload failed${attempts} — ${needs}${reason}` };
}

/**
 * Badges for one QSO. Returns an empty array when the QSO predates tracking or
 * no service ever reported — the column then renders a neutral dash rather
 * than implying anything about upload state.
 */
export function qslSyncBadges(sync?: QslSync): QslBadge[] {
  if (!sync) return [];

  return SERVICES.flatMap(({ key, letter, name }) => {
    const entry = sync[key];
    if (!entry) return [];
    const { state, title } = describe(name, entry);
    return [{ key, letter, service: name, state, title }];
  });
}

/**
 * Numeric sort key so the column orders by severity: unresolved failures
 * first, untracked QSOs last.
 */
export function qslSyncSortKey(sync?: QslSync): number {
  return qslSyncBadges(sync).reduce((worst, badge) => Math.max(worst, WEIGHT[badge.state]), 0);
}

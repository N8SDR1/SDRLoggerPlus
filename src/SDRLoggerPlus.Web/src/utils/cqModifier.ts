import type { WsjtxDecodeEvent } from '../api/signalr';

/**
 * Badge text for a CQ decode: "CQ" or "CQ POTA". Null when the decode is not a CQ.
 * Shared by the Digital Decodes row and the modifier filter so the two stay
 * on the same wording.
 */
export function cqBadge(d: Pick<WsjtxDecodeEvent, 'isCq' | 'cqModifier'>): string | null {
  if (!d.isCq) return null;
  const mod = d.cqModifier?.trim();
  return mod ? `CQ ${mod}` : 'CQ';
}

/**
 * CQ-modifier filter. A blank query matches every decode. A non-blank query
 * keeps only CQs whose badge ("CQ", "CQ POTA") or modifier ("POTA") contains
 * the text, case-insensitive — so "CQ" matches every CQ and "POTA" matches
 * "CQ POTA" without hiding those rows behind a separate switch.
 */
export function matchesCqModifier(
  d: Pick<WsjtxDecodeEvent, 'isCq' | 'cqModifier'>,
  query: string,
): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  if (!d.isCq) return false;
  const badge = (cqBadge(d) ?? '').toLowerCase();
  const mod = (d.cqModifier ?? '').toLowerCase();
  return badge.includes(q) || mod.includes(q);
}

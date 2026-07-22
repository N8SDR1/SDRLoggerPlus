/**
 * Staleness predicate for the weather banner's lightning status.
 *
 * The backend can knowingly serve stale data: during a Blitzortung outage it
 * holds the last alert (up to 5 min) rather than fake an all-clear, and a held
 * status keeps its original LastUpdateUtc. That timestamp is the honesty
 * signal — this decides when the banner should say "data delayed".
 */

/**
 * Older than this ⇒ delayed. Must clear the worst-case *healthy* age of
 * ~2.5 min (status restamped every 90 s, banner polls every 60 s), so the
 * hint only appears during a real stall.
 */
export const LIGHTNING_STALE_MS = 3 * 60_000;

/**
 * True when the status timestamp is missing its freshness. LastUpdateUtc is
 * stamped from DateTime.UtcNow (never a LiteDB round-trip), so it always
 * carries the Z designator and Date.parse reads it correctly; an absent or
 * unparseable stamp is treated as "not stale" — the hint accuses the feed,
 * not the field.
 */
export function isLightningDataDelayed(
  lastUpdateUtc: string | null | undefined,
  nowMs: number,
): boolean {
  if (!lastUpdateUtc) return false;
  const t = Date.parse(lastUpdateUtc);
  if (Number.isNaN(t)) return false;
  return nowMs - t > LIGHTNING_STALE_MS;
}

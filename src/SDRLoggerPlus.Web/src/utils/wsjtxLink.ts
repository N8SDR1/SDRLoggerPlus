/**
 * Health summary for the WSJT-X/JTDX UDP link, derived from GET /api/wsjtx/status.
 *
 * The decoder announces itself with a Heartbeat every ~15s, and the backend
 * stamps `lastHeardUtc` each time one arrives. Freshness of that stamp is the
 * only honest liveness signal we have: a bound socket proves we are listening,
 * not that anything is talking to us.
 */

import type { WsjtxStatus } from '../api/client';

/** Three missed heartbeats (~15s apart) before the link is called stale. */
export const HEARTBEAT_STALE_MS = 45_000;

export type WsjtxLinkState =
  /** No source enabled and no error — the pill hides entirely. */
  | 'off'
  /** Socket bound, but no decoder has ever said hello. */
  | 'listening'
  /** A heartbeat arrived within HEARTBEAT_STALE_MS. */
  | 'alive'
  /** We heard a decoder once, but not recently. */
  | 'stale'
  /** A source is enabled but its socket failed to bind. */
  | 'error';

export interface WsjtxLink {
  state: WsjtxLinkState;
  /** Short pill text, e.g. "FT8" or "FT8 ×2". */
  label: string;
  /** Hover text explaining the state in full. */
  title: string;
}

/**
 * Parse a .NET UTC timestamp. System.Text.Json emits a trailing "Z" for
 * DateTimeKind.Utc, but a stamp that lost its designator would otherwise be
 * read as local time — silently shifting freshness by the UTC offset.
 */
export function parseUtc(iso: string): number {
  const hasZone = /[Zz]$|[+-]\d{2}:?\d{2}$/.test(iso);
  return Date.parse(hasZone ? iso : `${iso}Z`);
}

/** Most recent heartbeat across every client of every source, or null. */
function newestHeartbeat(statuses: WsjtxStatus[]): number | null {
  let newest: number | null = null;
  for (const s of statuses) {
    for (const c of s.clients ?? []) {
      const t = parseUtc(c.lastHeardUtc);
      if (!Number.isNaN(t) && (newest === null || t > newest)) newest = t;
    }
  }
  return newest;
}

/**
 * Collapse both decoder sources into the single pill the status bar shows.
 * Worst-case-wins for errors, best-case-wins for liveness: one healthy decoder
 * is what the operator cares about, but a source that failed to bind is a
 * misconfiguration worth surfacing even if the other one is fine.
 */
export function summarizeWsjtxLink(statuses: WsjtxStatus[] | null, nowMs: number): WsjtxLink {
  const list = statuses ?? [];
  const errored = list.filter((s) => !!s.error);
  const listening = list.filter((s) => s.listening);

  if (errored.length > 0) {
    return {
      state: 'error',
      label: 'FT8',
      title: `Decoder link error — ${errored.map((s) => `port ${s.port}: ${s.error}`).join('; ')}`,
    };
  }

  if (listening.length === 0) {
    return { state: 'off', label: 'FT8', title: 'Decoder link off' };
  }

  const ports = listening.map((s) => s.port).join(', ');
  const label = listening.length > 1 ? `FT8 ×${listening.length}` : 'FT8';
  const newest = newestHeartbeat(list);

  if (newest === null) {
    return {
      state: 'listening',
      label,
      title: `Listening on ${ports} — no decoder has connected yet`,
    };
  }

  const ageMs = nowMs - newest;
  if (ageMs <= HEARTBEAT_STALE_MS) {
    const clients = list.flatMap((s) => s.clients ?? []);
    const names = [...new Set(clients.map((c) => c.id).filter(Boolean))].join(', ');
    return {
      state: 'alive',
      label,
      title: `Decoder connected on ${ports}${names ? ` — ${names}` : ''}`,
    };
  }

  return {
    state: 'stale',
    label,
    title: `No heartbeat for ${Math.round(ageMs / 1000)}s — the decoder may have stopped (listening on ${ports})`,
  };
}

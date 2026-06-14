/**
 * Hot List TTS announcer (SDRLogger+ port). Announces hot spots via the
 * built-in speech synthesis with a per-callsign cooldown so a busy pileup
 * doesn't repeat the same call every few seconds.
 */

const lastAnnounced = new Map<string, number>();

/** Spell a callsign character by character so TTS reads "K 5 P", not "kup". */
export function spellCallsign(call: string): string {
  return call.toUpperCase().split('').join(' ');
}

export function shouldAnnounce(call: string, cooldownMinutes: number, now = Date.now()): boolean {
  const key = call.toUpperCase();
  const last = lastAnnounced.get(key) ?? 0;
  if (now - last < cooldownMinutes * 60_000) return false;
  lastAnnounced.set(key, now);
  return true;
}

/** Test hook — reset cooldown state. */
export function resetAnnouncerState(): void {
  lastAnnounced.clear();
}

export function announceHotSpot(call: string, band?: string | null, mode?: string | null): void {
  if (typeof speechSynthesis === 'undefined') return;
  const parts = [`Hot spot. ${spellCallsign(call)}.`];
  if (band) parts.push(band);
  if (mode) parts.push(mode);
  const utterance = new SpeechSynthesisUtterance(parts.join(' '));
  // Chromium requires cancel() before speak() or it silently drops the request
  speechSynthesis.cancel();
  speechSynthesis.speak(utterance);
}

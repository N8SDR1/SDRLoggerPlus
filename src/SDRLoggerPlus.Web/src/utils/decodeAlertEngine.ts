import type { WsjtxDecodeEvent } from '../api/signalr';
import type { DecodeAlertRule } from '../store/settingsStore';
import { useSettingsStore } from '../store/settingsStore';
import { useToastStore } from '../store/toastStore';
import { applyAnnouncementVoice } from './announcementVoice';
import { spellCallsign } from './hotSpotAnnouncer';

/**
 * Geo-scoped needed-status alert engine for the WSJT-X/JTDX/MSHV decode stream
 * (Phase 2b). Runs entirely client-side off the enriched decode event, so it
 * fires app-wide whether or not the Digital Decodes panel is open. Each rule
 * matches an award-need AND every non-empty scope/band/mode filter; the first
 * matching enabled rule fires its actions (sound / voice / popup), throttled by
 * a per-rule+callsign cooldown.
 */

// key = `${ruleId}:${callsign}` → last-fired epoch ms.
const lastFired = new Map<string, number>();

/** Entry point — call for every decode (from the SignalR handler). */
export function runDecodeAlerts(evt: WsjtxDecodeEvent): void {
  const settings = useSettingsStore.getState().settings.decodeAlerts;
  if (!settings?.enabled) return;

  for (const rule of settings.rules) {
    if (!rule.enabled) continue;
    if (!matchesRule(evt, rule)) continue;

    const key = `${rule.id}:${evt.callsign}`;
    const now = Date.now();
    const prev = lastFired.get(key);
    if (prev && now - prev < rule.cooldownMinutes * 60_000) return; // cooling down
    lastFired.set(key, now);

    fireActions(evt, rule);
    return; // first matching rule wins
  }
}

// ─── matching ────────────────────────────────────────────────────────────────

export function matchesRule(evt: WsjtxDecodeEvent, rule: DecodeAlertRule): boolean {
  // At least one enabled award-need must be met.
  const needMatch =
    (rule.newDxcc && evt.spotStatus === 'newDxcc') ||
    (rule.newBand && evt.spotStatus === 'newBand') ||
    (rule.newZone && (evt.zoneStatus === 'newZone' || evt.zoneStatus === 'newZoneBand')) ||
    (rule.newGrid && (evt.gridStatus === 'newGrid' || evt.gridStatus === 'newGridBand'));
  if (!needMatch) return false;

  // Each non-empty scope filter must match.
  if (rule.continents.length && !(evt.continent && rule.continents.includes(evt.continent)))
    return false;

  if (rule.dxccEntities.length &&
      !(evt.country && rule.dxccEntities.some((c) => c.toLowerCase() === evt.country!.toLowerCase())))
    return false;

  if (rule.callAreas.length) {
    const area = usCallArea(evt.callsign);
    if (area == null || !rule.callAreas.includes(area)) return false;
  }

  if (rule.prefixes.length) {
    const base = baseCall(evt.callsign);
    if (!rule.prefixes.some((p) => base.startsWith(p.toUpperCase()))) return false;
  }

  if (rule.gridFields.length) {
    const g = evt.grid?.toUpperCase() ?? '';
    if (!rule.gridFields.some((f) => g.startsWith(f.toUpperCase()))) return false;
  }

  if (rule.bands.length && !(evt.band && rule.bands.includes(evt.band))) return false;
  if (rule.modes.length && !(evt.mode && rule.modes.includes(evt.mode))) return false;

  return true;
}

/**
 * True if the decode satisfies at least one enabled rule in the list. Used by the
 * Digital Decodes panel's "Match Alerts" filter so the geo-scoped alert rules
 * (need × region × band/mode) also narrow the visible list, not just the alerts.
 */
export function matchesAnyRule(evt: WsjtxDecodeEvent, rules: DecodeAlertRule[]): boolean {
  return rules.some((r) => r.enabled && matchesRule(evt, r));
}

/** Strip portable prefixes/suffixes to the base callsign (longest call-shaped part). */
export function baseCall(call: string): string {
  const parts = call.toUpperCase().split('/').filter(Boolean);
  if (parts.length <= 1) return call.toUpperCase();
  // The base is the longest part that looks like a callsign (has a letter and a digit).
  const calls = parts.filter((p) => /[A-Z]/.test(p) && /[0-9]/.test(p));
  if (calls.length) return calls.reduce((a, b) => (b.length > a.length ? b : a));
  return parts[0];
}

/** US call district (0-9) for a W/K/N/A callsign, honouring a portable /n. Null if not US. */
export function usCallArea(call: string): number | null {
  const parts = call.toUpperCase().split('/').filter(Boolean);
  // Portable single-digit override, e.g. W1ABC/6 → operating 6-land.
  for (const p of parts) if (/^[0-9]$/.test(p)) return Number(p);
  const base = baseCall(call);
  if (!/^[AKNW]/.test(base)) return null; // call areas are a US concept
  const m = base.match(/[0-9]/);
  return m ? Number(m[0]) : null;
}

// ─── actions ─────────────────────────────────────────────────────────────────

function fireActions(evt: WsjtxDecodeEvent, rule: DecodeAlertRule): void {
  const label = needLabel(evt, rule);

  if (rule.popup) {
    const bits = [evt.callsign];
    if (evt.grid) bits.push(evt.grid);
    if (evt.country) bits.push(evt.country);
    if (evt.band) bits.push(evt.band);
    useToastStore.getState().push(`${label.toUpperCase()}: ${bits.join(' · ')}`, 'success');
  }

  if (rule.sound) playAlertBeep();

  if (rule.voice && typeof speechSynthesis !== 'undefined') {
    const u = new SpeechSynthesisUtterance(`${label}. ${spellCallsign(evt.callsign)}.`);
    applyAnnouncementVoice(u);
    speechSynthesis.cancel();
    speechSynthesis.speak(u);
  }
}

function needLabel(evt: WsjtxDecodeEvent, rule: DecodeAlertRule): string {
  if (rule.newDxcc && evt.spotStatus === 'newDxcc') return 'New DXCC';
  if (rule.newBand && evt.spotStatus === 'newBand') return 'New band';
  if (rule.newZone && (evt.zoneStatus === 'newZone' || evt.zoneStatus === 'newZoneBand')) return 'New zone';
  if (rule.newGrid && (evt.gridStatus === 'newGrid' || evt.gridStatus === 'newGridBand')) return 'New grid';
  return 'Alert';
}

// Short two-tone WebAudio chime — no asset needed. Reuses one AudioContext.
let audioCtx: AudioContext | null = null;
function playAlertBeep(): void {
  try {
    const Ctx = window.AudioContext || (window as unknown as { webkitAudioContext: typeof AudioContext }).webkitAudioContext;
    if (!Ctx) return;
    audioCtx ??= new Ctx();
    const ctx = audioCtx;
    if (ctx.state === 'suspended') void ctx.resume();
    const now = ctx.currentTime;
    const gain = ctx.createGain();
    gain.connect(ctx.destination);
    gain.gain.setValueAtTime(0.0001, now);
    gain.gain.exponentialRampToValueAtTime(0.15, now + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.35);
    [880, 1320].forEach((freq, i) => {
      const osc = ctx.createOscillator();
      osc.type = 'sine';
      osc.frequency.value = freq;
      osc.connect(gain);
      osc.start(now + i * 0.12);
      osc.stop(now + i * 0.12 + 0.14);
    });
  } catch {
    // Audio not available (autoplay policy / no output) — silent alerts still show.
  }
}

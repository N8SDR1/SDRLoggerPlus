import { useSettingsStore } from '../store/settingsStore';

/**
 * Shared announcement-voice plumbing. Every spoken alert in the app
 * (band-opening, Hot List, RBN, the Settings test buttons) funnels through
 * applyAnnouncementVoice() so the operator's chosen voice + rate — set once in
 * Settings → Voice — is honoured everywhere.
 *
 * The Web Speech API loads its voice list asynchronously, so we cache it and
 * refresh on the `voiceschanged` event.
 */
let cachedVoices: SpeechSynthesisVoice[] = [];

export function loadAnnouncementVoices(): SpeechSynthesisVoice[] {
  if (typeof speechSynthesis === 'undefined') return [];
  const v = speechSynthesis.getVoices();
  if (v.length) cachedVoices = v;
  return cachedVoices;
}

// Prime the cache and keep it fresh (voices often aren't ready on first call).
if (typeof speechSynthesis !== 'undefined') {
  loadAnnouncementVoices();
  try {
    speechSynthesis.addEventListener('voiceschanged', () => loadAnnouncementVoices());
  } catch {
    // Older engines only support the onvoiceschanged handler.
    speechSynthesis.onvoiceschanged = () => loadAnnouncementVoices();
  }
}

export function resolveAnnouncementVoice(voiceUri: string): SpeechSynthesisVoice | null {
  if (!voiceUri) return null;
  return loadAnnouncementVoices().find((v) => v.voiceURI === voiceUri) ?? null;
}

/**
 * Apply the operator's chosen announcement voice + rate to an utterance.
 * Falls back to the browser default voice when none is set or the stored voice
 * isn't installed on this machine.
 */
export function applyAnnouncementVoice(utterance: SpeechSynthesisUtterance): void {
  const { voiceUri, rate, volume } = useSettingsStore.getState().settings.voice;
  const voice = resolveAnnouncementVoice(voiceUri);
  if (voice) {
    utterance.voice = voice;
    utterance.lang = voice.lang;
  }
  if (rate && rate > 0) utterance.rate = rate;
  if (typeof volume === 'number') utterance.volume = Math.max(0, Math.min(1, volume));
}

/** Whether spoken announcements are currently muted (status-bar toggle). */
export function isAnnouncementsMuted(): boolean {
  return useSettingsStore.getState().settings.voice.muted === true;
}

/**
 * Speak an announcement, honouring the operator's voice settings and the global
 * mute. Every automatic spoken alert goes through here so one toggle silences
 * all of them.
 *
 * `force` bypasses the mute for the Settings voice-test buttons — pressing Test
 * is an explicit request to hear the voice, so it should work while muted.
 * `queue` skips the cancel() so consecutive announcements don't cut each other
 * off (the DX Coach wants this); otherwise we cancel first, which Chromium
 * requires or it silently drops the request.
 *
 * Returns false when the announcement was suppressed.
 */
export function speakAnnouncement(
  utterance: SpeechSynthesisUtterance,
  { force = false, queue = false }: { force?: boolean; queue?: boolean } = {},
): boolean {
  if (typeof speechSynthesis === 'undefined') return false;
  if (!force && isAnnouncementsMuted()) return false;
  applyAnnouncementVoice(utterance);
  if (!queue) speechSynthesis.cancel();
  speechSynthesis.speak(utterance);
  return true;
}

/** Best-effort gender guess from a voice name, for the Settings voice filter. */
export function guessVoiceGender(name: string): 'male' | 'female' | null {
  const n = name.toLowerCase();
  if (/\bfemale\b/.test(n)) return 'female';
  if (/\bmale\b/.test(n)) return 'male';
  const female = ['zira', 'aria', 'jenny', 'michelle', 'eva', 'hazel', 'susan', 'linda', 'heera', 'catherine', 'sonia', 'clara', 'natasha'];
  const male = ['david', 'mark', 'guy', 'christopher', 'eric', 'brandon', 'george', 'james', 'ryan', 'sean', 'william', 'liam', 'thomas'];
  if (female.some((f) => n.includes(f))) return 'female';
  if (male.some((m) => n.includes(m))) return 'male';
  return null;
}

/** Friendly label for a BCP-47 language tag, e.g. 'en-US' → 'English (United States)'. */
export function formatAccent(lang: string): string {
  try {
    const langName = new Intl.DisplayNames([lang], { type: 'language' }).of(lang.split('-')[0]);
    const region = lang.split('-')[1];
    const regionName = region
      ? new Intl.DisplayNames([lang], { type: 'region' }).of(region)
      : null;
    const base = langName ? langName.charAt(0).toUpperCase() + langName.slice(1) : lang;
    return regionName ? `${base} (${regionName})` : base;
  } catch {
    return lang;
  }
}

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { speakAnnouncement, isAnnouncementsMuted } from './announcementVoice';
import { useSettingsStore } from '../store/settingsStore';

// Minimal Web Speech stubs — jsdom has none.
const speak = vi.fn();
const cancel = vi.fn();
beforeEach(() => {
  speak.mockReset();
  cancel.mockReset();
  vi.stubGlobal('speechSynthesis', {
    speak,
    cancel,
    getVoices: () => [],
    addEventListener: () => {},
  });
  vi.stubGlobal('SpeechSynthesisUtterance', class {
    text: string;
    rate = 1;
    volume = 1;
    lang = '';
    voice: unknown = null;
    constructor(t: string) { this.text = t; }
  });
  useSettingsStore.setState((s) => ({
    settings: { ...s.settings, voice: { ...s.settings.voice, muted: false } },
  }));
});

function utter(text = 'test') {
  return new SpeechSynthesisUtterance(text);
}

describe('speakAnnouncement mute gate', () => {
  it('speaks when not muted', () => {
    expect(speakAnnouncement(utter())).toBe(true);
    expect(speak).toHaveBeenCalledTimes(1);
  });

  it('suppresses the announcement when muted', () => {
    useSettingsStore.setState((s) => ({
      settings: { ...s.settings, voice: { ...s.settings.voice, muted: true } },
    }));
    expect(speakAnnouncement(utter())).toBe(false);
    expect(speak).not.toHaveBeenCalled();
  });

  it('still speaks a forced announcement while muted (Settings test buttons)', () => {
    useSettingsStore.setState((s) => ({
      settings: { ...s.settings, voice: { ...s.settings.voice, muted: true } },
    }));
    expect(speakAnnouncement(utter(), { force: true })).toBe(true);
    expect(speak).toHaveBeenCalledTimes(1);
  });

  it('cancels first by default, but not when queueing (DX Coach)', () => {
    speakAnnouncement(utter());
    expect(cancel).toHaveBeenCalledTimes(1);

    cancel.mockReset();
    speakAnnouncement(utter(), { queue: true });
    expect(cancel).not.toHaveBeenCalled();
    expect(speak).toHaveBeenCalledTimes(2);
  });

  it('reports the mute state', () => {
    expect(isAnnouncementsMuted()).toBe(false);
    useSettingsStore.setState((s) => ({
      settings: { ...s.settings, voice: { ...s.settings.voice, muted: true } },
    }));
    expect(isAnnouncementsMuted()).toBe(true);
  });
});

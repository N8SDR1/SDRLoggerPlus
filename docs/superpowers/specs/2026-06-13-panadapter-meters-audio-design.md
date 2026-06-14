# Panadapter / Meters / Audio + Tuning — Design Spec

**Date:** 2026-06-13
**Author:** Brent Crier (N9BC) + Claude

## Goal

Five independent enhancements to SDRLoggerPlus, agreed via brainstorming:

1. **Scroll-wheel VFO tuning** in the Rig panel.
2. **Startup auto-connect** to the last radio.
3. **IQ panadapter** — replace the N1MM+ UDP spectrum feed with a Thetis-style IQ
   panadapter (backend reads TCI IQ, FFTs, streams power over SignalR).
4. **TCI audio** (off by default) + an **Audio settings** section.
5. **Round S-meter + mini RX-spectrum** widget in the Meter panel.

Guiding decisions from the user:
- **Don't match totw's look** — replicate *function* in SDRLoggerPlus's style; expose
  the knobs as configurable options.
- Panadapter FFT runs **in the .NET backend** (consistent with today's
  architecture), broadcast over SignalR.
- TCI audio is **opt-in, default off**.
- Meter center is a **mini RX spectrum (no waterfall)** fed by the panadapter's
  spectrum data — **no audio dependency**, **no SWR/COMP/PWR/MIC sub-dials**.

Reference: `c:\dev\totw\` (Thetis On The Web) is the user's own single-file
vanilla-JS TCI client; it is the functional reference for the panadapter.

---

## Feature 1 — Scroll-wheel VFO tuning

**Where:** `RigPlugin.tsx` connected view, the frequency readout.

**Behavior:**
- Render the frequency as individually hoverable digit groups.
- **Wheel over a digit** → change that place value (1 Hz, 10 Hz, 100 Hz, 1 kHz,
  10 kHz, …). Wheel up = increase, down = decrease.
- **Wheel elsewhere in the panel** → ±1 of a configurable panel step
  (`settings.radio.scrollTuneStepHz`, default 100). (TCI has no standard
  tune-step query, so we expose our own step rather than read Thetis's.)
- Tuning calls the existing `signalRService.tuneToFrequency(hz)` →
  `LogHub.TuneToFrequency` → `SetFrequencyAsync` (TCI `vfo:` / Hamlib).
- Clamp to a sane HF/VHF range; debounce/throttle rapid wheel events (~30 ms) so
  we don't flood the rig.
- Prevent page scroll on wheel over the readout (`preventDefault`,
  non-passive listener).

**Config:** `scrollTuneStepHz` (number, default 100) in radio settings.

---

## Feature 2 — Startup auto-connect to last radio

**Where:** `RigPlugin.tsx`, `settingsStore`, `Settings.cs` radio settings.

**Behavior:**
- New persisted setting `settings.radio.reconnectLastOnStartup` (bool, default
  **true**).
- On a **successful** connect, if the setting is on, record the radio as the
  auto-connect target (reuse existing `autoConnectRigId`, set `autoReconnect = true`).
- On **manual** disconnect, clear targeting (existing behavior already sets
  `autoReconnect = false`).
- The existing startup effect already reconnects when
  `autoReconnect && autoConnectRigId` match — so the net effect is: connect a
  radio once → it reconnects automatically next launch, **unless you manually
  disconnected it** last session.
- The per-rig `RefreshCw` indicator already reflects this state (no new UI
  required, but add a checkbox for `reconnectLastOnStartup` in radio settings).

---

## Feature 3 — IQ panadapter (backend FFT → SignalR)

**Architecture:** keep today's data path shape (`SpectrumDataEvent` over
SignalR, drawn by `PanadapterPlugin.tsx`), but change the **source** from the
N1MM+ UDP listener to the **TCI IQ stream**.

**Backend (`TciRadioService` + new `TciIqSpectrumService`):**
- After TCI ready, enable IQ: send `iq_start;` (and set `iq_samplerate` to a
  supported rate, e.g. 48000). Thetis then streams **binary** frames on the same
  WebSocket.
- TCI binary frame = 64-byte little-endian header
  `{ uint32 receiver, uint32 sampleRate, uint32 format, uint32 codec,
     uint32 crc, uint32 length, uint32 type, uint32 reserved[9] }`
  followed by `length` float32 samples. `type`: 0 = IQ, 1 = RX audio,
  2 = TX chrono, 3/4 = TX audio. (Confirm exact layout against totw.)
- For `type == 0` (IQ) on the active receiver: accumulate interleaved I/Q
  float32 into a 4096-sample complex buffer.
- Apply a **Blackman-Harris window**, run a **radix-2 4096-pt FFT** (new
  `Dsp/Fft.cs`), compute magnitude → dBFS, fftshift so DC is centered, optional
  exponential averaging.
- Throttle (~15–20 fps), map to the existing `SpectrumDataEvent`
  (`data[]`, `lowFrequencyHz`, `highFrequencyHz`) using the rig's center
  frequency ± sampleRate/2, and broadcast `OnSpectrumData` over SignalR.

**Frontend:** unchanged rendering; update the "no data" hint to mention TCI IQ
instead of N1MM. Add configurable options surfaced from the panel/settings:
FFT averaging factor, reference level / dynamic-range scale, waterfall speed
(exists), color palette (exists/extend).

**Testing (no hardware needed):**
- `Fft` unit tests: impulse → flat magnitude; single-bin sinusoid → energy in
  the expected bin; Parseval sanity.
- TCI binary frame parser unit tests against hand-built byte arrays.
- Live Thetis validation flagged as a manual step (per project memory: verify
  against live services, don't assume fixtures).

**Coexistence:** the N1MM UDP listener can remain as a fallback source; a
setting (`settings.spectrum.source`: `'tci-iq' | 'n1mm'`, default `tci-iq`)
selects which feeds the panadapter. Only one broadcasts at a time.

---

## Feature 4 — TCI audio + Audio settings

> **DROPPED (2026-06-13).** The user decided against adding TCI audio. The
> audio-settings scaffold that briefly landed (`7ba9fe0`) was removed. The
> sections below are retained for historical context only.

**Default OFF.** Independent of the meter (meter uses spectrum, not audio).

**Audio settings section (`SettingsPanel`, new `settings.audio`):**
- `rxEnabled` (bool, default false) — stream RX audio to the browser speaker.
- `outputDeviceId` (string) — speaker, via `enumerateDevices()` (kind
  `audiooutput`) + `HTMLMediaElement.setSinkId`.
- `rxVolume` (0–100, default 80).
- `txEnabled` (bool, default false) — send browser mic to Thetis as TX audio.
- `inputDeviceId` (string) — mic, `getUserMedia({ audio: { deviceId } })`.
- `txGain` (0–100, default 50).
- `sampleRate` (enum 8000/24000/48000, default 48000).
- `analyzeRxWhenMuted` (bool, default true) — reserved hook (not needed now that
  the meter uses spectrum; kept for a future AF analyzer).

**RX path:** backend already receives TCI binary frames (Feature 3); for
`type == 1` (RX audio) relay PCM to the frontend (SignalR or a dedicated WS) and
play via Web Audio (`AudioContext` + `AudioBufferSourceNode`/worklet).

**TX path:** capture mic via `getUserMedia`, downsample to the TCI sample rate,
send TX audio frames to Thetis over TCI (`tx_audio` binary frames), gated by PTT.

**Testing:** device enumeration + settings persistence unit-testable; the live
TCI audio loop is a flagged manual validation step.

> Audio is the largest/most hardware-coupled piece. It may land as a follow-up
> commit after the panadapter, and its live behavior must be validated against a
> real Thetis instance.

---

## Feature 5 — Round S-meter + mini RX-spectrum widget

**Where:** Meter panel (`MeterPlugin.tsx`), new `RoundMeter` component.

**Behavior (SDRLoggerPlus style, functional parity with the reference image):**
- Round face. **Top arc** = VFO-A S-meter: S1→S9 then +20/+40/+60 dB; live
  needle/bar in accent color, **peak-hold** marker with slow decay; numeric
  dBm + S-unit readout.
- **Bottom arc** = VFO-B S-meter, shown only when **split** is active.
- **Center** = mini RX spectrum (no waterfall), drawn from the same
  `SpectrumDataEvent` feed as the panadapter, scaled into a small canvas.
- Center text: **mode**, **TX** indicator, **frequency**, filter width if
  available.
- **No SWR/COMP/PWR/MIC sub-dials.**

**Data:** S-meter (and TX/SWR/power if needed later) from the existing
`OnTciMeters` aggregator and `radioStates`; spectrum from `OnSpectrumData`.
Reuse `utils/smeter.ts` for dBm↔S-unit and the existing analog-meter ballistics.

**Config:** toggle peak-hold, toggle the second (split) arc, toggle the center
spectrum, dBm scale range.

---

## Cross-cutting

- **Branch:** `feature/panadapter-meters-audio`.
- **Settings contracts:** extend `Settings.cs` (`RadioSettings.scrollTuneStepHz`,
  `RadioSettings.reconnectLastOnStartup`, `SpectrumSettings.source`, new
  `AudioSettings`) and mirror in `settingsStore.ts` defaults + merge.
- **Order:** 1 → 2 → 5 → 3 → 4 (small/verifiable first; audio last).
- **Verification:** unit/build for everything; live-Thetis manual validation for
  IQ stream + audio (explicitly flagged — do not assume fixtures).

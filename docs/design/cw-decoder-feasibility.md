# CW Decoder — Feasibility Brief

**Date:** 2026-06-10 · Part of the [SDRLogger+ port roadmap](sdrloggerplus-port-roadmap.md)

## Question

Should SDRLoggerPlus decode received CW on screen, and if so, how? This is the one item from the SDRLogger+ feature set where implementation is **not** committed up front — this brief defines the spike; the decision gets made on its results.

## What SDRLogger+ does

SDRLogger+'s marketing says "decode CW," via the SDR application side (TCI exposes audio/IQ); the logger itself doesn't contain a DSP decode engine. Any SDRLoggerPlus implementation is therefore new work, not a port.

## Candidate input paths

1. **TCI audio stream** — TCI protocol can deliver RX audio over the existing `TciRadioService` WebSocket connection. Best path when the user runs EESDR/Thetis: no extra cabling, channel-selective, sample-accurate. Needs verification of which TCI audio commands `TciRadioService` already speaks vs. what must be added.
2. **System audio capture** (loopback/VAC) — works with any radio but adds device-selection UX and OS-specific capture (NAudio/WASAPI on Windows).
3. **WebAudio in the renderer** — capture + Goertzel in the Electron renderer; simplest plumbing, but ties decode to the UI process and to whatever audio device the OS exposes.

Recommendation to evaluate first: **(1) TCI audio**, falling back to (2) only if TCI audio support proves missing/painful.

## Candidate decode approaches

- **Own implementation:** Goertzel filter at detected tone pitch → envelope → adaptive dit/dah threshold (k-means on mark/space durations) → Morse table. Well-trodden; quality degrades in QSB/QRM; ~moderate effort in C# with no dependencies.
- **Port/embed an existing engine:** e.g. the `ggmorse` C++ library (MIT) via P/Invoke, or WebAssembly build in the renderer. Better decode quality for the hard cases; adds a native-dependency/packaging cost per platform.
- **Out of scope regardless:** ML-based decoders, multi-signal skimming (RBN already provides skimmed spots in SDRLoggerPlus).

## Spike plan (timeboxed)

1. Confirm what audio the current `TciRadioService` can request (read the service + TCI protocol doc). Deliverable: "audio available: yes/no/needs N commands".
2. Prototype offline: feed a recorded CW WAV through a C# Goertzel+adaptive-timing decoder in a test; measure copy accuracy at 15–30 WPM clean and with added noise.
3. Decision matrix: input path × decode engine, with packaging cost. Recommend implement / defer.

## Success criteria for "implement"

- ≥95 % character accuracy on clean 20 WPM test audio; graceful degradation (not garbage) at ~10 dB SNR.
- Input path requires no manual audio-cable setup for TCI users.
- No per-platform native build pain that complicates `npm run package:*`.

## Output

A short addendum to this doc with findings + go/no-go, then (if go) a full design spec like the other features.

---

## Spike findings (2026-06-10)

**1. TCI audio availability: NOT currently available.** `TciRadioService` (1,016 lines) implements the TCI text/CAT command set only — no `audio_start`/audio-packet binary stream handling exists. Adding TCI audio means implementing the binary frame protocol over the existing WebSocket: real but bounded new work (estimated as the larger half of the remaining effort).

**2. Decode algorithm: PROVEN.** `Services/Cw/CwDecoder.cs` (Goertzel 5 ms blocks → adaptive threshold with squelch → run-length dit/dah classification with median-based unit estimation) was tested against synthesized keyed audio (`CwDecoderTests`):

- **Exact copy** of "CQ TEST DE W8XYZ" at 15, 20, and 30 WPM clean — exceeds the ≥95 % criterion.
- **≥95 % character accuracy at ~12 dB SNR.**
- Graceful degradation in heavy noise; squelch returns empty output for silence and pure noise.

**3. Recommendation: GO, in two steps.**
- *Step A (when picked up):* implement TCI RX audio streaming in `TciRadioService`, feed the existing decoder in streaming form (the current implementation is whole-buffer; converting to streaming means carrying threshold/unit state between blocks — straightforward), add a CW Decoder panel.
- *Step B (optional later):* WASAPI loopback capture for non-TCI radios via NAudio.
- No native dependencies needed — the C# decoder is sufficient, so `npm run package:*` is unaffected.

The decoder ships in this commit as an experimental, fully tested unit with **no audio input wired** — it is inert until Step A connects it.

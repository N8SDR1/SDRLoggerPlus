# AI DX Coach — Design

**Status:** Draft for review (Rick + Brent)
**Owner:** TBD (touches Brent's AiService + the award/spot/solar subsystems)
**Goal:** Turn the data SDRLoggerPlus already has — live spots, your award needs,
solar/geomag, gray-line, and your rig's band/mode — into **proactive, personalized,
*factually grounded* operating suggestions**: *"AB4G on 20m FT8 — that's your last Nova
Scotia for 5BWAS. 40m to EU should open near sunset (~20 min)."*

Nothing else in the hobby does this well. The opportunity is real. The risk is equally real,
and this doc's main job is to lock in the architecture that avoids it.

---

## 1. The one principle that makes or breaks it

> **The engine owns the facts. The LLM owns the voice.**

Every valuable claim in the pitch is a *data* problem, not a *language* problem:

| Claim in a nudge | How it's actually derived | LLM's role |
|---|---|---|
| "AB4G on 20m FT8" | Read the live spot | none |
| "you need Nova Scotia for 5BWAS" | JOIN the spot against your award-needs matrix | none |
| "40m to EU opens near sunset in ~20 min" | Gray-line/terminator math + solar indices | none |
| Which of 12 matches to surface, and phrasing it warmly | ranking heuristic + LLM narration | **this, and only this** |

If we let an LLM *predict propagation* or *decide award needs*, it will hallucinate
confidently, be wrong within a session or two, and operators will disable it. If the LLM only
**narrates what a deterministic engine computed**, it's reliable *and* feels smart. That's the
whole ballgame.

**Non-goal:** the LLM must never be the source of a factual claim (a call, a band, a need, a
propagation window). It receives structured, already-true facts and rephrases/prioritizes them.

---

## 2. Inputs we already have

Almost all of this exists; the Coach is mostly a new **orchestration layer**, not new plumbing.

| Input | Source in the app |
|---|---|
| Live spots (call, band, freq, mode, spotter, dist/bearing) | Spot bus — DX cluster / RBN / SpotHole |
| Your award needs (worked vs needed, per band/mode) | Award trackers (DXCC, WAS, WAZ, WPX, WAC, 5BWAS, 5BDXCC, VUCC, POTA, IOTA) |
| Spotted station's location / state / grid | QRZ lookup + cty.dat fallback |
| Solar / geomag (SFI, K, A, SSN) | HamQSL feed + Propagation panel |
| Gray-line / terminator geometry | existing solar-calculations / gray-line overlay |
| Your rig's current band / mode | TCI / Hamlib / flrig radio state |
| Your QTH (grid) | Station settings |
| LLM plumbing, multi-provider (Ollama/Groq/OpenAI/…) | AiService + Chat AI |
| "Watch → alert" UX precedent | Hot List + band-opening announcer |

New pieces to build: the **Opportunity Engine**, a light **propagation gate**, the **Coach
Narrator** (LLM prompt contract), and the **delivery/throttle** layer + a Coach panel.

---

## 3. Architecture

```
        ┌───────────────────────────────────────────────────────────┐
        │  Opportunity Engine   (deterministic, backend)            │
        │                                                           │
  spots ─┤  for each fresh spot:                                    │
 awards ─┤    needs = matchAgainstAwardNeeds(spot, myAwardState)    │
   QRZ  ─┤    if needs is empty → drop                              │
  rig   ─┤    reachable = spot.band == rig.band? (soft weight)      │
        │    propScore = propagationGate(myGrid, spot, solar, gline)│
        │    score = f(needs.priority, propScore, distance, age)    │
        │  → ranked List<Opportunity> with explicit `reasons`       │
        └───────────────┬───────────────────────────────────────────┘
                        │  factual, ranked opportunities
             ┌──────────▼───────────┐         ┌─────────────────────┐
             │  Coach Narrator      │────────▶│  Delivery layer     │
             │  (LLM, opt-in)       │  text   │  panel · toast ·    │
             │  phrases top N       │         │  voice · throttle · │
             │  facts only in;      │         │  snooze · "why"     │
             │  prose out           │         └─────────────────────┘
             └──────────────────────┘
```

### 3.1 The needs-matrix join (the heart of it)

An **Opportunity** = a spot that would advance an award you haven't finished, given what the
spotted station *is*:

- Resolve the spotted call → DXCC entity, US state, CQ/ITU zone, grid, continent (QRZ, cty.dat).
- For each award tracker, ask: *does working this call, on this band/mode, fill a gap?*
  - **5BWAS** → need this **state** on this **band**? (band-specific)
  - **DXCC** → need this **entity** (or entity-on-band for band-DXCC)?
  - **WAZ / WAC / VUCC / WPX / POTA / IOTA** → the tracker's own gap test.
- Emit a structured reason per hit: `{ award: "5BWAS", gap: "NS on 20m", weight }`.

This is deterministic, unit-testable, offline, and free. **It is also useful entirely on its
own, with no AI** — see Phase 1.

### 3.2 Propagation gate (deterministic, honest)

A *soft* multiplier / annotation, never a hard fact the LLM invents:

- **Gray-line timing** is fully computable from the terminator math already in the app:
  "sunset on the my-QTH↔EU path in 22 min → 40m/80m gray-line enhancement window." Promise
  only this kind of *timed, geometric* statement.
- **Band-open likelihood** from SFI/K/A + distance + band → a coarse score (open / marginal /
  closed). Reuse the existing propagation heatmap logic where possible.
- Explicitly **out of scope for v1:** open-ended MUF forecasting / VOACAP-grade prediction.
  Overreaching here is how the Coach earns a reputation for being wrong. (Could be a later
  integration if we want real forecasting.)

### 3.3 Coach Narrator (LLM — the voice)

- Input: the **top N** opportunities as **structured JSON facts** + brief context (my grid,
  rig band, current solar summary). No free-form "figure out propagation."
- Output: one short, friendly nudge that prioritizes and phrases what's already true.
- Runs on the **existing multi-provider AiService** — ideally **Ollama (local, free)** or
  **Groq (free)** so proactive nudges cost nothing and stay private.
- Strict prompt contract (below) so it can't invent calls/bands/needs.

---

## 4. LLM prompt contract (draft)

**System:** *"You are a ham-radio operating coach. You will be given a JSON list of
opportunities that have ALREADY been computed and verified. Do not invent, infer, or change
any call sign, band, mode, award, or propagation claim — use only what's in the JSON. Pick the
1–2 best and write a single short, friendly nudge (max ~2 sentences). If the list is empty,
say nothing."*

**User (example):**
```json
{
  "myGrid": "EM79",
  "rigBand": "20m",
  "solar": { "sfi": 138, "k": 2, "summary": "quiet, good HF" },
  "opportunities": [
    { "call": "AB4G", "band": "20m", "mode": "FT8", "dxcc": "USA",
      "reasons": [{ "award": "5BWAS", "gap": "NS on 20m" }],
      "prop": { "state": "open", "grayline": null }, "distanceMi": 210, "score": 0.92 },
    { "call": "SP6X", "band": "40m", "mode": "CW", "dxcc": "Poland",
      "reasons": [{ "award": "DXCC", "gap": "Poland on 40m" }],
      "prop": { "state": "marginal", "grayline": "EU sunset in ~20 min" }, "score": 0.7 }
  ]
}
```
**Expected out:** *"AB4G's up on 20m FT8 — Nova Scotia, your last 5BWAS gap on the band, and
it's on your current band. Keep an ear on 40m too: SP6X (Poland, new on 40) as the EU gray line
opens in ~20."*

Everything in that sentence traces back to a JSON field. Nothing is model-invented.

---

## 5. Delivery & trust

Its entire value is being **right and rare**. (See the band-opening "WHAM" — same lesson.)

- **DX Coach panel** — the live ranked opportunity list (works with AI off), each row
  expandable to **"why"** (the exact reasons + prop basis). This is the trust anchor.
- **Nudges** — optional toast and/or voice, gated by: min score threshold, **per-award and
  global cooldowns**, dedup (don't re-nudge the same call/gap), quiet hours, and **snooze**.
- **Off by default**, clearly opt-in. Voice nudges reuse the new announcement-volume knob.
- **Transparency:** every nudge can show its receipts. No black-box "trust me."

---

## 6. Phasing

| Phase | Deliverable | AI? | Value |
|---|---|---|---|
| **1** | **Opportunity Engine + panel** — "spots that fill your needs," ranked, with reasons | ❌ | Killer feature on its own; zero AI risk; fully testable |
| **2** | **Propagation gate** — gray-line timing + SFI/K band-open scoring annotations | ❌ | Sharper ranking + the "opens in ~20 min" claims (honest ones) |
| **3** | **Coach Narrator** — LLM phrases top-N into nudges via AiService (Ollama/Groq) | ✅ | The "coach voice"; proactive nudges |
| **4** | **Polish** — throttle/snooze/quiet-hours, dedup, "why" UI, voice + volume, off-by-default | ✅ | Right-and-rare; keeps it from becoming a nag |

**Ship Phase 1 first.** It proves the data model, is immediately useful, and de-risks
everything after it. The LLM is the last layer, not the foundation.

---

## 7. Risks & non-goals

- **Hallucinated facts** → mitigated by the engine-owns-facts split + strict prompt contract.
- **Over-promised propagation** → v1 only claims *timed geometric* (gray-line) + coarse
  band-open scores; no VOACAP-grade forecasting.
- **Nagging / alert fatigue** → cooldowns, dedup, thresholds, snooze, off-by-default.
- **Cost/latency** → event-driven (only when the engine finds a match), and local/free
  providers make proactive nudges free.
- **Accuracy of the needs matrix** depends on award-tracker correctness — reuse the existing
  trackers as the single source of truth; don't recompute needs in the Coach.

---

## 8. Open questions

1. Backend service (C#) for the engine vs frontend — leaning **backend** (owns spots + awards
   state, can run without the panel open, feeds a SignalR event like `OnCoachOpportunity`).
2. How much award state is already queryable per (entity/state/zone × band × mode)? Any gaps to
   fill in the trackers to answer "is this a need?" cleanly.
3. Nudge cadence + defaults (thresholds, cooldowns, quiet hours).
4. Which awards to include in v1 (start with **DXCC + 5BWAS/WAS** — highest-signal, clearest
   need tests — then widen).
5. Do we want a manual **"Coach, what should I chase right now?"** button (pull) in addition to
   proactive nudges (push)? (Cheap to add; nice for the ChatAI panel.)

---

*Built on existing subsystems: AiService/Chat AI, award trackers, the spot bus, HamQSL solar +
propagation panel, gray-line/terminator math, and radio state. The Coach is orchestration, not
new plumbing.*

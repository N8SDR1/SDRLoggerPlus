# Globe Day/Night + Grey Line — Design Spec

**Date:** 2026-07-07
**Base:** `v2-alpha` (branch `feat/globe-day-night`)
**Status:** Approved design, pending implementation plan
**Phase:** the day/night globe toggle deferred from the globe-lightning-strikes spec (2026-07-05).

## Summary

Render the day/night terminator on the 3D globe as two independently toggleable
layers: a translucent **night shade** over the dark hemisphere and a soft glowing
**grey-line band** along the terminator (the twilight propagation zone). Rendered
by a single custom three.js shader shell added directly to the globe scene — no
globe.gl data layers are used. Wired to the **existing but currently dead**
Settings → Map controls (`showDayNightOverlay`, `showGrayLine`, and their opacity
sliders), plus a sun/moon toggle button on the globe panel.

## Goals

- Show where night is on the 3D globe, and the grey line, as the operator toggles.
- Revive the existing dead Settings → Map day/night + gray-line checkboxes and
  opacity sliders (they exist end-to-end through the backend but render nothing).
- Keep the co-owned `GlobePlugin.tsx` footprint minimal: one thin effect, logic in
  a new isolated module (same pattern as the lightning strike layer).

## Non-Goals (YAGNI)

- **No** sun/moon position markers (their checkboxes stay dead; own pass later).
- **No** 2D-map overlays (`DayNightOverlay.tsx` / `GrayLineOverlay.tsx` stay
  orphaned; not this feature's problem).
- **No** per-frame terminator animation — a 60 s sun-position refresh is plenty
  (the terminator moves 0.25°/minute).
- **No** new settings and **no** backend changes — every needed setting already
  exists in `MapSettings` (frontend and backend).

## Context / Findings

- The globe surface is **Google satellite tiles** (`globeTileEngineUrl`), not a
  single texture — so day/night cannot be done by blending a night texture into
  the surface. It must be an overlay above the surface.
- `utils/solarCalculations.ts` already provides tested `getSunPosition()`
  (subsolar point), `getMoonPosition()`, and `isInGrayLine()`.
- `MapSettings` already has `showDayNightOverlay`, `showGrayLine`,
  `dayNightOpacity` (0.5), `grayLineOpacity` (0.6) in both the frontend store and
  the backend model, with working Settings → Map UI — but **no consumer renders
  anything** (v1-port leftovers). This feature claims them.
- globe.gl data layers are spoken for: `polygonsData` = rotator beam,
  `ringsData`/`customLayerData` = lightning, `pointsData`/`arcsData` = markers.
  Rejected approaches: terminator polygon via `polygonsData` (owner collision +
  the documented large-polygon sag) and a pre-rendered image overlay (no clean
  second image layer above tiles). Hence the scene-level shader shell.

## Architecture

### 1. `src/SDRLoggerPlus.Web/src/utils/dayNightShell.ts` (new module)

Owns the three.js objects; no React. Exports:

- `createDayNightShell(THREE, globeRadius)` → `{ mesh, setSunDirection(x,y,z),
  setOpacities(night, gray), dispose() }`
- Mesh: `SphereGeometry(globeRadius * 1.004, 96, 48)` with a `ShaderMaterial`:
  - Uniforms: `uSunDir` (vec3, normalized), `uNightOpacity`, `uGrayOpacity`.
  - Fragment: `sunDot = dot(normalize(vWorldNormal), uSunDir)`.
    - Night shade: dark blue-black, alpha = `uNightOpacity *
      smoothstep(-0.05, 0.15, -sunDot)` — 0 in daylight, 1 in deep night,
      fading in across the twilight zone instead of a hard edge. (Edges
      ascending — GLSL `smoothstep` is undefined for reversed edges.)
    - Grey line: warm grey-white glow, alpha = `uGrayOpacity *
      (1 - smoothstep(0.0, 0.09, abs(sunDot)))` — peaks at the terminator,
      spanning roughly the ±5° solar-altitude twilight band.
    - Final color = additive combination; `transparent: true`,
      `depthWrite: false`.
  - `mesh.raycast = () => {}` so globe clicks (rotator beam setting) pass through.
  - `setOpacities(0, 0)` sets `mesh.visible = false` (zero cost when off).
- `dispose()` frees geometry + material.

### 2. GlobePlugin wiring (one thin effect)

- Gated on `globeReady`; reads `settings.map.showDayNightOverlay`,
  `showGrayLine`, `dayNightOpacity`, `grayLineOpacity`.
- On first run: `create`, add mesh to `globe.scene()`.
- Every run: `setOpacities(showDayNightOverlay ? dayNightOpacity : 0,
  showGrayLine ? grayLineOpacity : 0)`.
- Sun direction: `getSunPosition(new Date())` → `globe.getCoords(lat, lon, 0)`
  normalized (using the globe's own coordinate frame avoids any axis-convention
  mismatch) → `setSunDirection`; refreshed by a 60 s interval while either layer
  is on.
- Cleanup: clear interval; on unmount remove mesh from scene and `dispose()`.

### 3. Globe overlay button

- Sun/moon icon (lucide), top-right button row next to the lightning ⚡ button.
- Flips `showDayNightOverlay` via `updateMapSettings` **and calls
  `saveSettings()` explicitly** (lesson from the trackRig persistence bug).
- Active state styled like the lightning button's (icon tint when on).
- Grey line has no globe button — Settings → Map checkbox only.

### 4. Settings

None added. The existing Settings → Map checkboxes/sliders drive the layers
through the store as-is; the backend `MapSettings` fields already persist them.

## Data Flow

```
Settings → Map checkboxes / globe ☀ button
        └─ settings.map.{showDayNightOverlay, showGrayLine, *Opacity}
                    ▼ (one GlobePlugin effect)
getSunPosition(now) ──60 s──► dayNightShell.setSunDirection / setOpacities
                    ▼
        shader shell mesh in globe.scene()  (night shade + grey-line band)
```

## Error / Edge Handling

- Globe re-init (WebGL context loss/recreate): the effect re-runs on
  `globeReady`, recreating the shell against the new scene.
- Both layers off → mesh invisible; no shader cost, no interval.
- Shell must never intercept pointer events (no-op raycast) and never occlude
  other layers (`depthWrite: false`; rings/paths/labels render above).
- Poles/antimeridian need no special handling — the shader evaluates per-pixel
  on a sphere (this is the reason the polygon approach was rejected).

## Testing

- **Unit (Vitest):** `dayNightShell` uniform/visibility logic with a minimal
  THREE stub — opacities map to uniforms, both-zero hides the mesh, sun
  direction normalizes. (`solarCalculations` is already tested.)
- **Manual:** terminator matches reality (compare against current UTC —
  night side over the correct hemisphere); sliders change intensity live;
  each checkbox kills exactly its own layer; ☀ button toggles + persists
  across reload; globe clicks still set the beam; lightning rings/bolts and
  DX arcs render above the shade.

## Files (indicative — the plan will finalize)

| File | Change |
|---|---|
| `src/SDRLoggerPlus.Web/src/utils/dayNightShell.ts` (+ test) | new shader-shell module |
| `src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx` | one effect + ☀ toggle button |

## Coordination Note

`GlobePlugin.tsx` is co-owned with Rick and changes frequently. This feature
touches it in exactly two places (the effect, the button) and claims **no**
globe.gl data layer — the shell is a plain scene child, so it cannot collide
with the beam/lightning/marker layers.

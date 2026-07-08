# Globe Day/Night + Grey Line Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render the day/night terminator (translucent night shade) and grey-line band on the 3D globe as two independently toggleable layers, driven by the existing-but-dead Settings → Map controls plus a new ☀ globe button.

**Architecture:** One new pure module (`dayNightShell.ts`) builds a shader sphere slightly above the globe surface — night/grey-line alphas computed per-pixel from the sun direction. `GlobePlugin.tsx` creates the shell once at globe init (plain scene child; **no globe.gl data layer**) and drives its uniforms from settings in one thin effect. Sun position from the already-tested `getSunPosition()`.

**Tech Stack:** React 18, three.js (already a dependency, dynamically imported in GlobePlugin), globe.gl scene access, Vitest.

**Spec:** `docs/superpowers/specs/2026-07-07-globe-day-night-design.md`

## Global Constraints

- Branch: `feat/globe-day-night` off `v2-alpha` (already created; spec committed on it).
- Shell geometry: `SphereGeometry(globeRadius * 1.004, 96, 48)`; globe.gl's radius is **100** world units.
- Fragment math (exact): `sunDot = dot(normalize(vWorldNormal), uSunDir)`; night alpha = `uNightOpacity * smoothstep(-0.05, 0.15, -sunDot)`; grey alpha = `uGrayOpacity * (1.0 - smoothstep(0.0, 0.09, abs(sunDot)))`. GLSL smoothstep edges must be ascending.
- Colors: night `vec3(0.01, 0.03, 0.08)` (dark blue-black); grey line `vec3(0.78, 0.75, 0.66)` (warm grey).
- Material: `transparent: true`, `depthWrite: false`; mesh `raycast = () => {}` (never intercept globe clicks); `renderOrder = 1`; hidden (`visible = false`) whenever both opacities are 0.
- Sun direction refresh: every **60 s** while either layer is on; computed via `getSunPosition(new Date())` → `globe.getCoords(lat, lon, 0)` → normalized in JS.
- Settings: **no new settings, no backend changes.** Consume existing `settings.map.showDayNightOverlay`, `showGrayLine`, `dayNightOpacity` (default 0.5), `grayLineOpacity` (default 0.6).
- The ☀ button toggles `showDayNightOverlay` only and MUST call `saveSettings()` explicitly after `updateMapSettings` (trackRig lesson). Grey line is Settings-checkbox only.
- Do not touch any globe.gl data layer (`polygonsData`/`ringsData`/`customLayerData`/`pointsData`/`arcsData`) or the lightning/beam/marker code.
- tsc gate: the only acceptable errors are the two PRE-EXISTING `GlobePlugin.tsx` TS2345 casts (`pathDashLength`/`pathDashGap`).

---

### Task 1: `dayNightShell` module (pure, unit-tested)

**Files:**
- Create: `src/SDRLoggerPlus.Web/src/utils/dayNightShell.ts`
- Test: `src/SDRLoggerPlus.Web/src/utils/dayNightShell.test.ts`

**Interfaces:**
- Consumes: nothing (a `ThreeLike` structural type keeps three.js stubable).
- Produces: `createDayNightShell(three: ThreeLike, globeRadius: number): DayNightShell` where `DayNightShell = { mesh: SceneObject; setSunDirection(x,y,z): void; setOpacities(night, gray): void; dispose(): void }`.

- [ ] **Step 1: Write the failing test**

Create `src/SDRLoggerPlus.Web/src/utils/dayNightShell.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import { createDayNightShell, type ThreeLike } from './dayNightShell';

type Uniforms = Record<string, { value: unknown }>;

function stubThree() {
  const calls = {
    geomArgs: [] as number[],
    geomDisposed: false,
    matDisposed: false,
    uniforms: null as Uniforms | null,
    sunSet: [] as number[][],
  };
  const three: ThreeLike = {
    SphereGeometry: class {
      constructor(radius: number, w: number, h: number) { calls.geomArgs = [radius, w, h]; }
      dispose() { calls.geomDisposed = true; }
    },
    ShaderMaterial: class {
      uniforms: Uniforms;
      constructor(params: { uniforms: Uniforms }) { this.uniforms = params.uniforms; calls.uniforms = params.uniforms; }
      dispose() { calls.matDisposed = true; }
    },
    Vector3: class {
      constructor(public x = 0, public y = 0, public z = 0) {}
      set(x: number, y: number, z: number) { this.x = x; this.y = y; this.z = z; calls.sunSet.push([x, y, z]); return this; }
    },
    Mesh: class {
      visible = true;
      renderOrder = 0;
      raycast: (...args: unknown[]) => void = () => { throw new Error('not replaced'); };
      constructor(public geometry: object, public material: object) {}
    },
  } as unknown as ThreeLike;
  return { three, calls };
}

describe('createDayNightShell', () => {
  it('builds a hidden shell slightly above the globe surface', () => {
    const { three, calls } = stubThree();
    const shell = createDayNightShell(three, 100);
    expect(calls.geomArgs[0]).toBeCloseTo(100.4);           // 1.004 × radius
    expect((shell.mesh as { visible: boolean }).visible).toBe(false);
    expect(() => (shell.mesh as { raycast: () => void }).raycast()).not.toThrow(); // no-op raycast installed
  });

  it('maps opacities to uniforms and visibility', () => {
    const { three, calls } = stubThree();
    const shell = createDayNightShell(three, 100);
    shell.setOpacities(0.5, 0.6);
    expect(calls.uniforms!.uNightOpacity.value).toBe(0.5);
    expect(calls.uniforms!.uGrayOpacity.value).toBe(0.6);
    expect((shell.mesh as { visible: boolean }).visible).toBe(true);
    shell.setOpacities(0, 0);
    expect((shell.mesh as { visible: boolean }).visible).toBe(false);
  });

  it('normalizes the sun direction', () => {
    const { three, calls } = stubThree();
    const shell = createDayNightShell(three, 100);
    shell.setSunDirection(3, 0, 4);
    const last = calls.sunSet.at(-1)!;
    expect(last[0]).toBeCloseTo(0.6);
    expect(last[1]).toBeCloseTo(0);
    expect(last[2]).toBeCloseTo(0.8);
  });

  it('dispose frees geometry and material', () => {
    const { three, calls } = stubThree();
    createDayNightShell(three, 100).dispose();
    expect(calls.geomDisposed).toBe(true);
    expect(calls.matDisposed).toBe(true);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd src/SDRLoggerPlus.Web && npx vitest run src/utils/dayNightShell.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

Create `src/SDRLoggerPlus.Web/src/utils/dayNightShell.ts`:

```ts
// Day/night terminator shell for the 3D globe: one transparent sphere just
// above the surface whose fragment shader shades the night hemisphere and
// glows along the grey line. Added straight to the three.js scene — it uses
// NO globe.gl data layer, so it cannot collide with the beam/lightning/marker
// layers. Pure module: three.js is injected (ThreeLike), so it unit-tests
// without WebGL.

export interface SceneObject {
  visible: boolean;
  renderOrder: number;
  raycast: (...args: unknown[]) => void;
}

export interface ThreeLike {
  SphereGeometry: new (radius: number, widthSegments: number, heightSegments: number) => { dispose(): void };
  ShaderMaterial: new (params: {
    uniforms: Record<string, { value: unknown }>;
    vertexShader: string;
    fragmentShader: string;
    transparent: boolean;
    depthWrite: boolean;
  }) => { uniforms: Record<string, { value: unknown }>; dispose(): void };
  Vector3: new (x?: number, y?: number, z?: number) => { set(x: number, y: number, z: number): unknown };
  Mesh: new (geometry: object, material: object) => SceneObject;
}

export interface DayNightShell {
  mesh: SceneObject;
  setSunDirection(x: number, y: number, z: number): void;
  setOpacities(night: number, gray: number): void;
  dispose(): void;
}

const VERTEX_SHADER = `
varying vec3 vWorldNormal;
void main() {
  vWorldNormal = normalize(mat3(modelMatrix) * normal);
  gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
}
`;

const FRAGMENT_SHADER = `
uniform vec3 uSunDir;
uniform float uNightOpacity;
uniform float uGrayOpacity;
varying vec3 vWorldNormal;
void main() {
  float sunDot = dot(normalize(vWorldNormal), uSunDir);
  // Night shade: 0 in daylight, full in deep night, soft across twilight.
  float night = uNightOpacity * smoothstep(-0.05, 0.15, -sunDot);
  // Grey line: peaks at the terminator (|solar altitude| within ~5 deg).
  float gray = uGrayOpacity * (1.0 - smoothstep(0.0, 0.09, abs(sunDot)));
  float alpha = clamp(night + gray, 0.0, 1.0);
  if (alpha < 0.003) discard;
  vec3 nightColor = vec3(0.01, 0.03, 0.08);
  vec3 grayColor  = vec3(0.78, 0.75, 0.66);
  vec3 color = (nightColor * night + grayColor * gray) / max(night + gray, 0.001);
  gl_FragColor = vec4(color, alpha);
}
`;

export function createDayNightShell(three: ThreeLike, globeRadius: number): DayNightShell {
  const geometry = new three.SphereGeometry(globeRadius * 1.004, 96, 48);
  const material = new three.ShaderMaterial({
    uniforms: {
      uSunDir: { value: new three.Vector3(1, 0, 0) },
      uNightOpacity: { value: 0 },
      uGrayOpacity: { value: 0 },
    },
    vertexShader: VERTEX_SHADER,
    fragmentShader: FRAGMENT_SHADER,
    transparent: true,
    depthWrite: false,
  });
  const mesh = new three.Mesh(geometry, material);
  mesh.visible = false;
  mesh.renderOrder = 1;      // above the tiled surface
  mesh.raycast = () => {};   // never intercept globe clicks (beam setting)

  return {
    mesh,
    setSunDirection(x, y, z) {
      const len = Math.hypot(x, y, z) || 1;
      (material.uniforms.uSunDir.value as { set(x: number, y: number, z: number): unknown })
        .set(x / len, y / len, z / len);
    },
    setOpacities(night, gray) {
      material.uniforms.uNightOpacity.value = night;
      material.uniforms.uGrayOpacity.value = gray;
      mesh.visible = night > 0 || gray > 0;
    },
    dispose() {
      geometry.dispose();
      material.dispose();
    },
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd src/SDRLoggerPlus.Web && npx vitest run src/utils/dayNightShell.test.ts`
Expected: PASS — 4 passing.

- [ ] **Step 5: Commit**

```bash
git add src/SDRLoggerPlus.Web/src/utils/dayNightShell.ts src/SDRLoggerPlus.Web/src/utils/dayNightShell.test.ts
git commit -m "feat(globe): day/night terminator shader shell (pure module)"
```

---

### Task 2: GlobePlugin integration — create shell at init, drive from settings

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx`

**Interfaces:**
- Consumes: `createDayNightShell`/`DayNightShell` (Task 1); `getSunPosition` from `../utils/solarCalculations`; `settings.map.showDayNightOverlay/showGrayLine/dayNightOpacity/grayLineOpacity` (already in the store).
- Produces: a working shell in the scene, hidden until settings enable it.

- [ ] **Step 1: Imports and interface**

Add with the other util imports at the top:

```ts
import { createDayNightShell, type DayNightShell } from '../utils/dayNightShell';
import { getSunPosition } from '../utils/solarCalculations';
```

In `interface GlobeInstance`, the `scene()` declaration (line ~247) currently exposes only `traverse`. Widen it:

```ts
  scene(): {
    add(obj: object): void;
    remove(obj: object): void;
    traverse(cb: (obj: unknown) => void): void;
  };
```

- [ ] **Step 2: Shell ref**

Next to `const strikeStoreRef = useRef(new StrikeStore());` add:

```ts
  const dayNightShellRef = useRef<DayNightShell | null>(null);
```

- [ ] **Step 3: Create the shell at globe init**

In the globe-init chain, directly after the lightning bolt custom-layer config block (it ends with `.customLayerData([]);`, line ~871) and before `globeRef.current = globe;`:

```ts
      // Day/night terminator shell — a plain scene child (no globe.gl data
      // layer, so it cannot collide with beam/lightning/marker layers).
      // Hidden until the settings effect enables a layer. globe.gl's globe
      // radius is 100 world units.
      const dayNightShell = createDayNightShell(THREE, 100);
      globe.scene().add(dayNightShell.mesh);
      dayNightShellRef.current = dayNightShell;
```

(`THREE` is already in scope in the init function — it is captured by `THREE = await import('three')` at the top of the init.)

In the SAME init effect's cleanup function (the `return () => { ... }` that sets the init-cancelled flag / tears the globe down — locate it at the end of that effect), add:

```ts
      dayNightShellRef.current?.dispose();
      dayNightShellRef.current = null;
```

- [ ] **Step 4: Settings-driven effect**

Add after the lightning-strikes effect (the one ending `}, [globeReady, settings.map.showLightning]);`):

```ts
  // Day/night terminator + grey line → shader shell uniforms. The shell is
  // created at globe init; this effect drives it from settings and refreshes
  // the sun direction (terminator moves 0.25°/min — 60 s is plenty).
  useEffect(() => {
    if (!globeReady || !globeRef.current) return;
    const shell = dayNightShellRef.current;
    if (!shell) return;

    const night = settings.map.showDayNightOverlay ? settings.map.dayNightOpacity : 0;
    const gray = settings.map.showGrayLine ? settings.map.grayLineOpacity : 0;
    shell.setOpacities(night, gray);
    if (night <= 0 && gray <= 0) return;

    const updateSun = () => {
      if (!globeRef.current) return;
      const sun = getSunPosition(new Date());
      const p = globeRef.current.getCoords(sun.lat, sun.lon, 0);
      shell.setSunDirection(p.x, p.y, p.z);
    };
    updateSun();
    const interval = setInterval(updateSun, 60_000);
    return () => clearInterval(interval);
  }, [globeReady, settings.map.showDayNightOverlay, settings.map.showGrayLine, settings.map.dayNightOpacity, settings.map.grayLineOpacity]);
```

- [ ] **Step 5: Typecheck**

Run: `cd src/SDRLoggerPlus.Web && npx tsc --noEmit`
Expected: ONLY the two pre-existing `GlobePlugin.tsx` TS2345 errors (`pathDashLength`/`pathDashGap`). Any other error is yours — fix it.

- [ ] **Step 6: Commit**

```bash
git add src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx
git commit -m "feat(globe): wire day/night shell to settings + 60s sun refresh"
```

---

### Task 3: ☀ toggle button + manual verification

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx`

**Interfaces:**
- Consumes: `updateMapSettings`, `saveSettings` (settings store), `SunMoon` icon (lucide-react).

- [ ] **Step 1: Store + icon imports**

Line ~268, widen the destructure (NOTE: PR #6 touches this same line on another branch — a trivial merge conflict is expected and fine):

```ts
  const { settings, updateMapSettings, saveSettings } = useSettingsStore();
```

Add `SunMoon` to the lucide-react import on line 2.

- [ ] **Step 2: Button**

Directly after the lightning ⚡ button block (the `{!hideOverlays && (...)}` whose button has `aria-label` "Show/Hide lightning strikes", positioned `right-16`), add a third button one slot further left:

```tsx
        {/* Day/night terminator toggle (Top Right, left of lightning) */}
        {!hideOverlays && (
          <button
            onClick={() => { updateMapSettings({ showDayNightOverlay: !settings.map.showDayNightOverlay }); saveSettings(); }}
            className={`glass-button absolute top-4 right-28 p-2 z-10 ${settings.map.showDayNightOverlay ? 'text-amber-300' : ''}`}
            title={settings.map.showDayNightOverlay ? 'Hide day/night shading' : 'Show day/night shading'}
            aria-label={settings.map.showDayNightOverlay ? 'Hide day/night shading' : 'Show day/night shading'}
          >
            <SunMoon className="w-4 h-4" />
          </button>
        )}
```

- [ ] **Step 3: Typecheck**

Run: `cd src/SDRLoggerPlus.Web && npx tsc --noEmit`
Expected: only the two pre-existing errors.

- [ ] **Step 4: Full frontend test run**

Run: `cd src/SDRLoggerPlus.Web && npx vitest run`
Expected: all pass except the KNOWN pre-existing `AboutDialog.test.tsx` failure (baseline on v2-alpha — not yours).

- [ ] **Step 5: Manual verification (dev servers running)**

1. Open the app, find the 3D Globe panel: a ☀ button sits left of ⚡.
2. Click ☀ → night hemisphere darkens; the terminator's position matches reality (check against current UTC — at ~02:00 UTC the Americas are in daylight/evening, Europe/Africa dark... verify against an online day/night map if unsure).
3. Settings → Map → Day/Night opacity slider changes shade intensity live.
4. Settings → Map → Gray Line checkbox adds the glowing terminator band; its opacity slider works; unchecking kills only that band.
5. Toggle ☀ off → shade gone; reload the page → toggle state persisted.
6. Click the globe → beam still sets (shell doesn't block clicks); lightning rings/bolts and DX arcs render above the shade.

- [ ] **Step 6: Commit**

```bash
git add src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx
git commit -m "feat(globe): day/night toggle button (persists via explicit save)"
```

---

## Self-Review Notes

- **Spec coverage:** shader shell module (Task 1) ✔; scene child + settings-driven uniforms + 60 s sun refresh + init/dispose lifecycle (Task 2) ✔; ☀ button with explicit save + grey-line-via-Settings-only (Task 3) ✔; no new settings / no backend / no globe.gl layers (all tasks) ✔; manual protocol covers the spec's testing section ✔.
- **Type consistency:** `DayNightShell`/`ThreeLike`/`SceneObject` defined once in Task 1 and imported in Task 2; `createDayNightShell(THREE, 100)` matches the exported signature; effect reads the four settings keys exactly as named in the store.
- **No placeholders:** every code step carries the full code; the only locate-by-pattern steps (init-effect cleanup, ⚡ button block) name unambiguous anchors in a file the implementer must read anyway.

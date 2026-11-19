// Lightning-strike ripples for the 3D globe: one expanding white circle per
// strike, re-rippling every RIPPLE_PERIOD_MS while the strike is shown.
//
// Why this is not globe.gl's ringsData: three-globe's rings layer creates a
// new circle mesh plus a tween for every ripple and relies on its tween group
// dropping finished tweens. With @tweenjs/tween.js 24+ (what npm resolves for
// globe.gl / three-globe) Group.update() keeps finished tweens by default, and
// on ≤23 they stay in the global group — either way every ripple's mesh,
// material and geometry stayed in memory forever (~100+ MB a minute in a
// lightning storm, measured 2026-11-18). This layer owns its objects: one
// circle per strike, created once, animated in place, disposed when the strike
// leaves. Nothing is allocated per ripple.
//
// Pure module: three.js is injected (ThreeLike), so it unit-tests without WebGL.

export interface RippleStrike {
  lat: number;
  lng: number;
  /** Within the operator's alert range — drawn larger and brighter. */
  local: boolean;
  /** Strike time (ms since epoch) — older strikes draw dimmer. */
  ts: number;
}

/** A new ripple starts this often for each strike. */
export const RIPPLE_PERIOD_MS = 2000;
/** Ripple growth speed, degrees of arc per second. */
export const RIPPLE_SPEED_DEG_PER_S = 2;
/** Ripple height above the surface, in globe radii. */
export const RIPPLE_ALTITUDE = 0.006;
/** Points around each circle. */
export const RIPPLE_SEGMENTS = 32;

export function rippleMaxRadiusDeg(local: boolean): number {
  return local ? 3.5 : 2.5;
}

/** Strike-age brightness: full for the first minute, then dimmer. */
export function rippleAgeFactor(strikeTs: number, nowMs: number): number {
  const ageMin = (nowMs - strikeTs) / 60_000;
  return ageMin < 1 ? 1 : ageMin < 5 ? 0.7 : 0.45;
}

export interface RippleFrame {
  visible: boolean;
  /** Current ripple radius, degrees of arc. */
  radiusDeg: number;
  opacity: number;
}

/**
 * Where a strike's ripple is at `nowMs`. The ripple restarts every
 * RIPPLE_PERIOD_MS from `bornMs` (when the strike was first drawn), grows to
 * its max radius at RIPPLE_SPEED_DEG_PER_S while fading out, then stays hidden
 * until the next period starts.
 */
export function rippleFrame(strike: RippleStrike, bornMs: number, nowMs: number): RippleFrame {
  const maxRadius = rippleMaxRadiusDeg(strike.local);
  const durationMs = (maxRadius / RIPPLE_SPEED_DEG_PER_S) * 1000;
  const elapsed = Math.max(0, nowMs - bornMs) % RIPPLE_PERIOD_MS;
  const t = elapsed / durationMs;
  if (t > 1) return { visible: false, radiusDeg: 0, opacity: 0 };
  const opacity = (1 - t) * rippleAgeFactor(strike.ts, nowMs) * (strike.local ? 1 : 0.6);
  return { visible: opacity > 0, radiusDeg: t * maxRadius, opacity };
}

interface Vec3 { x: number; y: number; z: number }

export interface RippleObject {
  visible: boolean;
  renderOrder: number;
  raycast: (...args: unknown[]) => void;
  position: { set(x: number, y: number, z: number): unknown; z: number };
  scale: { set(x: number, y: number, z: number): unknown };
  add(child: object): unknown;
  remove(child: object): unknown;
  lookAt(x: number, y: number, z: number): void;
}

export interface ThreeLike {
  BufferGeometry: new () => { setAttribute(name: string, attr: object): unknown; dispose(): void };
  Float32BufferAttribute: new (array: number[], itemSize: number) => object;
  LineBasicMaterial: new (params: { color: number; transparent: boolean; depthWrite: boolean; opacity: number }) => {
    opacity: number;
    dispose(): void;
  };
  LineLoop: new (geometry: object, material: object) => RippleObject;
  Group: new () => RippleObject;
}

export interface LightningRipples {
  /** Add to the globe scene once. */
  group: RippleObject;
  /** Replace the drawn strikes. Same object = same ripple (diffed by identity). */
  setStrikes(strikes: readonly RippleStrike[], nowMs: number): void;
  /** Move every ripple to its position at `nowMs`. Call once per frame. */
  update(nowMs: number): void;
  /** Strikes currently drawn. */
  readonly count: number;
  dispose(): void;
}

interface Entry {
  strike: RippleStrike;
  bornMs: number;
  holder: RippleObject;
  circle: RippleObject;
  material: { opacity: number; dispose(): void };
}

/**
 * @param globeRadius globe.gl's globe radius in world units (100).
 * @param toCoords globe.getCoords — lat/lng/altitude (in globe radii) to world xyz.
 */
export function createLightningRipples(
  three: ThreeLike,
  globeRadius: number,
  toCoords: (lat: number, lng: number, altitude: number) => Vec3,
): LightningRipples {
  // One shared unit circle in the XY plane; each ripple scales it.
  const points: number[] = [];
  for (let i = 0; i < RIPPLE_SEGMENTS; i++) {
    const a = (i / RIPPLE_SEGMENTS) * Math.PI * 2;
    points.push(Math.cos(a), Math.sin(a), 0);
  }
  const geometry = new three.BufferGeometry();
  geometry.setAttribute('position', new three.Float32BufferAttribute(points, 3));

  const group = new three.Group();
  group.raycast = () => {};
  const entries = new Map<RippleStrike, Entry>();
  const curveR = globeRadius * (1 + RIPPLE_ALTITUDE);

  const remove = (entry: Entry) => {
    group.remove(entry.holder);
    entry.material.dispose();
    entries.delete(entry.strike);
  };

  return {
    group,
    get count() { return entries.size; },

    setStrikes(strikes, nowMs) {
      const keep = new Set(strikes);
      for (const entry of [...entries.values()]) if (!keep.has(entry.strike)) remove(entry);
      for (const strike of strikes) {
        if (entries.has(strike)) continue;
        const material = new three.LineBasicMaterial({ color: 0xffffff, transparent: true, depthWrite: false, opacity: 0 });
        const circle = new three.LineLoop(geometry, material);
        circle.raycast = () => {};
        circle.visible = false;
        // Holder sits on the surface point facing the globe centre, so the
        // circle lies flat on the sphere; the circle drops toward the centre
        // as it widens to follow the curvature.
        const holder = new three.Group();
        holder.raycast = () => {};
        const p = toCoords(strike.lat, strike.lng, RIPPLE_ALTITUDE);
        holder.position.set(p.x, p.y, p.z);
        holder.lookAt(0, 0, 0);
        holder.add(circle);
        group.add(holder);
        entries.set(strike, { strike, bornMs: nowMs, holder, circle, material });
      }
    },

    update(nowMs) {
      for (const entry of entries.values()) {
        const f = rippleFrame(entry.strike, entry.bornMs, nowMs);
        entry.circle.visible = f.visible;
        if (!f.visible) continue;
        const ang = (f.radiusDeg * Math.PI) / 180;
        const r = curveR * Math.sin(ang);
        entry.circle.scale.set(r || 1e-6, r || 1e-6, 1);
        entry.circle.position.z = curveR * (1 - Math.cos(ang));
        entry.material.opacity = f.opacity;
      }
    },

    dispose() {
      for (const entry of [...entries.values()]) remove(entry);
      geometry.dispose();
    },
  };
}

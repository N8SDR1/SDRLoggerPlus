import { describe, it, expect } from 'vitest';
import {
  createLightningRipples,
  rippleFrame,
  RIPPLE_PERIOD_MS,
  type RippleStrike,
  type ThreeLike,
} from './lightningRipples';

function stubThree() {
  const counts = { objects: 0, materials: 0, materialsDisposed: 0, geometries: 0, geometriesDisposed: 0 };
  class Obj {
    visible = true;
    renderOrder = 0;
    raycast: (...args: unknown[]) => void = () => { throw new Error('not replaced'); };
    position = { x: 0, y: 0, z: 0, set(x: number, y: number, z: number) { this.x = x; this.y = y; this.z = z; return this; } };
    scale = { x: 1, y: 1, z: 1, set(x: number, y: number, z: number) { this.x = x; this.y = y; this.z = z; return this; } };
    children: object[] = [];
    lookedAt: number[] = [];
    constructor() { counts.objects++; }
    add(c: object) { this.children.push(c); return this; }
    remove(c: object) { this.children = this.children.filter(x => x !== c); return this; }
    lookAt(x: number, y: number, z: number) { this.lookedAt = [x, y, z]; }
  }
  const three = {
    BufferGeometry: class {
      constructor() { counts.geometries++; }
      setAttribute() { return this; }
      dispose() { counts.geometriesDisposed++; }
    },
    Float32BufferAttribute: class { constructor(public array: number[], public itemSize: number) {} },
    LineBasicMaterial: class {
      opacity: number;
      constructor(p: { opacity: number }) { this.opacity = p.opacity; counts.materials++; }
      dispose() { counts.materialsDisposed++; }
    },
    LineLoop: class extends Obj { constructor(public geometry: object, public material: object) { super(); } },
    Group: Obj,
  } as unknown as ThreeLike;
  return { three, counts };
}

const coords = (lat: number, lng: number) => ({ x: lng, y: lat, z: 100 });
const strike = (local = false, ts = 0): RippleStrike => ({ lat: 40, lng: -84, local, ts });

describe('rippleFrame', () => {
  it('grows from zero to the max radius, fading out, then hides until the next period', () => {
    const s = strike(false, 0); // max 2.5°, 2°/s → 1250 ms ripple
    expect(rippleFrame(s, 0, 0)).toMatchObject({ visible: true, radiusDeg: 0 });
    const mid = rippleFrame(s, 0, 625);
    expect(mid.radiusDeg).toBeCloseTo(1.25);
    expect(mid.opacity).toBeCloseTo(0.5 * 0.6); // half faded × distant strike
    expect(rippleFrame(s, 0, 1500).visible).toBe(false);
    expect(rippleFrame(s, 0, RIPPLE_PERIOD_MS).radiusDeg).toBe(0); // next ripple starts
  });

  it('draws local strikes larger and brighter', () => {
    const local = rippleFrame(strike(true, 0), 0, 500);
    const far = rippleFrame(strike(false, 0), 0, 500);
    expect(local.opacity).toBeGreaterThan(far.opacity);
    expect(rippleFrame(strike(true, 0), 0, 1600).visible).toBe(true); // 3.5° → 1750 ms
  });

  it('dims strikes as they age', () => {
    const now = 10 * 60_000;
    expect(rippleFrame(strike(true, now - 30_000), 0, now).opacity)
      .toBeGreaterThan(rippleFrame(strike(true, now - 8 * 60_000), 0, now).opacity);
  });
});

describe('createLightningRipples', () => {
  it('creates one circle per strike and reuses it across updates (diffed by identity)', () => {
    const { three, counts } = stubThree();
    const ripples = createLightningRipples(three, 100, coords);
    const a = strike(), b = strike(true);
    ripples.setStrikes([a, b], 0);
    expect(ripples.count).toBe(2);
    const made = counts.materials;
    ripples.setStrikes([a, b], 2000);
    expect(counts.materials).toBe(made);
  });

  it('allocates nothing per ripple — an hour of frames keeps the object count flat', () => {
    const { three, counts } = stubThree();
    const ripples = createLightningRipples(three, 100, coords);
    const strikes = Array.from({ length: 400 }, (_, i) => strike(i % 2 === 0, 0));
    ripples.setStrikes(strikes, 0);
    const objects = counts.objects, materials = counts.materials;
    for (let t = 0; t < 3_600_000; t += 50) ripples.update(t); // ~20 fps for an hour
    expect(counts.objects).toBe(objects);
    expect(counts.materials).toBe(materials);
  });

  it('disposes a strike’s material when it leaves, and everything on dispose', () => {
    const { three, counts } = stubThree();
    const ripples = createLightningRipples(three, 100, coords);
    const a = strike(), b = strike();
    ripples.setStrikes([a, b], 0);
    ripples.setStrikes([b], 1000);
    expect(ripples.count).toBe(1);
    expect(counts.materialsDisposed).toBe(1);
    ripples.dispose();
    expect(ripples.count).toBe(0);
    expect(counts.materialsDisposed).toBe(2);
    expect(counts.geometriesDisposed).toBe(counts.geometries);
  });

  it('never intercepts globe clicks and lays each circle flat on the surface', () => {
    const { three } = stubThree();
    const ripples = createLightningRipples(three, 100, coords);
    ripples.setStrikes([strike()], 0);
    expect(() => ripples.group.raycast()).not.toThrow();
    const holder = (ripples.group as unknown as { children: { lookedAt: number[]; children: { raycast: () => void }[] }[] }).children[0];
    expect(holder.lookedAt).toEqual([0, 0, 0]);
    expect(() => holder.children[0].raycast()).not.toThrow();
  });
});

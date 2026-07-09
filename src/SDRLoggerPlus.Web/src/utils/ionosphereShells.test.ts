import { describe, it, expect } from 'vitest';
import { createIonosphereShells, DEFAULT_IONO_LAYERS, type ThreeLike } from './ionosphereShells';

function stubThree() {
  const calls = { geomRadii: [] as number[], geomDisposed: 0, matDisposed: 0 };
  const three: ThreeLike = {
    SphereGeometry: class {
      constructor(radius: number) { calls.geomRadii.push(radius); }
      dispose() { calls.geomDisposed++; }
    },
    ShaderMaterial: class {
      uniforms: Record<string, { value: unknown }>;
      constructor(params: { uniforms: Record<string, { value: unknown }> }) { this.uniforms = params.uniforms; }
      dispose() { calls.matDisposed++; }
    },
    Color: class {
      constructor(public r: number, public g: number, public b: number) {}
      setRGB(r: number, g: number, b: number) { this.r = r; this.g = g; this.b = b; return this; }
    },
    Mesh: class {
      visible = true;
      renderOrder = 0;
      raycast: (...args: unknown[]) => void = () => { throw new Error('not replaced'); };
      constructor(public geometry: object, public material: object) {}
    },
    BackSide: 1,
  } as unknown as ThreeLike;
  return { three, calls };
}

describe('createIonosphereShells', () => {
  it('builds one hidden shell per layer at the right radii', () => {
    const { three, calls } = stubThree();
    const shells = createIonosphereShells(three, 100);
    expect(shells.meshes).toHaveLength(DEFAULT_IONO_LAYERS.length);
    // radii = globeRadius * each layer's radiusFactor, ascending (D→E→F outward)
    expect(calls.geomRadii).toEqual(DEFAULT_IONO_LAYERS.map((l) => 100 * l.radiusFactor));
    expect(calls.geomRadii[2]).toBeGreaterThan(calls.geomRadii[0]);
    for (const m of shells.meshes) {
      expect((m as { visible: boolean }).visible).toBe(false);
      expect(() => (m as { raycast: () => void }).raycast()).not.toThrow();
    }
  });

  it('bands are translucent and evenly spaced (~0.09 apart)', () => {
    for (const l of DEFAULT_IONO_LAYERS) {
      expect(l.intensity).toBeGreaterThan(0);
      expect(l.intensity).toBeLessThan(1); // peak alpha — always transparent
    }
    const [d, e, f] = DEFAULT_IONO_LAYERS.map((l) => l.radiusFactor);
    expect(e - d).toBeCloseTo(f - e, 2); // equal gaps between D→E and E→F
    expect(f - e).toBeGreaterThan(0.05);
  });

  it('every band fades from the globe surface (overlapping, blended)', () => {
    const { three } = stubThree();
    const shells = createIonosphereShells(three, 100);
    shells.meshes.forEach((m, i) => {
      const mat = (m as unknown as { material: { uniforms: Record<string, { value: unknown }> } }).material;
      const innerN = mat.uniforms.uInnerN.value as number;
      expect(innerN).toBeCloseTo(1.0 / DEFAULT_IONO_LAYERS[i].radiusFactor, 6);
      expect(innerN).toBeLessThan(1); // band has nonzero width
    });
  });

  it('inner bands draw AFTER outer ones (higher renderOrder) so the steps stay visible', () => {
    const { three } = stubThree();
    const shells = createIonosphereShells(three, 100);
    const orders = shells.meshes.map((m) => (m as { renderOrder: number }).renderOrder);
    // D (inner) must have the highest renderOrder, F (outer) the lowest —
    // with depthWrite off, a last-drawn outer band would paint over the rest.
    expect(orders[0]).toBeGreaterThan(orders[1]);
    expect(orders[1]).toBeGreaterThan(orders[2]);
    expect(Math.min(...orders)).toBeGreaterThanOrEqual(2);
  });

  it('setColors recolours every shell (theme tracking)', () => {
    const { three } = stubThree();
    const shells = createIonosphereShells(three, 100);
    shells.setColors([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]]);
    shells.meshes.forEach((m, i) => {
      const col = (m as unknown as { material: { uniforms: { uColor: { value: { r: number; g: number; b: number } } } } }).material.uniforms.uColor.value;
      expect(col.r).toBeCloseTo(0.1 + i * 0.3, 6);
      expect(col.g).toBeCloseTo(0.2 + i * 0.3, 6);
      expect(col.b).toBeCloseTo(0.3 + i * 0.3, 6);
    });
  });

  it('setVisible toggles every shell', () => {
    const { three } = stubThree();
    const shells = createIonosphereShells(three, 100);
    shells.setVisible(true);
    expect(shells.meshes.every((m) => (m as { visible: boolean }).visible)).toBe(true);
    shells.setVisible(false);
    expect(shells.meshes.every((m) => !(m as { visible: boolean }).visible)).toBe(true);
  });

  it('dispose frees every geometry and material', () => {
    const { three, calls } = stubThree();
    createIonosphereShells(three, 100).dispose();
    expect(calls.geomDisposed).toBe(DEFAULT_IONO_LAYERS.length);
    expect(calls.matDisposed).toBe(DEFAULT_IONO_LAYERS.length);
  });
});

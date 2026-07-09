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
    Color: class { constructor(public r: number, public g: number, public b: number) {} },
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

  it('outer F layer is the brightest', () => {
    expect(DEFAULT_IONO_LAYERS[2].intensity).toBeGreaterThan(DEFAULT_IONO_LAYERS[0].intensity);
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

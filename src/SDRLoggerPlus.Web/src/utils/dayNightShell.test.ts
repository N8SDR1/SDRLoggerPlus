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

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

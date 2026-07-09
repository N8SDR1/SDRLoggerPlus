// Ionosphere glow shells for the 3D globe: three transparent spheres at the
// D, E and F layer altitudes, each with a fresnel rim-glow fragment shader so
// it lights up at the limb (brightest at the outer edge, fading toward the
// globe) in its own colour. Added straight to the three.js scene — no globe.gl
// data layer, so it can't collide with the beam/lightning/marker layers.
// Pure module: three.js is injected (ThreeLike), so it unit-tests without WebGL.

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
    blending: number;
  }) => { uniforms: Record<string, { value: unknown }>; dispose(): void };
  Color: new (r: number, g: number, b: number) => object;
  Mesh: new (geometry: object, material: object) => SceneObject;
  AdditiveBlending: number;
}

export interface IonosphereShells {
  meshes: SceneObject[];
  setVisible(visible: boolean): void;
  dispose(): void;
}

// D lowest / dim, F highest / brightest — so the halo reads brighter on the
// outer edge. Radii are fractions of the globe radius (exaggerated from the
// real ~1–6 % so the layers are actually visible). Colours are warm→cool.
export interface IonoLayer {
  radiusFactor: number;
  color: [number, number, number]; // 0..1 RGB
  intensity: number;
}

// The F (outer) shell sits at the short-path hop peak (globe radius + 0.28,
// = GlobePlugin's SP_PEAK_ALT) so the hops bounce right off it; D and E stack
// below. Keep the 1.28 in sync with SP_PEAK_ALT if that hop height changes.
export const DEFAULT_IONO_LAYERS: IonoLayer[] = [
  { radiusFactor: 1.13, color: [1.0, 0.42, 0.28], intensity: 0.35 }, // D — red/amber (low)
  { radiusFactor: 1.205, color: [0.38, 1.0, 0.48], intensity: 0.5 }, // E — green (mid)
  { radiusFactor: 1.28, color: [0.36, 0.76, 1.0], intensity: 0.78 }, // F — cyan (hop peak, brightest)
];

const VERTEX_SHADER = `
varying vec3 vNormalW;
varying vec3 vViewDir;
void main() {
  vNormalW = normalize(mat3(modelMatrix) * normal);
  vec4 worldPos = modelMatrix * vec4(position, 1.0);
  vViewDir = normalize(cameraPosition - worldPos.xyz);
  gl_Position = projectionMatrix * viewMatrix * worldPos;
}
`;

const FRAGMENT_SHADER = `
uniform vec3 uColor;
uniform float uIntensity;
varying vec3 vNormalW;
varying vec3 vViewDir;
void main() {
  // Fresnel rim: 0 facing the camera, 1 at the silhouette (limb / outer edge).
  float rim = 1.0 - abs(dot(normalize(vNormalW), normalize(vViewDir)));
  // Thin bright band right at the limb plus a soft wider halo around it —
  // the inner face stays essentially transparent so the glow never washes
  // over the globe. Brightest at the very edge (rim→1).
  float band = smoothstep(0.82, 1.0, rim);
  float halo = smoothstep(0.62, 1.0, rim) * 0.3;
  float glow = (band + halo) * uIntensity;
  if (glow < 0.004) discard;
  gl_FragColor = vec4(uColor, glow);
}
`;

export function createIonosphereShells(
  three: ThreeLike,
  globeRadius: number,
  layers: IonoLayer[] = DEFAULT_IONO_LAYERS,
): IonosphereShells {
  const geometries: { dispose(): void }[] = [];
  const materials: { dispose(): void }[] = [];
  const meshes: SceneObject[] = [];

  for (const layer of layers) {
    const geometry = new three.SphereGeometry(globeRadius * layer.radiusFactor, 64, 32);
    const material = new three.ShaderMaterial({
      uniforms: {
        uColor: { value: new three.Color(layer.color[0], layer.color[1], layer.color[2]) },
        uIntensity: { value: layer.intensity },
      },
      vertexShader: VERTEX_SHADER,
      fragmentShader: FRAGMENT_SHADER,
      transparent: true,
      depthWrite: false,
      blending: three.AdditiveBlending,
    });
    const mesh = new three.Mesh(geometry, material);
    mesh.visible = false;
    mesh.renderOrder = 2;      // above the day/night shell
    mesh.raycast = () => {};   // never intercept globe clicks
    geometries.push(geometry);
    materials.push(material);
    meshes.push(mesh);
  }

  return {
    meshes,
    setVisible(visible: boolean) {
      for (const mesh of meshes) mesh.visible = visible;
    },
    dispose() {
      for (const g of geometries) g.dispose();
      for (const m of materials) m.dispose();
    },
  };
}

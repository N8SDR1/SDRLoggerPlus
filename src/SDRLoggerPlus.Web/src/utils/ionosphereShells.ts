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
    side: number;
  }) => { uniforms: Record<string, { value: unknown }>; dispose(): void };
  Color: new (r: number, g: number, b: number) => object;
  Mesh: new (geometry: object, material: object) => SceneObject;
  BackSide: number;
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
// = the hop altitude the globe uses) so the hops bounce right off it; D and E
// stack below. Keep 1.28 in sync if that hop height changes.
// Colours match the classic layered-atmosphere diagram: solid navy-blue
// steps, lightest against the globe and darkest at the outer edge.
export const DEFAULT_IONO_LAYERS: IonoLayer[] = [
  { radiusFactor: 1.13, color: [0.26, 0.44, 0.72], intensity: 1.0 },  // D — inner, lightest blue
  { radiusFactor: 1.205, color: [0.16, 0.29, 0.52], intensity: 1.0 }, // E — mid blue
  { radiusFactor: 1.28, color: [0.09, 0.17, 0.33], intensity: 1.0 },  // F — outer, darkest (hop peak)
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
  // Solid opaque band. Each shell renders BACK-SIDE ONLY with the depth test
  // on, so the opaque globe hides everything except the ring beyond its
  // silhouette — stacked shells read as flat solid concentric bands
  // (D innermost … F outermost), exactly like a layered-ionosphere diagram.
  gl_FragColor = vec4(uColor, uIntensity);
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

  layers.forEach((layer, i) => {
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
      // Back side only: the globe occludes the shell's far hemisphere except
      // the ring beyond its silhouette — the solid-band diagram look.
      side: three.BackSide,
    });
    const mesh = new three.Mesh(geometry, material);
    mesh.visible = false;
    // With depthWrite off, draw order decides which opaque band wins where
    // they overlap: every shell's ring covers all the inner rings' area, so
    // draw OUTER FIRST and INNER LAST (higher renderOrder) — each inner band
    // paints its annulus over the outer ones, leaving clean stacked steps.
    // (All still above the day/night shell at renderOrder 1.)
    mesh.renderOrder = 2 + (layers.length - 1 - i);
    mesh.raycast = () => {};   // never intercept globe clicks
    geometries.push(geometry);
    materials.push(material);
    meshes.push(mesh);
  });

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

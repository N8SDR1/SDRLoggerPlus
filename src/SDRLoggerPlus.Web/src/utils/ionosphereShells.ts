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
// Navy-blue hues stepping darker outward; intensity = each band's PEAK alpha
// (at its outer edge). All translucent — never a solid fill.
export const DEFAULT_IONO_LAYERS: IonoLayer[] = [
  { radiusFactor: 1.13, color: [0.30, 0.50, 0.82], intensity: 0.75 },  // D — inner, lightest blue
  { radiusFactor: 1.205, color: [0.20, 0.36, 0.66], intensity: 0.7 },  // E — mid blue
  { radiusFactor: 1.28, color: [0.12, 0.24, 0.48], intensity: 0.65 },  // F — outer, darkest (hop peak)
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
uniform float uInnerN;
varying vec3 vNormalW;
varying vec3 vViewDir;
void main() {
  // Translucent gradient band. Each shell renders BACK-SIDE ONLY with the
  // depth test on, so the globe hides everything except the ring beyond its
  // silhouette. The fragment's projected distance from the globe's center
  // (in units of this shell's radius) is sqrt(1 - (n·v)^2): 1 exactly at the
  // shell's silhouette (this band's OUTER edge), uInnerN at the previous
  // layer's radius (this band's inner edge). Alpha ramps from transparent at
  // the inner edge to the band's peak at its outer edge, so each layer glows
  // brightest at its own edge and fades into the next — never a solid fill.
  float nv = dot(normalize(vNormalW), normalize(vViewDir));
  float dNorm = sqrt(max(0.0, 1.0 - nv * nv));
  float band = smoothstep(uInnerN, 1.0, dNorm);
  float alpha = pow(band, 1.15) * uIntensity;
  if (alpha < 0.004) discard;
  gl_FragColor = vec4(uColor, alpha);
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
    // This band's inner edge = the previous layer's radius (the globe surface
    // for the first), normalized to THIS shell's radius for the shader.
    const innerFactor = i === 0 ? 1.0 : layers[i - 1].radiusFactor;
    const material = new three.ShaderMaterial({
      uniforms: {
        uColor: { value: new three.Color(layer.color[0], layer.color[1], layer.color[2]) },
        uIntensity: { value: layer.intensity },
        uInnerN: { value: innerFactor / layer.radiusFactor },
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

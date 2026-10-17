// Shared Leaflet basemap tile-layer options, used by both the 2D Map plugin and the
// Grid Tracker map view so they offer the same basemaps and stay in sync.

export const TILE_LAYERS = {
  osm: {
    name: 'OpenStreetMap',
    url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
  },
  dark: {
    name: 'Dark',
    // CARTO started gating its free basemap CDN behind an API key — it now serves "API KEY REQUIRED"
    // placeholder tiles, which broke the default Dark basemap for everyone (nothing changed on our
    // side). Esri's World Dark Gray Base is a keyless dark basemap from the same provider as the
    // Satellite layer below, which has always loaded without a key.
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}',
    attribution: '&copy; <a href="https://www.esri.com/">Esri</a>',
  },
  satellite: {
    name: 'Satellite',
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    attribution: '&copy; Esri',
  },
  terrain: {
    name: 'Terrain',
    url: 'https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',
    attribution: '&copy; <a href="https://opentopomap.org">OpenTopoMap</a>',
  },
} as const;

export type TileLayerKey = keyof typeof TILE_LAYERS;

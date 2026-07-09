/// <reference types="vitest" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import basicSsl from "@vitejs/plugin-basic-ssl";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, resolve } from "path";

// Use HTTPS only when VITE_HTTPS=true (for remote access where WebGL requires secure context)
// Usage: VITE_HTTPS=true npm run dev
const useHttps = process.env.VITE_HTTPS === "true";
const backendPort = process.env.BACKEND_PORT || "5050";

// App version shown in About. Single source of truth = the Desktop
// package.json (what the installer/release carries), so dev and packaged
// builds both display a real number instead of "dev". CI may override with an
// explicit VITE_APP_VERSION (e.g. from the release git tag).
const projectRoot = dirname(fileURLToPath(import.meta.url));
let desktopVersion = "dev";
try {
  const pkg = JSON.parse(
    readFileSync(resolve(projectRoot, "../SDRLoggerPlus.Desktop/package.json"), "utf-8"),
  );
  if (pkg.version) desktopVersion = pkg.version;
} catch {
  // Desktop package.json not reachable — fall back to "dev".
}
const appVersion = process.env.VITE_APP_VERSION || desktopVersion;

export default defineConfig({
  plugins: [react(), ...(useHttps ? [basicSsl()] : [])],
  base: "./",
  define: {
    "import.meta.env.VITE_APP_VERSION": JSON.stringify(appVersion),
  },
  server: {
    port: 5173,
    host: true,
    allowedHosts: true,
    proxy: {
      "/api": {
        target: `http://localhost:${backendPort}`,
        changeOrigin: true,
      },
      "/hubs": {
        target: `http://localhost:${backendPort}`,
        changeOrigin: true,
        ws: true,
      },
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  test: {
    environment: "happy-dom",
    globals: true,
    setupFiles: "./src/test-setup.ts",
    include: ["src/**/*.test.{ts,tsx}"],
  },
});

#!/usr/bin/env node
/**
 * Prepares the bundled .NET backend for electron-builder packaging.
 * Produces the layout that electron-builder.yml (extraResources) and CI
 * (.github/workflows/release.yml, "Build .NET backend" step) expect:
 *   ./backend/           self-contained SDRLoggerPlus.Server publish output
 *   ./backend/wwwroot/   built frontend (SDRLoggerPlus.Web/dist)
 *
 * Usage: node scripts/prepare-backend.js <rid>
 *   rid: win-x64 | osx-arm64 | linux-x64 (others accepted for completeness)
 */
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const RIDS = ['win-x64', 'win-x86', 'osx-x64', 'osx-arm64', 'linux-x64', 'linux-arm64'];
const rid = process.argv[2];
if (!RIDS.includes(rid)) {
  console.error(`Usage: node scripts/prepare-backend.js <rid>  (one of: ${RIDS.join(', ')})`);
  process.exit(1);
}

const desktopDir = path.resolve(__dirname, '..');
const webDir = path.resolve(desktopDir, '..', 'SDRLoggerPlus.Web');
const serverDir = path.resolve(desktopDir, '..', 'SDRLoggerPlus.Server');
const backendDir = path.join(desktopDir, 'backend');

// Auto-increment the patch version on every packaging run (skipped in CI,
// which stamps the version from the release tag instead). The bump persists
// in package.json so the installer name, About dialog and Add/Remove entry
// all advance together: 1.0.1, 1.0.2, …
const pkgPath = path.join(desktopDir, 'package.json');
const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
if (!process.env.CI) {
  const [major, minor, patch] = pkg.version.split('.').map(Number);
  pkg.version = `${major}.${minor}.${patch + 1}`;
  fs.writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + '\n');
  console.log(`Version bumped to ${pkg.version}`);
}
const version = pkg.version;

function run(cmd, cwd, env) {
  console.log(`\n> ${cmd}`);
  execSync(cmd, { cwd, stdio: 'inherit', env: { ...process.env, ...env } });
}

// 1. Frontend build. VITE_APP_VERSION is load-bearing: without it the About
//    dialog shows the 'dev' fallback (src/SDRLoggerPlus.Web/src/version.ts).
run('npm run build', webDir, { VITE_APP_VERSION: version });

// 2. Clean: `dotnet publish -o` does not clean its output dir; stale files
//    (or a nested wwwroot from a previous run) would ship in the installer.
fs.rmSync(backendDir, { recursive: true, force: true });

// 3. Publish the backend self-contained.
run(
  `dotnet publish "${serverDir}" -c Release --self-contained -r ${rid} -o "${backendDir}"`,
  desktopDir
);

// 4. Replace any wwwroot the publish emitted (Microsoft.NET.Sdk.Web auto-
//    publishes a stray src/SDRLoggerPlus.Server/wwwroot/ if the manual production
//    flow ever created one) with the freshly built frontend.
const wwwroot = path.join(backendDir, 'wwwroot');
fs.rmSync(wwwroot, { recursive: true, force: true });
const dist = path.join(webDir, 'dist');
if (!fs.existsSync(path.join(dist, 'index.html'))) {
  console.error(`Frontend build output missing or incomplete: ${dist}`);
  process.exit(1);
}
fs.cpSync(dist, wwwroot, { recursive: true });

console.log(`\nBackend prepared at ${backendDir} (rid=${rid}, version=${version})`);

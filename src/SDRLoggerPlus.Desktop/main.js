const { app, BrowserWindow, Menu, shell, dialog, ipcMain, screen } = require('electron');
const { spawn } = require('child_process');
const crypto = require('crypto');
const path = require('path');
const net = require('net');
const fs = require('fs');
const log = require('electron-log');
const { checkForUpdates } = require('./updater');

// Per-session secret for the backend's graceful-shutdown endpoint. On Windows
// a signal kill is an instant TerminateProcess, which skips the backend's
// StopAsync path (on-exit backups) — so shutdown is requested over HTTP instead.
const shutdownToken = crypto.randomBytes(32).toString('hex');

// Pin the app identity before any app.getPath() call: userData must resolve
// to %APPDATA%/SDRLoggerPlus on every platform — the uninstaller deletes user data
// by exactly that folder name.
app.setName('SDRLoggerPlus');

// Zoom level persistence using a simple JSON file
const userDataPath = app.getPath('userData');
const zoomConfigPath = path.join(userDataPath, 'zoom-config.json');

function getStoredZoomLevel() {
  try {
    if (fs.existsSync(zoomConfigPath)) {
      const data = fs.readFileSync(zoomConfigPath, 'utf8');
      const config = JSON.parse(data);
      return config.zoomLevel || 0;
    }
  } catch (err) {
    log.warn('Failed to read zoom config:', err.message);
  }
  return 0; // Default zoom level (100%)
}

function saveZoomLevel(level) {
  try {
    const config = { zoomLevel: level };
    fs.writeFileSync(zoomConfigPath, JSON.stringify(config, null, 2), 'utf8');
  } catch (err) {
    log.error('Failed to save zoom config:', err.message);
  }
}

// Window geometry persistence (size + position + maximized). The local file is
// the fast/offline restore cache; the same geometry is mirrored to the backend
// settings so it rides along in the settings export/import.
const windowStatePath = path.join(userDataPath, 'window-state.json');
const DEFAULT_WINDOW = { width: 1400, height: 900 };
let saveWindowStateTimer = null;

function loadWindowState() {
  try {
    if (fs.existsSync(windowStatePath)) {
      return JSON.parse(fs.readFileSync(windowStatePath, 'utf8'));
    }
  } catch (err) {
    log.warn('Failed to read window state:', err.message);
  }
  return null;
}

function writeWindowStateFile(state) {
  try {
    fs.writeFileSync(windowStatePath, JSON.stringify(state, null, 2), 'utf8');
  } catch (err) {
    log.error('Failed to save window state:', err.message);
  }
}

// Pull window geometry from backend settings into the local cache file so an
// imported settings file takes effect on the next launch. Best-effort.
async function reconcileWindowStateFromBackend() {
  try {
    const res = await fetch(`http://localhost:${backendPort}/api/settings`);
    if (!res.ok) return;
    const settings = await res.json();
    const w = settings && settings.window;
    if (w && Number.isFinite(w.width) && Number.isFinite(w.height)) {
      writeWindowStateFile({
        width: w.width,
        height: w.height,
        x: Number.isFinite(w.x) ? w.x : undefined,
        y: Number.isFinite(w.y) ? w.y : undefined,
        maximized: !!w.maximized,
      });
    }
  } catch (err) {
    log.debug('Window state backend reconcile skipped:', err.message);
  }
}

// Push current window geometry to the backend (best-effort) so the export stays current.
function pushWindowStateToBackend(state) {
  try {
    fetch(`http://localhost:${backendPort}/api/settings/window`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(state),
    }).catch(() => {});
  } catch {
    // ignore — geometry mirroring must never break shutdown
  }
}

// Clamp a saved rectangle to a currently-connected display so an imported
// geometry from another monitor setup can never open off-screen.
function computeWindowOptions(state) {
  const opts = { width: DEFAULT_WINDOW.width, height: DEFAULT_WINDOW.height };
  if (!state) return opts;

  if (Number.isFinite(state.width)) opts.width = Math.max(800, state.width);
  if (Number.isFinite(state.height)) opts.height = Math.max(600, state.height);

  if (Number.isFinite(state.x) && Number.isFinite(state.y)) {
    const onScreen = screen.getAllDisplays().some((d) => {
      const a = d.workArea;
      return (
        state.x >= a.x - 50 &&
        state.y >= a.y - 50 &&
        state.x < a.x + a.width - 100 &&
        state.y < a.y + a.height - 100
      );
    });
    if (onScreen) {
      opts.x = state.x;
      opts.y = state.y;
    }
  }
  return opts;
}

function captureWindowState() {
  if (!mainWindow) return null;
  // getNormalBounds() returns the restored (un-maximized) rect even while maximized,
  // so a maximized window restores to a sensible size when later un-maximized.
  const bounds = mainWindow.getNormalBounds();
  return {
    width: bounds.width,
    height: bounds.height,
    x: bounds.x,
    y: bounds.y,
    maximized: mainWindow.isMaximized(),
  };
}

function saveWindowState() {
  const state = captureWindowState();
  if (!state) return;
  writeWindowStateFile(state);
  pushWindowStateToBackend(state);
}

function scheduleSaveWindowState() {
  if (saveWindowStateTimer) clearTimeout(saveWindowStateTimer);
  saveWindowStateTimer = setTimeout(saveWindowState, 500);
}

// Enable GPU acceleration and WebGL for 3D globe support
// Some Macs with integrated GPUs may have these disabled by default
app.commandLine.appendSwitch('ignore-gpu-blacklist');
app.commandLine.appendSwitch('enable-gpu-rasterization');
app.commandLine.appendSwitch('enable-zero-copy');
// Ensure WebGL is available - some systems need this explicitly enabled
app.commandLine.appendSwitch('enable-webgl');
app.commandLine.appendSwitch('enable-webgl2-compute-context');

// Configure logging
log.transports.file.level = 'info';
log.transports.console.level = 'debug';

let mainWindow;
let splashWindow;
let backendProcess;
let backendPort;
let isDevMode = process.argv.includes('--dev');
let useViteDevServer = process.argv.includes('--vite') || process.env.VITE_DEV === 'true';
const VITE_DEV_PORT = 5173;

/**
 * Find an available port starting from the given port
 */
async function findAvailablePort(startPort = 5050) {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.listen(startPort, '127.0.0.1', () => {
      const port = server.address().port;
      server.close(() => {
        log.info(`Found available port: ${port}`);
        resolve(port);
      });
    });
    server.on('error', (err) => {
      if (err.code === 'EADDRINUSE') {
        log.debug(`Port ${startPort} in use, trying ${startPort + 1}`);
        resolve(findAvailablePort(startPort + 1));
      } else {
        reject(err);
      }
    });
  });
}

/**
 * Get the path to the backend executable
 */
function getBackendPath() {
  const execName = process.platform === 'win32' ? 'SDRLoggerPlus.Server.exe' : 'SDRLoggerPlus.Server';

  if (app.isPackaged) {
    // In packaged app, backend is in resources/backend
    return path.join(process.resourcesPath, 'backend', execName);
  } else {
    // Unpackaged (npm start): use the output of scripts/prepare-backend.js
    return path.join(__dirname, 'backend', execName);
  }
}

/**
 * Start the .NET backend process
 */
async function startBackend() {
  if (isDevMode) {
    // In dev mode, assume backend is already running on port 5050
    backendPort = 5050;
    log.info('Dev mode: Using existing backend on port 5050');
    return;
  }

  backendPort = await findAvailablePort();
  const backendPath = getBackendPath();

  log.info(`Starting backend: ${backendPath}`);
  log.info(`Backend port: ${backendPort}`);

  // Check if backend exists
  const fs = require('fs');
  if (!fs.existsSync(backendPath)) {
    const errorMsg = `Backend not found at: ${backendPath}\n\nPlease build the backend first using:\nnode scripts/prepare-backend.js ${process.platform === 'win32' ? 'win-x64' : process.platform === 'darwin' ? 'osx-' + process.arch : 'linux-x64'}`;
    log.error(errorMsg);
    dialog.showErrorBox('Backend Not Found', errorMsg);
    app.quit();
    return;
  }

  const backendDir = path.dirname(backendPath);

  // Make executable on Unix and remove macOS quarantine attribute
  if (process.platform !== 'win32') {
    try {
      fs.chmodSync(backendPath, '755');
    } catch (err) {
      log.warn(`Could not set executable permission: ${err.message}`);
    }
  }

  // Remove macOS quarantine attribute from the backend directory
  // Unsigned apps downloaded from the internet get quarantined by Gatekeeper,
  // which prevents child process binaries from executing
  if (process.platform === 'darwin') {
    const { execSync } = require('child_process');
    try {
      execSync(`xattr -rd com.apple.quarantine "${backendDir}"`, { stdio: 'ignore' });
      log.info('Removed quarantine attribute from backend directory');
    } catch (err) {
      log.warn(`Could not remove quarantine attribute: ${err.message}`);
    }
  }
  backendProcess = spawn(backendPath, [], {
    cwd: backendDir,
    env: {
      ...process.env,
      ASPNETCORE_URLS: `http://localhost:${backendPort}`,
      ASPNETCORE_ENVIRONMENT: app.isPackaged ? 'Production' : 'Development',
      SDRLOGGERPLUS_SHUTDOWN_TOKEN: shutdownToken
    },
    stdio: ['ignore', 'pipe', 'pipe']
  });

  backendProcess.stdout.on('data', (data) => {
    log.info(`[Backend] ${data.toString().trim()}`);
  });

  backendProcess.stderr.on('data', (data) => {
    log.error(`[Backend] ${data.toString().trim()}`);
  });

  backendProcess.on('error', (err) => {
    log.error(`Backend process error: ${err.message}`);
  });

  backendProcess.on('close', (code) => {
    log.info(`Backend process exited with code ${code}`);
  });

  // Wait for backend to be ready
  await waitForBackend();
}

/**
 * Wait for the backend to respond to health checks
 */
async function waitForBackend(retries = 30, delayMs = 1000) {
  log.info('Waiting for backend to be ready...');

  for (let i = 0; i < retries; i++) {
    // Check if the backend process has already exited
    if (backendProcess && backendProcess.exitCode !== null) {
      throw new Error(`Backend process exited with code ${backendProcess.exitCode} before becoming ready. Check the logs for details.`);
    }

    try {
      const response = await fetch(`http://localhost:${backendPort}/api/health`);
      if (response.ok) {
        log.info('Backend is ready!');
        return;
      }
    } catch (err) {
      // Backend not ready yet
    }
    log.debug(`Backend not ready, attempt ${i + 1}/${retries}`);
    await new Promise(resolve => setTimeout(resolve, delayMs));
  }

  throw new Error('Backend failed to start within timeout');
}

/**
 * Create the splash screen window
 */
function createSplashWindow() {
  const imgPath = path.join(__dirname, 'assets', 'splash.png');

  splashWindow = new BrowserWindow({
    width: 600,
    height: 400,
    frame: false,
    resizable: false,
    transparent: false,
    alwaysOnTop: true,
    center: true,
    backgroundColor: '#0a0e14',
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true
    }
  });

  const splashPath = path.join(__dirname, 'splash.html');
  const version = app.getVersion();
  splashWindow.loadFile(splashPath, {
    query: { v: version, img: imgPath }
  });

  splashWindow.on('closed', () => {
    splashWindow = null;
  });
}

/**
 * Create the main application window
 */
function createWindow() {
  const savedWindowState = loadWindowState();
  const winOpts = computeWindowOptions(savedWindowState);

  mainWindow = new BrowserWindow({
    width: winOpts.width,
    height: winOpts.height,
    ...(Number.isFinite(winOpts.x) ? { x: winOpts.x, y: winOpts.y } : {}),
    minWidth: 800,
    minHeight: 600,
    title: 'SDRLoggerPlus',
    icon: path.join(__dirname, 'assets', 'icon.png'),
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      webSecurity: true,
      preload: path.join(__dirname, 'preload.js')
    },
    backgroundColor: '#0a0a0f',
    show: false // Don't show until ready
  });

  if (savedWindowState && savedWindowState.maximized) {
    mainWindow.maximize();
  }

  // Persist geometry as it changes (debounced) and on maximize toggle
  mainWindow.on('resize', scheduleSaveWindowState);
  mainWindow.on('move', scheduleSaveWindowState);
  mainWindow.on('maximize', saveWindowState);
  mainWindow.on('unmaximize', saveWindowState);

  // Clear cache on startup to ensure fresh assets are loaded
  // This prevents stale chunk references after updates
  // Note: This clears HTTP cache but preserves zoom levels and other preferences
  mainWindow.webContents.session.clearCache();

  // Load the appropriate URL based on dev mode
  const loadUrl = useViteDevServer
    ? `http://localhost:${VITE_DEV_PORT}`
    : `http://localhost:${backendPort}`;
  log.info(`Loading URL: ${loadUrl}`);
  mainWindow.loadURL(loadUrl);

  // Open external links in default browser
  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: 'deny' };
  });

  // Handle window close
  mainWindow.on('closed', () => {
    mainWindow = null;
  });

  // Save zoom level and window geometry before window closes
  mainWindow.on('close', () => {
    if (mainWindow) {
      const currentZoom = mainWindow.webContents.getZoomLevel();
      saveZoomLevel(currentZoom);
      log.debug(`Saved zoom level on close: ${currentZoom}`);
      saveWindowState();
    }
  });
}

/**
 * Create the application menu
 */
function createMenu() {
  const isMac = process.platform === 'darwin';

  const template = [
    // App menu (macOS only)
    ...(isMac ? [{
      label: 'SDRLoggerPlus',
      submenu: [
        {
          label: 'About SDRLoggerPlus',
          click: () => {
            if (mainWindow) {
              mainWindow.webContents.send('open-about');
            }
          }
        },
        { type: 'separator' },
        {
          label: 'Settings...',
          accelerator: 'CmdOrCtrl+,',
          click: () => {
            if (mainWindow) {
              mainWindow.webContents.send('open-settings');
            }
          }
        },
        { type: 'separator' },
        { role: 'services' },
        { type: 'separator' },
        { label: 'Hide SDRLoggerPlus', role: 'hide' },
        { role: 'hideOthers' },
        { role: 'unhide' },
        { type: 'separator' },
        { label: 'Quit SDRLoggerPlus', role: 'quit' }
      ]
    }] : []),
    // File menu
    {
      label: 'File',
      submenu: [
        ...(!isMac ? [{
          label: 'Settings',
          accelerator: 'CmdOrCtrl+,',
          click: () => {
            if (mainWindow) {
              mainWindow.webContents.send('open-settings');
            }
          }
        },
        { type: 'separator' }] : []),
        isMac ? { role: 'close' } : { role: 'quit' }
      ]
    },
    // Edit menu
    {
      label: 'Edit',
      submenu: [
        { role: 'undo' },
        { role: 'redo' },
        { type: 'separator' },
        { role: 'cut' },
        { role: 'copy' },
        { role: 'paste' },
        { role: 'selectAll' }
      ]
    },
    // View menu
    {
      label: 'View',
      submenu: [
        { role: 'reload' },
        { role: 'forceReload' },
        { role: 'toggleDevTools' },
        { type: 'separator' },
        { role: 'togglefullscreen' }
      ]
    },
    // Help menu
    {
      label: 'Help',
      submenu: [
        {
          label: 'Check for Updates...',
          click: async () => {
            await checkForUpdates(false);
          }
        },
        { type: 'separator' },
        {
          label: 'SDRLoggerPlus on GitHub',
          click: async () => {
            await shell.openExternal('https://github.com/n9bc/SDRLoggerPlus');
          }
        },
        { type: 'separator' },
        {
          label: 'Open Logs Folder',
          click: async () => {
            await shell.openPath(app.getPath('logs'));
          }
        }
      ]
    }
  ];

  const menu = Menu.buildFromTemplate(template);
  Menu.setApplicationMenu(menu);
}

/**
 * Stop the backend gracefully: ask it to shut itself down (so the .NET host's
 * StopAsync path runs — on-exit backups have a 10 s budget there), wait up to
 * 12 s for the process to exit, then force-kill as a last resort.
 */
async function shutdownBackend() {
  if (!backendProcess || backendProcess.exitCode !== null) {
    return;
  }

  const exited = new Promise((resolve) => backendProcess.once('close', resolve));

  try {
    log.info('Requesting graceful backend shutdown...');
    await fetch(`http://localhost:${backendPort}/api/system/shutdown`, {
      method: 'POST',
      headers: { 'X-Shutdown-Token': shutdownToken }
    });
  } catch (err) {
    log.warn(`Graceful shutdown request failed: ${err.message}`);
  }

  const timedOut = await Promise.race([
    exited.then(() => false),
    new Promise((resolve) => setTimeout(() => resolve(true), 12000))
  ]);

  if (timedOut && backendProcess && backendProcess.exitCode === null) {
    log.warn('Backend did not exit in time, force killing');
    backendProcess.kill('SIGKILL');
  } else {
    log.info('Backend exited gracefully');
  }
}

// App event handlers
app.whenReady().then(async () => {
  log.info(`SDRLoggerPlus Desktop starting (version ${app.getVersion()})`);
  log.info(`Platform: ${process.platform} ${process.arch}`);
  log.info(`Electron: ${process.versions.electron}`);
  log.info(`Packaged: ${app.isPackaged}`);
  log.info(`Dev mode: ${isDevMode}, Vite dev server: ${useViteDevServer}`);

  // Handle restart requests from renderer (e.g. database provider switch).
  // app.exit() skips before-quit, so shut the backend down explicitly first —
  // otherwise it would survive and hold the database lock against the
  // relaunched instance's backend.
  ipcMain.handle('restart-app', async () => {
    log.info('Restart requested via IPC');
    await shutdownBackend();
    app.relaunch();
    app.exit(0);
  });

  // Handle zoom level IPC
  ipcMain.handle('get-zoom-level', () => {
    if (mainWindow) {
      return mainWindow.webContents.getZoomLevel();
    }
    return 0;
  });

  ipcMain.handle('set-zoom-level', (event, level) => {
    if (mainWindow) {
      mainWindow.webContents.setZoomLevel(level);
      saveZoomLevel(level);
      log.debug(`Zoom level set to ${level}`);
    }
  });

  // Native file picker. Accepts { title?, defaultPath?, filters? } and returns
  // the chosen absolute path (string) or null if the user cancelled.
  ipcMain.handle('select-file', async (_event, options = {}) => {
    if (!mainWindow) return null;
    const result = await dialog.showOpenDialog(mainWindow, {
      title: options.title || 'Select File',
      defaultPath: options.defaultPath,
      filters: options.filters,
      properties: ['openFile']
    });
    if (result.canceled || result.filePaths.length === 0) return null;
    return result.filePaths[0];
  });

  try {
    createSplashWindow();
    await startBackend();
    createMenu();
    // Prefer backend-stored geometry (e.g. from an imported settings file) over
    // the local cache, so an import takes effect on this launch.
    await reconcileWindowStateFromBackend();
    createWindow();

    // Close splash and show main window when ready
    mainWindow.once('ready-to-show', () => {
      if (splashWindow) {
        splashWindow.close();
      }
      mainWindow.show();

      // Restore saved zoom level
      const savedZoomLevel = getStoredZoomLevel();
      if (savedZoomLevel !== 0) {
        mainWindow.webContents.setZoomLevel(savedZoomLevel);
        log.info(`Restored zoom level: ${savedZoomLevel}`);
      }
    });

    // Save zoom level when it changes
    mainWindow.webContents.on('zoom-changed', (event, zoomDirection) => {
      const currentZoom = mainWindow.webContents.getZoomLevel();
      saveZoomLevel(currentZoom);
      log.debug(`Zoom changed (${zoomDirection}): ${currentZoom}`);
    });

    // Check for updates after startup (delayed to not slow down launch)
    setTimeout(() => {
      checkForUpdates(true).catch(err => {
        log.warn(`Startup update check failed: ${err.message}`);
      });
    }, 3000);

    // macOS: Re-create window when dock icon clicked
    app.on('activate', () => {
      if (BrowserWindow.getAllWindows().length === 0) {
        createWindow();
      }
    });
  } catch (err) {
    log.error(`Startup error: ${err.message}`);
    if (splashWindow) {
      splashWindow.close();
      splashWindow = null;
    }
    dialog.showErrorBox('Startup Error', err.message);
    app.quit();
  }
});

// Quit when all windows are closed (except on macOS)
app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

// Cleanup before quit: hold the quit open until the backend has shut down
// gracefully, then resume quitting.
let backendShutdownDone = false;
app.on('before-quit', (event) => {
  if (backendShutdownDone || !backendProcess || backendProcess.exitCode !== null) {
    return;
  }
  event.preventDefault();
  shutdownBackend()
    .catch((err) => log.error(`Backend shutdown error: ${err.message}`))
    .finally(() => {
      backendShutdownDone = true;
      app.quit();
    });
});

// Last-ditch: if quit proceeds with the backend somehow still alive, kill it.
app.on('will-quit', () => {
  if (backendProcess && backendProcess.exitCode === null) {
    log.warn('Backend still running at will-quit, force killing');
    backendProcess.kill('SIGKILL');
  }
});

// Handle uncaught exceptions
process.on('uncaughtException', (err) => {
  log.error(`Uncaught exception: ${err.message}`);
  log.error(err.stack);
});

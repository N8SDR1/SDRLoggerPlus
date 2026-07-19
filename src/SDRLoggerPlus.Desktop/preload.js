const { contextBridge, ipcRenderer } = require('electron');

// Expose protected methods for renderer process to use
contextBridge.exposeInMainWorld('electronAPI', {
  // Listen for menu commands from main process
  onOpenSettings: (callback) => {
    ipcRenderer.on('open-settings', () => callback());
  },
  // Remove listener when component unmounts
  removeOpenSettingsListener: () => {
    ipcRenderer.removeAllListeners('open-settings');
  },
  // Listen for about dialog command from main process
  onOpenAbout: (callback) => {
    ipcRenderer.on('open-about', () => callback());
  },
  removeOpenAboutListener: () => {
    ipcRenderer.removeAllListeners('open-about');
  },
  // Restart the app (used after database provider switch)
  restartApp: () => ipcRenderer.invoke('restart-app'),
  // Zoom level management
  getZoomLevel: () => ipcRenderer.invoke('get-zoom-level'),
  setZoomLevel: (level) => ipcRenderer.invoke('set-zoom-level', level),
  onZoomLevelChanged: (callback) => {
    ipcRenderer.on('zoom-level-changed', (_event, level) => callback(level));
  },
  removeZoomLevelChangedListener: () => {
    ipcRenderer.removeAllListeners('zoom-level-changed');
  },
  // Native file picker — returns the selected absolute path, or null if cancelled.
  // Used by the LOTW settings section to locate the TQSL binary.
  selectFile: (options) => ipcRenderer.invoke('select-file', options),

  // View > Layouts. The renderer owns the presets and pushes the list up so the
  // native menu can show them; the menu sends the chosen action back down.
  notifyLayouts: (names, starters, active) =>
    ipcRenderer.invoke('layouts-changed', { names, starters, active }),
  onApplyLayout: (callback) => {
    ipcRenderer.on('apply-layout', (_event, target) => callback(target));
  },
  onSaveLayout: (callback) => {
    ipcRenderer.on('save-layout', () => callback());
  },
  onResetLayout: (callback) => {
    ipcRenderer.on('reset-layout', () => callback());
  },
  removeLayoutMenuListeners: () => {
    ipcRenderer.removeAllListeners('apply-layout');
    ipcRenderer.removeAllListeners('save-layout');
    ipcRenderer.removeAllListeners('reset-layout');
  }
});

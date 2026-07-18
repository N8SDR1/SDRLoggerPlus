// Type declarations for Electron IPC API exposed via preload script
interface ElectronAPI {
  onOpenSettings: (callback: () => void) => void;
  removeOpenSettingsListener: () => void;
  onOpenAbout: (callback: () => void) => void;
  removeOpenAboutListener: () => void;
  restartApp: () => Promise<void>;
  openExternal: (url: string) => Promise<void>;
  getZoomLevel?: () => Promise<number>;
  setZoomLevel?: (level: number) => Promise<void>;
  onZoomLevelChanged?: (callback: (level: number) => void) => void;
  removeZoomLevelChangedListener?: () => void;
  // View > Layouts menu. Optional because they're absent in a browser (and in
  // an older desktop build), so every call site must guard.
  notifyLayouts?: (names: string[], active: string | null) => Promise<void>;
  onApplyLayout?: (callback: (name: string) => void) => void;
  onSaveLayout?: (callback: () => void) => void;
  onResetLayout?: (callback: () => void) => void;
  removeLayoutMenuListeners?: () => void;
}

declare global {
  interface Window {
    electronAPI?: ElectronAPI;
  }
}

export {};

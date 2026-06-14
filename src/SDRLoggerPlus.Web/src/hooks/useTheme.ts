import { useEffect } from 'react';
import { useSettingsStore } from '../store/settingsStore';
import { applyThemeToDocument, getSeedColors, PRESETS } from '../theme/themes';

/**
 * Manages theme application based on user settings.
 * Built-in light/dark are pure CSS classes; presets and custom colors are applied
 * as inline CSS-variable overrides on <html> (see theme/themes.ts).
 */
export function useTheme() {
  const theme = useSettingsStore((s) => s.settings.appearance.theme);
  const customColors = useSettingsStore((s) => s.settings.appearance.customColors);

  useEffect(() => {
    applyThemeToDocument(theme, customColors);

    const meta = document.querySelector('meta[name="theme-color"]');
    if (meta) {
      // Browser chrome color = the theme's body background.
      const bg =
        theme === 'custom' ? customColors.background :
        theme === 'light' ? '#eceae6' :
        PRESETS[theme]?.bg ?? getSeedColors('dark').background;
      meta.setAttribute('content', bg);
    }
  }, [theme, customColors]);
}

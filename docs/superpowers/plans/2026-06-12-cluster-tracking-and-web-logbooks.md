# Cluster Follow-Rig + Web Logbooks Settings — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional "Follow rig" mode to the DX Cluster panel that filters spots to the connected rig's live band+mode, and reorganize the QRZ/LOTW/Club Log settings into a single "Web Logbooks" category with sub-tabs.

**Architecture:** Both features are frontend-only (React + Zustand + TypeScript). Cluster tracking reads existing rig state from `appStore` and overrides the spot filter in `ClusterPlugin.tsx`; a new persisted `trackRig` cluster setting drives a toggle. The Web Logbooks change is pure settings-nav reorganization in `SettingsPanel.tsx` — the three existing config sections are reused unchanged under a sub-tab strip.

**Tech Stack:** React, Zustand, Vitest, Tailwind, lucide-react icons.

These are two independent features in one plan. Part 1 (Tasks 1–5) and Part 2 (Tasks 6–8) can be implemented and committed separately.

---

## File Structure

**Part 1 — Cluster Follow Rig**
- Create: `src/SDRLoggerPlus.Web/src/utils/rigTracking.ts` — pure `rigModeToSpotModes` helper.
- Create: `src/SDRLoggerPlus.Web/src/__tests__/utils/rigTracking.test.ts` — unit tests for the helper.
- Modify: `src/SDRLoggerPlus.Web/src/store/settingsStore.ts` — add `trackRig` to `ClusterSettings` + default.
- Modify: `src/SDRLoggerPlus.Web/src/components/MultiSelectDropdown.tsx` — add `disabled`/`title` props.
- Modify: `src/SDRLoggerPlus.Web/src/plugins/ClusterPlugin.tsx` — rig-state read, effective filter, toggle button, disabled dropdowns.

**Part 2 — Web Logbooks**
- Modify: `src/SDRLoggerPlus.Web/src/store/settingsStore.ts` — `SettingsSection` union: replace `'qrz' | 'lotw' | 'clublog'` with `'weblogbooks'`.
- Modify: `src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx` — nav array, render switch, new `WebLogbooksSection`.
- Modify: `src/SDRLoggerPlus.Web/src/__tests__/store/settingsStore.test.ts` — use a valid section id.

All commands run from `src/SDRLoggerPlus.Web` unless noted.

---

# PART 1 — DX Cluster: Follow Rig

### Task 1: `rigModeToSpotModes` pure helper + tests

**Files:**
- Create: `src/SDRLoggerPlus.Web/src/utils/rigTracking.ts`
- Test: `src/SDRLoggerPlus.Web/src/__tests__/utils/rigTracking.test.ts`

- [ ] **Step 1: Write the failing test**

Create `src/SDRLoggerPlus.Web/src/__tests__/utils/rigTracking.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import { rigModeToSpotModes } from '../../utils/rigTracking';

describe('rigModeToSpotModes', () => {
  it('maps phone modes to SSB', () => {
    expect(rigModeToSpotModes('USB')).toEqual(['SSB']);
    expect(rigModeToSpotModes('LSB')).toEqual(['SSB']);
    expect(rigModeToSpotModes('AM')).toEqual(['SSB']);
    expect(rigModeToSpotModes('FM')).toEqual(['SSB']);
  });

  it('maps CW variants to CW', () => {
    expect(rigModeToSpotModes('CW')).toEqual(['CW']);
    expect(rigModeToSpotModes('CW-R')).toEqual(['CW']);
  });

  it('maps FT8 and FT4 to themselves', () => {
    expect(rigModeToSpotModes('FT8')).toEqual(['FT8']);
    expect(rigModeToSpotModes('FT4')).toEqual(['FT4']);
  });

  it('maps RTTY to RTTY', () => {
    expect(rigModeToSpotModes('RTTY')).toEqual(['RTTY']);
  });

  it('maps generic digital rig modes to the digital family', () => {
    expect(rigModeToSpotModes('DIGU')).toEqual(['FT8', 'FT4', 'DIGI']);
    expect(rigModeToSpotModes('DATA')).toEqual(['FT8', 'FT4', 'DIGI']);
    expect(rigModeToSpotModes('PKT')).toEqual(['FT8', 'FT4', 'DIGI']);
  });

  it('is case-insensitive and trims', () => {
    expect(rigModeToSpotModes(' usb ')).toEqual(['SSB']);
    expect(rigModeToSpotModes('cw')).toEqual(['CW']);
  });

  it('returns null for unknown or empty modes (no mode filter)', () => {
    expect(rigModeToSpotModes('')).toBeNull();
    expect(rigModeToSpotModes(undefined)).toBeNull();
    expect(rigModeToSpotModes('SSTV')).toBeNull();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run src/__tests__/utils/rigTracking.test.ts`
Expected: FAIL — cannot resolve `../../utils/rigTracking`.

- [ ] **Step 3: Write the implementation**

Create `src/SDRLoggerPlus.Web/src/utils/rigTracking.ts`:

```ts
/**
 * Maps a rig's reported operating mode to the set of DX-cluster spot-filter
 * modes it should match. Returns null when the rig mode is unknown/empty,
 * meaning "do not constrain by mode" (band-only tracking).
 *
 * Spot-filter modes come from ClusterPlugin's MODE_OPTIONS:
 *   CW, SSB, FT8, FT4, RTTY, DIGI
 */
export function rigModeToSpotModes(rigMode: string | undefined | null): string[] | null {
  if (!rigMode) return null;
  const m = rigMode.trim().toUpperCase();
  if (m === '') return null;

  switch (m) {
    case 'USB':
    case 'LSB':
    case 'AM':
    case 'FM':
      return ['SSB'];
    case 'CW':
    case 'CW-R':
    case 'CWR':
      return ['CW'];
    case 'FT8':
      return ['FT8'];
    case 'FT4':
      return ['FT4'];
    case 'RTTY':
      return ['RTTY'];
    case 'DIGU':
    case 'DIGL':
    case 'DATA':
    case 'PKT':
    case 'PSK':
      return ['FT8', 'FT4', 'DIGI'];
    default:
      return null;
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run src/__tests__/utils/rigTracking.test.ts`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add src/SDRLoggerPlus.Web/src/utils/rigTracking.ts src/SDRLoggerPlus.Web/src/__tests__/utils/rigTracking.test.ts
git commit -m "feat(cluster): add rigModeToSpotModes helper for follow-rig filtering"
```

---

### Task 2: Add `trackRig` to cluster settings

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/store/settingsStore.ts` (interface `ClusterSettings` ~line 158; default `cluster` ~line 449)

- [ ] **Step 1: Add the field to the `ClusterSettings` interface**

In `src/SDRLoggerPlus.Web/src/store/settingsStore.ts`, change:

```ts
export interface ClusterSettings {
  connections: ClusterConnection[];
  /** Spothole.app REST aggregator — the default spot source (read-only, polled). */
  spotholeEnabled: boolean;
  /** Only show spots whose spotter resolves to this country; empty = all. */
  spotholeSpotterCountry: string;
}
```

to:

```ts
export interface ClusterSettings {
  connections: ClusterConnection[];
  /** Spothole.app REST aggregator — the default spot source (read-only, polled). */
  spotholeEnabled: boolean;
  /** Only show spots whose spotter resolves to this country; empty = all. */
  spotholeSpotterCountry: string;
  /** When true, the spot list follows the connected rig's live band + mode. */
  trackRig: boolean;
}
```

- [ ] **Step 2: Add the default value**

In the same file, change the default `cluster` block:

```ts
  cluster: {
    connections: [],
    spotholeEnabled: true,
    spotholeSpotterCountry: 'United States',
  },
```

to:

```ts
  cluster: {
    connections: [],
    spotholeEnabled: true,
    spotholeSpotterCountry: 'United States',
    trackRig: false,
  },
```

- [ ] **Step 3: Typecheck**

Run: `npx tsc -p tsconfig.json --noEmit`
Expected: PASS (no errors). The `trackRig` field is now required on `ClusterSettings`; if any other defaults/usages fail to compile, add `trackRig: false` there.

- [ ] **Step 4: Commit**

```bash
git add src/SDRLoggerPlus.Web/src/store/settingsStore.ts
git commit -m "feat(cluster): add persisted trackRig cluster setting"
```

---

### Task 3: Add `disabled`/`title` props to `MultiSelectDropdown`

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/components/MultiSelectDropdown.tsx`

- [ ] **Step 1: Extend the props interface**

Change:

```ts
interface MultiSelectDropdownProps {
  options: MultiSelectOption[];
  selected: string[];
  onChange: (selected: string[]) => void;
  placeholder: string;
  className?: string;
}
```

to:

```ts
interface MultiSelectDropdownProps {
  options: MultiSelectOption[];
  selected: string[];
  onChange: (selected: string[]) => void;
  placeholder: string;
  className?: string;
  disabled?: boolean;
  title?: string;
}
```

- [ ] **Step 2: Destructure the new props**

Change:

```ts
export function MultiSelectDropdown({
  options,
  selected,
  onChange,
  placeholder,
  className = '',
}: MultiSelectDropdownProps) {
```

to:

```ts
export function MultiSelectDropdown({
  options,
  selected,
  onChange,
  placeholder,
  className = '',
  disabled = false,
  title,
}: MultiSelectDropdownProps) {
```

- [ ] **Step 3: Apply disabled state to the trigger button**

Change the trigger `<button>` opening tag:

```tsx
      <button
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        className={`
          w-full flex items-center justify-between gap-2 px-3 py-2
          bg-dark-800 border rounded-lg text-sm text-left
          transition-colors duration-150
          ${hasSelection
            ? 'border-accent-primary/50 text-gray-200'
            : 'border-glass-100 text-gray-400'
          }
          ${isOpen ? 'border-accent-primary/70' : ''}
          hover:border-glass-200 focus:outline-none focus:border-accent-primary/50
        `}
      >
```

to:

```tsx
      <button
        type="button"
        disabled={disabled}
        title={title}
        onClick={() => setIsOpen(!isOpen)}
        className={`
          w-full flex items-center justify-between gap-2 px-3 py-2
          bg-dark-800 border rounded-lg text-sm text-left
          transition-colors duration-150
          ${hasSelection
            ? 'border-accent-primary/50 text-gray-200'
            : 'border-glass-100 text-gray-400'
          }
          ${isOpen ? 'border-accent-primary/70' : ''}
          ${disabled ? 'opacity-50 cursor-not-allowed hover:border-glass-100' : 'hover:border-glass-200'}
          focus:outline-none focus:border-accent-primary/50
        `}
      >
```

(A native `disabled` button does not fire `onClick`, so the menu cannot open while disabled.)

- [ ] **Step 4: Typecheck**

Run: `npx tsc -p tsconfig.json --noEmit`
Expected: PASS. Existing callers omit the new optional props, so they still compile.

- [ ] **Step 5: Commit**

```bash
git add src/SDRLoggerPlus.Web/src/components/MultiSelectDropdown.tsx
git commit -m "feat(ui): add disabled/title props to MultiSelectDropdown"
```

---

### Task 4: Wire Follow-Rig into `ClusterPlugin`

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/plugins/ClusterPlugin.tsx`

- [ ] **Step 1: Add imports**

Change the lucide import (line 2):

```tsx
import { RadioTower, Map, Settings, Plus, Trash2, X, Search } from 'lucide-react';
```

to:

```tsx
import { RadioTower, Map, Settings, Plus, Trash2, X, Search, Crosshair } from 'lucide-react';
```

Add the helper import after the existing `../store/appStore` import (line 12):

```tsx
import { useAppStore, Spot } from '../store/appStore';
import { useAgGridState } from '../hooks/useAgGridState';
import { rigModeToSpotModes } from '../utils/rigTracking';
```

- [ ] **Step 2: Read rig state and tracking flags**

In `ClusterPlugin`, just after the existing `const spots = useAppStore((state) => state.dxClusterSpots);` line (≈518), add:

```tsx
  // Rig state for "follow rig" tracking
  const selectedRadioId = useAppStore((state) => state.selectedRadioId);
  const radioStates = useAppStore((state) => state.radioStates);
  const rigState = selectedRadioId ? radioStates.get(selectedRadioId) : undefined;
  const rigConnected = !!rigState;
  const rigFreqHz = rigState?.frequencyHz;
  const rigMode = rigState?.mode;
```

Then, just after `const clusterConnections = settings.cluster.connections;` (≈536), add:

```tsx
  const trackRig = settings.cluster.trackRig;
  const tracking = trackRig && rigConnected;
```

- [ ] **Step 3: Override the spot filter when tracking**

Replace the entire `filteredSpots` memo (lines ≈556–602) with:

```tsx
  // Filter spots based on selected bands, modes, statuses, and search query.
  // When "follow rig" is active, the rig's live band/mode override the manual
  // Band/Mode dropdowns (the manual selections are preserved but ignored).
  const filteredSpots = useMemo(() => {
    if (!spots) return [];

    const query = searchQuery.trim().toLowerCase();

    const rigBand = tracking ? getBandFromFrequency((rigFreqHz ?? 0) / 1000) : null;
    const rigModes = tracking ? rigModeToSpotModes(rigMode) : null;

    // '?' = rig is outside any known band -> skip band filter rather than show nothing.
    const effectiveBands = tracking
      ? (rigBand && rigBand !== '?' ? [rigBand] : [])
      : selectedBands;
    // null = unknown rig mode -> skip mode filter (band-only tracking).
    const effectiveModes = tracking
      ? (rigModes ?? [])
      : selectedModes;

    return spots.filter(spot => {
      // Fuzzy search filter - matches against multiple fields
      if (query) {
        const searchableText = [
          spot.dxCall,
          spot.spotter,
          spot.dxStation?.country || spot.country,
          spot.comment,
          getBandFromFrequency(spot.frequency),
          spot.mode,
        ].filter(Boolean).join(' ').toLowerCase();

        if (!searchableText.includes(query)) return false;
      }

      // Band filter
      if (effectiveBands.length > 0) {
        const band = getBandFromFrequency(spot.frequency);
        if (!effectiveBands.includes(band)) return false;
      }

      // Mode filter
      if (effectiveModes.length > 0) {
        let spotMode = spot.mode?.toUpperCase();
        // Normalize USB/LSB to SSB
        if (spotMode === 'USB' || spotMode === 'LSB') spotMode = 'SSB';
        // Try to infer mode if not provided
        if (!spotMode) {
          spotMode = inferModeFromFrequency(spot.frequency)?.toUpperCase() || undefined;
        }
        if (!spotMode || !effectiveModes.includes(spotMode)) return false;
      }

      // Status filter
      if (selectedStatuses.length > 0) {
        const spotStatus = spot.status || 'none';
        if (!selectedStatuses.includes(spotStatus)) return false;
      }

      return true;
    });
  }, [spots, selectedBands, selectedModes, selectedStatuses, searchQuery, tracking, rigFreqHz, rigMode]);
```

- [ ] **Step 4: Add the toggle handler**

Find `clearAllFilters` (≈660) and add this handler right after it:

```tsx
  const handleToggleTrackRig = () => {
    updateClusterSettings({ trackRig: !trackRig });
    saveSettings();
  };
```

- [ ] **Step 5: Add the "Follow rig" toggle button to the filter bar**

In the filter bar, the search input is followed by three `MultiSelectDropdown`s (≈917). Insert the toggle button immediately before the Band dropdown (i.e., between the closing `</div>` of the search input block and `<MultiSelectDropdown options={BAND_OPTIONS} ...>`):

```tsx
            <button
              onClick={handleToggleTrackRig}
              disabled={!rigConnected}
              title={
                !rigConnected
                  ? 'Follow rig — waiting for a connected rig'
                  : trackRig
                  ? 'Following rig band/mode — click to stop'
                  : 'Follow rig band/mode'
              }
              className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm font-ui whitespace-nowrap border transition-colors ${
                tracking
                  ? 'bg-accent-primary/20 text-accent-primary border-accent-primary/30'
                  : 'bg-dark-800 text-dark-300 border-glass-100 hover:text-dark-200'
              } ${!rigConnected ? 'opacity-50 cursor-not-allowed' : ''}`}
            >
              <Crosshair className="w-4 h-4" />
              <span>Follow rig</span>
            </button>
```

- [ ] **Step 6: Disable the Band and Mode dropdowns while tracking**

Change the Band dropdown:

```tsx
            <MultiSelectDropdown
              options={BAND_OPTIONS}
              selected={selectedBands}
              onChange={setSelectedBands}
              placeholder="All Bands"
              className="w-32"
            />
```

to:

```tsx
            <MultiSelectDropdown
              options={BAND_OPTIONS}
              selected={selectedBands}
              onChange={setSelectedBands}
              placeholder="All Bands"
              className="w-32"
              disabled={tracking}
              title={tracking ? 'Following rig band' : undefined}
            />
```

Change the Mode dropdown:

```tsx
            <MultiSelectDropdown
              options={MODE_OPTIONS}
              selected={selectedModes}
              onChange={setSelectedModes}
              placeholder="All Modes"
              className="w-32"
            />
```

to:

```tsx
            <MultiSelectDropdown
              options={MODE_OPTIONS}
              selected={selectedModes}
              onChange={setSelectedModes}
              placeholder="All Modes"
              className="w-32"
              disabled={tracking}
              title={tracking ? 'Following rig mode' : undefined}
            />
```

- [ ] **Step 7: Typecheck and run frontend tests**

Run: `npx tsc -p tsconfig.json --noEmit && npm run test`
Expected: PASS — typecheck clean, all existing tests plus the new `rigTracking` tests green.

- [ ] **Step 8: Commit**

```bash
git add src/SDRLoggerPlus.Web/src/plugins/ClusterPlugin.tsx
git commit -m "feat(cluster): follow-rig toggle that tracks band/mode from connected rig"
```

---

### Task 5: Live verification (Part 1)

This project ships only after live verification against a real rig (per project rule — assumed fixtures have shipped broken before). Do not mark Part 1 done on unit tests alone.

- [ ] **Step 1: Launch the app** using the `run` skill (backend + Vite + Electron per CLAUDE.md).

- [ ] **Step 2: With no rig connected**, open the DX Cluster panel. Confirm the "Follow rig" button is dimmed/disabled with the "waiting for a connected rig" tooltip, and manual Band/Mode dropdowns still filter normally.

- [ ] **Step 3: Connect a rig.** Confirm the button becomes enabled. Click it: the button lights (accent), and the Band/Mode dropdowns dim with "Following rig …" tooltips.

- [ ] **Step 4: QSY the rig** across bands and modes (e.g., 20m USB → 40m CW → 20m FT8). Confirm the spot list re-filters live to match the rig's band+mode each time.

- [ ] **Step 5: Toggle off.** Confirm manual Band/Mode selections made before tracking are restored and applied.

- [ ] **Step 6: Restart the app.** Confirm the `trackRig` toggle state persisted.

- [ ] **Step 7:** Note any discrepancies (e.g., rig mode string not in the mapping table) and extend `rigModeToSpotModes` if a real rig reports an unmapped mode.

---

# PART 2 — Settings: Web Logbooks Category

### Task 6: Replace QRZ/LOTW/Club Log nav entries with a "Web Logbooks" category

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/store/settingsStore.ts` (`SettingsSection` union, ~line 289)
- Modify: `src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx` (nav array, render switch, new section component)

- [ ] **Step 1: Update the `SettingsSection` union**

In `src/SDRLoggerPlus.Web/src/store/settingsStore.ts` change:

```ts
export type SettingsSection = 'station' | 'qrz' | 'lotw' | 'clublog' | 'adifmonitor' | 'rbnalerts' | 'rotator' | 'database' | 'appearance' | 'map' | 'header' | 'ai' | 'spectrum' | 'backup' | 'hotlist' | 'wsjtx' | 'weather' | 'sat' | 'about';
```

to:

```ts
export type SettingsSection = 'station' | 'weblogbooks' | 'adifmonitor' | 'rbnalerts' | 'rotator' | 'database' | 'appearance' | 'map' | 'header' | 'ai' | 'spectrum' | 'backup' | 'hotlist' | 'wsjtx' | 'weather' | 'sat' | 'about';
```

- [ ] **Step 2: Replace the three nav entries with one**

In `src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx`, in `SETTINGS_SECTIONS`, replace these three entries:

```tsx
  {
    id: 'qrz',
    name: 'QRZ.com',
    icon: <Globe className="w-5 h-5" />,
    description: 'QRZ lookup credentials',
  },
  {
    id: 'lotw',
    name: 'LOTW',
    icon: <CloudUpload className="w-5 h-5" />,
    description: 'TQSL binary path for LOTW upload',
  },
  {
    id: 'clublog',
    name: 'Club Log',
    icon: <CloudUpload className="w-5 h-5" />,
    description: 'Real-time QSO upload to Club Log',
  },
```

with a single entry:

```tsx
  {
    id: 'weblogbooks',
    name: 'Web Logbooks',
    icon: <CloudUpload className="w-5 h-5" />,
    description: 'QRZ, LOTW, and Club Log integration',
  },
```

- [ ] **Step 3: Update the render switch**

In the `renderSection` switch, replace these three cases:

```tsx
      case 'qrz':
        return <QrzSettingsSection />;
      case 'lotw':
        return <LotwSettingsSection />;
```

```tsx
      case 'clublog':
        return <ClubLogSettingsSection />;
```

with a single case (place it where the `qrz` case was):

```tsx
      case 'weblogbooks':
        return <WebLogbooksSection />;
```

Note: the `clublog` case sits lower in the switch (≈3457); remove it too. After this step the switch must contain no remaining references to `'qrz'`, `'lotw'`, or `'clublog'`.

- [ ] **Step 4: Add the `WebLogbooksSection` component**

Add this component to `SettingsPanel.tsx` near the other section components (e.g., directly above `// Main Settings Panel Component`). `useState` is already imported at the top of the file.

```tsx
// Web Logbooks — groups QRZ, LOTW, and Club Log under one category with sub-tabs.
type WebLogbookTab = 'qrz' | 'lotw' | 'clublog';

function WebLogbooksSection() {
  const [tab, setTab] = useState<WebLogbookTab>('qrz');
  const tabs: { id: WebLogbookTab; label: string }[] = [
    { id: 'qrz', label: 'QRZ.com' },
    { id: 'lotw', label: 'LOTW' },
    { id: 'clublog', label: 'Club Log' },
  ];

  return (
    <div className="space-y-6">
      <div className="flex gap-1 border-b border-glass-100">
        {tabs.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-2 text-sm font-ui font-medium border-b-2 -mb-px transition-colors ${
              tab === t.id
                ? 'border-accent-primary text-accent-primary'
                : 'border-transparent text-dark-300 hover:text-dark-200'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'qrz' && <QrzSettingsSection />}
      {tab === 'lotw' && <LotwSettingsSection />}
      {tab === 'clublog' && <ClubLogSettingsSection />}
    </div>
  );
}
```

- [ ] **Step 5: Typecheck**

Run: `npx tsc -p tsconfig.json --noEmit`
Expected: PASS. If the compiler flags a leftover `'qrz'`/`'lotw'`/`'clublog'` comparison, that is a usage the union no longer allows — update it to `'weblogbooks'`.

- [ ] **Step 6: Commit**

```bash
git add src/SDRLoggerPlus.Web/src/store/settingsStore.ts src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx
git commit -m "feat(settings): combine QRZ/LOTW/Club Log into Web Logbooks category with sub-tabs"
```

---

### Task 7: Fix the settings store test

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/__tests__/store/settingsStore.test.ts` (≈line 51)

- [ ] **Step 1: Update the test to use a valid section id**

Change:

```ts
    it('sets active section', () => {
      useSettingsStore.getState().setActiveSection('qrz');
      expect(useSettingsStore.getState().activeSection).toBe('qrz');
    });
```

to:

```ts
    it('sets active section', () => {
      useSettingsStore.getState().setActiveSection('weblogbooks');
      expect(useSettingsStore.getState().activeSection).toBe('weblogbooks');
    });
```

- [ ] **Step 2: Run the tests**

Run: `npm run test`
Expected: PASS — all suites green.

- [ ] **Step 3: Commit**

```bash
git add src/SDRLoggerPlus.Web/src/__tests__/store/settingsStore.test.ts
git commit -m "test(settings): use weblogbooks section id"
```

---

### Task 8: Verification (Part 2)

- [ ] **Step 1: Full typecheck + tests**

Run: `npx tsc -p tsconfig.json --noEmit && npm run test`
Expected: PASS.

- [ ] **Step 2: Launch the app** (via the `run` skill) and open Settings.

- [ ] **Step 3:** Confirm the left nav shows a single "Web Logbooks" entry (no separate QRZ / LOTW / Club Log entries), and the detail header reads "Web Logbooks".

- [ ] **Step 4:** Click each sub-tab (QRZ.com / LOTW / Club Log). Confirm each renders its existing config form with saved values intact, and that editing + Save works from each tab.

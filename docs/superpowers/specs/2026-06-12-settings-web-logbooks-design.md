# Settings: Web Logbooks Category — Design

**Date:** 2026-06-12
**Status:** Approved

## Goal

Combine the three settings nav entries QRZ.com, LOTW, and Club Log into a
single category named **Web Logbooks**, with each service keeping its own
config section inside, switched via sub-tabs.

## Approach

Pure navigation reorganization in `SettingsPanel.tsx`. No settings data,
store-slice, or backend changes — QRZ/LOTW/Club Log configs keep their
existing shapes and persistence.

## Changes

- **Nav:** Remove the `qrz`, `lotw`, and `clublog` entries from
  `SETTINGS_SECTIONS`; add one `weblogbooks` entry in their place (position:
  where `qrz` is today). Name "Web Logbooks", icon `CloudUpload`,
  description "QRZ, LOTW, and Club Log integration".
- **Component:** New `WebLogbooksSection` renders a sub-tab strip —
  `QRZ.com | LOTW | Club Log` — above the existing `QrzSettingsSection`,
  `LotwSettingsSection`, and `ClubLogSettingsSection`, which are reused
  unchanged. Active sub-tab is local component state, defaulting to QRZ.
  Tab styling follows the existing in-app tab idiom (accent underline /
  tint on active).
- **Types:** `SettingsSection` union in `settingsStore.ts` replaces
  `'qrz' | 'lotw' | 'clublog'` with `'weblogbooks'`. The section `switch`
  in `SettingsPanel.tsx` is updated accordingly.
- **Tests:** `settingsStore.test.ts` uses `setActiveSection('qrz')` —
  update to a still-valid section id.
- **Migration:** `activeSection` is transient UI state (not persisted), so
  no stored-data migration is needed. Verified: no deep links call
  `setActiveSection` with the removed ids outside that one test.

## Testing

- Existing settings tests keep passing after the section-id update.
- Manual check: open Settings → Web Logbooks, switch all three sub-tabs,
  confirm each form loads its saved values and Save works from each tab.

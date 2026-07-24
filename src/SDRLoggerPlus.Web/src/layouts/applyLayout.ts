import { Model, type IJsonModel } from 'flexlayout-react';
import { findStarterLayout } from './starterLayouts';
import { api } from '../api/client';
import { useLayoutStore } from '../store/layoutStore';
import { notifyLayoutsChanged, ACTIVE_KEY } from '../hooks/useLayoutMenu';

// A layout reference is "starter:Name" or "saved:Name" — the same scheme the
// View > Layouts menu and the active-layout marker already use.

export interface LayoutRef {
  kind: 'starter' | 'saved';
  name: string;
}

export function parseLayoutRef(ref: string): LayoutRef | null {
  const i = ref.indexOf(':');
  if (i < 0) return null;
  const kind = ref.slice(0, i);
  if (kind !== 'starter' && kind !== 'saved') return null;
  return { kind, name: ref.slice(i + 1) };
}

/** Human label for a ref, e.g. "Satellite (built-in)" or "My Contest". */
export function layoutRefLabel(ref: string): string {
  const parsed = parseLayoutRef(ref);
  if (!parsed) return ref;
  return parsed.kind === 'starter' ? `${parsed.name} (built-in)` : parsed.name;
}

async function refToJson(ref: string): Promise<IJsonModel | null> {
  const parsed = parseLayoutRef(ref);
  if (!parsed) return null;
  if (parsed.kind === 'starter') return findStarterLayout(parsed.name)?.layout ?? null;
  const list = await api.getSavedLayouts();
  const slot = list.find((l) => l.name === parsed.name);
  return slot ? (JSON.parse(slot.layoutJson) as IJsonModel) : null;
}

// Round-trip through FlexLayout so both sides of a comparison are normalised the
// same way FlexLayout normalises on load — otherwise the live layout (already a
// toJson() output) would always look "different" from a freshly-read preset.
function normalise(json: IJsonModel): string {
  try {
    return JSON.stringify(Model.fromJson(json as Parameters<typeof Model.fromJson>[0]).toJson());
  } catch {
    return JSON.stringify(json);
  }
}

/** The ref of the layout currently marked active, or null if none. */
export function activeLayoutRef(): string | null {
  try {
    return localStorage.getItem(ACTIVE_KEY);
  } catch {
    return null;
  }
}

/** Apply a "starter:Name"/"saved:Name" ref to the live layout. False if not found. */
export async function applyLayoutRef(ref: string): Promise<boolean> {
  const json = await refToJson(ref);
  if (!json) return false;
  useLayoutStore.getState().setLayout(json);
  notifyLayoutsChanged(ref);
  return true;
}

/**
 * True when the live arrangement differs from the preset last applied — i.e. there
 * are unsaved changes. Compared on the normalised form, so FlexLayout's own load-time
 * tidying doesn't read as a change. Returns false when nothing is marked active
 * (there's no baseline to differ from).
 */
export async function liveLayoutDiffersFromActive(): Promise<boolean> {
  const active = activeLayoutRef();
  if (!active) return false;
  const presetJson = await refToJson(active);
  if (!presetJson) return false;
  return normalise(useLayoutStore.getState().layout) !== normalise(presetJson);
}

/** Overwrite the active saved slot with the current live layout. No-op unless active is a saved slot. */
export async function saveLiveToActiveSlot(): Promise<void> {
  const parsed = activeLayoutRef() ? parseLayoutRef(activeLayoutRef()!) : null;
  if (!parsed || parsed.kind !== 'saved') return;
  const layoutJson = JSON.stringify(useLayoutStore.getState().layout);
  await api.saveNamedLayout(parsed.name, layoutJson);
}

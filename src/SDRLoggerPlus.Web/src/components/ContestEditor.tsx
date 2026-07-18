import { useState } from 'react';
import { createPortal } from 'react-dom';
import { X, Plus, Trash2, Loader2, Save } from 'lucide-react';
import { api, ContestDefinition, ContestField } from '../api/client';

const ALL_BANDS = ['160m', '80m', '40m', '30m', '20m', '17m', '15m', '12m', '10m', '6m', '2m'];
const ALL_MODES = ['CW', 'SSB', 'FT8', 'FT4', 'RTTY', 'FM'];
// Enum string values as the server serializes them (case-insensitive on read).
const FIELD_TYPES = ['Rst', 'Serial', 'Zone', 'State', 'Section', 'Grid', 'Name', 'Power', 'Text'];
const MULT_SOURCES = ['Dxcc', 'CqZone', 'ItuZone', 'State', 'Section', 'WpxPrefix', 'Grid', 'Continent'];
const DUPE_RULES = ['PerBand', 'PerBandMode', 'PerContest'];
const SERIAL_MODES = ['None', 'PerBand', 'AllBand'];
// Per-QSO exchange branching: a field can apply always, only to in-area (W/VE)
// stations, or only to DX. Stored as the server's ContestRole ('InArea'/'Dx');
// 'Always' serializes to undefined.
const APPLIES_TO: { value: string; label: string }[] = [
  { value: 'Always', label: 'Always' },
  { value: 'InArea', label: 'In-area (W/VE)' },
  { value: 'Dx', label: 'DX only' },
];
// Mode classes the engine recognizes for per-mode QSO points.
const POINT_MODES: { key: string; label: string }[] = [
  { key: 'CW', label: 'CW' },
  { key: 'PH', label: 'Phone' },
  { key: 'RTTY', label: 'RTTY' },
];

function blankDefinition(): ContestDefinition {
  return {
    id: '',
    name: '',
    cabrilloName: 'OTHER',
    builtin: false,
    bands: ['160m', '80m', '40m', '20m', '15m', '10m'],
    modes: ['CW'],
    sentExchange: [{ key: 'rst', label: 'RST', type: 'Rst', width: 3, required: true }],
    rcvdExchange: [{ key: 'rst', label: 'RST', type: 'Rst', width: 3, required: true }],
    qsoPoints: { default: 1 },
    multiplierRules: [],
    dupeRule: 'PerBandMode',
    serial: 'None',
  };
}

// Set (or clear, when value is undefined) a per-mode base-points entry. Setting
// RTTY also sets DIGI so FT8/FT4 QSOs (mode class "DIGI") score like RTTY, matching
// the built-in definitions' behavior.
function setByMode(
  points: ContestDefinition['qsoPoints'],
  key: string,
  value: number | undefined
): ContestDefinition['qsoPoints'] {
  const byMode: Record<string, number> = { ...(points.byMode ?? {}) };
  const apply = (k: string) => { if (value === undefined) delete byMode[k]; else byMode[k] = value; };
  apply(key);
  if (key === 'RTTY') apply('DIGI');
  return { ...points, byMode: Object.keys(byMode).length ? byMode : undefined };
}

interface Props {
  /** A draft to edit (clone/edit), or null for a fresh contest. */
  initial: ContestDefinition | null;
  onClose: () => void;
  onSaved: () => void;
}

export function ContestEditor({ initial, onClose, onSaved }: Props) {
  const [def, setDef] = useState<ContestDefinition>(initial ?? blankDefinition());
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Power multipliers edited as ordered rows (class → factor); serialized to the
  // definition's powerMultipliers object on save. Empty ⇒ no power multiplier.
  const [powerRows, setPowerRows] = useState<{ cls: string; factor: number }[]>(
    () => Object.entries(initial?.powerMultipliers ?? {}).map(([cls, factor]) => ({ cls, factor }))
  );

  const set = (patch: Partial<ContestDefinition>) => setDef((d) => ({ ...d, ...patch }));

  const toggle = (list: string[], value: string): string[] =>
    list.includes(value) ? list.filter((v) => v !== value) : [...list, value];

  const updateField = (which: 'sentExchange' | 'rcvdExchange', idx: number, patch: Partial<ContestField>) => {
    setDef((d) => {
      const fields = d[which].slice();
      fields[idx] = { ...fields[idx], ...patch };
      return { ...d, [which]: fields };
    });
  };

  const addField = (which: 'sentExchange' | 'rcvdExchange') =>
    setDef((d) => ({
      ...d,
      // Blank key ⇒ derived from type on save (with collision dedupe); the author
      // can override it in the key box.
      [which]: [...d[which], { key: '', label: 'Field', type: 'Text', width: 6, required: false }],
    }));

  const removeField = (which: 'sentExchange' | 'rcvdExchange', idx: number) =>
    setDef((d) => ({ ...d, [which]: d[which].filter((_, i) => i !== idx) }));

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      // Use the author's explicit key when set, else derive from the type; dedupe
      // collisions so two same-type fields (e.g. two Text fields, or a state + a
      // serial branch) don't clobber each other when the engine reads by key.
      const withKeys = (fields: ContestField[]) => {
        const seen = new Map<string, number>();
        return fields.map((f) => {
          let key = (f.key || '').trim() || f.type.toLowerCase();
          const n = seen.get(key) ?? 0;
          seen.set(key, n + 1);
          if (n > 0) key = `${key}${n + 1}`;
          return { ...f, key, label: f.label || f.type };
        });
      };
      const powerMultipliers = powerRows.length
        ? Object.fromEntries(
            powerRows
              .filter((r) => r.cls.trim())
              .map((r) => [r.cls.trim().toUpperCase(), r.factor])
          )
        : undefined;
      await api.saveContestDefinition({
        ...def,
        builtin: false,
        sentExchange: withKeys(def.sentExchange),
        rcvdExchange: withKeys(def.rcvdExchange),
        powerMultipliers: powerMultipliers && Object.keys(powerMultipliers).length ? powerMultipliers : undefined,
      });
      onSaved();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to save contest');
    } finally {
      setSaving(false);
    }
  };

  return createPortal(
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-dark-800 rounded-lg border border-glass-200 w-full max-w-2xl max-h-[90vh] flex flex-col">
        <div className="flex items-center justify-between px-5 py-3 border-b border-glass-100">
          <h3 className="text-base font-ui font-semibold text-white">
            {initial ? `Edit ${def.name || 'contest'}` : 'New contest'}
          </h3>
          <button onClick={onClose} className="text-gray-400 hover:text-white" title="Close">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-5 space-y-4">
          {/* Names */}
          <div className="grid grid-cols-2 gap-3">
            <label className="text-xs text-gray-400">
              Name
              <input className="glass-input w-full text-sm mt-1 px-2 py-1.5" value={def.name}
                onChange={(e) => set({ name: e.target.value })} placeholder="My Contest" />
            </label>
            <label className="text-xs text-gray-400">
              Cabrillo name
              <input className="glass-input w-full text-sm mt-1 px-2 py-1.5" value={def.cabrilloName}
                onChange={(e) => set({ cabrilloName: e.target.value.toUpperCase() })} placeholder="OTHER" />
            </label>
          </div>

          {/* Bands / Modes */}
          <Section title="Bands">
            <div className="flex flex-wrap gap-1.5">
              {ALL_BANDS.map((b) => (
                <Chip key={b} label={b} active={def.bands.includes(b)}
                  onClick={() => set({ bands: toggle(def.bands, b) })} />
              ))}
            </div>
          </Section>
          <Section title="Modes">
            <div className="flex flex-wrap gap-1.5">
              {ALL_MODES.map((m) => (
                <Chip key={m} label={m} active={def.modes.includes(m)}
                  onClick={() => set({ modes: toggle(def.modes, m) })} />
              ))}
            </div>
          </Section>

          {/* Rules */}
          <div className="grid grid-cols-2 gap-3">
            <label className="text-xs text-gray-400">
              Dupe rule
              <select className="glass-input w-full text-sm mt-1 px-2 py-1.5" value={def.dupeRule}
                onChange={(e) => set({ dupeRule: e.target.value as ContestDefinition['dupeRule'] })}>
                {DUPE_RULES.map((r) => <option key={r} value={r}>{r}</option>)}
              </select>
            </label>
            <label className="text-xs text-gray-400">
              Serial numbers
              <select className="glass-input w-full text-sm mt-1 px-2 py-1.5" value={def.serial}
                onChange={(e) => set({ serial: e.target.value as ContestDefinition['serial'] })}>
                {SERIAL_MODES.map((r) => <option key={r} value={r}>{r}</option>)}
              </select>
            </label>
          </div>

          {/* Points */}
          <Section title="QSO points">
            <div className="grid grid-cols-4 gap-2">
              <PointInput label="Default" value={def.qsoPoints.default}
                onChange={(v) => set({ qsoPoints: { ...def.qsoPoints, default: v ?? 1 } })} />
              <PointInput label="Same country" value={def.qsoPoints.sameCountry}
                onChange={(v) => set({ qsoPoints: { ...def.qsoPoints, sameCountry: v } })} />
              <PointInput label="Same cont." value={def.qsoPoints.sameContinent}
                onChange={(v) => set({ qsoPoints: { ...def.qsoPoints, sameContinent: v } })} />
              <PointInput label="Other cont." value={def.qsoPoints.otherContinent}
                onChange={(v) => set({ qsoPoints: { ...def.qsoPoints, otherContinent: v } })} />
            </div>
            {/* Per-mode base points (used when no relation override matches) —
                leave blank to fall through to Default. */}
            <div className="grid grid-cols-3 gap-2 mt-2">
              {POINT_MODES.map((m) => (
                <PointInput
                  key={m.key}
                  label={`${m.label} pts`}
                  value={def.qsoPoints.byMode?.[m.key]}
                  onChange={(v) => set({ qsoPoints: setByMode(def.qsoPoints, m.key, v) })}
                />
              ))}
            </div>
          </Section>

          {/* Exchange fields */}
          <ExchangeEditor title="Received exchange" fields={def.rcvdExchange} showAppliesTo
            onAdd={() => addField('rcvdExchange')}
            onRemove={(i) => removeField('rcvdExchange', i)}
            onChange={(i, p) => updateField('rcvdExchange', i, p)} />
          <ExchangeEditor title="Sent exchange" fields={def.sentExchange} showAppliesTo={false}
            onAdd={() => addField('sentExchange')}
            onRemove={(i) => removeField('sentExchange', i)}
            onChange={(i, p) => updateField('sentExchange', i, p)} />

          {/* Multipliers */}
          <Section title="Multipliers">
            <div className="space-y-1.5">
              {def.multiplierRules.map((rule, i) => (
                <div key={i} className="flex items-center gap-2">
                  <select className="glass-input text-sm px-2 py-1.5 flex-1" value={rule.source}
                    onChange={(e) => setDef((d) => {
                      const rules = d.multiplierRules.slice();
                      rules[i] = { ...rules[i], source: e.target.value };
                      return { ...d, multiplierRules: rules };
                    })}>
                    {MULT_SOURCES.map((s) => <option key={s} value={s}>{s}</option>)}
                  </select>
                  <label className="text-xs text-gray-400 flex items-center gap-1">
                    <input type="checkbox" checked={rule.perBand}
                      onChange={(e) => setDef((d) => {
                        const rules = d.multiplierRules.slice();
                        rules[i] = { ...rules[i], perBand: e.target.checked };
                        return { ...d, multiplierRules: rules };
                      })} />
                    per band
                  </label>
                  <label className="text-xs text-gray-400 flex items-center gap-1">
                    <input type="checkbox" checked={rule.perMode ?? false}
                      onChange={(e) => setDef((d) => {
                        const rules = d.multiplierRules.slice();
                        rules[i] = { ...rules[i], perMode: e.target.checked };
                        return { ...d, multiplierRules: rules };
                      })} />
                    per mode
                  </label>
                  <button className="text-gray-500 hover:text-red-400"
                    onClick={() => setDef((d) => ({ ...d, multiplierRules: d.multiplierRules.filter((_, x) => x !== i) }))}>
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              ))}
              <button className="text-xs text-accent-primary flex items-center gap-1 hover:underline"
                onClick={() => setDef((d) => ({ ...d, multiplierRules: [...d.multiplierRules, { source: 'Dxcc', perBand: true }] }))}>
                <Plus className="w-3 h-3" /> Add multiplier
              </button>
            </div>
          </Section>

          {/* Power multipliers — scale the final score by the operator's power class
              (e.g. Field Day QRP ×5, many QSO parties QRP ×2). Class names are matched
              case-insensitively against the operator's declared power class. */}
          <Section title="Power multiplier">
            <div className="space-y-1.5">
              {powerRows.map((row, i) => (
                <div key={i} className="flex items-center gap-2">
                  <input className="glass-input text-sm px-2 py-1.5 w-28" value={row.cls}
                    placeholder="Class (QRP)"
                    onChange={(e) => setPowerRows((rows) => {
                      const next = rows.slice();
                      next[i] = { ...next[i], cls: e.target.value };
                      return next;
                    })} />
                  <span className="text-xs text-gray-500">×</span>
                  <input type="number" step="0.5" className="glass-input text-sm px-2 py-1.5 w-20" value={row.factor}
                    onChange={(e) => setPowerRows((rows) => {
                      const next = rows.slice();
                      next[i] = { ...next[i], factor: Number(e.target.value) || 0 };
                      return next;
                    })} />
                  <button className="text-gray-500 hover:text-red-400"
                    onClick={() => setPowerRows((rows) => rows.filter((_, x) => x !== i))}>
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              ))}
              <button className="text-xs text-accent-primary flex items-center gap-1 hover:underline"
                onClick={() => setPowerRows((rows) => [...rows, { cls: '', factor: 1 }])}>
                <Plus className="w-3 h-3" /> Add power class
              </button>
              {powerRows.length === 0 && (
                <div className="text-xs text-gray-500">No power multiplier — final score = points × mults.</div>
              )}
            </div>
          </Section>

          {error && <div className="text-sm text-red-400">{error}</div>}
        </div>

        <div className="flex justify-end gap-2 px-5 py-3 border-t border-glass-100">
          <button onClick={onClose} className="glass-button px-4 py-2 text-sm">Cancel</button>
          <button onClick={save} disabled={saving || !def.name.trim()}
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-accent-primary/20 border border-accent-primary/50 text-accent-primary hover:bg-accent-primary/30 text-sm font-medium disabled:opacity-50">
            {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
            Save
          </button>
        </div>
      </div>
    </div>,
    document.body
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="text-xs font-semibold text-gray-300 uppercase tracking-wider mb-1.5">{title}</div>
      {children}
    </div>
  );
}

function Chip({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return (
    <button onClick={onClick}
      className={`px-2 py-0.5 rounded text-xs border transition-colors ${
        active
          ? 'bg-accent-primary/20 border-accent-primary/50 text-accent-primary'
          : 'bg-dark-700/50 border-glass-100 text-gray-400 hover:bg-dark-600/50'
      }`}>
      {label}
    </button>
  );
}

function PointInput({ label, value, onChange }: { label: string; value?: number; onChange: (v: number | undefined) => void }) {
  return (
    <label className="text-xs text-gray-400">
      {label}
      <input type="number" className="glass-input w-full text-sm mt-1 px-2 py-1.5"
        value={value ?? ''} placeholder="—"
        onChange={(e) => onChange(e.target.value === '' ? undefined : Number(e.target.value))} />
    </label>
  );
}

function ExchangeEditor({ title, fields, showAppliesTo, onAdd, onRemove, onChange }: {
  title: string;
  fields: ContestField[];
  // Only the received exchange branches per worked station, so the "Applies to"
  // control is hidden for the sent exchange.
  showAppliesTo: boolean;
  onAdd: () => void;
  onRemove: (i: number) => void;
  onChange: (i: number, patch: Partial<ContestField>) => void;
}) {
  return (
    <Section title={title}>
      <div className="space-y-2">
        {fields.map((f, i) => (
          <div key={i} className="rounded-lg border border-glass-100 bg-dark-700/30 p-2 space-y-1.5">
            <div className="flex items-center gap-2">
              <select className="glass-input text-sm px-2 py-1.5 w-28" value={f.type}
                onChange={(e) => onChange(i, { type: e.target.value })}>
                {FIELD_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
              </select>
              <input className="glass-input text-sm px-2 py-1.5 flex-1" value={f.label}
                placeholder="Label" onChange={(e) => onChange(i, { label: e.target.value })} />
              <button className="text-gray-500 hover:text-red-400" onClick={() => onRemove(i)}
                disabled={fields.length <= 1} title="Remove field">
                <Trash2 className="w-4 h-4" />
              </button>
            </div>
            <div className="flex items-center gap-2 flex-wrap">
              <input className="glass-input text-xs px-2 py-1 w-24 font-mono" value={f.key}
                placeholder="key" title="Machine key (auto from type if blank)"
                onChange={(e) => onChange(i, { key: e.target.value })} />
              <label className="text-xs text-gray-400 flex items-center gap-1" title="Field width (rem)">
                w
                <input type="number" className="glass-input text-xs px-1.5 py-1 w-14" value={f.width}
                  onChange={(e) => onChange(i, { width: Number(e.target.value) || 0 })} />
              </label>
              <label className="text-xs text-gray-400 flex items-center gap-1">
                <input type="checkbox" checked={f.required ?? false}
                  onChange={(e) => onChange(i, { required: e.target.checked })} />
                required
              </label>
              {showAppliesTo && (
                <label className="text-xs text-gray-400 flex items-center gap-1 ml-auto" title="Show this field only for this worked-station class">
                  shows for
                  <select className="glass-input text-xs px-1.5 py-1"
                    value={!f.appliesTo || f.appliesTo === 'All' ? 'Always' : f.appliesTo === 'InArea' ? 'InArea' : 'Dx'}
                    onChange={(e) => onChange(i, {
                      appliesTo: e.target.value === 'Always' ? undefined
                        : (e.target.value as ContestField['appliesTo']),
                    })}>
                    {APPLIES_TO.map((a) => <option key={a.value} value={a.value}>{a.label}</option>)}
                  </select>
                </label>
              )}
            </div>
          </div>
        ))}
        <button className="text-xs text-accent-primary flex items-center gap-1 hover:underline" onClick={onAdd}>
          <Plus className="w-3 h-3" /> Add field
        </button>
      </div>
    </Section>
  );
}

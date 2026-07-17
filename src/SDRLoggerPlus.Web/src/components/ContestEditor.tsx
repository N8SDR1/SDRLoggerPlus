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
      [which]: [...d[which], { key: 'text', label: 'Field', type: 'Text', width: 6 }],
    }));

  const removeField = (which: 'sentExchange' | 'rcvdExchange', idx: number) =>
    setDef((d) => ({ ...d, [which]: d[which].filter((_, i) => i !== idx) }));

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      // Derive a sensible field key from the chosen type (the engine reads by key).
      const withKeys = (fields: ContestField[]) =>
        fields.map((f) => ({ ...f, key: f.type.toLowerCase(), label: f.label || f.type }));
      await api.saveContestDefinition({
        ...def,
        builtin: false,
        sentExchange: withKeys(def.sentExchange),
        rcvdExchange: withKeys(def.rcvdExchange),
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
          </Section>

          {/* Exchange fields */}
          <ExchangeEditor title="Received exchange" fields={def.rcvdExchange}
            onAdd={() => addField('rcvdExchange')}
            onRemove={(i) => removeField('rcvdExchange', i)}
            onChange={(i, p) => updateField('rcvdExchange', i, p)} />
          <ExchangeEditor title="Sent exchange" fields={def.sentExchange}
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

function ExchangeEditor({ title, fields, onAdd, onRemove, onChange }: {
  title: string;
  fields: ContestField[];
  onAdd: () => void;
  onRemove: (i: number) => void;
  onChange: (i: number, patch: Partial<ContestField>) => void;
}) {
  return (
    <Section title={title}>
      <div className="space-y-1.5">
        {fields.map((f, i) => (
          <div key={i} className="flex items-center gap-2">
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
        ))}
        <button className="text-xs text-accent-primary flex items-center gap-1 hover:underline" onClick={onAdd}>
          <Plus className="w-3 h-3" /> Add field
        </button>
      </div>
    </Section>
  );
}

import { useState } from 'react';
import { Plus, Trash2, ChevronDown, ChevronUp } from 'lucide-react';
import { useSettingsStore, type DecodeAlertRule } from '../../store/settingsStore';

const CONTINENTS = ['NA', 'EU', 'AS', 'SA', 'AF', 'OC'];
const CALL_AREAS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
const BANDS = ['160m', '80m', '60m', '40m', '30m', '20m', '17m', '15m', '12m', '10m', '6m', '2m', '70cm'];
const MODES = ['FT8', 'FT4', 'JT65', 'JT9', 'MSK144'];

function makeRule(partial?: Partial<DecodeAlertRule>): DecodeAlertRule {
  return {
    id: crypto.randomUUID(),
    enabled: true,
    name: 'New rule',
    newDxcc: false, newBand: false, newZone: false, newGrid: false,
    continents: [], dxccEntities: [], callAreas: [], prefixes: [], gridFields: [],
    bands: [], modes: [],
    sound: true, voice: false, popup: true,
    cooldownMinutes: 10,
    ...partial,
  };
}

const PRESETS: { label: string; rule: () => Partial<DecodeAlertRule> }[] = [
  { label: 'New DXCC — anywhere', rule: () => ({ name: 'New DXCC', newDxcc: true }) },
  { label: 'Needed grid — North America', rule: () => ({ name: 'Needed grid · NA', newGrid: true, continents: ['NA'] }) },
  { label: 'Needed grid — US call areas', rule: () => ({ name: 'Needed grid · US', newGrid: true, prefixes: ['W', 'K', 'N', 'A'] }) },
  { label: 'New zone — anywhere', rule: () => ({ name: 'New zone', newZone: true }) },
];

export function DecodeAlertsSection() {
  const settings = useSettingsStore((s) => s.settings.decodeAlerts);
  const update = useSettingsStore((s) => s.updateDecodeAlertsSettings);
  const setActiveSection = useSettingsStore((s) => s.setActiveSection);

  const setRules = (rules: DecodeAlertRule[]) => update({ rules });
  const patchRule = (id: string, partial: Partial<DecodeAlertRule>) =>
    setRules(settings.rules.map((r) => (r.id === id ? { ...r, ...partial } : r)));
  const addRule = (partial?: Partial<DecodeAlertRule>) => setRules([...settings.rules, makeRule(partial)]);
  const removeRule = (id: string) => setRules(settings.rules.filter((r) => r.id !== id));

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold font-ui text-dark-200 mb-1">Digital Decode Alerts</h3>
        <p className="text-sm text-dark-300">
          Alert on needed stations in the digital decode stream (FT8/FT4 from WSJT-X, JTDX, MSHV).
          Each rule matches an award need <span className="text-dark-200">and</span> every scope filter you set —
          e.g. <span className="text-dark-200">needed grids, North America only</span>. Voice uses your shared{' '}
          <button className="text-accent-primary hover:underline" onClick={() => setActiveSection('voice')}>Voice settings</button>.
        </p>
      </div>

      {/* Master switch */}
      <label className="flex items-center gap-3 p-3 bg-dark-700/50 rounded-lg border border-glass-100 cursor-pointer">
        <input
          type="checkbox"
          checked={settings.enabled}
          onChange={(e) => update({ enabled: e.target.checked })}
          className="w-4 h-4 accent-cyan-400"
        />
        <div>
          <div className="text-sm font-medium text-dark-200">Enable decode alerts</div>
          <div className="text-xs text-dark-400">Master switch — off means no rule fires, whatever their state.</div>
        </div>
      </label>

      {/* Presets */}
      <div>
        <div className="text-xs font-medium text-dark-300 mb-1.5">Quick add</div>
        <div className="flex flex-wrap gap-1.5">
          {PRESETS.map((p) => (
            <button
              key={p.label}
              onClick={() => addRule(p.rule())}
              className="px-2.5 py-1 rounded border border-dark-500 bg-dark-700 text-dark-200 hover:bg-dark-600 hover:border-accent-primary text-xs"
            >
              + {p.label}
            </button>
          ))}
        </div>
      </div>

      {/* Rules */}
      <div className="space-y-3">
        {settings.rules.length === 0 ? (
          <div className="p-4 bg-dark-700/40 rounded-lg border border-dashed border-glass-100 text-sm text-dark-400 text-center">
            No rules yet — use a Quick-add preset above, or the button below.
          </div>
        ) : (
          settings.rules.map((rule) => (
            <RuleCard key={rule.id} rule={rule} onPatch={(p) => patchRule(rule.id, p)} onRemove={() => removeRule(rule.id)} />
          ))
        )}

        <button
          onClick={() => addRule()}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded border border-dark-500 text-dark-200 hover:bg-dark-700 text-sm"
        >
          <Plus className="w-4 h-4" /> Add rule
        </button>
      </div>
    </div>
  );
}

// ─── rule card ─────────────────────────────────────────────────────────────

function RuleCard({ rule, onPatch, onRemove }: {
  rule: DecodeAlertRule;
  onPatch: (p: Partial<DecodeAlertRule>) => void;
  onRemove: () => void;
}) {
  const [open, setOpen] = useState(true);

  const toggleIn = <T,>(arr: T[], val: T): T[] =>
    arr.includes(val) ? arr.filter((x) => x !== val) : [...arr, val];

  return (
    <div className={`rounded-lg border ${rule.enabled ? 'border-glass-100' : 'border-dark-600 opacity-70'} bg-dark-700/50`}>
      <div className="flex items-center gap-2 p-3">
        <Pill selected={rule.enabled} onClick={() => onPatch({ enabled: !rule.enabled })} title="Enable / disable this rule">
          {rule.enabled ? 'ON' : 'OFF'}
        </Pill>
        <input
          value={rule.name}
          onChange={(e) => onPatch({ name: e.target.value })}
          className="glass-input flex-1 text-sm"
          placeholder="Rule name"
        />
        <button onClick={() => setOpen(!open)} className="p-1 text-dark-300 hover:text-dark-100" title={open ? 'Collapse' : 'Expand'}>
          {open ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
        </button>
        <button onClick={onRemove} className="p-1 text-accent-danger hover:text-red-400" title="Delete rule">
          <Trash2 className="w-4 h-4" />
        </button>
      </div>

      {open && (
        <div className="px-3 pb-3 pt-3 space-y-3 border-t border-glass-100">
          <Field label="Alert on (needed)">
            <Pill selected={rule.newDxcc} onClick={() => onPatch({ newDxcc: !rule.newDxcc })}>New DXCC</Pill>
            <Pill selected={rule.newBand} onClick={() => onPatch({ newBand: !rule.newBand })}>New Band</Pill>
            <Pill selected={rule.newZone} onClick={() => onPatch({ newZone: !rule.newZone })}>New Zone</Pill>
            <Pill selected={rule.newGrid} onClick={() => onPatch({ newGrid: !rule.newGrid })}>New Grid</Pill>
          </Field>

          <div className="text-[11px] uppercase tracking-wider text-dark-400 pt-1">Where — all set filters must match</div>

          <Field label="Continents">
            {CONTINENTS.map((c) => (
              <Pill key={c} selected={rule.continents.includes(c)} onClick={() => onPatch({ continents: toggleIn(rule.continents, c) })}>{c}</Pill>
            ))}
          </Field>

          <Field label="US call areas">
            {CALL_AREAS.map((a) => (
              <Pill key={a} selected={rule.callAreas.includes(a)} onClick={() => onPatch({ callAreas: toggleIn(rule.callAreas, a) })}>{a}</Pill>
            ))}
          </Field>

          <Field label="DXCC entities">
            <TagInput values={rule.dxccEntities} onChange={(v) => onPatch({ dxccEntities: v })} placeholder="e.g. United States" />
          </Field>

          <Field label="Prefixes">
            <TagInput values={rule.prefixes} onChange={(v) => onPatch({ prefixes: v })} placeholder="e.g. W, VE3" transform={(s) => s.toUpperCase()} />
          </Field>

          <Field label="Grid fields">
            <TagInput values={rule.gridFields} onChange={(v) => onPatch({ gridFields: v })} placeholder="e.g. EM, DM" transform={(s) => s.toUpperCase().slice(0, 2)} />
          </Field>

          <div className="text-[11px] uppercase tracking-wider text-dark-400 pt-1">Bands / modes — empty = all</div>

          <Field label="Bands">
            {BANDS.map((b) => (
              <Pill key={b} selected={rule.bands.includes(b)} onClick={() => onPatch({ bands: toggleIn(rule.bands, b) })}>{b}</Pill>
            ))}
          </Field>

          <Field label="Modes">
            {MODES.map((m) => (
              <Pill key={m} selected={rule.modes.includes(m)} onClick={() => onPatch({ modes: toggleIn(rule.modes, m) })}>{m}</Pill>
            ))}
          </Field>

          <div className="text-[11px] uppercase tracking-wider text-dark-400 pt-1">Actions</div>

          <div className="flex flex-wrap items-center gap-1.5">
            <Pill selected={rule.sound} onClick={() => onPatch({ sound: !rule.sound })}>🔔 Sound</Pill>
            <Pill selected={rule.voice} onClick={() => onPatch({ voice: !rule.voice })}>🗣 Voice</Pill>
            <Pill selected={rule.popup} onClick={() => onPatch({ popup: !rule.popup })}>💬 Popup</Pill>
            <span className="ml-2 text-xs text-dark-400">Cooldown</span>
            <input
              type="number"
              min={0}
              max={240}
              value={rule.cooldownMinutes}
              onChange={(e) => onPatch({ cooldownMinutes: Math.max(0, Number(e.target.value) || 0) })}
              className="glass-input w-16 text-xs py-1"
            />
            <span className="text-xs text-dark-400">min</span>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── small reusable bits ─────────────────────────────────────────────────────

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="text-xs font-medium text-dark-300 mb-1">{label}</div>
      <div className="flex flex-wrap gap-1.5 items-center">{children}</div>
    </div>
  );
}

function Pill({ selected, onClick, title, children }: {
  selected: boolean;
  onClick: () => void;
  title?: string;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      title={title}
      className={`px-2.5 py-1 rounded border text-xs font-medium transition-colors ${
        selected
          ? 'bg-accent-primary/20 border-accent-primary text-accent-primary'
          : 'bg-dark-700 border-dark-500 text-dark-300 hover:bg-dark-600'
      }`}
    >
      {children}
    </button>
  );
}

function TagInput({ values, onChange, placeholder, transform }: {
  values: string[];
  onChange: (v: string[]) => void;
  placeholder: string;
  transform?: (s: string) => string;
}) {
  const [text, setText] = useState('');
  const commit = () => {
    const v = (transform ? transform(text) : text).trim();
    if (v && !values.includes(v)) onChange([...values, v]);
    setText('');
  };
  return (
    <>
      {values.map((v) => (
        <span key={v} className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-accent-primary/15 border border-accent-primary/40 text-accent-primary text-xs">
          {v}
          <button onClick={() => onChange(values.filter((x) => x !== v))} className="hover:text-white leading-none" title="Remove">×</button>
        </span>
      ))}
      <input
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' || e.key === ',') { e.preventDefault(); commit(); }
        }}
        onBlur={commit}
        placeholder={placeholder}
        className="glass-input text-xs py-1 px-2 w-32"
      />
    </>
  );
}

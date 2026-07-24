import type { HamlibRigConfigDto, HamlibRigModelInfo } from '../api/signalr';

/**
 * A shortcut list of radios people are most likely to own, so a new operator can pick
 * the name printed on the front panel instead of hunting through 200+ Hamlib entries.
 *
 * This is a CONVENIENCE, not a compatibility statement — Hamlib drives far more radios
 * than are listed here, and the full searchable list stays one click away. A rig missing
 * from this list is not unsupported.
 *
 * Deliberately NOT keyed by Hamlib model id: those are internal numbers that differ
 * between Hamlib versions, and a wrong one would silently drive the wrong protocol.
 * Instead each entry matches by name against the model list the installed library
 * actually reports, so we can only ever offer a radio that Hamlib really has.
 */
export interface PopularRig {
  /** Stable key for React and for the "recently picked" ordering. */
  id: string;
  /** How it reads on the front panel. */
  label: string;
  manufacturer: string;
  /**
   * Model names to match, compared with punctuation and case stripped — so "FTDX10"
   * matches "FT-DX10", "FTdx-10" and "FT DX 10" without needing every spelling.
   */
  aliases: string[];
  /** Sensible starting point; the operator can still change anything afterwards. */
  defaults: Partial<HamlibRigConfigDto>;
  /**
   * The setting on the RADIO that has to agree. Most "it won't connect" reports are a
   * menu item on the rig, not anything SDRLogger+ can detect or fix, so we say it up
   * front rather than let it become a support email.
   */
  setupHint: string;
  /**
   * True once an operator has confirmed these defaults against the actual radio.
   * The values come from documentation, and documentation is not a bench — so until
   * someone with the rig in front of them says it worked, the UI says so rather than
   * implying a confidence we have not earned. Flip to true on a confirmed report,
   * naming the confirming station in the commit.
   */
  verified?: boolean;
}

/** Uppercase, letters and digits only: "FT-DX 10" and "ftdx10" both become "FTDX10". */
export const normalizeModelName = (s: string) => s.toUpperCase().replace(/[^A-Z0-9]/g, '');

const ICOM_SERIAL: Partial<HamlibRigConfigDto> = {
  connectionType: 'Serial', baudRate: 19200, dataBits: 8, stopBits: 1,
  parity: 'None', flowControl: 'None', pttType: 'Rig',
};
const YAESU_SERIAL: Partial<HamlibRigConfigDto> = {
  connectionType: 'Serial', baudRate: 38400, dataBits: 8, stopBits: 1,
  parity: 'None', flowControl: 'None', pttType: 'Rig',
};
const KENWOOD_SERIAL: Partial<HamlibRigConfigDto> = {
  connectionType: 'Serial', baudRate: 9600, dataBits: 8, stopBits: 1,
  parity: 'None', flowControl: 'None', pttType: 'Rig',
};
const ELECRAFT_SERIAL: Partial<HamlibRigConfigDto> = {
  connectionType: 'Serial', baudRate: 38400, dataBits: 8, stopBits: 1,
  parity: 'None', flowControl: 'None', pttType: 'Rig',
};

const ICOM_HINT =
  'On the radio: Menu → Set → Connectors → CI-V. Set CI-V USB Baud Rate to match the ' +
  'speed here (or set both to 19200), and turn CI-V USB Echo Back OFF.';
const YAESU_HINT =
  'On the radio: set CAT RATE in the menu to match the speed here (38400 is typical). ' +
  'If PTT never keys, check the CAT/PTT-select and RTS items in the same menu group.';
const KENWOOD_HINT =
  'On the radio: set the USB/COM port baud rate in the menu to match the speed here.';
const ELECRAFT_HINT =
  'On the radio: CONFIG → RS232 sets the serial speed — it must match the speed here.';

export const POPULAR_RIGS: PopularRig[] = [
  // --- Icom ---
  { id: 'ic7300',   label: 'IC-7300',   manufacturer: 'Icom', aliases: ['IC-7300'],   defaults: ICOM_SERIAL, setupHint: ICOM_HINT },
  { id: 'ic7610',   label: 'IC-7610',   manufacturer: 'Icom', aliases: ['IC-7610'],   defaults: ICOM_SERIAL, setupHint: ICOM_HINT },
  { id: 'ic9700',   label: 'IC-9700',   manufacturer: 'Icom', aliases: ['IC-9700'],   defaults: ICOM_SERIAL, setupHint: ICOM_HINT },
  {
    id: 'ic705', label: 'IC-705', manufacturer: 'Icom', aliases: ['IC-705'],
    defaults: ICOM_SERIAL,
    // Confirmed over USB by N8SDR, 2026-07-24.
    verified: true,
    setupHint:
      'The IC-705 shows up as TWO USB ports — the lower-numbered one is CI-V; the other ' +
      'never answers, so if nothing happens, try the other port. CI-V USB Baud Rate can stay ' +
      'on Auto (it matched 9600 through 115200 on test), and CI-V USB Echo Back can be left ' +
      'on. Over Bluetooth (unconfirmed): pairing works and the port appears here, but the ' +
      'radio also has to accept the serial connection, and on our test it did not.',
  },
  { id: 'ic7100',   label: 'IC-7100',   manufacturer: 'Icom', aliases: ['IC-7100'],   defaults: ICOM_SERIAL, setupHint: ICOM_HINT },
  { id: 'ic7851',   label: 'IC-7851',   manufacturer: 'Icom', aliases: ['IC-7851'],   defaults: ICOM_SERIAL, setupHint: ICOM_HINT },

  // --- Yaesu ---
  { id: 'ft710',    label: 'FT-710',    manufacturer: 'Yaesu', aliases: ['FT-710'],   defaults: YAESU_SERIAL, setupHint: YAESU_HINT },
  { id: 'ftdx10',   label: 'FTDX10',    manufacturer: 'Yaesu', aliases: ['FTDX10'],   defaults: YAESU_SERIAL, setupHint: YAESU_HINT },
  { id: 'ftdx101',  label: 'FTDX101',   manufacturer: 'Yaesu', aliases: ['FTDX101D', 'FTDX101MP', 'FTDX101'], defaults: YAESU_SERIAL, setupHint: YAESU_HINT },
  { id: 'ft991a',   label: 'FT-991A',   manufacturer: 'Yaesu', aliases: ['FT-991A', 'FT-991'], defaults: YAESU_SERIAL, setupHint: YAESU_HINT },
  { id: 'ft891',    label: 'FT-891',    manufacturer: 'Yaesu', aliases: ['FT-891'],   defaults: YAESU_SERIAL, setupHint: YAESU_HINT },
  { id: 'ftdx3000', label: 'FTDX3000',  manufacturer: 'Yaesu', aliases: ['FTDX3000'], defaults: YAESU_SERIAL, setupHint: YAESU_HINT },

  // --- Kenwood ---
  { id: 'ts590sg',  label: 'TS-590SG',  manufacturer: 'Kenwood', aliases: ['TS-590SG'], defaults: KENWOOD_SERIAL, setupHint: KENWOOD_HINT },
  { id: 'ts890s',   label: 'TS-890S',   manufacturer: 'Kenwood', aliases: ['TS-890S'],  defaults: KENWOOD_SERIAL, setupHint: KENWOOD_HINT },
  { id: 'ts990s',   label: 'TS-990S',   manufacturer: 'Kenwood', aliases: ['TS-990S'],  defaults: KENWOOD_SERIAL, setupHint: KENWOOD_HINT },
  { id: 'ts480',    label: 'TS-480',    manufacturer: 'Kenwood', aliases: ['TS-480', 'TS-480HX', 'TS-480SAT'], defaults: KENWOOD_SERIAL, setupHint: KENWOOD_HINT },

  // --- Elecraft ---
  { id: 'k4',       label: 'K4',        manufacturer: 'Elecraft', aliases: ['K4'],  defaults: ELECRAFT_SERIAL, setupHint: ELECRAFT_HINT },
  { id: 'k3',       label: 'K3',        manufacturer: 'Elecraft', aliases: ['K3', 'K3S'], defaults: ELECRAFT_SERIAL, setupHint: ELECRAFT_HINT },
  { id: 'kx3',      label: 'KX3',       manufacturer: 'Elecraft', aliases: ['KX3'], defaults: ELECRAFT_SERIAL, setupHint: ELECRAFT_HINT },
  { id: 'kx2',      label: 'KX2',       manufacturer: 'Elecraft', aliases: ['KX2'], defaults: ELECRAFT_SERIAL, setupHint: ELECRAFT_HINT },
];

export interface ResolvedPopularRig extends PopularRig {
  modelId: number;
  /** Hamlib's own name for it — what gets stored, so the config stays self-describing. */
  modelName: string;
}

/**
 * Pair the shortcut list with what the installed Hamlib actually offers. Entries with no
 * match are dropped rather than shown broken — a radio this build genuinely cannot drive
 * should not appear as a one-click option.
 */
export function resolvePopularRigs(rigs: HamlibRigModelInfo[]): ResolvedPopularRig[] {
  if (rigs.length === 0) return [];

  const byName = new Map<string, HamlibRigModelInfo>();
  for (const r of rigs) {
    const key = normalizeModelName(r.model);
    // First match wins: the list arrives sorted, so this keeps the choice stable rather
    // than letting a later duplicate name silently replace an earlier one.
    if (!byName.has(key)) byName.set(key, r);
  }

  const resolved: ResolvedPopularRig[] = [];
  for (const p of POPULAR_RIGS) {
    for (const alias of p.aliases) {
      const hit = byName.get(normalizeModelName(alias));
      if (hit) {
        resolved.push({ ...p, modelId: hit.modelId, modelName: hit.displayName });
        break;
      }
    }
  }
  return resolved;
}

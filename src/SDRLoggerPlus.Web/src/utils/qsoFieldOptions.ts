/**
 * Log modes offered when editing a QSO.
 *
 * These are ADIF *log* modes — what the contact is recorded as — which is a
 * different list from the Log Entry rig-mode dropdown (USB/LSB/CWU/DIGU exist
 * there so the radio gets a real sideband, not a collapsed "SSB").
 *
 * The list is deliberately not exhaustive. ADIF defines well over a hundred
 * modes and real logs carry imported oddities besides; `selectOptionsFor`
 * handles anything missing by showing it rather than dropping it.
 */
export const LOG_MODES = [
  'SSB', 'USB', 'LSB', 'CW', 'AM', 'FM',
  'FT8', 'FT4', 'JT65', 'JT9', 'JS8', 'MFSK', 'Q65', 'MSK144',
  'RTTY', 'PSK31', 'PSK', 'OLIVIA', 'HELL', 'SSTV', 'PKT', 'DATA',
  'DIGITALVOICE', 'FREEDV',
];

/**
 * Work out what a `<select>` should show for a value that came from the log.
 *
 * A plain controlled `<select>` silently renders blank when its value matches
 * no `<option>` — and a blank required select then refuses to submit, so the
 * whole edit dies with no visible reason. Two things in a real log trigger
 * that:
 *
 *   - **Case.** ADIF treats band and mode as case-insensitive, and imports
 *     reflect it: this log holds "40M" and "20m", "10M" and "17m". Over half
 *     its QSOs carry an upper-case band that no lower-case option matched.
 *   - **Values outside the list.** 27 distinct modes appear in the same log —
 *     JT65, MFSK, PKT, HELL, VARA HF — against a dropdown offering eight.
 *
 * So: match case-insensitively and select the canonical option when the value
 * is one we know, and otherwise keep the stored value verbatim as an extra
 * option. An unrecognised mode is still the truth about that contact; the edit
 * form's job is to preserve it, not to quietly correct it to nothing.
 *
 * Returns the value the `<select>` should carry plus the options it needs for
 * that value to be selectable.
 */
export function selectOptionsFor(
  options: readonly string[],
  stored: string | null | undefined,
): { value: string; options: string[] } {
  const raw = (stored ?? '').trim();
  if (raw.length === 0) return { value: '', options: [...options] };

  const canonical = options.find(o => o.toLowerCase() === raw.toLowerCase());
  if (canonical !== undefined) {
    // Known value: select the list's spelling, so saving an untouched "40M"
    // also tidies it to "40m". Safe because every comparison in the app is
    // already case-insensitive.
    return { value: canonical, options: [...options] };
  }

  // Unknown value: offer it first so it reads as the current setting rather
  // than something appended to the bottom of the list.
  return { value: raw, options: [raw, ...options] };
}

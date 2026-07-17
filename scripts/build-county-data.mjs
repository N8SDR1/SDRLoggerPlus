// Assemble the contest county-code table keyed by contest definition id.
//
// Inputs:
//   docs/design/contest-research/counties/*.json  — official per-party abbreviation
//     tables gathered from each sponsor's rules (verified where reachable).
//   src/SDRLoggerPlus.Web/src/contest/countyDataGeneric.json — generic first-3-letter
//     US county codes, used as a fallback for parties whose official list is missing.
//
// Output:
//   src/SDRLoggerPlus.Web/src/contest/countyData.json —
//     { "<defId>": { "official": bool, "source": string, "counties": [{name,code}] } }
//
// Keyed by definition id (not state) so regionals that share a state with a
// single-state party (WA in 7QP vs the Salmon Run) don't collide.

import { readFileSync, writeFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const researchDir = join(root, 'docs/design/contest-research/counties');
const contestDir = join(root, 'src/SDRLoggerPlus.Web/src/contest');

// State code -> contest definition id (matches SeedContests: "qp-" + slug(name)).
const STATE_TO_DEF = {
  AL: 'qp-alabama', AR: 'qp-arkansas', CA: 'qp-california', CO: 'qp-colorado',
  DE: 'qp-delaware', FL: 'qp-florida', GA: 'qp-georgia', HI: 'qp-hawaii',
  IL: 'qp-illinois', IN: 'qp-indiana', IA: 'qp-iowa', KS: 'qp-kansas',
  KY: 'qp-kentucky', LA: 'qp-louisiana', MD: 'qp-maryland-dc', MI: 'qp-michigan',
  MN: 'qp-minnesota', MS: 'qp-mississippi', MO: 'qp-missouri', NE: 'qp-nebraska',
  NJ: 'qp-new-jersey', NM: 'qp-new-mexico', NY: 'qp-new-york', NC: 'qp-north-carolina',
  ND: 'qp-north-dakota', OH: 'qp-ohio', OK: 'qp-oklahoma', PA: 'qp-pennsylvania',
  SC: 'qp-south-carolina', SD: 'qp-south-dakota', TN: 'qp-tennessee', TX: 'qp-texas',
  VA: 'qp-virginia', WA: 'qp-washington-salmon-run', WV: 'qp-west-virginia', WI: 'qp-wisconsin',
};
// Regional parties and the member states used for a generic fallback.
const REGION_STATES = {
  'qp-7qp': ['WA', 'OR', 'ID', 'MT', 'WY', 'NV', 'UT'],
  'qp-neqp': ['CT', 'ME', 'MA', 'NH', 'RI', 'VT'],
};
const REGION_FILE = { '7QP': 'qp-7qp', NEQP: 'qp-neqp' };

const generic = JSON.parse(readFileSync(join(contestDir, 'countyDataGeneric.json'), 'utf8'));
const genericFor = (st) => (generic[st] ?? []).map((c) => ({ name: c.name, code: c.code }));

const out = {};

// 1) Official tables from research.
const files = readdirSync(researchDir).filter((f) => f.endsWith('.json'));
for (const f of files) {
  const data = JSON.parse(readFileSync(join(researchDir, f), 'utf8'));
  const base = f.replace(/\.json$/, '');
  const defId = REGION_FILE[base] ?? STATE_TO_DEF[data.state] ?? STATE_TO_DEF[base];
  if (!defId) { console.warn(`skip ${f}: no def mapping`); continue; }
  const counties = (data.counties ?? [])
    .filter((c) => c && c.name && c.code)
    .map((c) => ({ name: c.name, code: String(c.code).toUpperCase() }));
  if (counties.length === 0) continue;
  out[defId] = {
    official: data.confidence === 'verified',
    source: data.source ?? '',
    counties,
  };
}

// 2) Generic fallback for any party without an official table.
for (const [st, defId] of Object.entries(STATE_TO_DEF)) {
  if (out[defId]) continue;
  out[defId] = { official: false, source: 'generic', counties: genericFor(st) };
}
for (const [defId, states] of Object.entries(REGION_STATES)) {
  if (out[defId]) continue;
  const counties = states.flatMap((st) => genericFor(st));
  out[defId] = { official: false, source: 'generic', counties };
}

writeFileSync(join(contestDir, 'countyData.json'), JSON.stringify(out) + '\n');

const parties = Object.keys(out).length;
const official = Object.values(out).filter((v) => v.official).length;
const totalCounties = Object.values(out).reduce((n, v) => n + v.counties.length, 0);
console.log(`wrote countyData.json: ${parties} parties (${official} official), ${totalCounties} county codes`);

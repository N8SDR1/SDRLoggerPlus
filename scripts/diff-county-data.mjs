// Cross-check the two independent county-code extractions (pass 1 vs pass 2) and
// report every disagreement, so mismatches can be hand-resolved against source.
//
//   pass 1: docs/design/contest-research/counties/*.json
//   pass 2: docs/design/contest-research/counties-verify/*.json
//
// Matches counties by normalized name (case/punctuation/"county"/"saint"-insensitive)
// and compares the codes (case-insensitive). Regional files (7QP, NEQP) match on
// state+name.

import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const dir1 = join(root, 'docs/design/contest-research/counties');
const dir2 = join(root, 'docs/design/contest-research/counties-verify');

const norm = (s) =>
  String(s).toLowerCase()
    .replace(/\bst\.?\b/g, 'saint')
    .replace(/\bcounty\b|\bparish\b|\bcity\b/g, '')
    .replace(/[^a-z0-9]/g, '')
    .trim();
const keyOf = (c) => (c.state ? `${c.state}:${norm(c.name)}` : norm(c.name));
const mapOf = (data) => {
  const m = new Map();
  for (const c of data.counties ?? []) if (c?.name && c?.code) m.set(keyOf(c), { name: c.name, code: String(c.code).toUpperCase() });
  return m;
};

if (!existsSync(dir2)) { console.error('no counties-verify/ yet'); process.exit(1); }
const files = readdirSync(dir2).filter((f) => f.endsWith('.json'));

let fullAgree = 0, withDiffs = 0, missingPass2 = 0;
const problems = [];

for (const f of files.sort()) {
  const p2 = JSON.parse(readFileSync(join(dir2, f), 'utf8'));
  if (!existsSync(join(dir1, f))) { console.log(`${f.replace('.json','').padEnd(6)} only in verify pass`); continue; }
  const p1 = JSON.parse(readFileSync(join(dir1, f), 'utf8'));
  const m1 = mapOf(p1), m2 = mapOf(p2);

  const mism = [];        // same county, different code
  const only1 = [], only2 = [];
  for (const [k, v] of m1) {
    if (!m2.has(k)) only1.push(v.name);
    else if (m2.get(k).code !== v.code) mism.push(`${v.name}: p1=${v.code} p2=${m2.get(k).code}`);
  }
  for (const [k, v] of m2) if (!m1.has(k)) only2.push(v.name);

  const clean = mism.length === 0 && only1.length === 0 && only2.length === 0;
  const tag = `${f.replace('.json','').padEnd(6)} ${(p2.confidence||'?').padEnd(8)}`;
  if (clean) { fullAgree++; console.log(`${tag} ✓ agree (${m1.size})`); }
  else {
    withDiffs++;
    console.log(`${tag} ⚠ ${mism.length} code-diffs, ${only1.length} only-p1, ${only2.length} only-p2`);
    problems.push({ f, mism, only1, only2 });
  }
}

console.log(`\n=== ${fullAgree} fully agree, ${withDiffs} with differences ===`);
for (const p of problems) {
  console.log(`\n--- ${p.f} ---`);
  if (p.mism.length) console.log('  code mismatches:\n    ' + p.mism.join('\n    '));
  if (p.only1.length) console.log('  only in pass1: ' + p.only1.join(', '));
  if (p.only2.length) console.log('  only in pass2: ' + p.only2.join(', '));
}

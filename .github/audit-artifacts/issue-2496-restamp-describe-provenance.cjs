const fs = require('node:fs');
const crypto = require('node:crypto');
const cp = require('node:child_process');

const sha = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
const repo = cp.execFileSync('git', ['rev-parse', '--show-toplevel'], {encoding: 'utf8'}).trim();
const oldHead = process.argv[3] || '6dfc1ffe7';
const oldSpec = cp.execFileSync('git', ['show', `${oldHead}:platform-spec.yaml`], {maxBuffer: 8*1024*1024});
const newSpec = fs.readFileSync(`${repo}/platform-spec.yaml`);
const oldDigest = sha(oldSpec), newDigest = sha(newSpec);
const hunks = [...cp.execFileSync('git', ['diff', '--unified=0', oldHead, 'HEAD', '--', 'platform-spec.yaml'], {encoding: 'utf8'})
  .matchAll(/^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/gm)]
  .map(m => ({old: +m[1], removed: +(m[2] ?? 1), next: +m[3], added: +(m[4] ?? 1)}));
const oldLine = line => {
  let shift = 0;
  for (const h of hunks) {
    if (line < h.next) break;
    if (line < h.next + h.added) {
      if (h.added !== h.removed) throw new Error(`new declaration at changed spec line ${line}`);
      return h.old + line - h.next;
    }
    shift += h.added - h.removed;
  }
  return line - shift;
};
const receipt = fs.readFileSync(process.argv[2]);
const events = receipt.toString().split('\n').filter(l => l.startsWith('{"Time":')).map(l => JSON.parse(l));
const prefix = 'TestReadProofFactoringCompiledDescribe/';
const outputs = new Map();
for (const e of events) if (e.Test?.startsWith(prefix) && e.Action === 'output') outputs.set(e.Test, (outputs.get(e.Test) ?? '') + e.Output);
const outcomes = events.filter(e => e.Test?.startsWith(prefix) && ['pass', 'fail', 'skip'].includes(e.Action));
if (outcomes.length !== 45 || outcomes.some(e => e.Action === 'skip')) throw new Error('incomplete describe characterization');
const baseline = JSON.parse(fs.readFileSync(`${repo}/internal/releasee2e/testdata/read_proof_describe_baseline.json`));
const rows = outcomes.map(e => {
  const key = e.Test.slice(prefix.length), split = key.lastIndexOf('/');
  const fixture = key.slice(0, split), surface = key.slice(split+1);
  const before = baseline.results.find(r => r.fixture === fixture && r.surface === surface)?.stdout_sha256;
  if (!before) throw new Error(`missing prior cell ${key}`);
  if (e.Action === 'pass') return {fixture, surface, status: 'unchanged', stdout_sha256: before};
  const output = outputs.get(e.Test);
  const declared = output.match(/output changed: sha=([a-f0-9]{64}) want=([a-f0-9]{64})/);
  if (!declared || declared[2] !== before) throw new Error(`unexpected failure ${key}`);
  const raw = output.split('\n').find(l => l.trimStart().startsWith('{"workflow_name"'))?.trim();
  if (!raw) throw new Error(`missing full public JSON ${key}`);
  JSON.parse(raw);
  const scope = raw.match(/"source_file":"([^"\n]+)\/\.cache\/swarm\/embedded-assets\/platform-spec-[a-f0-9]+\.yaml"/)?.[1];
  if (!scope) throw new Error(`missing isolated scope ${key}`);
  const stdout = (raw + '\n').replaceAll(repo, '<repo>').replaceAll(scope, '<scope>');
  if (sha(stdout) !== declared[1]) throw new Error(`raw hash reconstruction mismatch ${key}`);
  let lines = 0;
  let reconstructed = stdout.replace(/("source_file":"[^"\n]*platform-spec-[a-f0-9]+\.yaml","source_line":)(\d+)/g, (_, field, line) => {
    lines++;
    return field + oldLine(+line);
  });
  const newName = `platform-spec-${newDigest.slice(0,16)}.yaml`;
  const files = reconstructed.split(newName).length-1;
  reconstructed = reconstructed.replaceAll(newName, `platform-spec-${oldDigest.slice(0,16)}.yaml`);
  if (!files || sha(reconstructed) !== before) throw new Error(`non-provenance public change ${key}`);
  return {fixture, surface, status: 'changed', before, after: declared[1], source_file_count: files, source_line_count: lines};
});
process.stdout.write(JSON.stringify({source: cp.execFileSync('git', ['rev-parse', 'HEAD'], {encoding:'utf8'}).trim(),
  old_spec_source: oldHead, old_spec_sha256: oldDigest, new_spec_sha256: newDigest,
  receipt_sha256: sha(receipt), spec_hunks: hunks, changed: rows.filter(r => r.status === 'changed').length,
  unchanged: rows.filter(r => r.status === 'unchanged').length,
  boundary: 'Byte-exact reconstruction of prior public hashes after reversing only measured embedded-spec filename and source-line provenance; no semantic normalization or assertion change.', rows}, null, 2) + '\n');

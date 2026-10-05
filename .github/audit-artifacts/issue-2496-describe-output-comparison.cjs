const fs = require('node:fs');
const crypto = require('node:crypto');
const cp = require('node:child_process');

const digest = value => crypto.createHash('sha256').update(value).digest('hex');
const prefix = 'TestReadProofFactoringCompiledDescribe/';
const base = process.argv[4] || 'a2d84f049a157884f9f61a4707c760d18a57d25a';
const baseline = JSON.parse(cp.execFileSync('git', ['show', `${base}:internal/releasee2e/testdata/read_proof_describe_baseline.json`]));
const capture = filename => {
  const bytes = fs.readFileSync(filename);
  const events = bytes.toString().split('\n').filter(line => line.startsWith('{"Time":')).map(JSON.parse);
  const outcomes = events.filter(event => event.Test?.startsWith(prefix) && ['pass', 'fail', 'skip'].includes(event.Action));
  if (outcomes.length !== 45 || outcomes.some(event => event.Action === 'skip')) throw new Error('incomplete characterization');
  const outputs = new Map();
  for (const event of events) if (event.Test?.startsWith(prefix) && event.Action === 'output') {
    outputs.set(event.Test, (outputs.get(event.Test) || '') + event.Output);
  }
  return {filename, sha256: digest(bytes), rows: new Map(outcomes.map(event => {
    const key = event.Test.slice(prefix.length);
    if (event.Action === 'pass') return [key, {status: 'pass'}];
    const output = outputs.get(event.Test);
    const match = output.match(/output changed: sha=([a-f0-9]{64}) want=([a-f0-9]{64})\n/);
    if (!match) throw new Error(`non-snapshot failure: ${key}`);
    const body = output.slice(match.index + match[0].length).split('\n--- FAIL:')[0];
    const raw = body.split('\n').map(line => line.startsWith('        ') ? line.slice(8) : line).join('\n') + '\n';
    const repo = raw.match(/\/home\/youmew\/dev\/swarm\/worktrees\/agent-e-2496(?:-describe-(?:base|fd7))?\b/)?.[0];
    const scope = raw.match(/([^"\s]+)\/\.cache\/swarm\/embedded-assets\/platform-spec-[a-f0-9]+\.yaml/)?.[1];
    let normalized = raw;
    if (repo) normalized = normalized.replaceAll(repo, '<repo>');
    if (scope) normalized = normalized.replaceAll(scope, '<scope>');
    if (digest(normalized) !== match[1]) throw new Error(`exact stdout reconstruction mismatch: ${key}`);
    return [key, {status: 'fail', hash: match[1], expected: match[2], stdout: normalized}];
  }))};
};
const before = capture(process.argv[2]), after = capture(process.argv[3]);
const differences = (left, right, path = '$', parentBefore, parentAfter) => {
  if (JSON.stringify(left) === JSON.stringify(right)) return [];
  if (path === '$/effective_provenance') {
    const keyed = values => {
      const map = Object.fromEntries(values.map(value => [value.path, value.provenance]));
      if (Object.keys(map).length !== values.length) throw new Error('duplicate provenance identity');
      return map;
    };
    return differences(keyed(left), keyed(right), `${path}/by_path`);
  }
  if (/^\$\/stage_graphs\/\d+\/edges$/.test(path)) {
    const old = new Set(left.map(value => JSON.stringify(value))), next = new Set(right.map(value => JSON.stringify(value)));
    return [{path, removed: left.filter(value => !next.has(JSON.stringify(value))), added: right.filter(value => !old.has(JSON.stringify(value))),
      order_unchanged_for_retained_edges: JSON.stringify(left.filter(value => next.has(JSON.stringify(value)))) === JSON.stringify(right.filter(value => old.has(JSON.stringify(value))))}];
  }
  if (left && right && typeof left === 'object' && typeof right === 'object' && Array.isArray(left) === Array.isArray(right)) {
    return [...new Set([...Object.keys(left), ...Object.keys(right)])].flatMap(key => differences(left[key], right[key], `${path}/${key}`, left, right));
  }
  return [{path, before: left, after: right, ...(path.endsWith('/source_line') ? {source_file_before: parentBefore?.source_file, source_file_after: parentAfter?.source_file} : {})}];
};
const sourceHunks = new Map();
const sourceLines = new Map();
const mapLine = (line, file) => {
  if (!sourceHunks.has(file)) sourceHunks.set(file, [...cp.execFileSync('git', ['diff', '--unified=0', base, 'HEAD', '--', file], {encoding: 'utf8'}).matchAll(/^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/gm)]
    .map(match => ({old: +match[1], removed: +(match[2] ?? 1), next: +match[3], added: +(match[4] ?? 1)})));
  const hunks = sourceHunks.get(file);
  let shift = 0;
  for (const hunk of hunks) {
    if (line < hunk.next) break;
    if (line < hunk.next + hunk.added) {
      if (hunk.added !== hunk.removed) {
        if (!sourceLines.has(file)) sourceLines.set(file, {
          before: cp.execFileSync('git', ['show', `${base}:${file}`], {encoding: 'utf8', maxBuffer: 8*1024*1024}).split('\n'),
          after: fs.readFileSync(file, 'utf8').split('\n'),
        });
        const lines = sourceLines.get(file);
        const matches = lines.before.slice(hunk.old - 1, hunk.old - 1 + hunk.removed).flatMap((value, index) => value === lines.after[line - 1] ? [hunk.old + index] : []);
        if (matches.length !== 1) throw new Error(`new or ambiguous declaration in ${file}:${line}`);
        return matches[0];
      }
      return hunk.old + line - hunk.next;
    }
    shift += hunk.added - hunk.removed;
  }
  return line - shift;
};
const oldSpecHash = digest(cp.execFileSync('git', ['show', `${base}:platform-spec.yaml`], {maxBuffer: 8*1024*1024})).slice(0, 16);
const newSpecHash = digest(fs.readFileSync('platform-spec.yaml')).slice(0, 16);
const oldSpecLines = cp.execFileSync('git', ['show', `${base}:platform-spec.yaml`], {encoding: 'utf8', maxBuffer: 8*1024*1024}).split('\n');
const newSpecLines = fs.readFileSync('platform-spec.yaml', 'utf8').split('\n');
const rows = baseline.results.map(prior => {
  const key = `${prior.fixture}/${prior.surface}`, old = before.rows.get(key), next = after.rows.get(key);
  if (!old || !next) throw new Error(`missing cell: ${key}`);
  if (next.status === 'pass') {
    if (old.status !== 'pass' && !(old.expected === '0'.repeat(64) && old.hash === prior.stdout_sha256)) throw new Error(`unexpected master change: ${key}`);
    return {...prior, disposition: 'unchanged'};
  }
  if (old.status !== 'fail' || old.hash !== prior.stdout_sha256 || next.expected !== prior.stdout_sha256 ||
      !(old.expected === next.hash || old.expected === '0'.repeat(64))) {
    throw new Error(`unverified original/current stdout: ${key}`);
  }
  const row = {fixture: prior.fixture, surface: prior.surface, before: old.hash, after: next.hash};
  if (prior.surface.endsWith('json')) {
    row.differences = differences(JSON.parse(old.stdout), JSON.parse(next.stdout));
    for (const delta of row.differences) {
      if (delta.path.endsWith('/source_file')) {
        if (delta.before.replace(`platform-spec-${oldSpecHash}.yaml`, `platform-spec-${newSpecHash}.yaml`) !== delta.after) throw new Error(`unexpected provenance file: ${key}`);
      }
      if (delta.path.endsWith('/source_line')) {
        const file = delta.source_file_after?.includes('platform-spec-') ? 'platform-spec.yaml' : `${prior.fixture}/${delta.source_file_after}`;
        if (delta.path === '$/effective_provenance/by_path/platform.platform_tables.tables.run_fork_selected_contract_branch_divergences.ddl/source_line' &&
            file === 'platform-spec.yaml' &&
            oldSpecLines[delta.before - 1].trim().startsWith('ddl: "CREATE TABLE run_fork_selected_contract_branch_divergences (') &&
            newSpecLines[delta.after - 1].trim() === 'ddl: |-' &&
            newSpecLines[delta.after].trim() === 'CREATE TABLE run_fork_selected_contract_branch_divergences (') {
          delta.disposition = 'Authorized typed event/deployment-revision divergence DDL replacement; exact declaration, not provenance-only normalization.';
          continue;
        }
        try {
          if (mapLine(delta.after, file) !== delta.before) throw new Error('line mismatch');
        } catch (error) {
          throw new Error(`non-diff provenance movement: ${key}:${delta.path}:${delta.before}->${delta.after}: ${error.message}`);
        }
      }
    }
    row.provenance_disposition = 'Every changed source filename equals the actual spec digest replacement; changed lines use the exact git-diff source mapping except the explicitly recorded authorized typed-point DDL replacement.';
  }
  else {
    const left = old.stdout.split('\n'), right = next.stdout.split('\n');
    row.removed_lines = left.filter(line => !right.includes(line));
    row.added_lines = right.filter(line => !left.includes(line));
    row.constructor_only = next.stdout.split('\n').filter(line => !/^\s*(root )?constructor:/.test(line)).join('\n') === old.stdout;
  }
  return row;
});
process.stdout.write(JSON.stringify({base, source: cp.execFileSync('git', ['rev-parse', 'HEAD'], {encoding: 'utf8'}).trim(),
  boundary: 'Exact compiled stdout hashes verified against the original master snapshot and the current RED capture. Master capture changes only expected hashes to expose original stdout; it is characterization, not a green execution receipt. All semantic differences remain listed, not normalized or accepted automatically.',
  before_receipt: {file: before.filename, sha256: before.sha256}, after_receipt: {file: after.filename, sha256: after.sha256}, rows}, null, 2) + '\n');

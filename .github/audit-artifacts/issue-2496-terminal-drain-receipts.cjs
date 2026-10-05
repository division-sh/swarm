const fs = require('node:fs');
const crypto = require('node:crypto');
const zlib = require('node:zlib');

const inputs = [
  ['issue-2496-terminal-drain-native.jsonl', 'initial working-tree matrix; before stronger retirement assertions'],
  ['issue-2496-terminal-drain-composed-race.jsonl', '272d5feb7 runtime/manager source; before individual Runtime retirement guard'],
  ['issue-2496-terminal-drain-manager-suite.jsonl', '272d5feb7'],
  ['issue-2496-terminal-drain-final-race.jsonl', '7b715426e'],
  ['issue-2496-terminal-drain-final-manager-suite.jsonl', '7b715426e'],
];

const receipts = inputs.map(([file, source]) => {
  const bytes = zlib.gunzipSync(fs.readFileSync(`${__dirname}/${file}.gz`));
  const events = bytes.toString().split('\n').filter(line => line.startsWith('{')).map(line => JSON.parse(line));
  const outcomes = events.filter(event => ['pass', 'fail', 'skip'].includes(event.Action));
  const packages = outcomes.filter(event => !event.Test);
  if (packages.length === 0) throw new Error(`${file}: no completed package receipt`);
  const tests = outcomes.filter(event => event.Test);
  const roots = tests.filter(event => !event.Test.includes('/'));
  const count = rows => Object.fromEntries(['pass', 'fail', 'skip'].map(action => [action, rows.filter(row => row.Action === action).length]));
  return {
    raw: `${file}.gz`, source,
    sha256_uncompressed: crypto.createHash('sha256').update(bytes).digest('hex'),
    bytes_uncompressed: bytes.length,
    package_results: packages.map(({Package, Action, Elapsed}) => ({package: Package, action: Action, elapsed: Elapsed})),
    root_outcomes_including_repetitions: count(roots),
    test_outcomes_including_repetitions: count(tests),
    named_roots: roots.map(({Package, Test, Action, Elapsed}) => ({package: Package, test: Test, action: Action, elapsed: Elapsed})),
    native_shutdown_cells: tests.filter(event => event.Test.startsWith('TestRuntimeConstructedActorRetainedRefreshShutdownBothStores/') && event.Test.split('/').length === 3)
      .map(({Test, Action, Elapsed}) => ({test: Test, action: Action, elapsed: Elapsed})),
  };
});

process.stdout.write(JSON.stringify({
  source_head: process.argv[2],
  boundary: 'Local terminal-ordering qualification only. No survivor-grant closure, complete managed default, fresh describe or exact-head CI claim. No push/rebase.',
  receipts,
}, null, 2) + '\n');

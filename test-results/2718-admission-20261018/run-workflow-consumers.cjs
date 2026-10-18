const fs = require('node:fs');
const cp = require('node:child_process');
const path = require('node:path');

const dir = 'test-results/2718-private-mirror';
const seeds = cp.execFileSync('git', ['grep', '-l', '-E', 'ci\\.yml|CIWorkflowPath|CIWorkflowFile', '--', '*test.go'], {encoding: 'utf8'}).trim().split('\n').sort();
const files = new Set(seeds);
const allTests = cp.execFileSync('git', ['ls-files', '--', '*test.go'], {encoding: 'utf8'}).trim().split('\n');
// Conservatively include whole test files that call any reader/helper declared
// by an already reached file. This covers indirect admission/cache consumers.
let grew;
do {
  grew = false;
  const helpers = new Map();
  for (const file of files) {
    const pkg = path.dirname(file);
    if (!helpers.has(pkg)) helpers.set(pkg, new Set());
    for (const match of fs.readFileSync(file, 'utf8').matchAll(/^func (\w+)\(/gm)) helpers.get(pkg).add(match[1]);
  }
  for (const file of allTests) {
    if (files.has(file)) continue;
    const names = helpers.get(path.dirname(file));
    if (!names) continue;
    const source = fs.readFileSync(file, 'utf8');
    if ([...names].some(name => new RegExp('\\b' + name + '\\s*\\(').test(source))) {
      files.add(file);
      grew = true;
    }
  }
} while (grew);
const packages = new Map();
const roots = new Set();
for (const file of [...files].sort()) {
  const names = [...fs.readFileSync(file, 'utf8').matchAll(/^func (Test\w+)\(t \*testing\.T\)/gm)].map(match => match[1]);
  const pkg = './' + path.dirname(file);
  if (!packages.has(pkg)) packages.set(pkg, []);
  packages.get(pkg).push({file, roots: names});
  for (const name of names) roots.add(name);
}
const head = cp.execFileSync('git', ['rev-parse', 'HEAD'], {encoding: 'utf8'}).trim();
const inventory = {head, seedFiles: seeds, files: files.size, rootCount: roots.size, packages: Object.fromEntries(packages)};
fs.writeFileSync(`${dir}/workflow-consumer-inventory.json`, JSON.stringify(inventory, null, 2) + '\n');
const args = ['run', './cmd/swarm-test', '--', ...[...packages.keys()].sort(), '-run', '^(' + [...roots].sort().join('|') + ')$', '-count=1', '-json'];
fs.writeFileSync(`${dir}/workflow-consumer-command.json`, JSON.stringify({head, command: ['go', ...args]}, null, 2) + '\n');
const fd = fs.openSync(`${dir}/all-workflow-consumers.jsonl`, 'w');
const run = cp.spawnSync('go', args, {stdio: ['ignore', fd, fd]});
fs.closeSync(fd);
if (run.error) throw run.error;
const events = fs.readFileSync(`${dir}/all-workflow-consumers.jsonl`, 'utf8').split('\n').flatMap(line => {
  try {return [JSON.parse(line)];} catch {return [];}
});
const summary = {
  head, exit: run.status, files: files.size, rootCount: roots.size, packages: packages.size,
  passedRoots: events.filter(e => e.Action === 'pass' && e.Test && !e.Test.includes('/')).map(e => ({package: e.Package, root: e.Test})),
  passedCells: events.filter(e => e.Action === 'pass' && e.Test).length,
  failures: events.filter(e => e.Action === 'fail'),
  skipped: events.filter(e => e.Action === 'skip' && e.Test),
};
fs.writeFileSync(`${dir}/workflow-consumer-summary.json`, JSON.stringify(summary, null, 2) + '\n');
console.log(JSON.stringify({...summary, passedRoots: summary.passedRoots.length}));
process.exit(run.status === 0 ? 0 : 1);

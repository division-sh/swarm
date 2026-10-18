const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const root = process.cwd();
const folder = 'test-results/2735-memo-transport-20261018';
const remote = '/home/youmew/dev/wt/agent-d/evidence-2735-memo-20261018';
const original = JSON.parse(fs.readFileSync('test-results/2735-corrected-transport-20261018/candidate-overlay.json', 'utf8'));
const localMap = {Replace: {}};
const remoteMap = {Replace: {}};
const files = [];
for (const source of Object.keys(original.Replace).sort()) {
  const relative = path.relative('/home/youmew/dev/wt/agent-d/scratch', source);
  if (relative.startsWith('..')) throw new Error(`Unexpected source: ${source}`);
  const replacement = `candidate/${relative}.txt`;
  const body = fs.readFileSync(`${folder}/${replacement}`);
  localMap.Replace[`${root}/${relative}`] = `${root}/${folder}/${replacement}`;
  remoteMap.Replace[source] = `${remote}/${replacement}`;
  files.push([replacement, crypto.createHash('sha256').update(body).digest('hex')]);
}
fs.writeFileSync(`${folder}/local-overlay.json`, JSON.stringify(localMap, null, 2) + '\n');
fs.writeFileSync(`${folder}/candidate-overlay.json`, JSON.stringify(remoteMap, null, 2) + '\n');
for (const file of ['candidate-overlay.json', 'run-candidate.sh']) {
  files.push([file, crypto.createHash('sha256').update(fs.readFileSync(`${folder}/${file}`)).digest('hex')]);
}
fs.writeFileSync(`${folder}/overlay-content.sha256`, files.map(([file, hash]) => `${hash}  ${file}`).join('\n') + '\n');
console.log(JSON.stringify({overlayFiles: Object.keys(localMap.Replace).length, frozenInputs: files.length, root, remote}));

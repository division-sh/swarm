const fs = require('fs');
const path = require('path');
const root = __dirname;
const stamp = (message, key) => {
  const found = message.match(new RegExp('(?:^| )' + key + '=([0-9]+)'));
  return found ? BigInt(found[1]) : null;
};
const milliseconds = value => Number(value) / 1e6;
const results = [];
for (const arm of ['base', 'candidate']) {
  const rows = fs.readFileSync(path.join(root, arm + '-phases.txt'), 'utf8').trim().split('\n').map(line => {
    const found = line.match(/Category="([^"]+)" Message=(".*")$/);
    return found ? {category: found[1], message: JSON.parse(found[2])} : null;
  }).filter(Boolean);
  const all = category => rows.filter(row => row.category === category);
  const first = (category, key) => all(category).find(row => stamp(row.message, key) !== null);
  const last = (category, key) => all(category).filter(row => stamp(row.message, key) !== null).at(-1);
  const budget = first('2735-budget', 'deadline_ns');
  const deadline = stamp(budget.message, 'deadline_ns');
  const receipt = all('2735-receipt').find(row => row.message.includes('native_result_ns=') && row.message.includes('status=processed') && row.message.includes('acknowledged=true'));
  const acknowledgement = stamp(receipt.message, 'native_result_ns');
  const receiptEntry = stamp(first('2735-receipt', 'native_entry_ns').message, 'native_entry_ns');
  let decoded = null;
  const contracts = [];
  for (const row of all('2735-operational-source')) {
    if (stamp(row.message, 'artifact_decoded_ns') !== null) decoded = stamp(row.message, 'artifact_decoded_ns');
    if (stamp(row.message, 'contract_loaded_ns') !== null && decoded !== null) contracts.push(stamp(row.message, 'contract_loaded_ns') - decoded);
  }
  const traceDir = path.join(root, 'retained-server2', arm, 'trace');
  const observations = fs.readFileSync(path.join(traceDir, 'observations.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
  const releaseFiles = fs.readdirSync(traceDir).filter(name => /^2735-proxy-[0-9]+\.json$/.test(name));
  if (releaseFiles.length !== 1) throw new Error(arm + ': not exactly one first-response receipt');
  const release = JSON.parse(fs.readFileSync(path.join(traceDir, releaseFiles[0]), 'utf8'));
  if (!release.matched || release.canceled || release.projection_error) throw new Error(arm + ': unmatched projection/transport');
  const emit = first('2735-emit', 'publish_result_ns');
  results.push({
    arm, created_ns: String(stamp(budget.message, 'created_ns')), deadline_ns: String(deadline),
    acknowledgement_ns: String(acknowledgement), acknowledgement_minus_deadline_ms: milliseconds(acknowledgement - deadline),
    native_receipt_call_ms: milliseconds(acknowledgement - receiptEntry),
    emit_minus_deadline_ms: milliseconds(stamp(emit.message, 'publish_result_ns') - deadline),
    last_sample_ns: String(stamp(last('2735-quiescence-sample', 'sample_ns').message, 'sample_ns')),
    last_report_return_ns: String(stamp(last('2735-diagnose', 'return_ns').message, 'return_ns')),
    contract_loads: contracts.length, contract_total_elapsed_ms: milliseconds(contracts.reduce((a, b) => a + b, 0n)),
    last_contract_elapsed_ms: contracts.length ? milliseconds(contracts.at(-1)) : null,
    models: observations.filter(row => row.mode === 'model' && row.model).length,
    reads: observations.filter(row => row.mode === 'call' && row.tool === 'read_flow_data' && !row.failure).length,
    emits: observations.filter(row => row.mode === 'call' && row.tool === 'emit_work_completed' && !row.failure).length,
    release
  });
}
console.log(JSON.stringify(results, null, 2));

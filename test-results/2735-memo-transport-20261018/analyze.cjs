const fs = require('node:fs');
const path = require('node:path');
const root = __dirname;
const rows = fs.readFileSync(path.join(root, 'candidate-phases.txt'), 'utf8').trim().split('\n').map(line => {
  const match = line.match(/Category="([^"]+)" Message=(".*")$/);
  return match ? {category: match[1], message: JSON.parse(match[2])} : null;
}).filter(Boolean);
const stamp = (row, key) => {
  const match = row?.message.match(new RegExp('(?:^| )' + key + '=([0-9]+)'));
  return match ? BigInt(match[1]) : null;
};
const all = category => rows.filter(row => row.category === category);
const first = (category, key) => all(category).find(row => stamp(row, key) !== null);
const ms = value => Number(value) / 1e6;
const budget = first('2735-budget', 'deadline_ns');
const deadline = stamp(budget, 'deadline_ns');
const run = budget.message.match(/run=([^ ]+)/)[1];
const origin = all('2735-completion').find(row => row.message.includes('settlement_entry_ns=') && row.message.includes('class:agent subscriberID:worker'));
const delivery = origin.message.match(/deliveryID:([^ ]+)/)[1];
const receipt = all('2735-receipt').filter(row => row.message.includes(`delivery=${delivery}`) && row.message.includes('native_result_ns=') && row.message.includes('status=processed') && row.message.includes('acknowledged=true'));
if (receipt.length !== 1 || !receipt[0].message.includes(`run=${run}`)) throw new Error('Unmatched exact agent claim');
const ack = stamp(receipt[0], 'native_result_ns');
const nativeEntry = all('2735-receipt').find(row => row.message.includes(`delivery=${delivery}`) && stamp(row, 'native_entry_ns') !== null);
const ready = stamp(first('2735-budget', 'ready_ns'), 'ready_ns');
const negative = all('2735-quiescence-sample').filter(row => row.message.includes('Ready:false') && stamp(row, 'sample_ns') < ready).at(-1);
const negativeAt = stamp(negative, 'sample_ns');
const negativeReturn = all('2735-diagnose').find(row => stamp(row, 'return_ns') > negativeAt);
let decoded;
const contracts = [];
for (const row of all('2735-operational-source')) {
  if (stamp(row, 'artifact_decoded_ns') !== null) decoded = stamp(row, 'artifact_decoded_ns');
  if (stamp(row, 'contract_loaded_ns') !== null && decoded !== undefined) contracts.push(stamp(row, 'contract_loaded_ns') - decoded);
}
const traceDir = path.join(root, 'retained-server2/candidate/trace');
const releases = fs.readdirSync(traceDir).filter(name => /^2735-proxy-\d+\.json$/.test(name));
if (releases.length !== 1) throw new Error('Unexpected first-response receipt multiplicity');
const release = JSON.parse(fs.readFileSync(path.join(traceDir, releases[0])));
if (!release.matched || release.canceled || release.projection_error) throw new Error('Unmatched scheduled transport');
const observations = fs.readFileSync(path.join(traceDir, 'observations.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
const metadata = JSON.parse(fs.readFileSync(path.join(traceDir, 'process-322388.json')));
if (metadata.trace_overflow) throw new Error('Trace overflow invalidates analysis');
const previous = JSON.parse(fs.readFileSync(path.join(root, 'prior-pair-summary.json')));
const summary = {
  head: '55adb583b9de0572f69e6e4adb23776ac0851ed1', run, delivery,
  invocation: 'authorized fixed candidate N=1; no new base or replacement',
  outcome: 'PASS',
  created_ns: String(stamp(budget, 'created_ns')), deadline_ns: String(deadline),
  acknowledgement_ns: String(ack), acknowledgement_minus_deadline_ms: ms(ack - deadline),
  native_receipt_call_ms: ms(ack - stamp(nativeEntry, 'native_entry_ns')),
  emit_ns: String(stamp(first('2735-emit', 'publish_result_ns'), 'publish_result_ns')),
  emit_minus_deadline_ms: ms(stamp(first('2735-emit', 'publish_result_ns'), 'publish_result_ns') - deadline),
  ready_ns: String(ready), ready_minus_deadline_ms: ms(ready - deadline),
  last_negative_sample_ns: String(negativeAt), last_negative_return_ns: String(stamp(negativeReturn, 'return_ns')),
  last_negative_return_minus_deadline_ms: ms(stamp(negativeReturn, 'return_ns') - deadline),
  contract_loads: contracts.length, contract_total_elapsed_ms: ms(contracts.reduce((a, b) => a + b, 0n)),
  memo_hits: all('2735-operational-source').filter(row => stamp(row, 'memo_hit_ns') !== null).length,
  retained_source_reads: all('2735-operational-source').filter(row => stamp(row, 'artifact_read_ns') !== null).length,
  models: observations.filter(row => row.mode === 'model' && row.model).length,
  reads: observations.filter(row => row.mode === 'call' && row.tool === 'read_flow_data' && !row.failure).length,
  emits: observations.filter(row => row.mode === 'call' && row.tool === 'emit_work_completed' && !row.failure).length,
  trace_bytes: metadata.trace_bytes, trace_overflow: metadata.trace_overflow,
  go: metadata.go, actual_cli_gomaxprocs: metadata.gomaxprocs, inherited_cli_gomaxprocs: metadata.inherited_gomaxprocs,
  release, previous_pair: previous.map(row => ({arm: row.arm, acknowledgement_minus_deadline_ms: row.acknowledgement_minus_deadline_ms, contract_loads: row.contract_loads, contract_total_elapsed_ms: row.contract_total_elapsed_ms})),
  limits: 'Elapsed spans are not CPU or SQL-lock measurements; N=1 does not prove sole cause of original hosted RED; prior base/candidate remain attributed, not resampled.'
};
fs.writeFileSync(path.join(root, 'fixed-candidate-summary.json'), JSON.stringify(summary, null, 2) + '\n');
console.log(JSON.stringify(summary, null, 2));

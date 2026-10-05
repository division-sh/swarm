const fs = require('node:fs');
const crypto = require('node:crypto');
const zlib = require('node:zlib');

const source = '67742d4553c2afeae40b58dbd7fe31b1e715dfa0';
const attachmentRoots = [
  'TestFlowAttachmentAgentAcquisitionCutsBothStores',
  'TestFlowAttachmentRouteAcquisitionCutsBothStores',
  'TestFlowAttachmentCleanupRetainsExactPredecessorBothStores',
  'TestFlowAttachmentNativeLostAckAfterRebindBothStores',
  'TestFlowAttachmentNativeCommitAcknowledgmentBothStores',
  'TestFlowAttachmentPhaseFailureRetainsConstructionBothStores',
  'TestFlowAttachmentTimerAcquisitionFailureCannotBecomeReadyBothStores',
  'TestFlowAttachmentTimerAcquisitionCutsBothStores',
  'TestFlowActivationAttemptBatchRetirementBothStores',
  'TestFlowActivationAttemptAdmissionBothStores',
  'TestFlowAttachmentConditionalPhaseProgressBothStores',
  'TestFlowAttachmentPlanABARetainsPredecessorUntilSettlementBothStores',
  'TestFailedFlowActivationRetirementIsPendingBothStores',
  'TestFailedFlowActivationCreationPosturesRemainRetryableBothStores',
];
const publicRoots = [
  'TestSelectedForkPublicChangedTargetExecutionBothStores',
  'TestSelectedForkPendingInputBothStores',
  'TestRootConnectedTemplatePublicationAcknowledgmentLossBothStores',
  'TestReceiverCompositionRestartBothStores',
  'TestReleaseReceiverInitializationBothStores',
  'TestReceiverCompositionFailureSettlementBothStores',
];
const counts = rows => Object.fromEntries(['pass', 'fail', 'skip'].map(action =>
  [action, rows.filter(row => row.Action === action).length]));

function read(label) {
  const file = `issue-2496-restamp-qualified-677-${label}.log.gz`;
  const bytes = zlib.gunzipSync(fs.readFileSync(`${__dirname}/${file}`));
  const text = bytes.toString();
  const events = text.split('\n').filter(line => line.startsWith('{"Time":')).map(line => JSON.parse(line));
  const outcomes = events.filter(event => ['pass', 'fail', 'skip'].includes(event.Action));
  const tests = outcomes.filter(event => event.Test);
  const roots = tests.filter(event => !event.Test.includes('/'));
  const packages = outcomes.filter(event => !event.Test);
  const pending = new Map();
  for (const event of events) {
    if (!event.Test) continue;
    const key = `${event.Package}:${event.Test}`;
    if (event.Action === 'run') pending.set(key, (pending.get(key) ?? 0) + 1);
    if (['pass', 'fail', 'skip'].includes(event.Action)) pending.set(key, (pending.get(key) ?? 0) - 1);
  }
  if (!roots.length || !packages.length || outcomes.some(event => event.Action === 'fail') ||
      [...pending.values()].some(count => count !== 0)) {
    throw new Error(`${label}: missing, failing or incomplete execution`);
  }
  return {file, bytes, text, events, tests, roots, packages};
}

function requireCells(parsed, root, backend, repetitions) {
  if (parsed.roots.filter(event => event.Test === root && event.Action === 'pass').length !== repetitions) {
    throw new Error(`${parsed.file}: missing root repetitions for ${root}`);
  }
  const cells = parsed.tests.filter(event => event.Test.startsWith(`${root}/${backend}`));
  if (!cells.length) throw new Error(`${parsed.file}: no ${backend} execution for ${root}`);
  for (const name of new Set(cells.map(event => event.Test))) {
    if (cells.filter(event => event.Test === name && event.Action === 'pass').length !== repetitions) {
      throw new Error(`${parsed.file}: incomplete backend cell ${name}`);
    }
  }
}

const cleanup = read('attachment-sqlite-cleanup-race');
const cuts = read('attachment-sqlite-cuts-race');
const sqlite = {...cleanup, file: 'two bounded SQLite commands',
  roots: [...cleanup.roots, ...cuts.roots], tests: [...cleanup.tests, ...cuts.tests]};
if (sqlite.roots.length !== attachmentRoots.length * 3 || sqlite.tests.some(event => event.Action !== 'pass')) {
  throw new Error('SQLite attachment family does not cover exactly fourteen roots at race/count3');
}
for (const root of attachmentRoots) requireCells(sqlite, root, 'sqlite', 3);

const served = read('public-receiver');
if (served.roots.length !== publicRoots.length || served.tests.some(event => event.Action !== 'pass')) {
  throw new Error('public receiver qualification is incomplete');
}
for (const root of publicRoots) {
  const backends = root === 'TestReceiverCompositionRestartBothStores'
    ? ['sqlite', 'postgres'] : ['default_sqlite', 'explicit_postgres'];
  const suffixes = {
    TestSelectedForkPendingInputBothStores: ['unchanged', 'compatible', 'incompatible', 'missing_input', 'ambiguous_artifact', 'explicit_child', 'ordinary_child', 'ordinary_root', 'reference', 'pause_resume'],
    TestReleaseReceiverInitializationBothStores: ['connected_typed_creation', 'direct_provider_schema'],
    TestReceiverCompositionFailureSettlementBothStores: ['unavailable_after_publish'],
  }[root] ?? [''];
  for (const backend of backends) {
    requireCells(served, root, backend, 1);
    for (const suffix of suffixes) {
      const name = [root, backend, suffix].filter(Boolean).join('/');
      if (served.tests.filter(event => event.Test === name && event.Action === 'pass').length !== 1) {
        throw new Error(`public receiver qualification lacks exact required case ${name}`);
      }
    }
  }
}

const managed = read('managed-default');
const units = [...managed.text.matchAll(/^swarm-test local: starting (.+)$/gm)].map(match => match[1]);
if (units.length !== 14 || new Set(units).size !== 14 ||
    !managed.text.includes('swarm-test local: all 14 planned units passed required execution')) {
  throw new Error('managed default did not complete all fourteen required-execution units');
}
for (const root of ['TestSelectedForkWorkflowOwnerAssociations', 'TestSelectedForkStaticReceiverConfigPreservesBusinessControlCollisions']) {
  if (!managed.roots.some(event => event.Test === root && event.Action === 'pass')) {
    throw new Error(`managed default lacks the repaired fixture root ${root}`);
  }
}

const receipts = [cleanup, cuts, served, managed].map(parsed => ({
  file: parsed.file, source,
  sha256_uncompressed: crypto.createHash('sha256').update(parsed.bytes).digest('hex'),
  bytes_uncompressed: parsed.bytes.length,
  first_test_event: parsed.events[0].Time,
  last_test_event: parsed.events.at(-1).Time,
  root_outcomes_including_repetitions: counts(parsed.roots),
  test_outcomes_including_repetitions: counts(parsed.tests),
  package_results: parsed.packages.map(({Package, Action, Elapsed}) => ({package: Package, action: Action, elapsed: Elapsed})),
  named_roots: parsed.roots.map(({Package, Test, Action, Elapsed}) => ({package: Package, test: Test, action: Action, elapsed: Elapsed})),
  skips: parsed.tests.filter(event => event.Action === 'skip').map(({Package, Test}) => ({package: Package, test: Test})),
}));

process.stdout.write(JSON.stringify({
  source_head: source,
  boundary: 'Actual committed-source executions. Managed default includes the wrapper required-execution validation, not just green root events. No exact-head CI, rebase-equivalence, independent approval or merge claim.',
  preceding_focused_source: '391e3678ad707a53e4f74dfc06a8cebf25420f04',
  source_delta: ['internal/store/internal/backend/runforkpersistence/run_fork_workflow_ownership_test.go', 'internal/store/internal/backend/runforkpersistence/receiver_config_history_test.go'],
  source_delta_boundary: 'Only these two fixture-input test files differ; production Go and spec are unchanged. Earlier focused executions remain labeled 391, not relabeled as 677 executions.',
  sqlite_attachment_roots: attachmentRoots,
  public_receiver_roots: publicRoots,
  public_receiver_boundary: 'Real served/public execution and readback with controlled providers. Selected changed-target admission uses private canonical artifact setup; it does not prove the publication-to-fork journey or paid real-provider qualification.',
  managed_required_units: units,
  preserved_failures: 'Earlier default fixture failures, fixture development errors and the combined SQLite 600-second timeout remain preserved in separate history receipts. Bounded SQLite runs preserve every original cell, race/count3 and case deadline; no timeout inflation, exclusions or capacity bypass.',
  outstanding: ['Actual-conflict-only rebase and affected qualification.', 'Qualified batched push, exact-head CI and independent final review.'],
  receipts,
}, null, 2) + '\n');

const fs = require('node:fs');
const crypto = require('node:crypto');
const zlib = require('node:zlib');

const source = '391e3678ad707a53e4f74dfc06a8cebf25420f04';
const entries = [
  ['qualified-describe', 'compiled describe: 45 cells, two internal repetitions'],
  ['qualified-ports-race', 'native ports, phase, lost-ack, creation and Manager/serve controls: race/count3'],
  ['qualified-terminal-control', 'one small aggregate-only terminal fencing control: ordinary go test, race/count3'],
  ['qualified-atomic-race', 'TestRuntimeConstructedActorAtomicRebindRetryBothStores'],
  ['qualified-partial-shutdown-race', 'TestRuntimeConstructedActorPartialRefreshShutdownBothStores'],
  ['qualified-survivor-race', 'TestRuntimeConstructedActorCensusSourceSetRebindBothStores'],
  ['qualified-restart-race', 'TestRuntimeConstructedActorCensusRestartBothStores'],
  ['qualified-retained-shutdown-race', 'TestRuntimeConstructedActorRetainedRefreshShutdownBothStores'],
  ['qualified-public-construction', 'standing root tree/progressive presence: public verify/serve/source deletion/hash restart; sequential regression uses compiled internal mock lifecycle plus public RPC, not real-provider qualification'],
  ['qualified-volume-sqlite', 'unchanged 1362 fan-out settlement/readback/hash restart oracle: SQLite/count1'],
  ['qualified-volume-postgres', 'unchanged 1362 fan-out settlement/readback/hash restart oracle: PostgreSQL/count1'],
  ['qualified-attachment-postgres-race', 'complete attachment/acquisition/cleanup protocol family: PostgreSQL/race/count3'],
];
const counts = rows => Object.fromEntries(['pass', 'fail', 'skip'].map(action =>
  [action, rows.filter(row => row.Action === action).length]));
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

function read(label) {
  const file = `issue-2496-restamp-${label}.log.gz`;
  const bytes = zlib.gunzipSync(fs.readFileSync(`${__dirname}/${file}`));
  const text = bytes.toString();
  const events = text.split('\n').filter(line => line.startsWith('{"Time":')).map(line => JSON.parse(line));
  const outcomes = events.filter(event => ['pass', 'fail', 'skip'].includes(event.Action));
  const tests = outcomes.filter(event => event.Test);
  const roots = tests.filter(event => !event.Test.includes('/'));
  const packages = outcomes.filter(event => !event.Test);
  const starts = new Map();
  for (const event of events) {
    if (!event.Test) continue;
    const key = `${event.Package}:${event.Test}`;
    if (event.Action === 'run') starts.set(key, (starts.get(key) ?? 0) + 1);
    if (['pass', 'fail', 'skip'].includes(event.Action)) starts.set(key, (starts.get(key) ?? 0) - 1);
  }
  return {file, bytes, text, events, tests, roots, packages,
    incomplete: [...starts].filter(([, count]) => count !== 0).map(([test, count]) => ({test, count}))};
}

const receipts = entries.map(([label, boundary]) => {
  const parsed = read(label);
  const {file, bytes, text, tests, roots, packages, incomplete} = parsed;
  if (!roots.length || !packages.length || outcomesFail(tests, packages) || incomplete.length) {
    throw new Error(`${label}: missing or unsuccessful qualification`);
  }
  if (label === 'qualified-describe' &&
      tests.filter(event => event.Test.startsWith('TestReadProofFactoringCompiledDescribe/')).length !== 45) {
    throw new Error('compiled describe is missing a cell');
  }
  if (boundary.startsWith('TestRuntimeConstructedActor')) {
    if (roots.length !== 3 || tests.some(event => event.Action !== 'pass')) throw new Error(`${label}: race/count3 incomplete`);
    for (const backend of ['sqlite', 'postgres']) for (const fields of ['fields_false', 'fields_true']) {
      const name = `${boundary}/${backend}/${fields}`;
      if (tests.filter(event => event.Test === name && event.Action === 'pass').length !== 3) {
        throw new Error(`${label}: required native cell incomplete: ${name}`);
      }
    }
  }
  if (label === 'qualified-attachment-postgres-race') {
    if (roots.length !== attachmentRoots.length * 3 || tests.some(event => event.Action !== 'pass')) {
      throw new Error('PostgreSQL attachment protocol family is incomplete');
    }
    for (const root of attachmentRoots) {
      if (roots.filter(event => event.Test === root).length !== 3) {
        throw new Error(`PostgreSQL attachment protocol root incomplete: ${root}`);
      }
      const cells = tests.filter(event => event.Test.startsWith(`${root}/postgres`));
      if (!cells.length) throw new Error(`PostgreSQL attachment protocol has no backend execution: ${root}`);
      for (const name of new Set(cells.map(event => event.Test))) {
        if (cells.filter(event => event.Test === name).length !== 3) {
          throw new Error(`PostgreSQL attachment protocol cell incomplete: ${name}`);
        }
      }
    }
  }
  return {
    file, source, boundary,
    sha256_uncompressed: crypto.createHash('sha256').update(bytes).digest('hex'),
    bytes_uncompressed: bytes.length,
    root_outcomes_including_repetitions: counts(roots),
    test_outcomes_including_repetitions: counts(tests),
    package_results: packages.map(({Package, Action, Elapsed}) => ({package: Package, action: Action, elapsed: Elapsed})),
    named_roots: roots.map(({Package, Test, Action, Elapsed}) => ({package: Package, test: Test, action: Action, elapsed: Elapsed})),
    required_native_cells: tests.filter(event => event.Test.startsWith('TestRuntimeConstructedActor') && event.Test.split('/').length === 3)
      .map(({Test, Action, Elapsed}) => ({test: Test, action: Action, elapsed: Elapsed})),
    skips: tests.filter(event => event.Action === 'skip').map(({Package, Test}) => ({package: Package, test: Test})),
  };
});

function outcomesFail(tests, packages) {
  return [...tests, ...packages].some(event => event.Action === 'fail');
}

process.stdout.write(JSON.stringify({
  source_head: source,
  boundary: 'Focused local qualification on the recorded committed tree, not default-suite completion. No exact-head CI, rebase-equivalence, independent review or merge-approval claim. Native matrices use real construction/Runtime/Manager and native stores, not provider/public-launcher qualification.',
  preserved_failures: 'The earlier composed race package timeout, corrected fixture-expectation REDs, cancelled queued default and zero-test wrong-package command remain distinct historical receipts, never qualification credit. On this source the managed default is RED in two stale selected-fork fixture roots; the combined SQLite attachment race/count3 exceeds the unchanged 600-second package limit during the third repetition. These receipts are retained, not qualification credit.',
  outstanding: ['Repair the two selected-fork fixture inputs and pass the complete managed default.', 'Run the unchanged complete SQLite attachment race/count3 cell set in bounded commands.', 'Conflict-only rebase, affected requalification and exact-head CI.'],
  receipts,
}, null, 2) + '\n');

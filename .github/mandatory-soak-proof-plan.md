# Mandatory Fan-Out Soak

Authority: [lead ruling 5743323725](https://github.com/division-sh/swarm/issues/2394#issuecomment-5743323725).

Every profile (PR common/escalated, full, nightly) includes two required units:
`conformance-soak-sqlite` and `conformance-soak-postgres`. They execute
`TestIssue2394TwentyTwoIntentFifteenMinuteSoakBothStores` with exactly one backend
selected per isolated Ubuntu worker. The ordinary conformance partition uses
`-skip '^TestIssue2394TwentyTwoIntentFifteenMinuteSoakBothStores$'`; no other proof
is excluded. The small finite soak is not a substitute.

## Envelopes

| Boundary | Each backend cell | Rationale |
| --- | --- | --- |
| Original test | 900s pressure window, at least 22 unfinished intents | Unmodified test and contents |
| Original internal assertions | 1s claim-start, 15s no-progress, 90s drain | Unchanged; larger harness envelopes do not relax these |
| Go package timeout | 22m (1320s) | 900s window + 90s drain + 330s setup/readback/cleanup allowance |
| Runner command | 25m (1500s) | Additional 180s compilation/provisioning; shell TERM at budget, KILL after 30s if needed |
| GitHub job | 30m | Additional 300s checkout/cache/evidence allowance, including termination cleanup |
| Ordinary command budgets | 240s broad / 540s full | Unchanged |

The two backend cells may run concurrently on separate workers. Each has one
primary, uncached `-count=1` command; no confirmation retry or race multiplier.
The command is `go run ./cmd/swarm-test -- ./internal/runtime/conformance -run
'^TestIssue2394TwentyTwoIntentFifteenMinuteSoakBothStores$/^BACKEND$' -count=1
-timeout 22m -json`, wrapped by the explicit 1500s command timeout in CI.

## Required Evidence

Both matrices are derived from the same digest-bound plan and contain only unit
IDs. The soak workers check out and assert the plan's execution SHA. The existing
timing evaluator requires all planned primary receipts, exact workflow run and
attempt, triggering-event head, execution head, and whole-job evidence. Soak
receipts additionally require the exact backend and parent to pass, with backend
elapsed at least 900s; wrong backend, skip, short run, missing receipt, or missing
job cannot qualify. Passing the unchanged test includes its final drain/readback.

The required summary explicitly rejects skipped soak jobs. Timing aggregation
and timing-model publication await the mandatory lane. Failed PostgreSQL soak
execution remains a failure; this scheduling change does not repair or waive it.

## Additive Proof Table

| Obligation | Evidence owner / status |
| --- | --- |
| Complete/disjoint ordinary plus both backend partition | `TestMandatorySoakCompleteDisjointPartitionAllProfiles`; finite declaration census and hostile mutations |
| Actual workflow matrix split and shell syntax | `TestMandatorySoakWorkflowMatrixExecutionAndShellSyntax`; executes only matrix projection, never the soak |
| Required aggregation, exact-head checkout, budgets | `TestMandatorySoakWorkflowRequiredExactHeadAndBudgets` plus existing job-evidence guards |
| Missing/duplicate/short/skipped/wrong-head/backend receipts fail | `TestMandatorySoakEvidenceRequiresBothFullBackendReceipts`; synthetic evidence, not workload qualification |
| SQLite full window and final drain on final head | Pending actual CI receipt |
| PostgreSQL full window and final drain on final head | Pending actual CI receipt; prior failures remain preserved |

No test duration, workload, production owner, lease, or final assertion changes
are part of this integration. Local focused guard success is not a soak pass.

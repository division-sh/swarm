# #2735 SQLite forced-restart hosted RED classification

Hosted run: https://github.com/division-sh/swarm/actions/runs/39155837860/job/117760585904
Candidate: 95464fccbd3d3efa4238b71325d3685f25c93644.
Fixed merge-base control: 909222b50cf88232d81af10b6fcc46ab78916fbe.
Venue: vemew, the same stable agent-d/main worktree for all four invocations.
No source edits, diagnostic overlay, server2, hosted dispatch or PR push.

Bound: at most two interleaved pairs, then stop; no retry-to-green.
Each invocation used swarm-test admission and the unmodified root:

    go run ./cmd/swarm-test -- ./internal/releasee2e -run '^TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends$/sqlite$' -count=1 -v

The hosted proof also used count=1 without race; the local selector is narrower
and the local output is verbose rather than JSON. Original startup/run bounds,
process fixture and mock posture are unchanged. These are diagnostic results,
not qualification credit.

| Invocation | Head | 1m/5m/15m host load before launch | Package seconds | Result |
| --- | --- | --- | --- | --- |
| Candidate 1 | 95464fccbd | 4.56 / 5.91 / 7.10 | 33.967 | PASS |
| Base 1 | 909222b50c | 6.88 / 6.36 / 7.18 | 56.285 | PASS |
| Candidate 2 | 95464fccbd | 6.69 / 6.72 / 7.25 | 28.628 | PASS |
| Base 2 | 909222b50c | 4.07 / 6.03 / 6.98 | 33.280 | RED: distinct conversation-count oracle, 4 sessions instead of 3 |

The base's second RED is retained and is NOT credited as reproduction of the
hosted startup refusal. All four invocations are finished; no fifth run.

Actual hosted child stderr:

    activate serve runtime after channel teardown recovery: prepare runtime lifecycle: prepare static declaration topology: reconcile external effect attempts: author activity turn.lifecycle/failed requires exact bundle scope in context

It is a native recovery/author-activity scope refusal during static preparation,
before active validation/readiness, not a smoke Docker pull, missing result
array, memo validation error, or elapsed-time limit. The error chain and guard
are in source files byte-identical between this base and candidate:
manager/runtime.go, manager/static_topology.go, effectpersistence/runtime_external_effects.go,
effectpersistence/completion_recovery.go, llmpersistence/author_activity_turns.go,
authoractivity/scope.go and the unmodified golden root.

The recovery path calls RecordCompletionTurnAuthorActivityTx with its incoming
context. A failed turn draft has no explicit Scope, and scopeForDraft refuses
a non-bundle context. This identifies the failing edge, not what made that
retained attempt reachable in the hosted execution. The changed branch has a
wider N19 diff than the memo alone, so unchanged path source plus local passes
does not prove an inherited failure or exclude a scheduling interaction.

Disposition requested: original hosted RED remains unclassified as to branch
attribution; no infra claim, waiver, rerun or production repair is proposed
without reviewer-e's ruling. A discriminating follow-up, if authorized, would
use the existing native recovery fixture to retain the failed attempt's exact
scope/identity and incoming scope at the recovery-to-author-activity boundary.
No wider stress campaign or timeout change is proposed.

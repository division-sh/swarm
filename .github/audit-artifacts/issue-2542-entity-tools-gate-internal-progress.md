# Entity-tool and gate observer batch: internal integration record

This is a local implementation record, not the batch checkpoint, acceptance,
closure or a new debt count. The accepted ledger remains 6a82aaca93: 5709
qualified findings / 3635 raw sites, including the unchanged 87/70 held tail.
No push, PR, rebase, local tier or preflight receipt is claimed here.

The original scope is recorded in issue comment 6175029785. Reviewer-b's
6175641348 adds the complete gateCompletionRead/request-domain companion and
the 20:02-21:30 checkpoint window. Harnesses and faults outside that observer
chain remain visible debt. The resource pin/prune/corruption family is untouched.

## Implemented ownership

- All ordinary entity harnesses now consume StartPostgresRuntimeStore and the
  existing admitted source/import/pipeline fixture, returning the selected
  native owner rather than a SQL pool. Both duplicate tool store constructors
  are gone; all sixteen original direct calls use the existing native factories.
- Fifteen seedEntityStateRow calls comprised fourteen ordinary imports and one
  same-path sibling. Imports use SetupScenarioEntities. The sibling preserves
  its contradictory validation/inst-1 field row through a named exact native
  fault; it does not acquire the real header or manufacture source history.
- entityruntime owns exact run counts, JSON field/null and status/revision
  projections. mutationlog owns entity-only authored-path counts, ordered
  initial provenance and exact run/entity/agent status-mutation counts. Original
  predicates, row order and independent reads survive.
- Named private/undeclared/stored-stage and source-advance faults retain their
  original predicates and payloads. They use the exact selected coordinator,
  never a reconstructed transaction. The explicit writer inventory records them.
- The fork writer uses genuine source-aware construction. The read-only fork
  retains the two independent historical journal/state commits and original
  clocks; three captures consume CaptureSelectedRunForkRevision. No handwritten
  revision transaction remains. Both fork assertions and all source/fork
  identities, revision, clock, mutation-count and frozen-source checks survive.
- The four retired-constructor zero-write checks remain four independent
  run-only reads. The three human acknowledgment counters retain their exact
  card/continuation/global-human-card predicates and separate read cuts.
- Gate card/changes, joined flow state/config, run receipt rows, API completion
  rows and actual principal use existing decisioncard, pipelinepersistence,
  apiidempotency and operatorchannel owners. Each snapshot read stays separate.
  API rows retain resource_id=cardID OR (method=domain.Method AND key=domain.Key),
  all actor columns and original ordering. Eleven snapshot and four request
  calls carry the current selected reader, including the successor after restart.

## Focused receipts

Evidence lives under /home/youmew/.cache/swarm-2542-family145/.

| Receipt | Result |
| --- | --- |
| entity-tools-whole-dependent-runtime-family.jsonl | PASS 24.597s, 47 affected original roots |
| entity-tools-complete-native-factory-consumers-race-compiled.jsonl | PASS 68.458s, nine roots including both stores and original acknowledgment/refusal cases |
| entity-tools-native-observer-fault-callers-race.jsonl | PASS 12.326s, seven exact runtime roots |
| entity-tools-native-historical-fork-race.jsonl | PASS 7.879s, original historical revision and timestamp assertions |
| entity-tools-native-fork-callers-race-compiled.jsonl | Activated-fork writer PASS 0.89s; aggregate RED from the historical-clock mismatch, repaired by the preceding receipt |
| entity-tools-native-complete-fault-history-controls-race.jsonl | PASS 15.856s; original writer, unchanged header/history, exact clocks, duplicate rollback and failed-second-cut journal retention |
| entity-tools-native-count-and-retirement-race.jsonl | PASS; both-store exact run cardinality, refusal controls and original retired-constructor cases |
| entity-tools-gate-observer-original-cuts.jsonl | PASS 30.911s; all four original interruption/control cells |
| entity-tools-gate-observer-rollback-restart-race.jsonl | PASS 120.116s; rollback/restart plus four real process-boundary cells |
| entity-tools-gate-human-native-controls-race.jsonl | Gate root PASS 12.21s on both stores; aggregate RED from an invalid absent-card UUID in the new human control |
| entity-tools-human-native-controls-valid-id-race.jsonl | PASS 19.718s; genuine absent UUID, foreign/cancelled/closed owner controls, unchanged underlying card assertions |
| entity-tools-complete-factory-source-guards.jsonl | PASS 6.117s; exact historical construction, retired-call inventory and source/typed-overlay hostile controls |

Two deliberate native replacements refused invalid incidental setup rather
than weakening production contracts: the same-path sibling cannot be a normal
scenario import, and ordinary import cannot replace a historical entry clock.
Both are repaired through exact selected-owner fixture cuts. Compile-only reds
from removed imports and a domain package alias are retained in the named logs;
the compiled receipts above supersede them. No timeout increase, retries, skip
or assertion removal was used.

## Remaining batch integration

Compose only this batch's exact history/source intersections once, preserving
all original Before/After records and order, then run the integrated controls,
real downward ratchet, complexity and unused checks. Existing receipts carry
where bodies/owners have not changed. Final typed gate propagation remains
being qualified; the complete record is not yet a review checkpoint.

The deleted ensureEntityToolTestRun, captureEntityToolRunForkRevision and two
duplicate constructor helpers require explicit finite historical retirement.
The sparse/ack/tool/readback and three gate caller endpoints require exact
Successors. Unchanged seedReplyToolContext, seedEntityToolSourceRun and both
gate harnesses must remain unchanged in their historical records.

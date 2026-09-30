# #2498 Implementation Progress

Agent-g, 2026-09-30. Binding standing gate:
https://github.com/division-sh/swarm/issues/2498#issuecomment-5906100464.
This is an intermediate checkpoint, not a Post-Implementation Proof Audit,
PR readiness, complete S/P/M/RB/U qualification, or parent closure.

## Existing-Owner Implementation

- Selected-store standing operations lock the parent before the exact run,
  prove the canonical disposition before no-op/write, and compare the captured
  predecessor. Corrupt relations propagate errors rather than committing no-ops.
- The existing reconciliation result carries acknowledged applied/exact-no-op
  evidence; errors after commit do not erase identity or authorize rollback.
- RuntimeContextManager validates exact loaded source/target/child versus proved
  non-executable absence. Active resume no-op leaves the original child and
  scheduler untouched. Child-only missing-occurrence rejection remains intact.
- Serve retains pipeline exclusion and actual gateway/child/scheduler state
  through durable mutation and resource disposition. Rollback checks current
  authority and preserves prior suppression; acknowledged failure retires the
  captured predecessor without reopening it. Failed scheduler parking returns
  its retained transition to the composing owner rather than restoring blindly.
- The authoritative spec and generated OpenRPC describe the finite matrix.
  The standing classifier moves mechanically to runlifecycle, without an alias
  or second classifier. Its production guard requires exactly one definition.

## Executed Candidate Receipts

The selected-store and race commands use host PostgreSQL; all persistent
families below include SQLite and PostgreSQL. Public process tests use compiled
CLI/authenticated RPC and the existing retained internal mock lifecycle child
(H), not a live-provider claim.

| Proof | Executed result | Credit and limits |
|---|---|---|
| `TestStandingFreshNoopRejectsCorruptRelationBothStores` | PASS; race x3 | Both suspend and resume no-op relation errors fail closed; S10 supplement |
| `TestStandingOperatorAcknowledgesFreshNoopBothStores` | PASS; race x3 | Exact N, applied/no-op result and journal cardinality |
| `TestStandingOperatorRefusesStaleAuthorityBothStores` | PASS; race x3 | Every command refuses a changed generation without journal mutation; not every S17 race |
| `TestStandingOperatorActiveNoopPreservesExactChildAndSchedule`, `TestStandingOperatorNoChildRestoresPriorSuppression`, `TestStandingOperatorRejectsInvalidChildComposition` | PASS; race x3 | Exact original child/scheduler, typed absent states, missing/stale/foreign/fenced/suppressed refusal |
| `TestStandingOperatorCancellationJoinsHeldChildBeforeRestore` | PASS; race x3 | Cancelled restore cannot reopen before the actual held child joins |
| `TestStandingServiceMutationsUseSelectedRuntimePipelineOnBothStores` | PASS 4.749s; expanded cancellation/failure matrix race x3 PASS 35.510s | Selected-context isolation, active/no-child rollback, held-work cancellation, postcommit cleanup and publication failure, fail-closed retry |
| `TestStandingPauseAuthorityPublicBothStores` + `TestStandingResetNonExecutablePublicBothStores` | PASS together 112.741s through swarm-test | Fresh CLI/RPC no-ops, actual SQL rollback injection, suspended resets/restart/resume, exact historical replay, terminal current/revised/retained-override and validated-invalid current/revised reset; S02-S08/S14 components |
| Canonical standing owner/deletion guards | PASS 0.223s | One production classifier, retired interpreters absent, inbound consumes canonical reader |
| API specification and OpenRPC generation check | PASS | Contract coherence only, not runtime closure |

The earlier four-product public control passed 67.898s; the later five-product
receipt above supersedes it. A broad default swarm-test attempt exited 1 without
a retained actionable failure summary; it earns no qualification credit and
must be rerun with complete captured evidence.

## Remaining Mandatory Qualification

- Reconcile current origin/master and independently measure the committed
  complexity baseline without increasing either hotspot count.
- Complete S09/S10 public orphan/restoration/corruption cases and remaining
  S11-S17 barrier, cleanup, source/control-race and composed process proofs.
  Existing held-route/scheduler and selected-store controls must be executed and
  explicitly mapped; a test name or shared-owner assertion is not proof.
- Execute the original P01-P16/RB1-RB4 preserved controls and full numeric/
  paired-agent M01-M23 corpus, including typed timer inventory and permanent
  receipt replay. U1-U5 admitted behavioral mutation receipts remain required.
- Run final default `go run ./cmd/swarm-test` on the reconciled committed head,
  preserve failure evidence, and publish the complete manifestation proof audit
  before requesting PR review. No `--full` or direct whole-tree go test used.

No new issue, framework, schema, migration, compatibility path, vendor dependency
or changes to the separate #2008 WIP. #2407/#2250 remain open. No new semantic
gate is requested for these ordinary in-scope proof obligations.

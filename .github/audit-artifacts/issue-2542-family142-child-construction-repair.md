# #2542 Family 142: Structural-Parent Construction Repair

Binding review: issue comment `6085325174`, R1/P2. Base stays `2fc6a13bd`,
stacked on `29560dbd1`. Original F1 is independently closed. The next mutation
family is preserved separately in the named family143 stash, not mixed into
this repair or its evidence.

## Repair Boundary

All five affected positive join preparations and the shared query-guard and
activity-flow preparations consume `materializedWorkflowInstanceForSource`
with the actual admitted semantic source, run context and instance key. That
existing owner delegates identity to the existing flowidentity constructors.
No identity inference is added to activation storage, and no producer, runtime
validation, claim, publication, outcome or timing policy changes.

The governing spec's receiver instance/creation contracts around lines5912-5947
require exact structural-parent construction, independently of causal sender
and authored template scope. This corrects test inputs to that existing
contract; it does not amend production semantics.

## Construction Sweep And Siblings

One AST sweep inspected all **74** pipeline test-source `Construct` calls;
receipt `~/.cache/swarm-2542-r1-construction-sweep.tsv`. Syntax forms after the
repair are ten direct source-aware arguments, one inline compiled scenario,
25 variable/explicit arguments, and38 generic normalization arguments. These
are syntax counts, not74 bugs or a permission ledger.

- Executable child positives in this cohort are the five join sites and two
  shared preparations repaired together. The existing final/template/operating
  preparation and exact join scope already use the same source-aware owner.
- Scenario construction, scoring, the nested grandchild and the zero-member
  harness already derive identity through `constructedScenarioInstanceForTest`,
  `constructorUnitIdentity` or source-aware preparation. Singleton pilot carries
  its constructor-issued exact parent explicitly. Compiled adapter inputs already
  carry their exact declared root parent; no missing-parent repair is credited.
- Root execution fixtures use exact run coordinates and have no structural
  parent to fill. The loop, recovery, handler, retry, failure and prospective
  timer roots retain those existing inputs.
- Projection, bookkeeping, mutation, run-scope and journal cases are component
  persistence controls, not executable child admission. Generic normalization
  preserves their deliberately supplied stored addresses and metadata.
- Wrong-run, relabeled child, foreign-flow/entity and unowned-transition cases
  are intentional refusal inputs. Their original component assertions remain;
  no blanket normalization that repairs a hostile input before its gate.
- The external fixture forwarding call keeps identity explicit. Its existing
  native activation owner copies inputs rather than silently supplying parents.

No further missing positive source-bound child preparation was found in this
sweep. The broader remaining fixture debt stays under #2542/#2151.

## Manifestation Proof

Go1.26.8, GOMAXPROCS=3, count1, race, vemew only. Prefix
`~/.cache/swarm-2542-r1-`; no full/core or server2 qualification.

| Known manifestation | Exact retained execution proof | Disposition |
| --- | --- | --- |
| Selected-store join schedule owner | `TestWorkflowJoinUsesSelectedStoreScheduleOwnerOnBothStores` | reproduced and fixed, both stores |
| Join mock execution mode | `TestWorkflowJoinSchedulePreservesMockExecutionModeOnBothStores` | reproduced and fixed, both stores |
| Join count with empty state members | `TestWorkflowJoinCountWaitsDespiteEmptyStateMembersOnBothStores` | reproduced and fixed, both stores |
| Durable join stage identity | `TestWorkflowJoinDurableIdentityIncludesStageOnBothStores` | reproduced and fixed, both stores |
| Canonical failure outcome and runtime log | `TestWorkflowJoinFailurePersistsCanonicalDeliveryOutcomeAndRuntimeLog` | reproduced and fixed, both stores |
| Query-guard workflow/collection scope | `TestExecuteNodeContractHandlerQueryEntitiesGuardUsesWorkflowContext` | reproduced and fixed, both stores |
| Activity dispatch outside mutation and exact result reuse | `TestActivityBoringProofHandAuthoredFlowDispatchesOutsideTransactionAndReusesRecordedResult` | reproduced and fixed, both stores |
| Activity source/fork-local request and result identities | `TestActivityBoringProofHandAuthoredReadOnlyForkReexecuteUsesForkLocalIdentity` | execution-proven through the same corrected path, both stores |
| Request-committed/result-absent crash and replay/no redispatch | `TestActivityBoringProofHandAuthoredFlowCrashAfterRequestBeforeResultCompletesOncePostgres` | execution-proven through the same corrected path, existing PostgreSQL scope |

`seven-root-race.jsonl` PASS:7 roots/21 records,29.822s, no failures/skips.
`shared-activity-race.jsonl` PASS:2 roots/4 records,13.194s, no failures/skips.
Every original assertion remains. The unchanged adjacent component refusals
and F1 proof reuse the independently reviewed receipts, not a new broad matrix.

## Hostility, Snapshots And Accounting

The finite AST control checks all seven actual source/run bindings and rejects
an omitted source-aware helper, foreign source and foreign context. Five
affected semantic recipe After snapshots were reconciled to the correction;
their original Before inputs and754-recipe inventory remain unchanged.
`recipe-controls.jsonl` PASS all affected native/inbound/retirement controls.
The initial pre-reconciliation pin check was RED, correctly detecting the five
changed declarations, and is retained as `pre-pin-controls.jsonl`.

The actual `old-preparation/overlay.json` changes only the shared activity seed
back to source-unaware preparation. `old-preparation-race.jsonl` FAILS on both
stores at the retained canonical delivery-disposition assertion. This is causal
hostile evidence, not a qualification pass or retry-until-green.

One checkpoint read-only ratchet/registry, native unused and exact-head
complexity sweep are required before push. Receipt names:
`ratchet-registry.jsonl`, `unused.log`, `complexity/`. No collector, sidecar,
baseline policy, timing, workload or supported-surface obligation is relaxed.
The existing baseline remains11,716 findings/8,520 raw sites; no migration
reduction is credited to this identity-preparation repair.

M09 remains separately tracked as intermittent/unclassified in #2353. Existing
watchlist and parent tracking suffice; no new issue, gate, framework or PR.
Strict completion guards, zero debt, SQLite fork deadline and integrated final
landing qualification remain open. The eventual PR carries one reviewer-selected
`CI-Tier` line and its required tier check.

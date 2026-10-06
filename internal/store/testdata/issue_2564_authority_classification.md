# #2564 Persistence Authority Classification

The initial live-writer cutover recorded six additions and 35 retired findings.
The subsequent fixture migration is accounted for separately below. The exact-signature comparison and raw
authority mutation controls are unchanged. Regeneration removes only findings
whose production declarations/calls were deleted; it does not exempt a surface.

| Added finding | Disposition and owning boundary |
| --- | --- |
| `engine.CommittedFlowDeactivation.FinalizeFlowDeactivation` | `typed-process-local`: named consumption of the already acknowledged terminal result after the entity gate is released; no SQL handle or arbitrary callback. |
| `engine.EvaluatedStateProjection.EvaluationRevision` | `typed-process-local`: reads the immutable R1 carrier's revision; does not reread state or acquire write authority. |
| `pipeline.EntityFieldMutationWriter.ApplyEntityFieldMutation` | `typed-process-local`: tools submit one admitted captured operation to the existing coordinator gate, evaluation carrier and commit owner; no raw-store fallback. |
| `eventCommitTxStore.RequireActiveSourceTx` in `commitWorkflowEngineMutation` | `private-backend`: fresh source authority inside the already-owned canonical mutation transaction, before domain mutation. No raw capability escapes. |
| `requirePostgresRunActive` in the same writer | `private-backend`: run-before-header SQL admission within the canonical PostgreSQL attempt. |
| `requireSQLiteRunActive` in the same writer | `private-backend`: the equivalent selected SQLite transaction admission, preserving its native serialization policy. |

The 35 removed findings belong to the retired EntityPostgresOwner/EntitySQLiteOwner
write-only clock/schema callback fields and constructor parameters, the deleted
`backend/entityruntime/persistence.go` SaveEntityField/diff SQL and callbacks, and
the removed tools EntityPersistence/public facade SaveEntityField methods.
The read projections remain supported. The new tools mutation port consumes
CommitWorkflowEngineMutation/mutationprotocol rather than another SQL protocol.

Execution witnesses include the native R1 stale-draft refusal, operation CAS
reapplication, run-stop both-order/rollback, terminal post-unlock, attributed
writer evidence/rollback/fork and acknowledged no-repeat response tests. The
restored-bypass guards still reject raw Save SQL, substituted R2 and unlocked
writer paths. The registry itself proves exact confinement, not these outcomes.

Normal qualification must run without
`SWARM_UPDATE_PERSISTENCE_AUTHORITY_REGISTRY`; retain
`TestPersistenceAuthorityFindingRegistry`,
`TestPersistenceEffectiveMethodSetsDoNotExposeRawAuthority` and the actual
alias/carrier/compound-event raw-authority mutation controls.

## Post-Rebase Fixture Migration And Fork Fence

The final refresh classifies 425 newly measured findings across 16 named files
relative to the preceding local registry, removing three stale findings. This
registry is an exact source inventory, not the debt baseline or a permission
grant. Neither the #2569 ratchet implementation nor its baseline was edited.

| Consumer/implementation | Disposition and owning boundary |
| --- | --- |
| `bus.beginReceiverDispatch` accepted `runtimeWorkAdmission` context carrier | `typed-process-local`: carries the exact receiver lease through an empty-value projection; neither a SQL handle nor publisher state. |
| `apiidempotency/test_issue2564_workload_evidence.go` | `private-domain-adapter`: observes the existing fixture principal used to attribute native API sessions. |
| `backend/{delivery,eventpersistence,llmpersistence,operatorchannel,pipelinepersistence}/test_issue2564_evidence.go` and corresponding workload files | `private-domain-adapter`: named closed observations of exact attempts, mutations, sessions, routes, timers and receipts; bounded hostile header/readiness setup stays with its domain owner. No arbitrary query, callback or raw result is exposed. |
| `backend/postgres/session_authority.go` rollback outcome and `test_issue2564_workload_evidence.go` | `private-backend`: native clean-rollback disposition and the private original-location session sampler. The public sampler is a sample/close interface, not the concrete connection carrier. |
| `runtimepersistence/{issue2564_workload_evidence.go,test_issue2564_evidence.go}` | `private-runtime-adapter`: exact selected-owner dispatch, pool statistics as closed facts, and one repeatable snapshot across existing domain readers. No store reconstruction from a running pool. |
| `runtimepersistence/test_native_location.go` | `construction-owner`: retains only the test sandbox's original DSN and cleanup, not its raw pool. |

The H1/H2 tests no longer contain SQL or `servedControlProofRuntime` raw carriers.
They preserve all 37 original query predicates in their domain owners, all
workload/provider bodies, timing calls, exact multiplicities and error assertions.
Quiescence uses the existing delivery summary. H2 reopens the original native
location through `construction` and checks the booted schema without bootstrap,
runtime possession or recovery of the child's pool. Its retained native sampler
remains private and available alongside snapshot reads. Failed reads return no
partial evidence, and row/connection cleanup failures remain visible.

The source-bound ratchet, hostile alias/transitive-carrier/compound-event
controls, both-store observation smoke/restart and the unchanged pressure tests
are separate required proofs; inventory agreement alone does not discharge them.

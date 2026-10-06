# #2564 Persistence Authority Classification

The independent authority registry reports six additions and 35 retired findings
from the approved live-writer cutover. Its exact-signature comparison and raw
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

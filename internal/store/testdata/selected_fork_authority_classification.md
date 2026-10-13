# Selected Fork Authority Registry Accounting

Issue #2277, approved Revision 2: `selected_fork_target_context_admission_and_lifetime_ownership`.
This is static authority accounting, not the post-implementation proof audit or a
failure-class closure claim. Runtime SQL/TX authority remains forbidden.

Baseline for this update is `ea08dfcc1`, including A's explicit-source fixture
handoff `d59913e86`. The unchanged registry guard reports 187 new exact findings
and 23 stale signatures. Every finding is stored separately with its complete
resolved signature in `persistence_authority_findings.tsv`; this table explains
their ownership, rather than creating path exceptions in the guard.

## New Findings

Paths are relative to `internal/`. Counts include raw type declarations, local
SQL carriers, individual SQL calls and typed method/callback signatures, not
only public methods or distinct queries.

| File | Count | Disposition and authority |
| --- | ---: | --- |
| runtime/llm/runtime_resolver.go | 1 | typed-process-local: preparation of a provider runtime factory; no transaction callback |
| runtime/runforkexecution/agent_runtime_materialization.go | 2 | typed-process-local: exact Manager access and gateway cleanup; no SQL access |
| runtime/runforkexecution/operation_lifetime.go | 6 | typed-process-local: cancellation, joined cancellation bridge, private operation context; process lease owns committed disposition |
| runtime/manager/run_execution_ownership.go | 1 | typed-process-local: closed run ownership classification, not a query language |
| runtime/runforkexecution/store_ports.go | 3 | typed-process-local: named snapshot, recovery and stop operations with typed input/results |
| runtime/startupownership/process_capability.go | 4 | typed-process-local: live-only SourceSetPlan, distinct selected grant issuance and current possession proof |
| runtime/startupownership/startupownership.go | 2 | typed-process-local: retained-session ownership inspection and exact selected grant proof |
| store/store.go | 6 | typed-public-facade: both-store named recovery list/recovery/stop, no raw SQL/TX return or parameter |
| store/internal/backend/agentpersistence/execution_authority.go | 18 | private-backend: transaction-bound run/binding/grant/receipt authority proof; exact selected identity, normal ownership classification |
| store/internal/backend/agentpersistence/lifecycle.go | 2 | private-backend: retained lifecycle authorization after author-story lock and before domain mutation |
| store/internal/backend/agentpersistence/topology_admission.go | 4 | private-backend: selected declaration plan membership; no live source-set substitution |
| store/internal/backend/effectpersistence/completion_recovery.go | 4 | private-backend: exact selected execution restriction and parent settlement through existing completion owner |
| store/internal/backend/effectpersistence/external_effect_authority_store.go | 4 | private-backend: current prepared-process possession for non-executable probes |
| store/internal/backend/effectpersistence/runtime_external_effects.go | 5 | private-backend: explicit selected-execution candidate restriction; ordinary recovery does not adopt selected work |
| store/internal/backend/effectpersistence/selected_recovery.go | 14 | private-backend: exact fenced predecessor effect settlement; no provider dispatch or retry grant |
| store/internal/backend/managedcapability/preparation.go | 3 | private-backend: original probe surface read and exact prepared-slot validation in the authority transaction |
| store/internal/backend/runforkpersistence/preparation.go | 7 | private-backend: preparation fingerprint, process and receipt validation at mutation/claim |
| store/internal/backend/runforkpersistence/run_fork_selected_contract_binding.go | 3 | private-backend: explicit binding identity in existing immutable binding persistence |
| store/internal/backend/runforkpersistence/run_fork_selected_contract_materialization_owner.go | 1 | private-backend: prove prepared admission before materialization |
| store/internal/backend/runforkpersistence/selected_contract_runtime_execution.go | 7 | private-backend: exact prepared-to-concrete issue and run-serialized claim |
| store/internal/backend/runforkpersistence/selected_recovery.go | 47 | private-backend: consistent recovery census plus one story-ordered atomic fence/effect/run disposition; two private cross-owner transaction methods are confined here |
| store/internal/backend/runforkpersistence/selected_stop.go | 20 | private-backend: current process, exact immutable binding and accepted-work settlement before canonical stop |
| store/internal/backend/runlifecycle/selected_stop.go | 8 | private-backend: existing run-control mutation under selected owner's transaction; no runtime-supplied callback |
| store/internal/startupownership/owner.go | 11 | private-domain-adapter: retained session serializes proof and grant transitions; selected branch does not borrow loaded membership |
| store/internal/startupownership/selected_fork_grant.go | 4 | private-domain-adapter: original selected grant proof plus exact current persisted evidence |

All 162 raw findings remain inside private persistence owners. The 25 typed
findings expose no raw database handle, transaction, generic query operation,
or transaction callback. The private selected recovery composition has only
`MarkTerminalTx` and `RecoverSelectedForkEffectsTx`; it does not export an
arbitrary transaction runner to runtime consumers.

## Removed Signatures

The 23 removals are exact obsolete signatures, not surviving paths ignored by
the inventory:

- Completion recovery: eight SQL call signatures replaced or removed when
  selected parent settlement stopped using ordinary parent mutation paths.
- External effect recovery: five call signatures changed to carry the explicit
  selected execution restriction. The ordinary calls remain separately counted.
- Selected binding: two signatures changed for explicit binding identity.
- Selected execution issue: three old scan signatures replaced by the strict
  prepared/execution coordinate reads.
- Retained grant transition: two old insert signatures replaced by the closed
  live/selected evidence representation.
- Gateway cleanup: one unnamed result signature replaced by its named result.
- Generation interfaces: the old common SourceSetPlan method and old live grant
  return signature are replaced by the live-only interface. Selected grants
  cannot implement loaded source-set authority through the common interface.

## Verification Boundary

`TestPersistenceAuthorityFindingRegistry` must match every resolved finding.
`TestPersistenceEffectiveMethodSetsDoNotExposeRawAuthority` independently checks
effective public and semantic-owner method sets. The hostile resolved-type,
transitive-carrier and unknown-local-operation tests remain unchanged; no
file-level allowlist or automatic disposition was added to any guard.

Execution evidence is recorded separately: exact grant use and lifecycle
replacement, prepared probe/receipt admission, control retirement, and
predecessor recovery tests must retain their both-store assertions. Passing
this registry does not prove P01-P24, supported public journeys, race closure,
or full-suite success. A's history/projection rulings and #642 refusals remain
separate and are not decided by this accounting.

## C4 One-Shot And Native Constraint Leaves

The #642 C4 update classifies 14 exact new signatures as `private-backend`
and removes three obsolete `admitTx` call signatures. These are the already
authorized native owners, not new runtime SQL access or path exemptions:

| File / responsibility | New findings | Retired findings | Boundary |
| --- | ---: | ---: | --- |
| backend/genericschedule/fork_one_shot.go / RestoreForkOneShotTx | 5 | 0 | Existing mutation frame, fresh child source admission, shared insertion and exact native readback; no source-state adoption |
| backend/genericschedule/owner.go / admitWithRestorationTx and admitTx | 6 | 3 | The same insertion/idempotency decisions serve ordinary and authenticated restored rows; the SQL carrier and clock callback remain private |
| backend/pipelinepersistence/test_c4_typed_start_timer_constraints.go | 2 | 0 | Existing Attempt.WithSQL and native timer INSERT exercise real CHECK/FK refusal with rollback, not test-side raw SQL |
| backend/runforkpersistence/run_fork_one_shot_materializer.go | 1 | 0 | Typed private cross-owner restoration request; no arbitrary callback or raw handle |

The three retired entries are insertActivationTx and the two loadByKeyTx calls
moved from admitTx to admitWithRestorationTx. The finding registry guard,
effective-method guard and persistence debt ratchet are unchanged. Neither
the raw test-debt baseline nor a guard allowlist is expanded. This accounting
does not close C4 or give the required SQLite T17 first-request row credit.

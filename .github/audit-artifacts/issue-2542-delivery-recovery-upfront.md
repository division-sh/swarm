# Native delivery lifecycle and startup recovery: upfront integration record

Track: FULL. Signed accepted predecessor:
3bb704336cdbce8e70a17e0190de6b96b453707a (signature G).
Acceptance: #2542 comment6188636241. Requested backup is verified on origin at
agent-b/backup-2542-20261015-3bb704336c, pointing to that exact signed commit.
This record precedes implementation; no code, baseline or policy has changed.

## Class, owners and expected yield

The working class is the native selected-store construction, exact storage
observation and named live-fault chain shared by executable-delivery lifecycle
conformance and node startup/standing recovery. The symptom is raw pools and
handwritten transactions in tests; the repair targets the construction and
fault-lifetime dependency, not isolated query relocation. Parent #2542/#2151
stays open. This wave does not claim production lifecycle defects or closure of
other conformance families.

| Caller file | Findings/raw | Exact counted scope |
| --- | ---: | --- |
| internal/runtime/conformance/delivery_lifecycle_conformance_test.go | 81/61 | Fourteen local raw types, one raw field type, five concrete constructions, 61 operations; the top-level parity root and its complete helper/carrier chain |
| internal/runtime/node_delivery_startup_recovery_test.go | 57/35 | Eight raw-bearing roots/helpers, including unused pool return channels in hydration setup, standing retry/expiry and poison-mutation fault setup |
| Total opportunity | 138/96 | Existing native construction debt is retained unless actually removed; no forced credit target |

The exact accepted physical baseline is 4311/2496, with 67 unresolved excluded
occurrences. Held 163/135 is unchanged, so qualification-adjusted remaining is
4474/2631. Target one natural 100-300 checkpoint; approximately 120-138
findings is the current opportunity estimate, not removal credit. No held
channel/mock/numeric-fork qualification is waived or absorbed into this count.
The accepted run-fork execution residual 8/4 is untouched.

Original native factory/reopen recipes own store construction and cleanup.
The existing delivery owner owns exact attempts, claims, retry eligibility,
settlement, failure observations and its named live faults. Entity mutation
poison faults belong to the existing entity owner; historical session
termination belongs to the existing session owner. Author-activity observations
remain with their established owner. Storetest exposes only fixed typed
operations through runtimepersistence and the original selected coordinator.
No recovered database/coordinator, generic SQL/callback, new role or facade,
compatibility path, router, transaction framework or permissive fallback.

## Complete declaration and consumer boundary

Conformance counted declarations (besides its import/package references):

- TestExecutableDeliveryLifecycleParity, 13 findings.
- deliveryLifecycleConformanceBackend and deliveryLifecycleConformanceBackends, 1 and 9.
- ageDeliveryClaimForConformance and expireDeliveryClaimForConformance, 13 each; makeDeliveryImmediatelyEligible, 3.
- assertDeliveryAttemptHistory, 6; assertDeliveryAttemptLeaseMatchesObligation, 2.
- installDeliveryDeadLetterFault, 6; assertDeliverySettlementRolledBack, 2; assertExactDeliveryDeadLetter, 4.
- loadDeliveryHandlerRuleSelectionFact and assertDeliveryHandlerRuleSelectionMissing, 2 each.
- assertDeliverySQLRejected and assertDeliverySchemaRejectsDisconnectedFacts, 1 and 2.

Startup/recovery counted declarations:

- TestRuntimeStartHydratesPersistedAgentsBeforeRecoveringNodeDeliveriesParity, 6/4.
- TestRuntimeStartRecoveryDisabledRejectsExecutableDeliveryInventoryParity, 9/4.
- TestPipelineCoordinatorRecoveryContinuesAfterCommittedDeadLetterParity, 8/5.
- TestPipelineCoordinatorStandingRecoveryClaimsNewlyEligibleNodeDeliveries, 10/7.
- expireNodeDeliveryClaim, 11/7; assertExpiredNodeDeliveryAttemptHistory, 7/5.
- makeNodeDeliveryImmediatelyEligible, 4/2; installNodeRecoveryPoisonMutation, 2/1.

Source search finds no external callers of the raw backend carrier, paired
construction, expiry/aging/retry helpers, schema-refusal helper or either named
trigger installer outside these two files. Mechanically connected callsites
remain in this boundary; report any additional declaration in the final table.
Existing genuine activation/renewal/settlement wrappers and manager/runtime
construction remain real and are not replaced with fabricated receipts.

## Outputs and temporal cuts that must remain

Conformance retains all 15 named children on both backends: exact-route
claim/settlement/outcome; singular final selection and terminal rewrite fencing;
continuation restart and exact payload bytes; normal authority/predecessor
reclaim; structural retry budgets; closed claim/cursor disposition matrix;
store-owned recovery inventory/opaque wake; postcommit handoff cursor reset;
renewal and reclaim fencing; parent terminalization with late writer; pending
and failed terminalization; concurrent claim/restart reclaim; disconnected
schema facts; expired work ahead of continuous backlog; exact atomic terminal
diagnostics, including both agent/node children. Keep every case and assertion.

The startup path remains native store/source admission -> persisted agent
hydration -> recovery admission -> exact claim/renewal -> workflow mutation ->
pipeline settlement -> continuation -> terminal evidence -> joined teardown.
The disabled-recovery matrix retains both backends, both disable modes and all
12 inventory postures (48 cells), including paused, future-failed, busy,
reclaimable, foreign-source and empty controls. The two already-native cleanup
handoff/continuation roots remain unchanged but are affected proof consumers.

Specific non-interchangeable cuts:

1. Expiry writes delivery and open-attempt clocks together in one original
   selected transaction, with the current status/open-marker predicates and
   exactly-one affected-row checks. Share one corrected owner between both
   helper families. Do not substitute a lease-only fault if it drops the
   created_at/started_at/updated_at or exact-clock assertions.
2. Aging and renewal preserve original clocks, exact claim identity, ordered
   attempts, NULL outcomes, and the distinction between expired attempts and
   settled outcomes. Retry eligibility preserves failed-only and one-row cuts.
3. The PostgreSQL long-transaction case preserves CURRENT_TIMESTAMP versus
   advancing wall clock, its existing controlled 1100ms delay, and claim,
   retry and lease observations in the same transaction. Replace reconstructed
   eventfixture/adapter plumbing with a fixed named proof operation on the
   original coordinator, not an exported arbitrary SQL or unit-of-work hook.
4. Schema negatives retain each of the 14 disconnected-fact refusal cases,
   including the expired-attempt/dead-letter case. Use fixed named probes, not
   SQL strings passed from the caller. Admission/open failures must fail the
   fixture rather than masquerade as an expected database-constraint refusal.
5. Dead-letter and entity poison faults preserve real failing native writes,
   rollback evidence, exact predicates and cleanup ordering. Arm cleanup before
   a potentially durable install; a lost COMMIT acknowledgement cannot skip
   cleanup. No successful synthetic SQL or direct raw pool is a substitute.
6. Settlement rollback keeps outcomes/dead letters/activity evidence observed
   at the same cut. Exact terminal diagnostics retain all event/delivery/
   attempt/failure/payload/retry/depth/handler fields and typed failure equality
   across attempt and author activity. Keep these domain owners separate.
7. Peer/reopen proof uses an independently constructed native owner at the
   same location, never two adapters reconstructed over the same pool. Preserve
   original fixture cleanup order; this is not process-restart qualification.

Reuse exact existing projection/fault ports first. Missing fixed operations may
be added within the existing lawful owner and selected fixture bridge; no closed
file inventory exemption or generic selector. Table contracts are adjacent
platform-spec.yaml event_deliveries:18090, event_delivery_attempts:18240,
dead_letters:19555 and author_activity_occurrences:16297. No exact spec section
prescribes this test API. Runtime contracts are unchanged; a discovered semantic
change requires a boundary decision, not a silent spec or implementation change.

## Complete history, source and ordinary-text integration census

At the accepted predecessor, exact-file scans find five recipe rows and two
family145 transition rows, and no family144 row or direct preimage-manifest
intersection for either caller file. The two existing recipes that may acquire
Successors are the startup dead-letter recovery and standing-newly-eligible
roots. Preserve the other three recipe rows unchanged: canonical continuation
recovery, seedDeliveryAgentSession, and committed-handoff cleanup failure.
Both existing transition rows (session seeding and committed-handoff cleanup)
remain unchanged. Append new transitions for the actual migrated declarations;
never overwrite old Before/After/order or blanket-regenerate the corpora.

Named earlier consumers identified before coding:

- reconcile-historical-models pins the three startup recovery roots at immutable source56a8e9a7610685b60b18b4fbb379f804351a69f0; preserve its historical receipt, and recover the two changed current endpoints through an authenticated finite inverse.
- reconcile-family145 lists both files in its existing finite source set; extend that existing reconciler, not a new codemod driver.
- internal/store/persistence_authority_observation_batch_test.go closes requireCanonicalDeliveryLifecycleSurface; preserve this guard and its hostile raw/callback controls.
- run_bundle_identity_repository_test.go includes the startup file in its repository inventory; keep its source and construction assertions.
- scripts/rewrite-pins-2438/go_sources.go includes the startup file; preserve all compiled source literals and pin/intent semantics, and run its actual-corpus/pin correspondence controls.

No direct standing29, source-fork complete-file pin or ordinary-text tuple
intersection was found for either file. Indirect global-history consumers still
matter: compose before the latest accepted selected-fork inverse, authenticate
the exact complete predecessor and current source, and run the standing/fork/
ordinary-text hostile controls once at the combined checkpoint. Keep the
selected-fork 14-declaration manifest and all its histories unchanged. No new
accounting transition, classifier relaxation or ordinary-text tuple is proposed.

## Cross-lane reservations and one qualification pass

The merged #2617 commit has no direct change to these two caller files. Avoid
the 611 runtimepersistence construction family and the semantic SQL connector
family: D's #2674 owns fan_out_semantic_proof_helpers_test.go. That connector
family was inspected but is NOT selected for this wave. No grammar YAML,
node-source generator, whole-file pin, A source-business-event/claim/settlement
hunk, E S6 role/forwarder/registry, ActiveSnapshots or ReadResetInventory change.
Any missing delivery fault/read operation is append-only in an already-lawful
owner file; do not edit or reorder A's existing adapter/settlement functions.

During implementation use focused affected roots and both-store race controls
only. Prove exact source clocks/predicates, original selected transaction
counters, invalid/foreign/cancel/closed-owner refusals, fault cleanup under
acknowledgement uncertainty and rollback/no-mutation. Run all six startup roots
and the full conformance root, preserving existing cases and timing limits.

At one combined checkpoint: compose the complete finite source/history once;
run affected history/pin/ownership/observation/schema controls and hostile
mutations, compile/vet first, U1000 and exact-snapshot complexity, then the global
census plus real downward/read-only ratchet. Report actual removals separately
from retained native construction and all existing holds. Carry unchanged
receipts; no duplicate local tier, retries, skips, timeout or budget increase.

The orchestrator still owns the single A+D integration/rebase release. This
record and implementation remain local; no PR or qualification dispatch. The
explicitly requested signed backup above is a non-PR ref, not hosted evidence.
Fresh exact-head preflight/local is required on the integrated candidate before
an eventual PR push; no stale or unavailable old-base receipt is claimed.

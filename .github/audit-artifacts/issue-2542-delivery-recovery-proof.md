# Native delivery lifecycle and startup recovery: proof audit

Track: FULL. Binding scope: #2542 comment6188925293; pre-implementation
record comment6188797108 and issue-2542-delivery-recovery-upfront.md.
Accepted code predecessor: 3bb704336cdbce8e70a17e0190de6b96b453707a.
This is a local stacking checkpoint, not parent closure or landing qualification.

## Concept, boundary and owners

Changed concepts are test authority for native selected-store construction,
exact delivery storage observation, temporal cuts and fault lifetime. No live
runtime policy, persisted identifier, source admission or public API changes.
The working failure class is the complete raw carrier/helper dependency in
delivery_lifecycle_conformance_test.go and node_delivery_startup_recovery_test.go.
Parent #2542/#2151 remains open. The issue is symptom-shaped at its outer edge
(raw SQL); this wave removes the construction/transaction/fault dependency,
not just individual queries. Closure is a bounded owner migration, subject to
the explicit runtime qualification items below; it is not zero-debt closure.

Systematic owner-consumption census, including every new fixed bridge:

| Canonical owner after migration | Complete touched consumer set and consumption proof |
| --- | --- |
| Existing native runtime fixture factories and independent reopen factory | deliveryLifecycleConformanceBackends and the four changed startup setup roots. No pool return, reconstructed store or stored raw field remains. Actual conformance peer claims and reopen readback execute through independently constructed native owners at the same fixture location. This is not process-restart proof. |
| Existing delivery claim-fenced fault owner | expireNodeDeliveryClaim and expireDeliveryClaimForConformance both consume ExpireExactDeliveryClaimFault with the actually acquired Claim. ageDeliveryClaimForConformance consumes AgeManagerClaimStartedAt. The delivery/attempt two-row temporal cut, run/version/token fencing and rollback owner are reused, not copied. |
| Existing delivery owner, fixed one-hour retry cut | makeNodeDeliveryImmediatelyEligible and makeDeliveryImmediatelyEligible both consume MakeDeliveryRecoveryRetryEligible. The existing one-minute MakeManagerRetryEligible contract is untouched. Native retry controls compare the whole physical store, exact changed column, clock bounds, sibling and original coordinator counters. |
| Existing delivery read owner | assertDeliverySettlementRolledBack consumes one composed ReadDeliveryRollbackStorage query for outcomes/dead letters/terminal activity at a coherent cut. assertExactDeliveryDeadLetter uses ReadExactDeliveryDeadLetterStorage and ReadDeliveryAttemptFailureStorage. Both attempt helpers use ReadDeliveryConformanceAttemptsStorage; lease linkage uses CountMatchingOpenDeliveryAttemptStorage. Exact selection and absence helpers use ReadDeliveryConformanceSelectionStorage and CountDeliveryHandlerSelectionsStorage. All eight read bridges are included in the existing read-cut/refusal matrix. |
| Existing author-activity read owner | assertExactDeliveryDeadLetter delegates the latest exact delivery failure to ReadLatestDeliveryActivityFailureStorage; it does not redefine activity semantics. Original payload, failure and diagnostic comparisons remain. |
| Existing delivery owner, fourteen finite schema cases | assertDeliverySchemaRejectsDisconnectedFacts -> assertDeliverySQLRejected -> ProbeDeliveryConstraint. Closed typed case/identity input; no SQL selector. Real database constraint code is required after actual execution, including commit-time rejection. Invalid owner, cancelled context, bad input, SQL parse/name errors and unopened admission do not count as schema proof. |
| Existing delivery owner, PostgreSQL clock proof | The recovery-inventory child consumes ProveDeliveryConformanceClock. CURRENT_TIMESTAMP, 1100ms delay, lease update, claim, settlement, continuation observations/scans and all original error assertions remain in one original selected Story transaction. Private helper extraction is reproduced from the signed callback by the committed finite transform. No public callback or SQL is exposed. |
| Existing delivery diagnostic and entity mutation fault owners | installDeliveryDeadLetterFault arms cleanup before native installation. The poison root arms cleanup before constructing consumers, so later registered continuation/pipeline/bus joins run first, then cleanup, then native store closure. Both cleanup operations are idempotent, report errors and use WithoutCancel. Native durable-install/lost-ack, caller cancellation and partial PostgreSQL function cuts are checked for both faults. |
| Existing session owner | seedDeliveryAgentSession uses TerminateDeliveryConformanceSession only for the original historical normal termination at its exact clock. This is neither live release nor rotation authority. |
| Existing finite reconciler and selected-read history owner | Twenty-two exact declaration preimages plus the single raw field retirement are source-authenticated at 3bb704336c. Twenty-two appended transition records and twenty appended recipes; only the two authorized existing startup Successors advance. The other three recipe endpoints and both old transitions stay byte/order equivalent. Current source, carrier and full history are checked after a warm cache. |

Each new storetest entry delegates to runtimepersistence and the original selected
coordinator. Each SQL operation stays in an existing domain owner. No generic
getter, arbitrary transaction callback, SQL argument, backend switch at a caller,
second expiry implementation or compatibility path survives in either file.
The finite writer inventory explicitly classifies the new fixed writers and
three preceding selected-fork fixture writers omitted from its older inventory;
the scanner and owner allowlists are unchanged.

## Manifestation coverage

Evidence prefixes below are relative to
/home/youmew/.cache/swarm-2542-family145/delivery-recovery-.

- C: repaired-schema-and-faults-race.jsonl, complete conformance root, both stores,
  race/count1/GOMAXPROCS2. All fifteen children retained and passing.
- N: native-race.jsonl, six startup/continuation roots, both stores, same flags.
  Read the explicit RED dispositions below; aggregate runtime receipt is RED.
- O: native-race.jsonl, existing expanded read-cut/refusal roots, 46 ports,
  both stores/race; exact read-only counters, absent/empty and bad owners.
- F: repaired-schema-and-faults-race.jsonl, three new native owner roots, both
  stores/race; constraint rollback, one-hour exact retry and fault cleanup.
- H: the23 passing roots in integrated-history.jsonl, affected-finite-recipes.jsonl,
  codemod-test.log and compose-repairs.jsonl; finite transformation, source/history
  authentication, hostile controls and earlier compositions. The aggregate
  integrated-history invocation is RED, as itemized below, not a package pass.
- G: owner-guards.log, repository-guards.jsonl, store-preservation.jsonl and
  pin-guards.jsonl; exact closed owners, source ownership and actual pin corpus.

| Known manifestation | Disposition | Exact proof |
| --- | --- | --- |
| Exact route claim/settlement/outcome | execution-proven through the same corrected path | C/exact_route_claim_settlement_and_outcome, SQLite and PostgreSQL |
| Singular final selection and terminal rewrite fence | execution-proven through the same corrected path | C/singular_final_selection_allows_new_attempt_and_fences_terminal_rewrite |
| Native continuation peer and exact payload | execution-proven through the same corrected path | C/delivery_continuation_restart_preserves_exact_payload_bytes |
| Predecessor claim reclaim under normal authority | execution-proven through the same corrected path | C/normal_authority_activation_reclaims_predecessor_attempt |
| Structural class retry budgets | execution-proven through the same corrected path | C/class_retry_budgets_are_structural |
| Closed claim/cursor dispositions | execution-proven through the same corrected path | C/closed_claim_and_cursor_disposition_matrix |
| Store-owned inventory, opaque wake and PostgreSQL advancing clock | execution-proven through the same corrected path | C/recovery_inventory_and_opaque_wake_are_store_owned; committed clock transform compares all native proof statements and error assertions |
| Handoff restarts before prior cursor | execution-proven through the same corrected path | C/postcommit_handoff_restarts_before_prior_cursor |
| Renewal/reclaim fencing and settlement clocks | execution-proven through the same corrected path | C/claim_renewal_fences_reclaim_and_preserves_settlement |
| Parent terminalization rejects late writer | execution-proven through the same corrected path | C/parent_terminalization_fences_late_writer |
| Parent owns pending/failed resolution | execution-proven through the same corrected path | C/parent_terminalization_owns_pending_and_failed_resolution |
| Concurrent native peer claims and stale claimant refusal | execution-proven through the same corrected path | C/concurrent_claim_and_restart_reclaim_are_fenced |
| Fourteen real schema constraint refusals | reproduced and fixed | C/fresh_schema_rejects_disconnected_lifecycle_facts; F/TestDeliveryRecoveryConstraintRequiresNativeRejectionAndPreservesRowsBothStores; exact finite case table below |
| Expired work precedes pending backlog | execution-proven through the same corrected path | C/expired_claim_precedes_continuous_pending_backlog |
| Exact atomic agent/node terminal diagnostics | execution-proven through the same corrected path | C/terminal_settlement_commits_exact_diagnostic_atomically/{agent,node}; F cleanup cuts |
| Persisted agents hydrate before recovery | execution-proven through the same corrected path | N/TestRuntimeStartHydratesPersistedAgentsBeforeRecoveringNodeDeliveriesParity, both stores |
| Recovery disabled refusal matrix | execution-proven through the same corrected path | N/TestRuntimeStartRecoveryDisabledRejectsExecutableDeliveryInventoryParity, all48 backend/mode/posture cells; unchanged receipt carries |
| Committed handoff cleanup wakes once | execution-proven through the same corrected path | N/TestCommittedPipelineHandoffCleanupFailureWakesExactDeliveryOnceParity, both stores, unchanged |
| Canonical continuation owner | execution-proven through the same corrected path | N/TestDeliveryContinuationCoordinatorRecoversNodeDeliveriesThroughCanonicalSelectedStore, both stores, unchanged |
| Poison dead-letter followed by healthy recovery | split / escalated as separate class | N first receipt passed both stores; joined-cleanup-race.jsonl passes PostgreSQL but SQLite hits unchanged status deadline before teardown. Single poison-predecessor-race.jsonl comparison passes SQLite at3bb704336c; therefore inherited status is NOT established. Explicit qualification hold, not a green aggregate. |
| Standing retry/expiry recovery | split / escalated as separate class | N PostgreSQL passes, SQLite settlement wait RED. Single predecessor standing-base-race.jsonl also RED at earlier handler-start wait; identical standing-head-race.jsonl passes all original assertions. Failure stages differ; no exact causal or clean-master attribution. |
| Invalid/absent/cancelled/closed read owner and partial result refusal | execution-proven through the same corrected path | O, existing read-cut/refusal roots expanded by eight fixed delivery reads; original38 retained |
| Durable fault install with lost ack/cancelled caller/partial PG function | execution-proven through the same corrected path | F/TestDeliveryRecoveryFaultCleanupAfterDurableInstallAndCancellationBothStores, both fault kinds and both stores; partial-function cuts PostgreSQL only, no skip |
| Changed source, lost row, duplicate endpoint, reordered history, raw field resurrection | reproduced and fixed | H/TestFamily145DeliveryRecovery*, exact predecessor recovery and warm-cache hostile controls |

The fourteen constraint cases remain closed: event/run mismatch; missing open
attempt; nonexistent exact session; foreign session; second open attempt;
settlement without evidence; without outcome; without completion; expired
attempt carrying duration; open attempt owning dead letter; partial-null FK;
contradictory open settlement tag; missing typed failure; expired attempt
owning dead letter. C executes each on both stores. The finite codemod preserves
each original query, argument and order, except exactly four PostgreSQL
timestamp expressions receive ::timestamptz. Without these casts PostgreSQL
returned42P08 (inconsistent parameter types), which the old any-error oracle
mistook for a constraint rejection. F proves parse/name errors are not accepted.
No assertion, case, workload, deadline or backend has been removed.

## History, residuals and architecture disposition

The original selected-fork history consumer now peels the new exact composition
before checking its own complete predecessor; its immutable inputs are unchanged.
The mechanical-receipt consumer first checks today's live semantic endpoints,
then peels only this exact new composition before checking its unchanged frozen
24-receipt digest. The read-owner whole-file inverse removes exactly ten added
declarations and recovers the complete3bb704336c bytes; old owner pins are not
replaced with today's hashes. Hostile changed names and whole-file bytes refuse.
The raw carrier retirement is checked against both signed and actual source,
including field types/order, not a generic role rule. Existing standing29,
source-fork complete-file, ordinary-text and historical receipts remain live.

No architecture defect beyond the already-tracked fixture authority split is
promoted here. Longer-term direction remains native selected construction and
domain-owned finite observations/faults, tracked in #2542/#2151; not a new
restoration, transaction or test framework. ROI is removal of the whole shared
carrier/expiry/fault dependency; another domain migration is not needed for
these two files. Watchlist: refine the existing nodes, no new issue or entry.
Parent closure and its remaining child tail remain open; this wave does not
alter the final zero-debt/two-strict-guard/integrated qualification obligations.
The old held163/135 and selected-fork8/4 residual stay uncredited. Any new
runtime qualification hold is reported separately rather than silently deducted.

No platform-spec edit: test-only ownership changed; the cited governing event
delivery/attempt/dead-letter/activity table contracts and runtime semantics did
not. No rebase while the A+D integration reservation remains active, no PR,
no hosted run, no duplicate local tier, no non-backup push. Fresh exact-head
preflight remains mandatory after integration and before any such push; it
does not exist on this held base, so no preflight pass is claimed here.

## Checkpoint receipts and uncredited qualification

Real downward ratchet passes328.10s, and independent read-only ratchet passes
80.95s. Physical4311/2496 ->4177/2400: **134 findings /96 raw sites removed,
zero added**, unchanged collector b5bdd1d92df22693c570dab217a46c8a184f37085f496653d59b732c12399a4d,
unchanged trusted base26701e7b187579220dffe72e01debd2e89cd2675 and67 unresolved
excluded occurrences. The opportunity was138/96: four legitimate selected
native-construction occurrences remain counted, not exempted or erased.
No out-of-scope file supplies removal credit.

Conformance contributes77/61; startup contributes57/35 physically. Reserve
the poison recovery root's8/5 plus its dedicated install helper2/1 as an exact
**new uncredited10/6 qualification item** pending reviewer disposition. Thus
the maximum separable proposal is124/90, not an assertion of acceptance.
Old163/135 remains held. With the new10/6 reserve, provisional adjusted remaining
is4350/2541; if later all134/96 qualify it would be4340/2535. The accepted
remaining4474/2631 is unchanged until reviewer-b awards actual credit.

Runtime REDs and bounded comparisons (Go1.26.8, GOFLAGS empty, GOMAXPROCS2,
race/count1; no deadline changes, skips or retry loop):

- Standing root: initial candidate SQLite settlement wait RED; predecessor
  single cell RED21.992s at the earlier two-handler wait; candidate single
  cell PASS7.189s with every original assertion. PostgreSQL passes. Base/head
  loads14.10 and9.93 were recorded; this does NOT establish matching quiet load,
  the same signature, pressure causality, clean-master inheritance or zero cost.
- Poison root: earlier candidate both-store receipt passes5.86s; after moving
  cleanup ahead of consumer creation, PostgreSQL passes but SQLite waits out the
  original2s status deadline at in_progress. The one accepted-predecessor SQLite
  comparison passes7.423s (root6.39s), so keep the10/6 reserve UNCLASSIFIED.
  Its failure occurs before teardown; no claim that earlier cleanup caused it.
  No production repair is attempted without a causal boundary decision.

Preservation controls and quality:

- Owner/boundary sweep PASS9.383s; seven repository/inventory roots PASS10.323s;
  store source/role/standing/hostile controls PASS35.912s; complete rewrite-pins
  package PASS0.166s. The existing46-port native read matrix passes both stores
  under race; its two roots pass20.31s and10.20s. New native constraint/retry/
  cleanup roots pass18.90s/16.05s/14.03s respectively.
- Finite source/history tests, immutable source-fork and standing histories
  have23 root passes in the combined receipt. That invocation subsequently
  timed out at300s inside the all-recipe root; it is NOT green. The affected
  runtime/conformance recipe partition was then run separately and passes0.961s.
  Unchanged recipe partitions carry prior receipts; no timeout was enlarged.
- The new whole-read-owner inverse and old mechanical-receipt composition
  repair pass together1.385s. The committed schema/clock transforms reproduce
  their exact outputs and reject changed label/query/argument/clock, duplicate
  or missing cases, repeated application and preexisting owner catalogs without
  partial source writes. No blanket history re-pin.
- Two older guard roots remain RED on BOTH candidate and accepted3bb704336c:
  TestFamily145StandingIngressControlWholeOwnerAdditionsKeepEveryEarlierByte
  (test_event_support.go and read_projections.go) and
  TestHistoricalGeneratorsIntersectOwnedCheckoutWithImmutablePredecessorFiles
  (fork-companions, fork-notices, public-a2-endpoints, public-join and
  receiver-public-readback). See predecessor-preservation.jsonl, candidate
  integrated-history.jsonl/historical-model.jsonl. No failing generator consumer
  source or frozen generator manifest was edited; the new read-file inverse restores
  the exact prior file. This is narrow pre-batch attribution, NOT a waiver or
  claim that the older ledger is landing-ready. Reviewer disposition is open.
- Final vet passes with -composites=false (the existing five unkeyed literals
  are not claimed as a plain-vet pass). Pinned Staticcheck0.8.1 U1000 passes
  every changed package scope. New functions all meet the30 complexity ceiling;
  signed-snapshot swarm-complexity evidence accompanies the checkpoint. No
  suppression, guard relaxation, budget or timeout change. git diff --check PASS.

Wall-time split, rounded phase estimates from command timestamps rather than
instrumented CPU measurements (tests overlap editing): about
35min owner/caller/codemod/control edits;13min focused native proof and bounded
comparison work;12min history/guard integration, census, quality and audit.
The batch started09:30:54Z. Exact end and signed SHA are in the issue checkpoint.
Physical rate is measured separately from accepted credit; no accepted-rate
claim is made before review.

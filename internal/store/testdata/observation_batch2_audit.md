# Ratchet Migration Batch 2: Pipeline And Connector Observations

## Boundary And Governing Context

Part of #2542 / #2151, under broad approval5897692464 and ratchet delivery
ruling5999476669. Batch 1 landed as #2572 at83482f4ad. This extraction begins
there and preserves integration888cb4958; it does not cherry-pick the entire
integration or change production execution, mutation admission, transaction
policy, faults, shutdown, deadlines or workload selection.

The bounded working class is raw physical readback in the nine enumerated
pipeline/connector operation families. The symptom was ordinary fixtures acting
as their own SQL/read coordinator. The helper was an entry point, not the audit
boundary: all held named reader consumers and wrapper caller fanout were swept.
The issue framing is broad enough under the approved ratchet decomposition.
Achieved/intended closure is **touched seam canonicalized**, not parent failure
class elimination. All remaining raw construction, mixed observations and
fixture interpretation remain explicitly owned by #2542, not compatible paths
for the new observations.

Binding spec: platform-spec.yaml,
engine.runtime_core_persistence_store_contracts.selected_contracts,
selected_runtime_store_projection.raw_sql_policy. This PR records
pipeline_connector_observation, fan_out_component_observation and
reply_return_observation under the existing private bounded fixture operations.
Closed operation owners retain raw mechanics; public bridges return only
detached facts. Exact inherited physical predicates are not replaced by public
API output as its own oracle.

## Owner And Consumption Audit

The real owners are the original native PostgreSQL/SQLite backend read
coordinators below runtimepersistence, not a database recovered by a local
helper. Existing eventFixtureDialectForTest validates the admitted original
native composition; none of these operations constructs a store or coordinator.
Existing transaction probes prove one original read snapshot and zero writes.

Executable-delivery SQL additionally belongs to the existing closed
backend/delivery/read_projections.go owner: FixtureHandlerSelectionStorageTx
and FixtureFanOutTriggeredIntentStorageTx hold the two fixed joins. The
runtimepersistence operations below are their sole direct consumers, preserving
validation and the original selected read transaction. Detached evidence aliases
do not decode stored display labels, source coordinates, capsule bytes or status.
The closed domain guard is independent of the authority census; passing the
census never permits another executable-delivery SQL owner.

| Family / Canonical Operation | Complete Touched Consumers | Old Path / Systematic Consumption |
| --- | --- | --- |
| Handler selection / ReadHandlerSelectionStorageForTest | assertPersistedHandlerRuleSelectionInFlow and its complete wrapper fanout; TestSelectionRetryAfterRealCASConflictBothStores | moved to owner; exact physical node selection columns and retry-zero row count preserved; old direct selection SQL removed |
| Credential leak / ReadConnectorCredentialLeakStorageForTest | assertSlackManagedConnectorNoStoredSecret; assertTelegramConnectorSupportedSurfaceNoStoredSecret and every call | moved to owner; exact serialized LIKE and result/failure OR retained; old dialect-specific queries removed |
| Activity inventory / ReadActivityAttemptStorageForTest | runtimeConnectorActivityRows/ForSource; all 18 journal/count/status helpers for GitHub issue/comment, Graph, Notion, Slack and Telegram; assertProposedEffectProofCounts (six calls) | moved to owner; journal LoadActivityAttempt remains the complete terminal-record owner; run/tool/source, oldest started_at, NULL source and duplicate counts retained |
| Reply history / ReadReplyReturnStorageForTest | TestA2FieldlessPairedReplyPreservesConstructedExecutionOnBothStores | moved to owner; terminal context replay/equality and settlement remain actual lifecycle assertions; historical/duplicate context/dead-letter SQL removed |
| Published outcomes / ReadFanOutPublishedOutcomeStorageForTest | a2MapFanOutExecution.outputs and all callers | moved to owner; original outcome-to-event join, ordinal order, physical payload, kind and consumer JSON assertions retained |
| Run progress / ReadFanOutRunProgressStorageForTest | a2MapFanOutExecution.assertProgress and all callers | moved to owner; every physical intent plus outcome cardinality share the original snapshot; unchanged independent event-count query remains G's separate event observation tail |
| Trigger capsule / ReadFanOutTriggeredIntentStorageForTest | a2MapFanOutExecution.compositeReceipt and every composite caller | moved to owner; original node/delivery/declaration scope, source coordinates and raw capsule retained; compiled digest, capsule and lineage validation remain consumer assertions |
| Intent completion / ReadFanOutIntentCompletionStorageForTest | a2MapFanOutExecution.compositeOutputs and every composite caller | moved to owner; exact full intent key, all ordinal outcomes and final cursor/cardinality/status retained; actual publication, delivery, schedule and no-duplicate assertions remain |
| Pinned source / ReadPinnedAuthoredMutationStorageForTest | entity-field branch of compositeReceipt | moved to owner with prerequisite pinned reader from4b485e306; exact run/entity/mutation/domain/path and physical bytes retained; no source decoding, repair or fallback |

The public storetest bridges are SQL-free and return copied JSON/row facts.
The selected fixture/context itself still has inherited raw carriers needed by
other unmigrated setup/fault observations. They remain debt, not a relabeled
permission or a closed carrier. Mixed root tests and assertProgress are not
falsely marked wholly SQL-free. The finite completed-helper guards cover the
actually closed bridges, selection/connector helpers, journal filtering and
fan-out output/capsule/completion methods; the source-bound ratchet protects
the entire inherited site corpus independently.

Sibling/source probe: existing native store/read probes, all six connector
families, selection retry, fieldless reply, map/list/composite/empty-map and raw
source refusals, and every held call to these specific ports. Other activity
result/failure, entity/history, fork, setup, source and served observations have
different physical predicates and remain the approved later batches. No port
with a caller-chosen query, table, ordering, callback or reconstructed ownership
was added.

Cross-lane handoffs remain binding: G owns canonical event/status, construction
and location introductions; C owns creation/history/receipt extensions. Those
unlanded introductions are not copied here. The missing fork status/event-name
and mutation writer coordinates were explicitly recorded in6012160678. Refusal
ledger observations remain with their canonical ledger-owner extraction, rather
than restoring independent SQL against the ledger in this observation batch.

## Codemod And Proof

The finite committed pipeline-observations codemod carries 28 complete function
recipes across 11 files, plus this output. Actual candidate overlay type checking
precedes all writes. Unknown/changed bindings or work, ambiguous/missing functions,
invalid recipes/source and unavailable files fail closed; inert layout is allowed.
Repetition produces zero changes. It does not import future transforms or a
generic SQL translator. The pinned reader's consumer travels with the composite
recipe, rather than adding a raw compatibility bridge for a missing prerequisite.

| Manifestation | Status | Exact Execution Proof |
| --- | --- | --- |
| Physical selection columns, raw display text, node/sibling exclusion and failed-CAS zero count | reproduced and fixed | TestHandlerSelectionStoragePreservesExactColumnsAndScopesBothStores; TestHandlerSelectionStorageRejectsUnavailableAndInvalidOwners; durable selection journey and TestSelectionRetryAfterRealCASConflictBothStores |
| Serialized credential leak counts, original scopes and no partial result | reproduced and fixed | both TestConnectorCredentialLeakStorage* roots; Slack and Telegram actual gateway/activity journeys |
| Orphan request, duplicate/tool/source/earliest journal witnesses and complete terminal record | reproduced and fixed | both TestActivityAttemptStorage* roots; TestRuntimeConnectorJournalWitnessesPreserveToolSourceAndEarliestAttemptBothStores; six real connector journeys; both approved-activity roots |
| Historical/duplicate contexts and exact event-linked dead-letter association | reproduced and fixed | both TestReplyReturnStorage* roots; TestA2FieldlessPairedReplyPreservesConstructedExecutionOnBothStores |
| Outcome-run/ordinal/physical payload witness | reproduced and fixed | both TestFanOutPublishedOutcomeStorage* roots; compiled immutable/whitespace map and composite execution roots |
| All intent and outcome history with no partial progress snapshot | reproduced and fixed | both TestFanOutRunProgressStorage* roots; compiled map, composite and raw source refusal roots |
| Trigger/key/source/capsule exact physical witness | reproduced and fixed | TestFanOutCompositeStoragePreservesTriggerKeySourceAndPhysicalCapsuleBothStores; composite lifecycle and both compiled map roots |
| Exact-key completion, origins, missing/cancelled/unavailable evidence | execution-proven through the same corrected path | TestFanOutCompositeStorageRejectsInvalidOwnersOriginsAndPartialEvidenceBothStores; composite lifecycle and map retained/refusal roots |
| Pinned mutation coordinates, physical bytes and rejected authority | reproduced and fixed | both TestPinnedAuthoredMutationStorage* roots; composite lifecycle, immutable source and whitespace-key journeys |

Focused vemew proof: **34 roots /158 passing records /68 required backend cells**
under race, no failures/skips, explicit successful package terminals. The committed
observation_batch2_proofs.json is the exact named execution inventory; backend
cells require actual run/pass evidence, including compound backend/case names.
These are production-shaped component/connector journeys, not live external
provider calls, served API/CLI closure, or final integrated/full qualification.

Additional focused checks: five codemod control roots, candidate no-edit repetition,
original registry, old/new completed-family controls, hostile raw/callback/method
mutants, spec checks and vet. Preparation failures are retained: missing unlanded
constructor/pinned-reader prerequisites were repaired by the existing landed
reopen constructor plus bounded pinned reader; a layout control exposed position-
sensitive formatting and now uses position-independent AST formatting. The first
registry refresh identified79 new legitimate private-runtime-adapter rows; every
row is a bounded native read operation, not an ordinary-site role change.

Corrected tier (reviewer6013292686 and user direction): **CI lifecycle**;
**Local core plus whole store-runtime-full-02, the enumerated native-store and
runtime-owner structural guard roots, and all 34 named roots**. Final repaired-head
qualification is pending. The previous f521 hosted core and original 34-root
focused receipts are historical evidence, not the new head's qualification.
The f521 server2 core was cancelled when approval was withdrawn; all its workers
joined and no interrupted aggregate is credited. No new server2 run starts without
the lead's explicit handover.
Exact committed base/head complexity is mandatory before push. No timing budget,
collector, planner, workload, deadline, skip or correctness assertion is relaxed.

## Closed Delivery Owner Repair (Supersedes Original Guard Claim)

Reviewer6013292686 independently reproduced new violations in
test_handler_selection_storage.go and test_fan_out_storage.go, plus inherited
master diagnostic SQL from #2572. Approval at f521 was withdrawn. These are
same-concept siblings, not a different semantic concept or waived private SQL.
The two new queries now live in the existing closed delivery projection file;
exact query text, joins, argument order, physical columns, ordering, absent-row
behavior and source/capsule bytes are retained. Both operations discard partial
evidence on query, scan, row iteration, close or transaction failure. Public ports
remain unchanged and SQL-free. The production domain guard and its owner map are
unchanged. No compatibility path, renamed query or private-file allowance exists.

Inherited master diagnostic violation is separately fixed by urgent #2575,
d5cece6ac. Before that lands/rebase, the local unchanged domain guard now reports
ONLY that inherited diagnostic, proving the two new joins were relocated, but
this is still a red guard and not qualification. Rebase and final guard success
are required before push. The exact pre-repair red and interrupted core receipts
are retained. A preparation compile failure from removing a still-needed
fanoutobligation import was fixed before qualification and earns no credit.

Focused owner controls retain original both-store transaction probes, exact
node/event/run/declaration sibling exclusion, raw display text, canonical identity
refusal, cancelled/closed/unavailable storage and NULL-source partial-read refusal.
Actual durable selection/CAS and compiled fan-out journeys remain among the
unchanged 34 named final-head proofs; no workload is removed to make guards pass.
The minimal scope is existing-owner repair, not another lifecycle framework.

## Parent And Tracking

Committed debt: **15,183 ->15,065;118 occurrences removed, zero added**. Collector
494fd3b6300c4163241395ef9e3aa59ce58eb32f45e9f5d8bc5a5078401303d5 is unchanged.
All67 excluded-source uncertainties remain; confirmed raw-operation debt is11,181.
Full census includes legitimate private/infrastructure operations; its53,783
findings /38,501 raw-operation sites are not the remaining unauthorized debt.
The final committed-head read-only ratchet must bind to actual extraction83482f4ad.

Parent #2542/#2151 remains OPEN. Remaining delivery estimate: up to three grouped
migration batches (native/setup/workflow; fork/recovery/standing; served/channel/
mailbox) plus final closure, within the seven-PR ceiling; medium packing confidence.
This dependency-adjusted second observation batch absorbs the held pipeline read
groups without creating one-helper PRs or an unnamed tail. Zero debt, uncertainty
disposition, both expanded global guards, original SQLite fork-deadline cut and
final integrated full remain jointly required for parent closure.

Watchlist decision: existing delivery_and_replay_ownership,
runtime_store_backend_default_and_sqlite_portability and shutdown_and_runtime_lifecycle
mapping remains sufficient; no new node/issue/framework. Architecture feedback is
the already-tracked raw-carrier and shared-fixture authority tail, promoted under
#2542 rather than another abstraction. Removing those owners across the remaining
finite corpus is multi-batch work with high ROI (prevents coordinator/semantic
escapes), not a new restoration or compatibility project. G01 collector-transition
acceptance is a separately recorded G-owned guard delta, not silently folded here.

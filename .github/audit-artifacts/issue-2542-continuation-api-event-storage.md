# API Event Count And Nested Storage Evidence

Parent #2542 / #2151. The shared countEventsByName family and all52
predecessor calls migrate to the ORIGINAL selected read coordinator. The complete
26-root caller inventory and30 finite recipes are committed. Count-name keys are
exact storage keys, never trimmed/case-folded or interpreted as runtime routing.
Three nested assertion helpers also lose raw handles rather than growing paired
raw/native parameters: publication refusal, event replay and singleton-agent
replay. Their twelve callers now pass the original pg or fixture.pg.

Owners: native selected read transactions own snapshots; existing closed
delivery/read_projections.go owns exact event_id plus subscriber_type=agent
cardinality; existing eventrecord PostgreSQL/SQLite adapters own the two-column
source_event_id/payload text rendering; bounded API fixture operations aggregate
detached facts. Public storetest bridges accept only original ownership and exact
event/name coordinates. No SQL, dialect, callback, table selector or reconstructed
coordinator escapes. Replay counts, both exact causal source IDs and audit payload
remain physical facts, not complete-event admission or replay authorization.

Scope is preserved: event-name and event.replayed counts include ALL runs,
history and physical rows. Refusal counts include ALL runs and API command
receipts, including expired history. Agent-delivery counts include every status
for the exact event but exclude node recipients. SQL NULL source identity remains
empty text; payload uses native driver SQL text rather than reserialization or
canonical decoder filtering. Each aggregate is one original selected read
transaction and discards all evidence on late failure. Workloads, correctness
assertions, timeouts, source/actor setup and actual execution paths are unchanged.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Shared event-name count and26 execution roots | reproduced and fixed | All named original roots under race; complete independent AST workload equality, exact owner/caller inventory and30 finite whole-function recipes. |
| Refusal has zero runs/events/command receipts | reproduced and fixed | Original assertion helper and its nine call sites retain all three zero assertions; selected read snapshot preserves physical whole-store scope and exact event name. |
| Event and singleton-agent replay physical conservation | reproduced and fixed | Three helper call sites retain exact original/replay agent cardinality, whole-store event.replayed count, both causal IDs, and all original/replay/agent audit-payload assertions. |
| Exact whole-store/name/receipt scope | execution-proven through the same corrected path | TestAPIEventStoragePreservesWholeStoreCountsLineageAndNativeSnapshotBothStores under race: two runs, case/blank/whitespace-distinct names, expired API history, cross-run audit history, native payload text and one original read/no writes. |
| Agent-only delivery scope and NULL lineage | execution-proven through the same corrected path | Same native root includes a real node obligation alongside two agent obligations; exact agent count excludes the node. Original root NULL source IDs remain empty. |
| Raw/cancelled/closed/unavailable and late read | execution-proven through the same corrected path | TestAPIEventStorageRefusesRawCancelledClosedAndUnavailableOwnershipBothStores; final-event lookup failure after counts returns zero aggregate. Schema fault restoration is deferred before assertions and completes before close. |
| Unknown caller/owner, lost assertion or input | execution-proven through the same corrected path | Committed caller inventory, independent assertion/workload oracle, hostile substitution controls, named four-helper raw-authority adversary, actual overlay type check and inert replay. |

Supersession: the subsequent physical-cardinality continuation migrates the
formerly remaining all-run/all-event/API-command scalar helpers and their two
nested refusal consumers. This earlier count/replay proof remains unchanged;
its thirteen overlapping finite consumer recipes are composed forward, not
duplicated. Other count-by-ID, SQLite-name and delivery helper debt stays open.

Systematic consumer audit: all52 old count calls and all twelve nested assertion
calls are moved; helper-to-helper count calls disappear into typed aggregates.
The26 roots below preserve their full source, API operations, failures, replay,
routing and readback. Existing unrelated countAllRunRows/countAPIIdempotencyRows,
countEventDeliveries/countEventDeliveriesForEvent and direct fixture SQL remain
explicit same-parent debt in #2542's decreasing census; they are not compatibility
bridges or new permissions. They consume overlapping physical row families but
are not claimed closed by this leaf-helper migration. No new semantic interpreter
or ownership decision beyond the declared broad migration is introduced.

Closure: touched count/nested-assertion family canonicalized, NOT elimination of
all API storage-authority debt or the parent. The source inspection entry point
was followed through nested helpers and complete caller propagation. Existing
watchlist/tracker nodes remain sufficient; no framework, new tracker or architecture
abstraction is warranted. No PR, core/full tier or final integrated qualification
is claimed. Final zero-debt and global completion remain open.

Binding spec: selected_runtime_store_projection.raw_sql_policy,
api_event_storage_observation. Grouped census/registry, all78 guards, complete
overlay/hostile controls, partition/spec, complexity and definitive unused are
required before push. Receipts:
 /home/youmew/.cache/swarm-2542-local-20261007/increment20-*.

## Complete Execution Consumers
- TestOperatorRuntimeControlHandlersUseIngressOwnerAndIdempotency
- TestOperatorEventReplayPublishesDistinctReplayEventAuditAndIdempotency
- TestOperatorEventReplayStoresIdempotencyBeforeAuditPublishReadiness
- TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError
- TestOperatorEventReplaySubsetAndFailClosedCases
- TestOperatorAgentReplayProjectsSingletonEventReplayOwner
- TestOperatorAgentReplayFailClosedCases
- TestOperatorRunStartHandlersPersistRootEventAndReplayIdempotency
- TestOperatorRunStartHandlersFailClosedBeforePersistence
- TestOperatorRunCompletionSystemNodeFlowConvergesSupportedSurfaces
- TestOperatorEventPublishHandlersPersistEventReportDeliveriesAndReplayIdempotency
- TestOperatorEventPublishRootEventNameWinsOverFlowLeafAliases
- TestOperatorEventPublishHandlersRequireCanonicalBundleHashForCreateNewWork
- TestOperatorEventPublishPostgresUsesPublisherScopeWithPlainRequestContext
- TestOperatorEventPublishReturnsStoredCompletionWithoutPostCommitReadback
- TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate
- TestOperatorEventPublishPostCommitCompletionFailureReplaysWithoutDuplicate
- TestOperatorEventPublishPreCommitFailureFailsClosedWithDeclaredError
- TestOperatorEventPublishIsUnavailableWithoutDurableAckPublisher
- TestOperatorEventPublishExplicitRunTargetRequiresExistingNonterminalRun
- TestOperatorEventPublishOperatorReferenceValidatesSameRunProvenance
- TestOperatorEventPublishOperatorReferenceRejectsInvalidReferenceBeforePersistence
- TestOperatorEventPublishHandlersFailClosedBeforePersistence
- TestOperatorRuntimeContextManagerRoutesCreateNewWorkToSelectedBundle
- TestOperatorEventPublishIdempotencyReplayDoesNotRequireLoadedRuntimeContext
- TestOperatorRuntimeContextManagerRoutesEventReplayByOriginalRunBundle

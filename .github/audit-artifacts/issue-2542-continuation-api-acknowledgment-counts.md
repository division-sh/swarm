# API Agent Delivery And Pipeline Acknowledgment Counts

Parent #2542 / #2151. Two physical counter families migrate completely:
countEventDeliveriesForEvent and countPipelineReceiptsForEvent receive only the
original selected owner and exact event ID. The duplicate context-free
countEventDeliveries is DELETED. Its three callers use the same shared helper
with context.Background(), preserving the old context-free query posture.
Seven execution roots and both helpers are represented by nine transformations,
four new recipes and five composed original snapshots (234 total).

Canonical owners: existing delivery/read_projections.go retains all executable
delivery SQL, through its already-qualified FixtureAgentEventCardinalityTx;
existing pipelinepersistence/owner_operations.go owns the fixed platform/pipeline
receipt count. Original selected native read coordination owns one transaction
per scalar. Detached integers only cross the public bridge. No callback, SQL,
selector, dialect, handle or reconstructed coordinator escapes.

Predicates remain exact: requested event_id plus subscriber_type=agent for
delivery, and requested event_id plus subscriber_type=platform/subscriber_id=pipeline
for acknowledgment. No status/outcome/run/eligibility filter is added. Pipeline
acknowledgment is NOT executable agent/node settlement, as the binding schema
contract states. Context-bearing callers retain their exact original context.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Durable acknowledgment before actual post-commit dispatch | reproduced and fixed | Original durable-ack root under race, channel-gated real dispatch and original zero-before/one-after pipeline receipt assertions. |
| Post-commit receipt/completion failure and exact replay | reproduced and fixed | Two original roots under race; same response, typed failure, no duplicate and exact durable delivery/receipt conservation. |
| Explicit-run followup and paused/gated dispatch | reproduced and fixed | Original followup, paused and gated replay roots under race, complete earlier gates and all existing cardinality/readiness/synchronization assertions. |
| Mock-only rejection does not mutate original deliveries | reproduced and fixed | Original mock-only root under race, unchanged typed refusal and exact before/after conservation. |
| Agent-only native scalar and absent event | execution-proven through the same corrected path | Both-store physical proof persists two agents plus node, exact shared scalar returns2 excluding node and absent0;21 original reads/no writes. |
| Exact pipeline subscriber and outcome-neutral physical history | execution-proven through the same corrected path | New both-store native root uses lawful typed pipeline acknowledgment plus original-coordinator fixed waiting/unrelated-subscriber physical cuts; requested pipeline count1, absent0, zero agent deliveries,3 original reads/no writes. |
| Raw/cancelled/closed ownership, wrong key/context/owner/assertion or old alias | execution-proven through the same corrected path | Extended native refusal controls; independent complete AST/context/workload and hostile controls; exact seven-root caller inventory; named raw-helper and raw/delegating/method/build-excluded retirement adversaries; actual complete overlay/type and inert replay. |

Core consumption: the existing native API census family invokes the exact AST
retirement owner once. A structural control pins that invocation, so typed
delegating aliases cannot evade the core census merely by containing no SQL.

All duplicate/shared counter consumers are accounted for; unrelated direct
event/delivery/metadata/poll/fixture SQL remains explicit parent debt. This is
bounded physical observation, not new delivery eligibility, pipeline settlement,
replay or routing authority. Existing private row-family owners are reused,
without a guard exception or new architecture/framework/tracker.

Closure: two counter families and duplicate/callers, NOT all API reader or parent
elimination. The full relevant execution window and original temporal assertions
remain with callers; shared ownership alone is not proof. Binding spec:
selected_runtime_store_projection.raw_sql_policy.api_acknowledgment_cardinality_observation
and storage event_receipts (pipeline acknowledgment is not executable settlement).
Final zero-debt/strict guards/integrated proof remains OPEN. Before push require
monotonic census/registry,78 guards/adversaries, full finite overlay/hostile controls,
partition/spec, exact-head complexity and definitive unused. No PR, aggregate tier
or server2 use. Receipts:
 /home/youmew/.cache/swarm-2542-local-20261007/increment24-*.

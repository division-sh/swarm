# Fan-Out Serving Authority Registry Accounting

Issue #2394, including the approved publication-group, exact revision-effect and
consistent public-event read amendments. This is a static authority census, not
the manifestation proof audit or a full-suite passing claim.

The unchanged registry guard first found 365 new exact findings and 261 stale
findings after the exact-fact and read-snapshot migrations. No invalid raw
authority disposition was reported. A later canonical delivery-handoff reader
adds its own exact private adapter observations. The final TSV is generated from
compiler-resolved findings and then explicitly classified; it is not a file or
receiver-name exemption. New calls, fields, method signatures or callbacks still
require a new exact registry entry.

## Classified Owners

Paths below are relative to `internal/store/`. Every current changed finding in
each row is separately enumerated with its enclosing function, member/call
ordinal and resolved type in `persistence_authority_findings.tsv`.

| Changed owner files | Disposition | Ownership justification |
| --- | --- | --- |
| `internal/backend/delivery/{adapter,dead_letter_owner,dead_letters,lifecycle,receiver_materialization}.go` | private-backend | Existing claim, settlement, attempt, materialization-dependent and dead-letter transactions now carry the caller-owned exact revision collector. Returned generated IDs and outcome/dead-letter dependencies are contributed before the same outer finalizer. PostgreSQL renewal preserves both mutation counts. The handoff observation remains in the delivery adapter and caller transaction. |
| `internal/backend/delivery/snapshots_batch.go` | private-backend | Bounded complete membership plus canonical snapshot admission, not a public SQL predicate or caller callback. Independent direct membership retains detection of rows hidden by the canonical run/event join. |
| `internal/backend/effectpersistence/completion_settlement.go`, `internal/backend/entityruntime/persistence.go` | private-backend | Existing generated completion/entity IDs and their owning runs are returned by the same mutation for exact fact accounting. No construction or transaction authority moves to runtime. |
| `internal/backend/eventpersistence/{events,sqlite_events}.go`, `internal/backend/eventrecord/postgres/adapter.go` | private-backend | Existing admitted-event writer calls now pass the exact-effects collector; caller admission, duplicate handling and outer transaction remain authoritative. |
| `internal/backend/eventrecord/{postgres,sqlite}/admitted_batch.go` | private-backend | Bounded physical reads consume the same canonical record, strict settlement decoder, integrity reconstruction and inherited-owner validation. Missing or duplicate requested membership still fails. |
| `internal/backend/genericschedule/owner.go` | private-backend | Existing activation insertion and malformed terminalization return their actual changed timer/run coordinates; no new schedule interpreter or broad query capability. |
| `internal/backend/llmpersistence/{postgres,postgres_sessions,sqlite}.go` | private-backend | Completion-memory, stateless audit, session acquisition/rotation/adoption/release and turn mutations contribute exact current and, where applicable, previous ownership. Provider/session admission stays inside existing owners. |
| `internal/backend/pipelinepersistence/{fan_out_barrier_owner,fan_out_owner,flow_instance_routes}.go` | private-backend | Exact fan-out progress/outcomes and canonical barrier folds remain selected-store operations. The per-intent physical join and route SQL bind changes do not grant alternate event or receiver admission. |
| `internal/backend/pipelinepersistence/{owner_operations,publication_group,publication_group_read,publication_settlement_kernel,scenario_setup}.go` | private-backend | Existing pipeline/member disposition and group validation now propagate explicit revision effects. The group read supplies its current read transaction to the canonical delivery handoff observation. Finalization remains the existing named outer operation; no runtime SQL closure is exposed. |
| `internal/backend/pipelinepersistence/{workflow_engine_mutation_commit,workflow_engine_timer_commit,workflow_timer_occurrence_commit}.go` | private-backend | Existing atomic workflow/entity/timer operations contribute exact generated, moved or deleted coordinates, including paired dependencies. No new timer grammar or execution policy. |
| `internal/backend/replycontext/owner.go`, `internal/backend/runforkpersistence/run_fork_delivery_event_replay.go` | private-backend | Existing reply/replay composition passes the collector through the same private transaction and canonical delivery adapter. Historical identity and fork policy remain unchanged. |
| `internal/backend/runforkrevision/{effects,exact_absence,fact_insert,finalizer,latest_facts,projection}.go` | private-backend | One exact/whole-family effect owner, canonical projection, absence validation, bounded fact INSERT and finalizer. Retry reset is a private in-memory callback, not transaction delegation. No arbitrary query predicate or automatic full-scan fallback. |
| `internal/backend/runlifecycle/{run_lifecycle_candidates,run_lifecycle_obligations}.go` | private-backend | Existing completion ownership and obligation reads consume canonical fan-out facts and contribute exact completion effects; no new serving or completion scheduler. |
| `internal/operatorsurface/{delivery_projection,operator_event_batch,operator_observability_read_surface,sqlite_runtime_observability}.go` | private-domain-adapter | Public list/get own a caller-scoped consistent read. Private callbacks and helpers receive that transaction for event, delivery, dead-letter and lineage projection. Only typed DTOs cross the public surface; no SQL/TX method is added to its facade. |
| `internal/startupownership/owner.go` | private-domain-adapter | Retained-session ownership inspection calls the shared private projection within its existing session/read transaction. The runtime receives typed ownership, never a handle or callback. |
| `internal/runtimepersistence/test_backend_support.go`, `storetest/event.go`, `testutil/deliveryfixture/{adapter,schema}.go` | fixture-2151 | Named fixture adapters follow the changed canonical signatures and shared schema helper. Unrevisioned precondition setup remains explicitly test-only; actual history proofs use their outer collector/finalizer. No production caller is authorized by these entries. |

## Retired Entries And Guard Preservation

Stale entries represent replaced effects-less signatures, old per-member/per-event
read calls, old whole-family revision-reader signatures, and SQL calls changed to
return exact generated ownership. Removal is by exact finding key, not by ignoring
a file or suppressing unresolved findings. Unchanged classifications are preserved.

`persistence_authority_registry_test.go` is unchanged. Its compiler-resolved
effective-method checks and hostile alias, promoted-method, callback, context,
transitive-carrier and extra-operation controls remain mandatory. In particular,
an additional query inside an otherwise approved private file does not inherit a
classification. Final execution receipts and precise refreshed counts are recorded
separately; this document does not imply they have passed.

The refreshed delta classifies 368 previously unclassified exact findings:
267 private-backend, 74 private-domain-adapter and 27 fixture-2151. The three
additional findings beyond the initial 365 belong to the canonical delivery
handoff reader and its group consumer. These counts describe this refresh, not
all entries accumulated by the broader PR.

The native startup-grant fixture correction adds five separately enumerated
`typed-process-local` callbacks in
`internal/runtime/startupownership/grant_fault_test_support.go`: four observation
or error-injection hooks and the exact probe-removal closure. The installer
requires the existing concrete, prepared live grant. The hooks cannot supply
grant evidence, replace a native session, or authorize a successful transition;
the removal closure only clears the same installed probe. These five entries do
not exempt the file or classify any future callback automatically. The registry
guard itself remains unchanged.

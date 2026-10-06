# Batch 3 Local Family: SQL-Free API Read And Control Setup

Parent: #2542 / #2151. Local predecessor: `275089d18`. The approved
#2575 CI split is separate; no batch-3 source is pushed or qualified by that PR.

## Owner And Boundary

Six API proof roots unnecessarily received an infrastructure pool only to
reconstruct a selected PostgreSQL store. They now consume the already-landed
`storetest.StartPostgresRuntimeStoreWithReopen` owner directly, discarding its
unused reopen capability. No new constructor, pool getter, raw-handle map,
backend interpreter or G-owned first-landing port is introduced. SQLite setup
and the existing production API read/control/idempotency owners are unchanged.

The existing native fixture constructs from its original sandbox location,
bootstraps through the same `BootstrapPostgresRuntimeStore`, binds the same
payload admission and registers exact selected close with surfaced errors.
API event-bus/work-occurrence cleanup is registered later and therefore joins
before that close. Sandbox release remains the existing infrastructure owner;
the six consumers no longer independently repeat its cleanup registration.

G's pending F01/F02 convenience/location helpers are not required for these
six roots: the original no-argument reopen constructor is already on master.
Optional-location and complete event/storage evidence ports allocated to G
remain with G. This does not duplicate that handoff or alter its first landing.

Binding spec: `engine.runtime_core_persistence_store_contracts.selected_contracts.
selected_runtime_store_projection` capability families
`public_api_read_and_control_capabilities`, `selected_runtime_mutation_unit_of_work`,
and `raw_sql_policy` (particularly the public storetest prohibition and inherited
#2542 debt rule). This is fixture consumption of that contract, not a change
to production architecture, runtime semantics or supported capability availability.

## Complete Consumer And Manifestation Census

| Root | Corrected path and exact preserved proof |
| --- | --- |
| TestSelectedStoreRunReadHandlersExecuteAcrossBackends | Native SQLite/PostgreSQL -> RequireRun -> run.get/list/diagnose RPC success; both original cells retained. |
| TestFanOutReadAPISelectedStores | Native SQLite/PostgreSQL -> RequireRun -> run.fan_out.list; exact run, empty physical result and observed timestamp assertions retained. |
| TestOperatorEntityHandlersServeContractEntityTypesFromPostgres | Native PostgreSQL -> exact source/run/entity construction -> entity.list/get/aggregate; every type, field and state assertion retained. This remains its original PostgreSQL-specific contract. |
| TestOperatorRunControlHandlersTypedResourceErrors | Native PostgreSQL -> canonical controller/bus -> stop/pause/continue missing-run rejection and paused-without-owner refusal; all typed codes retained. |
| TestOperatorRunStopDoesNotReplayCommittedTransitionAfterReconciliationFailure | Native PostgreSQL -> API idempotency and original controller probe -> two calls in both pending/failed reconciliation cells; exact stored diagnostics and one execution retained. No production replay rewrite. |
| TestOperatorRunStartHandlersLeaveSplitControlMethodsUnavailable | Native PostgreSQL -> exact existing source/bus/handler -> all five split control methods refuse with METHOD_UNAVAILABLE. |

Each root is entirely raw-authority-free after the change, not just its opener.
These are the SQL-free setup cohort inside the mapped API family. Adjacent API
context, administrative capacity, stage restart, session and event-publication
proofs still have live physical queries, hostile faults or raw-bearing sibling
helpers. Those require their mapped exact storage/fault owners and remain
explicit #2542 migration work; introducing a new getter solely to relocate their
construction would be an invalid addition, not this family's completion.

## Codemod And Independent Controls

Six frozen `native-api-sql-free-setup` function rewrites are appended to the
existing finite snapshot codemod. Its earlier 47 recipes are byte-for-byte
unchanged. Existing dispatcher, exact snapshot binding and all-or-nothing
type-check-before-write remain; preflight now includes the affected API package.
The pure transformation test proves every non-setup workload/assertion statement
is identical and rejects a live pool use, altered constructor argument, extra
cleanup work or different admission source. No query translation or broad rule
matches new functions. Some AST-emitted layout is compacted; executable content
is independently equal apart from the reviewed three-to-one setup replacement.

The completed-family guard consumes the exhaustive approved debt collector,
not the narrower descriptive registry. It rejects every raw authority finding
in all six roots. Copied onto the actual compiled `275089d18` predecessor, it
fails with all six roots' original pool, reconstruction and constructor findings
(12.013s), not a compiler error. Applying the committed executable to that same
source performs six rewrites across five files, type-checks the complete overlay
and reproduces the candidate files byte-for-byte. Candidate repetition is empty.

## Focused Vemew Evidence

- Six roots / seventeen passing records under race, zero fail/skip, package
  terminal PASS22.377s. Original SQLite/PostgreSQL run-read and fan-out cells,
  pending/failed replay cells and five unavailable-method cells all executed.
- Exhaustive downward census: `15,042 -> 15,018`, twenty-four occurrences
  removed, ZERO added. Confirmed raw-operation debt: `11,162 -> 11,150`.
  Sixty-seven unresolved excluded-source occurrences remain unchanged.
  Collector: `494fd3b6300c4163241395ef9e3aa59ce58eb32f45e9f5d8bc5a5078401303d5`.
- Descriptive registry unchanged: 12,377 resolved findings verified. No source
  role, ordinary-site permission, collector policy or global allowance changed.
- New completed-family guard PASS11.40s; actual predecessor negative FAIL12.013s.
- Existing closed-delivery, revision accessor, exact retirement inventory,
  retirement negative control and release public-process boundary roots PASS.
- Codemod snapshot, complete-statement equivalence, unknown binding/work,
  idempotence and atomic preflight controls PASS.

Receipts: `/home/youmew/.cache/swarm-2542-local-20261003/batch3-native-api-*`.
The focused backend run used vemew's configured canonical PostgreSQL source;
an additionally started B-owned instance was unused and is stopped. No server2,
aggregate tier or repeat-full qualification was consumed. Independent committed
head complexity and final pre-push store checks remain mandatory.

Closure is these six fixture roots only. The grouped native/activity/retained
batch, strict zero-debt completion guards, SQLite fork-deadline qualification
and final integrated full proof are still open. No parent closure is claimed.

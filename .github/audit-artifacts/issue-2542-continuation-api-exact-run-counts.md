# API Exact-Run Counter Ownership

Parent #2542 / #2151. countRunRowsByID and countEventRowsByRunID consume
the original selected native read coordinator, each reading ONLY its respective
fixed row family at exact run_id. The duplicate countSQLiteEventRowsByRunID is
DELETED, not a compatibility delegate. All nine existing consumers migrate to
their original pg, fixture.pg or SQLite owner. No backend selector, getter,
callback, query/table selector or reconstructed store crosses the fixture port.

Original predicates and all assertions remain: exact UUID run matching, all
event rows in that run without type/status/source filtering, absence as zero,
and no implicit dependency on unrelated tables. Both-store physical controls
cover two different existing runs, three-versus-one events and absent runs,
19 original read commits/no writes, cancellation/raw/closed ownership and
unavailable events while the independent exact-run count still succeeds.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Explicit-run followup and private/invalid target conservation | reproduced and fixed | Three original publication roots under race with exact run/event and typed refusal assertions unchanged. |
| Runtime-context loaded/unavailable/mismatched/deactivated source handling | reproduced and fixed | Four original runtime-context roots under race, same bundle/run/routing/refusal/workload and exact event count. |
| Run-start fails closed before persistence | reproduced and fixed | Original full fail-closed root under race; all original gates and no-row assertions retained. |
| SQLite exact selected-run followup | reproduced and fixed | Original SQLite root under race; same two-event selected-run assertion; all scoped SQL/getter logic retired into the shared owner. |
| Physical scope/refusal/table independence | execution-proven through the same corrected path | Extended original both-store native cardinality/refusal roots under race; exact run only, empty absent run, one original selected read per counter and no unrelated row-family gate. |
| Unknown owner/key/assertion/consumer or reintroduced duplicate | execution-proven through the same corrected path | Independent complete AST work/key/binding oracle, hostile substitutions and exhaustive nine-root caller inventory; named raw-helper and raw/delegating retirement adversaries; actual complete overlay/type and inert replay. |

Eleven transformations are represented by five new finite recipes and six
composed older snapshots, never duplicate function ownership. Public bridges
return detached integers. Existing native count ownership is reused; no semantic
validation/replay policy is added. Other delivery/receipt readers and direct
fixture SQL remain explicit #2542 census debt. The earlier SQLite audit's remaining
exact-run reader is now closed; its earlier proof is preserved and composed.

Closure: two shared scoped counters, their duplicate interpreter and complete
callers; NOT all API persistence-authority or parent elimination. Existing issue/
watchlist nodes remain sufficient, with no new framework, issue or design gate.
Binding spec: selected_runtime_store_projection.raw_sql_policy,
api_exact_run_cardinality_observation. Final zero debt, strict completion and
integrated proof remain OPEN. Require monotonic census/registry,78 guards and
adversaries, full finite overlay/hostile controls, partition/spec, exact-head
complexity and definitive unused before push. No PR, aggregate tier or server2.
Receipts: /home/youmew/.cache/swarm-2542-local-20261007/increment23-*.

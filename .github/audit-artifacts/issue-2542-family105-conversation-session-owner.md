# Cohort 105: original live conversation registry

Approved #2542/#2151 original selected construction/coordinator/raw_sql_policy and
session live-authority ownership govern. acquireLiveConversationSession now receives
the caller's existing sessions.Registry. It no longer reconstructs a PostgreSQL
store and bootstraps another coordinator over the caller's database.

Its complete sole consumer, canonical watchdog conversation readback, uses native
PostgreSQL construction and forwards that original registry. Source/run/agent lifecycle
admission, BuildTestInfrastructure ownership scope, identity/lock owner, conversation
and watchdog writes, public turn/list readback and deferred release remain unchanged.
The release is registered before the added ownership assertion so assertion failure
cannot strand the acquired grant. No new semantic owner, raw getter or delegate exists.

The positive conformance proof remains PostgreSQL-scoped. A collector requires exactly
one acknowledged original acquisition write and zero active work. A committed hostile
overlay reinstates the old same-database registry reconstruction; the real proof must
fail specifically at this ownership assertion, not compilation/setup/timeout, and its
deferred original release must not report an error. Existing native selected-lease
currency controls cover the adjacent session-owner behavior on both stores separately.

Two finite recipes/type overlay permit only original-registry forwarding, native
construction and the additional ownership/early-release cut. All original watchdog
and public projection assertions remain. Census decreases without additions.

Value: removes a real duplicate coordinator at live lease acquisition, not merely a
read confinement change. No measured flake-rate claim. Other conformance turn setup
and observation families remain tracked raw debt. No production semantics/spec change,
architecture/framework/compatibility split or new issue; parent closure remains open.

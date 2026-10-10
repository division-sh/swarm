# #2542 continuation: duplicate-publication physical state

Both exact-duplicate bus roots consume the existing original-coordinator semantic
event-fixture evidence operation instead of four raw SELECT/scan pairs per store.
An error-returning storetest bridge delegates directly to that existing operation;
the existing fatal-on-error convenience reader delegates to the same bridge.
This preserves caller/subtest error handling without another SQL owner, parser,
selector, raw getter or compatibility interpreter.

The fixed consumer mapping retains stored runs.event_count (not a recomputed
event total), exact run status, event primary-key presence, all delivery rows and
every settled delivery attempt joined to that event. Delivery IDs are physical
primary keys, so the existing projection map's length is its row cardinality.
The closed delivery owner counts the same event/closure_kind=settled relation as
the former derived-table count, with no current-claim/status/run restriction.
The existing read operation supplies one coherent original native snapshot;
wrong-run/corrupt event or late reader failure yields no successful tuple.

Complete root inverses replace only the observation callback. The actual event,
route, three publish owners, original versus expanded recipients, no-op state
comparisons, held/settled claim, cancellation and repeated terminal duplicate
phase remain unchanged. Actual race proofs are both
TestEventBusExactDuplicateIsOperationNoOp roots. Their preserved nonzero event,
delivery and settled-outcome expectations prohibit always-zero observations.
Whole mapping and hostile counter/key/error controls protect each detached field.

Governing raw_sql_policy, existing fixture/delivery watchlist and broad gate
suffice. No new architecture, framework, SQL classification, runtime/spec policy
or tracker. This closes duplicate-publication observation only; other bus source,
fault, pool/context and construction debt remains #2542/#2151. Exact counts/final
receipts are on the issue. No core/full/server2/hosted/fork-deadline/closure claim.

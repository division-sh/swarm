# Pre-Implementation Coverage Audit Addendum: Failure Discard Ordering

Qualification source: `a73cabe917975637ab655bc682239ec8d3c18859`, based on
`origin/master@276d7723f`. The complete native 114-root matrix passes. The
separate historical non-native CI census is still running and exposes a real
omitted cleanup consumer, not a passing qualification result.

`TestSelectedContractActivationAllowsCausalForkLocalRuntimeLogDiagnostic` fails
on both stores in its existing `missing` case. Its causal-parent rejection,
zero diagnostic publications, pending outbox and failed activation assertions
remain intact. The later owned cleanup fails with `retire flow activation
attempt worker-flow/worker-001: load successor flow activation attempt: sql: no
rows in result set`.

The execution path is preparation -> exact materialization -> selected
agent/route attachment -> real terminal diagnostic -> activation refusal ->
`cleanupSelectedContractExecutionFailure` -> operation-free selected-store
discard -> deferred `completePreparation` / `PreparedSelectedFork.Close` ->
Manager/route retirement -> exact attempt abandonment. Discard has already
deleted the constructed headers and cascaded readiness before the final step
can acknowledge abandonment. The direct execution sink and historical
activation-gate sink both use that ordering.

The observed helper is an entry point, not the audit boundary. The same-class
consumer census comprises all seven failure calls in
`runforkexecution/execution.go` and all four calls in `activation_gate.go`;
both defer preparation completion. Construction, route staging, retained
controls, stop/reset ownership and named store discard remain their existing
canonical owners. The permanent-operation branch retains failed durable work
rather than discarding it and is a different disposition, not permission to
erase active cleanup evidence. Successful activated forks transfer ownership
to retained controls and must not be closed by the failure repair.

Working class remains the approved construction/attachment/retirement class
in #2496, specifically O5 / P36 / D1. Immediate parent #2411 and broader
decomposition #2250 remain open. This does not authorize new historical replay,
freeze changes, missing-row retirement success, detached cleanup, or a generic
continuation/retry registry.

Proposed bounded correction: consume the existing preparation's joined
retirement before operation-free durable discard, keep exact cleanup evidence
until retirement is acknowledged, and propagate cleanup failure while retaining
its owner. No relaxed SQL check or compatibility interpretation. Inventory both
execution sinks rather than patching only the reproduced causal test.

Required proof: unchanged six-cell causal diagnostic root on both stores;
deterministic successful, fail-once and persistent preparation cleanup versus
discard ordering, including no durable deletion when retirement fails; existing
partial-construction/owned-manager/grant/stop controls; both-store header,
readiness, route, timer and foreign-run isolation through final joined cleanup.
Keep the paired typed-point/effect matrices unchanged and rerun qualification
after the correction.

Tracker decision: additive consumer repair in existing #2496/#2525, not a new
issue or parent closure. Watchlist mapping remains
`shutdown_and_runtime_lifecycle` and the existing construction/attachment node;
the discovered sink is an additional live manifestation in that map. Parent
tail estimate is unchanged. Architecture disposition proposed: promote now in
the existing failure cleanup owner, approximately one bounded implementation
and proof pass, high ROI; no new abstraction is needed.

Please record independent confirmation of this bounded ordering repair before
production cleanup changes. That consumer remains unchanged meanwhile; the
independent fixture migrations, fresh describe and qualification continue.
No failure-class closure, final-review request or merge waiver is claimed.

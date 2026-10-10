# Cohort 122: ruled existing-run engine fixture

Binding correction: reviewer-b comment6069320676, implementing prior request
6069166093. platform-spec.yaml event run disposition1384 and immutable
run_model.lifecycle.creation19544 govern. The source helper explicitly created a
scenario_setup run; this witness asserted engine execution, NOT run creation.

Use ExistingRunRootIngressWithRoutingSource. The finite recipe explicitly records
that class correction and removal of the unused parent argument; every other
event fact (ID/type/source/envelope/routing/payload/mode/historical clock) is equal.
Do not hide the changed event class inside generic workload normalization.

Replace raw constructor/unit delivery protocol with the existing native prepared
entity and real publication, original claim/renewal heartbeat, selected current
state and original handler/handoff. The original handled=true, one emission and
custom.emitted assertions are byte-for-byte retained. Stop/join heartbeat even on
assertion failure, before existing bus/runtime/store cleanup.

Both-store proof retains scenario origin before refusal and after execution. The
wrong creating constructor reaches the SAME EnsureRunForAdmittedEvent gate used
by publication and fails with the exact origin conflict before workflow state or
claim mutation; no event is published and physical workflow counts are unchanged.
Original lifecycle/source/read ports are exposed as typed roles on the existing
fixture, not a new owner/SQL delegate. A positive-path old-constructor overlay must
fail on both stores at native publication, without changes to implementation.

Sibling newConstructorHandlerUnitCoordinator callers remain explicit unit/raw
parent debt. Their own origin and creation intent must be audited when migrated;
no blanket conversion of genuine creation witnesses. No production origin policy,
new framework, compatibility exception or parent-closure claim.

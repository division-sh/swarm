# Cohort 127: shared standalone diagnostic-direct fixture writer

Binding: #2542/#2151 original selected writer, no reconstructed backend or fake
transaction protocol; closed runtime-log admission remains binding. The shared
standalone helper loses raw DB/dialect/callback forwarding in all four external
call sites. Preserve diagnostic-direct class, platform.runtime_log, producer,
global/runless lineage, exact JSON bytes, live mode and historical/subsecond clock.

Use the existing named RuntimeLogPersistence/PersistRuntimeLog owner, not generic publication. A
controlled first attempt through generic publication correctly refused the closed
event type on both stores. The first direct admitted carrier also remained outside
the closed admission census, so it was replaced rather than granted an exception.
Existing eventfixture payload binding supplies exact evidence; the canonical
runtime-log owner owns admission, validation and persistence itself. All
current callers used producer runtime; retire that free argument and use the
named operation's fixed producer. Exact native store types and nil-owner refusal
remain, with no backend reconstruction, SQL or new persistence owner.

Both-store controls prove one original write commit, exact canonical record,
zero fabricated runs/deliveries/entities/API receipts and same-ID exact replay.
Existing real API proofs retain grouping, subsecond-before-limit, canonical
failure versus prose, lifecycle fields and malformed-record refusal. Complete
finite caller snapshots preserve payloads/clocks and all public assertions.

The shared run-scoped diagnostic helper still has two raw-bearing consumers in
selected-fork execution and serve fixtures. Their additional receipt/revision
seeds remain explicit parent migration debt; this cohort does not claim complete
diagnostic-class or fake-protocol retirement. The observation harness also still
exposes raw authority for its separate conflicting-receipt control. No production
semantics/spec change, compatibility behavior or measured flake reduction.

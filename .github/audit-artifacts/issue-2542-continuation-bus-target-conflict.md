# Cohort 78: exact target-ownership corruption cut

The remaining target-owner fixture getter and raw write/read helpers are removed.
A typed conflicting DeliveryRoute is passed to the named fault; its existing
identity/target encoding operations run before mutation. Exact event and original
route identity still select the same three-column update. It executes through
the ORIGINAL selected coordinator and the closed delivery owner, not a rebuilt
backend. It remains an intentionally unrevisioned physical fault, as before,
not a production source mutation or history migration. Exactly one affected row
is now enforced before commit, so missing/ambiguous cuts fail closed and rollback.

The original COUNT/MIN read, including both native JSON/text forms and strict
NULL/error behavior, executes in the original read transaction and returns only
detached fields. Failed reads/mutations return no partial facts or row counts.
The closed-owner guard rejected a new sibling SQL file. The unchanged write
and read bodies were relocated into lifecycle.go and read_projections.go,
respectively; no guard inventory, allowance or exception was changed.
The original readback, restarted bus, exact duplicate/preflight contradiction,
and before/after unchanged-row assertions remain identical on both backends.

Whole-function snapshots cover both helpers and their complete caller; all four
dialect SQL bodies are compared verbatim apart from whitespace. Both original
native roots and a new both-store control run under race. The control checks
original write/read counters, exact fields, wrong event/identity, canceled and
closed ownership, invalid route and unchanged physical state after refusals.
There is no query selector, arbitrary column/table operation, raw getter,
reconstruction, raw fixture seed, compatibility layer or timeout increase.

This closes the explicitly retained cohort77 corruption tail within the approved
#2542 named-fault/exact-read boundary. It introduces no production semantics or
spec contract, framework, tracker or watchlist node. Parent final closure and
other bus/component/shared-fixture families remain open.

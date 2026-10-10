# #2542 continuation: event delivery diagnostic observation

## Class, Owners and Consumption

Approved class: raw persistence authority in fixture setup and observations,
under the existing broad #2151/#2542 gate. This cohort closes the complete
catalog event-delivery dump family, not the open parent migration.

Canonical row owner: delivery/read_projections.go ReadEventDeliveryDiagnosticRows.
The existing private served-delivery observation adapter enters one original
selected native read transaction and returns detached six-field rows through
storetest. Public consumers cannot select SQL, tables, predicates or callbacks,
obtain a raw handle, or reconstruct a persistence coordinator.

The exhaustive known consumer set is dumpEventDeliveries and its single original
failure-only call in assertTargetedNodeDelivery. Its primary delivery/receipt and
dead-letter assertions are unchanged. The new native observation controls consume
the same owner; existing attempt, causal-frontier, subscriber-manifest, receiver
and operator readers observe different documented tuples and retain their owners.
No parallel local event-delivery dump SQL or backend interpreter survives.

## Conserved Storage Contract

Both complete original query literals are independently compared with the new
owner: exact event ID only; every subscriber type and status; NULL-to-empty
defaults; physical nested target flow-instance/entity text; ascending created_at
and delivery_id. There is no extra run, eligibility, success, claim-version or
recipient filter. The original PostgreSQL UUID cast and SQLite binding remain.

The diagnostic formatter retains all fields, row order, separators and <none>.
Failures now explicitly say observation_error instead of attempting to identify
the driver's read stage in a SQL-owning consumer. Refusal or late read failure
discards the complete row set; diagnostic text never supplies primary correctness.
Closing the cursor is checked as well as query, scan and iteration errors.

## Execution and Negative Controls

TestCatalogCausalDeliveryDiagnosticsUseExactNativeReadBothStores executes the real
chain-depth catalog on SQLite/PostgreSQL under race. The new control compares the
six facts with independently loaded public delivery evidence, requires exactly
one original read commit, zero writes and no active transaction, and formats that
same actual nonempty route with h.db absent. Missing-event, canceled-context,
closed-store, foreign-owner and missing-owner probes must not grant evidence.

The finite committed codemod migrates the one complete original helper. Exact
whole-body consumer and both-query oracles reject changed owner/context/event,
field substitution, error suppression, row truncation and changed separators.
No timeout, retry, skip, workload or primary assertion was weakened. Existing
closed executable-delivery and the other 77 structural guards remain obligations.

## Tracking and Closure

No new architecture owner, framework, compatibility path, issue or watchlist node
is needed. The existing fixture-authority watchlist remains sufficient. Final
counts, private occurrence classifications and qualification receipts are recorded
in the increment comment. Zero debt, strict completion, SQLite fork-deadline and
integrated qualification remain open under #2542/#2151. No core/full or hosted CI
qualification is claimed for this stacking increment.

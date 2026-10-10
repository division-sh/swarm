# Cohort 85: node-delivery status and failure diagnostics

All seven consumers in the mixed-route and three-level-connect journeys pass
their original selected event store, not its database pool. The success witness
reuses ReadServedDeliveryStatusCount with exact event/node/subscriber/status;
five-second deadline, ten-millisecond poll and exact one-row assertion remain.

The three postmortem projections preserve every PostgreSQL column, COALESCE,
predicate, ordering and output field. Delivery/dead-letter reads live in the
closed delivery read owner; receipt reads live in pipelinepersistence. One
original selected read transaction assembles detached strings and returns no
partial evidence on error. SQLite uses the corresponding native JSON/text form.
The old silent auxiliary-query omission and missing receipt Rows.Err check
are removed: a failed projection fails closed instead of reporting an incomplete
diagnostic as healthy. This changes test-observation error handling, not runtime
execution policy or the healthy formatting/assertions.

The existing two whole-caller recipes are reconciled, not duplicated. Their
oracle permits only original-owner substitution for these seven calls. A new
finite helper snapshot pins timing/count and compares all three original SQL
projections; an AST consumption guard and raw-pool negative cover every caller.
Both real PG journeys execute unchanged under race. A both-store native control
checks ordered delivery rows, real settled pipeline receipt, real dead letter,
original coordinator read count, exact foreign key and no partial evidence
on cancellation, closed or missing authority.

This closes this named observation family, not the callers' remaining raw
setup/count/target helpers. Existing broad approval/raw_sql_policy apply.
No production/spec/framework/tracker change or new exemption is introduced.

# Cohort 90: run-stop exact receipt witnesses

Both foreground-claim and recovery-drain stop journeys reuse cohort 89's
pipelinepersistence receipt owner through the original selected coordinator.
The exact event/platform/pipeline predicate and background read context remain;
only the detached outcome is consumed. The additional reason projection is not
used to classify success or override an error. Absence still fails the assertion.

The finite foreground recipe and the existing complete-event seed recipe pin
every claim-conflict detail, rollback witness, cancellation cut, unrelated-run
progress, required child publication and recovery-before-stop ordering. Actual
SQLite/PostgreSQL journeys run under race; cohort 89's native owner control
separately proves receipt identity, cancellation and closed-owner refusal.

The raw run/control, pending-delivery and revision snapshot helper remains
explicit debt for a later family. This closes only the receipt witnesses. No
new owner, SQL allowance, runtime/spec/framework or architecture change; existing
#2542 approval and selected-store raw_sql_policy remain binding.

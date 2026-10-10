# Cohort 91: shared stop rollback and final-state witness

The two foreground/recovery stop consumers now obtain the shared detached
snapshot through the original selected read transaction. Run/control rows remain
with runlifecycle, executable-delivery status counts remain in the existing closed
delivery read owner, and revision cardinality reuses runforkrevision's existing
classified accessor. No new independent SQL owner or generic selector exists.

The finite whole-helper recipe compares all three original SQL predicates and
every mapped field. It preserves the exact run key, LEFT JOIN/COALESCE control
default, physical pending/processing/retrying statuses and physical revision row
count. There is no eligibility, status normalization, latest selection or history
decoding. Any sub-read error discards the whole snapshot. One original read
transaction replaces the three formerly independent connection reads.

Both-store native controls prove initial and paused state, two target versus
three foreign pending deliveries, revision conservation through pause, one read/no writer transaction,
missing rows, cancellation and closed/missing authority. The unchanged held-claim,
busy rollback, cancelled-stop, unrelated-run and required-publication journeys
run under race on both stores.

This closes the shared stop snapshot family. Complete-event constructor/card/
live-lock and other raw-authority families remain counted debt. No production
runtime/spec behavior, framework, compatibility or architecture split; existing
#2542 approval and selected-store raw_sql_policy govern.

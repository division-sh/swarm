# Cohort 81: native chain-depth constraint fault

The complete-event fixture's corruption attempt now uses a named eventrecord
fault through the exact selected coordinator. The UPDATE, event predicate and
caller depth are unchanged on both stores. Depth is deliberately NOT validated
by the adapter: the real database CHECK remains the assertion's subject.
The unrevisioned fixture transaction preserves this physical fault cut; errors
roll back, and a successful update must identify exactly one row.

The finite codemod compares both original UPDATE literals and exact caller
context/event/owner. Native controls require PostgreSQL SQLSTATE 23514 and the
SQLite CHECK extended code, then read the original depth through the ordinary
prepared-event owner. Transaction probes prove rollback on the original
coordinator, a real successful update and no active lease. Missing/invalid,
cancelled, closed and absent-owner controls cannot change the event.
The unchanged recovery-surface and managed-backlog journeys prove the real
schema rejection still precedes successful complete snapshot dispatch.

No production event rule is changed. This bounded named fault is within the
approved #2542 raw_sql_policy migration, not a compatibility path. Other
complete-event storage/card/live-lock seams remain counted parent debt.

# Cohort 72: original publication pool and replay owner

The two PostgreSQL saturation roots retain their original four-connection cut,
four aligned workers, synchronous/acknowledged forms, all four replay surfaces,
15-second work contexts, five-second alignment and ten-second result deadlines.
Receipt cardinality remains exact event/platform/pipeline across all outcomes.
Decision-route readback remains the exact event row and requires completed.

The native construction recipe now owns the pool and joined close. Four replay
workers consume that original selected store and its obligation owner, rather
than reconstructing four independent coordinators over the same pool. Run seeds
consume the original lifecycle fixture. Existing receipt observation consumes
the original read transaction and pipeline owner. The new exact decision-route
status observation lives under that same pipeline owner and returns only text.

The named PostgreSQL setup operation fixes both open and idle limits at four.
It refuses canceled/closed/missing owners and active dedicated reservations;
it does not repair or resize live reservations. Later retain/release remains
the unchanged production capacity owner. Its control proves 4/0 -> 5/1 -> 4/0
and refusal without resizing at 5/1. This is PostgreSQL-only pool behavior,
not claimed SQLite pool parity. Exact route observation is proved on both stores.

Workers are canceled and joined before selected-store cleanup on every exit.
Start barriers are cancellation-aware, and replay release is idempotent so an
assertion failure cannot strand work behind an unreleased fixture barrier.
Successful paths retain their original alignment and result assertions.

Finite whole-function snapshots and their oracle compare the complete old/new
workloads, allowing only native setup, original-owner propagation, named exact
readback and explicit joined cleanup. No new selector, raw getter, callback,
fake transaction key, reconstruction, waiver, retry or timeout increase exists.

These are routine fixture/observation consumers under the approved #2542 class
and the existing raw_sql_policy. No production semantic/spec change, new issue
or watchlist node is required. Parent zero-debt and integrated closure remain
open, including other bus construction and exact joined storage observations.

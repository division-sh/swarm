# Cohort 82: bounded retry-window and receipt owners

Both competing EventBus instances now consume the original selected store.
Reconstructing a PostgreSQL store from its pool cannot create a competing
coordinator. The competing actor remains a separate bus with independent
interceptors/cursor, so the bounded claim exclusion and later reclamation proof
still exercises real concurrent consumers, not a fake lock protocol.

The held first worker has an assertion-failure-safe release-once and joined
defer. All original signals, claim-window limits, receipt/replay assertions and
five-second normal-path bounds are unchanged. No retries or timeout changes.
The shared receipt helper uses existing CountPipelineEventReceiptStorage and
pipelinepersistence's exact event/platform/pipeline physical COUNT. No outcome,
run, claim or row-limit predicate is added. All existing callers consume this
same corrected helper; no private SQL/read owner is introduced.

Two finite snapshots prove only those substitutions and the joined cleanup.
The original PostgreSQL concurrent-window root and both-store replay-surface
and cancellation-between-batches roots run under race. The entire complete-event
fixture is not declared closed: its raw construction/card/lock/readback tail
remains counted. Existing #2542 approval/raw_sql_policy bind this routine family;
no production semantics/spec, architecture or new tracking decision changes.

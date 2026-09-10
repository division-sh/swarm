# Completed Arrival-Join History (#642)

Binding split: https://github.com/division-sh/swarm/issues/642#issuecomment-5611194341

`fork_retained_join_success_test.go.txt` is the byte-identical original
`internal/serveapp/fork_retained_join_test.go` at `d4e77eadb`. SHA256:
`a24e4b0a28a643c56bda44ae35805588dae1e6cc1a90438efa9e07c15055ba59`.
The active artifact guard enforces that preservation. Both original fixtures,
successful source completion, captured revisions, complete child join/loop
projection, repeated materialization and source-isolation assertions remain.

Only successful fork/child projection is **split / escalated to #642**. This
artifact is not a skipped test, passing refusal proof, or support claim. It remains
RED under current admission. The active `TestServedJoinWriterCompletedHistoryRefusal*`
tests execute the same source journeys and captured-generation assertions, then
successfully shut down and join the real source runtime before taking the
all-application-table baseline. Delivery settlement alone does not join background
completion-candidate writes to `runs.completion_due_at`. With those writers joined,
the tests assert both exact blockers and no database mutation on first and repeated
store-only refusal, on SQLite and PostgreSQL. No table or column is excluded.

To reproduce the original oracle from the repository root without changing
production code or the checkout, use the checked-in overlay:

```sh
go run ./cmd/swarm-test -- \
  -overlay=internal/serveapp/testdata/future_capabilities/fork_retained_join_overlay.json ./internal/serveapp \
  -run '^TestServedJoinWriterForkRetainedGenerations(BothStores|SeparateCheckpointBothStores)$' \
  -count=1 -timeout=5m
```

The expected current failure is exactly `fork materialization requires execution-ready
plan; blockers: timer_history_unproven, flow_route_history_unproven`, after real
source completion. Run broad future matrices through `go run ./cmd/swarm-test`.
This changes neither selected pending scheduler policy nor already-supported
ordinary fan-out, receiver, and generation behavior. No admission stub or runtime
allowlist is supplied with the overlay.

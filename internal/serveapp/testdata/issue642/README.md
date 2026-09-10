# Completed Arrival-Join History: Deferred Success Proof

Binding disposition: [#642 G19 ruling](https://github.com/division-sh/swarm/issues/642#issuecomment-5611194341).

`fork_retained_join_future_success.go.txt` is the byte-exact original
`internal/serveapp/fork_retained_join_test.go` from E commit `0a197d459`
(A checkpoint `d4e77eadb`). SHA-256:
`a24e4b0a28a643c56bda44ae35805588dae1e6cc1a90438efa9e07c15055ba59`.

It preserves both served SQLite/PostgreSQL fixtures, real initial/repeated join
completion, captured generation assertions, future child loop/join projection,
exact members/outputs/timer/outcome evidence, repeat materialization and source
preservation. This artifact expects a capability that is not currently admitted.
It is not a skipped supported test or a production compatibility path.

From the repository root, reproduce the original future-success assertions without
editing the working tree:

```sh
go run ./cmd/swarm-test -- -overlay=internal/serveapp/testdata/issue642/overlay.json ./internal/serveapp -run '^TestServedJoinWriterForkRetainedGenerations(BothStores|SeparateCheckpointBothStores)$' -count=1 -timeout=3m
```

The current expected result of this artifact is failure at materialization with
`timer_history_unproven, flow_route_history_unproven`, after real source join
completion. An artifact failure is not a successful child execution receipt.

The normal compiled tests retain both real source journeys and instead assert the
exact current refusal, no child materialization, and unchanged application facts
on the first and repeated requests. Their names remain unchanged. Future #642 work
must reestablish the archived positive assertions when its own gate approves the
history capability; it must not merely remove the refusal checks.

This ordinary inert-history question is distinct from selected pending timer,
join or fan-out execution. No scheduler, historical classification expansion or
source-freeze exception is authorized here. Supported ordinary fan-out child and
fork-of-fork completion remains in the active test suite.

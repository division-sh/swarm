# Selected Fan-Out Future Success Oracle

Split to #642 under #2433 ruling comment5619777969. The archived full success
oracle remains required; bounded selected fan-out support is now implemented
and tested as described below. This is not complete #642 closure.

`fork_selected_fan_out_success_test.go.txt` is the complete, byte-identical
`run_fork_fan_out_generation_test.go` from integrated baseline `6eefe8afb`.
SHA256: `f64bbfa086c788802ab328623150377eb52e1b73e991b2db90c401eb81efe490`.
`TestSelectedFanOutFutureSuccessArtifactPreserved` enforces that receipt.

From the repository root, run the original future-success assertions without a
production overlay or an admission bypass:

```sh
go test -overlay=internal/store/internal/runtimepersistence/testdata/future_capabilities/fork_selected_fan_out_overlay.json ./internal/store/internal/runtimepersistence -run '^TestForkFanOutGenerationWriterEvaluatorBothStores$' -count=1
```

Both database harnesses must be available. Declaration-level fan-out barrier
refusal has been removed; that obsolete refusal is no longer an expected result.
Every original materialization, generation,
ordinal evaluation, delivery, readback, duplicate and source-isolation assertion
still applies. Do not replace the success oracle with an expected-failure wrapper.
Historical-loop execution and completion of this full overlay oracle remain
outstanding; successful historical materialization does not prove execution.

The active `TestSelectedForkFanOutBarrierMaterializationBothStores` preserves the
original source intent/barrier writer setup and proves exact selected
materialization and idempotent reuse for current, historical and no-loop cuts
on both stores. `TestIssue642FanOutBarrierFrontierBothStores` in
`internal/runtime/pipeline/fork_fan_out_barrier_external_test.go` proves the
bounded real-handler journey through an inherited success/dead-letter fold,
barrier outcome and separate final consumer, with source isolation and joined
retirement. That journey does not prove already-published barrier continuation,
historical-loop execution, barrier crash/restart or the entire archived oracle.
`TestOrdinaryForkFanOutOriginWriterEvaluatorBothStores` and
`TestOrdinaryForkOfForkFanOutOriginWriterEvaluatorBothStores` remain active positive
execution tests. The separate served G19 join artifact is not duplicated here.

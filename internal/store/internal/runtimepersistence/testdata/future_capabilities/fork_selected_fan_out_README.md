# Selected Fan-Out Future Success Oracle

Split to #642 under #2433 ruling comment5619777969. This is not fixed or supported
selected deferred execution. No production capability was added.

`fork_selected_fan_out_success_test.go.txt` is the complete, byte-identical
`run_fork_fan_out_generation_test.go` from integrated baseline `6eefe8afb`.
SHA256: `f64bbfa086c788802ab328623150377eb52e1b73e991b2db90c401eb81efe490`.
`TestSelectedFanOutFutureSuccessArtifactPreserved` enforces that receipt.

From the repository root, run the original future-success assertions without a
production overlay or an admission bypass:

```sh
go test -overlay=internal/store/internal/runtimepersistence/testdata/future_capabilities/fork_selected_fan_out_overlay.json ./internal/store/internal/runtimepersistence -run '^TestForkFanOutGenerationWriterEvaluatorBothStores$' -count=1
```

Both database harnesses must be available. On today's policy this test must fail
at selected preparation with `selected_contract_deferred_work_owner_unavailable`.
Once #642 provides the capability, every original materialization, generation,
ordinal evaluation, delivery, readback, duplicate and source-isolation assertion
still applies. Do not replace the success oracle with an expected-failure wrapper.

The active `TestSelectedForkFanOutDeferredWorkRefusalBothStores` preserves the
complete original source intent/barrier writer setup and proves the exact current
refusal and unchanged application tables on first and repeated preparation.
`TestOrdinaryForkFanOutOriginWriterEvaluatorBothStores` and
`TestOrdinaryForkOfForkFanOutOriginWriterEvaluatorBothStores` remain active positive
execution tests. The separate served G19 join artifact is not duplicated here.

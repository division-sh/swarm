## Hosted RED: classified; fixture repair proposed, no push

@reviewer-e, [run 39148692608 / catalog-required-verify](https://github.com/division-sh/swarm/actions/runs/39148692608/job/117738853229) at `0e78f146d8a0afb141628520c3e7c4f32033c339` is a **branch-owned missed protocol-fixture migration**, not infrastructure, an empty live report, a memo regression, or a rebase interaction. #2729 is paused. No source edit, PR push, hosted rerun or dispatch has been made.

### Cause and bounded comparison

The [shared mock response](https://github.com/division-sh/swarm/blob/0e78f146d8a0afb141628520c3e7c4f32033c339/internal/cliapp/test_command_test.go#L1599) omits `instances` and `attributions`. The branch's [strict decoder](https://github.com/division-sh/swarm/blob/0e78f146d8a0afb141628520c3e7c4f32033c339/internal/cliapp/diagnostics.go#L1074) correctly refuses that response, matching the branch's required RunDiagnosis schema. This is mock HTTP, so neither store reader nor the memo is reached. The production projection initializes both collections as non-null arrays, including the no-instance/no-turn case.

I ran exactly one diagnostic invocation on each pinned source, through swarm-test admission:

`go run ./cmd/swarm-test -- ./internal/cliapp -run '^TestSwarmTestCatalogCompanionsAreProtocolOnly$' -count=1`

| Source | Result |
| --- | --- |
| Candidate `0e78f146d8` | FAIL, package 3.330s; `instances and attributions are required` |
| Master/rebase base `909222b50c` | PASS, package 3.198s |

The whole `test_command_test.go` blob is identical on master, original `55adb583b9`, and current head (`6c9f1b141aaaefe8af32cd78aa633c5daed7f240`). `diagnostics.go` is also identical before/after rebase (`60c15a5f4711dbf6d4feb124013792a69c655511`). The branch introduced the required-field validation in `182a96b193`; its ordinary diagnostic fixtures were migrated, but this scenario helper was missed. The memo commit and rebase did not introduce it.

### Complete affected family

The retained hosted log has eight failing roots, all using that helper:

- `TestSwarmTestGeneratesConfiguredTelegramInputThroughProductionCommand`
- `TestSwarmTestRunsScenarioThroughPublicRPC`
- `TestSwarmTestSetupEntitiesSeedsRootAliasAndExpectationThroughPublicRPC`
- `TestSwarmTestSetupEntitiesSeedsRootRunEntityThroughPublicRPC`
- `TestSwarmTestRunsCatalogSmokeCompanionVisibleBehavior`
- `TestSwarmTestCatalogCompanionsAreProtocolOnly`
- `TestSwarmTestMailboxDecideFindsExactlyOneThenMutates`
- `TestSwarmTestHumanTaskDecideAndDeferUsePublicMailboxRPC`

Two more direct helper consumers, `TestSwarmTestRejectsCrossAnchorDecisionCardSelectorsBeforeLookup` and `TestSwarmTestMailboxDecideMissingVerdictFailsBeforeMailboxLookup`, reject before that mock response is used. Their pre-lookup refusal and no-call assertions remain required. No second handwritten operational-result helper exists in CLI tests beyond the already migrated `validDiagnosticRunDiagnosis` and this stale helper. Other red jobs are fail-fast consequences, not qualification passes.

### Narrow test-first proposal for your disposition

1. Add ONLY explicit empty `instances` and `attributions` arrays to `scenarioRunDiagnoseTestResult`; no fallback/defaulting in the decoder, no production/store/source-memo change. Retain every quiescence count, ready state, protocol-only classification, publish/readback and exact RPC-order assertion.
2. Add missing and null cases for each array inside existing `TestDiagnosticsFailClosedOnAPIAndMalformedResults`, requiring the exact existing required-arrays error/exit. The catalog's existing RED is the positive wire-path witness; its existing non-null empty arrays must be accepted after the fixture change. No new root or guard framework.
3. Run all ten helper-consumer roots, the malformed-result/status/projection family, affected CLI guards and the exact-head preflight. Diff-to-family map: fixture and decoder-negative tests -> `catalog-required-verify`; no production reach or change to hosted selection. CI remains `lifecycle`, Local `focused`, no extra units or duplicate local tier. Race x3 is not needed for a test-data-only diff.

Cost: adds **0 roots**; adds named missing/null wire-boundary negative reaches under the existing malformed-result invariant. Please confirm this bounded fixture-only batch before any repaired push. No timeout, authority, schema, budget, or assertion relaxation is proposed.

### Retained evidence

Full hosted log, failing-case list, both local logs and source identities are frozen in signed LOCAL evidence commit `f664425ab6e60fe17d0577476bca2656643048a2`, ref `refs/backup/agent-d/2735/20261018-catalog-red`, under `test-results/2735-catalog-red-20261018/`. It has not been pushed, honoring the hold. Candidate log SHA256 `690438eba11cfb8ca87f651ab9e9098e5d0c0d283730a56931a2449da622b33f`; master log SHA256 `cedc1f2963dd87e1bfcfeb63f096bca1ccc66cf148af3e749aff8c6764461967`; hosted log SHA256 `f70f12fcf3747830f96c3444db8d3071b5bfb05158baffd55a695c93f2c6d9e1`. This comparison classifies the new RED only; prior REDs are not reclassified or erased.

<!-- receipt v1 -->
receipt: head=0e78f146d8a0afb141628520c3e7c4f32033c339 tree=45b76d13662e5b724fa042425b6a5e2d76709760 parent=56281b14b7fc082a5fa4ef50ceb20513a7a94459 branch=agent-d/2720-stall-attribution signed=G dirty_files=0
preflight: test-results/preflight/20261018T170559.048990747-c573e565-fb02-4d17-ac79-f313bdef5eab/receipt.json sha256=ef9764c756e64758282c8a64f12cc13f2cb85b4bc18cff268d34710040250bd9 (exact-head complete unstarted=0)
tests: 2735 hosted RED diagnosis only: candidate catalog protocol root FAIL; pinned master same root PASS; eight failing roots share missing-array mock; source unchanged; no push or rerun
regenerated: not-claimed
generated: 2026-10-18T17:38:19Z

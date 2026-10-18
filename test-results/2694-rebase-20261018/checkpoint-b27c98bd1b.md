## Q10 delivered-#2695 rebase checkpoint: compile and affected behavior GREEN; four accounting guards remain RED

For reviewer-e under the [accepted reconciliation PLAN](https://github.com/division-sh/swarm/issues/2593#issuecomment-6226726911). Rebased the 59 local Q10 patches onto delivered **`a38d910819775a941d8b2ba63ce044c27547a3af`** (#2695). Local signed head **`b27c98bd1b6c9c02381627efb464ae2c696dd668`**, tree **`ac52fc7dbfbf3d373f85713798dc4aa462c8a998`**. No PR push, dispatch, server2 use or qualification claim. This is not FINAL HEAD.

### Rebase and shared ownership

Compared `d092f96b46..2713155625` with `d092f96b46..a38d910819`: 25 shared paths. Full path manifest retained in `test-results/2694-rebase-20261018/overlap-25-paths.txt`. Actual content conflicts were only `internal/serveapp/budget_stage_recovery_test.go` and `tools/fixture-codemod/pipeline-observations/native_exact_origin_successors_test.go`. Range-diff retains all 59 patches: 56 identical, three composed, no dropped patches. The third changed patch textually merged the fixture-setup guard; its source-order mismatch is disclosed below. The remaining fixture/spec-selector changes merged textually.

The import conflict retained both sides during replay; final compile exposed both imports as unused after later patches, so they were removed. The guard conflict keeps #2695's `embeddedSpecHistoricalPair1801` and Q10's exact current-source/refusal checks, with separate recipe/source variables rather than shadowing one `current` name.

Two signed post-rebase test repairs: `dd37fb88c8` requires a non-nil public lifecycle stage before asserting exact `done` plus the existing run identity in the feed proof; `b27c98bd1b` removes the retired `apiv1.Options.PlatformSpecPath` from two Q10 API fixtures, using #2695's embedded owner without changing auth, unknown-vs-absence, numeric, cursor or refusal assertions. No production repair or compatibility selector was added.

The immutable recipe identities/roles/Before/After/other metadata, excluding only current `Successor`, hash identically before and after rebase: **`78fb39d41d2d71d03842278c36b9da6c70f03267ced2833f4057c15e88b7e095`**. No frozen evidence packet was edited. Current total delta: 324 files, +13,644/-3,434 (net +10,210, including proofs and accounting); size is reported, not treated as a hard cap.

### Compile and affected executions at this head

The retained first compile at `dd37fb88c8` exposed unused imports and the two retired API options. After the bounded composition repair, **`go test ./... -run '^$' -count=1 -p=1 -json` passes all 212 package records, zero executed roots**. This is compile-only, not local tier credit.

One managed focused invocation at `b27c98bd1b` passes all four roots, **30 terminal PASS / zero FAIL or SKIP**, with explicit SQLite **and** PostgreSQL children:

- `TestQ10EntityDataCursorOrderingBothStores`.
- `TestQ10OperatorAndAgentLifecycleReadbackBothStores`, including absent-vs-real `unknown`, authored same-name fields and numeric comparisons.
- `TestBudgetRecoveryLoadsExactRetainedStagesAndStatelessPostureBothStores`, including missing/corrupt refusal children.
- `TestServedRootInputFeedsExecuteAndSettleBothStores`, including connected-child, same-flow and keyed-root-opening-event cases.

### Complete current accounting failure set

The complete `pipeline-observations` package sweep is retained: **200 root PASS, 1,537 terminal PASS, four root FAIL, zero SKIP**. Expected negative-control compiler/child-failure output is not counted as a failure. The actual four roots are:

| Failing root | Concrete composition cause |
| --- | --- |
| `TestEmbeddedSpecSuccessorsPreserveFiniteHistoryAndAllOtherWork` | The restart fixture now has Q10's exact header/data membership join, while #2695's frozen endpoint validator expects its own earlier full candidate. It refuses the changed current endpoint. |
| `TestEmbeddedSpecHistoricalPairPreservesOnlyGenuinelyUnlistedPaths` | Same listed restart endpoint; not a genuinely unlisted source and therefore correctly fails closed. |
| `TestFixtureSetupSuccessors2617PreservePriorHistoryAndExactCurrentOwners` | `testDynamicTopologyStartupPreflightTwoContexts`: expected-current is derived before the embedded-spec reversal, but actual source is compared after it. The two existing owner stages must be composed consistently. |
| `TestReviewedSnapshotCandidateOverlayTypeChecks` | `q10CurrentRecipe` matches the whole immutable Original row. #2695 changes its current Successor, so the restart row misses that exact Q10 endpoint match; predecessor preparation then rejects the lawful live join rather than silently rewriting it. |

The frozen 43-row Q10 endpoint packet and 28-row embedded-spec packet intersect in **exactly one identity**: `internal/serveapp/receiver_restart_test.go / TestReceiverCompositionRestartBothStores / native-served-delivery-quiescence-owner`. This is an actionable accounting-owner composition, not evidence of a new lifecycle semantic defect. I own the follow-up: compose the exact existing reversal/current-endpoint owners, preserving both packets, current-source hostility, required pair/count checks and append-only history. No hash-only refresh, TSV union, broader matching or exception is being used to turn these guards green. No new product ruling is requested.

### Remaining landing work / dependencies

#2695 is delivered; there is **no remaining wait on #2592 or #2698**. Q10's next code work is the finite accounting composition above. Then refresh the affected owner/registry/zero-addition debt evidence through their canonical owners, reconcile the proof ledger and contemporary paired logical-write measurement, and emit the actual landing selection. M25/CAS stay open/unclassified at their unchanged bounds; this checkpoint does not resample them or call them infrastructure.

Flow-instance overlap remains confined to Q10's existing owners: `workflow_instance_store.go` (target/companion validation and persisted projection), `flow_instance_activation_commit.go` (activation/tree/header companion commit), `flow_instance_header.go` (header and historical import), `workflow_instance_read.go` (both-store header/data scans), and `workflowheader/{projection,data_read}.go` (presence/readback). **No delta to `pipelinepersistence/flow_instance_index.go`.** The rebase repairs do not change those native owners or their atomic acknowledgement/unchanged-data companion checks.

Qualification remains **`CI-Tier: lifecycle` + the same exact 15 named units / `Local-Tier: focused`**, not blanket full and not a duplicate local tier. Old receipts keep their original attribution. Fresh affected controls, required both-store/negative children, applicable race proofs and exact-head preflight precede a slot-authorized push; none is claimed complete by a clean compile or the four focused roots.

Retained SHA256s, all under `test-results/2694-rebase-20261018/`:

| Receipt | SHA256 |
| --- | --- |
| `range-diff-a38d910819.txt` | `780489f223da281dc6baccd2194594f85b23cb821c4dbda7e8ffcb19c64877ac` |
| `compile-only-dd37fb88c8.jsonl` (RED) | `1f9679eb76ba248c1724dd421c86e233adae073cc4182fcca429870187f0c064` |
| `compile-only-b27c98bd1b.jsonl` (GREEN) | `4c8b2390c131c79a6c4a98e86c901c51a945ad70e17fae1ec9dbe2cea99c5366` |
| `recipe-owner-controls-b27c98bd1b.jsonl` (four RED roots) | `2d08eb3ef59d409da611d79a3fd228d7cd0483b950e057d25cc09e44e2403a8d` |
| `affected-behavior-b27c98bd1b.jsonl` (GREEN, both stores) | `2690c64189fce08abff94d065d291c3a507c16b93717654ac7cafdd7954cce69` |

Proof cost: zero new roots. No budgets, timeouts, baselines, ceilings or assertions were weakened.

<!-- receipt v1 -->
receipt: head=b27c98bd1b6c9c02381627efb464ae2c696dd668 tree=ac52fc7dbfbf3d373f85713798dc4aa462c8a998 parent=dd37fb88c8886ddaebf0fc158cfc013414cd3bd8 branch=agent-d/2593-current-owner-rebased-20261015 signed=G dirty_files=0
preflight: test-results/preflight/20261018T060147.134611862-93c46568-5c13-4bef-8b91-d45e9d0d061b/receipt.json sha256=380088c2d351b635f58fbdfb33409736675d9a2ade0edbda0dd9004177aa0c38 (STALE(head=74c07d528d) complete unstarted=0)
tests: compile ./... -run=^$:212 packages PASS; affected behavior:4 roots both stores PASS; pipeline-observations:200 PASS/4 FAIL
regenerated: not-claimed
generated: 2026-10-18T08:56:07Z

## RED-to-GREEN: exact setup-only complexity/summary guard migration

**For reviewer-e before any PR push:** signed local **`c28690895eca0927b1e2ab95094a79b8ec13bf19`**, tree **`1b54ec598c04f4247207169aee2bd29928e93e4a`**, parent the published `4077a0e675`. Source is available on non-PR backup `agent-d/backup-2718-complexity-guard-20261018`; [one-file delta](https://github.com/division-sh/swarm/compare/4077a0e675ba33b7c1a8c1ab4496f40fc79d1276...c28690895eca0927b1e2ab95094a79b8ec13bf19). **PR #2739 remains at 4077a0e675; no push or rerun.** Please confirm the bounded migration for the single repaired PR push.

### Classification and exact cause

[Full run 39129206484](https://github.com/division-sh/swarm/actions/runs/39129206484) is retained RED: **85 jobs success, 12 cancelled, three failed**. The real proof failure is [broad-06](https://github.com/division-sh/swarm/actions/runs/39129206484/job/117681930468), `cmd/swarm-complexity.TestWorkflowUnconditionalGateAndExecutableAggregate`; timing is **INCOMPLETE**, not an over-budget finding, and required summary reflects incomplete/failed proof coverage. No qualification or rerun credit is claimed for the cancelled jobs; cache tar warnings are not the cause.

The exact requested local reproduction at `4077a0e675` exits 1 with `complexity must depend only on cheap admission, never proof profile/expensive plan`. **There is no new needs edge:** `complexity.needs` remains exactly `[ci-preflight]`. The stale assertion also requires an empty job `if`; it therefore rejects the exact manual-setup-only exclusion already required by [Gate E 6248763428](https://github.com/division-sh/swarm/issues/2718#issuecomment-6248763428). The later assertion likewise still requires bare `always()` on required summary, which would violate setup isolation by executing the normal summary during setup.

### Bounded repair and preserved assertions

Only `cmd/swarm-complexity/workflow_test.go` changes: **+29/-3**. No workflow, dependency, permission, setup/verifier, planner, budget, timeout or runtime production change.

- Pin complexity to the exact existing expression `github.event_name != 'workflow_dispatch' || inputs.workspace_base_setup != true` and the same exact singleton cheap-admission dependency. Every ordinary event retains the unconditional gate; only the isolated setup dispatch is excluded.
- Pin required summary to `always() && (<that exact exclusion>)`. Its complexity dependency, real executable summary, original **three tiers x six complexity outcomes**, checkout pin, unconditional command and unconditional evidence-upload checks remain intact.
- Nine hostile subcases reject a missing exclusion, profile gate, draft gate, bypassed exclusion, missing/duplicate admission and proof-plan/image/setup dependencies. These are named reach under the existing root, not a new exception list or root.

The earlier focused set omitted the `cmd/swarm-complexity` package; canonical preflight did not substitute for that owner test. The explicit affected set now includes it alongside `cmd/swarm-test`, `cmd/swarm-test-timing` and `internal/testplanning`. No preflight/selection policy was changed to mask the omission.

### Exact-head receipts and family map

The original root is RED at the published predecessor and GREEN after this migration. At signed **c28690895e**, the full affected workflow-contract selection passes **59 roots / 626 terminals, zero failures or skips**. It includes the original executable aggregate matrix, all nine hostile cases and the actual-shell/setup/permissions/cache/refusal owner controls. Command: `GOMAXPROCS=2 go run ./cmd/swarm-test -- ./cmd/swarm-complexity ./cmd/swarm-test ./cmd/swarm-test-timing ./internal/testplanning -run 'CI|Workspace|Workflow|Preflight|Tier|Nightly|Summary|Image|Docker|Permission|Dispatch|Setup|Fork|Proof(Plan|Status|Gate)' -count=1 -p=1 -timeout=10m -json`.

**Preflight: pass (644.7s)**, complete **25/25**, zero unstarted, at this exact clean signed head. Receipt `test-results/preflight/20261018T132810.747145190-817f0b36-ea32-41a8-b6a3-c149613825b4/receipt.json`, SHA256 `d3fdd5b419c803978927636cd451b4d3e4ca859a854016df0be8cdd4ea3881ae`. No duplicate local tier, ceiling increase or hidden unchanged-head retry.

Affected families: **broad-06** owns the changed root; workflow/planner/preflight owner families are covered by the focused set. The candidate is test-only and does not change any runtime proof's behavior, native backend, source pin or image contents. Ordinary hosted selection remains **CI-Tier full / Local-Tier focused**, no CI-Units, because the enclosing PR changes canonical workflow infrastructure. Cost: **adds 0 roots**.

### Normal read-scoped GHCR cold miss is already observed

[SQLite local smoke](https://github.com/division-sh/swarm/actions/runs/39129206484/job/117680714854) **PASSED** on the retained RED run. Its log records `Packages: read`, **cache not found**, successful workflow-token login and native BuildKit `load metadata for` / `FROM` **ghcr.io/division-sh/swarm-workspace-golang@sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437**. There is no native ECR acquisition/fetch record. ECR text in the shell's printed fallback definition is not an executed read. This proves the normal job's authenticated cold-miss path; it does not turn the overall RED run green or replace fresh qualification.

Retained hashes: local RED `e29aeda4cbf7bb1cf02234f21873af7a966428083c51f6ab738d236e98b80689`; affected exact-head log `f5bbe7ceadce451ed3eaacf6a71f29a0552d9fd5647c0c12cb7f97060ddfbe33`; complete smoke log `13f8d716ba76b1433f6ebaacccbf02f4b3598c5dd8dc4511112219bad49f16dd`. All original setup and hosted failures remain attributed. **Awaiting reviewer-e confirmation before the one PR push.** No fourth setup is requested or needed.

<!-- receipt v1 -->
receipt: head=c28690895eca0927b1e2ab95094a79b8ec13bf19 tree=1b54ec598c04f4247207169aee2bd29928e93e4a parent=4077a0e675ba33b7c1a8c1ab4496f40fc79d1276 branch=agent-d/2718-private-workspace-base signed=G dirty_files=0
preflight: test-results/preflight/20261018T132810.747145190-817f0b36-ea32-41a8-b6a3-c149613825b4/receipt.json sha256=d3fdd5b419c803978927636cd451b4d3e4ca859a854016df0be8cdd4ea3881ae (exact-head complete unstarted=0)
tests: cmd/swarm-complexity:TestWorkflowUnconditionalGateAndExecutableAggregate RED at 4077a0e675 to GREEN at c28690895e; affected workflow owners:59 roots/626 passes/zero failures or skips; exact-head preflight pass; PR push held for reviewer-e
regenerated: not-claimed
generated: 2026-10-18T13:39:13Z

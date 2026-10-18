## RED -> GREEN: actual setup-input context, complete workflow-consumer sweep

For reviewer-e before a repaired PR push. Signed local head **`073a1183fedf55cac4c31d0e36c52c53126825a0`**, tree `8a5f6f69df110bd98d67723ae3261b8d444b4ac0`; source retained on non-PR ref `agent-d/backup-2718-admission-20261018`. **Not pushed to #2739, no hosted rerun or setup dispatch.** Selection remains **CI-Tier: full / Local-Tier: focused**, no CI-Units.

### Complete red analysis and bounded repair

[Run 39133302082](https://github.com/division-sh/swarm/actions/runs/39133302082) is retained RED: 83 successful jobs, 14 fail-fast cancellations, seven skipped, three failed. The one substantive failure is [broad-09](https://github.com/division-sh/swarm/actions/runs/39133302082/job/117693764539), `TestCIDraftAdmissionAndReadyTransitions`: the local expression evaluator has no binding for `inputs.workspace_base_setup`. CI timing-budget INCOMPLETE and Required test summary are consequences, not an acquisition failure or an over-budget result. No unchanged-head rerun is proposed.

The original named root reproduces locally with `undefined: inputs`. Following reviewer-e's relayed direction, **the actual workflow, setup exclusion, dependencies, token scopes, mirror selection, cold-miss/fallback behavior and every production command remain byte-identical to `c28690895e`**. The `ci.yml` Git blob is `099f7f50659af776e6b38f099bf0b06ecc31781f` at both heads. The evaluator now binds the existing typed Boolean setup input; its absent/empty/null value has false truthiness and the same loose Boolean equality used in these conditions. Only this known Boolean property gets that absent-value handling; other unbound facts still refuse. This follows [GitHub's context contract](https://docs.github.com/en/actions/reference/workflows-and-actions/contexts#inputs-context) and [documented coercion](https://docs.github.com/en/actions/reference/workflows-and-actions/expressions#operators), not a condition rewrite or a short-circuit bypass.

The existing admission root now evaluates **every actual job condition** for pull_request/push/schedule without input properties, and workflow_dispatch with omitted/false/true input, upstream and fork. Only an explicit upstream setup dispatch can execute the writer. All 17 ordinary jobs, including late publishers/summary, remain excluded in setup mode even when dependencies fail; absent/false keeps ordinary admission unchanged. Existing draft transitions, status-function, mandatory evidence and negative checks remain.

The complete local sweep also exposed **two stale exact-string consumers** hidden by earlier fail-fast cancellation: `TestWorkflowNativeUnionIsRequired` and `TestMandatorySoakWorkflowRequiredExactHeadAndBudgets`. Their expected strings now include the already approved isolated-setup exclusion, consistent with [Gate E](https://github.com/division-sh/swarm/issues/2718#issuecomment-6248763428) and [the accepted complexity counterpart](https://github.com/division-sh/swarm/pull/2739#issuecomment-6250653448). Native cadence/platforms, exact needs and timeouts, soak strategy/budgets, executable aggregation, skipped/failed/canceled refusals and every negative outcome remain checked. Their local RED and repaired GREEN are retained. **Delta from approved `c28690895e`: three test files, +44/-4; zero added test roots; no workflow/planner/policy/runtime/budget changes.**

### Complete local reach at the signed head

The retained executable inventory names **every root and source file run**, not just a package wildcard. Discovery starts from all 21 direct `ci.yml` readers/references and conservatively follows same-package reverse helper consumers (81 files). At this exact head: **253 native roots / 4,132 passing cases, zero failures and zero skips**, across:

| Packages | Reach |
| --- | --- |
| `internal/testtiming` | Admission/ready/setup matrix, native cadence, mandatory soak, late summary, publisher/cache/preflight contracts and all indirect workflow readers |
| `cmd/swarm-complexity`, `cmd/swarm-unused`, `cmd/swarm-test-timing` | Exact unconditional gate/native union, executable aggregates and attempt/compiler/plan/evidence acquisition/refusal consumers |
| `internal/testplanning`, `cmd/swarm-test` | Workspace hit/miss/auth/fork/setup shell controls, input identity, proof freezing/history and actual preflight-status consumption |
| `internal/cliapp`, `internal/testutil`, `internal/testcatalog`, `internal/testchanged`, `internal/store/internal/runtimepersistence` | CI source-root/fixture/Postgres ownership, catalog/selection coverage and conservative shared-helper guard consumers |

The inventory distinguishes three raw-string child-fixture function names from actual top-level roots, plus one Windows-only process-tree test reached by the conservative text closure; it does not claim Windows execution. All native selected roots passed. These are focused workflow consumers, not a duplicate local tier. No hosted run is used to discover the remaining consumers.

**Preflight: pass (407.3s)** at the signed exact head, complete with zero unstarted packages, all 21 selected packages plus vet green. `preflight/local` success published as status `56633882749`; receipt SHA256 `af55281cef3103771f17c64000327cdcd46567f5ad7bee1aee671b96d8e6c090`. Its soft cost target warning is disclosed; no selector or deadline changed. The first local preflight command refused before execution because the new candidate had not yet been uploaded; its refusal is retained. After the required non-PR staging upload, the canonical exact-head invocation passed unchanged. No guard retry-to-green or admission bypass.

**Request:** confirm this one batched changed head before its single repaired PR push under unchanged FULL/focused. Original setup and GHCR verification receipts remain attributed to their original heads; no fourth setup, new unit, local tier or settings change is proposed. #2735's deterministic source-reuse RED is retained separately and remains paused behind this priority repair.

<!-- receipt v1 -->
receipt: head=073a1183fedf55cac4c31d0e36c52c53126825a0 tree=8a5f6f69df110bd98d67723ae3261b8d444b4ac0 parent=6e31fd019bb56ecfd7e0b3ab5e1265f67c6b9dc2 branch=agent-d/2718-private-workspace-base signed=G dirty_files=0
preflight: test-results/preflight/20261018T145058.459755115-da6473e0-4ca5-45ba-9aa0-4eedfac71c2d/receipt.json sha256=af55281cef3103771f17c64000327cdcd46567f5ad7bee1aee671b96d8e6c090 (exact-head complete unstarted=0)
tests: workflow consumers:253 native roots, 4132 cases across 11 packages at 073a1183fe; direct plus helper closure, zero fail/skip; original admission RED retained, two sibling oracle REDs repaired; no PR push or dispatch
regenerated: not-claimed
generated: 2026-10-18T14:57:54Z

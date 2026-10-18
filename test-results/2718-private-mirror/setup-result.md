## Setup attempt: RED / stopped at the authorized boundary

[Draft PR #2739](https://github.com/division-sh/swarm/pull/2739) is open at exactly the approved signed head `e3d847b4d6cbf8f37253886ec741bb70955cdc5b`, tree `3012b33b412d554ef16eecae54e16b0e431977c3`. It remains DRAFT and unchanged. This is the single attempt authorized by [reviewer-e](https://github.com/division-sh/swarm/issues/2718#issuecomment-6249288883) and the orchestrator.

**Setup run [39122994179](https://github.com/division-sh/swarm/actions/runs/39122994179) FAILED.** Event `workflow_dispatch`, exact reviewed SHA; `workspace_base_setup=true` and full reviewed-head input supplied. Its only active job was [Private workspace base setup](https://github.com/division-sh/swarm/actions/runs/39122994179/job/117662247524). All **17 normal jobs were skipped**, including qualification, summaries and publishers. No qualifying run was dispatched.

### Observed facts

- The workflow-token Docker login succeeded. The initial package check returned absent. The zero-layer association-only image was published, then the workflow-token metadata check returned package `swarm-workspace-golang`, ID `16013475`, **visibility `private`**, linked to repository **`division-sh/swarm`**, ID `1167137603`. No settings change or alternate credential was needed. [Package](https://github.com/orgs/division-sh/packages/container/package/swarm-workspace-golang).
- The existing Buildx cross-registry copy completed (`DONE 16.6s`) and reported pushing the fixed source index digest **`sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437`** to `ghcr.io/division-sh/swarm-workspace-golang:1.25-bookworm`. That is copy output, **not** the independent complete graph verification or the final read proof.
- At `2026-10-18T11:23:18.3768212Z`, the subsequent fixed verifier failed: **`workspace mirror HTTP refusal 429; stop for access/policy disposition`**. The shell exited 1. The final `docker pull` was not reached and no successful setup artifact was uploaded.

### Classification and stop

The exact failure is HTTP **429 during final verification**, after association and copy. It is not a demonstrated settings/permission refusal: login, publication and repository metadata read already succeeded. However, the current terminal error does not retain the endpoint host/path, so the record cannot distinguish the GitHub metadata, ECR source/token, or GHCR destination/token/blob request. I do not attribute it to ECR, declare full digest/graph equivalence, or claim normal read-scoped token qualification from this evidence.

**Stopped.** No second dispatch, rerun, manual registry probe/write, source edit, settings change, token-scope refresh, PAT or visibility workaround. Request reviewer-e/orchestrator disposition of this retained failure before any next verification. The private package and the copied tag are left intact; no cleanup mutation is attempted. The PR stays draft. The normal full qualifying slot and real authenticated cold miss are still pending, with `CI-Tier: full` / `Local-Tier: focused` unchanged.

### Retained receipts

- Failure log: `test-results/2718-private-mirror/setup-failure.log`, SHA256 `0aa1c6b41de255621a61058e09b0c200b3a784cc63cfce28102bca2906e2d564` (the linked job contains the original).
- Single dispatch command output: `test-results/2718-private-mirror/setup-dispatch.log`, SHA256 `a5502f660ca062ec227728b0ef0a12603643bdf818b214207dd5f4a929c4f6d4`.
- Terminal blocking watch: `test-results/2718-private-mirror/setup-watch.log`, SHA256 `ad35c2b312cdcbb408f7a8cf248f1a992f71c2d1b1d63534749101993cae4df6`.
- Local checks are unchanged: 58 affected roots / 590 passes, no failures/skips; **Preflight: pass (395.6s)**, 25/25 exact-head phases. They do not supersede this real setup refusal.

<!-- receipt v1 -->
receipt: head=e3d847b4d6cbf8f37253886ec741bb70955cdc5b tree=3012b33b412d554ef16eecae54e16b0e431977c3 parent=71aff24888400f41a44cc3a31d11b3e9d9a4ccbe branch=agent-d/2718-private-workspace-base signed=G dirty_files=0
preflight: test-results/preflight/20261018T110610.174650955-f6a9213c-d1b3-49ce-8cdc-903bc7bb2d83/receipt.json sha256=fc80ad11e6db3918ff984285b2cbc68e882e1e8cc9e96c1c4a46845d37d0fd1b (exact-head complete unstarted=0)
tests: GHCR setup dispatch 39122994179: FAILED once with HTTP429 in final graph verification; package private+linked observed, copied digest reported, final token pull unexecuted; 17 normal jobs skipped; 58 local roots/590 passes and preflight25/25 unchanged
regenerated: not-claimed
generated: 2026-10-18T11:24:56Z

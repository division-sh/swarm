## RED-to-GREEN / setup-source head (not registry qualification)

Implementation of [the approved option-B PLAN](https://github.com/division-sh/swarm/issues/2718#issuecomment-6248731272), within [Gate E](https://github.com/division-sh/swarm/issues/2718#issuecomment-6248763428). The original cold-miss ECR acquisition defect is not claimed resolved until the real private mirror and authenticated cold miss have been verified.

Signed head: `e3d847b4d6cbf8f37253886ec741bb70955cdc5b`; tree: `3012b33b412d554ef16eecae54e16b0e431977c3`. Clean, conflict-free rebase onto master `6832b2a936f81371378eedf5c0d2de208b7daa3e`. Non-PR backup: `agent-d/backup-2718-private-base-final-20261018`.

### Implementation and authority

- One isolated setup job in the existing CI workflow, gated by manual dispatch, the default-off `workspace_base_setup` input, and exact upstream repository. A mandatory 40-hex reviewed-head input must equal the dispatch SHA and checkout HEAD. Only this job has `packages: write`; every normal job now has explicit permissions, with `packages: read` confined to workspace preparation.
- Token-only login uses password-stdin and a temporary Docker configuration outside the checkout/cache, removed by an EXIT trap. No secrets, PAT, public-visibility operation, repository/org settings change, package deletion, or access-policy workaround.
- An association-only, zero-layer scratch image establishes the private package's exact OCI source linkage. It is never consumed as a base. Before publication, existing package metadata must be private and associated with this repository; after association, that relationship must be verified before copying the base. Any permission/linkage/policy failure stops for the user.
- The source index is copied without relabeling, rebuilding or platform filtering. The consumed pin remains `sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437`. The fixed verifier compares raw source/destination manifests recursively, including auxiliary manifests and descriptor metadata, and streams every referenced destination config/layer to verify its size and digest. The setup then performs an authenticated digest pull. Real package metadata, the graph and token-read results remain pending the authorized dispatch.
- `Dockerfile.workspace` defaults to the original ECR-by-digest reference. Only trusted upstream push/schedule/manual or same-repository PR contexts select GHCR. A fork PR requires both base and head repository checks; missing/malformed head identity and fork-repository push/manual builds retain ECR without private login/read. The cache is exact on Dockerfile hash, full selected reference hash, OS and architecture; no cross-source restore.
- Hits load the retained archive and perform no login, pull or build. Misses preserve the existing five attempts and 5/10/20/40-second backoffs, current-attempt terminal-429 classification, pipefail status and no final sleep. Non-rate/authentication failures fail immediately, not through a credential or source escape.

### All normal jobs and post-processing

Setup exclusion is explicit on all 17 normal jobs: `ci-preflight`, `complexity`, `ci-plan`, `static-checks`, `sqlite-local-dev`, `macos-sqlite-possession`, `unused-linux`, `unused-darwin`, `unused-checks`, `proof-unit`, `semantic-smoke`, `timing-budget`, `required-tests`, `ci-tier`, `nightly-proof`, `publish-debt-analysis`, `publish-timing-model`. The existing normal-event conditions remain conjoined with that exclusion; always/post-processing jobs cannot bypass it. Setup has no dependency/output connection to qualification. Its sole result artifact is `workspace-base-setup-<sha>`, with `qualification: false`; it executes no proof tier and emits no required-proof, tier, nightly or master attestation.

### Local evidence

Cost: adds **0 roots**. Extends the existing workspace cache/input/hosted-owner roots with setup isolation, fixed-source association, complete graph verification, trusted/fork selection, token cleanup and actual-shell boundary-negative reach. All existing cache/refusal/retry controls and the nine provisioned Docker proof roots remain.

Focused plus affected CI/workflow/preflight guards at the exact head: **58 roots / 590 terminal passes, zero failures/skips**, across `cmd/swarm-test`, `cmd/swarm-test-timing`, `internal/testplanning`. Includes all four touched workspace roots. Actual workflow Bash is executed with Docker/registry stubs for hit/miss/fork, authentication, five-attempt/current-error, setup source binding, each failing setup stage and cleanup. Python graph/metadata controls use offline HTTP responses. These are not real registry/cold-miss receipts. An earlier local setup-exclusion helper failure is retained; it was repaired without removing the normal-summary isolation assertion.

- Final log: `test-results/2718-private-mirror/final-affected-controls.jsonl`, SHA256 `108c69f240d0730a8954b107b288428a2c13675856b5b86f334d7b17ddefbb4a`.
- Verifier SHA256: `b6b710d06d6290de1d50a169c33ae1c4a3b71891761cf61930dda9d51d55521a`.
- Workflow SHA256: `d5050f6831bb735aca8e3640846b000900cb1a1aef14622f729d23749625204b`.
- **Preflight: pass (395.6s)** at this exact head, all 25 phases passed, no unstarted phases. Receipt: `test-results/preflight/20261018T110610.174650955-f6a9213c-d1b3-49ce-8cdc-903bc7bb2d83/receipt.json`, SHA256 `fc80ad11e6db3918ff984285b2cbc68e882e1e8cc9e96c1c4a46845d37d0fd1b`. No duplicate local tier.
- Diff: 6 files, +1,001/-163 (net +838); no dependency vendoring, production runtime change, alteration of existing business assertions, proof-plan, budget or timeout changes to existing jobs.

### Publication and remaining proof

CI-Tier: full
Local-Tier: focused

No `CI-Units` line. Infrastructure workflow/permission/cache/setup changes reach the full job graph; focused shell tests cannot substitute for actual private-package acquisition and the existing Docker proof jobs. No duplicate local tier.

Request reviewer-e's setup-source review and the orchestrator's slot for draft publication plus the **one-time manual setup dispatch**. No PR push, registry mutation, dispatch, qualifying run or server2 access has occurred. Keep the setup PR draft until the actual private package, repository association, full index/blob equivalence and token-only read are verified. Then request the ordinary full qualifying slot with a real authenticated cold miss; setup itself is never qualification. Stop for user action if settings or access changes are required.

<!-- receipt v1 -->
receipt: head=e3d847b4d6cbf8f37253886ec741bb70955cdc5b tree=3012b33b412d554ef16eecae54e16b0e431977c3 parent=71aff24888400f41a44cc3a31d11b3e9d9a4ccbe branch=agent-d/2718-private-workspace-base signed=G dirty_files=0
preflight: test-results/preflight/20261018T110610.174650955-f6a9213c-d1b3-49ce-8cdc-903bc7bb2d83/receipt.json sha256=fc80ad11e6db3918ff984285b2cbc68e882e1e8cc9e96c1c4a46845d37d0fd1b (exact-head complete unstarted=0)
tests: cmd/swarm-test,cmd/swarm-test-timing,internal/testplanning:CI|Workspace|Workflow|Preflight|Tier|Nightly|Summary|Image|Docker|Permission|Dispatch|Setup|Fork|Proof(Plan|Status|Gate) (58 roots,590 passes,0 failures/skips); exact-head preflight
regenerated: not-claimed
generated: 2026-10-18T11:13:07Z

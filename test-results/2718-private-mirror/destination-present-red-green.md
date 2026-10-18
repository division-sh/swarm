## RED-to-GREEN: completely verified destination before copy

For reviewer-e under [the approved presence-path gate](https://github.com/division-sh/swarm/issues/2718#issuecomment-6249737525). **Source review only; no third dispatch or live registry work.**

Signed candidate **`4077a0e675ba33b7c1a8c1ab4496f40fc79d1276`**, tree **`4ba2ab8b3ae4c0bcae3ad7fc672b57c861bf1e50`**, on reviewed `0a8a7c2c3d`. Non-PR backup `agent-d/backup-2718-destination-present-20261018` is pushed. Master remains `6832b2a936`; draft [#2739](https://github.com/division-sh/swarm/pull/2739) stays at `0a8a7c2c3d` pending acceptance and release.

### Retained RED

Both live failures remain retained. [Attempt one](https://github.com/division-sh/swarm/actions/runs/39122994179/job/117662247524) completed the fixed-index copy, then returned an endpoint-unattributed verification HTTP429. [Attempt two](https://github.com/division-sh/swarm/actions/runs/39125260441/job/117668932557) failed on Buildx's explicit ECR fixed-root manifest GET, before final verification. Private package/linkage was confirmed; complete content verification and final token pull were not. Neither is claimed green or reclassified as settings refusal.

The discriminating offline actual-shell control declares a complete pinned destination and refuses any ECR copy. Against the old unconditional setup at `0a8a7c2c3d`, it failed with **exit 98: `ECR copy attempted despite a complete pinned destination`**. This is a new fixture overlay on the old workflow, not an assertion that that test existed at the published old head. Log `destination-present-red.log`, SHA256 `96142c715f88f5f08a802e4a221bdeb5c3b43913184a641b1b1e6e5f969baef8`. No real request was made.

### Owner-preserving repair

- One new internal CLI mode, `--verify-if-present`, delegates to the **existing** verifier with narrowly allowed absence. It authenticates, validates private/exact-repository linkage, hashes the fixed destination root's actual bytes, and completes the same manifest/subject/config/layer/auxiliary traversal with all digest/size and graph bounds intact. The fetched root is reused, not fetched or folded twice. Only that complete result returns 0.
- Status **3** is returned only for the existing initial-publication package lookup 404 or the authenticated fixed-root read's 404 **before traversal**. Token/descendant/subject/blob 404, 401/403, terminal throttling, transport failure, invalid metadata, corrupt bytes and incomplete content return failure, never absence. Default strict verification remains strict. HTTPS/auth redirect confinement is preserved; redirects to the normalized ECR source host are refused before following.
- The setup shell checks the actual status in `else`. Only 3 selects the existing association construction/check/push, one fixed-source copy and strict verification. A completely verified present destination skips **all** association build/push and ECR work; the authenticated final digest pull is still mandatory. There is no new copy/write retry, source fallback, permission, secret, dispatch input, settings change or deadline increase.
- Each attempt uses a fresh temporary directory containing credentials and the provisional JSON. Any prior final artifact is removed. Before the mandatory pull, the shell requires a nonempty valid result bound to the exact digest, private package/repository identity, current source SHA, root report and `qualification:false`. Only after pull succeeds is that new result published to the distinct artifact path.
- The synchronous EXIT cleanup preserves an existing failure status; if work succeeded but cleanup fails, it propagates the actual cleanup failure and removes the final result. Failed setup cannot expose a stale/partial success artifact, and upload still requires step success. All 17 normal jobs remain excluded and no proof/tier attestation is emitted.

### GREEN reach and local failures retained

**Cost: adds 0 roots.** New reach under the existing setup/verifier invariants, not a new framework, accounting exemption or mirror root.

Actual workflow-shell controls prove clean present and absent paths; no copy/publication on present; exactly one existing construction/copy path on absence; no advancement after every existing login/association/copy/verify/pull refusal; non-3 and cancellation status preservation; token/child/subject/blob-404 and throttling non-fallback; empty/malformed/wrong-head/qualifying output refusal; valid-looking stale-result exclusion; pull-before-artifact ordering; cleanup failure and original-error preservation across both paths. Credential cleanup and token non-disclosure checks remain.

Actual verifier controls exercise both strict and presence modes over the complete graph and every previous corruption/missing-content case. Only package/root 404 yields absence; other HTTP/transport/parser/integrity failures remain terminal. Success reads every object once, including the reused root. Existing 429 retry/exhaustion, safe endpoint attribution, partial-body reset and HTTPS/auth confinement controls remain; ECR redirect controls are added.

The first expanded local run had three **test expectation** failures: empty-input pipeline status and malformed-JSON status differed from hard-coded assumptions on the installed `jq`. Their outputs are retained in `destination-present-focused-wip.jsonl`. Expectations now derive those exact codes from independent native `jq -e .` negative controls; the setup must still return the exact original status and expose no artifact. No production failure was waived and no assertion was relaxed. The corrected intermediate run and the final signed-head run pass.

Final focused plus affected CI/workflow/summary/preflight/selection guards across `cmd/swarm-test`, `cmd/swarm-test-timing`, `internal/testplanning`: **58 roots / 616 terminal passes, zero failures/skips**, at the signed candidate. This is offline HTTP data and actual-shell Docker stubs, not real GHCR success. No duplicate local tier or server2 use.

- Final log SHA256: `b3911e7b0a9f1f4f6d0f60cb3ecab7dc89708f266995da2a894dc2f4c169ff61` (`destination-present-final-controls.jsonl`).
- Workflow SHA256 `a2e285fc1e15281749f25b1bb4e603c18dee829a9ffb59712ac6cf17de153477`; verifier `ca1ac4c06ea3999f736a469f3fde505f2c803dfc3dc859fdabcb077ab61ad1fc`; control file `6439245091f4c04eeec3777a8537b949f4dcb7ccdc15a661daf9c28c186bc7d1`.
- **Preflight: pass (348.3s)** at this signed head: 25/25 phases, zero failures/unstarted. Receipt `test-results/preflight/20261018T122855.163394523-63753e7d-e9d1-4b8d-9370-c59db0704b34/receipt.json`, SHA256 `04f51fa82125c0a49225b1bdd900c815c659fb53f7ecc51dca254b43a32f498c`.
- Repair delta: **3 files, +278/-40 (net +238)**. No vendoring, runtime edit, normal-job/proof-plan/budget change or additional live attempt.
- Cumulative setup PR: **6 files, +1,379/-163 (net +1,216)**.

The [signed evidence-only backup](https://github.com/division-sh/swarm/tree/agent-d/backup-2718-presence-final-evidence-20261018/test-results/2718-private-mirror) retains both setup failures, the discriminating offline RED, both intermediate local results, final affected log and this packet. The [canonical preflight receipt](https://github.com/division-sh/swarm/blob/agent-d/backup-2718-presence-final-evidence-20261018/test-results/preflight/20261018T122855.163394523-63753e7d-e9d1-4b8d-9370-c59db0704b34/receipt.json) is included. This is not a proposed landing head.

### Requested source verdict / execution hold

Please review the signed changed head. **The third dispatch remains blocked** until both source acceptance and the user/LEAD decision plus explicit orchestrator release. No third dispatch, registry probe/mutation, policy workaround, qualifying run or PR-branch push during implementation. Both failed attempts and the existing package/tags remain untouched.

CI-Tier **full**, Local-Tier **focused**, no CI-Units, unchanged under the infrastructure exception. After an authorized setup success, the normal read-scoped-token real cold-miss qualification still needs its separate slot; setup success would not supply that credit.

<!-- receipt v1 -->
receipt: head=4077a0e675ba33b7c1a8c1ab4496f40fc79d1276 tree=4ba2ab8b3ae4c0bcae3ad7fc672b57c861bf1e50 parent=0a8a7c2c3d26a194f65fdf7227cff130b7a8fafd branch=agent-d/2718-private-workspace-base signed=G dirty_files=0
preflight: test-results/preflight/20261018T122855.163394523-63753e7d-e9d1-4b8d-9370-c59db0704b34/receipt.json sha256=04f51fa82125c0a49225b1bdd900c815c659fb53f7ecc51dca254b43a32f498c (exact-head complete unstarted=0)
tests: cmd/swarm-test,cmd/swarm-test-timing,internal/testplanning:58 affected roots/616 passes/0 failures or skips; complete-destination and absence/refusal/cleanup controls; exact-head preflight; no third setup dispatch
regenerated: not-claimed
generated: 2026-10-18T12:35:03Z

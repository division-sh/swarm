## Second setup dispatch: RED / exact ECR endpoint / stopped

The released second dispatch ran **once** as [39125260441](https://github.com/division-sh/swarm/actions/runs/39125260441), at the approved full SHA `0a8a7c2c3d26a194f65fdf7227cff130b7a8fafd`, with `workspace_base_setup=true` and the exact reviewed-head input. Draft [#2739](https://github.com/division-sh/swarm/pull/2739) was updated to that SHA before dispatch; source binding passed. [Reviewer-e's authorization](https://github.com/division-sh/swarm/issues/2718#issuecomment-6249589272).

**FAILED** in [the setup publication job](https://github.com/division-sh/swarm/actions/runs/39125260441/job/117668932557), before final verification. All **17 normal qualification/summary/publishing jobs skipped**. No qualifying tier was dispatched, and there are no setup artifacts.

### Exact failure and classification

At `2026-10-18T11:56:58.7744369Z`, native Buildx reported:

```text
ERROR: httpReadSeeker: failed open: unexpected status from GET request to https://public.ecr.aws/v2/docker/library/golang/manifests/sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437: 429 Too Many Requests
toomanyrequests: Data limit exceeded
```

This is **public ECR source-manifest acquisition throttling during the copy**, not a GHCR verifier failure, authentication refusal or settings problem. The failing canonical host/path is explicit above. One terminal refusal is recorded; there was no workflow-level copy retry. Buildx's internal wire-request attempt count is not recorded, so I do not claim it was exactly one HTTP request.

The new destination-only verifier was **not reached**. Its HTTP429 handling applies to its reads, not to native registry-write/copy operations, as gated. This second failure does not retroactively identify the first attempt's lost endpoint: the first copy completed before a different, unattributed verification-request 429.

### Private linkage and verifier read accounting

The workflow-token login succeeded. Both pre-copy metadata checks returned package `swarm-workspace-golang`, ID **16013475**, visibility **private**, linked to repository **division-sh/swarm**, ID **1167137603**. The association-only tag push succeeded again. No settings, credentials or visibility changes were made.

| Verifier invocation / operation | Canonical requested endpoint | Logical attempts / retries | Result |
| --- | --- | --- | --- |
| `--before-publication` / `package-metadata` | `api.github.com/orgs/division-sh/packages/container/swarm-workspace-golang` | 1 / 0 | Existing private linked package returned |
| `--association-only` / `package-metadata` | `api.github.com/orgs/division-sh/packages/container/swarm-workspace-golang` | 1 / 0 | Private linkage reconfirmed after association push |
| Final verifier / `package-metadata` | Same GitHub package endpoint | 0 / 0 | Not reached |
| Final verifier / `token-exchange` | `ghcr.io/token` (query omitted) | 0 / 0 | Not reached |
| Final verifier / `destination-manifest` | `ghcr.io/v2/division-sh/swarm-workspace-golang/manifests/<digest>` | 0 / 0 | Root/child/auxiliary/subject traversal not reached |
| Final verifier / `destination-blob` | `ghcr.io/v2/division-sh/swarm-workspace-golang/blobs/<digest>` | 0 / 0 | Config/layer/auxiliary blob reads not reached |

The two successful metadata returns and zero verifier HTTP-failure records establish the two first-attempt logical reads above. Successes are not a separate wire trace; redirects/native internal requests are not counted as extra logical attempts. No verifier retry or backoff occurred.

**Final authenticated digest pull: NOT EXECUTED.** Complete independent mirror graph verification remains unproven. Attempt one's completed push is retained corroboration, not a replacement for those checks.

### Stop and retained evidence

**STOPPED.** #2739 stays draft at the reviewed SHA. No third dispatch, unchanged-head rerun, follow-up registry probe, source edit, manual copy, credential expansion, package cleanup, settings change or qualifying run. Further execution requires the **user via LEAD**, as instructed. A decision could consider verification of the already-copied destination without reacquiring ECR, but no such action or altered setup path is implemented or authorized here.

- Raw failure log `test-results/2718-private-mirror/setup-second-failure.log`, SHA256 `7cba81f4929059dbb94dc77c962bc680d95b889651c4d88b1fb45fc5385d818b`.
- Single dispatch output SHA256 `d1e70a1f5fd5ca3f91f2fa15c5446f7e67d23a63087b050a09da132b7d92fc6a`; blocking terminal watch SHA256 `6d8c45728f1a524c176c63f21f26fc0e7fbc511d54edd5910feb545526f1531c`.
- No local proofs repeated: exact-head focused 58 roots / 590 passes and **Preflight: pass (400.7s)** remain the source packet, not setup qualification. CI-Tier full / Local-Tier focused unchanged.

<!-- receipt v1 -->
receipt: head=0a8a7c2c3d26a194f65fdf7227cff130b7a8fafd tree=82de94b1104f3c6806d6ddc91dd8ea804e21698a parent=e3d847b4d6cbf8f37253886ec741bb70955cdc5b branch=agent-d/2718-private-workspace-base signed=G dirty_files=0
preflight: test-results/preflight/20261018T114153.106082230-fb69fa00-e853-4eb9-aa52-c668091e8b83/receipt.json sha256=dfe6bfb56fb61c3984a4692759a26a03efc5c4f130d16747de1706caee32d9c4 (exact-head complete unstarted=0)
tests: setup2 39125260441 FAILED at Buildx GET public.ecr.aws pinned manifest: HTTP429 Data limit exceeded; verifier metadata reads2 attempts1 each/retries0; final verifier/pull not reached;17 normal jobs skipped;STOP no third dispatch
regenerated: not-claimed
generated: 2026-10-18T11:59:36Z

## Third and FINAL GHCR setup: PASS

Executed exactly one dispatch under [reviewer-e's exact-head approval](https://github.com/division-sh/swarm/issues/2718#issuecomment-6250039883), [LEAD's final-attempt authorization](https://github.com/division-sh/swarm/issues/2718#issuecomment-6249756698), and the orchestrator's slot release. [Run 39128682998](https://github.com/division-sh/swarm/actions/runs/39128682998), [setup job 117679139016](https://github.com/division-sh/swarm/actions/runs/39128682998/job/117679139016), completed SUCCESS. Draft [#2739](https://github.com/division-sh/swarm/pull/2739) is exactly `4077a0e675ba33b7c1a8c1ab4496f40fc79d1276`, tree `4ba2ab8b3ae4c0bcae3ad7fc672b57c861bf1e50`, signed G. The reviewed-head input, dispatch GITHUB_SHA and checkout binding match. No source change or repeated dispatch occurred.

### Verified result

The retained report and final Docker pull both name the unchanged PR-recorded digest:

`sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437`

The authenticated verifier hashed the complete reachable destination graph: **11 manifests / 51 distinct blobs**, including the actual 6,531-byte root. Totals are 22,361 manifest bytes and 1,460,725,552 blob bytes, with every descriptor's digest and size checked. Every individual manifest/blob digest and byte count is in the [setup artifact](https://github.com/division-sh/swarm/actions/runs/39128682998/artifacts/12136803868). This is verification against the reviewed immutable digest, not tag existence, a source re-read, or a copy receipt.

Package `swarm-workspace-golang`, ID **16013475**, is **private** and linked to **division-sh/swarm**, repository ID **1167137603**, checked with the workflow token. The report binds the exact source head above and has `qualification: false`.

### Per-read attribution

| Operation / requested endpoint | Logical reads / verifier retries | Result |
| --- | --- | --- |
| Package metadata: `api.github.com/orgs/division-sh/packages/container/swarm-workspace-golang` | 1 / 0 | Exact private package and repository linkage verified. |
| Pull-token exchange: `ghcr.io/token`, fixed repository pull scope | 1 / 0 | Authenticated token obtained; token/query credentials are not published. |
| Fixed root: `ghcr.io/v2/division-sh/swarm-workspace-golang/manifests/<fixed-digest>` | 1 / 0 | Actual root bytes hash to the PR-recorded digest; root reused in traversal. |
| Remaining manifests: same GHCR `/manifests/<digest>` owner | 10 / 0 | All child/auxiliary manifests checked; exact identities/sizes in artifact. |
| Destination content: same GHCR `/blobs/<digest>` owner | 51 / 0 | Every distinct reachable config/layer/auxiliary blob hashed and size-checked. |
| Public ECR source | 0 / 0 | `already-present` skipped the entire source acquisition path. |

These are logical verifier-call counts, derived from the approved traversal, successful graph report and absence of any HTTP-error/retry diagnostics; not a wire-level packet trace. HTTPS blob redirects are not separately logged on success. No association build/push, Buildx copy, absent fallback or second verifier pass ran. The native Docker login and final digest pull each ran once; their internal HTTP counts are not instrumented and are not claimed.

Native pull began at **12:47:36.671Z** and at **12:47:53.075Z** reported the exact digest and `Status: Downloaded newer image for ghcr.io/division-sh/swarm-workspace-golang@<fixed-digest>`. At **12:47:53.079Z** the shell reported `already-present; no qualification or tier attestation`. The setup step exited successfully through synchronous credential cleanup; only afterward did the artifact step publish the fresh report.

### Isolation, receipts and next gate

Only the setup job executed; **all 17 normal jobs were SKIPPED**, including proof, required summary, tier and publisher jobs. No normal qualification, read-only job-token proof, or tier attestation is claimed. This setup job had its approved write-scoped workflow token; normal job-scoped `packages: read` and the real cold-miss build remain for ordinary PR qualification.

Exact-head focused evidence remains **58 roots / 616 passes / zero failures or skips**, and **Preflight: pass (348.3s)**, 25/25 phases. The [reviewed packet](https://github.com/division-sh/swarm/issues/2718#issuecomment-6249987141) preserves the original RED, intermediate local corrections and exact-head receipts. No duplicate local tier was run.

Retained files and SHA256:

| File | SHA256 |
| --- | --- |
| `setup-third-complete.log` | `b7503f93f4f0f834f106cdb8a716e5aacbc85cd051bad15068252015d4f5b545` |
| `setup-third-run.json` | `4218edfd57b482311da589174721685e247c0018a9bb6899a7fac81474fa4b8f` |
| `setup-third-artifact/workspace-base-setup.json` | `e68ae9f5e293c934792e648bba48c6205f503877b74c637e8856bde941669e4e` |
| `setup-third-dispatch.log` | `c212ef1dbc9998f8db9c93602aa56589313aef202095e705df44ff242e244f7a` |
| `setup-third-watch.log` | `de53f86a7d0f2aae51803c251ea0378d1ad83c4d9436f50a251b1b56f3b3f92c` |

Both earlier setup attempts remain FAILED and retained. The final attempt is PASS with no settings/permission workaround. Draft #2739 remains draft and the source head stays frozen. **Awaiting the orchestrator's ordinary qualification release** under unchanged **CI-Tier full / Local-Tier focused, no CI-Units**; no fourth setup or ordinary qualification dispatch was made.

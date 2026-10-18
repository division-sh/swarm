## RED-to-GREEN: bounded GHCR verifier repair

For reviewer-e under [the second-attempt gate](https://github.com/division-sh/swarm/issues/2718#issuecomment-6249373528). Source review only; no second setup dispatch has occurred.

**Signed candidate:** `0a8a7c2c3d26a194f65fdf7227cff130b7a8fafd`, tree `82de94b1104f3c6806d6ddc91dd8ea804e21698a`, on the reviewed first-attempt head `e3d847b4d6`. Non-PR backup `agent-d/backup-2718-verifier-429-20261018` is pushed. Draft [#2739](https://github.com/division-sh/swarm/pull/2739) remains published at `e3d847b4d6` until source acceptance and the orchestrator's release.

### Retained RED and cause

[Setup 39122994179](https://github.com/division-sh/swarm/actions/runs/39122994179/job/117662247524) remains failed: package creation/private association and copy completed; final verification was rate-limited, endpoint unattributed, and the final pull was not reached. The absence of endpoint context cannot be repaired retroactively. I do not classify that original request as ECR or as a settings refusal.

A new offline no-source-read control against the original verifier (source SHA256 `b6b710d06d6290de1d50a169c33ae1c4a3b71891761cf61930dda9d51d55521a`) failed specifically with `AssertionError: verifier re-read the anonymous ECR source: https://public.ecr.aws/token/?service=public.ecr.aws&scope=repository:docker/library/golang:pull`. This was the newly added fixture overlay on the old script, not an existing test represented as present at the old published head. Retained log `verifier-no-ecr-red.log`, SHA256 `4f32a9608d97317284084a4cdbe3cfaf08798d20adec0593844cd90083791050`.

### Authorized repair

- Final verification makes **no ECR request**. Its trust anchor is still the reviewed constant `sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437`, never a tag, response header or digest parsed from copy stdout. The destination root's actual bytes are hashed before parsing. The complete committed descriptor graph is traversed, including auxiliary manifests and subject references; every fetched manifest/config/layer/blob still has exact byte digest and size checks. Private/exact-repository validation and the final native digest pull remain.
- Each failed HTTP read now records operation (`package-metadata`, `token-exchange`, `destination-manifest`, `destination-blob`), canonical requested host/path, status and attempt. Redirected failures record final host separately. No credentials, query, signed redirect URL, headers, response body or raw exception URL are printed. 429 is classified `rate-limit`, 401/403 `access-refusal`.
- Only verifier HTTP 429s receive at most five attempts per logical read, with exact 5/10/20/40-second sleeps and no sleep after the last failure. Other HTTP errors, malformed metadata/JSON, missing content and integrity mismatches fail immediately. Response/error handles close before backoff; a retried blob creates a fresh hash/count instead of retaining its partial prefix. Exhaustion raises the original final HTTP error and the CLI exits nonzero.
- No workflow, permissions, package identity, digest, settings, credential source, copy/write operation, fork/cache behavior, setup exclusion, job deadline or qualification policy changed. The original five-attempt Docker/ECR miss handling remains untouched. Delta from the reviewed head: **2 files, +181/-41 (net +140)**, fixed verifier plus its existing owned controls. No vendoring or generic registry framework.

### GREEN evidence and cost

Cost: adds **0 roots**; new reach under the existing workspace-input/verifier invariant. No historical evidence or negative case removed.

The existing root now proves: destination-only full index/child/auxiliary/subject/blob success; zero ECR reads; changed root/platform/child/subject/config/layer/auxiliary content refusal; absent root/child/subject/layer refusal; size and malformed metadata refusal; exact private/repository identity; HTTPS and authorization redirect confinement. All corruption/missing-object controls assert single reads and no backoff.

For all four read roles it exercises 429-then-success, five-attempt exhaustion with exact sleeps, and non-retried 401/403/404/500. The partial-blob control proves the failed response is closed before backoff and fresh-state retry yields the exact complete digest. Signed-query/userinfo/header/body secrets are absent from the diagnostic JSON; the actual CLI refusal path exits 1 with no stdout or unsanitized exception output.

Focused + affected guards at **the signed candidate**, across `cmd/swarm-test`, `cmd/swarm-test-timing`, `internal/testplanning`: **58 roots / 590 terminal passes, zero failures/skips**. Selection: `CI|Workspace|Workflow|Preflight|Tier|Nightly|Summary|Image|Docker|Permission|Dispatch|Setup|Fork|Proof(Plan|Status|Gate)`. This includes all four touched workspace roots and the CI/summary/preflight/selection contracts; no duplicate local tier. These are offline HTTP responses and actual-shell Docker stubs, not real mirror success.

- Final affected log: `test-results/2718-private-mirror/verifier-final-affected-controls.jsonl`, SHA256 `7e8c5c76138d819d7eb66637ca5d93e675836153041000eb73b07ce501af7a3e`.
- Verifier SHA256: `f999cf27f775fd3cfb35c39ef9c59a1f1b92b439a15d61365066cd0e463d8abb`.
- Owned control file SHA256: `62427b320ddb5f9b5f03bfe0b7c5e32d9d8b8e8140821f0684e2433a72da536b`.
- **Preflight: pass (400.7s)** at the signed candidate: all 25 phases passed, no unstarted phases. Receipt `test-results/preflight/20261018T114153.106082230-fb69fa00-e853-4eb9-aa52-c668091e8b83/receipt.json`, SHA256 `dfe6bfb56fb61c3984a4692759a26a03efc5c4f130d16747de1706caee32d9c4`.
- Cumulative setup PR delta: 6 files, +1,141/-163 (net +978). No new dependency or vendoring.

The [signed evidence-only backup](https://github.com/division-sh/swarm/tree/agent-d/backup-2718-verifier-evidence-20261018/test-results/2718-private-mirror) retains the original failed setup, offline RED, final affected log and this packet, alongside the [exact-head preflight receipt](https://github.com/division-sh/swarm/blob/agent-d/backup-2718-verifier-evidence-20261018/test-results/preflight/20261018T114153.106082230-fb69fa00-e853-4eb9-aa52-c668091e8b83/receipt.json). It is evidence, not the candidate landing head.

### Requested source verdict / held execution

Please review this changed head for the gate's bounded second attempt. On acceptance I will ask the orchestrator for release, publish the accepted repair once and dispatch **exactly one** setup with the full newly reviewed SHA. No second dispatch, registry probe/write, qualification run, policy workaround or server2 access during this repair. The package and existing tags remain intact, and the PR stays draft.

Setup must still prove the real private association, complete destination content graph and final workflow-token pull. It is not normal read-scoped cold-miss qualification. The existing infrastructure selection remains **CI-Tier full / Local-Tier focused, no CI-Units**; ordinary full qualification needs its separate slot after real setup verification.

<!-- receipt v1 -->
receipt: head=0a8a7c2c3d26a194f65fdf7227cff130b7a8fafd tree=82de94b1104f3c6806d6ddc91dd8ea804e21698a parent=e3d847b4d6cbf8f37253886ec741bb70955cdc5b branch=agent-d/2718-private-workspace-base signed=G dirty_files=0
preflight: test-results/preflight/20261018T114153.106082230-fb69fa00-e853-4eb9-aa52-c668091e8b83/receipt.json sha256=dfe6bfb56fb61c3984a4692759a26a03efc5c4f130d16747de1706caee32d9c4 (exact-head complete unstarted=0)
tests: cmd/swarm-test,cmd/swarm-test-timing,internal/testplanning:affected CI/workspace guards (58 roots/590 passes,0 failures/skips); bounded verifier RED-to-GREEN, exact-head preflight; no second setup dispatch
regenerated: not-claimed
generated: 2026-10-18T11:49:20Z

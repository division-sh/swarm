## HTTP 429 classification / bounded verification proposal

For reviewer-e, correcting the generic terminal wording from [the single setup attempt](https://github.com/division-sh/swarm/actions/runs/39122994179/job/117662247524). **429 is a rate limit, not a settings or permission refusal. No user-settings action is justified by this result.**

### Facts and attribution limit

The package **was created** as `ghcr.io/division-sh/swarm-workspace-golang`, ID `16013475`, private, linked to `division-sh/swarm` (repository ID `1167137603`). This was verified by the setup workflow token after publishing the association-only tag, at `11:23:00.663Z`. Its raw metadata is retained in the failed-job log; no PAT or settings workaround was used. The copy then completed in 16.6s and reported pushing the approved `sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437` index at `11:23:17.539Z`.

**The exact HTTP endpoint cannot be recovered from this attempt.** The [caught HTTPError](https://github.com/division-sh/swarm/blob/e3d847b4d6cbf8f37253886ec741bb70955cdc5b/.github/scripts/verify-workspace-base.py#L158) was reduced to its status code; URL, response headers and traceback were discarded. Job annotations contain only the exit-1 message, and no setup artifact was uploaded. The final error at `11:23:18.376Z` is 0.838s after copy completion, which does not uniquely identify a request.

The final verifier's ordered read path is: GitHub package metadata; ECR token; GHCR token; source/destination manifest pairs; destination blobs. Therefore `public.ecr.aws` token/source-manifest re-read is a plausible suspect after the copy's source traffic, but **not an evidenced endpoint classification**. The record does not exclude a GHCR request or the repeated GitHub metadata request. I will not label an inferred ECR manifest URL as the observed failure. The missing endpoint context is a diagnostic defect in my verifier.

### Proposed bounded change; awaiting approval

Change only the fixed verifier and its existing owned offline controls; no workflow permission, setup selection, retry/budget, package, digest or fork behavior change:

1. Log HTTP failures with the sanitized actual endpoint host/path (never credentials, query, signed redirect parameters or token bodies). Classify 429 as rate limiting; reserve access-refusal wording for 401/403. Preserve the original nonzero failure and fail closed for every other error. No additional retry.
2. Remove the verifier's redundant post-copy ECR token/source-manifest requests. Anchor verification to the **already approved immutable source digest constant**, not the copy's printed success or a mutable tag. Fetch the destination index by that digest, hash its actual bytes to the approved value, then recursively fetch every referenced manifest, auxiliary manifest, config and layer and verify its actual bytes against each descriptor digest and size. Continue checking the real private package/repository identity and authenticated final pull.
3. The integrity argument is content-addressed: the approved root hash commits to the exact platform/annotation/child-descriptor bytes; each checked child hash and size commits to its next descriptors and blobs. Dropped/added/changed platforms or auxiliaries change the root or a checked child digest. This retains full graph evidence without anonymous source re-reads after copying. A copy log, manifest HEAD or successful native-platform pull alone is never accepted as equivalence.
4. Keep the existing corruption/missing-node/size/authentication/linkage controls and add reach under the same roots proving that verification performs **zero ECR reads** and that rate-limit errors retain a safe endpoint without secrets. No new test root, relaxed assertion or generic registry framework. Run focused controls, affected guards and exact-head preflight before any authorized push; CI-Tier full / Local-Tier focused remain unchanged.

This is a **proposal**, not an implemented change or an assertion that the original 429 was ECR. It needs reviewer-e's approval because it replaces the explicit second source-byte read with verification of the same trusted digest commitment. After approval and local evidence, request the orchestrator's release for at most **one** further setup dispatch at the newly reviewed full SHA. A failure stops with the actual endpoint recorded. The ordinary read-scoped, authenticated cold-miss qualifying run is a separate later slot, not supplied by setup.

PR [#2739](https://github.com/division-sh/swarm/pull/2739) stays DRAFT at `e3d847b4d6`; no source edit, new registry probe, second dispatch, rerun, settings change, credential expansion or package mutation has occurred during this classification.

<!-- receipt v1 -->
receipt: head=e3d847b4d6cbf8f37253886ec741bb70955cdc5b tree=3012b33b412d554ef16eecae54e16b0e431977c3 parent=71aff24888400f41a44cc3a31d11b3e9d9a4ccbe branch=agent-d/2718-private-workspace-base signed=G dirty_files=0
preflight: test-results/preflight/20261018T110610.174650955-f6a9213c-d1b3-49ce-8cdc-903bc7bb2d83/receipt.json sha256=fc80ad11e6db3918ff984285b2cbc68e882e1e8cc9e96c1c4a46845d37d0fd1b (exact-head complete unstarted=0)
tests: read-only setup39122994179 classification: HTTP429 rate-limit; exact endpoint lost by HTTPError handler, no traceback/artifact; private linked package and completed copy observed; no new tests, probes or dispatches
regenerated: not-claimed
generated: 2026-10-18T11:30:17Z

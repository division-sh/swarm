## BLOCKING / bounded diagnostic complete: unmatched arms; smoke remains unresolved

For reviewer-e under [the exact diagnostic gate](https://github.com/division-sh/swarm/pull/2735#issuecomment-6248358947). **Two invocations consumed, no replacement:** base `d092f96b466a9b7564587e6a6caf0478580dfb59`, then candidate `80b43c1a238ad360363c6217b2a269094a333258`, N=1 each, only `TestWorkspaceMCPCompiledDockerConformance/disconnect=false`. **Both fail before the intended real-model response cut because my diagnostic wire projection is incompatible with the canonical request. This pair cannot attribute the original hosted RED.** No third invocation, qualifying rerun, production repair or timeout proposal follows.

### Venue and fixed preparation

Exclusive server2 (`hetzner1`), Go **go1.26.8 linux/amd64**, `GOMAXPROCS=2`, actual test-process affinity **0,1**, identical non-arm environment hash `7e4cfff31cd7e1452bbd8d74f58186f197d77f1cb7f65b05cc4edf70ceadbbf9`. The real CLI metadata also records runtime GOMAXPROCS **2**; its inherited GOMAXPROCS variable is empty because the unchanged golden environment removes it. No claim that an absent variable was set in that child.

Both arms use the same existing image **`sha256:0c09f37b09828ac8460ef68fccb8aea5891a9cbcdc1c73c91f6b1e13da8073b1`**, never retagged or rebuilt. It differs from the earlier quiet-pair image; no equivalence to that earlier image is asserted. Both source Dockerfiles are byte-identical. Three native products were precompiled per arm before either invocation; the actual CLI, observer and test-helper executable hashes/build information were also recorded before the public command launches. The normalized environment and source/overlay hashes were checked before each arm.

| Arm / source | Start UTC / 1m load | Root / leaf wall | Retained outcome |
| --- | --- | --- | --- |
| Base `d092f96b46` | 13:45:54.223 / 1.20 | 27.025s / 17.66s | CLI exit 3; expected `work.completed` missing; final agent delivery terminal/dead_letter. |
| Candidate `80b43c1a23` | 13:46:23.423 / 1.17 | 27.891s / 18.42s | Same pre-target refusal/missing-event class; final agent delivery terminal/dead_letter. |

Exact command per arm: `taskset -c 0,1 go test ./internal/releasee2e -run '^TestWorkspaceMCPCompiledDockerConformance$/^disconnect=false$' -count=1 -timeout=4m -v`, with frozen diagnostic-only Go overlays. Original **10s / 25ms**, one-minute public-command fuse, business/cardinality/refusal and cleanup assertions remain. Adds **0 roots**; no local tier or race-qualification claim.

One preparation compile refusal occurred **before any diagnostic invocation**: the new stdlib `runtime` import collided with the existing application runtime import in cliapp. It was aliased, re-frozen and rebuilt; the initial compile log and old/final overlay manifests are retained. It was not a failed root replaced by another draw.

### Why the intended experiment is invalid

The canonical [worker request](https://github.com/division-sh/swarm/blob/80b43c1a23/internal/runtime/workspace/worker/protocol.go#L48) embeds `*pythonmodule.Request`, whose [Input is `[]byte`](https://github.com/division-sh/swarm/blob/80b43c1a23/internal/runtime/pythonmodule/runtime.go#L51). JSON encodes it as a base64 string. My proxy-only observation added a nested **struct-valued** `Module.Input` to inspect `tool_results`. That projection rejects the string in the proxy's existing `json.Unmarshal` refusal branch, before executing the real model command. This is a diagnostic-overlay defect, not a branch production change. The failing raw decode error was not logged; no invented error text or captured raw request is claimed.

The retained observations corroborate the pre-target boundary: each arm has three original probe records and **zero model/read/emit rows**, with no successful-response/release receipt. Passive traces record `mock_worker_outcome_uncertain`, completion disposition `current` with **OriginSettled=false**, then the exact worker claim's acknowledged **terminal** receipt. These are failure settlement acknowledgements, not successful completion/emit/normal-delivery acknowledgements.

Both quiescence waits return Ready before their original recorded deadlines; the later **event expectation** fails because no `work.completed` was produced. There is no quiescence-deadline failure here, hence no failure-branch stack capture. Neither the 7.09s response schedule nor the intended successful provider/tool/settlement chain was reached. No stale-negative/late-ack, production-regression, inheritance or infrastructure conclusion is supported.

### Retained capture and limits

The overlay used bounded in-memory stdlib execution traces (64 MiB cap), flushing outside observed command spans; actual trace sizes are **4,250,043 / 4,714,655 bytes**, overflow=false. Recorded budget creation/deadline, public diagnose call/return, quiescence interval/counters, report/snapshot/source-processing and exact worker-claim/completion/turn/receipt spans remain available, but describe this refusal path only. Native SQL phase breakdown was unavailable: **no new transaction hooks, readers, getters, options or observer SQL were added**, and no whole-call elapsed time is called lock wait. The diagnostic proxy projection is not retained as a production repair.

Final observations, the last snapshot and timing file were copied by **current pathname after the existing observer join**, before TempDir cleanup. Their own timestamps and separate post-join copy/mtime receipts remain; the periodic snapshot is not claimed atomic with a deadline. Monotonic trace clocks are process-local; explicit wall-nanosecond stamps share the same server2 clock, without claiming nanosecond cross-process accuracy.

Frozen complete overlay-file manifest SHA256: **`0b7379cc2c6764a5512082107821aa38c08764f180f2889a86876bcac052f3fa`**. Base/candidate overlay-map hashes are `b5af0197897057959a8d92975c16da58f9fce6d73e0f731d24a2db614547e6c9` / `89116678fc8b4189ff84bcd8d5b893b85c7e1a459b11333a71d3e5db691f6ad2`; runner hash `2b14defca99b613fd8adaca808063ed745c298981262ea14af50707ea038640e`.

Base/candidate raw trace SHA256: `1d764692e5b67b85738733814c2408448567d1425edc6c95c4cfe9ad269ddabe` / `038c1c91f49581b0ab48a617b05b0557825a59b13866bba3a5c32ab1d655db41`. Root logs: `1e80fa6fbe67d4c5e0607f53d992e289de8fbaa3169ef362483b06b937d3f890` / `dcd54bcd428d7e8ff3eb31d6a3ae3e364a8f5c560ad398d570c2ccdc535cc788`. Actual executable hashes and all final files/metadata are in the retained packet.

**server2 released.** My two remote worktrees were restored, owned runtime temp directories removed, no owned runtime/test process remained, and the original image tag plus B's running container were untouched. No further server2 access after release. The tracked local API-repair tree stays clean at `80b43c1a23`; no #2735 push or diagnostic overlay lands.

**Disposition returned:** LIMITED/unmatched evidence; original smoke hold stays open. Per the gate, sampling stops and the unresolved hosted-RED qualification decision returns to **USER/LEAD**, not an automatic replacement run or another speculative campaign. The accepted API repair remains separately accepted.

<!-- receipt v1 -->
receipt: head=80b43c1a238ad360363c6217b2a269094a333258 tree=188419a252573ae08969bbe79f4091967c0e155b parent=1057402d41387757b5cb2e6a29a1746fc9e10389 branch=agent-d/2720-stall-attribution signed=G dirty_files=0
preflight: test-results/preflight/20261018T072054.393634949-a3cf13e2-2e55-4f14-a5e1-82be6269517d/receipt.json sha256=252e0bd2e479fa6d1d8060ef022a9290a3e69729f40c81202660f3f4274d39fd (STALE(head=1057402d41) complete unstarted=0)
tests: 2735 approved slow-transport diagnostic:N=1 base d092f96b46 plus N=1 candidate80b43c1a23, both failed before target due diagnostic wire projection; no replacement; current-path files after join retained; server2 released
regenerated: not-claimed
generated: 2026-10-18T13:54:36Z

## Corrected diagnostic: base PASS, candidate deadline RED; read-path regression evidence

For reviewer-e under [6250793010](https://github.com/division-sh/swarm/pull/2735#issuecomment-6250793010) and [the orchestrator's explicit operational authorization](https://github.com/division-sh/swarm/pull/2735#issuecomment-6250806608), retaining every bound of [the original gate](https://github.com/division-sh/swarm/pull/2735#issuecomment-6248358947). **Exactly one newly authorized pair, N=1 each; no replacement or hosted run.** The [earlier unmatched arms](https://github.com/division-sh/swarm/pull/2735#issuecomment-6250762335) remain failed, separately retained and invalid for causal inference.

### Execution and instrument precondition

**Before accessing server2**, the corrected observation passed the existing `TestCompiledWorkspaceWorkerIdentityEntry/2735_canonical_byte_input_projection` reach. It serializes the real `worker.Request` with its `pythonmodule.Request.Input []byte` using `json.Marshal`, as the [canonical sender](https://github.com/division-sh/swarm/blob/80b43c1a23/internal/runtime/workspace/worker_execution.go#L109) does. Initial and continuation inputs project correctly, both frame and decoded input remain byte-identical, the retired object projection rejects that canonical frame, and malformed observation cannot be marked schedulable. Those encoder/type owners are unchanged between the two pins. The actual proxy now uses the canonical request type, decodes byte input only for observation, and never changes forwarded request/response bytes; a projection error records an unmatched observation rather than refusing a real response. These are overlay-only changes, not a landing repair or new API.

Both native products and complete overlays were frozen/hashed before the arms; actual CLI/observer/helper hashes were recorded again before public invocation. Source pins: **base `d092f96b466a9b7564587e6a6caf0478580dfb59`**, **candidate `80b43c1a238ad360363c6217b2a269094a333258`**. Same existing image `sha256:0c09f37b09828ac8460ef68fccb8aea5891a9cbcdc1c73c91f6b1e13da8073b1`, Go **go1.26.8**, test affinity **0,1**, root GOMAXPROCS **2** and actual CLI runtime GOMAXPROCS **2**. The unchanged golden environment strips that variable in the CLI; the runtime value is measured, not inferred. Non-arm environment hashes match (`350432751db22939a0d93fdaa0096b63577d530e0d4648407edb1b6aee96bc11`).

| Arm | Server2 start UTC / 1m load | Exact-root outcome |
| --- | --- | --- |
| Base | 14:10:56.447 / 0.53 | PASS, 25.843s |
| Candidate | 14:11:24.462 / 0.64 | FAIL, 25.957s; step-1 quiescence deadline: active deliveries **1**, pipeline/timers/session leases **0** |

Only `TestWorkspaceMCPCompiledDockerConformance/disconnect=false` ran, `-count=1 -timeout=4m`, with original **10s / 25ms**, one-minute public-command fuse and all original assertions. Both arms performed **two real model calls, one real read, one accepted emit**. Initial model transport completed in 1.709s / 1.681s; release was held to the unchanged 7.09s invocation-start schedule, forwarding at **7.09142s / 7.09314s**. Both receipts are matched, uncanceled and projection-error-free. No calibration, stress ladder, new root, native transaction reader or qualifying tier.

### Discriminating timeline

All deadline/acknowledgement/diagnose values below are from each actual CLI process. Raw nanosecond stamps are retained; displayed comparisons are rounded, not claims of cross-process nanosecond accuracy.

| Boundary | Base (14:11 UTC) | Candidate (14:11 UTC) |
| --- | --- | --- |
| Actual 10s budget created | 13.545665 | 41.542806 |
| Actual deadline D | **23.545665** | **51.542806** |
| Emit publication + feedback acknowledged | D -60.5ms | D +98.4ms |
| Exact agent delivery's acknowledged processed receipt | **23.522701, D -23.0ms** | **51.693186, D +150.4ms** |
| Surrounding native receipt call, not a SQL-phase/lock measurement | 4.518ms | 3.423ms |
| First quiescence Ready / deadline failure | Ready at 23.536370 | Failure at **51.711512**, D +168.7ms |

The candidate's last negative quiescence sample was **51.423863**, before D; its source artifact was decoded at **51.424540**, but synchronous `LoadWorkflowContractBundleFromArtifact` finished only at **51.709370**. Snapshot callback ended **51.710983**, public RPC returned **51.711501**, then the original deadline branch rejected the older `active_deliveries=1` result. That one contract-load tail is **284.830ms**, spanning the deadline and the processed acknowledgement. The receipt had committed before RPC return, but **not before D**. Therefore this pair does **not** satisfy the proposed acknowledged-before-deadline/stale-negative-only signature.

The exact candidate [read owner](https://github.com/division-sh/swarm/blob/80b43c1a23/internal/store/internal/operatorsurface/run_operational_evidence.go#L24) reloads the retained source contract for each coherent report. Passive spans record **34 contract loads totaling 9,166.018ms elapsed**, versus **zero** on base. This is synchronous elapsed work, **not measured CPU time or SQL lock wait**. It establishes the added read/snapshot tail and a candidate-only deadline failure under the matched slow-response schedule. It does not independently prove how much of the acknowledgement delay is caused by CPU scheduling, pool/native contention or normal single-draw variation. The final receipt call itself is short and completes before the read snapshot closes; a blanket claim that snapshot possession blocks the entire commit would be unsupported.

Both completion chains use disposition `current`, OriginSettled=false, return normally through continuation/OnEvent/turn finish, then acknowledge the exact processed worker claim. No drained-origin mismatch or recorder-held-native-call deadlock is observed. No statement that all recorder/scheduler effects are exonerated follows from N=1.

### Disposition, capture and next boundary

**Observed user-facing consequence:** under the authorized delayed-response schedule, candidate `swarm test` exits **3** with a deadline/active-delivery report even though the read/emit path and exact delivery settlement complete before that diagnostic call returns. Base succeeds just inside the same bound. This is **branch-regression evidence on the production public-read/command path**, not a lost-delivery/rollback leak, an infrastructure waiver or a timeout-increase argument. The added repeated contract loading and post-sample report tail are directly attributed; the causal resource edge behind the late acknowledgement remains limited as above. I reported that split to the orchestrator before proposing any repair. **Please dispose that product/read-path finding; no production edit, automatic next campaign or qualifying rerun is assumed.** The original smoke hold and separately accepted API repair remain.

Raw Go traces are **6,144,149 / 6,629,620 bytes**, below the bounded buffer, overflow=false. The original candidate deadline branch took exactly one bounded 32KiB stack capture, after recording its failure; no progress wait or cancellation suppression. Final observer files were copied by **current pathname after the existing join** with separate mtime/copy receipts. Their periodic snapshots at 23.322 / 51.363 still precede the later exact acknowledgements; their `in_progress` rows are not evidence of a continuing leak. No added observer SQL, native getters, options or transactiontest calls; missing native phase breakdown stays disclosed.

**server2 released.** Own runtime temp directories were removed, both own remote checkouts restored, no own test/runtime process remained, and the existing image tag/B container/other deployments were unchanged. No server2 access after release. Tracked source remains clean at `80b43c1a23`; diagnostic overlays do not land.

Frozen overlay manifest SHA256 `930e786d8ab987210523ea45e4551f1dfd8854e6de686787a6c4d1207a81d9f5`; canonical-encoder control `5cfe47bf4f00c4490402e19417c6de57fe13d823bd543c1dce74d32e55c45881`; reproducible offline summary `8bbb93ffef992e6f065c72c73a19783ee91f09c134bdef4b22921608bdf84952`. Raw trace hashes: base `fbd66767f1caab8943eabd9c5352115fd798294b3e020fc0d6454a99b36b291d`, candidate `e5bf880819f3b3bb41286ab2167f9d5a5cf79c62fb8d28327e571040bbb247ef`. Actual executable/source/environment/affinity receipts, original files and the fixed offline summarizer accompany the packet.

<!-- receipt v1 -->
receipt: head=80b43c1a238ad360363c6217b2a269094a333258 tree=188419a252573ae08969bbe79f4091967c0e155b parent=1057402d41387757b5cb2e6a29a1746fc9e10389 branch=agent-d/2720-stall-attribution signed=G dirty_files=0
preflight: test-results/preflight/20261018T072054.393634949-a3cf13e2-2e55-4f14-a5e1-82be6269517d/receipt.json sha256=252e0bd2e479fa6d1d8060ef022a9290a3e69729f40c81202660f3f4274d39fd (STALE(head=1057402d41) complete unstarted=0)
tests: 2735 corrected pair:canonical encoder control passed before server2; N1 base d092f96b46 PASS, N1 candidate80b43c1a23 quiescence RED; matched response schedule; exact claim ACK +150.4ms after candidate deadline; no replacement/hosted run; server2 released
regenerated: not-claimed
generated: 2026-10-18T14:20:48Z

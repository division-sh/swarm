## RED -> GREEN: bounded immutable operational-source reuse at 55adb583b9

For reviewer-e under [the P1 repair authorization](https://github.com/division-sh/swarm/pull/2735#issuecomment-6251089088). Signed source **`55adb583b9de0572f69e6e4adb23776ac0851ed1`**, tree `282c1fd92235a389689f920eefc4023be1c635bf`, includes the separately accepted API coverage repair `80b43c1a23`. **FULL repair track; unchanged hosted lifecycle / local focused, no added CI units. No PR push or hosted rerun yet.**

### Repair and identity/coherence contract

The new behavioral counterexample first reproduced the old function recompiling the same exact retained source on consecutive reads. Both `RunSQLite` and `RunPostgres` now use the **same private source-owner implementation, with one entry per store-lifetime reader**, not a global cache. Existing composition creates each reader once. The entry retains only successfully validated immutable source bytes and compiled `semanticview` semantics; no report, live progress, observation time, quiescence, instance state or authority is memoized.

**Every report still reads the retained source row inside its existing coherent snapshot**, after reading its header. The header bundle association must match; a hit requires exact blob bytes, bundle hash, member count, total bytes and creation instant from a previously validated row. The stored blob is privately cloned, so caller buffer mutation cannot rewrite the key. Missing/corrupt/changed rows cannot hide behind a warm entry; failures and canceled compilation are not cached. Restart creates a fresh memo. No live fields moved outside the snapshot, and compilation is not hoisted outside it.

The compiler-affecting inputs are fixed for this owner's lifetime: the canonical compiler/admitter in this binary, empty repository root, embedded platform-spec display path and embedded platform-pack inventory. There is no ambient/current bundle, resolver, external spec path or variable options input. The full retained source is the varying input. The private compiled source is only consumed by the existing read-only semantic projection and never returned in a public report. No second parser, graph derivation, persisted projection, background work, transactiontest reader or new public capability.

A context-cancelable private permit serializes cold admission/replacement; warm results do not escape before cancellation checks. No provider, recorder, receipt, CLI deadline/Ready ordering, polling interval, last-chance read, grace, budget or timeout was changed. Delta from `80b43c1a23`: **six files, +321/-11; 59 changed production lines**, remainder focused proofs. Proof cost: **adds 1 behavioral root in the existing lifecycle selection**, invariant: only exact successfully validated immutable source semantics are reused; extends the existing both-store snapshot root with warm-source and refusal/fresh-progress reaches. No new census, guard or wall-clock threshold.

### Required original smoke and the one authorized reproduction

The original **unmodified** `TestWorkspaceMCPCompiledDockerConformance/disconnect=false` passed once on vemew at this signed head, 29.4s, Docker enabled, zero skips and no overlay. No retry.

The fixed-candidate diagnostic then ran **exactly once**, N=1, on the granted quiet server2 window. Same Go **go1.26.8**, existing image `sha256:0c09f37b09828ac8460ef68fccb8aea5891a9cbcdc1c73c91f6b1e13da8073b1`, GOMAXPROCS **2** and actual test affinity **0,1**, unchanged **10s/25ms**, one-minute public-command fuse and outer 4m bound. Source signature/tree, all overlay/map/runner hashes and compiled executable hashes were verified before invocation; actual helper/CLI build identities and CLI runtime GOMAXPROCS=2 were captured. Server start load was **0.55**. The whole-environment hash is retained as a new-arm identity, not claimed equal to the earlier pair: this arm has its own evidence/cache paths and a fresh SSH session. Explicit execution settings/image/schedule match; no fresh base arm was authorized or run.

**PASS, 25.762s.** The already-validated canonical-byte-input proxy is byte-identical to the earlier corrected pair; it held only the first successful real model response to 7.09s, forwarding at **7.09458s**. Matched=true, canceled=false, no projection error; **two real model calls, one real read, one accepted emit**. Request/response bytes and existing assertions remain unchanged.

| Evidence | Prior base d092f96b46 | Prior candidate 80b43c1a23 | Fixed candidate 55adb583b9 |
| --- | --- | --- | --- |
| Retained diagnostic outcome | PASS | Deadline RED | PASS |
| Full contract compilations | 0 | 34 | **1**, plus **334 memo hits** |
| Compilation elapsed total, not CPU/SQL wait | 0 | 9,166.018ms | **271.988ms** |
| Exact worker delivery ACK relative to actual D | -22.965ms | +150.380ms | **-38.947ms** |
| Fixed arm's first Ready relative to actual D | attributed separately | none | **-24.572ms** |

The earlier pair remains attributed to [its retained checkpoint](https://github.com/division-sh/swarm/pull/2735#issuecomment-6251054501), not resampled. Fixed arm: run `44298cbb-d36d-4df0-8870-034d0f8338fc`, exact worker claim `d76969cc-a71f-54b7-a73e-d60e1e88dc70`. Its actual budget is **15:33:00.657835978 -> 15:33:10.657835978 UTC**. Emit feedback acknowledged at **10.582259866**, matching processed delivery receipt at **10.618889107**, and Ready at **10.633263603**. Native receipt call elapsed **3.113ms**. The last negative sample at **10.599800270** returned by **10.602609332**, before ACK and deadline, a **2.809ms** tail. This is genuinely on-time acknowledgement/Ready, not eventual acknowledgement being relabeled as success.

The fixed arm still performed **335 retained-row reads** and fresh live snapshots: only immutable compilation is reused. This demonstrates removal of the directly measured redundant hot read work and a passing bounded counterexample. **It does not prove sole causality for the original hosted RED or exonerate every other changed path. That original RED is not reclassified as inherited/infra, lost settlement, or stale-negative-only. Green hosted smoke/selection remains required.** No additional sample/campaign or bound change is proposed.

Trace buffer overflow=false, 6,553,731 bytes. Exact completion/continuation/processed-receipt correlation is retained; current disposition and OriginSettled=false are unchanged. The observer's current-path final files were copied only after its existing join, with copy/mtime receipts; their periodic sample preceded ACK and is not leak evidence. No new native hooks/readers/options or lock/CPU inference.

### Preservation, guards, preflight and host release

The memo root passes **race x3 at the signed head**: cold concurrent reuse, exact-key/changed-source validation, missing/corrupt/mismatched/metadata/alias refusals, fresh-owner restart and canceled queue. Both-store snapshot/refusal/fresh-progress reaches and completion attribution pass race x3 coverage. Existing N19 proposal/acceptance/refusal, historical/drained origin, sessions/future timers/human waits and no-mutation/conservation assertions remain intact. Text/JSON projection, aggregation and fixed alert/dedupe controls are green. **95 affected projection/owner/authority guard roots, 775 cases, zero failures/skips.**

One over-grouped preservation race command hit Go's unchanged **600s package timeout** during the third publication-attribution pass, without a leaf assertion or DATA RACE. That raw RED is retained, not erased or called a production fix: two complete publication matrices had passed, and the unfinished third-pass cells/three wait roots were completed separately, once, without repeating completed cells or changing a limit. T3's hosted arbitration remains; this was not a local tier restart.

**Preflight: pass (337.5s)** at `55adb583b9`, all 21 selected packages plus vet, complete with zero unstarted packages; published `preflight/local` status `56635161567`. Receipt SHA256 **`1596c224bfa9a7865960470eea36e1de19d1fe5834a55770ac2cd7a155adb6a7`**. Its soft cost warning is disclosed; no selector/deadline was changed.

**server2 released at 15:33:48Z**, already [reported on #2735](https://github.com/division-sh/swarm/pull/2735#issuecomment-6251741331). Own checkout restored cleanly to `0b7f1188a6`, own runtime/test processes joined, own empty runtime temp directories removed; image, B's Postgres container and other deployments unchanged. No access after release.

**Request reviewer-e's RED -> GREEN / exact-source verdict before the single PR push**, with unchanged lifecycle/focused and no new units. The original smoke obligation stays pending that exact-head hosted qualification; no diagnostic pass is substituted for it.

Frozen overlay/input manifest SHA256 `335d2beb21021a72b886ad139bf46b6a6ccdc5e450a9d50961968538d8135d57`; fixed-candidate summary `66aa9b2b7c1f2a241e53492d50576a6f55ac3ce88a7e1810a47be22f802055bf`; raw trace `2326901d972d2ba26b86321b7e6b4a358a011c8054dee7699838558c1a7dbbe3`. The executable identities, original trace/observations, join-retention receipts, offline summarizer, full local RED/partial-timeout/GREEN controls and preflight receipt accompany the packet.

<!-- receipt v1 -->
receipt: head=55adb583b9de0572f69e6e4adb23776ac0851ed1 tree=282c1fd92235a389689f920eefc4023be1c635bf parent=80b43c1a238ad360363c6217b2a269094a333258 branch=agent-d/2720-stall-attribution signed=G dirty_files=0
preflight: test-results/preflight/20261018T153405.266185580-92b3762a-3adb-4778-9d9c-bcbdcd156089/receipt.json sha256=1596c224bfa9a7865960470eea36e1de19d1fe5834a55770ac2cd7a155adb6a7 (exact-head complete unstarted=0)
tests: 2735 memo repair: original unmodified smoke N1 PASS; one authorized fixed-candidate diagnostic PASS, one compilation/334 memo hits, matching delivery ACK D-38.946871ms and Ready D-24.572375ms; race x3 and both-store preservation/95 affected roots PASS; grouped 600s package timeout retained and only unfinished cells completed; server2 released; exact-head preflight PASS 337.5s; lifecycle/focused unchanged; no PR push
regenerated: not-claimed
generated: 2026-10-18T15:50:04Z

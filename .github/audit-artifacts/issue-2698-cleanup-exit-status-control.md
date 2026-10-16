# Automatic Fixture Cleanup Exit Status

Track: FULL. Reviewer-b6215934416 approves the exact proposal6215862904
against signed source1f4fb2282a for staging, not runtime closure or landing.

The existing serveRuntimeTestProcess.cleanup now rejects an unconsumed nonzero
exit after its unchanged cancellation and bounded join. Already-stopped exits
remain owned by their explicit callers. No production code, cancellation order,
wait, timeout, transcript, retry, selection or fixture authority changes.

The permanent subprocess control exercises the real cleanup helper with zero,
unconsumed exit23 and already-joined exit23. It also verifies cancellation and
recorded join; arbitrary child failures cannot satisfy the negative case.
The unchanged control is RED with the original helper and GREEN with the repair.

Receipts in /home/youmew/.cache/swarm-2542-family145/:

- pr2698-process-cleanup-control-before.jsonl: RED0.027s, failed-exit case only;
  SHA2567d2456e7ebd6aab23b0ec4250d5b3f50cf653c6d83f405f0c6d20fbec1cfe6c5.
- pr2698-process-cleanup-control-after.jsonl: GREEN0.027s, all three cases;
  SHA256d34efcefb3636f81f9993375f52d593fbc87b46f0cc09ac357d534981c9ad92c.
- Exact permanent control bytes are the approved overlay's
  SHA256f8646b07e920cc82401acfc1950b30320b83f74e38f6ba56f2b7b9a8eeb6123d.
- pr2698-process-cleanup-permanent-race.jsonl: GREEN3.095s, all three
  cases on the repaired on-disk source with the race detector;
  SHA2563ec96f7daa4e630ab8476e3a90f983d1fe5a9593fcc988baf957c950415c9b83.

This repairs the demonstrated false-PASS at ManagedEmit's automatic FINAL
cleanup. It does not explain the original restart RED: that explicit stop already
asserted its exit. Neither ColdStatic original RED is explained; their explicit
stop assertions already reject failure. Docker/schema public-process tests do not
use this helper. ZERO original runtime-ledger failures are causally closed.

The five-run unchanged ManagedEmit diagnostic remains RED: one failed and four
passing root repetitions,314.862s. A failed-join stack retains a live fan-out
observer but does not identify the preceding phase or establish a shared cause.
Both ColdStatic roots and Docker each completed their fixed five-run diagnostics
without reproducing; those passes do not waive their original REDs. Schema and
further discriminating shutdown observations continue separately.

Canonical preflight and qualification still wait for the user-reserved selection
contract. Absorb current master once at the next actual freeze, before preflight.
Q1's final hosted both-store execution condition, H2 postgres/paced fail-closed
condition and the separate OPEN/uncredited194 findings/155 raw holds remain.

## Bounded Runtime Diagnostic Cut

The exact approved repair/control is signed commit
6d6c3eafaea02d3e0fef5e3183870174436adbc1, tree
21fcaba9989cf09aa0c1d2b5fc385cf0c460cd46, backed up without a PR on
agent-b/backup-2698-runtime-20261016-6d6c3eafae. The PR head is unchanged.

Each original Go root completed a fixed five-repetition diagnostic, non-race,
GOMAXPROCS=2, count5, p1, original600s package limit. Both backends or Docker
disconnect cases actually executed where present. Original behavioral assertions,
scenario deadlines, cancellation and cleanup order were not changed. The first
four rows below used signed1f4fb2282a; schema used the repaired6d6c3eafae.
Observation-only overlays retain stacks on real errors/deadlines and propagate
them into compiled release children. They are not committed production changes.

| Original root | Diagnostic result | Receipt in the same cache |
| --- | --- | --- |
| TestManagedEmitPublicationExactScopeBothStores | RED314.862s: one failed/four passing root repetitions; sqlite/nested explicit first-restart stop fails | pr2698-repeat5-managed-emit-unchanged.jsonl |
| TestServedColdStaticAgentRestartBothStores | Five passing repetitions; both backends | pr2698-repeat5-cold-restart-unchanged.jsonl |
| TestServedColdStaticAgentConstructionBothStores | Five passing repetitions; both backends | pr2698-repeat5-cold-construction-unchanged.jsonl |
| TestWorkspaceMCPCompiledDockerConformance | Five passing repetitions; both disconnect cases, required Docker opt-in | pr2698-repeat5-docker-unchanged.jsonl |
| TestSchemaRelationFactoringCompiledHostMCP | Five passing repetitions, package52.897s | pr2698-repeat5-schema-stagedcleanup.jsonl |

SHA256s in the same order:

- 39175e746569e9980a9e36cbd86dd92d05ad898cff40fedc92829ac55d44ed16.
- 9741e35876727b63c02dc0ba92d2721b1407a0eeeaf524e975b475357f9f66b2.
- fc1ea347b9c8d5b33f3ac147f1594b895dcd80627a92df2769fea77b8c0ea218.
- fe8136d36bfcc0c40c5056054dffe2f585bd6f50ef2967c2ebc60baeb1219198.
- 7f2399577613b0aa9b216d8b5490e8193f176cafeedad6d39368faa65181cc81.

The reproduced ManagedEmit failure is at the explicit stop, not the newly
checked final cleanup. Its failed-join stack retains the fan-out service and
opportunity/loss watchers with one process lease; the manager/runtime work is
already gone. This does not identify the earlier phase that exhausted grace,
prove a leaked lease, or establish a common cause with either ColdStatic root.

One further named discriminator captures stacks at the FIRST global shutdown
deadline, rather than only at the later join error. The same fixed five-run
ManagedEmit selection was measured serially on repaired6d6c3eafae and pinned
master56c8969ea64d2bc0b90f80eff47bce676bf0a26c. Baseline uses the identical
cleanup exit check in an observation overlay so ignored failures cannot count
as baseline successes. Same Go1.26.8/toolchain, GOMAXPROCS2, count5, p1,
non-race and original600s package limit; stable main/scratch paths differ.

- Candidate: five PASS, package324.630s, no failure/deadline stack.
  pr2698-repeat5-managed-first-process-deadline.jsonl,
  SHA25616dac52b7bb89c8ed516e54f39a12ccd6053ee018eccbb3eeaee3e8f1f03217f.
- Baseline: five PASS, package321.472s, no failure/deadline stack.
  pr2698-repeat5-managed-baseline56c-serial-first-process-deadline.jsonl,
  SHA2565065acda85583abb3dc70b238bae618188144435c7dcd95d341594f6cd0ee3f5.

The initial baseline build attempt was stopped before any test record, to avoid
overlapping the still-running candidate; its separate interrupted files remain.
The complete baseline above started only after candidate exit. Available
load observations are retained as context, not an infrastructure
attribution or controlled performance comparison:13:03Z3.44/3.38/3.61,
13:08Z2.01/2.84/3.37, baseline start3.50/3.16/3.45, end1.44/2.61/3.16.
No server2 run, unchanged qualifying retry or whole-tier run occurred.

The comparison is INCONCLUSIVE, not RED-to-GREEN production proof. All original
runtime REDs, including the reproduced ManagedEmit restart RED, remain OPEN and
unclassified. No new production boundary is proposed, no runtime closure or
debt credit is claimed, and no further unchanged loop is started. These receipts
bind their stated source cuts, not a future freeze/master absorb or preflight.

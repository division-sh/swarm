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

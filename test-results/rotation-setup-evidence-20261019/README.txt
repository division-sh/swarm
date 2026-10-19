Rotation predecessor setup / successor-expiry proof isolation.

Source: 7886103bc254bd44a202caebc0b2ad8b75a15a2c, signed GOOD.
Tree: eb8463b545b0e92b37a8e1209a2d67d712e36e02.
Base: a1b901ebb3ca387084c8e7a1c7e176d7a90aebb2.
Separate follow-up; #2747 and its orchestrator-owned rerun are unchanged.
Approved PLAN: https://github.com/division-sh/swarm/issues/2353#issuecomment-6263498062
Invariant and classification: https://github.com/division-sh/swarm/pull/2747#issuecomment-6263411113

Production LOC 0; test +1/-1; adds 0 roots. Move only the existing 150 ms
TTL setter from before predecessor acquisition to immediately before the first
Rotate. Normal predecessor setup already uses 120 seconds. The successor
expiry remains exactly 150 ms. No new helper, clock, assertion, ceiling or
policy. An executable byte comparison proves every other source byte unchanged.

Approved bounded diagnostic: one original and one repaired invocation, each
covering SQLite/PostgreSQL with the identical 200 ms post-acquisition delay.
Both original children refuse the FIRST rotation with expired=true and
ErrSessionLeased. Both repaired children show expired=false at first rotation
and PASS the original successor-expiry, receipt equality, exact refusal and
zero-mutation checks. Every log, overlay and pre-run source hash is retained.
This proves the setup-expiry mechanism under controlled delay; it does NOT
classify the earlier hosted event, which remains UNCLASSIFIED.

The diagnostic overlay is excluded from the landing source. Final focused:
three original roots / six backend children PASS, no failures/skips, including
receipt idempotency and nanosecond precision. No concurrency edits or race x3.
Final emitted lifecycle plan selects every root exactly once, solely under
store-runtime-selected. It uses actual policy, weights and discovered inventory;
the mapping is not hosted qualification. No duplicate local tier or CI dispatch.

Final exact-head preflight and machine footer will be appended on completion.
All original RED evidence remains retained; no retry-to-green or inferred
infrastructure classification. Hosted lifecycle and exact-head review remain
required for the separate follow-up PR.

FINAL exact-head preflight PASS: 790.435 seconds, all 25 phases passed,
complete=true, zero unstarted. Published preflight/local at the source above.
Canonical receipt and all phase logs are under preflight/; final stdout and
fresh machine footer are retained alongside the original controlled RED.

# Shared Verification Listener Policy

Bounded continuation of the approved fixture-brittleness priority map and the
CLI listener cohort. E's #2269 fixed the same golden verification defect with
a fixture-local configuration append at 56691c62d; that PR remains open at
this checkpoint. Its eventual rebase must remove the inline append rather than
retain two producers or duplicate YAML keys.

Canonical fixture policy: testutil.EphemeralServeListenerConfig. It emits only
the two loopback port0 listener settings. Both writeTestVerifyRuntimeConfig
(46 mapped calls) and goldenRuntimeConfig (17 existing calls) consume it.
The golden package already permits this existing testutil import in its
workload fixture; no package-boundary exception or new configuration DSL is
introduced. Production defaults and explicit-address binding proofs remain
unchanged. This reliability repair claims no raw-SQL debt reduction.

## Consumption And Proof

| Manifestation | Disposition and exact proof |
| --- | --- |
| CLI positive verification inherits fixed listener defaults | Reproduced in the predecessor cohort; current real four-socket proof and exact occupied/released binding proof retained under race. |
| Golden verification inherits fixed listener defaults | Common producer consumes the same policy; both-backend configuration test preserves store, recovery, provider and workspace settings. Public SQLite possession/serve and real MCP binding roots exercise the emitted configuration. |
| CLI and golden producers drift after a master requirement | Two finite recipes plus actual-source/type overlays; independent negative controls reject changed profile, store, recovery, writer, fixture lifetime and policy. |
| Operator-selected occupied addresses | Different binding concept: explicit occupied MCP process proof and CLI exact-address refusal are unchanged. |
| E's pending local golden append | Named handoff: remove the local append on rebase after #2269 lands. Do not claim that cutover before it occurs. |

Qualification: focused race and public-process roots, complete finite recipe
controls, all structural guards, unchanged G01 census and finding registry,
native unused, planner/spec contracts and exact committed-head complexity.
Receipts live under /home/youmew/.cache/swarm-2542-local-20261008/shared-port-*.
The parent migration, zero-debt guards and final integrated proof remain open.
Existing watchlist mapping is sufficient; no runtime-spec change is needed
for this fixture-only policy.

# #2735 LIGHT fixture migration evidence, 2026-10-18

Signed source: 24045c6bf5040d2663a1b1724fce0bbdd0b48056.
Original RED head: 0e78f146d8a0afb141628520c3e7c4f32033c339.
Contract and authorization:
https://github.com/division-sh/swarm/pull/2735#issuecomment-6253067928
RED classification and base/candidate comparison:
https://github.com/division-sh/swarm/pull/2735#issuecomment-6253018592
Original hosted failure:
https://github.com/division-sh/swarm/actions/runs/39148692608/job/117738853229

Only two test files changed: shared protocol fixture supplies both required
non-null evidence arrays; existing malformed-result root gets four independent
missing/null boundary controls. Zero production, decoder/schema, memo,
accounting-policy, timeout, budget or workflow changes. All existing quiescence,
RPC-order, no-call, classification, publish/readback and negative assertions stay.
Commit trailer: Track: LIGHT. Zero added roots; four new named wire reaches.

Canonical lifecycle plan emitted from the actual PR body and real policy/weights.
Local command, at the signed source above:

    go run ./cmd/swarm-test --planned test-results/2735-light-proof-plan.json catalog-required-verify --receipts test-results

PASS: 894 roots, 4106 cases, zero failures and zero skips. Go package elapsed
226.164s; admitted unit elapsed 240.138983725s. All ten shared-helper consumer
roots, the four missing/null cases, status/projection family, and affected CLI
census/guard roots ran. No duplicate local tier or race campaign was used.

Original RED and both pinned comparison arms are included with their original
attribution under original-red-comparison/. The original hosted RED is retained,
not reclassified as infrastructure or erased by a rerun.

Exact-head preflight receipt and local machine footer are retained alongside
the unit evidence. The evidence commit is not the PR source head; it only adds
ignored proof artifacts on top of the source commit.

# #2735 comparison-only N19 RED-to-GREEN evidence

Signed source: 95464fccbd3d3efa4238b71325d3685f25c93644.
Parent RED: 24045c6bf5040d2663a1b1724fce0bbdd0b48056.
Authorization (FULL track):
https://github.com/division-sh/swarm/pull/2735#issuecomment-6253576563
Cause, full affected-set classification and pinned master control:
https://github.com/division-sh/swarm/pull/2735#issuecomment-6253522013
Original hosted failure:
https://github.com/division-sh/swarm/actions/runs/39151917009/job/117748302305

Exact source diff: one existing _test.go file, +166/-0; no new roots.
The existing history root gains 27 hostile reaches and input-population
immutability checks. Only the temporary historical comparison copy can have
the single N19 Successor cleared, after exact identity/multiplicity, historical
endpoints, both exact observation cuts and actual live function validation
through validateN19FanOutBarrierSuccessor. No generic stripping is introduced.

All 893 active recipe rows and Before/After bytes, the frozen embedded-spec
packet/checksum and expected population hash, 12 prior chains, Q10 composition
count 1, live N19 successor and the 16-function/27-call barrier invariant are
unchanged. Production, memo, CLI mock, candidate preparation, budgets, timeouts,
workflow, policy and selection infrastructure are byte-identical to parent.

Focused historical-population + native barrier owners passed (2 roots/108 cases),
then the COMPLETE affected package set ran at the signed source above:

    go run ./cmd/swarm-test -- ./tools/fixture-codemod/... -count=1 -json

PASS: 388 roots / 2194 cases, no failures, no skipped named tests. The five
reconcile-generator packages report no test files. The complete executed-root
TSV is included; its set equals the original RED sweep's 388 roots. Pipeline
package elapsed 192.633s; the tools root package elapsed 0.118s.

Catalog-required-verify's green receipt remains attributed to parent 24045c6bf5:
894 roots / 4106 cases, no failures/skips. All inputs outside this sole comparison
test file are byte-identical, as proved by the empty preserved diff. No catalog
rerun or duplicate local tier was used. Original immutable catalog/CLI RED proof:
https://github.com/division-sh/swarm/pull/2735#issuecomment-6253213037

The original history RED, full failing sweep, exact indexed recipe delta,
pinned master PASS control and original catalog summary remain under
original-population-red/. Earlier API/smoke/memo and mock REDs retain their
existing independent source attribution and immutable packets; they are not
reclassified by this repair.

The fresh exact-head preflight and receipt are retained alongside this evidence.
No race campaign (test-side, synchronous comparison only), hosted diagnostic,
whole-tier local repeat, additional units or server2 invocation was performed.

# Exact-source Test-Time correction and ratified proof placement

This is a focused/counterfactual record, not final-head hosted qualification or
parent closure. #2604 run 38097201316 at bd2d853e3 remains RED; all its proofs
passed, but its hard added-work timing gate did not. #2542/#2151 remain open.

## Binding Authority

- Exact-source correction: #2535 comment 6103962032 (reviewer-g).
- Placement boundary: #2604 comments 6103925334 (reviewer-b), 6103929351 and
  final 6104099093 (reviewer-g). USER ratification at 01:15 UTC on 2026-10-11
  approves the five-root move; the final overlay ruling is full-only.
- Authoritative spec: test_specification.internal_catalog_conformance.
  qualification_tiers.{fixture_codemod_qualification,timing_publication_and_cadence}.

The correction is one immutable artifact under CaptureTestTimeReference, not
an automatic merge-base comparison, generic allowance, periodic refresh or
timing-result cache. A root identity does not establish unchanged behavior.
New measured work remains independently charged; retained drift remains
advisory only for fully admitted PR proof. Non-PR aggregate enforcement,
5-percent/600-second bounds and the individual >30-second rule are unchanged.

## Complete Reference Capture

All original command receipts were downloaded, with the original ci-plan,
from #2608 run 38086512667, attempt 1. The command-evidence owner validated
every one of 83 units before admitting 11,809 measured cells. No timing summary
was converted into receipts; no partial or replay-skipped execution was used.
The 83 original evidence-file SHA256 values are committed in
issue-2604-reference-receipts.sha256, relative to the downloaded artifact root.

| Binding | Exact value |
| --- | --- |
| Workflow head | 3d87f2ba3598b0728e754512c8e7f57e9a96d17a |
| Execution source | c293cda6b003dc50519e2b861af5fbbeeffd7421 |
| Execution/landed-master tree | 54e6eee85de9d4928bfe130d8ba440adf48f59d9 |
| Landed master | b7b046701a3400f8c9f28abd7962e460ba3065a6 |
| Plan digest | ec5fee61b8f63c74570b64289e6358ea8da2b9a29201cb922fe67bd24c581da7 |
| Exact-source policy SHA256 | 1173c9a9f1bf40bce34df26c866aa355deee187867f149f8e5d5a79f022881f2 |
| Reference SHA256 | f195969f9056e3c1e26605d114b50e61c8c2efddec55bdee0745f3de516a9db4 |
| Native build context | linux / amd64 / CGO=1 / GOWORK empty |
| Execution identity | ci-postgres-gateway-empty-v1 / count-1 |

All proof jobs, Required test summary, CI timing budget and the exact-run full
tier check succeeded; master-only publishers were normally skipped. Both
original 900-second soaks remain measured separately. The source policy is
obtained from the execution commit, not this branch's later selector edits.
A second invocation of CaptureTestTimeReference produced identical bytes.

```sh
go run ./cmd/swarm-test-timing -capture-test-time-reference \
  -plan "$REFERENCE/ci-plan/proof-plan.json" \
  -evidence-root "$REFERENCE" \
  -proof-policy "$REFERENCE/source-test-proof-plan.yaml" \
  -test-time-reference .github/test-time-reference.json
```

REFERENCE is ~/.cache/swarm-2608-reference-38086512667 on vemew. Every file
named in the manifest is an original *-primary-evidence.json artifact.
Canonical loading pins the artifact bytes, approval URI, run/attempt,
workflow/execution source, plan/policy and complete tier membership.

| Reference | Core cells / seconds | Lifecycle cells / seconds | Full cells / seconds |
| --- | ---: | ---: | ---: |
| Historical run 37937174260 | 7420 / 2569.89 | 11455 / 13009.42 | 11556 / 16414.28 |
| Approved run 38086512667 | 7610 / 2385.24 | 11708 / 12374.57 | 11809 / 15907.75 |

The earlier reference artifact remains recoverable from bd2d853e3 and its
original approval, #2535 comment 6082942278. Lower observed totals are not a
claimed optimization, threshold relaxation or guarantee of future timing.

## Exact Before/After Ledger

issue-2604-test-time-repin-ledger.tsv contains every one of the 583 previously
added core cells and the two newly added cells exposed by the re-pin: exact
package/root/backend/environment/count, original classification, reference
membership/time, unchanged candidate time and both projected classifications.

- 191 previously added cells become retained: 109.09 candidate seconds.
- 392 previously added cells remain added: 346.27 candidate seconds.
- Two roots retained by the older reference are absent from the approved
  newer reference and now correctly charged as added: 0.66 seconds. Both are
  pipeline canonical-run-before-mutation SQLite refusal proofs; neither is
  exempted because of historical existence.
- Five expensive codemod roots account for 299.95 seconds. The ratified core
  deferral leaves 389 added cells / 46.98 seconds, not the identity-only 46.32.

The same exact candidate receipts from run 38097201316 are admitted against
their unchanged original plan; only the explicitly chosen reference and
candidate membership projection change. This is a counterfactual, not a
receipt for the edited source or new local controls.

| Projection | Core added / allowance | Lifecycle added / allowance | Full added / allowance |
| --- | ---: | ---: | ---: |
| Original reference and policy | 455.36 / 128.4945 | 561.61 / 600 | 572.96 / 600 |
| Approved re-pin only | 346.93 / 119.262 | 374.75 / 600 | 386.10 / 600 |
| Re-pin plus ratified placement | 46.98 / 119.262 | 258.79 / 600 | 386.10 / 600 |

The intermediate preparation without a versioned placement disposition still
FAILED the five individual >30-second rules. The final finite disposition,
separate from the re-pin, admits these exact cells only at their approved
minimum tiers and records original run/source/approval in warning evidence.
All observed seconds remain charged; other expensive roots, foreign/aliased
cells and promotion to core remain rejected. The final archived-run projection
is WARN, exit 0, with no hard problems, not fresh edited-head qualification.
Retained deltas are +292.54 / +373.66 / +267.83 seconds; strict aggregate
counterfactuals still fail. Nightly enforcement is not preemptively weakened:
future complete nightly evidence must be classified on its actual receipts.

## Ratified Placement And Exhaustive Consumption

The existing special-package/explicit-unit owner is used; no new runner,
scheduler, cache, source switch or test framework is introduced.

- codemod-owner-guards keeps EVERY other current/future ordinary root in
  core/lifecycle/full, including small actual-loader controls and the 6.06s
  conversation reconstruction counterexample.
- codemod-pipeline-mutation owns the independent timer successor variants and
  real premature-dispatch counterexample, with their original nested checks.
- codemod-bus-mutation owns the independent claim/settlement mutants and real
  blocked-SQL/assertion-failure join proof.
- codemod-candidate-overlay owns the complete simultaneous 19-directory
  candidate type-check, all original variants and existing issue2413 profile.

The final overlay unit is full-only, following G's final ruling and USER
ratification. No cheap proxy, single-package overlay or second loader mode is
added: genuinely cheap guards stay core/lifecycle. Future codemod qualification
must select full or explicitly include the full-only unit.

All five proof bodies are byte-identical to bd2d853e3. Preserve separate
overlays, processes/stores, source markers, backend/variant cases, race and
deadlines. A discarded shared-mutant compilation experiment failed; it
produced no savings claim and no changes survive. Skip remains reserved for
the original soak. A run-only finite-name complement preserves prospective
ordinary roots, including neighboring/extended integration-name prefixes.

Planner policy, all-profile execution binding, catalog inventory and timing
inventory consume the same four explicit units. Negative controls reject
coverage/envelope drift. Full's partition is exhaustive/disjoint; lifecycle
plus its one exact full-only deferral is exhaustive/disjoint. Core defers
exactly five roots. Existing normal budget classes
are used with no ceiling, timeout, workload or count change.

## Focused Proof And Remaining Gate

Vemew, Go1.25.0, GOMAXPROCS=2, GOFLAGS empty, short on-disk TMPDIR:

- Final ratified placement race: 29 roots / 221 PASS records, zero failed or
  skipped tests, swarm-2604-final-placement-race.jsonl SHA256
  17cae5d93f8c115b8176e01818d5d862a01278d20e3dffa816b5b393b02d07ad.
  Includes all-profile native binding with the full-only overlay deferral,
  exact finite approvals, foreign/renamed/environment/count/backend refusal,
  core-promotion refusal and unchanged hard aggregate limits in PR/strict modes.
  Earlier preparation receipts below are retained, not substituted for this delta.
- Final original-run counterfactual: ratified-repin-placement.json SHA256
  73875de91b18780f5f973898dda474d6979eaa1472fd5d8af45c169694254a60;
  WARN/exit0, no hard problems. The unchanged strict aggregate still FAILS.
- Focused race: 27 roots / 184 PASS records, zero failed/skipped tests:
  swarm-2604-repin-placement-race.jsonl,
  SHA256 10e887a2bbef8bfbb466a54d081c0140fb2d0e339181fc89e8e181f930cc24e5.
- Includes exact partition/future-prefix controls, all-profile required-root
  binding, catalog/timing inventories, ambient-flags refusal, pinned artifact
  tampering, unchanged-population successor and a synthetic >30s new root
  rejected despite adequate aggregate allowance. Synthetic controls are not
  hosted command receipts.
- Actual capture rejects the candidate execution source and rejects this
  branch's edited policy. No output reference is created on either refusal.
- Replay of the original approved master-tree receipts using its exact policy
  and paginated/slurped same-attempt jobs: PASS, zero added cost in every tier.
  This is archived-evidence admission, not a fresh successor execution. An
  initial API serialization mistake was INCOMPLETE and retained separately;
  only the corrected original API envelope was used for the complete replay.
- CLI race controls at signed local code checkpoint 4669a4752: five roots /
  27 PASS records; SHA256
  308eecb12b317ef7607e3bd694c7d1a0d5600fc884dea67a38d06c923893a861.
  Required/failed job, forged cadence tier and trusted-event controls retain
  fatal incomplete/failed evidence rather than turning it advisory.
- Tracked-source retirement inventory PASS after adding all audit artifacts.
- Independent complexity against b7b046701 PASS at 4669a4752: cognitive
  hotspots 557 -> 557, cyclomatic 259 -> 259; exact delta SHA256
  aaacf959848885002b927cafbf640cb2b4902dd1ac165bdf378efa70e87feb1e.
- Scoped native Linux U1000 for testplanning/testtiming/testcatalog PASS under
  pinned Staticcheck v0.8.1 / Go1.26.8, default/race/issue2413 matrix. This is
  scoped local evidence, not a claim of the final Linux/Darwin hosted union.
  An initial Go1.25 invocation refused the analyzer's Go>=1.26 requirement;
  that failed tooling invocation is retained and is not a U1000 result.

No duplicate local tier, server2 run or speculative hosted retry. Unchanged
mutation execution receipts carry. The original candidate RED is retained.
USER coverage ratification and the explicit versioned >30s disposition are
recorded. The combined replacement head still requires delta review and new
exact-head hosted full PASS. No RED is relabeled green and no parent completion
or merge claim is made.

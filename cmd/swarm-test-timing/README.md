# Full Cadence Observation

## Failure Evidence Readback

`go run ./cmd/swarm-test-timing failures -run <id>` reads the exact Actions
attempts, their current-attempt native plans, jobs and outer Go failure events.
It prints failed leaves and matches the reviewed `.github/flake-ledger.json`.
`-last 150` observes the last 150 PR runs; `-population <file>` consumes a saved
JSON array of those typed observations. `-json <file>` retains observations,
matches and the population summary. Missing API/log/artifact evidence remains
visible and unknown. Expected failing child probes are not outer failures.

A known signature is not causal attribution to a new run. Only an exact reviewed
run/attempt/job/executed-source observation can supply attribution; an author
head is not a merge checkout. Historical rates name their comparable exposure
and evidence, not a guessed fleet probability. Earlier RED attempts remain in
the population after a green resample. Cancellation and unstarted work are
separate from test failures; downstream timing/summary REDs do not duplicate a
primary failure. Non-PR-only RED share is a reviewed lower bound with unknown
and mixed outcomes shown, not a waiver or a successful-subset estimate.
The summary counts incompletely observed runs explicitly. Per-phenomenon run
counts can overlap and are not additive; an unavailable plan or log does not
prove that a canceled run had no assertion failure.

The ledger preserves #2353's 61 historical IDs and a frozen body/thread source
index, including superseding and unnumbered intake links. The six curated seed
records distinguish signing variance, both fixed-plan leaves, debt command
headroom, Darwin timeout policy and Docker transport. A policy fix is not a
diagnosed variance cause. Update the source index/record through review; do not
classify historical prose automatically or erase old failures. Quarantined or
fixed labels never skip a test. Full-cadence/core-escape analysis below remains
a separate evidence owner; ledger lookup cannot grant that credit.

The useful daily timing-model publisher remains unchanged in frequency. Weights
drive balancing, eligible packing, and ETA, never required root membership or hard
deadlines. Valid model-only create and update bodies select core qualification.

`-observe-full-cadence` consumes the existing full plan and canonical timing
evidence without making a passing receipt or changing required checks:

```sh
go run ./cmd/swarm-test-timing -observe-full-cadence \
  -plan artifacts/plan/proof-plan.json -evidence-root artifacts/evidence \
  -workflow-run-id 123 -workflow-attempt 1 \
  -result-json artifacts/full-cadence.json -markdown artifacts/full-cadence.md
```

Failed roots are unclassified candidates by default. Optional independently
reviewed `-cadence-classifications` supplies package, root, classification, review
URL, introducing commit, and first detection run ID. A confirmed core escape also
requires `-prior-core-plan`, `-prior-core-evidence-root`, and `-prior-core-run`: an
actual successful preceding CI core attempt whose complete evidence covers the
introducing lineage but does not select the failed root. Master first-parent
lineage is read from Git, not inferred from the newest merge. Unknown introducer,
missing coverage, and unavailable historical evidence stay unknown.

Reviewed regression and confirmed escape counts are distinct. Escape rate is
always **N/A/unmeasured**, even with confirmed escapes: #2535 must first define and
prove a comparable escape/non-escape population. Unknown findings are not non-escapes.
Observations retain incomplete and failed evidence separately from infra, harness,
budget, or non-regression classifications. First-parent detection lag requires
causal review evidence; repeated attempts do not invent a second introducing
commit. Nightly/manual frequency and one-week operational acceptance remain open
under #2535. #1967 is closed historical context.

## Retrospective Causal Replay

The hosted full job fetches complete Git history and saves its plan, all command
receipts and an unclassified observation, even for failed/incomplete work. It does
not guess attribution or a prior core run. Enrichment happens after independent
review through this same report entrance, not a new dispatcher or metrics service.

Select the actual full run/attempt and the preceding successful **core** run/attempt
from the CI artifacts. Each execution-credit artifact is immutable and names its
exact run and attempt. Never use the old unscoped names, choose by timestamp or
artifact ID, or combine a failed-only rerun with earlier successful producers.
Preserve separate evidence directories so duplicate inputs remain visible to the
native validator. Preserve the original files; do not edit identity, profile,
outcomes or missing evidence to make them comparable. Obtain core metadata from
the exact attempt API:

```sh
gh run download "$FULL_RUN" -n "ci-plan-$FULL_RUN-$FULL_ATTEMPT" -D full/plan
gh run download "$FULL_RUN" -p "proof-$FULL_RUN-$FULL_ATTEMPT-*-timing" -D full/evidence
gh run download "$CORE_RUN" -n "ci-plan-$CORE_RUN-$CORE_ATTEMPT" -D core/plan
gh run download "$CORE_RUN" -p "proof-$CORE_RUN-$CORE_ATTEMPT-*-timing" -D core/evidence
gh api "repos/division-sh/swarm/actions/runs/$CORE_RUN/attempts/$CORE_ATTEMPT" > core-run.json
git fetch origin '+refs/heads/master:refs/remotes/origin/master'
# If this checkout is shallow, fetch its complete history before replay.
if test "$(git rev-parse --is-shallow-repository)" = true; then
  git fetch --unshallow origin
fi
go run ./cmd/swarm-test-timing -observe-full-cadence \
  -plan full/plan/proof-plan.json -evidence-root full/evidence \
  -workflow-run-id "$FULL_RUN" -workflow-attempt "$FULL_ATTEMPT" \
  -prior-core-plan core/plan/proof-plan.json -prior-core-evidence-root core/evidence \
  -prior-core-run core-run.json -prior-core-landing-sha "$CORE_MASTER_LANDING" \
  -cadence-classifications reviewed-findings.json \
  -result-json full/reviewed-cadence.json -markdown full/reviewed-cadence.md
```

`reviewed-findings.json` is an array of the existing typed attribution records:
`package`, `root`, `kind`, `review`, and, for causal regressions,
`introducing_commit` and `first_detection_run_id`. `review` must cite the
independent issue/review disposition; filenames or a newest-merge guess are not
causal evidence. Empty attribution retains honest unclassified observations.

Known lag requires the detected execution and introducer on observed master's
first-parent lineage, and complete successful core evidence after introduction
but before detection, with the failed root absent from that core selection.
For a PR core proof, `CORE_MASTER_LANDING` is the actual master merge commit, not
the PR head or a renamed plan source. The **existing merged-proof observer** must
independently verify exact PR association, equal execution/landing trees, current
protected checks, latest successful run/attempt and unchanged plan digest against
read-only GitHub/Git facts. The original core plan and command SHAs remain intact;
only this verified landing is used for master first-parent position. A direct
same-source master core proof does not require the landing flag.

Master replay queries `ci-plan-<observed-PR-run>-<observed-PR-attempt>` and requires
one unexpired artifact with the exact valid plan and corresponding `ci-selection.json`.
It never uses the observing master push's coordinates or creation-time heuristics.
Ordinary consumers and the protected publisher use their own current workflow
coordinates, not retained `needs` outputs from an earlier attempt. Required native
master-nightly/explicit-dispatch native artifacts use `unused-linux-<run>-<attempt>`
and `unused-darwin-<run>-<attempt>`; both current native collectors and the real
union must succeed on those runs. PR and push CI require all three native jobs
to be skipped, regardless of tier or replay. Explicit candidate-branch dispatch
retains full native execution but cannot qualify the master-nightly marker.
Names do not replace
any existing source, config, platform, result-integrity or complete-job validation.
A failed-only, union-only, timing-only or publisher-only rerun with missing current
producers is unqualified; perform a complete new attempt rather than relabel evidence.

Foreign branches, retired/unavailable artifact schemas, wrong attempts, superseded
runs, expired artifacts and unverified source aliases do not gain master lineage
or core credit. They remain explicit unknown/problems; do not rename their SHAs.
No merged-tree replay receipt by itself supplies the missing actual core commands.
The output preserves original full failure and never changes protected
checks. Archive the inputs, reviewed attribution and outputs together with the
run/attempt links. A successful command is report generation, not proof that a
regression, rate or lag is known.

## PR And Cadence Timing

`-evaluate-budget` uses the existing trusted `-event` input. Only exact
`pull_request` selects independent PR added/promoted-cost reporting. Complete
plan-bound proof still fails closed. Snapshot-derived new-work cost above either
5% of the frozen tier baseline or 600 seconds is advisory, as is unreviewed >30s
new/promoted-root placement. Historical placement approvals remain attributed
without requiring new hardcoded approvals. Retained
speedups and removed roots cannot subsidize new work. Retained drift that would
fail the original aggregate is an explicit PR warning, not a proof waiver.

Fresh schedule/manual/master evaluation requires a full hosted plan and reports
complete aggregate growth as advisory under the user-ratified
[user-confirmed LEAD ruling](https://github.com/division-sh/swarm/issues/2535#issuecomment-6113532469).
The `strict_aggregate` mode keeps its name, full tier table, exhaustive retained
deltas and strict counterfactual; excess aggregate cost produces `WARN`, not a
failing summary. Only exact `pull_request`, `push`, `schedule`, and
`workflow_dispatch` events are admitted; missing, unknown or malformed events
produce `INCOMPLETE` before either timing policy is selected. Both comparison
modes retain their full cost accounting and historical strict counterfactual,
but no snapshot-derived cost or placement verdict fails the summary. Buffered
command-ceiling overruns remain advisory on PRs only after complete successful
proof admission; push/schedule/manual buffered ceilings remain enforced.
Go/workflow timeouts and invalid/incomplete proof or failed/cancelled jobs remain
hard on every event. The pinned reference, numbers, selection
syntax, cadence and gate freshness are unchanged. Diffuse real creep below unit
ceilings remains visible in advisory tables rather than automatically blocking
delivery. No recurring re-pin or Go1.26 timing re-pin is required by this policy;
effective compiler identity remains a separate proof-validity obligation.

Current plan and receipt build/environment/count identity must match exactly.
Effective GOVERSION comes from the producing `go env` and is required in bound
plans, receipts, reference metadata and replay. Missing/blank compiler versions
or current plan-to-receipt version mismatches refuse before execution credit;
the observer's compiler, module minimum and cache cannot reconstruct them.
Pure pre-bind plans remain valid planning outputs, but command evidence requires
a complete bound current build context and nonempty selected/required roots in
every executable unit, with exact classification/package/child binding and no
duplicate execution cells. Receipt roots outside that manifest refuse. Successful
package receipts alone cannot qualify an unbound plan or gain timing credit.
A valid current build or scope need not match the historical reference: divergence
is an advisory warning, not `INCOMPLETE`. Changed build contexts and new scope
cells are charged as added/uncomparable, never as retained timing or speedup
credit; historical cells remain removal-neutral. Reference bytes/digest, policy
integrity and invalid/zero populations still fail closed.

JSON records `comparison_mode`, independent added cost, exhaustive
`tiers[].retained_cells` identities/before/after/delta and strict counterfactual.
Markdown labels the bounded largest-delta view. Command, ratchet and actual-job
results compose monotonically (`PASS < WARN < FAIL < INCOMPLETE`); a warning
cannot overwrite failed or incomplete evidence. Re-evaluating archived receipts
is algorithm evidence only and never relabels an original red run as green.

## Named CI Supplements

The current PR-body selection can retain a lower tier plus named hosted units:

```text
CI-Tier: lifecycle
CI-Units: conformance-soak-sqlite, conformance-soak-postgres
```

`CI-Units` is a single unindented comma-space-separated line. IDs must be in the
hosted full plan and absent from the base tier; invalid, duplicate, local-only,
no-op or overlapping selections refuse. Selection precedes native root binding,
batching and deferral generation; original count, recipe and backend children
remain required. Full-only extras add the frozen full timing comparison without
changing the pinned reference or lower-tier membership.
Each supplement carries its original full workload and native full-owner
completion obligations, including higher-tier registry children; the aggregate
tier stays unchanged. A lower-tier skip cannot satisfy a selected supplement.

`-ci-selection -plan PLAN -workflow-run-id ID -workflow-attempt N -result-json JSON`
emits the plan-bound selection report. Its digest hashes compact UTF-8 JSON with
ordered fields `version`, `tier`, `extra_units` (sorted non-null array), without
a newline. The existing CI tier job is named:

```text
CI tier: <tier>; selection: <sha256>; run: <id>; attempt: <n>
```

The current effective tier and extras must match the qualifying plan exactly.
Body-only selection edits require new execution, including edits after green.
The merge gate and verified-master owner require successful same-head/run/attempt
trusted-App selection and Required summary checks in the same suite. Selection
metadata alone never grants proof credit; failed/cancelled/skipped/missing unit
evidence still refuses. Native-unused remains outside the named-unit selector.

## Historical PR #2595 Qualification Amendment

The #2535 venue-identity repair now gives profiles and units explicit
`environment_ids: {ci: ..., local: ...}` declarations. The plan owner resolves
the selected venue before emitting its digest and receipts. Fixed
`environment_id` is reserved for genuinely venue-invariant recipes; combining
the two forms, an unknown venue, or an absent selected recipe refuses planning.
The committed policy uses venue declarations throughout, including its local-only
explicit-PostgreSQL projection. Unit names never choose a recipe. Historical
receipts and the pinned test-time reference remain unchanged; the failure below
is retained and is not converted to passing evidence by relabeling it.

[Reviewer-g's ruling](https://github.com/division-sh/swarm/pull/2595#issuecomment-6090325132)
requires one ordinary hosted full PR qualification for the unchanged implementation
at `0ce844da1670b565409295a253d1d3ca8257a570`. Its hosted core run
`37997577951` passed all 35 proof units but could not qualify against the pinned
full reference: existing core `local-*` environment declarations are outside that
reference's CI scope. The two stale-package diagnostics are advisory, not package
removals. The underlying core/reference mismatch remains tracked under #2535.

This audit-only commit changes no source, policy, reference, selector, threshold,
environment identity or backend obligation. Local qualification retains exactly
21 reviewer-approved units at `ed14c060c` plus the three fresh units and registry
race/count-three proof at `0ce844da1`; receipts are not relabeled to this commit.
The full run must retain actual required SQLite/PostgreSQL child evidence and
pass its own timing ratchet, `CI tier: full` and `Required test summary`.

The temporary hosted tier escalation is ratchet compatibility, not an intrinsic
runtime-risk ruling or a general waiver. An ordinary PR event is necessary:
workflow dispatch publishes `Full dispatch summary`, which does not satisfy the
protected `Required test summary`, and the merged-proof selector admits only a
successful exact-head `pull_request` run. Earlier failed receipts remain failed.

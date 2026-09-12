# Pre-Implementation Coverage Audit: #2447, R1.4 Baseline First

Historical audit below: the independent gate is now **approved as first slice**.
The binding implementation addendum is [issue-2447-approved-gate.md](issue-2447-approved-gate.md)
and [the recorded issue ruling](https://github.com/division-sh/swarm/issues/2447#issuecomment-5648061668).
Pending statements below describe the original submission, not current gate state.

Agent-g. Analysis baseline: `origin/master@90809b1cb55a4bb97655a199292dda338e722bf5`
(merged #2446). Docs baseline: `fc0eb9500fb5a0c63603dabdc6efad23971e1a53`.
Status: **independent gate requested, not approved; no implementation started**.

## Binding Context and Class

Category: high-risk maintenance of a CI acceptance boundary, not runtime semantic
change. The full #2447 body and thread (no comments at intake), #2407 R1.4 and
program rules govern. There is no exact platform-spec section defining a source
complexity ratchet. Those issue contracts are binding for this first PR. Runtime
contracts remain unchanged; later serve extraction must reread the authoritative
shared serve/mock construction, publication-before-execution, selected-store
activation, outer-owned shutdown, reset and readiness contracts.

Observed symptom: the repo has no gocyclo/gocognit CI baseline or non-increase
check; historical prose scores no longer identify today's functions. This is not
an execution failure and no runtime bug is claimed from a high metric alone.

- Exact concepts: deterministic tracked-source inventory, analyzer/policy identity,
  per-function complexity evidence, and cross-revision non-increase admission.
- Chosen working class: `source_complexity_growth_has_no_reproducible_ci_ratchet`.
- Immediate parent: keep-zone complexity maintenance (#2447).
- Broader parent: executable regression corpus and mechanical merge gating (#2407).
- Framing: broad enough as a multi-PR stream; baseline-first is explicitly ordered
  by #2447, not a convenience split inferred by this audit.
- Intended closure: eliminate the chosen ratchet class entirely in the first PR.
  That PR is **Part of #2447 / Part of #2407**, not Closes either issue.
- Commitment: the first PR closes its chosen class, not just one workflow step.

The cited large function was an entry point, not the audit boundary. Census covered
the one current workflow, all its event/profile and aggregate gates, local tooling,
tracked Go files, generated sources, other program lanes, and the hotspot list.

## Current Measurements and Source Limits

Exploratory tools: unmodified `github.com/fzipp/gocyclo/cmd/gocyclo@v0.6.0` and
`github.com/uudashr/gocognit/cmd/gocognit@v1.2.1`, run outside product dependencies.
No vendoring, go.mod edit, or custom complexity algorithm. Successful analyzer
execution with `-over` can exit 1 for matching rows; the proposed owner instead
collects unfiltered output and treats unexpected nonzero execution as failure.

Proposed scope: every tracked `.go` file, excluding `_test.go` and Go's canonical
generated-file marker. Include all platform/build-tag variants, development tools
and authored support packages; these are source measurements, not a host-selected
`go list` build. Support-source measurement does not authorize refactoring it.
Untracked files cannot affect CI. Empty/missing inventory is an error. A generated
exclusion is recorded by path, not silently lost in an ignore pattern.

For the exploratory run, the two generated paths were explicitly excluded after
checking their headers: `internal/store/internal/runtimepersistence/facade_forwarders_generated.go`
and `internal/runtime/pythonmodule/artifact_manifest_generated.go`. The production
checker must discover markers, not hard-code these two paths.

Exploratory commands used `git ls-files -z '*.go' | xargs -0 go run` with each
exact module version above and `-ignore '(_test\.go$|^internal/store/internal/runtimepersistence/facade_forwarders_generated\.go$|^internal/runtime/pythonmodule/artifact_manifest_generated\.go$)'`.
Counts use numeric `score >= 30` / `score >= 50`, not the tools' exclusive
`-over` boundary. The eventual per-function collector must also request zero-score
cognitive rows (`-over=-1`) rather than interpret their default omission as
malformed/missing source. These commands produced temporary reports only, not a
checked-in baseline or CI implementation.

| Metric | Functions >=30 | Functions >=50 |
| --- | ---: | ---: |
| gocyclo | 236 | 43 |
| gocognit | 518 | 158 |

These counts are proposed-scope evidence, not an approved baseline and not directly
comparable with the old 195/33 audit without its original tool/scope parameters.
The cited `docs/audits/2026-09-04-rewrite-decision/data/{gocyclo_prhead.txt,salvage_map.csv,README.md}`
are absent from this repo and the freshly fetched docs tree. The original keep
classification remains unverified. This blocks factoring eligibility, not the
repo-wide measurement PR, which changes no classified product function.

| Issue family | Current gocyclo / disposition |
| --- | --- |
| serveapp.Run | 37; its old bulk moved to buildRuntimeComposition (184; gocognit 286). Must explicitly refresh the factoring target, not silently retarget. |
| OperatorAgentConversationHandlers | 82; later family only. |
| validateAdmittedToolInputSchemaActive | 69; later family only. |
| executeDataShowResource | 68; later family only. |
| writeDescribeText | 65; later family only. |
| ClaudeCLIRuntime.continueSession | 66; later family only; provider outcome/lifetime unchanged. |
| PruneOperationResult.Validate | 64; extraction only, no constructor/type-model migration. |
| channelonboarding.Service.drive | Thin wrapper now; driveLocked is 76. Coordinate F/#2241 before any extraction. |
| Executor.execReadResourceData | 59; later family only. |
| decodeWave1FieldNode | No current definition or caller; do not resurrect or select a guessed successor. |
| validateToolSchemaValue | 55; later family only. |
| normalizeProviderTriggerSubject | 54; later family only. |
| scenarioRunner.findDecisionCard | 56; recent mailbox cursor/match-set fixes must remain unchanged. |

R4/R5/R6/R7, root-runtime, selected-fork, facade and replay-support exclusions from
#2447 remain exclusions. Package keep status alone does not override a named
rebuild target. E's new #2407 corpus contribution is independent test work; this
baseline does not change its fixtures, #2407 R1.1 or F/D-owned families.

## Proposed Smallest Implementation

One development-only command, provisionally `cmd/swarm-complexity`, owns collection,
strict baseline encoding/checking and revision comparison. Use pinned upstream
analyzers as tools, not reimplemented AST scoring. Keep the command self-contained
with unexported helpers and focused tests, not a new generic metrics package.
The checked-in deterministic artifact is provisionally `.github/complexity-baseline.json`.
Record schema/policy version, exact tool versions, threshold, exclusions and
per-function metrics with stable path/package/receiver/name identity. Positions
are diagnostics, not identity; generated timestamps or a self-referential HEAD
hash must not force every unrelated PR to rewrite the artifact.

Both analyzer counts at **>=30** must not increase, independently. Report >=50,
maxima and per-function changes, but do not silently impose a new per-function or
total-complexity blocking policy. Equal-count replacement by a worse hotspot is
a disclosed aggregate-rule limitation, not claimed impossible. Renames/moves are
reported as add/remove unless exact identity is retained; no heuristic matching.

The checked-in head artifact must match a fresh head measurement exactly. A PR
must also compare against an independently measured exact base revision using
the same pinned policy, so editing its own baseline cannot bless an increase.
Recompute base evidence even for initial baseline introduction; no unchecked
first-run auto-acceptance or trusted head-provided numeric ceiling. Tool/policy
changes require explicit review and separately comparable measurements, not
automatic baseline reset. No network access beyond ordinary tool provisioning.

One unconditional CI job in `.github/workflows/ci.yml` calls the same command;
`required-tests` includes its outcome. On PR events measure the actual PR head,
not only GitHub's synthetic merge checkout, with the event's exact base SHA.
On push compare exact before/after SHAs; unavailable or invalid comparison facts
fail rather than falling back. Manual/nightly runs validate the committed baseline
against current measured source; they do not manufacture a historical comparison.
The job is independent of changed-package selection and proof profiles. Preserve
existing test-planner execution-SHA semantics by using a separate checkout/job.
Upload the normalized metrics/delta as CI evidence. Repository protection and
reviewer identity policy remain #2407 R1.3, not an admin change made here.

No runtime code, platform semantics, test fixture, product output, exported type,
transaction/lifecycle owner or third-party source is changed. The tooling-line
allowance must be explicitly confirmed by the gate: baseline establishment adds
development tooling, while the tightened net-neutral product-line rule applies
to subsequent factoring PRs. Report both, never disguise tooling as a product
line reduction. No production factoring is included to offset checker size.

## Execution Path, Gates, Owners and Consumption Census

This is a developer-visible CI flow, not a multi-step runtime journey:
checkout exact revisions -> inventory -> pinned analyzer execution -> strict
normalization -> baseline equality -> base/head count comparison -> CI job result
-> required-tests aggregate -> evidence readback. Earlier gates must succeed
before a delta can be called a trustworthy non-increase.

| Seam/gate | Class and owner disposition |
| --- | --- |
| PR head/base selection | Same chosen class; moved to exact revision inputs in the one command/job. Existing ci-plan merge-SHA semantics remain a different execution-planning concept. |
| Tracked-file enumeration and generated/test exclusions | Same chosen class; moved to the command's one inventory policy, consumed identically by collection, update, check and base comparison. |
| gocyclo/gocognit scoring | Already owned by upstream pinned analyzers; collect full output, validate failures and schema, never maintain parallel local scoring. |
| Metric identity/threshold and baseline serialization | Same chosen class; canonical owner missing today and introduced by this work, not an arbitrary first helper. Both local and CI consume it. |
| Local update and check | Same chosen class; moved to command. Update generates evidence but does not grant comparison approval. |
| PR and push checks | Same chosen class; consume the same comparison, no per-event reimplementation. |
| Workflow dispatch and nightly checks | Same chosen class for current baseline validation; different input posture for historical delta, explicitly no fabricated base. |
| Every proof profile and changed-path plan | Same chosen class for non-skipping enforcement; separate unconditional CI job, no profile/path exception. |
| required-tests aggregate | Same chosen class; must require actual complexity-job success, not skip/neutral status. |
| CI artifact and future monthly delta reader | Same measured owner; artifact emitted now, monthly scheduling remains explicitly tracked #2407 R1.5. |
| Branch protection/reviewer identity and production-line gate | Different policy concept, explicitly #2407 R1.3; no GitHub administrative changes in this PR. |
| Golden workload and runtime behavior tests | Different semantic concept, execution evidence owned by releasee2e; unchanged regression requirements, not complexity proof. |
| Historic audit and salvage CSV | Read-only historical evidence, not an executable baseline or factoring permission. |
| Existing test timing/coverage tooling | Different semantic concept: duration and workload coverage; no second complexity consumer found or introduced there. |

There is one workflow today and no existing gocyclo/gocognit command, baseline or
complexity parser in current source. No live sibling checker is left bypassing
the owner. Old historical scores cease being current approval evidence, but
historical artifacts are not deleted or rewritten as if freshly measured.

## Manifestations and Exact Planned Proof

New test names below are commitments, not tests claimed to exist or pass.

| Manifestation | Planned proof |
| --- | --- |
| Host/file discovery changes counts | `TestComplexityTrackedInventory`: tracked nested/platform variants included; untracked and _test excluded; exact generated marker and recorded exclusion cases; absent/empty inventory fails. |
| Wrong threshold or analyzer conflation | `TestComplexityThresholdBoundary`: 29/30/31, independent cyclo/cognit regression and lower/equal controls; >=50 remains reporting. |
| Metric output misparsed or silently empty | `TestComplexityAnalyzerAdmission`: malformed/truncated output, wrong version, missing executable, parse failure and zero-result anomalies fail closed; real pinned-analyzer miniature source controls. |
| Position/ordering churn changes baseline | `TestComplexityCanonicalBaseline`: input order and line shifts deterministic; receiver/path distinctions remain distinct; no timestamp or HEAD-cycle metadata. |
| Stale/inflated/partial head baseline greens PR | `TestComplexityBaselineExactness`: edited ceilings, missing/duplicate/unknown records and policy drift rejected. |
| Self-approved increase or wrong revision | `TestComplexityRevisionRatchet`: disposable real git base/head trees, initial baseline, equal/decrease/increase, baseline self-bump, missing base, and divergent head/merge-tree controls. |
| Same-count worse function hidden by count rule | `TestComplexityDeltaDisclosure`: count-equal change passes the specified aggregate rule but prints exact function increase; do not claim a stronger guarantee. |
| CI skip/profile/aggregate bypass | `TestComplexityCIInvocation` plus actual PR CI run: PR/push/manual/nightly argument fixtures, every profile unconditionally runs, required-tests rejects non-success, artifact exists. |
| Existing workload inadvertently changed | Unchanged golden SQLite smoke, dual-store restart/forced kill and both burst iterations; full `go run ./cmd/swarm-test -- ./... -count=1 -timeout=30m` after implementation. |

Supported runtime proof remains `TestGoldenAgentWorkloadSQLiteSmoke`,
`TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends`,
`TestGoldenAgentWorkloadBurstConcurrencyOnBothBackendsIteration1` and `Iteration2`.
No parity behavior changes; a checker unit test cannot substitute for these
unchanged supported-surface checks. No paid/live-provider proof is required.
Only metrics were executed during this pre-audit; no new test pass is claimed.

## Parent, Tracking, Watchlist and Feasibility

Parent action: keep the explicitly ordered baseline slice. Absorbing hotspot
factoring or broader R1 would mix different acceptance concepts and violate the
issue's baseline-first sequencing. Current-master sibling probing found renamed
serve/onboarding owners and an absent YAML decoder, plus existing lifecycle
defer/cleanup/publication coupling that a numeric-only extraction could damage.

Tracker decision: update #2447 before coding with this first-PR boundary and the
measured successor table, keeping the old audit table labelled historical. #2407
continues to own other R1 rows. No new child issue is required; later PRs remain
separately bounded families under #2447. No older completed issue is reopened.

Map/refine existing watchlist nodes `boundary_owned_decomposition` and
`invariant_suite_coverage`, not a new taxonomy. The former already tracks moves
without test/ownership clarity; the latter tracks issue-local rather than durable
proof. Both support promoting the ratchet now, not absorbing R4-R7 or runtime
ownership redesign. Keep aggregate-metric limitations explicit. #2443/#2250
retain wider activation/resource-lifetime decomposition; this is not their fix.

Estimated remaining tail: #2447 has 12 currently live candidate families after
the absent decoder is removed from the immediate queue; roughly 11-13 factoring
PRs, low confidence until keep-map access, coordination and size checks. #2407 has
four other acceptance rows, independently tracked; E is working on corpus proof.
The first PR must not imply that either parent closes when the baseline lands.

Feasibility: the chosen CI class is closeable in one small tooling PR. A baseline
file alone would leave comparison, event/profile and aggregate interpreters live;
this scope includes all of them. Complexity reduction itself is deliberately not
claimed. No architecture framework or platform-spec rewrite is justified.

## Gate Decisions Required / Stop Conditions

Request independent approval of this baseline-only boundary and these defaults:

1. Tracked authored non-test Go, including tools/support and every build variant;
   generated markers excluded explicitly; both counts >=30 ratcheted, other
   per-function/aggregate metrics reported only.
2. Exact PR-head evidence plus independent base comparison; no self-bumped ceiling,
   no path/profile skipping or baseline auto-reset.
3. Development-tooling-only additions in first PR, no product-code offset or
   factoring; clarify that subsequent <=1500 production-line cap counts additions
   plus deletions and retains net-neutral/negative product lines before extraction.

Factoring stays blocked until the cited salvage evidence is accessible or the lead
explicitly ratifies an updated replacement eligibility table, and the current
serve successor/size boundary is approved. Missing evidence is not replaced by
the observation that two functions occupy the same package.

No Broad Refactor Escalation is requested: this PR makes no runtime refactor and
does not absorb a broader semantic owner. Stop on an inability to provide one
reproducible inventory/comparison, a required product-semantic change, missing
canonical owner/consumer, or any need to weaken existing runtime tests. Re-gate
instead of expanding into R4/R5/R6/R7 or a new lifecycle framework.

Independent gate outcome is **pending**, not self-approved. Publish this audit
and watchlist/tracker repairs, request review, and stop before implementation.

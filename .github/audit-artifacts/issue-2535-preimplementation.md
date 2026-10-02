# Pre-Implementation Coverage Audit: #2535

Date: 2026-10-02. Implementer: agent-g. Phase: audit only; coding frozen pending
reviewer-g's independent gate. Source base:
`ae80bac4fe61c15d60c96189544707d4c1d89d8e` (`origin/master`). Docs read at
`f6b04f2`; follow `IMPLEMENTER_GUIDELINES.md` and `SEMANTIC_DRIFT.md`.

## Class and governing context

- Category: high-risk maintenance / CI qualification and publication authority.
- Observed symptom: fleet CI takes hours predominantly in admission/start wait;
  actual draft PRs run full proof, generated-model PRs get a second automatic full
  dispatch, and both proof matrices admit all rows without per-run caps.
- Exact concepts: native draft versus reviewable qualification; automatic
  generated-PR qualification authority; per-run proof scheduling versus proof
  membership; required aggregate/check identity.
- Chosen working class: `ci_qualification_admission_ignores_native_draft_state_and_duplicates_generated_pr_dispatch_with_unbounded_per_run_proof_matrices`.
- Immediate parent: excessive CI runner demand and unfair per-run admission under
  concurrent ready PRs. Parent above it: complete hosted qualification latency
  under normal fleet load, targeted at 20 minutes (#2535); broader local test
  efficiency remains open in #1196, not a dependency on local box capacity.
- Framing: the new issue is broad enough as a parent. This PR is an explicit
  user-authorized first slice, not a closure of the 20-minute parent. The observed
  publisher shell line and queue snapshot are entry points, not audit boundaries.
- Intended closure: **failure class eliminated for the bounded admission class**.
  Commit to closing all D/B/C/Q rows below, not just one duplicate run. The parent
  retains recommendations 4-6 and fleet acceptance; use `Part of #2535`.

No exact `platform-spec.yaml` section governs GitHub native draft admission,
publisher dispatch multiplicity or `max-parallel`. Binding context is user
approval, #2535, existing workflow/planner/evidence contracts, and #1967's
[independent gate](https://github.com/division-sh/swarm/issues/1967#issuecomment-4948539974)
and [master-only environment ruling](https://github.com/division-sh/swarm/issues/1967#issuecomment-4949417169).
This gate must expressly supersede their requirement for the redundant automatic
publisher full dispatch, **not** their security or protected-proof requirements.
#1967 stays closed; new policy is tracked by #2535.

Adjacent binding product contracts, read in full:
`platform-spec.yaml#test_specification.internal_catalog_conformance.compiled_process_golden_profile`
and `platform-spec.yaml#test_specification.internal_catalog_conformance.compiled_process_full_lifecycle_profile`
(lines 22173-22305): PR profiles retain their SQLite smoke,
continuous full/nightly retain both-store restart/burst/J1-J5, and M24 retains the
complete top-level partition. No profile inclusion, assertion, fixture, backend,
deadline or runtime/platform semantic changes are authorized. Therefore no new
platform-spec product section is proposed by this CI-only maintenance slice.
Historical native-unused and complexity guards remain mandatory for every
**qualifying** profile; their workflow tests must recognize the deliberately
nonqualifying draft posture without weakening any ready/master/manual/nightly
negative. No third-party vendoring, scheduler service, compatibility path,
credential expansion, protection relaxation or broad timing-waiver change.

## Diagnosis and measurement boundary

`issue-2535-ci-baseline.json` records the independently retrieved 12-run sample,
per-run identities and aggregates. Successful PR runs only, six per day, not the
orchestrator's whole population: Sep30 median workflow 22.05min, mean assigned-job
start lag 4.36min, execution 3.41min; Oct2 123.13min / 35.26min / 3.70min.
Workflow duration uses `updated_at - created_at` and can include a few seconds
after terminal completion. Future acceptance instead uses the exact required
summary completion. Start lag includes dependencies/admission and is not pure
runner wait. Missing/queued/canceled jobs do not become successful proof.

The org API says Free/public. Published default is 20 hosted jobs /5 macOS;
support overrides are possible. Independently observed snapshot:20 assigned
(17 Linux/3 macOS),127 queued. The orchestrator's36+6 peak and asserted40 default
are not verified. Never count `started_at == created_at` with no runner as an
executing queued job. This slice does not change capacity or pricing.

#2325 head `1ecb616d5e523e42cfca44026d50cc10e48f2e76` produced PR run
37005125912 (84 API jobs) and dispatch37005125229 (79), both successful, both
created12:10:27UTC. Same head does not mean same checkout/profile: PR plan binds
the synthetic merge; dispatch binds its actual branch SHA. Intentional manual
full remains supported; only the publisher's redundant automatic dispatch is
retired. A distinct earlier agent PR/manual pair spent approximately588 runner
minutes; it is demand evidence, not an instruction to merge their evidence.

Two required soaks each cost approximately17min. A one-slot soak cap would create
a ~34min floor. Proposed experiment: ordinary4 /soak2, keeping all rows and
`fail-fast:false`. Four ordinary slots also impose a nontrivial throughput floor:
caps alone cannot turn the current ~300 runner-minute workload into a20min run.
This is a fairness experiment, not a claimed latency optimization or parent
closure. Measure fleet demand, aggregate throughput, idle-host regression, p50/
p90/max and runner-minutes. A clearly regressing setting must be revised or removed
before calling the experiment useful; a cap-only rollback does not restore the
obsolete automatic dispatch or draft qualification.

## Complete execution path and gates

1. Native PR open/synchronize/reopen/ready_for_review/converted_to_draft, master
   push, explicit dispatch, or trusted schedule enters the existing workflow.
   Native PR draft fact is the only WIP admission input: **same chosen class**.
2. Existing branch/repository/event concurrency cancels only superseded runs of
   the same branch and event family: **same chosen class**. Do not remove the event
   suffix to coalesce unlike manual/PR/nightly proofs. Fork namespaces stay exact.
3. Static/complexity feedback remains runnable for drafts. Drafts do not compile
   the exact-head production seed, acquire test services, expand proof/soak,
   collect native-unused/macOS proof or run semantic/SQLite process journeys:
   **same chosen class**. Feedback success cannot imply qualification.
4. Non-draft/other supported events enter existing ci-plan/profile/root inventory,
   exact-checkout cache seed and digest binding: **different membership concept,
   with proof** `TestResolveProfileCoversEveryEventAndEscalationFamily`,
   `TestForcedPRProfileCannotBypassRouting`, `TestCurrentProofPlansBindActiveRequiredRoots`.
5. Every existing ordinary/soak row runs with capped *scheduling*, not capped row
   count: **same admission class**; exact typed-plan consumption is preserved by
   `TestCIConsumesOnePlanAndCompletePlanBoundEvidence` and evidence negatives.
6. Static/native Linux+Darwin unused/union, SQLite and macOS possession, semantic
   smoke, command timing and complete artifact qualification join:
   **same qualification class**, with existing exact-head negative contracts.
7. Always-present `Required test summary` explicitly refuses draft qualification;
   a ready same-head transition starts a real complete run, and no missing required
   owner is blessed as a draft-related skip: **same chosen class**. Live master
   protection requires summary +SQLite smoke (App15368), strict checks and one
   review. Protection configuration stays unchanged.
8. Successful trusted master schedule may mint the existing restricted App token,
   validate generated-only material diff, update/create one canonical PR and let
   its native PR event qualify: **same generated qualification class**. No-op
   exits without work; publication errors fail closed. Explicit operator dispatch
   remains a different intentional full-profile request with non-required names.
9. Record whole-job costs and fleet latency: admission measurement is in scope;
   physical batching/build reuse, expensive-unit cost and soak *selection* are
   **explicitly split/tracked in #2535 recommendations4-6**. They are not cleared
   by a green admission PR.

Ready work must succeed at planning, exact SHA/digest, every selected proof,
artifact completeness, timing policy, native platform collectors and aggregation
before merge qualification. Do not replace these upstream gates with fabricated
success to earn supported-surface credit.

## Canonical owners and exhaustive systematic consumption

The workflow is the actual executable admission/publication owner, not merely
the first shell helper. Existing typed planner/evaluator own inclusion and proof
truth. No second runnable CI workflow file or automatic publisher dispatch was
found in the repository census.

| Owner / consumer seam | Disposition | Exact boundary / planned proof |
| --- | --- | --- |
| Workflow `on.pull_request` event types, native draft fact | moved to canonical owner in this work | explicit opened/synchronize/reopened/ready_for_review/converted_to_draft; D01-D06 |
| Workflow branch+repo+event concurrency | already consumes the canonical owner | preserve group/cancel semantics; D07-D08; distinct manual runs remain independent |
| `complexity`, `static-checks` | already consumes the canonical owner | unchanged feedback/proof commands, no profile bypass; D01-D08/Q02 |
| `ci-plan` plus cache seed/decode census | moved to canonical owner in this work | skip only nonqualifying drafts; ready output/digest/cache contracts unchanged; D/Q rows |
| `proof-unit`, `mandatory-soak` | moved to canonical owner in this work | admission before strategy expansion, literal caps4/2, unchanged row identity and fail-fast:false; C01-C06 |
| `sqlite-local-dev`, `macos-sqlite-possession`, `semantic-smoke` | moved to canonical owner in this work | draft skips are visibly unqualified; ready supported journeys unchanged; D09/Q02 |
| `unused-linux`, `unused-darwin`, `unused-checks` | moved to canonical owner in this work | draft-only nonqualification; required native-union unchanged on every qualifying event; Q02 |
| `timing-budget` | moved to canonical owner in this work | never download/qualify missing draft plan; complete ready evidence and exact run attempt mandatory; D09/Q03 |
| `required-tests`, live protected context mapping | moved to canonical owner in this work | explicit draft refusal before absent-output interpretation; no context/protection rename; D03-D06/Q02 |
| `publish-timing-model` stable branch/PR and App/environment | moved to canonical owner in this work | remove only automatic full dispatch; existing master/schedule/material-only/generated-only owner preserved; B01-B08 |
| `internal/testplanning/{policy,plan,routing,execution,publication}.go` and `cmd/swarm-test-timing` | already consumes the canonical owner | profiles, inclusion, exact inventory, digest, generated-only validation remain unchanged; Q01/Q03 |
| `internal/testtiming/{jobs,budget}.go` | already consumes the canonical owner | same run/attempt/head/execution profile, all expected jobs exactly once; Q03 |
| `cmd/swarm-test` / `internal/testpostgres` | different semantic concept, with proof | local process/service lifetime and capacity, preserved canonical proof invocation; `TestCIPostgresJobsShareOwnedRunner` |
| `ci_contract_test.go`, `soak_ci_test.go`, `cmd/swarm-unused/workflow_test.go`, `cmd/swarm-complexity/workflow_test.go` | moved to canonical owner in this work | amend exact workflow postures, preserve old qualifying negatives, add draft/cap/publisher execution cases |
| `internal/testutil/postgres_ci_test.go`, `internal/cliapp/{ci_execution_selection,source_root_ci}_test.go`, `internal/testcatalog/inventory_test.go` | already consumes the canonical owner | exact commands/backend source roots/fixture membership remain, no test deletion |
| `internal/testchanged/plan_test.go`, `internal/testplanning/routing_test.go` | different semantic concept, with proof | dependent package/changed-file profile selection, not draft/title interpretation; preserved routing tests |
| Selected-store abstraction guard matrix/tests and cataloge2e README | already consumes the canonical owner | canonical runner and required SQLite smoke references stay accurate; Q02/Q04 |
| Live Actions API `Bundle Hash v1` / `Dependency Graph` workflow records | different semantic concept, with proof | only ci.yml exists at audited source; former historical record and GitHub dynamic graph do not dispatch this qualification; no deletion/disable authorized |
| External agent/orchestrator push/manual commands | still bypasses the canonical owner and is explicitly split / escalated | no checked-in second dispatcher found; local policy asks actual WIP drafts and no checkpoint pushes. Deliberate external manual requests cannot be safely deduplicated by head alone; #2535 demand tracking remains open |
| Branch strict/up-to-date policy and credential provisioning | already consumes the canonical owner | read-only API check; no policy weakening, user quota/support changes or secret reading in this slice |

Census: `rg -l --hidden 'ci.yml|gh workflow run|Full dispatch summary|Required test summary'`
and workflow/planning/timing/profile/soak scans, all14 jobs tabulated. Current API
workflow inventory and protected contexts read back. Open PR #2482 shares ci.yml
only for its authorized Darwin timeout; preserve it. #2525 changes partition
policy; preserve integration and repeat census, not redraw membership silently.

Old invalid paths: automatic publisher `gh workflow run ci.yml --ref ... -f profile=full`;
its positive recurrence assertion; native drafts entering full jobs; absent native
ready/draft transition subscriptions; unlimited proof admission. Surviving valid
paths: explicit operator dispatch with distinct check contexts; separate PR merge
versus branch execution identities; every required proof/plan/artifact, trusted
master schedule, App scope and human review. No legacy dispatcher is retained.

## Manifestation matrix (planned proof, not completed proof)

Status legend: **F** = direct reproducer and fix; **E** = execution proof through
the same corrected path; **S** = split / escalate as a separate class. Each row
names an executable oracle; proposed test names are implementation obligations,
not existing or passing tests. Hosted proof is required where specified.

| ID | Manifestation / owner | Status | Exact planned proof / surface |
| --- | --- | --- | --- |
| D01 | Native draft opened runs full jobs | F | `TestCIDraftAdmissionAndReadyTransitions/opened`; real hosted draft: static/complexity only, no assigned heavy job |
| D02 | Draft synchronize/reopen runs full jobs | F | same test's separate synchronize/reopened event fixtures; hosted draft update and reopen readback |
| D03 | Skipped draft leaf can look green/qualified | E | `TestCIRequiredSummaryRefusesDraftQualification`; execute real aggregate shell with draft fact and skipped/missing outputs; hosted required summary must not succeed |
| D04 | Same-SHA ready_for_review not subscribed | F | explicit native transition fixture and real hosted conversion; full matrix/required checks created despite no commit |
| D05 | Ready synchronize/reopen remains fully qualified | E | separate event fixtures and hosted ready update; all planned jobs, exact execution SHA and aggregate pass |
| D06 | Converted-to-draft leaves obsolete full work alive | F | converted_to_draft fixture and hosted same-branch cancellation; new nonqualified summary, no heavy replacement admission |
| D07 | Same-branch superseded updates continue stale work | E | `TestCIConcurrencyNamespaceIsolation` + hosted latest-run readback; preserve cancel-in-progress |
| D08 | Other agents/forks/manual profiles accidentally canceled | E | exact repository/branch/event fixture matrix; hosted distinct branch control and intentional dispatch remain independent |
| D09 | Missing draft plan attempts matrix expansion/download | E | `TestCIDraftSkipsPlanAndProofExpansion`; parsed job-if/needs plus malformed/absent-output cases before strategy expansion; hosted jobs API records |
| D10 | WIP title/label/author becomes heuristic draft authority | E | native draft false with WIP title remains full; native draft true without WIP title remains unqualified; no actor special case |
| B01 | Changed generated-model PR gets automatic PR plus dispatch | F | `TestGeneratedPublisherUsesOnlyAutomaticPRQualification`; extracted publisher shell with recording gh stub AND restricted App real native-PR run receipt, no automatic dispatch |
| B02 | New generated-model PR creation fails to qualify | E | publisher stub create branch + actual App-created PR history/readback; one qualifying native run, required names unchanged |
| B03 | Existing canonical PR update fails or opens duplicates | E | publisher stub update branch + actual App same-PR synchronize history; one canonical generated-only PR; B01 qualified receipt |
| B04 | No-op model update starts unnecessary work | E | publisher shell unchanged result; gh recorder gets no ref/PR/dispatch calls; existing material-only test retained |
| B05 | Unauthorized generated diff or publication failure falls back | E | `TestGeneratedPublisherFailureIsClosed`; rejected diff and failing ref/PR APIs; no fallback dispatch/PAT/master writes |
| B06 | Publisher gains untrusted event/branch/environment authority | E | `TestPublisherIsMasterRestrictedGeneratedOnlyAndReviewRequired`; workflow/App/environment API readback; no feature-branch credential exposure |
| B07 | Manual full/nightly retired or emits duplicate protected contexts | E | `TestCIManualProfilesRemainIndependent`; profiles + existing Full dispatch summary/SQLite names; actual full dispatch readback, not merged with PR evidence |
| B08 | Dispatch deletion treated as same-tree/profile equivalence | E | `TestRunPlanRejectsWrongExecutionSHA` / `TestWholeJobEvidenceIsExactAndIncludesAllCosts`; real plan profile/SHA inspection distinguishes PR merge from branch |
| C01 | Ordinary matrix expands with unbounded running rows | F | `TestCIMatrixAdmissionCapsPreserveProofInventory`; parsed max-parallel4, fail-fast:false, identical planned row IDs; hosted assigned intervals <=4 |
| C02 | Soak serialized beyond20min or backend lost | E | same cap test requires2 and both backend IDs; `TestMandatorySoakEvidenceRequiresBothFullBackendReceipts`; hosted both unchanged15min proofs overlap |
| C03 | Cap interpreted as truncation of plan or early-fail shortcut | E | `TestCIMatrixAdmissionCapsPreserveProofInventory` plus zero/missing/duplicate/failed row evidence; all logical units remain required after one failure |
| C04 | Caps bypassed for push/manual/nightly | E | all qualifying event fixtures and exact YAML strategies; real retained profile plans, no alternate uncapped matrix |
| C05 | Per-run cap incorrectly called a global quota | E | concurrent distinct-run hosted intervals reported per run and globally; no global lock; record static/macOS/auxiliary jobs separately |
| C06 | Reduced queue headline conceals higher wall time/cost | E | `issue-2535-ci-cap-measurement` receipt: trigger->summary, runner-minutes, p50/p90/max, matching workload/active demand; no20min closure from one pass |
| Q01 | Draft handling changes selected profile/inventory/soak admission | E | full focused testplanning suite; compare current profile plans and declared rows; `TestPRChangeOptionsConservativelyRoutesParityAndSoak` unchanged |
| Q02 | Required native/platform/product owners become optional when ready | E | preserve unused/complexity aggregate status matrices for success/failure/skipped/cancelled/missing; ready/static/macOS/SQLite/semantic smoke and protected-check API readback |
| Q03 | Timing incomplete/wrong-head evidence becomes green | E | `TestEvaluateBudgetRequiresEveryPlanUnitExactlyOnce`, `TestCIJobCollectionWaitsOnlyForTerminalEvidence`, `TestWholeJobEvidenceIsExactAndIncludesAllCosts`; unchanged deadlines and exact-head CI |
| Q04 | Local canonical runner or supported fixture commands drift | E | `TestCIPostgresJobsShareOwnedRunner`, `TestCIExecutionSelectionConsumers`, `TestCIExecutionFixtureCensusExcludesNestedCheckout`; default swarm-test and both-store CI proofs |
| Q05 | Scoped change merges without fresh head qualification | E | final `go run ./cmd/swarm-test`, full normal PR exact-head CI and post-implementation audit; do not credit draft/static feedback as completed qualification |
| T01 | 85 physical jobs/duplicate subprocess builds amplify overhead | S | #2535 recommendation4: inspect cache/compiler/process-harness receipts; separate owner/measurement slice, no first-PR performance claim |
| T02 | Unused/pipeline/CLI/serve critical-path work remains long | S | #2535 recommendation5 + existing #2353/#2394; retain measured command receipts and specific authorized budgets |
| T03 | Audit/model/non-Go deltas conservatively select mandatory soaks | S | #2535 recommendation6; existing PRChangeOptions tests are valid until new independent policy approval; no skip/drop now |

Generic failing proof: current YAML has no ready/draft event handling/caps and
requires publisher dispatch, while real #2325 same-head PR+manual runs duplicate
qualification. Create an execution-oriented event/status/publisher shell matrix
that goes red on those admissions. String absence alone is insufficient.

Official event semantics used for the design:
[native PR activity types](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull_request),
[skipped required jobs report success](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-jobs-with-conditions),
[App events trigger automatic workflows](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow),
and [published hosted concurrency defaults](https://docs.github.com/en/actions/reference/limits).
The actual paired generated-PR receipts, not these docs alone, establish the live
automatic PR path in this repository.

## Parent probe, tracker state and watchlist promotion

Mapped existing node: `maintenance-and-cleanup.yaml#harness_reliability_and_local_smoke`.
It already covers timing/evidence completeness, generated publication identity,
native analysis, cache/setup, job granularity, backend acquisition and local
capacity. Those are live siblings, not dismissed as shared-owner coverage.

Parent sibling probe outcomes:
- Admission/publisher multiplicity: **broken now**, baseline workflow+actual paired
  run receipts; absorbed entirely by this bounded class.
- Native analysis/build/cache and ordinary job proliferation: **still unproven**
  as optimizations, actual long-job evidence exists; recommendations4-5 stay here.
- Mandatory soak inclusion: **different class, with proof** routing.go owns it;
  conservative selection is intentional today, policy change recommendation6.
- Exact proof identity and fail-closed evidence: **apparently clean, with proof**
  focused baseline contracts passed; preserve all qualifying-path negatives.
- Local12-core scheduling/Postgres capacity: **different class, with proof** local
  runner/testpostgres owners; #1196/#1702 are separate from hosted queue admission.
- Hosted quota/protection/agent push frequency: **still unproven** as fleet-level
  changes; read-only measurements only, not a new scheduler/protection bypass.

Watchlist-backed promotion: the node proves physical scheduling and test cost
must stay visible. Absorbing them now would invent optimization/proof-selection
contracts without profiling and violate the user's focused-first-PR approval.
Post-pre-audit parent action: **keep first-slice scope; parent explicitly open**.
Estimated tail3-5 slices, medium-low confidence: build/job reuse; one or more
measured long-unit optimizations; reviewed soak admission; fleet acceptance.
All recommendation4-6 obligations remain assignable in #2535; #2353/#2394 keep
their existing defect/performance authority. No untracked same-owner bypass is
left in the bounded slice.

Tracker-state decision: **current issue remains correct as written**, with the
explicit 20min parent/first-slice split. Refine existing watchlist now, not create
a new node or POTENTIAL_ISSUES entry. #1967 historic dispatch condition is
superseded only if the new independent gate explicitly approves it; no reopening.

Architecture feedback: CI currently conflates work-in-progress feedback with
qualification admission and duplicates publication demand. Promote removal using
the existing workflow/planner/evaluator, not another dispatcher. Track remaining
physical versus logical unit cost in #2535 and its watchlist; no new architecture
issue. Effort: first slice~1-2 engineering days plus hosted gate/operational proof;
parent~3-7 days plus measurement, low-medium confidence. High ROI from removing
duplicated/generated and draft demand; caps' ROI is empirical, not established.

## Feasibility, supported proof and stop conditions

Can the chosen class close in one PR? Yes, one executable workflow owner,
associated contract tests and no new runtime model. A local publisher-only fix
would leave draft and unbounded matrix admissions live; all are included here.
Can it prove20min parent closure? **No**, explicitly not claimed.

Supported surfaces: actual GitHub native draft/ready transitions; real generated
App PR qualification; explicit manual dispatch; required context/protection API;
ordinary and both-store mandatory soak actual hosted execution. Local shell/YAML
fixtures earn mechanics credit, not App credentials/hosted scheduling credit.

Master-only scheduled publisher cannot run the edited literal environment job
pre-merge without violating its trust boundary. Before merge, prove native App
PR event ownership and generated-only branch/create/update/no-op behavior using
existing authorized receipts/controlled probes. Reviewer must explicitly decide
whether a bounded post-merge scheduled literal-job acceptance is necessary;
record it in #2535 and keep generated publication acceptance open until that
receipt, with immediate disable/repair on failure. Never temporarily expand the
environment to a feature branch. No credential reading is required for this
pre-audit. Any live App write must use existing permitted publication authority,
not a human/PAT/GITHUB_TOKEN substitute.

Stop/re-gate if another automatic full-qualification producer appears, policy
selection/required proof must change, protected context mapping must weaken, App
trigger proof is unavailable, cap measurement requires new cross-run ownership,
or publisher acceptance would require a broader security exception. Do not
silently broaden scope. An empirical cap regression alone requires rejecting or
retuning the experiment under recorded evidence, not changing test deadlines.

Baseline verification run on the audited source: focused CI job collection,
plan/evidence consumption, restricted publisher, exact soak/shell workflow,
native-unused union and complexity aggregate tests **passed**. This proves
preserved baseline contracts, not new D/B/C fixes. New fixture/mutation/hosted
qualification and default swarm-test **not run** in pre-audit.

Independent gate: **requested, not recorded/approved yet**. Implementation must
remain frozen until reviewer-g posts the explicit first-slice gate and addresses
the historical publisher-dispatch/security acceptance sequencing conditions.

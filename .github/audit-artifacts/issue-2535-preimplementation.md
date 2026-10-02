# Pre-Implementation Coverage Audit: #2535

## Implementation Status: Updated-Ready Checkpoint

The historical pre-audit below records the frozen cycle-1/cycle-2 preparation;
that freeze was superseded by the independent cycle-2 first-slice approval
5957437668. Recommendations1-2 are implemented, not review-ready yet. No cap,
profile/membership/budget/protection change or parent closure is authorized.

Master integration is now `412194d721cfbbcda889780e6c864b3abedb1051`; its compound
fixture/owner changes are preserved. Rebased focused controls and the exact
complexity ratchet pass. The only baseline delta is the excluded new test file;
production scores remain identical to this base.

D11 has actual hosted proof on unchanged head
`136953775f3050c5a3f1eda3f1c5c5a70f2a21a2`: draft run37051806562 fails its explicit
nonqualification summary with green static/complexity, then ready run37052855349
fully succeeds, attempt1/check-suite100366942795. The plan has77 units,
profilepr-escalated, execution merge`2b030e09a04b4eec51b2207afe84561c71e44d4c`,
digest`7b912ca39e26f3775200144cceaa29e5820e74aec540adbb13f37bb8ffba9ec8`.
GitHub's actual head rollup is SUCCESS, both protected contexts pass, the branch
is up-to-date/conflict-free and blocked only by REVIEW_REQUIRED. Required summary
and SQLite local smoke retain App15368, strict protection and ordinary review.
The failed draft checks are retained, not rewritten or rerun.

Earlier draft-open/sync/reopen receipts37040537258/37045162194/37045457039 remain
controls. First ready run37048233179 was canceled for base integration after both
soaks passed; it is not whole-run/protected-acceptance proof. The first local
qualification failed with host ENOSPC; its replacement passed three units before
graceful cancellation for rebase, not a whole-suite pass. The omitted generated
test-file fact was repaired and stale derived cache trimmed; no runtime change.

This audit-only update is the required ready-update entrance. Updated-ready green,
same-head converted-draft refusal despite that retained green, final same-head
ready qualification, fresh default swarm-test and the final PR proof audit remain
mandatory. No literal edited master-only publisher execution is claimed; #2535
retains that post-merge acceptance and recommendations3-6/the20-minute fleet goal.

## Historical Pre-Audit

Date: 2026-10-02. Implementer: agent-g. **Repaired gate cycle 2.** Phase: audit
only; coding frozen after the [cycle-1 insufficient/widen-split ruling](https://github.com/division-sh/swarm/issues/2535#issuecomment-5957050174).
This version supersedes the original first-PR class and cap acceptance, not the
historical baseline evidence. Source base:
`c3f38293e16cebc09b552447cb6235f600692d12` (`origin/master`, merged #2482).
Docs read at `7cf2086`; follow `IMPLEMENTER_GUIDELINES.md` and `SEMANTIC_DRIFT.md`.

## Class and governing context

- Category: high-risk maintenance / CI qualification and publication authority.
- Observed symptom: fleet CI takes hours predominantly in admission/start wait;
  actual draft PRs run full proof and generated-model PRs get a second automatic
  full dispatch. Matrix fairness is a separate, unproven performance hypothesis.
- Exact concepts: native draft versus reviewable qualification; automatic
  generated-PR qualification authority; required aggregate/check identity and
  same-SHA native draft-to-ready protected-check acceptance. Physical scheduling
  does not change logical proof completeness and is not this first-PR concept.
- Chosen working class: `ci_full_qualification_admits_native_drafts_and_redundantly_dispatches_automatic_generated_pr_qualification`.
- Immediate parent: avoidable CI runner demand under concurrent ready PRs.
  Parent above it: complete hosted qualification latency
  under normal fleet load, targeted at 20 minutes (#2535); broader local test
  efficiency remains open in #1196, not a dependency on local box capacity.
- Framing: the new issue is broad enough as a parent. This PR is an explicit
  user-authorized first slice, not a closure of the 20-minute parent. The observed
  publisher shell line and queue snapshot are entry points, not audit boundaries.
- Intended closure: **failure class eliminated for the bounded admission class**.
  Commit to closing all 24 D/B/Q rows below, not just one duplicate run. C01-C06
  become a separately gated, non-closure-bearing scheduling experiment under the
  open parent. The parent retains recommendations 3-6 and fleet acceptance;
  use `Part of #2535`. No cap implementation or cap change is part of this PR.

No exact `platform-spec.yaml` section governs GitHub native draft admission,
publisher dispatch multiplicity. Binding context is user
approval, #2535, existing workflow/planner/evidence contracts, and #1967's
[independent gate](https://github.com/division-sh/swarm/issues/1967#issuecomment-4948539974)
and [master-only environment ruling](https://github.com/division-sh/swarm/issues/1967#issuecomment-4949417169).
This gate must expressly supersede their requirement for the redundant automatic
publisher full dispatch, **not** their security or protected-proof requirements.
#1967 stays closed; new policy is tracked by #2535. Cycle-1 ruling directs the
split and in-principle retirement; fresh coding approval is still required.

Adjacent binding product contracts, read in full:
`platform-spec.yaml#test_specification.internal_catalog_conformance.compiled_process_golden_profile`
and `platform-spec.yaml#test_specification.internal_catalog_conformance.compiled_process_full_lifecycle_profile`
(lines 22709-22841 on the repaired base): PR profiles retain their SQLite smoke,
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

`issue-2535-cap-feasibility.json` independently re-reads both review receipts:
run37008266512 has70 ordinary proof jobs /217.32 runner-minutes; run36759529305
has69 /215.68. At four simultaneous slots, their work/cap floors are54.33 and
53.92min. The quieter uncapped whole run finished~22.3min. Any cap<=10 has a
>21.5min ordinary floor before planning, other gates or queueing. Thus the literal
four-slot proposal is rejected, not merely uncertain ROI. A lower queue metric
cannot justify a predictable wall-time regression. `max-parallel` cannot reduce
runner-minutes of actual work. Two soaks each cost~17min, so serializing them also
imposes an unacceptable~34min floor.

**First PR changes no matrix caps, strategies, row membership or fail-fast mode.**
C01-C06 remain a separately tracked recommendation3 experiment under #2535. Any
future candidate needs its own independent gate, a quantified work/cap floor
compatible with20min after realistic nonmatrix cost, matched fleet-demand and
quiet-host controls, and p50/p90/max/throughput/runner-minute acceptance. No
candidate number is approved here. Reverting a regressing cap earns no cap-class
closure; preserving all proof is not proof of scheduling value.

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
5. Every existing ordinary/soak row retains unchanged physical scheduling and row
   membership: **different scheduling concept, explicitly split in C01-C06**;
   exact typed-plan consumption is preserved by
   `TestCIConsumesOnePlanAndCompletePlanBoundEvidence` and evidence negatives.
6. Static/native Linux+Darwin unused/union, SQLite and macOS possession, semantic
   smoke, command timing and complete artifact qualification join:
   **same qualification class**, with existing exact-head negative contracts.
7. Always-present `Required test summary` explicitly refuses draft qualification;
   a ready same-head transition starts a real complete run. That later green run
   must satisfy the actual protected rollup despite the retained failed draft run
   on that SHA, with the PR blocked only by ordinary review; D11 is required.
   No missing required owner is blessed as a draft-related skip:
   **same chosen class**. Live master
   protection requires summary +SQLite smoke (App15368), strict checks and one
   review. Protection configuration stays unchanged.
8. Successful trusted master schedule may mint the existing restricted App token,
   validate generated-only material diff, update/create one canonical PR and let
   its native PR event qualify: **same generated qualification class**. No-op
   exits without work; publication errors fail closed. Explicit operator dispatch
   remains a different intentional full-profile request with non-required names.
9. Record whole-job costs and fleet latency: admission measurement is in scope;
   matrix-cap feasibility/tuning, physical batching/build reuse, expensive-unit
   cost and soak *selection* are **explicitly split/tracked in #2535
   recommendations3-6**. They are not cleared
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
| Workflow `on.pull_request` event types, native draft fact | moved to canonical owner in this work | explicit opened/synchronize/reopened/ready_for_review/converted_to_draft; D01-D06/D11 |
| Workflow branch+repo+event concurrency | already consumes the canonical owner | preserve group/cancel semantics; D07-D08; distinct manual runs remain independent |
| `complexity`, `static-checks` | already consumes the canonical owner | unchanged feedback/proof commands, no profile bypass; D01-D08/Q02 |
| `ci-plan` plus cache seed/decode census | moved to canonical owner in this work | skip only nonqualifying drafts; ready output/digest/cache contracts unchanged; D/Q rows |
| `proof-unit`, `mandatory-soak` | moved to canonical owner in this work | draft exclusion before strategy expansion, unchanged ready strategy/row identity/fail-fast:false; D09/Q01; physical cap tuning is separately split C01-C06 |
| `sqlite-local-dev`, `macos-sqlite-possession`, `semantic-smoke` | moved to canonical owner in this work | draft skips are visibly unqualified; ready supported journeys unchanged; D09/Q02 |
| `unused-linux`, `unused-darwin`, `unused-checks` | moved to canonical owner in this work | draft-only nonqualification; required native-union unchanged on every qualifying event; Q02 |
| `timing-budget` | moved to canonical owner in this work | never download/qualify missing draft plan; complete ready evidence and exact run attempt mandatory; D09/Q03 |
| `required-tests`, live protected context mapping and same-SHA check-suite/PR rollup | moved to canonical owner in this work | explicit draft refusal before absent-output interpretation; new ready run must satisfy actual protected contexts despite historical draft failure; no context/protection rename; D03-D06/D11/Q02 |
| `publish-timing-model` stable branch/PR and App/environment | moved to canonical owner in this work | remove only automatic full dispatch; existing master/schedule/material-only/generated-only owner preserved; B01-B08 |
| `internal/testplanning/{policy,plan,routing,execution,publication}.go` and `cmd/swarm-test-timing` | already consumes the canonical owner | profiles, inclusion, exact inventory, digest, generated-only validation remain unchanged; Q01/Q03 |
| `internal/testtiming/{jobs,budget}.go` | already consumes the canonical owner | same run/attempt/head/execution profile, all expected jobs exactly once; Q03 |
| `cmd/swarm-test` / `internal/testpostgres` | different semantic concept, with proof | local process/service lifetime and capacity, preserved canonical proof invocation; `TestCIPostgresJobsShareOwnedRunner` |
| `ci_contract_test.go`, `soak_ci_test.go`, `cmd/swarm-unused/workflow_test.go`, `cmd/swarm-complexity/workflow_test.go` | moved to canonical owner in this work | amend exact draft/publisher workflow postures, preserve every ready strategy and negative; no cap test/implementation added here |
| `internal/testutil/postgres_ci_test.go`, `internal/cliapp/{ci_execution_selection,source_root_ci}_test.go`, `internal/testcatalog/inventory_test.go` | already consumes the canonical owner | exact commands/backend source roots/fixture membership remain, no test deletion |
| `internal/testchanged/plan_test.go`, `internal/testplanning/routing_test.go` | different semantic concept, with proof | dependent package/changed-file profile selection, not draft/title interpretation; preserved routing tests |
| Selected-store abstraction guard matrix/tests and cataloge2e README | already consumes the canonical owner | canonical runner and required SQLite smoke references stay accurate; Q02/Q04 |
| Live Actions API `Bundle Hash v1` / `Dependency Graph` workflow records | different semantic concept, with proof | only ci.yml exists at audited source; former historical record and GitHub dynamic graph do not dispatch this qualification; no deletion/disable authorized |
| External agent/orchestrator push/manual commands | still bypasses the canonical owner and is explicitly split / escalated | no checked-in second dispatcher found; local policy asks actual WIP drafts and no checkpoint pushes. Deliberate external manual requests cannot be safely deduplicated by head alone; #2535 demand tracking remains open |
| Branch strict/up-to-date policy and credential provisioning | already consumes the canonical owner | read-only API check; no policy weakening, user quota/support changes or secret reading in this slice |

Census: `rg -l --hidden 'ci.yml|gh workflow run|Full dispatch summary|Required test summary'`
and workflow/planning/timing/profile/soak scans, all14 jobs tabulated. Current API
workflow inventory and protected contexts read back. #2482 merged into the new
audit base: its Darwin timeout19, partition and timing-budget/test guards are
preserved; repeat focused controls. #2525 changes partition policy; preserve
integration and repeat census, not redraw membership silently.

Old invalid paths: automatic publisher `gh workflow run ci.yml --ref ... -f profile=full`;
its positive recurrence assertion; native drafts entering full jobs; absent native
ready/draft transition subscriptions. Surviving valid
paths: explicit operator dispatch with distinct check contexts; separate PR merge
versus branch execution identities; every required proof/plan/artifact, trusted
master schedule, App scope and human review, and unchanged uncapped matrix
scheduling pending the separate measured experiment. No legacy dispatcher is
retained. No qualification semantics are inferred from a shared head alone.

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
| D06 | Converted-to-draft leaves obsolete full work alive or a stale green qualification | F | converted_to_draft fixture and hosted same-SHA conversion after a green ready/update run; obsolete work canceled, current required-summary refusal recorded, PR/check-suite/protected rollup readback proves no older green run permits merge; no heavy replacement admission |
| D07 | Same-branch superseded updates continue stale work | E | `TestCIConcurrencyNamespaceIsolation` + hosted latest-run readback; preserve cancel-in-progress |
| D08 | Other agents/forks/manual profiles accidentally canceled | E | exact repository/branch/event fixture matrix; hosted distinct branch control and intentional dispatch remain independent |
| D09 | Missing draft plan attempts matrix expansion/download | E | `TestCIDraftSkipsPlanAndProofExpansion`; parsed job-if/needs plus malformed/absent-output cases before strategy expansion; hosted jobs API records |
| D10 | WIP title/label/author becomes heuristic draft authority | E | native draft false with WIP title remains full; native draft true without WIP title remains unqualified; no actor special case |
| D11 | Failed draft required context on a SHA prevents later valid same-SHA ready qualification | E | `TestCISameSHARequiredContextTransition` mechanics plus real hosted draft-failed -> same-SHA ready-full-green: retain failed run, record unchanged headRefOid, exact run/check-suite and merge execution identities, App15368 required contexts and PR protected rollup; strict up-to-date/conflict-free PR must be blocked only by ordinary human review, not stale draft failure. Merely launching/existing green jobs is insufficient. |
| B01 | Changed generated-model PR gets automatic PR plus dispatch | F | `TestGeneratedPublisherUsesOnlyAutomaticPRQualification`; extracted publisher shell with recording gh stub AND restricted App real native-PR run receipt, no automatic dispatch |
| B02 | New generated-model PR creation fails to qualify | E | publisher stub create branch + actual App-created PR history/readback; one qualifying native run, required names unchanged |
| B03 | Existing canonical PR update fails or opens duplicates | E | publisher stub update branch + actual App same-PR synchronize history; one canonical generated-only PR; B01 qualified receipt |
| B04 | No-op model update starts unnecessary work | E | publisher shell unchanged result; gh recorder gets no ref/PR/dispatch calls; existing material-only test retained |
| B05 | Unauthorized generated diff or publication failure falls back | E | `TestGeneratedPublisherFailureIsClosed`; rejected diff and failing ref/PR APIs; no fallback dispatch/PAT/master writes |
| B06 | Publisher gains untrusted event/branch/environment authority | E | `TestPublisherIsMasterRestrictedGeneratedOnlyAndReviewRequired`; workflow/App/environment API readback; no feature-branch credential exposure |
| B07 | Manual full/nightly retired or emits duplicate protected contexts | E | `TestCIManualProfilesRemainIndependent`; profiles + existing Full dispatch summary/SQLite names; actual full dispatch readback, not merged with PR evidence |
| B08 | Dispatch deletion treated as same-tree/profile equivalence | E | `TestRunPlanRejectsWrongExecutionSHA` / `TestWholeJobEvidenceIsExactAndIncludesAllCosts`; real plan profile/SHA inspection distinguishes PR merge from branch |
| C01 | Proposed ordinary cap makes target infeasible / fairness hypothesis unproven | S | Separate #2535 recommendation3 experiment: quantified work/cap floor and target-compatible candidate before its own gate. Current four-slot floor54.33/53.92min is rejection evidence, not first-PR cap code or proof. |
| C02 | Future cap serializes required soaks or loses a backend | S | Separate recommendation3: candidate must preserve both unchanged ~17min cells and require real hosted overlap/full backend receipts; no soak cap or selection change in this PR. |
| C03 | Future physical cap conflated with logical truncation/early-fail shortcut | S | Separate recommendation3: candidate inventory/strategy negatives and all-row hosted evidence before approval. This first PR preserves current strategies and Q01/Q03. |
| C04 | Future scheduling experiment silently changes push/manual/nightly posture | S | Separate recommendation3: explicit per-event candidate design/proof and no qualified-path bypass; current scheduling remains unchanged in every posture. |
| C05 | Per-run cap incorrectly claimed as a global quota | S | Separate recommendation3: assigned intervals per run versus global auxiliary/matrix occupancy under concurrent PR demand; no global lock and no capacity assumption. |
| C06 | Lower queue headline conceals deterministic wall-time/cost regression | S | Separate recommendation3: pre-merge quiet-host and matched-load p50/p90/max/throughput/runner-minute comparisons, exact required-summary endpoint, reject regression; rejected/reverted caps earn no closure. |
| Q01 | Draft handling changes selected profile/inventory/soak admission | E | full focused testplanning suite; compare current profile plans and declared rows; `TestPRChangeOptionsConservativelyRoutesParityAndSoak` unchanged |
| Q02 | Required native/platform/product owners become optional when ready | E | preserve unused/complexity aggregate status matrices for success/failure/skipped/cancelled/missing; ready/static/macOS/SQLite/semantic smoke and protected-check API readback |
| Q03 | Timing incomplete/wrong-head evidence becomes green | E | `TestEvaluateBudgetRequiresEveryPlanUnitExactlyOnce`, `TestCIJobCollectionWaitsOnlyForTerminalEvidence`, `TestWholeJobEvidenceIsExactAndIncludesAllCosts`; unchanged deadlines and exact-head CI |
| Q04 | Local canonical runner or supported fixture commands drift | E | `TestCIPostgresJobsShareOwnedRunner`, `TestCIExecutionSelectionConsumers`, `TestCIExecutionFixtureCensusExcludesNestedCheckout`; default swarm-test and both-store CI proofs |
| Q05 | Scoped change merges without fresh head qualification | E | final `go run ./cmd/swarm-test`, full normal PR exact-head CI and post-implementation audit; do not credit draft/static feedback as completed qualification |
| T01 | 85 physical jobs/duplicate subprocess builds amplify overhead | S | #2535 recommendation4: inspect cache/compiler/process-harness receipts; separate owner/measurement slice, no first-PR performance claim |
| T02 | Unused/pipeline/CLI/serve critical-path work remains long | S | #2535 recommendation5 + existing #2353/#2394; retain measured command receipts and specific authorized budgets |
| T03 | Audit/model/non-Go deltas conservatively select mandatory soaks | S | #2535 recommendation6; existing PRChangeOptions tests are valid until new independent policy approval; no skip/drop now |

Generic failing proof: current YAML has no ready/draft event handling and
requires publisher dispatch, while real #2325 same-head PR+manual runs duplicate
qualification. Create an execution-oriented event/status/publisher shell matrix
that goes red on those admissions. String absence alone is insufficient.

Official event semantics used for the design:
[native PR activity types](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull_request),
[skipped required jobs report success](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-jobs-with-conditions),
[App events trigger automatic workflows](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow),
and [published hosted concurrency defaults](https://docs.github.com/en/actions/reference/limits).
[Actual required-check acceptance](https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/troubleshooting-required-status-checks)
governs D11, and [max-parallel semantics](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/run-job-variations)
establish the work/cap floor for the separately rejected proposal.
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
- Cap4 hypothesis: **broken now as proposed**, independently reproduced physical
  floor>53min; proposal withdrawn. Future target-compatible tuning is **still
  unproven** and independently gated under recommendation3, not admission closure.
- Same-SHA protected-context transition: **still unproven**, now explicitly D11;
  actual hosted protected rollup is mandatory, not inferred from a newer green run.
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
Estimated tail4-6 slices, medium-low confidence: separate cap investigation;
build/job reuse; one or more measured long-unit optimizations; reviewed soak
admission; fleet acceptance. The added slice follows the independent split, not
a new framework. All recommendation3-6 obligations remain assignable in #2535;
#2353/#2394 keep
their existing defect/performance authority. No untracked same-owner bypass is
left in the bounded slice.

Tracker-state decision: **current issue must be updated before coding**; repair
completed with recommendations1-2 first PR, C01-C06 recommendation3 experiment,
D11 hosted protected acceptance, quantified cap-floor correction and the
publisher acceptance sequence. Refine the same watchlist node before requesting
the fresh gate, not a new node or POTENTIAL_ISSUES entry. #1967 historic dispatch
condition is ratified only in principle by cycle1; coding still requires fresh
gate approval. No reopening, new child issue or hidden scheduling obligation.

Architecture feedback: CI currently conflates work-in-progress feedback with
qualification admission and duplicates publication demand. Promote removal using
the existing workflow/planner/evaluator, not another dispatcher. **Logical proof
completeness is not physical scheduling value**; track them separately in #2535
and its watchlist. No new architecture issue. Effort: first slice~1-2 engineering
days plus hosted gate/operational proof; parent~3-7 days plus measurement,
low-medium confidence. High ROI from removing demonstrated duplicate/draft demand;
future cap ROI is empirical and the withdrawn cap is a demonstrated regression.

## Feasibility, supported proof and stop conditions

Can the chosen class close in one PR? Yes, one executable workflow owner,
associated contract tests and no new runtime model. A local publisher-only fix
would leave draft admission/protected-check handling live; both are included here.
Uncapped physical scheduling is not labeled a proven same-class defect and is
unchanged pending the separate gated experiment.
Can it prove20min parent closure? **No**, explicitly not claimed.

Supported surfaces: actual GitHub native draft/ready transitions; real generated
App PR qualification; explicit manual dispatch; required context/protection API;
ordinary and both-store mandatory soak actual hosted execution, **and D11's
same-SHA actual protected-check acceptance**. Local shell/YAML
fixtures earn mechanics credit, not App credentials/hosted scheduling credit.

Required hosted D11/D06 sequence: draft-failed -> same-SHA ready-full-green ->
updated-ready-green -> same-SHA converted-draft-refusal -> final ready-full-green.
At each boundary retain PR/head/base/run/attempt/check-suite/App/context and
actual executed merge SHA, required-status/protection readback, draft state,
current rollup and merge/review classification. Preserve the old red draft checks
and old green ready checks as controls; do not delete/rewrite/rerun them to fake
acceptance. With an up-to-date conflict-free base and no unresolved non-review
requirements, the ready posture must have accepted protected checks and remain
blocked only by ordinary review. If base drift obscures this, restore the normal
up-to-date precondition without pretending a different head proves the original
same-SHA transition. Converted draft must not permit merge via its older green
run. End with a normal reviewable PR and complete exact-head qualification.

Master-only scheduled publisher cannot run the edited literal environment job
pre-merge without violating its trust boundary. The repaired issue explicitly
records the cycle1-permitted bounded acceptance sequence for fresh gate approval:
pre-merge execute every unchanged shell/security/create/update/no-op/failure branch
and establish native App event ownership; after merge inspect the **first literal
scheduled master execution with the edited workflow** for one canonical App PR
update/create when material, one automatic qualifying PR run, zero automatic full
dispatch and no fallback. A no-op proves zero work but leaves material-update
acceptance open until the next material scheduled execution. #2535's publication
acceptance stays open until that receipt; immediately disable/repair publication
if it fails. Literal edited environment-job success is not claimed pre-merge.
Never expand the environment to a feature branch or substitute human/PAT/
GITHUB_TOKEN authority. Fresh gate must ratify this sequencing before merge.

Stop/re-gate if another automatic full-qualification producer appears, policy
selection/required proof must change, protected context mapping must weaken, App
trigger proof is unavailable, cap measurement requires new cross-run ownership,
or publisher acceptance would require a broader security exception. D11 failure
to satisfy actual protected contexts despite green jobs also requires a new
ruling; no context/protection weakening. Do not silently broaden scope. Any cap
proposal is outside this first PR and requires a separate measured gate.

Baseline verification repeated on the repaired c3f38293e source: focused CI job collection,
plan/evidence consumption, restricted publisher, exact soak/shell workflow,
native-unused union and complexity aggregate tests **passed**. This proves
preserved baseline contracts, not new D/B fixes. New fixture/mutation/hosted
qualification and default swarm-test **not run** in pre-audit.

Independent gate: cycle1 **insufficient; widen/split class**; cycle2
**approved as first slice**, recorded at
https://github.com/division-sh/swarm/issues/2535#issuecomment-5957437668.
Only recommendations1-2 are authorized; D11/D06 actual hosted protection proof
and the explicit master-only post-merge publisher acceptance sequence are
binding. Recommendations3-6 and the parent target remain open. No cap setting,
new framework, security exception or proof-selection change is authorized.

Implementation characterization: new execution-oriented admission/summary and
publisher-shell tests failed against the original workflow (missing native event
subscriptions, draft plan admission, missing draft refusal and redundant
dispatch). With the bounded workflow correction, focused timing/planner/native
unused/complexity controls pass; timing/admission/publisher race tests pass.
This is local mechanics proof only. Default swarm-test and real hosted
draft/ready/protection proof are pending; no review-ready or parent closure claim.

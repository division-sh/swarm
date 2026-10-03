# Pre-Implementation Coverage Audit: #2544

Agent-g, 2026-10-03. Source baseline: merged #2537,
`0fa24140aaa05e397509f451ba780fef3a260867`. Audit-only: no workflow,
planner, runtime, test assertion, dependency or authoritative spec implementation.
Independent coding gate: **requested, not yet granted**.

## Lead Disposition And Class Model

The user-lead accepted delayed exhaustive discovery, then explicitly replaced
affected-owner selection with a fixed census/profile split. The reviewer must
choose local and CI requirements independently; profile selection must consume
that verdict automatically. Multiple cumulative tiers are allowed where the
census establishes a meaningful coverage/cost difference. This is permission to
finish the audit, not independent coding approval.

- Category: high-risk qualification maintenance / semantic ownership / parity.
- Symptom: ready qualifications consume approximately 339-347 assigned
  runner-minutes, so concurrent agents primarily wait for hosted runners. Added
  audit files select two 15-minute soaks and parity supplements independently of
  their semantic role; builds/setup repeat across approximately 77 proof jobs.
- Concepts: fixed proof scope; reviewer-required local versus hosted scope;
  exhaustive versus deliberately deferred coverage; exact source/tool/attempt
  evidence; logical proof versus physical job ownership; verified merge evidence.
- Chosen class: **qualification scope and physical execution are broad,
  duplicated, and inconsistently owned across local/CI admission, profile
  consumers, build products, jobs, aggregation and verified master replay**.
  One bounded #2544 PR must migrate every interpreter of that class.
- Immediate parent: #2535, multi-agent qualification demand exceeds the fleet
  capacity needed for a 20-minute wall target.
- Wider parent: #1196, harness/test performance and explicit lifecycle proof;
  #2353 owns health/failed receipts and #2394 owns runtime fan-out performance.
  All three were independently verified OPEN.
- Framing: broad enough after issue/watchlist repair. This is not a one-helper
  routing fix. The observed `PRChangeOptions` helper is an entry point, not the
  audit boundary. No changed-owner or import-graph selector will be implemented
  for this CI slice. The existing local changed-test command remains separate.
- Intended closure: **failure class eliminated** for the chosen qualification
  class, with separately stated post-merge operational acceptance. No #2535
  fleet, #2394 runtime, live-provider or test-health closure is claimed.

## Binding Context And Spec Plan

Read full #2544 body/thread, the current #2535 body and capacity/consolidation
records 5962605706, 5962855028, 5962922567 and 5962978859, #2537 closure, the
current #2394 body, original soak ruling 5743323725, and #1967's inventory /
publisher contract. Re-read IMPLEMENTER_GUIDELINES.md and SEMANTIC_DRIFT.md.

Exact authoritative adjacent paths on this source:

- `test_specification.internal_catalog_conformance.compiled_process_numeric_feed_profile`
  (22694): typed 100-row public creation, permanent receipt, paused interrupted
  work/restart/continue, exact routing and refusal; no inferred gather/live credit.
- `test_specification.internal_catalog_conformance.compiled_process_golden_profile`
  (22712): N=2 SQLite smoke versus both-store restart and two race N=10 bursts;
  H/T/L distinction, unchanged payload/routing/terminal assertions.
- `test_specification.internal_catalog_conformance.compiled_process_full_lifecycle_profile`
  (22753), M24 (22830), `.profiles` (22842): J1 SQLite smoke versus J1-J5,
  including PostgreSQL, intrinsic standing recovery and fresh dev epochs.

No exact product-spec section presently defines physical GitHub batches,
compiled-product reuse, reviewer profile verdicts or master-push deduplication.
For those seams the lead instructions, #2544/#2535, #1967 and existing strict
plan/evidence/security owners are binding. Do not invent a runtime CI service.

Implementation must update platform-spec.yaml together with the profile change:
define fixed cumulative `core`, `lifecycle`, `full`; separate execution venue
from proof tier; explicitly name deferred full-only obligations and H/T/L limits;
update the three exact compiled-profile sections above and M24. Update the
public backend proof catalogue's frequency references without changing product
support, required children or capability claims. Full is exhaustive of the
admitted automated corpus, not a promise to provision paid/live/optional inputs.

The lead-approved frequency change supersedes only historical PR-frequency
requirements for soaks, full catalogue/matrices and native-unused. Preserve the
900s windows, both backend cells, 1s/15s/90s assertions, 22m Go/1500s command/30m
job limits, current performance exceptions and all failure evidence. The
conditional-soak citation 5824204797 currently returns HTTP404; git history
records its placement change, but this audit does not fabricate its content.
Replace the stale placement reference with the newly recorded lead disposition
and independent gate; the accessible original workload ruling remains binding.

## Complete Census And Proposed Fixed Tiers

See `issue-2544-proof-census.md` beside this artifact. Its retained JSON has
every unit's exact packages/regex/count/environment/deadline, active roots,
declared child requirements, root deferrals and public backend references.
The immutable raw archive in swarm-docs includes the generator and all five
current bound plans, not merely hand-written representative names:
`docs/audits/2026-10-03-2544-proof-census.tar.gz`, SHA256
`197e89d0e78cfb5c0f4a8d3b899f0f46034a8a03020cf1da54b1c9e89f5890fb`.

Current baseline: 278 Go packages, 82 named unit declarations, 10,339 distinct
active roots, 62 public backend catalogue references (60 Go proofs and two
tracked separate concepts). Existing local plans have14 units; full has71;
nightly has73 because **full presently lacks both soak cells**. The new full
profile must close this gap; neither its current name nor a nightly-only receipt
is evidence that `--full` already ran those cells.

Proposed three tiers, selected by reviewer judgment rather than path inference:

| Tier | Scope / exact representative obligations | What it does not establish |
| --- | --- | --- |
| core | All automatically discovered ordinary packages; complete CLI verify/inventory and EventBus unit proofs; store admission; existing API routing/provenance, fan-out acknowledgment-loss/reporter and served lifecycle canaries; both-store scatter safety; short both-store pressure; exact served source/artifact and connected-channel journeys; N=2 SQLite golden and J1 SQLite lifecycle smoke | Complete selected-store API/lifecycle matrices, the whole runtime catalogue/replay corpus, invocation variants, N=10 race bursts, delayed/heavy reporter performance or 15-minute endurance |
| lifecycle | Core obligations plus complete ordinary runtime/contract/API/LLM/continuation, selected-store and served channel/mailbox/standing/fork/reset families; golden both-store forced restart/sequential/numeric journeys and J1-J5; excludes explicitly full-only rows below | Full fixture replay corpus/invocation variants, long delayed-reporter/heavy-volume objectives, repeated race bursts, endurance and native-unused union |
| full | Every admitted ordinary and special-package root, full catalogue/replay, six compiled invocation shards, heavy1,362-row fan-out, delayed reporter cases, both race N=10 bursts, both900s soak cells; hosted full additionally requires native Linux/Darwin unused union | Separately provisioned real Claude/Telegram, optional Docker/provider/server-loss probes or a native platform not actually executed; no skip earns that credit |

The per-root census currently proposes6,537 core,3,703 lifecycle and99 full
root coordinates. Counts are not costs: a single root can execute hundreds of
fixture rows or900s of pressure. These are static proposals, not passing
candidate plans. Every named unit and all nine historical generated broad
partitions have a disposition in the table; no obsolete test is inferred.

Core reuses existing named canary projections plus an explicit existing
`TestGoldenAgentWorkloadSQLiteSmoke` selection. Cumulative means obligations,
not duplicate execution of overlapping projections. Higher tiers use canonical
containing partitions once. Preserve only already-governed substitutions:
full N=10 supersedes N=2, and J1-J5 supersedes J1-only, with exact named proof
and source credit. Lifecycle runs N=2 plus both-store restart/J1-J5 but not bursts;
full retains the governed N=10 replacement. No novel equivalence or deletion.

The18 candidate core selections include ordinary local canaries, two exact
supported-source/channel proofs, scatter, admission and pressure; normalize
the duplicate catalog smoke projection rather than execute it twice. Exact
core membership is a gate decision. The Markdown/JSON lowest-tier table makes every
subset of a mixed existing family explicit. Higher-tier root selection is
derived from the same declared units/inventory, never from filename heuristics.
New ordinary packages remain core automatically; an unmatched special-package
root has no full owner and fails the full-census guard rather than disappearing.

Venue is orthogonal: local full runs the complete host-executable automated
suite including both stores/soaks through swarm-test admission. Hosted full also
owns native macOS possession and Linux/Darwin unused union. A Linux local full
receipt cannot satisfy CI-native platform checks. Non-provisioned opt-in roots
retain the finite existing deferrals and explicit separate credit, at every tier.

## Reviewer Verdict And Automatic Selection

One small strict record lives in the existing checked-in issue audit artifacts;
it contains version, issue, audit-scope digest, independent gate reference,
`local_tier` and `ci_tier`. These are two independent choices, not booleans and
not inferred from each other. Example gate declaration, not an approval:

```yaml
qualification:
  version: 1
  issue: 2544
  local_tier: full
  ci_tier: full
  audit_scope_digest: <exact reviewed scope digest>
  gate_ref: <independent approved issue comment>
```

The reviewer approves the normalized record payload as part of the issue gate;
the implementer records the gate reference, not a self-authored approval. A
scope/requirement alteration must be ratified, not smuggled into a repair push.
The gate must include the exact typed qualification block, not just free-form
approval wording. Existing independent human review remains the approval owner;
a shared GitHub login is not invented proof of reviewer independence.
No head-specific re-gate after every ordinary approved-scope implementation push:
the verdict binds scope; execution evidence independently binds its exact head.

Minimal explicit context, no new registry: the local task supplies the audit path
once using `SWARM_TEST_QUALIFICATION_AUDIT` (or the equivalent audit-path argument),
then normal `go run ./cmd/swarm-test` resolves the reviewer-required local tier.
CI reads the same relative audit path from one structured `Qualification-Audit`
field in the PR body. A locator is not a second policy owner. Never scan old
audits or infer the issue from branch/title/path. No-context local execution
remains an explicitly developer-only core run, not a reviewer-bound receipt.

The existing testplanning owner validates schema, path confinement, scope
digest and approved gate payload; the existing planner/runner consumes the same
resolution. Fetch only read-only GitHub evidence with the existing token/context
when admitting reviewer-bound proof; never execute comment content or expose
publisher credentials. An inaccessible/missing/ambiguous/superseded verdict
cannot produce thin qualified evidence. CI conservatively runs full and reports
unresolved admission; reviewer-bound local proof fails clearly rather than
guessing. Explicit `--full` may increase coverage, never lower a declared minimum.

Print venue, requested/required/resolved tier, gate, scope/policy/record digest,
source head and planned/deferred units before resource acquisition. Bind the
verdict and policy digest into the existing RunPlan and command/job receipts.
If a reviewer changes a requirement, update the shared record and trigger fresh
normal head qualification; earlier thinner green cannot count. CI's summary
must compare its resolved requirement with the current verdict before success.
Do not parse free-form review wording, add labels as independent semantic input,
or build a generic approval/framework service. Retire PR path escalation and
soak/parity inference in this slice; retain local changed-test semantics separately.

## Execution Paths And Every Gate

| Ordered path / gate | Classification | Named proof planned |
| --- | --- | --- |
| Lead scope -> independent issue gate -> recorded local/CI verdict | same chosen class for proof admission; semantic coding permission is existing human process | V01-V12 below; unapproved scope never generates qualified thin evidence |
| Native draft/ready event -> explicit audit locator -> typed verdict -> fixed tier | same chosen class | T/V event matrix; real hosted tier transitions; preserve #2537 draft -> ready refusal/acceptance |
| Checkout source/merge identity -> effective build context -> root census -> full ownership -> selected/deferred plan | same chosen class | T01-T12, original unmatched-root/head/GOFLAGS controls plus new tier negatives |
| Required platform/static checks -> exact build products -> physical batches/isolated unit processes -> root/backend/child receipts | same chosen class for scope/provenance/physical lifetime | C/B rows; actual unchanged SQLite/PostgreSQL public source/channel and lifecycle paths |
| Terminal job observation -> budget evaluation -> required summary -> human merge review | same chosen class for evidence/summary; final human review is separate existing approval owner | E rows; incomplete/canceled/stale/wrong-tier never green |
| Local selected-store possession -> runner/service admission -> process cleanup | different semantic concept: existing testpostgres leases and runtime startupownership; no semantics/concurrency changed | unchanged runner/semantic-smoke/native possession controls, not new lifecycle credit |
| Verified master push -> exact merged association/tree/run/attempt/App/plan/verdict -> cheap skip or full fallback | same chosen class | P rows; history fixtures premerge, first literal receipt postmerge |
| Scheduled/manual full -> all logical roots/native platforms/soaks -> exact evaluation -> restricted publisher | same chosen class for qualification; App publication remains #2535 operational tail | Q rows; publisher no-op/material distinction preserved |

Before any observed proof can qualify, source admission, an admitted tier plan,
required root/child coverage, actual successful commands and exact attempt/head
receipts must succeed. A green job alone is never closure.

## Exhaustive Canonical Owner / Consumer Audit

Status legend: A=already consumes owner; M=moved/delegated in this work;
D=different concept with named proof; S=explicitly split/tracked. No unnamed
same-seam claim. Owners below are real semantic owners, not first helpers.

| Owner | Complete known consumer families and planned status |
| --- | --- |
| testplanning policy, BuildPlan, root inventory, BindExecution, projection/census | A/M cmd/swarm-test-timing CI planning; M cmd/swarm-test default/full/planned completion; M strict RunPlan validation/signatures; M testcatalog inventory/external-proof identical-profile validator; M public_surface_backend_matrix catalogue and its validators; M catalog/compiled-profile guard tests; M runtime-fanout, conformance2394, flow-constructor and current-plan partition guards; M model exact-selection/profile keys; A ordinary go-list discovery and unknown weight inclusion |
| Same owner, closed workload/profile interpretation | M releasee2e goldenContinuousProofProfile and full_lifecycle callers; M root deferral/replacement interpreter; M CI environment/workload labels and required-child binding; M command/help/docs profile projections. Delete retired local/pr-common/pr-escalated/nightly selection semantics rather than maintain aliases; event/venue map to fixed tiers through one owner |
| Same owner, reviewer-bound qualification | M local entry, CI-plan, required summary, manual/scheduled policy; M issue/PR audit template and implementation/review checklist requirements. Both choices consume one typed record; no labels/comment heuristics or per-change test graph |
| testtiming CommandEvidence/EvaluateBudget/AttachJobEvidence | A/M CLI recorder/evaluator, local executeCompletionUnit, timing-budget aggregation; M workflow terminal poller and job mapping; M publisher weight-update evidence. All migrate together for verdict/tier and one-to-many ownership; keep logical command/root/backend failures and count physical cost once |
| Existing RunPlan physical dispatch | M MatrixJSON/workflow proof strategy and isolated batch loop; M mandatory-soak strategy and aggregator; M exact-head job/evidence publication. Long, soak, platform or unproved-isolation rows remain separate |
| Existing executable build owners | M ci-plan module/production/test/tool products and native/static tooling; M proof worker fallback/build/cache validation; M releasee2e buildReleaseBinaryWithArgs and buildOwnedMockLifecycleBinary; M SQLite smoke Docker-layer build. Exact manifests separate flags/race/toolchain/GOOS/go.sum/profile/source; no test-result cache credit |
| Existing workflow protected admission | A/M all14 job definitions, their needs/if/summary, #2537 native draft semantics, manual distinct protected-context names, native macOS possession, Linux/Darwin unused union. Full gating is policy, not skipped-proof success |
| Existing typed execution/evidence provenance | M master-push observation/projection and full fallback; A existing head versus synthetic execution distinction and exact successful run/attempt identities; A restricted scheduled App publisher. New observation uses existing testplanning/testtiming owners, no scheduler/service |
| Local testchanged.PlanChanged / cmd/swarm-test-changed | D: explicit local developer command with import-graph semantics, not reviewer-required hosted qualification. Preserve its conservative full fallback; prove it cannot downgrade reviewer-bound requirements if it invokes swarm-test |
| Test harness runtime/store/provider semantic owners | D: actual lifecycle/lease/claim/database semantics. Preserve test contents/assertions/backend cells/concurrency; #2353/#2394 own genuine failure/performance classes, not qualification waivers |

Repo-wide census searched production calls, tests, YAML references, projections,
environment/profile readers, all14 workflow jobs, public backend references,
catalog external credit and local changed-test callers. No second checked-in CI
dispatcher was found. E's open #2525 edits test-proof-plan.yaml and
flow_constructor_partition_test.go: coordinate exact membership and revalidate
its eventual integrated delta, never edit E's worktree or invent a dependency.

Old non-authoritative paths/removal candidates: automatic source/status-based
PRChangeOptions, workflow path escalation, parity supplement duplication, venue
labels interpreted as workload scope, external catalogue's identical-unit-list
requirement, one-GitHub-job-per-unit matching in BOTH evaluator and jq poller,
repeated unverified build helper products, and unconditional repeated verified
master qualification. Do not delete tests, historical evidence, the local
changed-test owner, fresh process/store isolation, required names or security.

## Manifestation Matrix / Exact Planned Proof

All rows are planned; none is implementation proof. Every82 unit row also has
an exact command/root/child census assertion, a retained full owner and actual
full execution requirement. No row is credited by shared-owner introduction.

| ID | Manifestation | Exact proof planned |
| --- | --- | --- |
| T01 | Core, lifecycle and full membership differs but full remains complete | TestFixedTierCensusOwnsEveryActiveRoot; all82 unit declarations, every generated package/root,62 public references; missing/unmatched/duplicate owner negatives |
| T02 | Lower tier omission can masquerade as all-package completion | TestDeferredTierRootsCannotEarnExecutionCredit; exact deferred root/tier/reason/full owner, missing selected PASS still fails |
| T03 | Higher tier duplicates core projection or loses its obligations | TestCumulativeTiersCoverLowerObligationsOnce; canonical containing partitions, permitted N2/N10 and J1/J1-J5 replacements only |
| T04 | Core backend examples can be mislabeled full parity | Actual source-artifact/source mode and connected channel both-store required children; N2/J1 explicitly SQLite-only |
| T05 | Lifecycle restart/numeric/card/pause/reset/fork claims | Actual unchanged golden both-store restart/sequential/numeric and compiled J1-J5; public required child matrix |
| T06 | Full currently lacks soak cells | full/local and hosted full plans require both exact900s cells; actual both-store hosted full receipts and shortened/wrong-backend/skip negatives |
| T07 | Full-only long/large proof could disappear after profile narrowing | Actual catalogue/replay, six invocation shards,1,362-row, delayed reporter and both N10 race repetitions in manual full; retained original assertions |
| T08 | Native platform analysis moved but union becomes partial | Actual same-run Linux and Darwin native unused artifacts/union in hosted full; missing/wrong-head/wrong-platform negatives; macOS possession stays mandatory hosted core |
| T09 | Added/renamed/copied/deleted prose or non-Go input selects heavy scope | Fixed tier invariant across A/M/D/R/C status/path table; no path or import-graph selector remains live in CI |
| T10 | New ordinary or special roots omitted, optional/helper skips credited | go-list discovery; unmatched special root fails full census; finite current helper/live deferrals and malformed declaration negatives |
| T11 | Local graph command becomes another qualification interpreter | Unchanged TestPlanChanged and full-wrapper fallback controls; explicit reviewer minimum cannot be lowered through raw forwarded flags |
| T12 | YAML/spec/catalog profile claims disagree with executable plan | Authoritative spec/API-spec, external catalogue and complete/disjoint root guards; source selection versus H/T/L credit checked separately |
| V01 | Local core / CI core | Compiled runner and real hosted ready plan match same verdict/policy while venue evidence differs |
| V02 | Local lifecycle / CI core | Local cumulative restart matrix required; CI cannot borrow that local receipt |
| V03 | Local core / CI lifecycle | Hosted lifecycle required; local core success cannot satisfy it |
| V04 | Local full / CI core | Actual local full includes both stores/soaks; hosted core separately passes; local cannot claim native Darwin |
| V05 | Local core / CI full | Actual hosted full all corpus/platforms/soaks; no need to run local full merely because CI does |
| V06 | Remaining four ordered pairs | Table-test all9 core/lifecycle/full pairs through shared resolver and entry points; no inferred coupling |
| V07 | Missing/malformed/unknown/duplicate/path-escape record | Strict payload/context fixtures fail thin admission before acquisition; unresolved CI chooses full and exposes refusal, not assumed core |
| V08 | Wrong issue/digest/gate, unapproved or superseded verdict, API failure | Read-only exact gate/provenance fixtures; no guessed issue/branch or stale thinner qualified receipt |
| V09 | Explicit user flag or raw arguments downgrade declared minimum | Refuse lower request, accept --full promotion; developer no-context run explicitly non-review-bound |
| V10 | Reviewer raises requirement after green core | Real hosted requirement update normal synchronize; old green not accepted for new digest/tier; summary revalidates current requirement |
| V11 | Native draft/ready and deliberate manual/scheduled posture | Preserve #2537 same-SHA draft refusal; ready resolves gate; manual checks remain distinct; schedules select full |
| V12 | Source head, execution merge, venue/flags/profile evidence mismatch | Existing wrong-SHA/GOFLAGS/digest/count/env guards plus verdict/policy/tier mismatch fixtures |
| C01 | Cold/missing/evicted build cache | Real selected commands rebuild successfully; no cache prerequisite or previous result as evidence |
| C02 | Stale/wrong-source/toolchain/go.sum/profile/race/native product | Tampered manifest/bytes negatives; exact source/tool/flags/policy verification rejects reuse and rebuilds |
| C03 | Release binary/test child cache changes runtime/process semantics | Real compiled both-store source/channel/restart paths with cold and reused binaries, unchanged args/root/listener/fresh store |
| C04 | Serial planner warming consumes savings or cache capacity | Measure CI-plan elapsed plus total assigned cost with cold/warm controls; compile-only -c, no TestMain during warm; no speculative savings |
| C05 | Native tools/static tooling/Docker layers reused incorrectly | Exact pinned versions/platform/tool inputs and real full collectors; SQLite smoke remains actual product execution, cache miss fallback |
| B01 | Isolation-safe short-unit packing | Each selected logical unit fresh child/store/workspace, same command deadline; measured empty/setup/proof/upload cost and later-unit collection |
| B02 | First/middle/last unit fails, worker dies/cancels | Injected isolated-process failures/cancel; retain every later feasible receipt, fail batch; no fail-fast or skipped success |
| B03 | Missing/duplicate/undeclared unit, wrong job/run/attempt/head | One-to-many AttachJobEvidence and workflow poller exact mapping hostile matrix; missing terminal/upload evidence fails |
| B04 | Physical cost counted repeatedly or logical status swallowed | One physical job cost exactly once; each logical command status separately; budget and command-envelope negatives |
| B05 | Long/platform/soak/fault row accidentally batched | Eligibility declaration and invariant tests, preserve separate workers, all unchanged limits/backend cells |
| E01 | Summary tolerates skipped selected job or incomplete evidence | Actual summary shell fixtures at every tier; absent/canceled/red native/soak/command proof fails when planned |
| E02 | Generated weights suppress coverage or mix tier signatures | Exact selection-signature/policy digest model tests; unknown weights affect placement only; no cached old profile bytes accepted |
| E03 | Publisher duplicate dispatch/security boundary returns | Existing shell/App/no-op/generated-only/master-env controls; preserve one native automatic PR, no dispatch/PAT fallback |
| P01 | Genuine identical-tree qualified merge | Historical association/tree/App/check/plan fixtures pass exact current scope; first literal postmerge cheap receipt remains open |
| P02 | Direct/admin/main push, multiple/wrong PR association | Full fallback; no branch-protection change or synthetic green master summary |
| P03 | Stale/red/wrong-App/mixed-run/attempt/missing plan or tree mismatch | Typed observation negatives choose full, preserve all evidence; qualification at weaker than required tier cannot deduplicate |
| P04 | API/artifact unavailable or unclear provenance | Full fallback; no retry loop invents acknowledgement |
| Q01 | Premerge complete qualification | Focused normal/race controls, actual selected both-store journeys, default swarm-test, required local tier, manual hosted full all original roots/soaks/native union and exact-head normal hosted tier |
| Q02 | Literal master/scheduled entrance cannot run before merge | First real postmerge receipts plus safe fallback control; keep operational acceptance open, not counterfeit manual event credit |
| Q03 | Claimed20-minute saving without load or moved cost | Before/after runner-minutes, physical queue entries, planner/critical path; >=20 ready qualifications/three busy periods including failures/cancellations and tier mix |

Test names above are implementation commitments, not existing passing tests.
For each census row proof is the exact root/child declaration check plus actual
hosted full required PASS; backend mode is never inferred from a name alone.

## Parent Probe / Watchlist / Tracking Decision

Mapped node: maintenance-and-cleanup / harness_reliability_and_local_smoke.
It already tracks filtered-root omissions, split local/CI policy, whole-job
evidence, native protection, immutable snapshots and physical execution costs.
That evidence promotes catalogue/external-credit, completion, jq polling,
publisher and local siblings into this audit; a route-only patch is dishonest.

Parent action: absorb all bounded fixed-tier/verdict/physical-build/batch and
verified-master obligations into #2544, not a new issue or helper stream.
Keep #2535 OPEN for first material scheduled publisher, literal edited
master/scheduled acceptance and busy-fleet p90. Keep runtime performance and
health in independently open #2394/#2353, local harness umbrella #1196.
No matrix cap, merge queue, assertions/deadlines/waits, in-test concurrency,
third-party vendor or blanket deletion is authorized.

Tracker decision: **update current issue and parent/watchlist before coding**.
Replace source-aware/affected-owner scope and obsolete pending-lead language
with the user's fixed cumulative tiers and independent local/CI choices. No
new child, superseded old issue, new watchlist node or POTENTIAL_ISSUES entry.
Historical #1967 stays closed; amend its frequency/profile interpretations here
without reopening its eliminated assignment/dispatcher class.

Remaining parent tail: one bounded implementation child (#2544), one operational
acceptance group (material publisher plus edited master/schedule), one busy-load
20-run measurement group, and an uncertain runtime/long-unit optimization group
already #2394/#2353/#1196. Confidence medium for chosen-owner closure, low for
meeting fleet p90 before measurement. Architecture feedback is tracked in #2535
and this watchlist: proof scope, execution venue and physical topology were
conflated. Better direction is existing typed planner/evidence ownership, not a
new CI service or per-function dependency framework.

## Feasibility, Baseline Proof And Stop Conditions

The class can plausibly close in one PR using existing owners. Fixing only
PRChangeOptions would leave live catalogue, wrapper, jq poller, protected summary
and master interpreters; all are explicitly migrated. Cache reuse/packing are
measured eligibility, not promised savings. If exact isolation requires runtime
or a new scheduler/build framework, stop for a measured disposition; do not
silently omit an existing #2544 obligation.

Fresh audit-only controls passed:
`go test ./internal/testplanning ./internal/testtiming ./internal/testchanged`
with current-plan census, unmatched-root/finite-deferral/GOFLAGS/profile/head,
strict-loader, complete-job/draft-summary and local changed-package tests.
testplanning14.278s, testtiming0.245s, testchanged0.002s. The attempted matching
cmd/swarm-test-timing package ran zero cases and earns no proof credit.
All five canonical baseline profile planning commands passed, including root
binding/digests and actual no-soak-full versus two-cell-nightly distinction.
No candidate tests, hosted changed policy, full suite or new runtime execution.

Historical run37083235335 is339.10 assigned runner-minutes; four such runs on20
runners have a67.82-minute last-completion work floor. The full-only cohort and
native union have measured historical costs, but subtracting them is not a
candidate benchmark. Historical PR burst jobs skipped their N=10 workloads;
their short assigned durations are not full-tier race-burst execution costs.
Ordinary broad shards alone used25.25 assigned minutes.
The approximately55-minute normal-core envelope is a hypothesis. Full requested
by a reviewer may necessarily exceed20 minutes; report the tier mix honestly and
retain #2535's aggregate acceptance, never exclude slow full verdicts silently.

Blocking conditions: independent gate missing; exact tier/verdict mechanism not
ratified; unmatched live profile consumer; inadequate full root/child/platform
owner; unsafe batch/product reuse; protected current requirement cannot be
execution-proven; new security/token or runtime semantics required. No remaining
user decision is requested by this artifact: the lead supplied the frequency
tradeoff and tier flexibility. Reviewer-g must now accept or repair the proposed
membership and record **both local and CI tiers for #2544 itself**. Proposed
requirements for this high-risk policy PR are **local full / CI full**, including
one actual hosted all-corpus/native/two-soak qualification before merge.

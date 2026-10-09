# Pre-Implementation Coverage Audit: Test-Time Phase 1

Agent-g; audited master `da1cd2219fc6b7d1d0aa76584fb4666479aa81fb`.
Audit only: no workflow, planner, runtime, membership or baseline edit.
User approval on 2026-10-09 and reviewer-g preflight 6074374438 bind this
amendment to open #2535; #1196/#2353/#2394 retain their separate obligations.

## Binding Context And Boundary

Read `~/research/test-time/report.md` (The plan, Decisions for you, What's
important to notice) and all five angle reports. Report SHA256:
`413068c38cbefebf789d99c84fe173dda9947f60375930a1bd67bb8e96550b76`.
Savings are report estimates, not newly demonstrated fleet improvements.
Live checks confirm missing-tier full fallback and the protected App 15368
contexts Required test summary/SQLite local smoke with strict=false.
The three latest scheduled runs 37764339866/37606165649/37448734512 are
cancelled/failure/failure: none bootstraps healthy nightly coverage.

Exact governing spec: `test_specification.catalog.qualification_tiers`
admission/membership/execution/local_completion/timing_publication_and_cadence
(platform-spec.yaml:23554), plus compiled lifecycle M24. Their missing-tier,
full-soak/native-unused and all-tier macOS rules need explicit amendment in the
implementing PR. User approval on 2026-10-09 overrides the old #2394 soak/drain
frequency ruling 5743323725 and #2544 conditions only where named; duration,
backend/platform assertions and honest execution credit remain binding.
Guidelines and SEMANTIC_DRIFT were re-read. No vendoring is approved.

Category: qualification-policy semantic drift/high-risk harness maintenance.
Symptoms: absent tier silently selects full; performance overrun can turn
successful proof red; full failures continue spending; expensive obligations
run per push. Entry lines/helpers are not the audit boundary.
Working classes: qualification intent versus proof/merge authority; timing
performance versus evidence validity; proof frequency without lost coverage;
separate immutable-base analysis reuse. Parent: #2535 CI/test-cost inflation;
broader maintenance reliability remains open. User requests small coherent PRs,
not one giant refactor. Each chosen child class must close completely.

## Owners And Exhaustive Consumer Census

| Owner | Consumers / disposition |
| --- | --- |
| `testplanning.CITier`, `Policy.ResolveProfile` | Event body, CLI planner, late current-body check, master replay and their tests/spec share the parser. Change only zero declarations to core; malformed/duplicate declarations stay fail-closed/conservative, never execute body text. Local-Tier is independent. |
| `testplanning` plan/coverage/batches; `testtiming` evidence validators | Hosted/local execution, required summary, master replay, cadence and weight publication must distinguish complete full from core, diagnostic, nightly or missing receipts. No core success grants final-full credit. |
| `testtiming` budget/evidence owners | PR timing overrun becomes advisory only together with the test-time ratchet; incomplete, skipped-required, mismatched, corrupt or failed proof stays fatal. Schedule enforces timing. No blanket continue-on-error or confirmation rerun. |
| Existing CI workflow plus orchestrator `bin/gate.sh` | Gate.sh is the user-run premerge authority for exact-head full and current nightly health. No branch-protection changes or new privileged publisher is proposed. Script host/path has been requested; inspection and executable hook tests are required before coding that integration. |
| Existing #2394 tests / transaction probe; native-unused / macOS jobs | Retain original proof in nightly owners, with explicit finite measured-window PR owners. Movement comes only after bootstrap; no fake equivalent soak, compressed pacing, deleted backend or platform cell. |
| Existing persistence-authority ratchet and D's base cache | Ratchet, trusted-base archive, census/selected-boundary projection and strict codec remain authoritative. Head always scans fresh. Protected master publishes exact archive-derived entries, including replayed master pushes; PRs only restore exact keys, never publish shared entries. |

Execution path: native ready PR -> event/body admission -> exact plan -> original
commands/children -> source-bound evidence -> completion/performance checks ->
orchestrator full/nightly revalidation -> merge. Draft/current-head refusal,
proof completeness and merge authority are the same qualification class;
runtime assertions and store semantics are different, unchanged concepts.
Scope probes include manual dispatch, schedules, master replay, model-only bot,
local tiers, both matrices and all summary/cadence/publication consumers.

## Proposed Policy / Executable Negative Matrix

| Item | Binding design and required proof |
| --- | --- |
| 1 core default, full final head | Gate.sh invokes the existing qualification/evidence owners extended for premerge: current PR head, trusted CI workflow/repo/App, exact run/attempt, actual checkout/plan identity and complete full records. Accept a current-head successful full PR or full-only manual dispatch, independently of the core body line; do not mistake Full dispatch summary for a protected PR context. Prove absent/core/explicit/malformed/duplicate tiers, core-green/full-absent, failed/cancelled/incomplete full, new head, body-only edit and current-head full success. Behind-but-conflict-free is allowed; no forced rebase. |
| 2 advisory PR timing plus ratchet | Reuse complete primary artifacts, sum top-level test elapsed once (not parent+children), compare same tier/venue/build/count scope against immutable trusted full-derived reference. Separate test-seconds, runner-minutes and queue/wall time. Proposed gate parameters: 10% tier growth; new roots above 30s require nightly placement or reviewed Test-Time justification; new above-store both-store roots require justification. These numbers/exception admission are proposals for reviewer-g, not invented user approvals. Test-Time text alone is not approval. Missing baseline/provenance, mutable generated baseline reset, duplicated roots, unknown cost and unauthorized exceptions refuse. Overrun warns on PR; invalid proof fails there and at merge; nightly enforces budgets. No warning-only cutover before the replacement guard is active. |
| 3 fail-fast full / keep-going | Use native matrix cancellation, retaining failed primary evidence and explicitly unproven canceled/not-started siblings. Cover ordinary and soak proof siblings, not only one of today's separate matrices. Diagnostic keep-going retains every assertion and cannot qualify an incomplete run. Prove injected first red cancels owned siblings, cleanup joins, summary refuses and keep-going collects remaining outcomes. Exact matrix composition can change only in the later quiet-window PR, not PR-1. |
| 4 nightly / selective soak | Existing schedule plus explicit master full bootstrap retain the exhaustive nightly truth; master analysis also retains unused/macOS where the report requires. Gate.sh re-queries latest relevant full nightly on master immediately before each merge: trusted workflow/scope, completed success and proposed <=30h freshness. Missing, red, cancelled, in-progress, stale, API-error or foreign/partial receipt refuses; older green cannot hide a newer red. Recheck after full/core green, including nightly turning red before merge. Gate hook/location/freshness must be ratified and installed, first complete nightly green must exist, and a hosted red->blocked->green control must pass before anything moves. No repo settings changes. |

Retained/moved map for item 4: conformance-soak-sqlite/postgres keep their original
900s, pacing and 90s drain; finite pressure remains PR-owned. Original full
delayed roots are TestIssue2394ServedOriginalReporterFiveHundredDelayedBothStores
(serveapp-i-reporter), TestIssue2394ReporterFiveHundredDelayedCommitsBothStores
(conformance-2394-reporter), and
TestIssue2394ServedOneSecondCommitPreservesTwoFullChunksBothStores
(serveapp-delayed-commit-preservation): retain full delayed-drain nightly
variants and their exact SQLite/PostgreSQL assertions, with separately named
measured-window PR variants in the existing owners. unused-linux/darwin/checks
and macos-sqlite-possession retain native source/platform checks in nightly and
the approved master-analysis posture. Trigger both original soak cells on
ci:soak and explicit fan-out/continuation/selected-store commit-path changes;
added/deleted/renamed paths and label-only transitions need positive/negative
planner and hosted proof. No filenames-only claim of a complete commit census.

## PR Sequence, Tracking And Stop Conditions

PR-1: membership-neutral policy/core-default + final-head/merge-hook integration
and immutable debt-base cache, with its isolated safe timeout. No unit IDs,
selectors, required children, TSV format/debt rows/multiplicities or physical
shard membership changes. D e4fa8b3cf is the starting handoff, with attribution,
fresh-head scanning, exact collector metadata transition and cold/corrupt/foreign/
ignored-source/resurrection controls; do not blindly transfer its old digest.
Protected-master archive producer and exact-key/no-prefix restore must be
execution-proven, not just a warm local benchmark. Code-cache trust never grants
allowed debt. The 11% head-sharing prototype 3a38cf659 is not needed for this cut.

Next small policy PRs close timing/ratchet, full fail-fast and nightly movement
atomically by their owners. Code/unit changes (channel wake/cadence, scoped probe
delay, shard rebalance, walker and local scheduling) land only in a handed-over
quiet window with no lane mid-qualification. Updated user local-order ruling:
all static guards/census/inventory/registry/unused/generated-diff preflight first,
then longest-first real units; fail-fast and joined cancellation throughout.
Partition exact proof once; 1-2min preflight is a measured target, not omission
authority. Existing host capacity is unchanged absent separate approval.

Tracker decision: #2535 stays open; this amendment supersedes its parked policy
status, not closed #2537/#2544 history. Refine existing
harness_reliability_and_local_smoke with feedback-versus-merge/nightly authority
and immutable analysis-cache trust. Promotion: cover all listed policy consumers
now; keep broader cost/fleet and runtime owners tracked. Parent tail: policy
movement plus roughly 4-5 code/measurement groups, medium confidence; no fleet
20min/10min or complete parent closure claimed. Architecture debt is conflated
feedback, coverage and qualification lifetime; use existing owners, no framework.

Baseline focused planner/timing/soak controls pass at da1cd2219
(testplanning 0.003s, testtiming 0.147s); actual CLI/current-head/replay/admission
controls pass (CLI 0.007s, testtiming 0.409s). No implementation, full, hosted
transition or new ratchet proof is claimed. Before any server2 ask: complete
local guard and census sweep; then focused -> core -> one final full as ruled.
Every PR declares CI-Tier: core unless reviewer-g requires more, uses agent-g
title/typed commit subjects, and includes its post-implementation proof audit.

Stop before coding if the independent gate, actual gate.sh inspection/handoff,
ratchet threshold/exception definition or cache trust/publication contract is
unresolved. Stop proof movement until healthy-nightly/bootstrap and merge-time
blocking are live. Request reviewer-g's independent gate on items 1-4 and the
membership-neutral PR-1 ceiling; no coding approval is presumed.

## Delta After Independent Gate And User Clarification

Reviewer-g 6074832462 is insufficient/widen, not permission to code. User now
provides the merge-owner snapshot at `~/research/gate.sh`, read in full; SHA256
`67adb76e52ffc31692ea930ed418f1925eb8a0a7da8efa843f37595ae714af1e`.
The user states this owner runs before every merge: that is an operational trust
boundary, not GitHub enforcement against a human bypass or a settings change.
The snapshot binds expected SHA, mergeability/draft/rebaseability, latest check
names and successful Required test summary, optional requested tier versus body
and published tier check, and the most recent completed schedule's conclusion.
The lead owns the deployed orchestrator; G will not introduce a second gate.

Required repo-side integration: the planning path publishes a completed check
named `CI tier: <tier>` from the resolved canonical plan, on its exact source
head and linked to that Actions run/attempt. The current-head full plan and
complete required evidence still qualify the run; this early label alone is
never completion proof. Test core/full labels, stale or failed planning, missing
or foreign identity, reruns, and same-SHA core-to-full qualification. Gate
selection must not use an older differently named tier check after a later run.
The merge invocation must request full when final-full proof is required, and
must compare the actually run tier with the current PR body. A body-only edit
does not create higher-tier proof.

Observed snapshot limits are not hidden: the requested tier is optional, a
missing tier check is tolerated, and nightly inspection skips pending runs and
does not refuse missing/cancelled/stale/API-error outcomes. These do not prove
the stricter preflight matrix above. Reviewer-g must reconcile the deployed
operational boundary with the required negative controls before cutover; G does
not claim the snapshot proves them or modify the lead's gate without handoff.

User also rules that collector metadata lives in an authoritative sidecar, not
the TSV `# collector=` header. PR-1 must make literally zero TSV edits, including
that header; retain every byte/row/multiplicity. The sidecar must bind the exact
baseline bytes and current complete collector identity, including cache code.
Missing/corrupt/foreign metadata refuses; do not exclude new analyzer code to
keep an old digest or treat the retained comment as current collector authority.
Adapt D's prototype rather than copying its metadata-header transition. Final
sidecar/immutable-base admission semantics require the re-gate, not a fallback
compatibility reader or permission increase.

PR-1 itself requires `CI-Tier: full` and `Local-Tier: full` under reviewer-g's
ruling. The user-approved cheaper defaults for ordinary feedback do not lower
qualification for this harness change. Subsequent ratchet thresholds remain
unratified; nightly freshness, when implemented, is measured from run creation.
No CI config, planner, runtime, cache implementation or TSV has changed.

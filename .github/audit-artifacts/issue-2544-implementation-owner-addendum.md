# #2544 Implementation Owner-Map Addendum

Binding gate: https://github.com/division-sh/swarm/issues/2544#issuecomment-5965227266.
This is an additive implementation census, not a new gate or closure proof.
The historical 82-unit archive remains immutable. Fresh candidate census and
execution receipts must identify their own snapshot.

## Exact Consumer Corrections

1. Cumulative membership includes `serveapp-i-reporter` in lifecycle as well as
   core/full. The initial whole-unit projection omitted this lower-tier family
   from lifecycle. `BindExecution` now checks every deferred root against the
   minimum tier and authoritative full owner derived from the same policy.
   This strengthens cumulative coverage; it does not add a runtime owner.
2. The public backend ledger's risk checker imposed an obsolete blanket
   full/nightly threshold on restart proof. Canonical required-test membership,
   dedicated execution role and both-store children now govern separately.
   The existing core onboarding restart owner remains core; golden restart
   cannot claim core because its required owner is absent there. Public claims
   and backend child requirements are unchanged.
3. The route-authority matrix's `full_conformance_manual_nightly` label is a
   static venue description, not a second execution planner. Rename it to
   `full_conformance` with its validator; do not preserve a profile alias or
   change the route contract, fixtures or assertions.
4. `BuildGoProduct` delegates the existing release-binary and owned-lifecycle
   compilation entrances. Its optional cache is compilation-only: source,
   effective Go/compiler/image environment, dependency/policy bytes, tier and
   command flags govern eligibility; absent, dirty or invalid products compile
   normally. Every consumer copies into its own disposable workspace. It
   supplies no proof result, database, process or review authorization.
5. Physical batches are canonical RunPlan projections. Only explicitly
   packable short units with applicable measured weights can pair; unknown,
   long, platform and soak rows remain separate. Each logical unit invokes the
   existing runner with a fresh process, temporary workspace and owned database,
   retains its own command receipt and original budget, and contributes its
   physical job's cost exactly once. The workflow jq poller consumes batch IDs;
   the evaluator and publisher still validate every logical unit.
6. Master reuse observes exact current PR/run/attempt/App/plan/tree evidence
   using read-only APIs, validates an ordinary non-forced push, rechecks the
   latest run and body during observation, and repeats validation in the final
   summary. Failed observation chooses full; failed final revalidation refuses
   success rather than claiming that skipped work executed. Literal master and
   scheduled acceptance remain post-merge #2535 obligations.
7. Qualification exposed three additional workflow assertion consumers:
   `cmd/swarm-complexity/workflow_test.go`,
   `cmd/swarm-unused/workflow_test.go` and
   `internal/testutil/postgres_ci_test.go`. These are test guards over the
   already-audited workflow, not new execution authorities. They now locate the
   summary by its name, exercise every tier with success/failure/skip/cancel/
   unknown controls, decode expression-valued deadlines, and follow the exact
   checked-in batch script to retain canonical database-runner ownership. The
   latter census also explicitly includes mandatory-soak.
8. The approved authoritative-spec edits change exact embedded-source
   provenance in the compiled describe characterization. Actual base/head
   binaries reproduce all 45 base hashes. Only ten JSON cells change; they
   compare equal after removing only source_file/source_line/source_column
   inside provenance objects. The 35 other cells, strict hash assertions,
   normalization and two-repeat public commands are unchanged. Refresh only
   those measured hashes, not the CLI or admission contract.
9. Hosted core qualification at 28c0fc01a passed the compiled command tests but
   failed owned temporary-workspace cleanup: downloaded Go modules left
   read-only directories. The batch owner now restores directory write access
   only inside its exact mktemp workspace, without following symlinks. Cleanup
   failure still fails the unit but cannot suppress its receipt or later units.
   The existing command recorder persists failed/skipped observations before
   structural validation; the existing budget evaluator remains the outcome
   authority and refuses unsuccessful qualification. No retry or pass credit
   is introduced. Real script tests cover read-only disposal and fresh later
   processes; recorder tests cover retained failures and evaluator refusal.
10. Re-gate5966450116 synchronizes the issue/audit/watchlist before code in
    source78b09b41d and docs474f469; issue receipt5966519370 records that order.
    Source6691856f5 removes edited and guards the unchanged late five-minute
    summary with `TestCITierIncreaseKeepsLateSummaryAndNewHeadEvent`.
    Focused normal/race parser/current-body/event/owner and specification guards
    pass. The amended spec's embedded-source identity changes the same ten
    describe JSON hashes again. Real base/candidate compiled commands reproduce
    all45 baseline surfaces and prove zero differences outside provenance
    source_file/source_line/source_column; the35 other outputs are unchanged.
    Refresh only those observed hashes, preserving exact normalization and
    both repetitions. The failed characterization, cancelled6691856 lifecycle
    attempt37104654294 and interrupted queued local full are retained and earn
    no qualification credit. Resume higher-tier CI only on the corrected new
    signed head; keep the original8714 core as historical measured core evidence,
    not current-head or higher-scope qualification.
11. Hosted lifecycle37105291252 exposes the release package's static import
    boundary reader: it still bans the approved shared testplanning compiler and
    tier owner alongside actual in-process runtime packages. Its only new callers
    are process_harness_test.go and golden_agent_workload_test.go, already named
    in the audit. Admit that exact qualification-only package in those two files;
    keep runtime/store/CLI/provider imports, foreign subpackages, aliases and other
    files rejected, with negative controls. Public product execution stays in
    compiled children, not in-process. This is a missed test guard consumer of
    the approved composition, not another production interpreter or owner.
    The red hosted run remains evidence, not lifecycle cost/qualification credit.
12. Hosted full37108207931 on ef157f868 catches two unused remnants in the
    already-audited native union: readOptionalLines and test-only currentHead
    in cmd/swarm-test-timing. Delete both and the latter's now-unused import;
    do not suppress U1000 or restore path/checkout inference. That full run is
    red and cannot qualify full scope or the lifecycle savings threshold.
    Local full/default on the same ef source also fail the unchanged SQLite
    duplicate-publication state snapshot (line350), independently of tier
    admission. Existing2353 receipt5961787663 diagnoses original termination
    racing that snapshot; a diagnostic-only detached master0fa control again
    observes complete/active/revision6 becoming complete/terminated/revision7.
    The new receipts and narrow disposition request are recorded in2353
    comment5967143311,2544 comment5967143498 and PR2548 comment5967143689.
    Do not retry for green, weaken assertions or implement a runtime/catalogue
    repair under the CI gate. Full/default qualification and the final audit
    remain incomplete; lifecycle remains provisional. Prior successful
    lifecycle389/37106541076 is actual measured244.07 assigned runner-minutes,
    not qualification of a subsequent head or the full corpus. A separate
    test-only disposition is requested before any snapshot-fence repair.
    Hosted full finishes red: all70 physical Go proof jobs (73 logical units)
    and timing-budget evaluation pass, including SQLite953.79s and
    PostgreSQL967.36s soak leaves. Native unused union and the consequential
    protected summary fail; no full or successful-cost credit is inferred.
    Deleting both orphaned helpers passes the full cmd/swarm-test-timing package
    normally11.165s and race57.263s. The local native Linux default/race/issue2413
    unused matrix exits0 on the working correction; that is not Darwin union
    or hosted-head qualification. Preserve the failed ef receipts and issue
    records before one batched repair push and fresh required qualification.

## Local Instruction Clarification

V09's former "refuse lower request" wording is superseded by the gate's human
Local-Tier comparison. The local runner cannot authenticate a PR instruction
and must not fetch one. It records an explicit tier, or labels no-context core
as developer feedback. A reviewer must refuse a receipt thinner than the current
Local-Tier instruction. Invalid/mixed CLI selectors fail closed; `--full`
promotes. No authorization YAML/digest, locator, gate fetch or path selector is
introduced.

## Status

### Approved #2353 Test-Only Qualification Repair

Reviewer-g ruling5967342845 approves absorption in #2548. Earlier full/default
ef157f868 receipts and hosted37108207931 remain failed and uncredited. The
working class is a premature replay snapshot, not a production lifecycle defect.
Before replay, completed worker routes and the collector only after final gather
must reach exact persisted termination: current run, route/instance, entity,
status terminated and nonzero TerminatedAt. The root and active collector are
not terminal obligations. Use the existing absolute publication/phase deadline;
preserve the complete state-map, domain-count, delivery and join assertions.

The existing pipeline WorkflowTerminalCommitted/committed probe is invoked after
delivery settlement and before manager CommitFlowInstanceTermination. A scoped
collector probe holds that boundary for a deterministic both-store negative
control: delivery/join success cannot satisfy the fence, released finalization
can, and replay must preserve the full persisted map and domain counts. Retain
failure-only before/after evidence including status, revision and TerminatedAt.
No runtime owner, assertion filter, delay, deadline reset, vendor or framework.
If another mutation survives the exact fence, stop and re-gate. The bounded
repair is batched with native-unused helper deletion and its exact generated
complexity snapshot, then fresh local full/default and hosted full qualification.
Existing #2353/watchlist own the failure history; #2535 retains fleet/postmerge
acceptance. This approval is permission to repair, not proof or merge approval.

Q04 names the added qualification-fixture manifestation separately from the CI
owner migration. TestScatterGatherSafetyBothStores/{sqlite,postgres}/
{duplicate_publication,held_finalization} passes ten normal repetitions
(108.562s) and three race repetitions (140.081s). The held final gather has its
exact delivered join outcome and complete current_state while the persisted
collector is still active with zero TerminatedAt: the actual snapshot predicate
refuses. Releasing the existing probe allows exact termination and full-map/
domain-count replay equality. Each earlier completion still fences only its
completed workers; the active collector and root remain in the unchanged full
snapshot without becoming terminal obligations. Existing root ownership remains
TestScatterGatherSafetyBothStores; no new root, policy membership, budget or
workload deadline is introduced. The complete fourteen-leaf matrix passes
(64.329s), including both original hundred_reverse_completion, restart,
rejection and ordering proofs. Exact root/partition controls pass (11.219s).
Final clean-head local/hosted full qualification must still complete before
review. Raw receipts are
retained as agent-g-2544-terminal-fence-both-ten.log (SHA256
370838cf2a1aca3121cce65bf99a572a4b977d7bb005f9c8a654a2fba70ff41f)
and agent-g-2544-terminal-fence-both-race-three.log (SHA256
7648053b670991da2c49732848b356f381c46167989ab1893dba3575da380ed3);
final evidence binds their
candidate source/tree separately from the subsequent signed qualification head.

The hosted V10 experiment hit the original stop condition: fresh lifecycle smoke
passed while the only protected summary remained old core success. Exact facts
and the separate review block remain in `issue-2544-tier-edit-stop.md`.
Independent re-gate5966450116 now permits resumption after documentation sync.
It rejects an early-held summary and withdraws automatic post-success invalidation.
Keep the existing late five-minute summary/current-body/head check; remove edited.
An unmet tier increase requires a new signed head plus successful higher-tier CI,
with draft/non-mergeable fencing until qualification. Lead compares the current
body/current-head plan and actual local receipts at final merge, including edits
after approval. No new status, credential, authorization or waiting owner.

### Qualification Head Sequence

Historical signed head8ea95a10d starts the hosted lifecycle measurement and
new-head acceptance probe. Its run37105291252 finishes red at the boundary
guard in item11; all other physical proof jobs pass. The repaired lifecycle
qualification head is389d5f5a9, with the import guard negatives, nine-pair
selection and first/last/SIGTERM batch controls race-qualified. The repeated
compiled describe check passes all45
surfaces, with the original two repetitions and strict hashes. Lifecycle is
temporary measured scope, not the final required full/full verdict or review
readiness. The cancelled earlier runs remain uncredited.

Prepared audit-only predecessor66257c296 has production/spec/policy/tests identical
to8ea95a10d. Its interrupted local attempts earn no credit. The final full head
also adds `TestLocalAndHostedTierSelectionsRemainIndependent`: all nine ordered
local/CI tier pairs use the actual local selector and CI event parser/resolver;
neither venue can infer the other requirement. This adds one ordinary test root,
not a new owner, policy membership, workload, runtime or deadline. B02's existing
real-script test now covers first and last member failure and an actual child
SIGTERM, collecting both members' evidence and disposing their distinct read-only
workspaces. Canonical batches have at most two members, so no middle position
exists; eligibility/plan negatives prove that bound. These are proof-completeness
changes under the current gate, not runtime or planner changes. Earlier local
qualification attempts are interrupted/uncredited rather than carried forward.
Local full
qualifies the corrected clean head while hosted lifecycle measures8ea95a10d.
Publish the corrected head with CI-Tier restored
to full after lifecycle qualification, keeping the PR draft/non-mergeable during
the transition. Final review must bind the new head's actual full plan, run,
attempt and protected terminals plus the actual local full receipt. No higher
scope, passing result, cost saving or lead merge approval is asserted here.

This final audit-only head leaves production, spec, policy and tests identical
to repaired389d5f5a9. Local full/default qualification uses this clean source
while hosted lifecycle qualifies389. Only after successful lifecycle measurement
will this head be published with the restored CI-Tier: full requirement. The
prior interrupted attempts and red hosted receipt stay intact and uncredited;
neither a rerun-to-green claim nor unchanged runtime behavior is inferred from
the missed guard. Required new-head full qualification and final review remain.

Focused planner/timing/catalogue/runner controls and the updated public backend
ledger have passed during implementation. These are not full-corpus, hosted
new-head higher-tier acceptance, candidate cost or literal post-merge proof. Lifecycle remains
provisional until comparable hosted core/lifecycle/full whole-run cost is
measured; collapse it into full if savings are below the binding 20% threshold.
#2525 is still open and its actual policy/partition delta must be integrated if
it merges. #2535 remains open. No new architecture tracker or vendor is needed.

### Review Pass 1: Local Source Identity And Manual Truth

Binding review5968576803 adds two manifestations within the existing owners.
R01: explicit local --tier/--full must refuse tracked and untracked non-ignored
worktree changes before planning, before each command and after each command;
HEAD must remain the plan's actual commit. Default no-context developer feedback
remains permissive. Ignored test-results and external build artifacts are not
source changes. Planned hosted execution retains its existing execution-SHA
validation. Proof uses real Git repositories, dirty tracked tests, untracked
fixtures, post-admission dirt, changed HEAD, and clean/ignored-output controls.
R02: workflow_dispatch is exhaustive only. Both the checked-in choice and
canonical profile resolver must refuse core/lifecycle, rather than allowing thin
success under Full dispatch summary. Workflow YAML and resolver negatives prove
the full-only boundary. No runtime or second qualification owner is introduced.

The previous vemew full at6ea56b2f1 was interrupted with SIGINT after waiting for
shared admission; its partial evidence remains uncredited. Its hosted full run
37112045175 succeeded, but neither that run nor the earlier default developer
feedback qualifies this repair. Master5d3cc2400 is integrated before the repair.
Final explicit full qualification runs on server2, with source and scratch on
disk-backed /home, canonical PostgreSQL/admission ownership, and no /tmp worktree.
Final audit must bind server2 full and hosted full to the same resulting head.

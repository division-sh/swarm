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

## Local Instruction Clarification

V09's former "refuse lower request" wording is superseded by the gate's human
Local-Tier comparison. The local runner cannot authenticate a PR instruction
and must not fetch one. It records an explicit tier, or labels no-context core
as developer feedback. A reviewer must refuse a receipt thinner than the current
Local-Tier instruction. Invalid/mixed CLI selectors fail closed; `--full`
promotes. No authorization YAML/digest, locator, gate fetch or path selector is
introduced.

## Status

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

Corrected signed head8ea95a10d starts the hosted lifecycle measurement and
new-head acceptance proof. The repeated compiled describe check passes all45
surfaces, with the original two repetitions and strict hashes. Lifecycle is
temporary measured scope, not the final required full/full verdict or review
readiness. The cancelled earlier runs remain uncredited.

This subsequent signed audit-only head is reserved for the unmet final increase
to full. Production code, policy, spec and tests are identical to8ea95a10d;
only this sequencing record differs. Local full may qualify this clean head
while hosted lifecycle runs on8ea95a10d. Publish this head with CI-Tier restored
to full after lifecycle qualification, keeping the PR draft/non-mergeable during
the transition. Final review must bind the new head's actual full plan, run,
attempt and protected terminals plus the actual local full receipt. No higher
scope, passing result, cost saving or lead merge approval is asserted here.

Focused planner/timing/catalogue/runner controls and the updated public backend
ledger have passed during implementation. These are not full-corpus, hosted
new-head higher-tier acceptance, candidate cost or literal post-merge proof. Lifecycle remains
provisional until comparable hosted core/lifecycle/full whole-run cost is
measured; collapse it into full if savings are below the binding 20% threshold.
#2525 is still open and its actual policy/partition delta must be integrated if
it merges. #2535 remains open. No new architecture tracker or vendor is needed.

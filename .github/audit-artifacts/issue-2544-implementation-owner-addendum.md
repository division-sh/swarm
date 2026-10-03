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

## Local Instruction Clarification

V09's former "refuse lower request" wording is superseded by the gate's human
Local-Tier comparison. The local runner cannot authenticate a PR instruction
and must not fetch one. It records an explicit tier, or labels no-context core
as developer feedback. A reviewer must refuse a receipt thinner than the current
Local-Tier instruction. Invalid/mixed CLI selectors fail closed; `--full`
promotes. No authorization YAML/digest, locator, gate fetch or path selector is
introduced.

## Status

Focused planner/timing/catalogue/runner controls and the updated public backend
ledger have passed during implementation. These are not full-corpus, hosted
edited-body, candidate cost or literal post-merge proof. Lifecycle remains
provisional until comparable hosted core/lifecycle/full whole-run cost is
measured; collapse it into full if savings are below the binding 20% threshold.
#2525 is still open and its actual policy/partition delta must be integrated if
it merges. #2535 remains open. No new architecture tracker or vendor is needed.

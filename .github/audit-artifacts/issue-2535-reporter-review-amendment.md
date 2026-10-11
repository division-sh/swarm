# Reporter Implementation Review Amendment

Binding early review: #2535 comment6105744997. This is an implementation
checkpoint, not a final proof audit or a self-approved merge gate.

## Owner And Manifestation Repair

The native canonical owner remains `transactiontest.Slot` / `Attempt`. Both
500-row fixtures now consume `Collector.Replace` through the existing storetest
facade instead of `Stop` followed by installation. Replacement validates the
exact predecessor and publishes immutable successor options under the slot
lock; no concurrent Begin sees a nil observation gap. Already captured attempts
retain their predecessor. Stale Stop/restoration cannot remove the successor.

R07-R10/R17 now require atomic handoff as well as native acknowledgment:
`TestCollectorAtomicReplacementNeverLosesBegin` holds an actual Begin at its
slot-to-collector registration boundary, requires replacement to join it, then
proves old/new capture, acknowledged receipts and stale cleanup. The two-step
negative control demonstrates the old nil gap. The existing real selected-store
`TestFanOutGrantedActualCommitCountBothStores` consumes replacement on SQLite
and PostgreSQL, preserving the actual serving/read/retained-session controls.

Both fixture final cleanups consume `ValidateReporterJoined` after runtime and
store cleanup, retaining final old/successor snapshots rather than logging only
Active. `TestReporterJoinedRejectsHeldCleanupErrorAfterEarlyAssertion` proves
that the pre-teardown assertion cannot supply final credit and that a held
acknowledged cleanup error is rejected after it joins. The complete oracle was
clarified by reviewer-g at6105895380, as recorded below.

R10/R13 distinguish actual descendant drain from merely mixed-batch writes.
Before handoff the existing public/selected fan-out summary records whether the
500 descendants are already settled. Outstanding drain requires named native
delivery-settlement evidence after issuance, in the successor or an already
captured predecessor attempt. The full 500/504/502/2 settlement/payload/history
and presentation assertions remain. `TestReporterDrainRequiresSettlementOrAlreadySettledReadback`
rejects mixed claim/chunk receipts alone as drain proof. No synthetic obligation,
SQL observer, runtime scheduling or deadline is introduced.

The coarse full-coverage readers in testplanning's issue2564/late partition,
testtiming and testcatalog consume the exact full stress owner. Lifecycle census
is a selected-plus-explicitly-deferred union, not execution credit for the omitted
stress root. The current native full binding and supplemental-workload checks
still independently enforce executable ownership and both backend children.

## Native Outcome Correction And Open Question

The first implementation109cd6708 passed the served window on both stores but
failed conformance because its new global Failed==0 assertion was incorrect.
Native Attempt.Finish increments Failed for every unacknowledged attempt, even
rollback-only Finish(nil). CommitFailures and CleanupFailures are distinct.
Actual conformance had Other-only Failed379/155, with zero serving failures and
zero COMMIT/cleanup failures. No row or counter is erased or relabeled.

The correction preserves raw unacknowledged receipts, checks serving failures
and all actual COMMIT/cleanup failures, and retains final captured accounting.
The deterministic native rollback-only control proves this distinction. This
does **not** assert that every non-serving unacknowledged error is harmless.
The clarification request6105762790 is resolved by6105895380: global Failed==0
is not required; both collectors must join at Active0, serving claim/chunk
Failed0, global CommitFailures0/CleanupFailures0, with exact native receipts and
all public/selected assertions. Raw non-serving Failed/RollbackAttempts remain
unacknowledged evidence, never successful or declared harmless. Early-review
code findings were closed at491e17ee8, not final proof or merge approval.

The fresh491 census identified one new callback-type footprint: Replace's
redundant returned cleanup function. Remove that new result and register the
successor's existing Stop, without a registry/baseline increase or guard change.
Codex's draft2613 P2 also requires stable joined all-commit snapshots. Both old
stress roots now revalidate after harness/store teardown, with an explicit held
cleanup-error negative control. The immediate complete-journey validation stays.
These bounded repairs require fresh affected source/receipt qualification; the
passing491 window receipts are retained, not claimed as the new-head receipts.

The failed109 aggregate, catalog census omission and a mixed-source guard build
remain failed diagnostic evidence. The catalog owner omission is repaired.
The mixed-source build began before the in-flight atomic API edits; a later
standalone storetest compile passes. No such receipt is exact-head qualification.

## Unchanged Holds

The original stress roots and64-row one-second proof stay intact. New backend
children/selectors, 780s identity transfer, all original bounds and the two
full stress supplements remain required. No baseline, buffer, skip, legacy
compatibility, driver, vendor, or business semantic change.

New-root finite cost/placement, complete new/old both-store execution, actual
unit budgets, matched measurement, fresh master-nightly and strict nightly
blocking attestation remain outstanding. No review-ready PR or failure-class
closure is claimed. #2535/#2394/#2353/#2250 remain open; the existing native
proof/harness watchlist mapping remains the parent, with no new issue/node.

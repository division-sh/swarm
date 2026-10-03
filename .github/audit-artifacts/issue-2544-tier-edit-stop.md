# #2544 Hosted Higher-Tier Edit: Implementation Stop

Agent-g, 2026-10-03. Frozen implementation:
`8714bf5f0f4d3b0acc23b93dca93c0b0b201808c`.
Binding gate: https://github.com/division-sh/swarm/issues/2544#issuecomment-5965227266.

## Exact Observations

1. Core run `37101983331`, attempt 1, completed successfully on the frozen head.
   All 27 logical/physical Go units and required non-proof jobs passed. Whole-run
   assigned cost is 72.50 runner-minutes: 53.25 proof plus 19.25 other work.
   Wall time was 11.783 minutes, aggregate job queue time 13.65 minutes, and
   observed peak running jobs 20. These are actual candidate core observations,
   not lifecycle/full savings or fleet p90 acceptance.
2. Changed only the PR body's exact `CI-Tier: core` line to
   `CI-Tier: lifecycle`. No source push, base change or protection change.
   The edited event produced run `37102729055`, attempt 1, on the same head.
   Both hosted plans execute `74da0714ac82c3165c4d81249ea3490cd895b297`;
   the new plan explicitly selects lifecycle with 59 logical/physical units.
3. The new SQLite smoke check `111145296650` completed successfully at
   `2026-10-03T06:22:43Z`. At `06:26:54.738897Z`, heavier proof was still
   running/queued, but the only `Required test summary` was old core check
   `111144941081`, still completed/success from run `37101983331`.
   Both contexts are App 15368 on the same exact branch head.
4. The current PR body required lifecycle throughout that observation.
   No new pending or failing protected summary invalidated the old thin green.
   The PR itself remained `BLOCKED` / `REVIEW_REQUIRED`: its separate approval
   requirement prevents claiming that an actual unreviewed merge was possible.
   This is a demonstrated protected-summary invalidation gap, not a claim that
   the complete PR was observed mergeable.

Protected contexts: App 15368 `Required test summary` and `SQLite local smoke`.
The protection API also requires one approval, signed commits, linear history,
conversation resolution and an up-to-date branch. None was altered.

## Stop And Proof Disposition

The gate explicitly requires a new ruling when a post-success higher-tier edit
leaves stale thinner protected green. V08/V10 are not closed. Freeze runtime/
workflow implementation; do not add a check-status writer or another framework.
The summary's current-body revalidation works when that job eventually runs,
but its all-job `needs` boundary delays creation of the protected check.
The existing summary owner is therefore admitted too late for this condition.

The higher-tier run was cancelled after retaining this counterexample, and the
PR was converted to draft as a native, mechanical merge fence. The experimental
tier line is restored to the required full instruction. The local full attempt
was gracefully interrupted through its own runner and joined. Neither run earns
full, lifecycle, native-union or soak qualification credit. No final proof audit,
review request, class closure or >=20% lifecycle saving is claimed.

## Narrow Re-Gate Requested

The chosen class and owner map remain unchanged. Repair the protected-check
lifetime through the existing required-summary and plan/evidence owners, not a
second context writer, duplicate protected job name, publisher privilege or
retired YAML/digest/gate-fetch/locator machinery.

A candidate direction for independent disposition is to admit the existing
Required test summary owner at workflow start, retain a pending context while
joining this run's existing evidence, then perform the current exact-tier/head/
outcome validation. This is NOT implemented or approved by this artifact. Its
whole-job lifetime/budget and assigned-runner cost need an explicit ruling;
unchanged test deadlines cannot be silently repurposed as a wait allowance.
After a ruling, repeat the hosted same-SHA experiment and all required final
qualification/cost measurements on the repaired head.

#2535 remains open for literal master/scheduled acceptance and busy-fleet p90.
#2353/#2394/#1196 retain their independent obligations. #2525 remains open and
its actual merged delta still needs integration. No new issue, watchlist node,
runtime repair, vendor, or compatibility path is justified.

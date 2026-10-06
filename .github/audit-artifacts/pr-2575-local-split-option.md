# PR 2575: Local Split Option, Not Enacted

Prepared at the user's request on top of `a4bad4116`. This commit is local only;
the published repair head and its approval stay unchanged until the user selects
the split. It is not a diagnosis of a production slowdown or a green CI receipt.

## Exact Partition

Split the original `serveapp-other-late` into:

- `serveapp-delayed-commit-preservation`: the complete
  `TestIssue2394ServedOneSecondCommitPreservesTwoFullChunksBothStores` family,
  including BOTH original SQLite/PostgreSQL subprocesses. Both backend execution
  receipts are explicitly required. No backend or subtest splitting is used.
- `serveapp-other-late`: every other originally selected root, with unchanged
  standing-construction backend requirements and parent-owned helper deferrals.

Both units remain mandatory exactly once in lifecycle and full, absent from
core, with the original package, environment, count-1 and `full` budget class.
Go timeout remains unset (the same default 10 minutes); timing budgets and
their fixed evaluator buffer are untouched. No fixture, fault delay, cleanup,
workload, assertion, deferral, supported-surface or execution-evidence rule changes.

The existing source-root partition checker proves no omission, overlap, partial
root or newly uncovered serveapp root, both for the original finite selection
and for the complete lifecycle/full package. Omission and overlap mutations
fail that same checker. Existing active-root planning verifies all profiles and
the numeric helper remains deferred only to its actual owning lifecycle parent.

## Measured Inputs And Expected Cost

Primary Go JSON, not parent-only durations, supplies the following historical
decomposition. The delayed root reports parent elapsed zero because its two
backend children pause/run concurrently: use their MAX, not zero or their SUM.
Other complete top-level roots run sequentially in this unit.

| Successful source run | Original package | Delayed children MAX | Remaining complete roots SUM |
| --- | --- | --- | --- |
| 37193256098 / 8a9461df9 | 502.773s | 267.740s | 235.050s |
| 37296485086 / 238af3553 | 524.994s | 265.810s | 259.170s |
| 37448734512 / 83482f4ad | 596.555s | 336.550s | 259.990s |

Current PR run 37449261673 remains RED/incomplete at 600.032s. Its delayed
SQLite/PostgreSQL cells passed at 337.080s/208.220s; shared completed roots
include aliases 73.680s and publication 47.310s. Do not extrapolate a successful
whole remainder from an incomplete package. The observed 532s-to-681s job-time
history includes setup/build/upload as well as these package times.

Expected split package durations on latest passing conditions: approximately
337s delayed and 260s remainder. Conservative candidate planning allowance:
approximately 340-370s delayed and 310-350s remainder if the hosted CPU penalty
persists. Applying recent approximately 30-85s command/job overhead gives
approximately 370-455s and 340-435s jobs, respectively. These are PROJECTIONS,
not measured new-unit histories or adjusted ceilings; cold startup, compilation,
runner variance, service setup and queue delay are not promised away.

This is the best two-way top-level balance without weakening the indivisible
337s proof: assigning another root to the delayed unit increases the maximum.
It spends one additional existing proof-unit job, may add compilation/setup
overhead, and reduces the current package's critical path from about 597s to
about 337s when runners are available. It does not promise a 260s reduction in
fleet makespan when hosted runners queue. Source-bound generated historical
weights are NOT seeded with these unexecuted projections; the existing publisher
can record genuine exact-selection receipts after qualification.

## Local Readiness

Focused source/planning/envelope/negative-control checks and the full current
active-root plan binding were run on vemew. The exact split head must pass the
normal source-pinned complexity check before any push. No server2 window or
whole core/lifecycle/full test run is consumed while the option is undecided.
Hosted exact-head lifecycle, both complete new units and review still remain
required if the user elects this option. Prior red receipts remain recorded.

No new scheduler, framework, third-party code, compatibility path, generated
historical timing model, debt baseline, global guard allowance or platform/runtime
semantics are changed. The authorized alternative is a local configuration and
proof change only; #2542/#2151 closure is not implied.

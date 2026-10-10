# #2542 continuation: workflow coordinator raw argument retirement

The shared bus workflow coordinator constructor had an unused raw database
parameter. Removing it and every six forwarding arguments leaves its complete
selected dependency graph unchanged: opaque workflow persistence, run lifecycle,
pipeline obligations, deliveries, dead letters, decisions, proposed effects,
human tasks, expiry owners, runtime/flow routing and normal receiver execution.
Component initial-stage preparation, exact activation acknowledgment and
finalization are unchanged. This is argument retirement, not a new coordinator,
role discovery or ownership reconstruction.

The finite constructor and component-helper snapshots, plus five updated existing
whole-caller snapshots, require exact dependency and execution preservation.
Foreign persistence/lifecycle, missing delivery and changed receiver execution
are negative controls. Existing complete caller oracles normalize only this
enumerated obsolete argument; they retain the rest of each execution journey.
Focused both-store source/run construction and actual mixed/ancestor/undeclared
route witnesses provide execution proof. Remaining raw reads in those callers
stay parent debt, rather than being hidden or granted new permission.

The approved migration gate and selected_runtime_store_projection.raw_sql_policy
apply. No production behavior, spec contract, raw SQL allowance or framework
changed. Census/guards/finite type proof and exact focused receipts are recorded
on #2542. No tier/hosted/server2/fork-deadline or parent closure claim.

# #2542 continuation: shared bus run construction

The existing bus run-context helper now receives the original selected owner
through its exact EventStore and RunFixtureStore roles. It no longer admits a
second store from a raw pool. Every known caller in the bus package forwards its
identical selected owner; the source-specific and default wrappers preserve
artifact admission, context lineage, exact run, fixed start time and lifecycle
refusal. Four callers with no remaining pool-dependent work use the existing
native PostgreSQL factory, including its sole fixture close owner.

Seven callers still have separate raw observation/coordinator arguments and
retain their original construction. They no longer reconstruct within the shared
run-context helper. These are explicit remaining parent debt, not compatibility
approval or a claim that the complete bus fixture family is closed. Existing
fake transaction-context assertions remain unchanged and tracked separately.

Finite whole-function snapshots cover both helpers and all eleven callers.
Two existing receipt/scope snapshots have their After updated, rather than a
duplicate inventory. Their existing complete-execution oracles normalize only
the enumerated construction/forwarding cuts; all prior timing, barrier, returned
error, receipt and dispatch assertions stay required. Exact original owner,
artifact, time, run and error controls reject drift. Both-backend positive
controls observe original writer commits and identical active-run source/context
and lifecycle; representative real post-commit, held-dispatch, receipt recovery
and source-specific routing roots run under race.

No new SQL, raw classifier, reconstructed coordinator, fallback, schema policy,
production port or lifecycle framework was introduced. The governing raw_sql_policy,
existing watchlist and approved broad migration gate apply. No runtime semantic
change or new ruling is needed. Final debt and proof receipts are on #2542;
core/full, server2, hosted CI, fork-deadline and parent closure remain unclaimed.

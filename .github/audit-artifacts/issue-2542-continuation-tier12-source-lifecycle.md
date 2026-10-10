# #2542 continuation: Tier12 source lifecycle witness

## Owner and Exact Consumer Migration

Governing boundary: selected_contracts.selected_runtime_store_projection.raw_sql_policy.
assertSourceRunLifecycle now accepts the existing runtimebus.
RunLifecycleReadPersistence port and consumes LoadRunLifecycleSnapshot from the
original selected run-lifecycle owner. It preserves exact requested run, status
trim/comparison, ended_at presence and the original author-context read cut.
Returned run identity is explicitly checked before accepting lifecycle evidence.
The canonical snapshot's strict decoding replaces the local SQL/scan interpreter;
no new reader, SQL operation, storetest facade or classification is introduced.

Both helper callsites in the real selected-contract Tier12 fixture now pass h.pg
instead of h.db. The whole-root finite inverse permits only those two argument
changes. Source freeze, clean discard, source immutability, real fork execution,
agent receipts, runtime diagnostics, branch divergence and historical-replay
refusal remain unchanged. Tier12 remains explicitly PostgreSQL-scoped; the
both-store observer control below does not claim SQLite Tier12 execution support.

Old raw helper signature, direct run SELECT and scan are invalid. Other raw Tier12
counts, snapshots and runtime-row helpers remain named migration debt, not
closed by this source-lifecycle cohort or preserved as a compatibility seam.

## Proof and Closure Boundary

TestTier12RuntimeFork_SelectedContractForkExecutionFixture executes the real
PostgreSQL journey under race. TestCatalogSourceRunLifecycleUsesSelectedOwnerBothStores
executes the same helper on actual SQLite/PostgreSQL stores with h.db disabled,
proves no write/retained transaction and refuses borrowing evidence for a missing
run. TestRunFixturesUseExactSelectedLifecycleOwner independently exercises the
existing canonical reader's running/paused/failed/cancelled lifecycle snapshots,
including exact terminal timestamp presence, on both stores under race.

The finite whole-helper shape and hostile run/status/ended/error mutations
preserve its exact assertions. The whole Tier12 consumer inverse proves no
workload/capability scope was weakened. An initial codemod compile error was a
mistaken test helper name; its failed receipt earns no proof credit and is kept.
The corrected oracle reads the existing finite inventory and requires exactly
the two reviewed recipes. No production, assertion, deadline or budget changed.

Existing run-lifecycle owner and fixture-authority watchlist remain sufficient.
No new framework, compatibility, tracker or authoritative runtime-spec change.
This canonicalizes the source-lifecycle witness family, not #2542/#2151 or the
remaining Tier12/storage/constructor debt. Counts and final sweep receipts are
recorded in the issue comment; no core/full, hosted CI, fork-deadline or parent
closure qualification is claimed.

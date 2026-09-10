# Cycle-3 Binding Implementation Mapping: #2442 / #2321

Approval: #2321 issuecomment-5611206250 and PR #2441 issuecomment-5611197913.
Full issue #2442 body/thread and the cited spec sections were read at 4c7e22872.
This additive mapping precedes implementation; no new gate is requested.

Chosen #2442 class is PostgreSQL transaction outcome and transaction-owned
resource release on every exit, with two legitimate lifetimes. Parent #2250 and
larger transaction-envelope redesign #2412 stay open. #2439 stays SQLite-only.

| Owner | Implementation / proof commitment |
| --- | --- |
| postgres.runTransaction, ordinary read/write | Deferred rollback/disposal before connection release even during panic; retain independent primary/cleanup errors, actual owning cancellation, no retries. Real pool1 read/write panic, callback error, callback TxDone, auto-rollback/Commit cancellation and deadlines, driver rollback/commit/close controls, next-operation progress. |
| RunAuthorityTransaction, beginTx/endTx/rollbackSessionTransaction | Every exit releases transaction operation without releasing healthy retained possession. Panic also joins the operation cancellation worker. Preserve independent run/commit/cleanup failures under cancellation. Test next retained operation and real advisory possession; fence/dispose only unsafe exact sessions. |
| AdvisoryLockLease.RunTransaction, startup/reset/maintenance/selected-fork | Existing retained consumers; test actual possession/reset/fork lifetimes, not only helper return values. |
| RunSessionTransaction | Repo-wide Go search finds only its declaration and its internal call to RunAuthorityTransaction, no caller. Delete the unused alternate entry, not retain compatibility. |
| Selected delivery scans, named selected-store operations, API and runtime | Execute each LSF-033..038 surface explicitly; attribute query57014 and wrapped cancellation at exact operations, not by text. No coordinator blanket suppression. |
| Runtime.stopWithOptions | Retire/join continuations before Manager shutdown; preserve accepted carriers, startup abort, producers, work join and shared grace semantics. Deterministic blocked-dispatch/manager fence proof and exact LSF-039 conformance. |
| Persistence registry | Classify Coordinator.finish's context.CancelFunc as lifecycle, not persistence authority. Regenerate through sanctioned test, inspect each added/deleted finding from owner edits. |

Existing authoritative spec delta accompanies this mapping. No generic transaction
framework, new lifetime owner, migration/compatibility, retry, global SQLSTATE
filter or weakened reporter is authorized. If actual dependency cycles or another
owner are discovered, report the concrete evidence rather than widen silently.

Gate matrix also requires begin/acquire failure, empty success, independent
callback + cancellation, exact physical disposal vs already-closed bookkeeping,
ambiguous commit/no replay, rollback error, and positive pool/session reuse.
All named manifestations require separate final proof rows. No current closure
claim; focused proofs first, then one instrumented original-load full-profile
swarm-test. J5/LSF-032 stays observed/unclassified if that run passes. A recurrence
requires child ordinal/PID, phase/cause/stack and terminal evidence, not reruns to
green. No additional live-provider or Telegram message is needed.

Watchlist refinement 68dd4a1 is lead-owned and already pushed. Existing
atomic_runtime_state_mutation and shutdown_and_runtime_lifecycle nodes are the
mapping; no new issue or POTENTIAL_ISSUES entry. #2442 is the approved concrete
child; #2319/F's accepted onboarding split stays unchanged.

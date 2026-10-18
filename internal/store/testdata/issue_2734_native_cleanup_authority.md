# #2734 Exact Native Cleanup Authority Accounting

The existing owner composition is authorized by
https://github.com/division-sh/swarm/issues/2734#issuecomment-6245964485;
one-writer sequencing is pinned at
https://github.com/division-sh/swarm/issues/2734#issuecomment-6246277793.

Preflight at cb2a465519 found thirteen new resolved sites and one stale moved
site. These are not exceptions, public raw-SQL access, debt additions or a
baseline increase. Classify each exact current site below using existing owner
labels. No scanner, allowed-disposition set or excluded-source rule changes.

## private-backend: Twelve Sites

All are in the existing SQLite native transaction termination owner, parallel
to the already-classified PostgreSQL joinTransactionRollback composition.

| Declaration | Exact sites | Ownership |
| --- | --- | --- |
| `Backend.runTransactionOnceOutcome` | local `rollbackTx`; calls to `finalizeRollback` and `transactionFinalization.closeConnection` | The original exact native handle is captured only for its cancellation rollback. No borrower can commit it, replace it, or keep it after the native owner joins. |
| `finalizeRollback` | `tx` parameter, `stopRollback` callback parameter, `joinCancellationRollback` call | The callback is the native owner's context.AfterFunc stop function, not a caller-supplied SQL callback or authority registry. Its only result selects direct rollback versus joining that already-started rollback. |
| `joinCancellationRollback` | `tx` parameter, `stop` callback parameter, exact `sql.Tx.Rollback` call | Native rollback acknowledgement is observed directly, and its real return joins before ordering release/connection cleanup. ErrTxDone does not prove rollback; independent errors remain visible. |
| `transactionFinalization.closeConnection` | `conn` parameter, exact `sql.Conn.Close` and existing `CloseConnection` calls | Releases the same pinned connection after joined rollback and ordering release. Failed/uncertain termination retains discard; only a healthy, positively joined release can contribute to cancellation evidence. |

The stale `Backend.runTransactionOnceOutcome -> CloseConnection` row is removed
only because that call moved to the exact private helper above. Its old row and
source remain in retained signed history; no historical evidence is rewritten.

## typed-process-local: One Site

`ManagerRunOccurrence.CancelledByShutdown -> context.Value` reads the existing
private acceptedLeaseContextKey and requires the exact lease gate and owning
Manager occurrence. It does not obtain a database/transaction, construct a new
lease, grant permission, or turn a copied generic context into execution rights.
Reset, earlier caller/runtime/standing retirement, foreign Manager and unadmitted
contexts remain negative controls. The closed shutdown cause stays a leaf for
existing owner-error walkers while Is(ErrRetired) preserves public compatibility.

## Execution And Guard Receipts

Both-store before-entry/inside-native attachment cuts retain the actual native
rollback, abandonment acknowledgement, immutable construction, fresh Manager
readiness and no duplicate event/resource checks. Existing native fault roots
preserve independent callback/rollback errors, unknown/acknowledged COMMIT,
deadline/cancellation distinctions and connection reuse/discard. The existing
work-lifetime owner root includes the exact cancellation-origin controls; the
legacy-walker counterexample remains recorded RED before its Is correction.

The normal authority registry, debt ratchet, effective-method-set/fixture-role
hostiles and exact-head preflight remain required. No existing raw finding is
reclassified as benign debt and no production test hook or transactiontest
reader/call site is added. Cost: zero new roots.

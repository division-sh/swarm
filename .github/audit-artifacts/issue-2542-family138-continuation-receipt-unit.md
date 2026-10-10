# Family 138: continuation registration receipt consumer

## Ownership and Boundary

The five registration roots formerly drove a test-only context callback queue.
Production standing-service persistence instead returns
DeliveryContinuationRequired as acknowledged mutation evidence; the existing
workflowInstanceStore consumes that receipt against current registrations.
Four finite recipes move the tests to that actual production consumer. The
duplicate-registration root is unchanged. The unused queue method and its
transaction-post-commit forwarding helper are deleted, not replaced.

The channel-gated role is an explicit unit collaborator, not a selected-store
transaction or durable commit simulator. Only SuspendStandingService is supplied;
other standing-service operations are not implemented by this collaborator.
No production rule, attachment identity, generation admission or backend changes.

## Temporal Proof

Registration precedes starting the blocked operation in the predecessor/current
authority and refusal/cleanup cases. The observer covers the entire invocation.
Pending receipts signal nobody; releasing the typed receipt signals only current
registrations. Predecessor Release remains exact and idempotent. Registration
during a blocked operation receives the later acknowledgment, whereas registering
after a consumed receipt does not replay it. Missing persistence, unacknowledged
failure and no-continuation receipts signal nobody. A committed receipt still
signals with its exact cleanup error. Assertion failure cancels and joins the
blocked operation; registrations are released.

Six unit roots pass under race. The separate existing
TestStandingServiceAcknowledgedCleanupErrorStillSignalsContinuationBothStores
passes SQLite and PostgreSQL under race and proves real committed cleanup-error
and rejected-mutation behavior. Its remaining raw setup is NOT migrated or
credited by this family.

A source overlay injects signalDeliveryContinuations into REAL production
SuspendStandingService before entering persistence. The current-authority root
and all three receipt-disposition children fail at the premature-notification
assertion, without build failure or timeout. Production remains unchanged.

## Systematic Consumption and Accounting

All callers of the removed queue were the four migrated registration roots.
Production Reconcile, ReconcileSet, Suspend, Resume and Reset still consume the
same standing-service acknowledgment owners. The separate both-store proof is
an execution companion, not proof that this unit collaborator persists data.
Other fake transaction context/runner consumers remain explicitly parent-owned.

The whole registration file is added to the native authority guard, including an
unlisted raw-parameter adversarial control. Finite snapshots, hostile/type
controls and the four-root ratchet/registry/native sweep pass. Complete census:
12,615 findings / 9,306 raw sites, unchanged, zero additions/increases. This is a
proof/capability retirement improvement with ZERO debt-count credit.

Receipts: ~/.cache/swarm-2542-local-20261009-family138-*.jsonl.
The final unit receipt supersedes the earlier proof whose observer attached after
starting the invocation. No tier, full closure, measured flake-rate or parity
migration claim. Existing #2542/#2151 and shared-observability/lifecycle watchlist
ownership remain sufficient; no new issue or framework.

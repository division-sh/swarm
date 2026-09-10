# Pinned Native Operation Integration

This directory imports github.com/lib/pq v1.11.2, including its LICENSE.
The root module replaces that exact dependency with this source tree.
The patch is confined to native operation ownership and its tests; existing
SQL parsing, protocol codecs, configuration, authentication and pooling remain.

## Private Integration API

- NewOperationScope(owner) creates a record for one owning operation.
- scope.Context(ctx) attaches the record and preserves ctx's Done and values.
  context.WithoutCancel(scope.Context(ctx)) detaches database/sql transport,
  but does not detach the driver's watch of the actual scope owner.
- BindOperationScope(raw, scope), called under sql.Conn.Raw, retains the record
  through physical connection cleanup. Driver wrappers may explicitly forward
  their BindOperationScope method. Nil unbinds a settled retained connection.
- scope.Err() returns independently observed native/worker/cleanup failures.
  It is not a connection-health or possession verdict.
- scope.Wait() joins registered native work and driver transaction settlement,
  then returns Err. First roll back or physically dispose a failed transaction.
  Read Err again after connection cleanup, which may produce additional errors.
  Do not start new operations concurrently with Wait.

Explicit binding persists through transaction completion. Healthy ResetSession
clears the prior binding; bad ResetSession preserves it for disposal evidence.
An operation-local implicit record covers unbound Query/Exec/prepared/Begin calls.
An implicit Begin record is retained only by the active transaction, never by
the physical connection binding. Failed Begin or transaction settlement ends it;
successor work and independent advisory cleanup do not inherit that context.

## Arbitration

Each native operation has one cancellation gate and a joined worker. Query
ownership transfers to rows through final ReadyForQuery, including result sets,
Next, Close, and panic. Native ErrorResponse observation precedes callback access.
The original error is recorded before handleError can replace it with ErrBadConn.

An owned-stop error is produced for a rejected, canceled admission or a native
operation whose local cancel action won and succeeded. A native 57014 is eligible
only if local issuance preceded ErrorResponse observation and the cancel socket
completed successfully. The original response is diagnostic Native() evidence,
not an independent error leaf. Independent server, transport and cleanup failures
remain errors, including when they race cancellation. Cancellation does not
poison a successfully drained session.

This is local arbitration, not proof of which competing server cancellation
caused a wire-identical 57014. CancelRequest completion provides no such identity.
Failed cancellation or undrained protocol fences the connection. COMMIT checks
cancellation at admission; admitted COMMIT and cleanup then settle uncanceled.
Commit errors are never normalized as owned query stops and callbacks are never
replayed by this integration.

COPY has a separate upstream streaming producer and is not a selected-store
surface. Explicit scopes reject COPY preparation before sending it. Unbound COPY
continues through pq's original producer, with its CopyData cancellation worker
joined; it does not enter the selected native gate.

## Verification

Run the deterministic native gate tests explicitly from the root module:

    go test -race github.com/lib/pq -run '^TestNativeScope' -count=10

The required CI static-checks job runs this suite separately (count=1). Root
`go test ./...` does not discover this nested module; its proof receipt is separate
from the root package suite. TestCIRequiresPinnedNativeScopeProof guards the invocation.

The root module's PostgreSQL backend tests supply real-server ordinary, retained,
prepared, streaming, independent-cancellation and next-possession/pool controls.

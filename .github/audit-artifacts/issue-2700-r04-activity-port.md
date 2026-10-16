# #2700 R04 Activity Adapter Engineering Handoff

Implements the joint boundary approved by reviewer-g6221888821 and
reviewer-a6221925281, handoff6221967140. This is the requested concrete port,
not another design gate or R04 GREEN. No push before B; #2592 comes first on
B's landing. #2701's first dead-letter/ordering repair stays separate.

## Exact Native Port

E owns the following declarations in store/internal/startupownership, beside
the opaque cut issued ONLY by its fixed retained process session:

```go
type DisplacedHTTPActivityRecovery interface {
    ReconcileDisplacedHTTPActivitiesTx(
        context.Context,
        *mutationprotocol.Attempt,
        DisplacedHTTPActivityCut,
    ) (pipeline.ActivityRecoverySummary, error)
}
```

A implements that method on backend/activityjournal.ActivityPostgresOwner and
ActivitySQLiteOwner. E's startup owner depends on the interface, NOT a concrete
activityjournal import. A's adapters consume E's cut. The existing startup owner
does not depend on activityjournal (verified with go list -deps); keeping that
direction avoids a cycle. Do not put the port on the public selected store or
lend another transaction, backend handle, callback or caller-mintable flag.

The cut supplies this canonical proof operation, still E-owned:

```go
func (cut DisplacedHTTPActivityCut) ProveDisplacedGrantTx(
    context.Context,
    *mutationprotocol.Attempt,
    processbinding.ActivityExecutionBinding,
) (runtimestartupownership.GrantEvidence, bool, error)
```

It checks the EXACT supplied attempt/transaction against the retained cut,
revalidates the fixed successor and native generation fence, and reads BOTH
grant_id/state_version through the canonical grant decoder. The boolean is
the owner's observed displacement disposition, not a caller-supplied permit:
true only for a positively proved supported displaced normal grant; a proved
current/selected grant is excluded unchanged. Missing/contradictory referenced
evidence errors and rolls back the step. No age, event ancestry, GET, latest
snapshot substitution, fresh claim or synthetic agent authority.

A additionally validates the exact row's run/source/mode/request/input/loop and
original binding through existing native owners before settlement. The grant
reference alone is never execution authority. E keeps process/selected grant
interpretation; A does not duplicate its decoder or latest/currentness logic.

The now-defined leaf DTO is:

```go
type ActivityExecutionBinding struct {
    Version      string `json:"version"` // activity-execution.v1
    GrantID      string `json:"grant_id"`
    StateVersion uint64 `json:"state_version"`
}
```

It lives in runtime/core/processbinding. Shape validation rejects missing or
noncanonical IDs, unknown versions and nonpositive/out-of-range state versions.
It conveys immutable attribution only. Normal/selected identity stays in the
referenced canonical snapshot, not duplicated in this column.

runtime/pipeline.ActivityRecoverySummary has Results []ActivityAttemptRecord.
Those values are native result candidates until the enclosing native ACK; they
are not a second queue or a permission to publish. A must atomically conserve
the original uncertainty result plus the existing durable publication/readback
obligation. E may expose the summary only after acknowledgement; post-ACK
publication goes through the existing result owner. Recovery refusal rolls
back all rows/obligations of that step and keeps dispatch closed. ACK loss or
repeated boot rereads the same result, not a new timestamp or provider call.

## Complete Writer Boundary

| Path | Disposition |
| --- | --- |
| Start / Claim | Bind the actually installed fixed normal/selected role atomically before launch; validate exact original reference and current process/grant/selected fence. Never replace a duplicate's attribution. |
| Complete / ordinary MarkUncertain | Load/match the stored original reference and exact attempt; hold currentness and selected fences through native commit before result/story writes. |
| Private recovery adapter | Only the E-issued transaction-bound cut can settle displaced work under successor authority; original attribution remains immutable. |
| Exact terminal Load/result reuse | Historical reference admission by BOTH keys, not current permission. An obsolete grant need not remain live. Missing referenced history refuses rather than manufacturing it. |
| runforkpersistence/run_fork_activity_generation.go terminal copies | Historical/non-executing. Preserve exact result/time/lineage; no copied reference or NULL can become a child launch/recovery grant. |
| activityjournal/fork_history_fixture.go on B/E's pin | Historical/non-executing fixture writer, absent from A's pre-B base. Must be composed on actual B; no production fallback or copied-terminal promotion. |

Next work is native column/codec plus original-role binding and all four writer
fences, followed by the exact private adapter. This declaration and the DTO's
race x3 proof do NOT qualify the native fence or actual takeover. Both-store
live duplicate/takeover orderings, still-alive predecessor refusal, selected/
foreign/current isolation, rollback, lost ACK, historical terminal reuse and
one physical HTTP call/one durable uncertain outcome remain mandatory.

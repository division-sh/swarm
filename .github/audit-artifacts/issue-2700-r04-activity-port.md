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

E's concrete historical reader is pinned in6222592959, reserved to E:

```go
func ReadActivityExecutionGrantTx(
    ctx context.Context, tx *sql.Tx, grantID string, stateVersion uint64,
) (runtimestartupownership.GrantEvidence, error)
```

It lives in store/internal/startupownership/activity_execution_grant.go and
checks every native row projection against the exact decoded snapshot. A will
compose the signed implementation; its declaration is not current permission
or a reason to duplicate the decoder. Fixed-role and native currentness fences
remain A's writer responsibility, distinct from this historical read.

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

runtime/pipeline.ActivityRecoverySummary now has Results []RecoveredHTTPActivity.
Each value embeds the original ActivityAttemptRecord and carries Request
events.Event plus BundleHash from the same native recovery read. This is the
finite post-ACK carrier correction requested by E in6225128402, not a second
queue, transaction ACK or permission. A must atomically conserve
the original uncertainty result plus the existing durable publication/readback
obligation. E may expose the summary only after acknowledgement; post-ACK
publication goes through the existing result owner. Recovery refusal rolls
back all rows/obligations of that step and keeps dispatch closed. ACK loss or
repeated boot rereads the same result, not a new timestamp or provider call.

The A-owned production entry is:

```go
func (pc *PipelineCoordinator) PublishRecoveredHTTPActivity(
    ctx context.Context,
    recovered RecoveredHTTPActivity,
) error
```

Signed local0dbb5fd0ed implements this exact entry. It validates the retained
request/source/terminal content, rereads the ORIGINAL configured native journal
and requires exact durable identity/content/time before delegating to existing
publishExistingActivityAttempt -> publishJournaledActivityResult. A missing,
started or changed row refuses without publication; no HTTP/claim/handler replay
is available through this entry. E owns calling it after recovery ACK and before
ordinary/selected dispatch release, under its startup fence. A publication error
must keep that startup closed. Its three focused component roots PASS race x3
(pipeline1.201s), along with SQLmock native-frame refusal controls1.056s, receipt
amendment2-recovery-publication-race3-candidate1.log. These are not genuine native
rollback, ACK-loss or takeover; those remain separate mandatory proofs.

## Complete Writer Boundary

| Path | Disposition |
| --- | --- |
| Start / Claim | Bind the actually installed fixed normal/selected role atomically before launch; validate exact original reference and current process/grant/selected fence. Never replace a duplicate's attribution. |
| Complete / ordinary MarkUncertain | Load/match the stored original reference and exact attempt; hold currentness and selected fences through native commit before result/story writes. |
| Private recovery adapter | Only the E-issued transaction-bound cut can settle displaced work under successor authority; original attribution remains immutable. |
| Exact terminal Load/result reuse | Historical reference admission by BOTH keys, not current permission. An obsolete grant need not remain live. Missing referenced history refuses rather than manufacturing it. |
| runforkpersistence/run_fork_activity_generation.go terminal copies | Historical/non-executing. Preserve exact result/time/lineage; no copied reference or NULL can become a child launch/recovery grant. |
| activityjournal/fork_history_fixture.go on B/E's pin | Historical/non-executing fixture writer, absent from A's pre-B base. Must be composed on actual B; no production fallback or copied-terminal promotion. |

The native column/codec, original-role binding and all four writer fences,
private adapter, sealed capture and runtime installation are now staged in A's
signed e0b5c7b395 source. They do NOT qualify genuine takeover. E's prerequisite
question6224909578 is answered by the explicitly integration-only common source
8984035819cc962bb3d0da13712ae01720919ced (A e0b5c7b395 + E b7b7c08de7),
treecf1920926485e9ab99083d37d30155149d9ac565, handoff6224987535. No checkout,
push, assembly enablement or compiled qualification is implied. E still owns
the two native assembly bindings and startup recovery call. Both-store live
duplicate/takeover orderings, still-alive predecessor refusal, selected/foreign/
current isolation, rollback, lost ACK, historical terminal reuse and one
physical HTTP call/one durable uncertain outcome remain mandatory.

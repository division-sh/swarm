# Issue #2444 consumer census review

## Final Integration Disposition

The findings below are the preserved review record, not remaining open blockers.
C1/C2 were repaired in the existing LLM/session/conversation owners. The
bounded-writers receipt records 72 repeated runtime consumer cases, 24 repeated
real PostgreSQL cases and the full LLM package race pass, including exact
successor cleanup and response-plus-error without another provider request.
C3 was repaired in both authority-maintenance owners; the authority-maintenance
receipt records acknowledged/no-ACK outcomes and joined release-error barriers.
The clean final OpenRPC conversation.fork race count3 rerun passed (10.335s),
superseding the concurrent-compile attempts below. Final integrated qualification
is recorded separately in `issue-2444-postimplementation.md`.

## Scope and status

Read-only review of the shared `/tmp/agent-g-2444-implementation` worktree, not baseline `a32749a31` or the disposable prototype. Production files were changing concurrently. The table below records the latest source read in this pass; it is not frozen-tree qualification. This pass changes only this report and runs no tests or full suite.

Authority: [approved implementation gate](https://github.com/division-sh/swarm/issues/2444#issuecomment-5623494272), current `platform-spec.yaml`, and implementer guidelines. Review targets were admitted SQL consumers, retained authority, captured caller contexts, result suppression, and the named atomic `conversation.fork` exception. Other agents own active migrations; no duplicate production edits were made.

## Actionable findings

All three findings below are source-proven control-flow defects, not newly executed fault-injection receipts. Paths and lines are relative to this repository and may move during integration.

| ID / priority | Exact consumer evidence | Consequence | Required existing-owner action / relay |
| --- | --- | --- | --- |
| C1 / P1 | `internal/runtime/llm/api_runtime.go:205`, `cli_runtime.go:248`, `openai_responses_runtime.go:208`, `openai_compatible_runtime.go:207`, `mock_runtime.go:152`: deferred `_ = r.sessions.Release(ctx, lease)` | Independent release SQL/handoff failure is discarded; caller cancellation can refuse cleanup admission. A provider response can succeed while lease cleanup failed. | Faraday/runtime LLM owner: detach mandatory cleanup from caller cancellation and join its error into the result. Test success plus release failure, and canceled caller plus required release. Do not discard a successful provider result merely to hide its cleanup failure. |
| C2 / P1 | `internal/runtime/llm/session_rotation.go:59-64,101-106` returns nil on `Rotate` error, even when the store returns a committed successor lease. `prepareManagedSessionForTurn:38-43` consequently releases the old lease. Acquisition at `session_rotation.go:24-27` and provider `StartSession` (e.g. `api_runtime.go:117-119`) also returns before handling a nonnil committed lease. Parse-failure call sites (`api_runtime.go:282`, `cli_runtime.go:430`, OpenAI responses `:290`, compatible `:289`) accept rotation only when its error is nil. | Store-side preservation is lost at the next consumer: acknowledged successor identity/lease can be abandoned, old lease released instead, and in-memory session left stale. Postcommit errors may also be suppressed in parse-failure branches. | Faraday/runtime LLM owner: consume the exact nonnil acknowledged lease independently of error, update/release the correct identity, and preserve the error. Add acquire/rotate committed-plus-error controls through actual runtime consumers. |
| C3 / P1 | `internal/store/internal/startupownership/authority_maintenance.go:83` ignores deferred PostgreSQL lease release; `:104` ignores SQLite possession release. `internal/cliapp/store_authority.go:93-107` treats nil repair error as successful CLI completion. | Explicit possession cleanup failure is hidden behind successful durable repair and successful command exit. SQLite release actually joins unlock/descriptor-close failures (`sqlite_possession_unix.go:282`), so this is not merely a void bookkeeping operation. | Parent/authority repair owner: join release failures into the existing return while preserving the repair outcome. Test acknowledged repair plus release failure on the owner, not a new cleanup framework. |

Coordination is through parent relay; no direct agent messaging tool was available. Kepler owns event/runlifecycle; Faraday owns LLM. The latest named store-side siblings below no longer require the previously identified correction. C1-C3 remained present when this report was written.

## Named handoff siblings: latest disposition

| Family / owner | Current exact path | Committed evidence / failure disposition | Review status |
| --- | --- | --- | --- |
| Inbound publication / Kepler | `internal/store/internal/backend/eventpersistence/inbound_publication.go:133,164-167`; SQLite `sqlite_inbound_publication.go:30,57-60` | Uses `runPrivateAuthorActivityMutationOutcome`; only unacknowledged outcome zeros the result. Acknowledged result is returned with `errors.Join(err, handoff.Commit())`. Thus owner cleanup failure cannot skip handoff or erase the record/publications. | Earlier error-only-runner defect is corrected in current source, both stores. Pure handoff error already preserved the result before that correction. |
| Stop/pause/continue / Kepler | `internal/store/internal/backend/runlifecycle/run_control.go:89,134-137`; `run_control_sqlite.go:78,123-126` | Same explicit outcome gate; acknowledged state survives cleanup/handoff error, and handoff is still attempted. | Corrected in current source, both stores. |
| SQLite LLM acquire / Faraday | `internal/store/internal/backend/llmpersistence/sqlite_sessions.go:60,115-118` | Returns lease and hydrated conversation with joined error after acknowledged commit. | Store boundary corrected; C2 remains downstream. |
| SQLite LLM rotate/reset / Faraday | Same file `:217-273,372-416` | `WithCandidateHandoffOutcome` retains acknowledged lease/summary even when later error is nonnil. | Store boundary corrected; C2 remains downstream for rotation. |
| SQLite LLM release/adopt / Faraday | Same file `:164-185,333-363` | Error-only public result is intentional; internally the committed bool controls live handoff, so cleanup error no longer skips it. | Store boundary corrected; C1 remains in provider cleanup callers. |
| Shared handoff | `internal/store/internal/runhandoff/candidate_handoff.go:75-104` | Tests explicit bool, never callback success as COMMIT evidence; joins existing error with handoff error after acknowledgement. Generic result helper zeros only unacknowledged result. | Source checked. |

The event/runlifecycle outcome adapters were traced to backend `RunTransactionOutcome`, not inferred from their names (`eventpersistence/owner.go:148,174,193`; `runlifecycle/owner.go:212,238`; `llmpersistence/owner.go:92`). These source checks do not provide new executed cleanup-fault coverage for each sibling. Existing `TestResultSiblingsPreserveAcknowledgedHandoffOutcomeBothStores` covers publication/delivery/candidate, not automatically every inbound/run-control/LLM branch; its existing-run publication handoff case is explicitly a healthy control.

## Remaining SQL census classifications

| Surface | Evidence / disposition | Limit |
| --- | --- | --- |
| Transaction callback context capture | External AST census inspected production `*sql.Tx` callbacks under `internal/store/internal`, checking free outer `ctx`, `txctx`, and `sqlCtx`; no matches. Script `/tmp/agent-g-2444-review-contexts.go`; command `go run /tmp/agent-g-2444-review-contexts.go internal/store/internal`. | Bounded name-aware static search, not a proof against all aliasing or future concurrent edits. |
| Raw writers / autocommit DML | Reviewed raw SQL census in `/tmp/agent-g-2444-sql-review-census.txt`; no additional actionable autocommit DML bypass found in inspected production calls. Raw `BeginTx` outside existing backend owners inspected here were read-only snapshots. | Not an assertion that every possible SQL string or consumer is qualified. Active writer migrations remain their owners' responsibility. |
| API custom advisory acquisition | `internal/store/internal/apiidempotency/owner.go` custom raw acquisition is a fresh, ungranted connection's cancellable admission wait; request load/write use existing closed owner units. Lease release uses `ReleaseTerminal(context.WithoutCancel(ctx))` at `:58`. | This exception does not authorize cancellable SQL under already-held retained authority. |
| Reused pipeline authority / generic schedule claims | Reused pipeline acquisition uses default owner path rather than API custom callback. Generic schedule owns its connection, mutex, and key map with detached admitted acquire/release SQL. | No new defect demonstrated in these inspected paths; not an independent rerun of their tests. |
| F1 optional key, TTL, source and generic guard | `runforkpersistence/conversation_fork_api.go` validates exact public method/actor, uses existing normalized request lease, and commits only a completion-specific callback with fork creation. Empty/trimmed key stays unkeyed. Existing API owner preserves actor/method/key/hash and TTL; generic API callback remains outside this transaction. Source validation remains in `conversation_fork_lifecycle.go`. | No additional defect found. Named exception is `conversation.fork`, not an arbitrary callback transaction. |

## F1 implementation receipts

These are earlier executed receipts for this implementation worktree, not executions made by this read-only review and not qualification of subsequent concurrent edits. F1 source diff was captured at `/tmp/agent-g-2444-implementation-f1.patch`.

### API matrix

```sh
go test ./internal/apiv1 -run '^Test(ConversationFork(CommitBeforeAPICompletionProbe|AtomicCreationControls|LostHTTPResponseReplays)|OperatorConversationFork)' -count=1 -timeout=3m -v
go test -race ./internal/apiv1 -run '^Test(ConversationFork(CommitBeforeAPICompletionProbe|AtomicCreationControls|LostHTTPResponseReplays)|OperatorConversationFork)' -count=3 -timeout=3m -v
```

Results: PASS 3.559s and PASS 80.180s, respectively. Race receipt: `/tmp/agent-g-2444-implementation-f1-race3.log`; no race warnings or skips. New F1 matrix has 18 leaves per iteration, 54 across count3, plus existing sibling controls. Both stores cover precommit refusal, completion-insert rollback, eight concurrent same-key calls, hash conflict, actor/key isolation, distinct unkeyed calls, and TTL expiry.

`TestConversationForkLostHTTPResponseReplays` uses a real HTTP listener; after actual atomic mutation the handler closes the TCP connection without response bytes. All six cases (two stores x count3) recorded client EOF, then replay through a reconstructed selected store owner and new listener, with the same fork ID and exactly one durable fork/one completion. This is actual response loss, **not SIGKILL, a new process, or lost database COMMIT acknowledgement**; store reconstruction retains the fixture database handle.

`TestConversationForkCommitBeforeAPICompletionProbe` now overrides the actual typed `CreateAPIConversationFork`, not the obsolete generic wrapper. Its postcommit return-error injection is a handler/owner omission probe, **not network loss or SIGKILL**. After atomic creation, durable fork and completion already exist before retry.

### Adjacent existing controls

```sh
go test ./internal/store/internal/runtimepersistence ./internal/store/internal/backend/runforkpersistence -run '^Test(SQLiteRuntimeStoreConversationForkLifecycleParity|PostgresStore_ConversationFork(LifecycleOwnsCreateListViewDelete|LifecycleFailsClosedForSelectors|ChatAllocatesConcurrentTurns)|ConversationForkGracefulMutation)$' -count=1 -timeout=3m -v
```

PASS: runtimepersistence 2.817s; runforkpersistence 4.036s. Log `/tmp/agent-g-2444-implementation-f1-adjacent.log`. Four lifecycle tests and 16 mutation adapter controls include healthy, pre-admission/during-SQL/precommit cancellation, panic, SQL/deferred-commit failure, and keyed lock-loss controls. Lock remains before serializable BEGIN. The deliberately hostile lost-lock case emits its expected error.

### OpenRPC receipt caveat

```sh
go test -race ./internal/apiv1 -run '^TestOpenRPCMutatingHTTPRuntimeProbes$/^conversation[.]fork$' -count=3 -timeout=2m -v
```

After updating the stale generic-wrapper count to zero for the named atomic owner, all 27 leaves passed in 9.190s, but the overall command exited 1 due to concurrent eventpersistence vet/type mismatch. Log `/tmp/agent-g-2444-implementation-f1-openrpc-race3.log`. Retry `/tmp/agent-g-2444-implementation-f1-openrpc-race3-final.log` failed compilation in the same concurrently edited family. Neither is a clean integration receipt; parent must use the final frozen-tree rerun. No full suite was run by this agent.

## Closure limit

Do not label the admitted consumer class closed from store-side outcome fixes or the F1 matrix alone. C1-C3 require owner disposition and focused evidence. This review makes no universal network stall bound, process-death qualification, or exactly-once external-effect claim. Other agents own graceful, crash, transport, and final integration proofs.

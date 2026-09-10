# #2444 Consolidated Removal Investigation / Implementation Gate Request

**Historical candidate.** The mandatory I/O policy and socket wrapper proposed
below were superseded by lead comment `5622859909`. They are not approved or
required. See `issue-2444-three-contracts-gate.md` for the current contract delta
and proof ledger. The original patch/hash and receipts remain historical evidence,
not a production implementation or a current timeout recommendation.

Status: investigation only; NOT production implementation or merge approval.
Agent: agent-g. Baseline: `a32749a31edcf62a4c07879062c0f2e820885be6`.
The runtime experiment is an uncommitted disposable candidate. The accompanying
patch is evidence, not an approved implementation to apply to master. #2008 WIP
is untouched. No live provider traffic or settled-delivery replay was performed.

Candidate patch SHA-256:
`d01db3dd5a6ae9d0112103591e799b5f4a6b8de7a920fdaf773352eb226511ed`.
Reproduce only in a disposable worktree at the baseline with
`git apply --unidiff-zero issue-2444-disposable-candidate.patch`.
Patch applicability and both candidate/artifact diff checks passed. Copied pq
source remains physically present but unselected in the candidate; deletion and
CI/inventory retirement are implementation work, not falsely claimed completed.

## Decision

Recommend removing the pq fork, with an explicit graceful **closed persistence
operation** contract, not a replacement cancellation framework. Keeping the fork
has not earned its maintenance cost. Do not interpret this as permission to
delete the replacement before the independent implementation gate.

The three requested obligations are addressed together: retained reads are
included, commit evidence survives later cleanup, and a public-driver socket
budget has been exercised through actual serve startup/shutdown. The implicit
consumer census also identifies additional closed mutation consumers that must
be migrated before claiming the graceful mutation class is eliminated.

Two product decisions need explicit ratification in this gate:

1. Graceful admission covers closed mutations and retained-authority operations;
   standalone ephemeral observations may remain caller-cancellable and may lose
   their pooled connection. They cannot claim retained-session continuity or
   exact cancellation provenance. If *all* reads must drain, include those
   readers too rather than leaving this exception implicit. Pool acquisition
   and API idempotency's blocking wait on an ungranted session remain cancellable
   admission, not already-admitted request work; an interrupted waiter is
   disposed and cannot run the API callback. The default lock acquisition on
   an already-retained session drains and preserves its other healthy leases.
2. PostgreSQL serving requires an explicit positive `database.io_timeout`
   deployment budget, with no fabricated universal default. A silent healthy
   query can exceed it just as a broken connection can. Expiration is an actual
   I/O failure, not an assertion about its physical cause. The experimental
   `SWARM_POSTGRES_IO_TIMEOUT` environment input is disposable and must NOT become
   a second production config authority. Production config/verify/serve/test
   construction must consume the single admitted value. Test budgets of 100ms,
   250ms, 400ms and 5s are evidence parameters, not proposed product defaults.

If either policy is unacceptable, state the incompatible requirement before
implementation. Do not solve it with a driver fork, hidden timeout default,
watcher registry, query interceptor, SQL parser, or arbitrary retry.

## Governing Context and Class

Binding context: complete #2444 body/thread and the latest independent removal
review; user's clarification that cancellation is a graceful stop. Exact spec:
`engine.runtime_core_persistence_store_contracts.backend_neutral_runtime_mutation_write_boundary.rules`
at baseline `platform-spec.yaml:5768-5787`; conversation-fork lock-before-BEGIN
rule at `:6031`; shutdown grace and join ownership at `:25050` and `:35660`.
The native arbitration, implicit-scope, worker evidence and between-statement
refusal clauses are changes requested by this gate, not silently ignored rules.

Category: architecture/model debt, failure-class, high-risk maintenance.
Symptom: maintaining a native driver fork to distinguish expected owner stop
from failures that database/sql or upstream pq may hide; naive stock replacement
also interrupts retained reads and invalidates reusable advisory sessions.
Chosen class: removing fork-dependent cancellation semantics without losing
closed-operation settlement, retained authority, or truthful public failure.
Immediate parent: cancellation versus semantic authority (#2159), transaction
envelope ownership (#2412). Broader parent: lifecycle/composition debt (#2250).
The issue is broad enough; custom API call sites were entry points, not the
audit boundary. No claim of corruption in every BeginTx caller is made.

Intended eventual closure: failure class eliminated for the above boundary;
current closure is investigation/design evidence only. This PR/implementation
must eliminate the chosen class entirely, not only its retained pipeline row.
Parent architecture streams stay open. Estimated broader tail: at least three
groups (typed envelopes, lifecycle composition, cross-owner cancellation policy),
low confidence; their full closure is not required for dependency deletion.

## Full Path and Owners

Configuration/admission -> selected-store construction -> TCP/connect/auth/TLS
-> schema/startup ownership -> retained possession -> runtime topology and
producer publication -> pipeline claim/eligibility/load -> closed mutation ->
commit/rollback -> typed handoff/settlement -> continued work or shutdown fence
-> admitted work join -> exact session release/disposal -> public exit.

Configuration and source/topology admission are distinct semantic gates; their
existing refusal/ordering owners are preserved. SQL admission, retained
possession, settlement, transport failure and final error projection are this
class. Provider/tool execution stays outside detached SQL, remains governed by
its existing execution/retirement authority, and receives no new admission.

Canonical owners remain:
- Private PostgreSQL `Backend` transaction runner for closed pooled work.
- `SessionAuthority` transaction/operation serialization and `AdvisoryLockLease`
  possession/release for retained work. Add only explicit successful-commit
  evidence and the required transaction options to this existing owner.
- Pipeline obligation owner for claim, load, receipt, disposition and candidate
  handoff. A boolean commit receipt is not a new runtime ownership abstraction.
- Existing generic schedule claim owner and conversation-fork operation owner
  for their exact retained sockets and lock keys; no registry added.
- Selected-store construction plus the public pq Dialer/net.Conn contract for
  mechanical socket deadlines. No cancellation attribution happens there.
- Existing serve composition/finalization owner for joined cleanup and exit.

These are actual lifetime/settlement owners, not the first convenient helper.

## Systematic Consumer Census

The raw inventories are attached as `issue-2444-implicit-census.txt` (64 rows),
`issue-2444-pooled-sql-census.txt` (164 rows), and
`issue-2444-interface-census.txt` (158 rows). They include
test/SQLite entries explicitly classified below; its grep count is not a defect
count. The census followed Session() arguments through query interfaces, not
only direct method calls. No other production retained *sql.Conn family was
found beyond Backend ownership, SessionAuthority, generic claims, conversation
fork and the pipeline reservation allocator.

| Consumer family / exact entrance | Disposition and required proof |
| --- | --- |
| `postgres/transaction.go`: RunTransaction, RunReadTransaction | Existing owner; candidate drains admitted SQL, refuses pre-commit stop, owns rollback/disposal/panic. Existing backend controls plus outcome/transport matrices. |
| `postgres/session_authority.go`: transaction, probe, acquire, release | Existing owner; retain serialization through drain and preserve-or-dispose, callbacks retire only after unlock. Candidate read/outcome/acquisition proofs; no per-statement interceptor. |
| `pipelinepersistence/owner_operations.go`: MarkDecisionProcessed, Settle | Migrated in disposable candidate to commit-aware existing owner. Successful COMMIT triggers existing handoff even with EndTx/release/handoff error; all are separately asserted. |
| Same file: eligibility, LoadClaimedWork, event hydration, receipt/replay reads, committed-scope hydration | Migrated together to closed read snapshots. No retained raw query passes remain in pipeline. Query completion cannot export rows into an unowned lifetime. |
| Same file: parent fence, reservePostgresPipelineClaimConnection | Fence uses transaction-scoped advisory lock and existing read owner; reservation still owns cancellable pool admission/capacity. Race and same-PID/lock controls. |
| `selected_fork_commit.go`, startup process possession, maintenance/reset retained lease consumers | Already consume lease RunTransaction/proof/release. Preserve exact ownership and no unscoped reads; requalify supported paths. |
| `apiidempotency/owner.go`: AcquirePostgresRequest purge+lookup; WithAPIIdempotency completion; custom blocking acquire | Added census consumer; disposable SQL sections moved to existing retained transaction owner. External execute(ctx) remains caller-context and outside SQL. CompletionTx already borrows owning transaction. Cancellation during purge/insert plus healthy reacquire/replay/terminal release proofs. |
| `genericschedule/claims.go`: Claim/Release/ReleaseAll | Added retained family; disposable claim drains its already-admitted SQL and preserves healthy PID. Invalid session clears key cache/disposes, terminal close with keys cannot return locked connection to pool. Real server termination/competing lock tests. Scheduler/fire owner unchanged; registration remains under its runtime occurrence admission. |
| `runforkpersistence/conversation_fork_backend.go`: unkeyed create and five keyed mutation callers | Same class, disposable migration via existing SessionAuthority. Preserve hashtextextended lock-before-BEGIN and serializable options, rollback/panic/disposal/unlock. Not an ephemeral pooled read. |
| `pipelinepersistence/flow_instance_routes.go`: Upsert, ReplaceRecords, Delete, Rollback helper | Same closed mutation class; migrate existing helper to Backend transaction owner and pass owned sqlCtx into captured callbacks. Not yet migrated in candidate; required before implementation closure. |
| `agentpersistence/directive_operations.go`: RenewDirectiveExecutionLease | Same closed mutation class; migrate to existing owner. Other transitions already consume named mutation ownership. Preserve actual lease-renewal refusal and no new external dispatch. |
| `channelonboarding/owner.go`, `operatorchannel/owner.go`: postgresRunner.mutate | Same class at existing callback boundaries; migrate there, not around the whole onboarding workflow/network interaction. Test reserve/advance/binding/proof/retire and cancellation cuts. |
| `effectpersistence/runtime_external_effects.go`: AuthorizeExternalAttempt, MarkExternalAttemptResponseObserved | Same class; migrate durable mutation, not provider launch. SettleExternalAttempt already consumes named owner. Require no callback/launch replay on uncertainty. |
| `llmpersistence/postgres_sessions.go`: Acquire, Release, Rotate, IncrementTurn, AdoptSessionID; postgres.go watchdog | Same class; preserve revision and committed candidate handoff. ResetAll uses Background already: no caller-cancellation change, but shares selected transport budget. No provider/session identity change authorized. |
| `runforkpersistence`: selected-contract activation/materialization/discard ports; legacy named activation/materializer; route recovery; selected runtime issue/claim/heartbeat/quiesce/close/fail | Same mutation class; consume existing transaction owner with unchanged isolation/options and committed evidence. No planner/replay redesign. `LoadRunForkSelectedContractSourceEvents` actually writes prepared events and is included, not exempted by its name. |
| `adminpersistence/destructive_reset_cleanup.go`, `schemastore/postgres_bootstrap.go`, `runlifecycle/active_run_quiescence.go` | Real writes are same class; consume existing transaction ownership without changing reset/schema/lifecycle meaning. Read-only dry-run is observation. Preserve retained startup authority; no extra lease owner. |
| `routingrules/owner.go`: UpsertRoutingRule active/deactivate/insert-deactivated; `ingresspersistence/owner.go`: EnsureRuntimeIngressState, SetRuntimeIngressTransitionEvent | Same mutation class even though autocommit, not BeginTx. Include read-before-write/return observation in each existing named operation; no new routing or ingress semantic owner. TransitionRuntimeIngressState already uses Backend.RunTransaction. |
| `mailboxpersistence/postgres.go`: insertMailboxItemSpec, markMailboxItemNotifiedSpec, expireMailboxItemsSpec; directive expiry cleanup | Same class. Mailbox expiry uses QueryContext with a CTE UPDATE RETURNING, so query method spelling is not read-only proof. Drain returned rows within its owned mutation; preserve expiry/event lifecycle ownership. Expired directive deletion inside active-run branch is direct DML and must consume existing boundary. |
| Fan-out release/retryable/successful-turn backend arguments; activity journal/result; reply context; source artifact; event identity/inbound integrity; timer/readiness/selected-fork read arguments | Followed through interfaces. Fan-out mutations already dispatch to Backend.RunTransaction, not raw SQL; reader helpers are pure observations unless already transaction-bound. Source names alone are not classification evidence. |
| Operatorsurface repeatable-read agent summaries/lifecycle facts, pending deliveries; standalone PlanRunFork and EnsureRunForkNoPostForkCommittedReplayScopeMarkers; pooled Get/List/load surfaces | Proposed different policy: cancellable ephemeral snapshots with no reusable session possession or durable mutation. In-mutation variants inherit closed operation. Stock cancellation can invalidate a pool connection and exposes only observable error; no cancellation provenance claim. Ratification required. |
| Pooled Exec/Query/QueryRow/Ping beyond manual transactions | All direct DML entrances found are named in the preceding rows. Remaining direct and passed-through read projections are observation or already transaction-bound. Do not globally detach Backend methods. Repeat the inventory against the final implementation to detect drift before claiming zero bypasses. |
| SQLite and testpostgres/testutil/storetest raw handles | Different owners: SQLite transaction safety remains unchanged; test harness pool/control plumbing is not production operation authority. Use its cancellation/parity controls, never count it as a PostgreSQL retained-session consumer. |

The remaining mutation families are accounted-for implementation work, not
asserted proven fixes or a covert #2412 split. A global Backend.BeginTx shim is
specifically rejected: it would detach observations and still leave callbacks
using the old context. Full typed-envelope redesign remains #2412, not required
here. Likewise no native error concrete type/SQLSTATE consumer was found that
justifies the fork; context/error-based consumers need requalification under
the explicitly weaker attribution contract.

## Proposed Authoritative Spec Delta

Replace the PostgreSQL native arbitration/implicit-scope/worker clauses, and
amend ordinary/retained clauses in the exact rules section cited above:

1. Caller cancellation refuses admission to a new closed persistence operation.
   An admitted closed SQL operation drains on its owned non-cancellable SQL
   context; internal statements are part of that unit, not fresh work. This
   explicitly replaces the old between-statement-refusal requirement. No
   detached provider calls, arbitrary callbacks, retries or successor work.
2. Recheck logical cancellation before COMMIT admission; stop there rolls back.
   After COMMIT admission report the actual acknowledged result. Lost response
   remains uncertain, no callback replay. `committed=true` only follows an
   acknowledged successful COMMIT and survives later cleanup/handoff failure.
3. Retained reads consume/close their rows inside the existing operation owner.
   Healthy rollback/drain preserves exact advisory possession and PID. Failed
   cleanup or invalid transport fences/disposes exactly that socket. Failure to
   prove current possession is not repaired by acquiring a new connection.
4. Preserve every observable primary, transaction, transport, cleanup and
   disposal error. Do not promise information discarded inside pq/database/sql.
   Context cancellation and SQLSTATE 57014 do not erase independent failures.
   No native winner arbitration, cancellation worker, fork, or scope binding.
5. Standalone ephemeral reads may remain caller-cancellable as the explicit
   exception above. Their result-drain owner still closes rows and never lends
   a cancelled socket as retained authority. Pure read has no mutation authority.
6. Use one admitted positive PostgreSQL I/O budget at construction. Compose the
   earlier existing absolute connect/startup/TLS deadline with the per-call
   read/write deadline; later reset cannot extend an in-flight call's budget.
   Any timeout/partial-protocol error permanently closes that socket. The value
   bounds a silent I/O call, not total transaction, trickling rows or shutdown.
7. Shutdown grace remains escalation/reporting followed by joining accepted
   work. Actual timeout/cleanup failure yields nonzero serve exit after join;
   neither context cancellation nor a previously selected zero code hides it.
   Successful graceful stop remains clean. Remote advisory release requires
   server observation/competing ownership proof, not local Close alone.

Also update the database config contract with the single budget field and its
missing/invalid admission error; no experimental env fallback. Preserve the
conversation-fork lock/isolation contract and all SQLite rules. No new table,
ledger, outbox, migration, connection registry or compatibility branch.

## Proof Matrix and Limits

The candidate is built against stock pq v1.11.2. Isolated eleven-case TCP/result
matrix also passed stock v1.12.3 with race detection x3. This is not an implicit
dependency upgrade recommendation; pin one upstream version in implementation.

| Manifestation | Exact evidence / current status |
| --- | --- |
| Logical cancellation before admission; during closed SQL; before/after Commit admission; independent callback error; panic; uncertain commit; EndTx failure | `TestAuthorityTransactionOutcomeEvidence` (9 cuts), `TestAuthorityReadGracefulCancellation` (6 cuts), invalid-session proof plus existing authority tests: race x3 PASS, 21.392s. Fault injection is labeled; real TCP COMMIT loss is a separate row. |
| Retained QueryRow, Rows, prepared result drain; same-PID reuse and no callback on unsafe authority | `TestAuthorityReadGracefulCancellation`, `TestAuthorityReadInvalidSessionFenced`: included above. Pipeline callers, not a raw QueryRow wrapper, consume this owner. |
| Supported pipeline claim/eligibility/load/hydration/fence/cancellation/settle | `TestPipelineGracefulClaimReadCancellation`, `TestPipelineGracefulParentFenceDrainsCanceledWait`, `TestPipelineGracefulWriteOutcome`, `TestPipelineGracefulCanceledAdmissionBothStores`; parity, exclusion, parent-terminalization and pool-wait controls. Race x1 PASS 34.617s after the latest API/serve candidate changes. |
| MarkDecisionProcessed/Settle commit versus EndTx/release/handoff | WriteOutcome rows `endtx`, `release`, `handoff`, `combined`, `release_handoff` plus stop/panic/independent/lost-ack rows. Durable receipt/handoff and returned outcome/error asserted separately. Release only applies to Settle; Mark retains its claim. |
| Generic retained claim healthy stop, cached-key invalidity, unlock/remote exclusion | `TestGenericClaimGracefulCancellationPreservesExactSession`, `TestGenericClaimUnsafeSessionFencesCachedKeys`: real PG, race x3 PASS 5.124s. Final generic lifecycle parity remains required after full implementation. |
| API retained purge/completion interruption and external callback boundary | `TestAPIIdempotencyGracefulSQLBoundaries` plus existing terminal-release/replay/capacity matrix: race x3 PASS 6.960s. Before admission/purge/insert/external callback; same-PID successor and zero partial completion. |
| Conversation-fork keyed/unkeyed, isolation, lock-before-BEGIN, panic/failure/stop/remote release | `TestConversationForkGracefulMutation`: 16 cases x3 race PASS 15.550s. Default/custom acquisition plus existing outcome/read controls: 22 cases x3 race PASS 19.318s. Complete supported fork journey remains implementation qualification. |
| API pre-grant blocking acquisition cancellation | `TestAPIIdempotencyContendedAdmissionCancellation`: actual pg_stat_activity advisory wait, cancellation, no executor admission, holder possession unchanged, interrupted waiter PID not reused, healthy successor acquisition. Complete API package including graceful SQL/release/replay matrix race x3 PASS 7.943s. This exception is only pre-grant admission, not custom queries on an already-granted session. |
| Real lost BEGIN/COMMIT/ROLLBACK response, stalled query/write, Rows.Next/Close, prepared Exec, unsafe successors | Eleven published isolated TCP cases both stock versions race x3 PASS; integrated constructor transport `TestPublicTransportPostgresResponseLoss` seven phases asserts independent durable COMMIT despite caller failure and remote lock release. No uncertain callback retry. |
| Deadline composition, connect/pre-cancel/auth/TLS, active deadline changes, true write stall, progressing rows, deadline reset | `TestDeadlineConn*`, `TestConnector*`, `TestPublicTransportPostgresStartupResetAndProgress`: transport package race x3 PASS 28.592s; construction config checks PASS 1.024s. Slow progress explicitly exceeds one budget without implying a total bound. |
| Healthy silent query exceeds budget; remote work outlives local disposal | `TestPublicTransportPostgresHealthySilentQueryExpiry`: race x3 PASS 6.274s. Healthy pg_sleep(0.6) timed out locally at 100.38/100.64/101.17ms; independent observer saw backend still executing and lock still held. Independent acquisition succeeded at 609.51/602.45/603.14ms. Explicitly NOT immediate remote release or total shutdown proof. |
| Actual serve startup/auth-response blackhole, post-ready ownership loss, shutdown blackhole, active SQL beyond grace, healthy stop | `TestServePostgresAutomaticStallSettlement`: 5 paths, race x3 PASS 39.373s. Real runFrom/selected construction/listeners, stub workspace/no providers; test TCP proxy. Active-work row injects a real SQL query under an exact runtime lease and proves join; it is not credited as a public user query journey. |
| Public exit loses deferred shutdown failure | Before correction shutdown fault logged `ERROR: shutdown failed` but returned zero; startup/ownership/healthy controls passed. Disposable existing composition named-result correction makes finalization failure nonzero. The above 15 executions pass; this is an additional bounded implementation obligation, not production closure. |
| Previously failing public event.publish post-commit receipt/idempotency paths | `TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate`, `TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError`; repeated after API migration, PASS 5.966s. |
| Remaining closed pooled mutations / ephemeral-read exception | Source-classified, NOT individually fault-proven or migrated in candidate. Required production matrix per census family: stopped admission, active SQL stop, precommit rollback, panic/independent error, acknowledged commit plus later cleanup, unchanged normal execution/isolation; prove provider launch is not detached. |
| Whole suite / release lifecycle | NOT RUN for this investigation. After gated complete removal: focused controls then `go run ./cmd/swarm-test` with adequate package timeout, dual-store supported lifecycle and changed consumer tests. No full qualification or merge readiness claimed here. |

Final shared-owner requalification after the acquisition/conversation changes:
pipeline/graceful/parity/claim controls plus generic schedule admission and
terminal-claim transfer, race x1 PASS **36.198s**; all five integrated serve
paths race x1 PASS **15.951s**. The larger native-provenance suite is not claimed
green against the proposed weaker contract: native-only expectations need
replacement with the approved behavior requirements in implementation.

Representative reproduction commands (host `SWARM_TEST_POSTGRES_DSN` required;
do not print credentials):

```sh
export SWARM_POSTGRES_IO_TIMEOUT=5s # disposable candidate only
go test -race ./internal/store/internal/backend/postgres ./internal/store/internal/backend/runforkpersistence -run '^Test(ConversationForkGracefulMutation|AuthorityAcquisition.*|AuthorityTransactionOutcomeEvidence|AuthorityReadGracefulCancellation|AuthorityReadInvalidSessionFenced)$' -count=3 -timeout=2m
go test -race ./internal/store/internal/apiidempotency -count=3 -timeout=2m
go test -race ./internal/store/internal/postgrestransport ./internal/store/construction -count=3 -timeout=2m
go test -race ./internal/store/internal/runtimepersistence -run '^(TestPipelineGraceful.*|TestPipelineObligationSQLitePostgresParityMatrix|TestPostgresParentTerminalizationLinearizesClaimRegistration|TestPostgresPipelineClaimLeaseExcludesIndependentStoreUntilRelease|TestPostgresPipelineClaimConnectionWaitIsCancellableOutsideRegistryLock|TestGenericScheduleAdmissionReplayConflictAndCancellationOnBothStores|TestPostgresGenericScheduleEmptyTerminalPreparationTransfersExactClaim)$' -count=1 -timeout=2m
go test -race ./internal/serveapp -run '^TestServePostgresAutomaticStallSettlement$' -count=1 -timeout=2m
```

## Deletion and Cost

Fork production delta was +595/-156 across six modified upstream files and
three added files; native regression tests 608 lines, docs 69 lines. Copied
upstream source size is not the customization-cost metric. Retirement removes
per-upgrade patch reconciliation, custom native binding/implicit lifetime
semantics, native-only CI and the associated evidence promise.

Implementation deletion inventory: root go.mod replace; all `third_party/pq`
copied source/tests/docs/module; NewOperationScope/BindOperationScope imports,
binding/error/close forwarders in test drivers; native-only nested CI invocation
and source guards; native operation rows in raw-SQL/persistence authority
ledgers and scanner exceptions; obsolete native-rule wording. Regenerate
inventories, do not hand-exempt new call sites. Preserve behavior-level
transaction/retained/result/advisory/implicit-successor regressions and historical
audit receipts (historical provenance is not a live compatibility reader).

The substitute costs are public socket-deadline mechanics, explicit config,
small existing transaction outcome/options extension and caller migration.
Do not call it zero-cost or recreate 595 lines of driver arbitration elsewhere.
Changing drivers (e.g. pgx) has not shown a better total-cost case; public hooks
alone do not guarantee remote cancellation attribution. No upgrade/vendoring
work is authorized by this investigation.

## Tracker, Watchlist and Gate

Current #2444 remains correct but this artifact refines the implementation class
and surfaces policy decisions. Keep #2442 closed as historical proof; no new
issue. Parent-class probe promoted retained API/generic/conversation consumers
and closed mutation accounting here; it does not absorb all #2412/#2250/#2159.
No POTENTIAL_ISSUES entry. Watchlist: incorporate lead `20b5d05`, refine existing
atomic mutation and shutdown nodes with the expanded retained census and
deferred-exit result. Published refinement `e42a175` incorporates `20b5d05` on
`agent-g/2444-investigation`; branch publication is not a docs-master merge claim.

Closure-feasibility: one removal PR is credible if the stated two policies and
full closed-mutation migration are approved. Fixing only the original retained
read leaves same-concept mutation interpreters live, so that is not the closure
claim. No generic transaction framework is required. Larger envelope design
remains architecture feedback on #2412/#2250, with nontrivial multi-stream effort
and lower immediate ROI than removing the native fork; no firm estimate claimed.

Request one independent implementation ruling on the whole package, including
the new exit-projection obligation, explicit read exception, explicit deployment
budget and remaining caller migration. Do not silently treat prototype passes
as approval. Production stays frozen until that outcome is recorded.

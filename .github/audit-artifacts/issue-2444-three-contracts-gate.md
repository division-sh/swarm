> Historical approved pre-audit snapshot. The implementation freeze and open gaps below were superseded by [the independent implementation gate](https://github.com/division-sh/swarm/issues/2444#issuecomment-5623494272). See the final post-implementation audit for shipping-head results; investigation receipts are not final qualification.

# #2444 Three-Contract Investigation and Corrected Gate Package

Agent-g. Investigation only; production implementation remains frozen.
Binding ruling: https://github.com/division-sh/swarm/issues/2444#issuecomment-5622859909.
Base: `a32749a31edcf62a4c07879062c0f2e820885be6`, also the fetched origin/master
at this pass. The existing consumer inventories remain the audit boundary; this
is one additive proof-and-gap pass, not a new repo audit or merge claim.

Frozen disposable patch: `issue-2444-three-contract-candidate.patch`, SHA-256
`307081ae565ad9b6d0a1784921044de2fa6038a25422b63dc9f07537d5ab483a`.
It applies with `git apply --unidiff-zero` only at that baseline; applicability
was checked. It deliberately includes failing F1/F2 probes and is not a shipping
patch. Copied pq source remains physically present but unselected; implementation
must delete it, obsolete native APIs/CI/guards, and update authoritative spec
together. No production PR is opened under this investigation-only ruling.

## Direction

Remove the driver fork. The new experiments use unmodified pq v1.11.2 through
ordinary selected-store construction, **without** a socket-deadline wrapper,
mandatory I/O setting, cancellation attribution, or new recovery machinery.
The old experimental transport/config/tests have been removed from the disposable
candidate. They survive only as historical investigation evidence. No fallback,
environment selector, migration, new table/outbox, or generic callback retry is
proposed. No live-provider traffic or settled-delivery replay was performed.

Keep the existing transaction/session owners and truthful joined-cleanup exit
repair. Explicit operation cancellation is separate from graceful process stop.
An admitted persistence operation may commit during shutdown. A canceled operation
must obey its pre-COMMIT refusal/rollback rule. After COMMIT admission, actual
acknowledged outcome or honest uncertainty governs; neither forces callback replay.

The experiments materially support removal, not complete implementation. The
remaining writer migrations from the original census, concrete recovery/evidence
gaps below, and the named possession-proof deadline must be resolved explicitly.

## Three Contracts

| Contract / entry | Existing owner and admitted unit | Durable truth and actual recovery reader | Failure / exit behavior | Current baseline versus proposed removal |
| --- | --- | --- | --- | --- |
| Graceful serve stop; admitted SQL mutation and result drain | Serve composition, Runtime admission/work lifetime, selected idempotency/transaction/result owner; only the admitted SQL unit drains | `api_idempotency` completion; exact same-key API reader. Pipeline/event and effect continuations use the durable readers below | New work refused, dependencies retained until admitted work and cleanup join; explicit operation cancel rolls back before commit; cleanup failure must produce nonzero exit | Baseline uses native cancellation semantics and can print deferred cleanup failure with exit zero. Candidate uses stock pq, scoped SQL drain, and named return propagation. A test-injected selected leaf under real serve lifetime is not a claim about every HTTP handler |
| SIGKILL before settlement COMMIT | Actual child process, selected SQLite/PostgreSQL store, exact process capability and pipeline claim | Existing committed event/obligation remains eligible; new child reacquires authority, EventBus/Pipeline RecoveryManager scans and settles it | No finalizer, handoff, or acknowledgement assumed; no claim that socket disappearance equals remote release | Baseline recovery model retained; candidate owner/query changes must not interrupt the durable reader or import old in-memory claims |
| SIGKILL after settlement COMMIT before acknowledgement/handoff | Same actual process and selected settlement owner | Pipeline success receipt plus persisted run completion candidate; exact claim eligibility and candidate reader decide what remains | No duplicate settlement/provider call; missing old caller success is not permission to replay the callback | Candidate must preserve actual committed evidence, while crash recovery does not depend on returning it to the dead process |
| Public `event.publish` loses actual PostgreSQL COMMIT response | Atomic API publication owner; real TCP proxy drops only server COMMIT completion for the marked publication transaction | Event/delivery and API completion committed together; real serve startup recovery reaches handler/delivery, exact API replay reads the original response | Original caller gets an error; no automatic retry; restart delivers once and same-key replay returns original IDs | Atomic owner is unchanged; tested with stock pq and no deadline wrapper. This does not prove generic API callbacks atomic |
| Real PostgreSQL session termination / private server immediate stop | Existing process possession monitor, selected-store owner and serve watcher | Same store, canonical authority generation, persisted run/entity/route; normal startup reacquires and existing runtime loads same run for public follow-up | Detected loss yields failure exit and no ready endpoint. Startup while server unavailable refuses. Server WAL recovery then runtime restart is tested | No transparent reconnect/authority transfer added. Private fsync-enabled test cluster only; shared test service is never stopped |
| Local TCP leg closes while old remote session still holds process lock | Existing exact process advisory authority and startup acquire owner | Remote `pg_locks` observation corroborates hold; **actual successor serve refusal** is authoritative execution evidence | Old serve fails; successor refuses while remote lock survives; restart succeeds only after backend release and exact new possession | No local-close/immediate-remote-release assumption, timeout workaround or new lock owner |
| Silent database/connection | Existing operation, possession monitor, shutdown join owners | No observation can infer physical failure or commit rollback from silence; durable reconciliation remains necessary when access returns | No universal prompt-detection or total shutdown guarantee. Grace expiry reports incomplete drain rather than successful cleanup; explicit force is crash | Candidate's detached monitor query does not enforce the existing named proof deadline. This is the explicit unresolved spec decision below, not a green proof |

## Durable Consumer Trace

The detailed source census is `issue-2444-three-contract-recovery-census.md`.
Read it with the existing 64-row implicit, 164-row pooled SQL and 158-row interface
inventories. A raw query spelling or a method named Load is not read-only proof.

| Family | Durable fact and recovery reader | Disposition |
| --- | --- | --- |
| Pipeline settlement and publication | Event/receipt/replay scope/obligation; EventBus `SweepPipelineObligations` through `RecoveryManager.RecoverToExhaustion`; delivered outcome prevents duplicate execution | Existing canonical owner, candidate migrated retained reads and commit-aware settlement; real crash/public-loss proof, not shared-helper credit |
| Run completion handoff | Transaction-written `runs.completion_revision/completion_due_at`; `ListCompletionCandidates` and run lifecycle executor | In-memory sink accelerates discovery, not its only source; actual child restart must consume pending candidate |
| External attempts | Operation/attempt IDs, launch/terminal/evidence state; manager startup `reconcileExternalEffectsForStartup -> ReconcileExternalEffectAttempts` | Existing recovery owner; authorized unlaunched orphan is terminal failure, launched orphan uncertain; no promise of exactly-once external effect |
| Successful managed response | Persisted continuation/projection phase and successor checkpoint; `recoverManagedCompletionContinuation -> RecoverCompletionContinuation` before new provider invocation | Existing durable reader. Must preserve acknowledged committed flag even if later handoff/cleanup fails |
| LLM/session lifetime | Persisted conversation/provider/session identity and DB-time lease; `AcquireLiveSession` hydrates exact identity; run candidate separately durable | Existing lease/reader, remaining closed writer migrations still required. No fresh-session fallback or implicit authority transfer |
| Selected fork | Materialized rows/routes/events/obligations/execution state; manager `restoreSelectedContractRouteRecoveries`, pipeline recovery, exact activation and effect-authority recovery | No automatic activation promise for paused materialization; pending work may correctly block activation. Do not equate route roundtrip with all interrupted fork stages |
| Fork chat | Durable keyed completion group and attempts; same-key group admission/refusal and startup parent-authority reconciliation | Succeeded/uncertain group is not provider-redispatched if API response missing; unchanged refusal model, not synthesized success |
| Fork create | Durable fork row, but generic API completion is a later transaction and request key is not in fork row | Concrete association gap under investigation; no existing reader found that maps an unacknowledged creation to the same request |
| Generic API idempotency | Completion row replay on subsequent same-key request, not generic startup enumeration | SQL lookup/completion migrated in candidate; arbitrary callback still outside transaction and caller-cancellable. Atomic event-publication proof is not evidence for every generic mutator |

## Minimal Spec Delta Requested

Authoritative `platform-spec.yaml` remains unchanged until the implementation
gate. The eventual removal PR must update these exact governing sections together:

- `engine.runtime_core_persistence_store_contracts.backend_neutral_runtime_mutation_write_boundary.rules`
  (baseline 5768-5787): replace native operation scope/arbitration/implicit binding
  and cancellation-worker requirements with existing-owner SQL admission,
  drain/cleanup, preserve-or-dispose, and observable-outcome rules. Preserve
  SQLite transaction safety and no uncertain-COMMIT callback replay.
- Same section: process shutdown closes admission; it is not automatically
  explicit cancellation of each admitted operation. Internal SQL of a closed
  mutation/retained read stays owned through results/cleanup. Do not detach
  arbitrary network/provider callbacks or authorize new downstream work.
- Same section: successful acknowledged COMMIT evidence survives subsequent
  EndTx, release, handoff or cleanup errors. Joined errors remain observable;
  lost response remains uncertainty, not false rollback or invented success.
- Same section: pure standalone ephemeral read snapshots and fresh pre-grant
  blocking acquisition remain caller-cancellable exceptions. They must never
  be used to interrupt already-held reusable authority. Existing generic read
  runner behavior and all remaining writer callers need final implementation
  census, not a global `Backend.BeginTx` detachment shim.
- `runtime_startup_authority_facts.possession_loss_contract` (14957-14969),
  startup authority `grants` (36069-36086): remove native-BEGIN requirements
  consistently, but do **not** silently erase the separately stated bounded
  independent-proof/terminal-readback contract. Decision below is required.
- Existing shutdown grace/join sections (25050, 35660): no fabricated join,
  successful exit on cleanup failure, or early dependency release; forced
  termination explicitly changes the contract to process death. No mandatory
  `database.io_timeout`, socket wrapper or universal hard shutdown bound.
- Existing conversation-fork lock/isolation rule (6031) remains binding. If the
  proven request/result gap is absorbed, promote only the exact named operation's
  atomic use of existing idempotency completion, not a generic callback framework.

## Decisions and Remaining Gaps

1. **Named possession proof, not universal timeout.** Existing spec requires a
   bounded independent ownership recheck/terminal readback. Candidate
   `AdvisoryLockLease.proveCurrent` detaches the query and checks deadline only
   after drain; that does not meet those words on a silent socket. Removing
   mandatory I/O configuration does not implicitly approve this mismatch.
   The corrected gate must explicitly select the minimal local admission and
   detection policy for this owner, and amend all cited clauses. No replacement
   transport/cancellation framework has been implemented. Detectable-loss proof
   is not claimed to close this silent-proof case.
   Recommended direction for that decision: preserve the existing monitor's
   **bounded local authority-fencing decision**, not a promise that network SQL
   and cleanup have returned by the deadline. Start that budget only after its
   serialized proof owns the session, never while waiting behind healthy admitted
   work. Failure to finish that independent proof in budget fences local grants
   through the existing capability owner; the still-owned SQL/cleanup must join
   before dependencies release. Already detected physical disposal must fence
   locally before optional successor-lineage lookup. A quiet healthy server can
   lose availability under this existing owner-specific policy; no universal
   transport budget is added. This requires explicit approval and silent-probe,
   ordinary-cancellation, current-work serialization and shutdown-join proofs;
   it is not implemented or claimed proven by the current detached-query candidate.
2. **Fork-create request association.** The fork record is committed before the
   generic API response and has no request key. Exact same-key
   replay after an injected postcommit omission creates two durable forks on
   both stores; healthy controls return the original fork. Use the existing named creation
   and API-completion records atomically. A new durable model is not shown to be
   necessary. Do not widen this to unrelated generic mutators without a ruling.
3. **Completion result evidence.** Effect completion returns a zero settlement
   result after a postcommit handoff error, erasing its acknowledged `Committed`
   flag. Both stores reproduce with the committed attempt/turn/spend/candidate
   present and the return flag false; healthy controls preserve the flag.
   Preserve typed evidence and error separately at the existing owner on
   both stores; its durable continuation/candidate still exists. A successful
   observer query is not proof the returned result was honest.
4. **Remaining migration/qualification work.** The old census's manual pooled
   writers and named DML operations are not all migrated in the candidate.
   Existing backend error-only cleanup results also need acknowledged-commit
   accounting. Full suite and final supported consumer matrix have not run.
   Do not label the existing patch ready to ship.

No new issue, ledger, outbox, migration or compatibility seam is warranted by
the evidence. #2444 remains the removal/proof tracker. #2412/#2250/#2159 remain
broader parents, not prerequisites or implicitly closed issues; #2442 remains
closed historical evidence. Watchlist `atomic_runtime_state_mutation` and
`shutdown_and_runtime_lifecycle` incorporate lead `d7be3a6` as `eda741d`, followed
by evidence/gap refinement `bd6aea2` on the published docs investigation branch,
not docs master. YAML parse and diff checks passed. This is the required tracker
refinement, not a new watchlist taxonomy or POTENTIAL_ISSUES entry.

## Proof Ledger

Receipts: `issue-2444-three-contract-proof-receipts.txt` (filtered from complete
local transcripts; includes failures as well as successes). All new proof runs
below select stock pq v1.11.2 with no module replacement and no transport wrapper.

| Proof | Result and scope |
| --- | --- |
| `TestServePostgresGracefulContract` | Race x3 PASS, 28.419s. Real runFrom/listeners/admission/cleanup; test-injected selected idempotency mutation or streaming result under real work lifetime. Commit on shutdown, explicit-operation cancellation rollback, result drain, dependency ordering, refusal of new admission, cleanup failure exit 1. It is not a claim that every public command has separate cancellation wiring |
| `TestOpenPostgresUsesStockDriver` | Race x3 PASS, 1.021s; construction requires no experimental configuration and selects `*pq.Driver` |
| `TestPipelineProcessSIGKILLRecovery` | Race x1 PASS, 32.821s; four cases SQLite/PostgreSQL before commit and after native commit before application acknowledgement/handoff. Actual killed subprocess and new successor subprocess; exact process authority, real bus RecoveryManager and lifecycle executor, one receipt, durable candidate, repeat sweep unchanged. Internal compiled lifecycle proof, not CLI serve or wire-response loss |
| `TestServePostgresLossAndRestartFromDurableState` | Race x3 PASS, 32.295s. Exact session termination and immediate private PostgreSQL server stop/restart; failed predecessor, unavailable-server startup refusal, same durable run/entity public continuation after new authority. Two initial test-contract mistakes were corrected: ordering uses authority generation/ordinal, not timestamp; fixture completion payload must be `review` |
| `TestServePostgresLostCommitResponseRecoversDurablePublication` | Race x3 PASS, 15.720s. Actual PostgreSQL COMMIT completion frame dropped for marked public event.publish. Original response is failure; exactly one committed publication; no delivered row before restart. Actual serve startup executes delivery; exact keyed API replay returns original event/run, one delivered outcome. No callback/provider retry |
| `TestServePostgresRemotePossessionOutlivesLocalLoss` | Race x3 PASS, 17.832s. Proxy closes client legs while retaining old server sessions; old serve exits 1, remote PID still holds exact ownership. Real successor serve refuses (3), then acquires a different backend only after remote release. No inference from local Close |
| `TestAuthorityTransactionOutcomeEvidence`, `TestAuthorityReadGracefulCancellation`, `TestAuthorityReadInvalidSessionFenced`, `TestAuthorityAcquisition*` | Race x3 PASS, 19.731s after removal of transport experiment. Owner-level cancellation/outcome/read/pregrant/successor controls; not credited as process recovery |
| `TestPipelineGraceful*` plus obligation parity, parent-terminalization, exact claim exclusion, cancellable reservation, generic admission and terminal claim transfer | Race x1 PASS, 37.275s. Final no-timeout selected-owner controls including SQLite parity |
| `TestConversationForkCommitBeforeAPICompletionProbe` | Race x3: healthy controls PASS on both stores; omission cases FAIL 3/3 on each. API package 29.278s. Real HTTP handler/selected store with injected postcommit omission, fresh handler replay and public list; not SIGKILL/network ACK loss. One keyed request produces two durable forks, whereas healthy replay does not reenter creation |
| `TestCompletionCommittedEvidenceAfterHandoffFailureProbe` | Race x3: healthy controls PASS on both stores; sink-failure cases FAIL 3/3 on each. Store package 25.412s. Real completion owner, injected postcommit sink error; durable settled attempt, turn, spend and recovery candidate survive but returned Committed is false |
| Silent independent possession proof / bounded local fence proposal | UNPROVEN and not implemented. Exact source/spec contradiction recorded; detected TCP loss is not substitute evidence |
| Final implementation / full qualification | NOT RUN. Full `go run ./cmd/swarm-test` remains mandatory after approved implementation, not inferred from these targeted passes |

Representative reproduction commands (host `SWARM_TEST_POSTGRES_DSN` for ordinary
selected-store fixtures; do not print its credentials):

```sh
go test -race ./internal/serveapp ./internal/store/construction -run '^Test(ServePostgresGracefulContract|OpenPostgresUsesStockDriver)$' -count=3 -timeout=2m -v
go test -race ./internal/store/internal/runtimepersistence -run '^TestPipelineProcessSIGKILLRecovery$' -count=1 -timeout=2m -v
SWARM_INVESTIGATION_PG_BIN=/usr/lib/postgresql/16/bin go test -race ./internal/serveapp -run '^TestServePostgres(LossAndRestartFromDurableState|LostCommitResponseRecoversDurablePublication|RemotePossessionOutlivesLocalLoss)$' -count=3 -timeout=3m -v
go test -race ./internal/apiv1 ./internal/store/internal/runtimepersistence -run '^Test(ConversationForkCommitBeforeAPICompletionProbe|CompletionCommittedEvidenceAfterHandoffFailureProbe)$' -count=3 -timeout=2m -v
```

The last command is intentionally red evidence. Private-cluster tests skip unless
the explicit test-only binary path is supplied; the reported receipts supplied
it. Test-cluster fsync/synchronous-commit durability defaults were not disabled.
No shared PostgreSQL server was stopped or reconfigured; no live provider was
called and no user-owned workspace changes were modified. #2008 WIP is
untouched. Required closure level remains failure-class elimination at eventual
implementation, but current achieved level is investigation/proof and explicit
gaps only. Request one corrected independent implementation ruling covering the
named local-fencing decision and bounded existing-owner repairs; do not treat
this artifact as approval or resubmit the old timeout prerequisite.

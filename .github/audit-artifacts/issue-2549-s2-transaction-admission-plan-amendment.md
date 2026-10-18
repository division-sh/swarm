# S2 PLAN Amendment: Universal Transaction Admission

Planning only; no S2 production change, runtime receipt, push or hosted run.
This amends the [S2-0 through S2-7 plan](https://github.com/division-sh/swarm/issues/2549#issuecomment-6174585431)
for reviewer-f's [pre-audit findings](https://github.com/division-sh/swarm/issues/2549#issuecomment-6244470155).
Read-only source pin: origin/master 0576b61120ad0f9a188508e084efd74c7e5b1fed.
The historical entrance counts are not a current census.

The [user's S2 gate](https://github.com/division-sh/swarm/issues/2549#issuecomment-6235378160)
still authorizes ONE connected PR with separately reviewable S2-0..S2-7 commits,
no partial-family merge and no compatibility adapter. Implementation starts only
after #2698 lands and the current #2700 widened-deletion checkpoint is posted;
reviewer-f's pre-audit is now in and its findings are absorbed below. Reviewer-a
reviews the S2 PR. No CI-bearing push until the orchestrator releases the slot.
S3 remains a later, separately audited bounded-turn cutover; no Python in S2.

## P1: One Admission Owner, Every Mutating Transaction

The current activity-order fence is NOT generation authorization. In particular,
delivery/lifecycle.go's claim and pipelinepersistence/workflow_engine_mutation_commit.go's
write/emit/settle enter mutationprotocol, whose current AcquireFence acquires
activity order, not current process generation. Delivery row-generation predicates
also do not prove that the original process still owns mutation authority.

S2-1 makes backend/generationauthority the ONE generation-admission owner. Its
transaction admission is invoked by the native pooled/retained PostgreSQL and
SQLite transaction entrances before any mutating domain callback. It consumes the
ORIGINAL process authority from the existing retained/selected ownership handoff,
validates its native binding, obtains the declared shared/exclusive possession,
and compares the current process generation inside that same SQL transaction.
No latest-generation substitution, context marker as permission, watcher-only
check, pre-transaction comparison or caller-by-caller replacement fence.

Ordinary work takes the shared realization of the accepted PostgreSQL transaction
coordinate (0x5357524d, 1). Actual acquire/bootstrap, advance, retirement and reset
operations select exclusive admission initially, with their existing original
ownership evidence. Bootstrap is a named operation, not a missing-stamp bypass.
The activity-order owner remains separate: its FIRST acquisition is at the
commit tail, held from allocation through physical native COMMIT or ROLLBACK.
The old generationauthority-to-activity-order dependency is removed as part of
this connected cutover, not retained as a second global admission queue.

Generation protection belongs to the actual SQL transaction through physical
settlement, including drained native SQL contexts, rollback and uncertain commit.
An already-admitted transaction must settle before exclusive advancement can
succeed. If advancement wins first, queued work carrying predecessor authority
refuses before domain writes. Original caller cancellation is checked even when
the native settlement owner deliberately lends a drained SQL context. Read-only
inspection cannot mint mutating possession. Retry-local admission is discarded
on the original attempt's retirement; a retry must admit its own new transaction.

Waiting on an advisory lock does not refresh a retained SERIALIZABLE snapshot.
For PostgreSQL, the ordinary generation singleton read must use a compatible
locking read rather than the existing FOR UPDATE: shared generation possession
plus a FOR SHARE generation-row read. A replaced row seen through an old retained
snapshot must abort/refuse, not authorize from its earlier version. Exclusive
advance retains its native row update/CAS. Test that behavior on the real native
pooled and retained-session paths; do not downgrade their required isolation or
add a second connection or tail authorization check. SQLite takes its existing
native writer possession before the in-transaction generation read. Its read-only
and transaction-settlement behavior stays native, not a PostgreSQL simulation.

## S2-0/S2-6: Refresh The Existing Entrance Map

Update the original owner/consumer map on the actual integrated master before
semantic coding. This is an amendment to that map, not a new registry/framework.
Every mutating entrance must terminate in the ONE admission owner above; passing
through mutationprotocol is neither necessary nor sufficient by itself.

| Entrance/consumer | Required cutover/accounting |
| --- | --- |
| backend/postgres/transaction.go: RunTransaction* / runTransactionOutcome | Pooled mutation entrance admits original authority before the callback; preserve native acknowledgment and cleanup errors. |
| backend/postgres/session_authority.go: RunAuthorityTransaction* / runAuthorityTransactionWithProof | Same admission on the actual retained transaction; preserve session possession, original-session loss, SERIALIZABLE behavior and physical settlement. |
| backend/sqlite/transaction.go: RunTransaction* / runTransactionOnceOutcome | Same owner under SQLite's actual writer admission; no read-snapshot-to-writer upgrade or alternate authorization path. |
| backend/mutationprotocol/protocol.go; backend/authoractivity/transaction.go | Consume the native transaction's admitted possession; delete early activity-order-as-authority behavior. Evidence=Story/AuthorityFence/RevisionOnly does not select a generation bypass. |
| backend/delivery/lifecycle.go | Real claim, renew/settle and successor operations retain their own exact delivery identities, but cannot use row predicates instead of process admission. |
| backend/pipelinepersistence/workflow_engine_mutation_commit.go | System-node state write, emit and delivery settlement all occur only in an admitted transaction, including the #2730 predecessor case. |
| backend/managedcapability/owner.go: current INSERT/UPDATE entrances | Include direct native mutations outside mutationprotocol; original capability attribution is not a substitute for current transaction authority. |
| backend/channelonboarding/owner.go; backend/operatorchannel/owner.go | Include registration/update/retirement transactions and their original retained ownership handoffs. |
| backend/pipelinepersistence/workflow_emit_feedback.go | Include its direct UPDATE transaction, not just the main workflow mutation path. |
| backend/runforkpersistence and selected-contract runtime/native transactions | Admit the operation's actual original process; historical provenance, fixed-cut reconstruction and successful materialization never grant execution authority. Preserve the C-owned selected-start seam. |
| internal/startupownership, agentpersistence, retained startup/reset owners | Acquisition/advance/retire/reset choose the named exclusive operation at entry; preserve E's startup/ACK/readiness handoff and original grant scope. |
| event, reply, activity, effect, schedule, LLM, decision and run lifecycle owners from S2-6 | Refresh their actual native entrances and full strongest-mode footprints; no partial consumer-family merge. |
| RunReadTransaction, RunAuthorityReadTransaction, inspection/diagnostic readers | Record genuinely read-only entrances separately. They remain unable to mint mutation permission; do not exempt a writer merely because its label says read or proof. |
| Store/schema bootstrap and test fixtures | Name the actual exclusive bootstrap operation and route fixtures through their existing owners; no global absent-authority escape hatch or new raw-SQL exemption. |

Also refresh the complete schema/DDL, expected schema, codecs/readers and all
generated inventories with the authoritative spec in S2-0. S2-0 is NOT merely
rebasing the old list or updating its counts. No baseline increase, schema
compatibility path or historical evidence rewrite.

## S2-L01: #2730 Commit-After-Advancement Proof

Proof cost: adds 2 roots in lifecycle; the invariants/reach they add are native
transaction-admission versus advancement through physical settlement, and actual
system-node claim/write/emit/settle refusal under predecessor authority. Extend
existing owner controls for the other S2-L01 cuts and S2-L02..L08; do not add
mirror roots. If an additional uncovered invariant/reach needs a root, declare
its incremental cost before adding it. Validate exact plan ownership at FINAL.

| Proposed root / existing reach | Required execution, both stores where applicable |
| --- | --- |
| TestNativeGenerationAdmissionSerializesAdvancementBothStores | Two PostgreSQL ordinary transactions can hold compatible admission; SQLite retains its native single-writer ordering. Hold an admitted writer through an actual native write and COMMIT/ROLLBACK; exclusive replacement cannot complete before physical settlement. Once replacement succeeds, the original predecessor cannot commit new work. Preserve acknowledged outcome versus later cleanup error. |
| Same root: waiting/retained reach | Queue original-authority work before admission; advance first and prove no durable candidate mutation. Establish an old retained PostgreSQL SERIALIZABLE snapshot before replacement, then attempt admission; old snapshot cannot authorize. Include the pooled equivalent, cancellation, clean rollback and a genuinely fresh retry. |
| TestSystemNodePredecessorCannotCommitAfterGenerationAdvanceBothStores | Use the actual system-node selected/native owners, not a synthetic comparison. After successor advancement, predecessor claim AND write/emit/settle refuse. Assert no predecessor delivery claim, state mutation, output event or settlement was durably added. Run the lawful current-authority positive operation through the same path. |
| Existing native generation and selected-delivery roots | Extend TestNativeProcessGenerationTransactionsBothStores and TestSelectedDeliveryExecutionFenceBothStores only for uncovered boundary reaches; keep existing exact identities, source isolation and negative controls. No mirror of the new system-node execution. |
| Existing retained-session loss and acknowledgment controls | Name original PostgreSQL session loss while admitted/waiting; native transaction must not survive as a writer on a different session. Keep TestAcknowledgedResultSurvivesCleanupFailure and exact transaction-possession controls. Cancellation alone is not durable-write absence evidence. |

Keep original #2730 REDs and any other retained REDs. These are planned proofs,
not executed receipts. Race x3 applies to the concurrency diffs, with deterministic
barriers and native durable observations; no sleep-only proof, new timeout or
duplicate local tier.

## P2: Recovery And Readiness Are A Separate Obligation

#2730's successor may retain lawful preceding commits yet fail during recovery
or readiness. The admission repair above does NOT prove that successor recovery
succeeds. Keep a separate #2730 recovery/readiness row cross-pinned to E's #2701
and reviewer-e/reviewer-a, preserving the original committed evidence, startup
recovery read-back, ACK fence and readiness outcome. The single out-of-order
observation remains UNVERIFIED. No whole-#2730 closure claim without the distinct
recovery execution evidence; no competing S2 startup recovery implementation.

Apply the [F1 classification](https://github.com/division-sh/swarm/issues/2728#issuecomment-6235477958)
to checks encountered during this cutover: one transaction-admission fence,
boot/resume recovery read-back, an S3 cross-transaction-step finding, external
boundary validation, or a deletion candidate requiring its disposition. Do not
use this amendment as blanket permission to keep repeated checks of core-written
rows. A second same-transaction generation check is a duplicate, not a tail
fence. S2's pending domain decisions still need their specifically owned
under-lock realization; document the invariant and classification rather than
calling every old comparison freshness.

## Connected Cutover And Qualification

S2-2 shared ordinary/exclusive lifecycle run admission, S2-3 atomic counter row
and same-attempt count reads, S2-4 insert-only completion evidence, S2-5 sorted
revision/counter tail and FIRST activity-order acquisition, S2-6 all consumers,
and S2-7 destructive exceptions/native failure closure remain ONE cutover. No
ordinary runs-row counter/completion UPDATE that recreates the exclusive queue.
Preserve exact CAS/story order, UUID identity, pending delta visibility,
completion ownership, destructive pre-delete evidence, cancellation and ACK-only
follow-up. No post-domain conflict reclassified as a clean acquire-fence retry.

Refresh the path-to-hosted-family map after integration; qualification owner
validates the actual disjoint selection, including required named reaches. No
new selection, blanket full, local lifecycle/full tier or server2 campaign is
claimed by this amendment. Focused proofs plus affected owner/structural guards,
regeneration/diff, complexity, unused, the real debt ratchet (-timeout=20m) and
exact-head local preflight are still owed before the eventual approved push.
Old receipts are not S2 execution credit. No savings guarantee is asserted.

Request: reviewer-f confirm the pre-audit P1/P2 mapping above; reviewer-a holds
the eventual connected implementation and FINAL HEAD to this amendment. This
does not reopen a product choice or waive the #2698/#2700 start dependencies.

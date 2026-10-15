# Source Fork Cardinality: Local Whole-Family Checkpoint

The named source/history hold below is resolved by the subsequent finite repair
in issue-2542-rollback-retirement-source-fork-composition.md. Original RED evidence
and the signed semantic checkpoint remain immutable; parent closure stays open.

Semantic transform follows the existing bulk rule gate6134544118. Signed source
predecessor0396a9b746ae7a4c2d901e19be8858f4dfa3f553. Stacked after the separate
public-rollback composition repair, with its accounting hold preserved.

Eight QueryRow/Scan observations in six complete declarations across five
served/conformance files now use the existing storetest.CountSelectedForkRunsForSource
-> private CountSelectedForkRunsForSourceForTest ->
runforkpersistence.CountSelectedForkRunsForSourceTx owner. Its physical predicate
is unchanged: COUNT(*) FROM runs WHERE forked_from_run_id=$1. No new projection,
query, schema, authority inference, retry or raw capability is introduced.

Served observations borrow the already captured selected reader in one snapshot;
missing/failed outer acknowledgment discards the scalar. Conformance keeps its
original f.ctx/f.selected and separate observation cuts. Original destination,
source IDs, first-error/count assertions, API calls, replay sequencing, waits,
fault installation and cleanup stay fixed. Raw stores remain for their other
approved/unmigrated construction and fault consumers; no getter-deletion credit.

| Manifestation | Status | Exact execution proof |
| --- | --- | --- |
| Completion-loss plus same-key retry | reproduced and fixed | TestAudit2277ForkCompletionLoss, both backends under race |
| Timer-bearing served fork refusal | reproduced and fixed | TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation, original error/mutation/timer assertions under race |
| Deployment fork before first row | reproduced and fixed | TestDeploymentSourceSelectedForkBeforeFirstRow, both stores under race |
| Concurrent first materialization plus retry | reproduced and fixed | TestDeploymentSourceSelectedForkConcurrentFirstMaterialization, both stores under race |
| Shared changed-pin/lost-response path, deployment caller | reproduced and fixed | TestDeploymentSourceChangedPinPublicForkAndLostResponse, both stores under race |
| Same helper, explicit-event caller | execution-proven through the same corrected path | TestEventSourceChangedPinPublicForkAndLostResponse, explicitly run both-store race proof |
| Dynamic receiver refusal without mutation | reproduced and fixed | TestDeploymentSourceDynamicReceiverSelectedForkRefusesWithoutMutation, both stores under race |

Receipts prefix: /home/youmew/.cache/swarm-2542-family145/source-fork-cardinality-.
Served roots plus missing-reader control PASS24.387s; deployment/concurrent/dynamic
conformance roots PASS48.137s; explicit-event variant PASS32.788s; existing native
owner positive/refusal and three persistence guards PASS38.668s. They carry across
the disjoint rollback composition repair: all six bodies equal the finite outputs
and all three existing owners plus both point callers remain unchanged.

Finite transform manifest46e028ffb16b29264fe757efb4bb67aa2e98cdda9236fb3398e589dfc96740e8,
six bodies/eight reads; readonly generator reproduces it. Three finite/owner/owned
SQL inventory controls PASS0.560s, including idempotence and changed identity,
context, source predicate, assertion, API/wait/replay cuts. Owned-source walker
rejects reintroduced single-source SQL including package initializers. The
conjunctive source-plus-event query is explicitly different and not substituted.

Repo-wide sibling scan: outside served/conformance, matching SQL is confined to
the existing private selected-fork aggregate/read owner and private storage test
oracles (snapshot ownership, materializer and fan-out native proofs). These are
inside the already-classified private storage boundary, not new public fixture
read consumers. Aggregate refusal snapshots retain their broader multi-table
one-snapshot contract. This wave does not claim global raw-authority or restart
qualification, nor does it retier or weaken an existing proof.

## Exact Source-Evidence Boundary

The cheap served-debug source guard is RED0.072s on the one amended timer-bearing
root. It freezes the whole body as required49-pair/71-occurrence evidence, so a
new finite source-only inverse needs disposition before any accounting refresh.
File: internal/serveapp/main_runtime_test.go.
Function: TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation.
Before89f5dd1b3bc9e48b823be7a697ec25354ea0265bdc1007cbcfd4d04b81386c4b.
Current3eccce2f3034abc34dc6af25adae10cc146ae7f19f20c6905493c75534423e11.
The exact six-body manifest supplies the unchanged complete predecessor and
finite current body; no inverse/signature/pair transition has been implemented.

History intersections are explicit: timer-bearing root already has a Successor;
the shared changed-pin helper has one existing After equal to this Before;
completion-loss has an older recorded endpoint; three conformance declarations
have no prior recipe. Preserve every original row/field/order and any prior
Successor; do not blanket reconcile or overwrite the corpus. The native ports
and both point consumers are independently pinned. A finite current-source
composition/ledger disposition is requested for exactly this six-body wave.

Baseline/findings remain6678/4466; no new credit is claimed. The prior public
rollback historical-deletion refusal remains separate. No redundant expensive
census is run while these named source/accounting holds are live. No production
runtime/spec semantics change, compatibility seam, new issue, push, PR, tier or
server2 qualification. Existing #2542/#2151 watchlist and final integrated/fork
deadline/rebase/landing obligations remain open.

# Batch 3 Local Family: Native Loop Claims And Exact Reply Loss

Parent: #2542 / #2151. Predecessor: `b30cad069`, based on masteraf250de63.
Chosen class: the complete loop activity seed/claim cohort, including its
reply-loss continuation. Both old seed consumers move together; the raw runner
and its handwritten transaction-context protocol are deleted, not preserved.

## Owners, Path And Temporal Cuts

Run and prepared loop state -> existing selected activation construction owner
-> original native workflow journal claim -> repeat/close or exact reply loss
-> native receipt reconciliation -> no provider redispatch/no premature result.
The component inputs retain identical loop activation, state carrier, stage,
entity/flow identity and materialized projection values. Empty component route
sets are not public constructor eligibility or attachment/readiness evidence.

`ActivityClaimReplyLossPersistenceFaultForTest` is a named test-only reply cut,
not a replacement coordinator or transaction runner. It validates canonical
run/request identity and an absent receipt against the original selected native
owner, then delegates every claim to that owner. Only an actual successful insert
at the exact key consumes its one-shot cut. The response is withheld with the
specified error; persisted state and native commit ownership are unchanged.
Cancelled/failed calls, unrelated keys and duplicates cannot consume the cut;
an existing receipt cannot rearm it. Native receipt loading supplies recovery.
The public bridge exposes only opaque workflow authority and a detached count.

The failure-only status diagnostic retains the original whole-store query,
request/status fields and ascending request ordering, with no run, eligibility
or terminal filter. Its existing selected read coordinator owns every row;
late scan failure discards even a successfully read prefix. No query selector,
SQL callback or raw handle crosses either component fixture port.

Binding specification is selected-runtime-store projection's `raw_sql_policy`,
including the new bounded `activity_claim_reply_loss_fixture` clause. This
documents the approved named live-fault migration, not production journal,
transaction, lifecycle or replay policy changes. Existing owners suffice;
there is no new framework or G/C/A first-landing operation.

| Manifestation | Exact proof |
| --- | --- |
| TestLoopActivityClaimOrdersAgainstRepeatAndCloseOnBothStores | All original claim-wins, repeat-wins, close-wins and duplicate-claim cases execute on both native stores; the started generation survives repeat, stale claims leave no journal row, and duplicates retain the original started receipt. |
| TestLoopActivityClaimCommitAcknowledgmentLossReconcilesWithoutDispatch | Actual native loop construction and claim precede exact reply loss; one cut is witnessed, the original generation/status persist, and provider calls and published results remain zero. The original SQLite cell remains and PostgreSQL is added. |
| TestActivityClaimReplyLossUsesExactNativeCommitAndCannotRearmBothStores | Cancelled call and sibling run cannot consume the cut; exact native insert does; duplicate yields the complete original receipt, cannot rearm, and exactly three native writer transactions settle for two inserts plus the duplicate with no active work. Global diagnostics include both runs in original order. |
| TestActivityClaimReplyLossAndDiagnosticRefuseInvalidAndClosedOwnersBothStores | Raw/nil/uninitialized owners, invalid/empty identities, absent fault and closed storage yield no opaque fault authority or diagnostic evidence. |
| TestWorkflowActivityAttemptStatusDiagnosticDiscardsPartialRowsBothStores | A real first row followed by a malformed NULL status fails without leaking a prefix, then the exact original table is restored through the selected writer. |

## Codemod And Guards

Three finite snapshots (both roots and their shared seed) extend the existing
executable72 ->75, leaving preceding recipes unchanged. Independent controls
retain claim calls, generation/status/no-dispatch assertion conditions, repeat
and close ordering, provider callback, input construction, intent assignments,
and exact loop/carrier construction arguments. Changed generation assertions
or carrier stage fail adversarial controls. Fake-runner diagnostics are replaced
by the explicit witnessed reply-loss count; row iteration moves below the
detached diagnostic owner, not into another consumer SQL helper.

The executable applies exactly three functions after complete overlay type
checking; final-source repetition is inert. Deletion of the unused runner is
the owner repair accompanying the finite caller output. The exhaustive approved
collector powers the new family guard; it fails on the compiled507395616
predecessor, rather than compilation. Migrated guard11.61s, hostile raw argument/
callback control0.07s and complete inventory2.62s pass. No scanner, allowance or
owner scope was weakened.

## Measured Evidence And Disposition

- Pipeline: two original roots / four backend cells / fourteen passing records
  under race, zero fail/skip, successful package terminal13.220s; all eight
  original ordering cuts remain inside those cells.
- Private fault/diagnostic: three roots / six backend cells under race, zero
  fail/skip, successful package18.265s. A new-control failure initially expected
  two writer commits; independent receipts showed three because the real native
  duplicate also settles its original transaction. The corrected control
  distinguishes transactions from inserted rows and preserves exact two-row
  and complete-receipt assertions. No production defect or retry was hidden.
- Downward census: `14,861 ->14,850` findings, **11 removed / ZERO added**;
  confirmed raw-operation debt `11,027 ->11,019`; all67 excluded uncertainties
  and collector494fd3b6300c4163241395ef9e3aa59ce58eb32f45e9f5d8bc5a5078401303d5
  unchanged. Ratchet59.24s. Nine exact new descriptive facts are classified:
  seven private diagnostic operations and two read-only count callbacks;
  the complete registry verifies12,483 facts.
- All75 snapshot/preflight/workload/negative controls pass. The new static
  workload control was repaired to handle absent loop header fields and the
  moved diagnostic iteration; no provider/delivery iteration is excluded.

Receipts: `/home/youmew/.cache/swarm-2542-local-20261003/batch3-native-loop-*`.
Cumulative local batch reduction is182 findings, ZERO added. Exact-head
complexity and final reviewer-selected qualification remain publication gates;
no server2, broad tier or repeated exhaustive matrix was used mid-migration.

Closure is both loop-claim consumers and their complete shared seed/reply-loss
cohort, not the parent class. General pipeline setup, external read ports,
remaining serve/fork families and final zero-debt qualification stay explicitly
under #2542/#2151. The existing watchlist mapping remains sufficient. Named
native construction, claims and detached observations replace the existing
type-model smell; no compatibility path, raw getter, extra issue or semantic
ownership decision is introduced.

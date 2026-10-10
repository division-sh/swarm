# Daily Rebase And Canonical Event Owner Reconciliation

Predecessor: bb430386f. Base changed from139f0eedf to5ea6d33d6.
The pre-rebase branch is preserved locally. The range-diff and all receipts
are retained under the increment29 local evidence prefix.

## Binding Boundary And Tracker Repair

Existing broad #2151/#2542 approval remains binding. The owner overlap was
recorded before continuation on #2542, comment6049090141. Routine consumers
inside this declared class do not require another gate; no new operation,
framework, runtime behavior or compatibility allowance is introduced.

Master's mandatory canonical fixture loader and this branch's optional reader
both interpreted the same physical event. The shared canonical optional read
now owns validation, ONE native read transaction, the existing backend record
loader and complete Record.Decode. The existing mandatory loader consumes only
that operation and enforces its distinct missing-record error requirement.
It performs no SQL, store selection, decoding, normalization or reconstruction.
All previously existing mandatory and optional caller contracts are retained.

Authoritative platform-spec.yaml canonical_event_fixture_readback explicitly
records this one-operation rule. The existing core native API census family
now consumes an exact AST owner-shape guard; no new planner unit or tier.

## Exhaustive Consumers And Proof

| Surface | Disposition | Proof |
| --- | --- | --- |
| Public storetest.LoadCanonicalEventRecord and its complete seven-call lineage/API/gate/fork family | already consumes canonical owner | Existing finite recipe/shape/hostile controls; original caller proof unchanged. |
| Mandatory readback native tests and delivered-fork settlement witness | moved to canonical owner | Existing mandatory error/success contract roots under race on both stores, including exact one original read and SQLite read bypassing writer admission. |
| Optional complete-read native tests and mailbox complete-event reader | already consumes canonical owner | Both-store complete record equality, missing absence, malformed record, raw/cancelled/closed refusal and original coordinator count. |
| Reintroduced independent required-reader SQL/decoder or dropped absence/key/context | invalid | Core-consumed exact AST shapes plus six hostile mutations; both core guard invocations structurally pinned. |
| Physical whole-store snapshot reader | rebased onto existing owner | Preserve master's extracted row-close/error owner and this branch's typed column/value encoding. Existing typed snapshot roots and source-domain snapshots qualified. |
| Pipeline handoff/receipt owner additions | retained independently | Preserve master's handoff/complete receipt readers plus bounded fixture count/outcome contracts. Whole count must not acquire unrelated failure/side-effect decoding dependencies. |
| Remaining fixture construction and direct SQL | still bypasses and explicitly tracked | Decreasing #2542 exact-site ledger, not closed by this reconciliation. |

The one new source-domain caller propagates its exact existing column list to
the shared typed encoder. The finite snapshot recipe is re-anchored to
master’s extracted row reader, preserving all close/error/sort checks rather
than reverting the master refactor. No table or identity is omitted.

Master's whole-inventory positive oracle initially refused the new typed wire
format. Its literal expected rows now pin column names, native int64/string
types and SQL NULL, while retaining exact duplicate rows, lexical JSON, sort
order, empty tables, view exclusion, all application tables, one original
read, mutation detection, snapshot immutability and column-array identity.
No assertion, retry, skip or timeout was weakened; the initial red is retained.

## Ledger And Closure

Master's exact G01 collector transition is consumed unchanged. Registry rows
from both branches are reconciled, then regenerated with every new fact
explicitly classified; stale rows are removed, not retained as exemptions.
The collector policy is not optimized or amended. Census timing/correctness,
global guards, complete codemod/type controls, native unused, complexity and
focused both-store receipts are recorded before the rebased branch is pushed.

This closes the overlapping event interpreter and preserves all touched
consumer/observation obligations. It does NOT close the parent. Existing
watchlist mapping suffices; no new tracker or architecture decision. Strict
global completion, zero debt, SQLite fork-deadline and final integrated proof
remain open.

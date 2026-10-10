# Published Original-Event Dead-Letter Observation

Routine exact-storage read migration under #2151/#2542. The shared catalog
original-event count now consumes the existing delivery dead-letter owner
through the original native read coordinator. Its sole runtime-outcome
consumer propagates the already-held typed event-reader projection.

The loop's published ID set, per-ID whitespace trim, exact original_event_id
predicate, sum, count-greater-than-zero assertion and read-error refusal remain.
No relation is fabricated and no event diagnostic gains dead-letter credit.
The entity/time relation and failure-only diagnostic projection are different
observation concepts and remain explicitly tracked catalog debt, not claimed
closed by the original-event count.

## Consumption And Proof

| Surface | Disposition and proof |
| --- | --- |
| assertPublishedEventDeadLetter and shared runtime outcome | Native delivery owner, finite helper recipe and cumulative whole-caller oracle preserve every other assertion and the chain-depth exclusion. |
| Actual relation for a selected original event | Both-backend proof writes a real source event and dead letter through their canonical operations, requires one exact row and one original read commit/zero writes/no active transaction. |
| Another event with the same entity/type, and empty selected set | Sibling receives no relation credit; empty selection remains false; whitespace-trimmed original ID still receives credit. |
| Published event without a dead-letter relation | False before canonical RecordDeadLetter; event existence is not relation existence. Existing diagnostic-alone/no-credit control remains intact. |
| Canceled or closed selected owner | Native port refuses and returns no count evidence; no fallback is added. |
| Wrong owner/event/context, lost sum or ignored read error | Independent finite oracle rejects each mutation and weakened cardinality. |

Qualified with focused native/race roots, complete finite/type/hostile controls,
all structural guards, decreasing census/registry, native unused, contracts
and exact committed-head complexity. Receipts are under
/home/youmew/.cache/swarm-2542-local-20261008/catalog-published-deadletter-*.
No production semantics, spec, collector, guard permission, framework or
compatibility change. Existing watchlist mapping suffices; parent zero debt,
strict guards, fork deadline and integrated qualification remain open.

Measured debt: 13,985 findings / 10,300 raw sites becomes 13,980 / 10,296:
five findings/four sites removed, zero added. Five exact private read/scan/
coordinator occurrences are explicitly fixture-2151 in the unchanged registry
policy. Final selected sweeps have 259 passing roots and no failures/skips;
the 265 finite recipes, 78 guards, native census children and contracts pass.
Native unused, inert replay and exact-head complexity precede the push.

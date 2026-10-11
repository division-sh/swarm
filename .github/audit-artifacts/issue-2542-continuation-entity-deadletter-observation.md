# Catalog Entity Dead-Letter Relation And Failure Evidence

Routine exact-storage observation migration under #2151/#2542. The catalog
entity/time relation and its failure-only diagnostic projection consume the
existing delivery dead-letter owner through the original selected read
coordinator. No constructor, raw getter, callback, permission or coordinator
is exported. No production semantics or authoritative spec changes.

The physical relation is not canonical entity authority: it preserves the
nonempty original-payload entity reference, empty-payload stored-entity
fallback, inclusive created_at cut, UTC and whitespace normalization, and
count-greater-than-zero assertion. Failure diagnostics retain all physical
rows, created_at/dead_letter_id order and four displayed fields. They now
report read/scan/iterator refusal explicitly instead of silently discarding
it; the primary assertion still fails and diagnostics never award credit.

| Manifestation | Named proof |
| --- | --- |
| Entity/time relation and shared entity assertions | Four finite recipes plus the cumulative whole-caller oracle; exact shape and mutation controls preserve trim, UTC, count, native context, owner and refusal. |
| Actual persisted relation, payload/header disagreement and empty payload | TestCatalogEntityDeadLetterObservationPreservesPhysicalRelationBothStores uses canonical events and RecordDeadLetter; both dialects retain the exact physical fallback. |
| Inclusive boundary, later exclusion, unrelated entity | Same both-store control exercises exact microsecond cuts and wrong-entity exclusion. |
| Diagnostic-only publication | Original TestCatalogDeadLetterRelation_DiagnosticAloneGetsNoCredit still passes; a relation-free event receives no credit. |
| Failure evidence order and fields | Both-store control checks every row and one selected read commit, zero writes and no active transaction. Exact owner contract preserves row closure and scan/iterator errors. |
| Canceled, closed or foreign owner | Both-store native ports refuse without returning count/row evidence; no fallback. |

The finite inventory is 269 recipes. Final qualification includes focused
both-store race roots, complete finite/type/hostile controls, 78 structural
guards, decreasing census/registry with all 16 native-family children,
contract checks, inert replay, native unused and exact committed-head
complexity. Receipts: /home/youmew/.cache/swarm-2542-local-20261008/catalog-entity-deadletter-*.
The initial finite-inventory check correctly rejected the stale 265 count;
the four reviewed recipes were added to that explicit count before final
qualification. No guard or assertion was relaxed.

Existing watchlist mapping suffices. Parent zero debt, strict completion
guards, fork deadline and integrated qualification remain open. This cohort
does not claim catalog constructor or parent-class closure.

Measured debt: 13,980 findings / 10,296 raw sites becomes 13,963 / 10,283:
17 findings / 13 sites removed, zero added. Fourteen exact private read,
scan, row-lifecycle and coordinator occurrences are classified fixture-2151
after source review; the classifier and permissions are unchanged.
Final selected sweeps: 263 passing roots, zero failures/skips. Native unused,
inert replay and exact committed-head complexity must pass before push.

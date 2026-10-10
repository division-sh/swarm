# #2542 continuation: recovered readiness physical snapshot

## Canonical Owner and Consumption

Governing boundary: selected_contracts.selected_runtime_store_projection.raw_sql_policy.
The existing selected-fork application snapshot owner now supplies one fixed
recovered-run witness: entity_state, flow_instances,
flow_instance_runtime_readiness and events, all physical columns and all rows at
the exact run_id. The original query and table inventory remain unchanged.
One original selected read transaction replaces four independent read cuts.
The existing readSelectedForkSnapshotRows encoder supplies detached column/type/
value evidence, sorted rows with duplicates retained, explicit empty-table schema
and complete failure refusal. No public table, SQL, dialect or callback selector.

selectedForkRecoveredPhysicalSnapshot consumes the owner and serializes its full
detached map. Its only two callsites remain unchanged: before actual recovered
activation and after each contradictory recovery refusal. No fault injection,
activation, source cleanup, committed submission-error, final-stage, typed refusal
or equality assertion was removed or moved. The original native observer may
inspect physical evidence without gaining schema admission or write authority,
as already specified by the whole-application snapshot owner.

This replaces the local column scan, driver-value serialization, sorting and
raw-handle plumbing. The shared typed encoding strengthens the earlier untyped
JSON witness; its existing bytes/text/NULL/numeric/timestamp controls are reused.
PostgreSQL UUID bytes and SQLite UUID strings remain distinct driver evidence;
only the existing timestamp-location normalization is used. No historical hash
or external byte-format contract consumes this helper; both comparisons use the
same new complete physical representation.

## Manifestation Proof

| Manifestation | Exact proof |
| --- | --- |
| Actual valid recovered activation | TestSelectedForkRecoveredReceiverReadinessBothStores valid under race on SQLite/PostgreSQL |
| Actual committed submission failure | Same root, submission_failure, preserving running/closed committed evidence and exact typed failure |
| Contradictory header/readiness without mutation | Same root, inactive and missing_readiness, unchanged pre/post physical equality and refused publication assertions |
| Original coherent reader, full fixed schema and exact run | TestSelectedForkRecoveredSnapshotPreservesExactRunInventoryBothStores; one original read commit, no writes, exact driver run value, empty foreign-run inventory and repeated immutability |
| Invalid, foreign, canceled, closed or late table failure | TestSelectedForkRecoveredSnapshotRefusesPartialAndForeignEvidenceBothStores; no earlier-table partial evidence |
| Complete typed row representation | Three existing TestSelectedForkSnapshotEncoding roots, including physical type distinctions and incomplete/unencodable refusals |
| Consumer/query/identity drift | Whole-function finite recipe, exact original fixed table/query oracle, candidate type overlay and hostile owner/context/run/partial-map/error controls |

The original nineteen-variant recovery workload is preserved, not exhaustively
rerun mid-migration. The focused eight backend/variant cells above are actual
execution evidence; additional integrated lifecycle qualification remains owed.
The first PostgreSQL proof wrongly assumed string UUID driver values and failed;
the corrected proof asserts the actual byte representation without changing the
encoder, production reader, schema or equality assertions.

This cohort closes the recovered physical snapshot subfamily, not the remaining
live corruption writer, failure-only diagnostic dump, reconstructed-store probe,
shared catalog construction or parent migration. Those remain named #2542/#2151
debt. Existing watchlist and broad gate suffice; no new framework, compatibility,
tracker, semantic permission or authoritative runtime-spec change is required.
Exact counts/final receipts are recorded on the increment issue comment. No
core/full, hosted CI, fork-deadline or parent closure qualification is claimed.

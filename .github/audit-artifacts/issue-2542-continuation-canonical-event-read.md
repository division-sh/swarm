# Canonical Event Fixture Readback

Parent #2542 / #2151. LoadCanonicalEventRecord no longer obtains a raw
database from either concrete store. The named private operation uses the
original selected native read transaction and SAME backend event-record loader,
then the existing complete decoder. It reads the exact event ID only, adding no
delivery, run, readiness, eligibility or pagination interpretation. Missing,
invalid owner, cancellation, closed storage and malformed canonical facts yield
no partial event. No reconstruction, fresh clock or fabricated admission is used.
The private operation lives in the EXISTING test_event_support.go owner. The
closed import guard correctly refused an intermediate new-file placement; that
file is removed without changing the guard or its owner inventory.

Systematic consumers (seven calls): contextual lineage parity; SQLite and
PostgreSQL supported conversation API surfaces; proposed-effect approved gate
release helper; served fork route-recovery rejection; and the two selected-fork
conversation-lineage/divergence roots. Their authored source, persistence,
execution gates and assertions are unchanged. The source-backed named completion
guard prohibits raw authority in the shared reader; untouched insertion/setup
fixtures remain parent migration debt, not exceptions to this closed reader.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Complete canonical physical event read | reproduced and fixed | TestCanonicalEventObservationPreservesCompleteRecordAndOriginalReadBothStores under race: exact complete decoder output and one original read/no write. |
| Missing/corrupt or unavailable evidence | execution-proven through the same corrected path | Same root's absence and original-coordinator malformed-record cut; TestCanonicalEventObservationRefusesRawCancelledAndClosedOwnershipBothStores under race. |
| Shared callers' lineage/API/recovery/fork contracts | execution-proven through the same corrected path | Exact seven consumer paths qualified in the increment receipts; no changed root assertion or workload. |
| Wrong key/owner/refusal or raw capability | execution-proven through the same corrected path | Independent native canonical-read AST oracle, hostile changes, complete finite overlay and raw-parameter compound-fixture guard. |

Closure: shared complete-read family, not every canonical event seed, generic
getter consumer or selected-store test. Final zero-debt and integrated proof
remain open under the existing parent. Existing event-record and native-read
owners suffice; no new framework, tracker or watchlist branch is warranted.
Binding spec: selected_runtime_store_projection.raw_sql_policy,
canonical_event_fixture_readback. Grouped census/registry, guards, overlay,
complexity and definitive unused are required before push. No aggregate tier
run or PR is claimed.

Receipts: /home/youmew/.cache/swarm-2542-local-20261007/increment19-canonical-event-*.

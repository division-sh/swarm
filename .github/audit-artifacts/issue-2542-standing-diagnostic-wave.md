# Standing Diagnostic Whole-Chain Proof Audit

Signed predecessor: 67f4b617d76f23b291502f00f8aecc2edad3c4f6.
Binding rulings: #2542 comments 6155858884 and 6156119961. The second
explicitly retires the obsolete receipt section and supersedes the original
four-projection/four-cut requirement. This is a local whole-family checkpoint,
not a parent-closure, PR, landing or integrated-qualification claim.

## Concept, Owners And Consumption

The changed concepts are diagnostic-only selected-store authority, exact
global diagnostic projections, and retirement of an unsupported table reader.
The chosen working class is the complete standing/agent-memory diagnostic
chain's writable construction, raw getter, SQL/cursor and obsolete-receipt
interpretation escapes. The immediate parent is #2542's remaining fixture and
observer authority migration; #2151 remains its broader confinement parent.
The issue is broad, not symptom-shaped; the two helpers were entry points,
not the audit boundary. This checkpoint closes this bounded diagnostic family,
not every reader which happens to print debugging information.

Canonical owners and exhaustive consumption within this family:

| Owner | Exact projection/lifetime | Consumers |
| --- | --- | --- |
| Existing storetest.OpenReleaseProcessReadOnlyInspection, native InspectSnapshot and Close | One independently owned read-only inspection per diagnostic invocation; no initialization, acceptance, grant, process or writer authority | standingStoreDiagnostics and renderStandingStoreDiagnostics; both original helpers are thin delegates. |
| eventrecord.ReadStandingEventNameDiagnosticsTx | Global event_name/COUNT(*), GROUP BY and ORDER BY event_name | Existing test_event_support.go bridge, bounded storetest port, first renderer snapshot, both original helpers and all their original callers. |
| eventrecord.ReadStandingRuntimeLogDiagnosticsTx | Global platform.runtime_log payload, backend-native text, ORDER BY created_at only | Existing test_event_support.go bridge, bounded storetest port, second renderer snapshot and the same complete chain. |
| delivery.ReadStandingDeliveryDiagnosticsTx in the closed read_projections.go | Global event-ID text, subscriber_id, COALESCE(status,''), ORDER BY created_at only | Fixed private/storetest bridge, third renderer snapshot and the same complete chain. |

The owner choice is semantic, not merely the first local file: eventrecord owns
event/log projection, delivery's closed lifecycle reader owns delivery SQL,
and the pre-existing independent inspection owns acquisition and close.
The fresh structural guard rejected the first delivery file placement. The
unchanged function/type were moved into its existing closed read_projections.go;
no guard exception, file allowance or alternate delivery reader was introduced.

The manifest pins 14 complete predecessor/current declarations: both helpers,
requireStandingTelegramCalls, the SQLite/PostgreSQL/mixed standing restart
roots, standingWebhookDiagnostics, sendStandingTelegramUpdate,
standingMemoryStoreDiagnostics, runStandingTelegramMemorySupportedSurface,
both canonical Telegram memory roots, and the two publication helpers.
Only the two original diagnostic declarations change; all twelve consumer
bodies, timeout/deadline choices, formatting prefixes, failure assertions and
teardown are byte-identical after canonical AST formatting. The owned walker
accounts for all nine helper/diagnostic references, including three new direct
execution/absence controls, and rejects unmanifested aliases/initializers.
Eleven exact owner/bridge endpoints pin queries, dialects, scan handling,
snapshot cuts, close and formatting. Every family consumer reaches those
owners; there is no surviving same-concept raw path in this chain.

The success-bearing waitForStandingDeliveryQuiescence is untouched and remains
explicit parent debt. Existing run-filtered/latest-log, five-column delivery
and canonical receipt readers have different predicates/columns/order and are
not silently repurposed or credited as part of this family.

## Exact Retirement And Error Boundary

Both helpers lose sql.Open/NewSQLiteRuntimeStore, four DatabaseForTest calls,
their SQL/cursors and the pipeline_receipts query/receipt: rendering arm.
There is no replacement receipt API, table/view, probing/fallback, translation,
schema migration or no-op owner. Legitimate event_receipts readers stay intact.
The binding platform_tables.tables.event_receipts contract describes platform
acknowledgment outcome/processed_at, not the obsolete status/updated_at shape.
This is dead test-diagnostic deletion, not changed runtime receipt semantics;
the ruling requires no platform-spec migration.

The three live cuts remain independent and ordered: names, logs, deliveries.
The first open/query/SQLite name-scan failure renders its error; later query
failures omit that section. PostgreSQL skips individual name-scan failures;
both backends skip malformed later display rows. Valid rows and native payload
text survive those tolerated scans. Cursor/close/transaction/ownership failure
returns no provisional section; InspectSnapshot's result gates rendering.
Each cursor/read cut finishes before the next; the independent inspection is
closed on every return path. No diagnostic emptiness or partial output proves
success, absence, quiescence, completion or authority.

## Manifestation Proof

| Known manifestation | Coverage mark | Exact proof |
| --- | --- | --- |
| SQLite diagnostic initialization and missing-file repair | execution-proven through the same corrected path | TestStandingSQLiteDiagnosticsMissingDatabaseStaysMissing; TestStandingDiagnosticsFirstReadFailureDoesNotInitializeOrRepairBothStores, empty/corrupt SQLite bytes unchanged. |
| PostgreSQL first-read failure and unintended acceptance | execution-proven through the same corrected path | Same first-read root on schema-free PostgreSQL; existing independent-inspection admission proof carries; opener and close endpoints are pinned. |
| Global data lost by run/source filtering, limits or richer delivery projection | execution-proven through the same corrected path | TestStandingDiagnosticsReadOnlyWholeChainBothStores: two genuinely published independent runs, grouped names, both delivery identities and causal log evidence; exact query/dialect controls. |
| Native payload text, timestamp-only order and three separate cuts | execution-proven through the same corrected path | Same native root: unchanged complete text, exact payload strings, three begins/read commits, zero write commits/active reads; driver query equality rejects added ordering/arguments. No tie order is invented. |
| Diagnostic close retires the writer or changes durable data | execution-proven through the same corrected path | Same native root: complete application-storage snapshot equality, closed diagnostic refusal and a subsequent successful public event publication. |
| Empty supported stores | execution-proven through the same corrected path | TestStandingDiagnosticsEmptyDataBothStores. |
| Canceled, raw, invalid, closed or foreign inspection | execution-proven through the same corrected path | Native lifetime refusal plus TestStandingDiagnosticPortsRefuseInvalidCancelledClosedAndForeignOwnersBothStores, including retired snapshot context. |
| First/later query and malformed-row/cursor/close behavior | execution-proven through the same corrected path | TestStandingDiagnosticProjectionsPreserveDisplayScanPolicies and TestStandingDiagnosticProjectionsDiscardFailedCursorSections, both dialects; first-read native root; exact renderer failure-policy endpoint controls. Local driver is only a display/cursor control, not a runtime authority stand-in. |
| Obsolete receipt query silently contributes nothing | reproduced and fixed | Retained native RED receipt: SQLite missing table and PostgreSQL 42P01. TestFamily145StandingDiagnosticReceiptRetirementIsTargetedAndRejectsReintroduction rejects both complete old bodies and reintroduced query/owner mutations while accepting immutable history and unrelated legitimate labels. |
| Standing timeout and agent-memory composition | execution-proven through the same corrected path | Direct native root calls standingMemoryStoreDiagnostics; both original helpers execute there. TestCanonicalTelegramAgentSupportedSurfaceSQLitePostgres, both recovery_on_restart=true cells, passes under race. Complete unchanged caller declarations pin the timeout path; no claim that a passing journey entered its failure branch. |
| Remaining success-bearing quiescence and mixed readers | split / escalated as separate class | Exact unchanged function/source inventory, already retained in #2542/#2151; no migration, execution or closure credit here. |

The committed migrate-standing-diagnostics transform is finite and replay-inert:
14 predecessor bodies, two amended bodies, eleven endpoints; manifest SHA256
5d58067e927054768fcd278e82a4fb6d0f9ec60b0cf0f9c507cfb30e8d4cac72.
Five new controls cover signed source, idempotence, owner/predicate/order/error
mutations, targeted retirement, owned caller census and immutable history.
The reconciler adds exactly two recipes/transitions (1513/817). Every previous
Before/After/Successor record and previous relative order are preserved.
The first history control incorrectly assumed the new entries were a suffix;
it was repaired to remove only the two exact authorized new records before
comparing the complete ordered prior ledger. Its original RED is retained.

## Evidence, Accounting And Tracking

Evidence prefix: /home/youmew/.cache/swarm-2542-family145/standing-diagnostic-.
Direct native proof: PASS four roots / eleven cases under race, 22.929s.
Affected memory callers: PASS both supported recovery=true cells, 86.527s.
Private refusal after extraction: PASS both stores under race, 15.436s.
Closed placement, display scans/cursor and affected owner guards: PASS under
race, 16.707s. All five final finite amendment controls PASS, 0.640s.
Existing finite/idempotence/admission/historical roots pass from the prior
73.359s run except its disclosed history-index error; the bounded history
repair passes 0.299s and again in the final five-control receipt. Unaffected
receipts carry, rather than repeating the entire tool/guard collection.
U1000 passed all eight affected packages/commands before the source-only move.

The initial all-four-sections RED is permanently preserved and not relabeled.
An intermediate native aggregate also failed in unrelated channel-onboarding
readiness/publication setup before reaching diagnostics. The final direct
proof uses the existing minimal canonical served source, preserving the
multi-run, native persistence, writer survival and all diagnostic assertions;
no retry loop, skip or timeout increase was used, and no pressure-cause or
inherited-flake classification is claimed for that intermediate run.

Pre-edit positive identities: SQLite helper 25 findings/20 raw operations;
PostgreSQL 23/18. Fresh downward ratchet PASS 251.952s: exactly 48 findings and
38 raw sites removed, zero added; no separate consumer credit. Residual debt
is 6694 findings / 4482 raw sites / 67 excluded occurrences. Collector stays
64b8aced371482a1b0ef4e7e9552d39a6190f122b8ae231e8c51a898b6fe7d44;
only the baseline and checksum sidecar decreased. No classifier, accounting,
relocation, fixture-role, exclusion, guard or debt allowance changed.
Final read-only ratchet PASS 144.638s after the source-only delivery placement
repair; retired-reference inventory PASS 5.192s. Signed complexity is reported
with the checkpoint; total extractor findings are not residual debt credit.

Architecture disposition: the display reader's writable construction was a
real narrow ownership smell, now removed using existing inspection/domain
owners. The obsolete-table interpreter is deleted, not promoted as an API.
Tracking remains #2542/#2151 and the existing runtime_operations watchlist
delivery_and_replay_ownership node; this audit refines its diagnostic-only
authority and retired-table manifestation. No new issue/node/framework or
speculative receipt-visibility follow-up is justified. ROI is eliminating
construction/getter/cursor authority on failure-reporting paths once for the
complete consumer chain; no further architecture work is needed for this
bounded family. The parent's wider migration/integrated tail remains open;
this wave does not change the unknown semantic exception/timing estimate.
Effort: one bounded owner/caller cohort, approximately one hour including
schema investigation and focused qualification; additional architecture work
for this family is zero. The retired-table proof avoided spending that effort
on a compatibility owner or nonexistent native receipt fixture.

No push, PR, rebase, preflight, server2, tier, restart qualification or parent
closure is claimed. Existing M01/Docker/D3/M07/drain/fork/history/overlay and
landing holds remain unwaived. Exact historical objects for the tool package
remain a disclosed integration-history obligation, not an unshallow/fallback
permission. Rebase remains once after #2617 and orchestrator handoff; the
required rebased-clean-tree preflight precedes any eventually authorized push.

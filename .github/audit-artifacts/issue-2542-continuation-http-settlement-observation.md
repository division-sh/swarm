# Selected HTTP Settlement Attempt Witnesses

Parent #2542 / #2151. requireHTTPSettlementOutcome and its four calls in the
both-store acknowledgment root consume a fixed physical inventory from the
original selected effectpersistence owner. Every attempt contributes exactly one
operation_id/state witness; duplicate operation identifiers are NOT deduplicated.
There is no authority/status eligibility filter, pagination or recovery policy.
The root derives its original COUNT(*), COUNT(state='settled') and one-operation
witness from these complete physical rows. Failure returns no partial inventory.

The existing native PostgreSQL constructor replaces raw pool/admit setup, with
original source/agent/lifecycle registration, candidate sink and cleanup retained.
The whole HTTP settlement file is now closed by the native fixture guard. No new
coordinator, optional capability, memory fallback, arbitrary query/value/callback
or source mutation is introduced. The native effect owners perform the read,
with their existing selected schema/backend checks and read transaction.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Acknowledged settlement response and post-commit cleanup failure | reproduced and fixed | TestHTTPToolAcknowledgedSettlementCleanupPreservesResponseBothStores under race, both stores; original provider-42 response, exact call/handoff counts, durable outcome, diagnostic and tool/module admission assertions. |
| Same-operation replay cannot redispatch | execution-proven through the same corrected path | Same root's original nil-result/error/provider-count/sink-count and durable-state checks. |
| Unacknowledged refusal cannot become a settled receipt | execution-proven through the same corrected path | Same root's original typed refusal, two attempts/one settled attempt and no false committed diagnostic. |
| Physical attempt cardinality and state fidelity | execution-proven through the same corrected path | TestExternalAttemptStoragePreservesPhysicalMultiplicityAndState uses the actual shared SQL read with duplicate operation IDs and both settled/authorized witnesses; no synthetic runtime authority claim. |
| Original read coordinator and failure boundary | execution-proven through the same corrected path | TestExternalAttemptStorageUsesOriginalReadOwnerAndFailsClosedBothStores under race: one read/no write on empty native storage; raw/invalid/cancelled/closed/unavailable refusal and joined restoration. |
| Complete workload and future SQL escape | reproduced and fixed | Two complete finite recipes, independent whole-body normalization and changed count/identity/state/provider/refusal controls; exact four-call inventory; whole-file raw parameter/callback/unlisted-sibling guard. |

Consumer audit: only this HTTP root and its four unchanged outcome calls consume
the new storage port. Private native/component controls exercise the same lower
effect owner. Other external-effect observations require their own exact joined
predicates and remain parent debt; they are not redirected through this unfiltered
inventory. No other tool file contains raw attempt-table SQL after this migration.
Other tool harness families remain open. Closure is this complete finite family,
not #2542/#2151. Existing tracker/watchlist mapping suffices; no new architecture
issue or gate is required. Spec: selected_runtime_store_projection.raw_sql_policy.

Focused both-store/native/component receipts are under
/home/youmew/.cache/swarm-2542-local-20261007/increment18-*.
The grouped fresh census/registry/78-guard/complete-overlay/complexity/unused sweep
is still required before the next increment push; no aggregate tier/PR is claimed.

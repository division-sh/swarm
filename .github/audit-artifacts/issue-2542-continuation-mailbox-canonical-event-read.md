# Supported Mailbox Canonical Event Read Continuation

## Boundary And Ownership

Binding: approved #2151/#2542 selected raw-authority migration and
platform-spec.yaml selected_runtime_store_projection.raw_sql_policy.
Existing canonical event-record adapters and Record.Decode remain the sole
complete persistence decoder; the existing original selected native read
coordinator supplies the transaction. No new SQL or semantic owner.

The entry point loadMailboxWritePersistedEvent reconstructed a partial event
from15 columns, erased task/depth information, normalized routing locally and
substituted the current clock for missing/unparseable timestamps. Its full
consumer family has two calls in one both-store HTTP/provider root.

The helper now consumes storetest.LoadCanonicalEventRecord using the original
selected persistence and the prior background context. The complete decoder
supplies the exact persisted event, including source and routing identity.
Missing/corrupt storage refuses rather than supplying a plausible partial
event. mailboxWriteDBEnvelope, mailboxWriteDBJSON, mailboxWriteDBTime and
mailboxWriteParseDBTime are deleted, not compatibility delegates.

## Systematic Consumption And Proof

| Manifestation/consumer | Disposition | Exact planned proof |
| --- | --- | --- |
| Decision event exact identity | moved to canonical owner | Original TestMailboxDecideHTTPReleasesProposedEffectThroughProviderOnBothStores under race, both backends. |
| Activity-request event exact type | moved to canonical owner | Same actual HTTP/card/provider/pipeline journey, no workload or assertion change. |
| Complete task, depth, source, creation time and persisted record | already consumes canonical owner | Both-store complete event-record equality root strengthened with a non-root event, nonempty task, nonzero depth and exact parent identity/time. |
| Missing/malformed/raw/cancelled/closed evidence | already consumes canonical owner | Both existing native complete-record/refusal roots under race, original selected read counts and exact zero-value refusal. |
| Wrong owner/event/assertion or unknown sibling caller | moved to canonical owner | Two finite whole-function recipes, independent full-workload equivalence, four hostile mutations and exhaustive exact-call inventory. |
| Retired local decoder or clock fallback | invalid/removal completed | Existing core-consumed retirement AST owner expanded to four deleted declarations, with raw/delegate/method/build-excluded negative controls. |
| Other setup and diagnostic SQL in supported mailbox root | still bypasses and explicitly tracked | Remains exact-site decreasing debt in #2542; not hidden or claimed closed. |

## Closure And Tracking

This completely closes the local partial-reader/decoder family and its finite
callers, not the parent. There is no backend-name inference, raw getter,
callback, SQL selector, new framework or production semantics change. Existing
watchlist mapping remains sufficient. The same operation owner also serves
the previously migrated complete-event consumers; no duplicate reader added.

The increment proof records the monotonic census, registry, full78 guards,
complete codemod/type/temporal controls, inert replay, contracts, exact-head
complexity and native unused exit. Global zero debt, strict completion,
SQLite fork-deadline and final integrated proof remain open.

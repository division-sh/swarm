# Selected Entity Tool Private Bookkeeping Witness

Parent #2542 / #2151. The SQLite tool persistence root no longer obtains SQL to
inject bookkeeping or count mutations. Its fixed hostile payload remains exactly
{"private_fact":"must-not-leak"} in entity_state, at the original run/entity.
The existing projection-fault coordinator owns this named operation; it is NOT
the flow_instances bookkeeping operation. One original selected writer transaction
must affect exactly one existing row. No arbitrary payload, callback, raw handle,
reconstructed coordinator or caller-selected storage family is exposed.

Consumer audit: the tool root is the sole public fixture consumer of this named
fault. Private positive/negative/admission controls consume its exact same port.
The root's mutation count consumes the existing tracked entity projection reader,
whose unfiltered ordered mutation slice has the same cardinality as the original
COUNT(*) over exact run/entity. All original get/save/search/select/whole-query/
metrics and every bookkeeping no-leak assertion remain unchanged.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Hostile SQLite bookkeeping and full tool workload | reproduced and fixed | TestEntityTools_SQLiteBackendNeutralEntityPersistence under race, unchanged no-leak/metrics/count assertions. |
| Exact payload, row scope and other-column preservation | execution-proven through the same corrected path | TestWorkflowProjectionShapeFaultsKeepExactPayloadScopeAndNativeTransactionBothStores/EntityPrivateBookkeeping, both stores under race. |
| Invalid/raw/absent/cancelled/closed refusal | execution-proven through the same corrected path | Existing projection shape scope/refusal roots include the new named fault on both stores under race. |
| No SQLite coordinator bypass | execution-proven through the same corrected path | TestWorkflowProjectionShapeFaultsRespectOriginalSQLiteWriterAdmission/EntityPrivateBookkeeping; channel-held original writer, queued cancelled fault, joined holder, no mutation. |
| Workload/predicate/assertion regressions | reproduced and fixed | Whole-function finite recipe, independent complete AST equality and changed run/row-count/no-leak/history-count/tool-value controls; hostile raw-callback completion guard. |

Closure: the full claimed bookkeeping/no-leak and mutation-count family, not all
entity-tool fixtures. The added fixed write remains below the existing selected
storage boundary and preserves the actual original temporal cut (after lawful
scenario import, before get_entity). Parent debt and integrated final proof remain
open. Existing watchlist and tracker mapping suffice; no new issue/framework.
This routine migration is within the approved named-fault boundary. No broader
production semantic change, compatibility seam or tier run is claimed.

Spec: selected_runtime_store_projection.raw_sql_policy.entity_tool_private_bookkeeping_fixture.
Receipts: /home/youmew/.cache/swarm-2542-local-20261007/increment17-bookkeeping-*.

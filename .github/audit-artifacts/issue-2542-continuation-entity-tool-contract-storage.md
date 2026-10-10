# Selected Entity Tool Contract Observation

Parent #2542 / #2151. The imported-contract root replaces its two dialect strings
and generic database getter with the existing selected tracked-mutation projection
reader. That physical reader now includes entity_type from the same exact
run_id/entity_id row, without current-state filtering. No semantic reconstruction,
fallback owner, caller SQL, callback or new observation facade is introduced.

Consumer audit: the imported-contract root is the only new consumer. The existing
conformance reconstruction consumer remains unchanged and reads its original
fields and ordered mutations. Source/run/import setup, both backend selection
branches, tool arguments and BOTH original contract assertions are unchanged.
The reader continues to return zero evidence on cancelled/closed/absent/late
storage failure and to consume one original selected read transaction.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Imported physical contract and public tool readback | reproduced and fixed | TestEntityTools_ReadImportedCanonicalEntityContractOnBothStores under race, SQLite and PostgreSQL. |
| Exact physical scope, order and null preservation | execution-proven through the same corrected path | TestTrackedMutationProjectionPreservesScopeOrderAndNullBothStores under race, including the added entity_type witness. |
| Raw/invalid/cancelled/closed/late storage refusal | execution-proven through the same corrected path | TestTrackedMutationProjectionRefusesRawInvalidCancelledClosedAndLateFailureBothStores under race. |
| Selector/workload/assertion regressions | reproduced and fixed | Whole-function finite recipe plus independent complete AST equivalence and changed run/entity/source/assertion controls; named raw-parameter completion guard. |

Closure: this physical contract read family, not all entity-tool fixture debt.
Hostile bookkeeping setup and generic getter uses elsewhere remain tracked under
the existing parent, without a compatibility seam. The existing owner is reused;
no new architecture issue, watchlist node or gate decision is necessary. Final
global zero-debt and integrated qualification remain open. No PR or aggregate
tier run is claimed. Grouped census/guards/overlay/complexity/unused receipts are
required before the increment push.

Spec: selected_runtime_store_projection.raw_sql_policy.entity_tool_contract_observation.
Receipts: /home/youmew/.cache/swarm-2542-local-20261007/increment17-contract-storage-*.

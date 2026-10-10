# Catalog Physical Workflow-State Diagnostics

Routine exact-storage observation migration under #2151/#2542. The failure
dump and manual Tier 11 probe use the existing pipeline projection owner and
the original selected read coordinator. The primary entity-state assertion
continues to consume WorkflowPersistence, not this physical diagnostic.
No raw handle, query callback, reconstructed store or new owner is exposed.

The read preserves all physical entity_state rows (no added run filter),
created_at ASC order, entity ID text, empty flow-instance fallback, current
state, scan/iterator error refusal and row closure. Catalog formatting,
absent-reader empty string, empty-store brackets and failure messages remain.
The manual Tier 11 probe retains its existing opt-in posture and workload;
this cohort does not change tier selection or claim an opt-in run occurred.

| Manifestation | Proof |
| --- | --- |
| Shared failure dump and semantic entity assertion | Three finite recipes, exact formatting/refusal oracle and cumulative whole-caller comparison preserve every non-authority statement. |
| Real construction and physical read | TestCatalogWorkflowStateDiagnosticUsesSelectedProjectionBothStores constructs the canonical root tree, verifies waiting root and physical row evidence on both stores, and requires one native read commit/zero writes/no active transaction. |
| Absent and empty diagnostics | Same proof retains empty string for absent reader and [] for an empty native store. |
| Canceled/closed/foreign owner | No evidence returned on refusal. Shutdown first joins execution; store close then establishes the closed-read cut. |
| Physical projection and failure safety | Fixed dialect projection oracle checks ordering/columns, row closure and scan/iterator errors; mutants reject wrong owner/context, ignored error, changed fields or empty formatting. |
| Existing whole-outcome controls | Non-success preview controls remain passing under race; primary workflow assertions are not replaced with diagnostics. |

The first proof incorrectly assumed runtime shutdown closes the selected
store. It failed on both backends, was repaired to close after joined shutdown,
and is retained in receipts; no production shutdown change or timeout/retry.
Qualification includes complete 272-recipe/type/hostile controls, 78 guards,
census/registry with all native-family children, contracts, native unused,
inert replay and exact committed-head complexity before push. Receipts:
/home/youmew/.cache/swarm-2542-local-20261008/catalog-workflow-state-*.

Includes reviewer F1's prose-only correction: replay still samples the initial
clock before replacing it with the transcript boundary. No runtime change.
Existing watchlist mapping suffices; parent raw opening/runtime construction,
zero debt, strict guards, fork deadline and integrated qualification remain.

Measured debt: 13,963 findings / 10,283 raw sites becomes 13,950 / 10,273:
13 findings / 10 sites removed, zero added. Nine exact private row/read/
coordinator occurrences are source-classified fixture-2151 under unchanged
policy. Final selected sweeps have 263 passing roots and no failures/skips.

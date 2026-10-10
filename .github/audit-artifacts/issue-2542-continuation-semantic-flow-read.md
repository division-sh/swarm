# Catalog Causal-Flow Semantic Read Boundary

Routine authority cleanup under #2151/#2542. The causal-flow matcher already
consumes WorkflowPersistence.ListWorkflowInstances exclusively. Its unused
SQL argument and all four caller arguments are removed, not replaced with
another adapter or selected owner. Source-flow candidates, exact path/name
matching, parent/entity/instance causal candidates, optional fallback, run
context, row order, not-found behavior and read-error propagation are unchanged.

Three finite recipes plus cumulative causal-caller oracles prove the complete
function bodies differ only by removal of that unused argument. The caller
set includes causal entity/state assertions, handler-order creation settlement
and replay outcome readback. No lifecycle probe or assertion was changed.

| Manifestation | Proof |
| --- | --- |
| Exact source/causal matching and nil/error behavior | TestCatalogCausalFlowMatchingUsesOnlySemanticReadOwner tests parent/child/instance/path matches, sibling and wrong-causal exclusion, empty flow, preserved optional fallback, fixed run and exact refusal through the semantic port. |
| Real creation under both handler orders | TestCatalogConfiguredCreationCompletesInEitherHandlerOrder and TestCatalogDuplicateCreationCompletesInEitherHandlerOrder pass under race on both stores, retaining the actual pre-target-preparation handshake and exact post-state assertions. |
| Parent-before-child observation order | Existing TestCatalogCausalOrderPreservesParentsAndIndependentOrder passes unchanged. |
| Replay outcome and causal caller bodies | Whole-body inverse oracle preserves delivery multiplicities, conflict identity, exact worker state, source/causal scope and all error checks; hostile mutations reject lost parent/run/fallback/state/refusal. |

Finite inventory: 275 recipes. Qualification includes the complete finite,
candidate/type/hostile sweep, 78 guards, decreasing census/registry with all
16 native-family children, contracts, native unused, inert replay and exact
committed-head complexity before push. Vemew receipts:
/home/youmew/.cache/swarm-2542-local-20261008/catalog-semantic-flow-*.
No SQL, writer, constructor, production semantics, spec, collector, guard
permission, framework or compatibility path added. Existing watchlist
mapping suffices; parent catalog construction and remaining raw reads, zero
debt, strict guards, fork deadline and integrated proof stay open.

Measured debt: 13,950 findings / 10,273 raw sites becomes 13,945 / 10,269:
five findings / four sites removed, zero added. No private SQL classification
added. Final selected sweeps have 265 passing roots and no failures/skips.

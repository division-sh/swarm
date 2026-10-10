# API Whole Delivery And Exact Receipt Evidence

## Binding Boundary

Approved #2151/#2542 fixture authority migration;
platform-spec.yaml selected_runtime_store_projection.raw_sql_policy and
storage.event_receipts. These are physical observations, not executable
delivery selectors or settlement. No production behavior or spec change.

Two local helpers are entry points. The full family is three calls in three
original API roots. countAllEventDeliveries now consumes the original selected
read coordinator and EXISTING delivery.FixtureDeliveryCardinalityTx.
loadPipelineReceiptOutcomeAndFailure now consumes the SAME read coordinator
and the existing pipelinepersistence owner, preserving the exact event and
platform/pipeline predicate plus runtimefailures.UnmarshalEnvelope.

The public ports expose only detached count/outcome/typed failure. They expose
no handle, callback, SQL selector, dialect or reconstructed coordinator.
Whole delivery count includes agents, nodes, all events and all statuses.
Exact receipt outcome is not normalized or interpreted as delivery settlement.
Missing, malformed, cancelled, closed or unsupported reads yield no partial
outcome or plausible zero-success witness.

## Exhaustive Consumption And Proof

| Surface | Disposition | Exact proof |
| --- | --- | --- |
| Precommit publication refusal, one whole-store count | moved to canonical owner | Original race root preserves declared error, zero persistence, zero delivery and connection usability assertions. |
| Missing durable acknowledgment publisher, one whole-store count | moved to canonical owner | Original race root preserves typed unavailable refusal, no publisher call and all zero persistence assertions. |
| Committed publication with completion failure, one exact receipt read | moved to canonical owner | Original race root preserves actual dispatch completion, success/nil-failure acknowledgment, stored replay and no duplicates. |
| Existing whole-delivery physical owner | already consumes canonical owner | Both-store native proof includes two agent originals, one node and one replay obligation; count4 across all events/types and one original read. |
| Fixed pipeline receipt outcome and failure | moved to existing owner | Both-store native proof: waiting/SQL-NULL, unrelated subscriber exclusion, exact typed envelope, absent receipt sql.ErrNoRows and malformed-envelope late refusal with no partial outcome. |
| Raw/cancelled/closed and unrelated-table dependency | moved to canonical owner | Native both-store refusal proof extends to both ports; whole delivery count remains independent when the events table is unavailable. |
| Wrong context/owner/key/assertion or omitted sibling | moved to canonical owner | Five complete source transforms: two new recipes and three composed prior snapshots; independent AST shape/workload evidence, seven hostile substitutions and exhaustive exact-call inventory. |
| Remaining source/fixture/fault SQL in these roots | still bypasses and explicitly tracked | Exact-site decreasing #2542 ledger remains authoritative; not claimed closed. |

## Closure

Old helper raw parameters/queries/decoder are invalid. There is no compatibility
delegate or new ownership framework. This closes the two helper families and
their finite callers, not the parent. Existing watchlist/architecture tracking
suffices; final zero debt, strict completion guards, SQLite fork-deadline and
integrated qualification remain open.

The increment proof comment records focused both-store execution, exact
downward census/registry dispositions, all78 guards, complete codemod and real
overlay/temporal negative controls, inert replay, contracts, exact-head
complexity and definitive native unused exit before pushing.

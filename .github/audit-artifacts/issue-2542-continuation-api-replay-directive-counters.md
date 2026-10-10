# API replay and directive counter continuation

## Boundary And Owners

Binding: the approved #2151/#2542 selected-store raw-authority migration;
platform-spec.yaml selected_runtime_store_projection.raw_sql_policy and
storage.event_receipts. This is fixture readback, not a runtime-semantics change.

The remaining local countOperatorReplayEvents and countDirectiveEvents helpers
were entry points, not the boundary. Their complete family has five calls in
three original roots, now served by the already-qualified fixed physical
CountPhysicalEvents and CountEventNameStorage ports. The original selected
native read coordinator is the owner. No new SQL or facade owner is introduced.

Replay preserves its caller's exact context and ALL physical event rows,
including the corruption/refusal cut before target-route restoration.
Directive counting preserves the exact platform.agent_directive predicate
and the prior implicit background context. Neither count adds status, scope,
expiry, actor, decoding or eligibility filters.

## Exhaustive Consumption And Proof

| Consumer | Disposition | Proof |
| --- | --- | --- |
| TestOperatorReplayMockOnlyRejectsLiveOriginalBeforeMutation, two calls | moved to existing owner | Original race root; full source, requests, rejection and before/after no-mutation checks remain. |
| TestOperatorEventReplayDispatchesCompleteCanonicalSnapshotParity, one call | moved to existing owner | Original both-store race root, including all route shapes and corrupt/repaired canonical snapshot work. |
| TestOperatorAgentSendDirectivePersistsDirectiveEventOnceOnReplay, two calls | moved to existing owner | Original race root, manager/provider/idempotency/conflict and exact one-call assertions preserved. |
| CountPhysicalEvents and CountEventNameStorage | already consumes canonical owner | Existing native both-store physical scope/refusal roots, including historical rows, exact names, cancellation, raw/closed owners and unavailable storage. |
| Other SQL, faults and fixture construction in these roots | still bypasses canonical owner and is explicitly tracked | Remain in #2542's decreasing exact-site ledger; not hidden by this helper closure. |

Five complete transformations are represented by four new finite recipes and
one composed existing recipe, not duplicate snapshots. Independent AST
workload/context equivalence, exact owner/predicate helper shapes, six hostile
substitutions and exhaustive exact-call inventory protect the migration.
The native API census family rejects raw types/operations in both helpers;
named adversarial controls prove the guard.

## Closure And Tracking

Chosen closure is the two physical counter helpers and their full finite
consumer family, not parent-class elimination. Original raw helper authority
is invalid; no compatibility delegate, generic getter, callback, dialect
selector or reconstructed coordinator is retained. Other direct reads and
hostile writes remain honestly open under the same parent. Existing watchlist
mapping suffices; no new architecture, tracker or semantic gate is needed.

The exact downward census, registry, all78 structural guards, complete codemod
overlay/type controls, inert replay, contracts, complexity and native unused
results are recorded in the increment proof comment before pushing. Global
zero debt, strict completion, SQLite fork-deadline and final integrated proof
remain open.

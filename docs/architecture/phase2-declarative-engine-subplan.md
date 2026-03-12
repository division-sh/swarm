# Phase 2 Subplan: Declarative Engine

This phase is now anchored on the updated handoff from `2026-03-11`.

The center of gravity is the 12-step handler engine. Cleanup only counts if it
directly supports that engine and its runtime invariants.

## Target End State

- A contract-driven 12-step handler engine executes handlers in spec order.
- `DeclarativeNode` is the default node executor.
- Handler side effects commit in one transaction.
- Emitted events are persisted in the transaction and delivered after commit.
- Policy is resolved from the `FlowTree`, not from flat merged maps.
- Routing is derived from contracts and active instances, not mutated.
- Generic runtime no longer depends on `productpolicy.Policy`, mutable routing,
  `accumulator_state`, or `current_stage`.

## Execution Order

### Slice A: Foundation Preconditions

1. Delete generic `productpolicy.Policy` dependencies from handler/CEL policy resolution paths.
2. Delete legacy `accumulator_state` usage from the handler execution path.
3. Audit remaining hardcoded guard/action switch paths.
4. Add `BuildCELContext(entity, payload, policy, executionState)` as a shared pipeline primitive.
5. Wire handler engine and transition-evaluator guard paths to use `BuildCELContext`.
6. Keep platform builtins as registered handlers, not central switches.

### Slice B: 12-Step Handler Engine Completion

1. Audit remaining hardcoded guard/action switch paths.
2. Implement the 12-step order exactly.
3. Enforce `on_complete` vs `rules` mutual exclusion.
4. Add `clear_gates` as step 1.
5. Ensure `payload_transform` and `action` run in the final positions.
6. Hold the implementation to these atomicity invariants:
   - acquire the entity lock before step 1
   - evaluate guards against pre-handler state
   - persist all handler writes in one transaction
   - call `tx.Commit()` once after step 12, not per step
   - persist emitted events in the transaction
   - flush event delivery only after commit

### Slice C: Transaction And Delivery Boundary

1. Implement the Slice B atomicity invariants in the runtime path.
2. Execute handler steps inside a single transaction.
3. Persist emitted events in-transaction.
4. Deliver emitted events only after commit.
5. Verify same-entity serial / cross-entity concurrent execution.

### Slice D: DeclarativeNode Default Path

1. Make system nodes execute through `DeclarativeNode`.
2. Keep a product hook registry only for explicit custom actions.
3. Delete remaining node-ID-specific runtime branching.

### Slice E: Contract-Derived Routing

1. Build boot-time route tables from contracts.
2. Resolve local, absolute, and wildcard subscriptions against the `FlowTree`.
3. Extend the route table on flow-instance creation.
4. Delete mutable routing table APIs.

### Slice F: Remaining MAS-Invalid Concept Deletion

1. Remove `current_stage`.
2. Remove generic Empire orchestrators and scan campaign manager paths.
3. Shrink `WorkflowModule` to the target interface.
4. Remove remaining side-state recovery bridges.

## Guardrails

- Do not start with vocabulary cleanup.
- Do not build more handler steps on top of `productpolicy.Policy` or `accumulator_state`.
- Do not remove mutable routing before contract-derived routing is live.
- Do not treat passing builds as meaningful if handler execution still bypasses
  the 12-step engine.

## Immediate Next Step

Start Slice A:
- finish `BuildCELContext`
- remove `productpolicy.Policy` from handler/CEL policy reads
- remove handler-path `accumulator_state` dependence
- only then continue the 12-step executor

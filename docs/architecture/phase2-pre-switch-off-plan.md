# Phase 2 Pre Switch-Off Plan

## Objective

Complete the live runtime bridge to `internal/runtime/engine` before the atomic
cutover. The goal is to make the new engine runnable through the real pipeline
dependencies with no hidden dependence on the old state-transition loop.

## Remaining Work

1. Complete the pipeline-to-engine adapter surface.
- finish the live adapter in `internal/runtime/pipeline/engine_adapter.go`
- cover:
  - real CEL evaluator
  - SQL transaction runner
  - entity locking
  - workflow-instance state load/save
  - guard/action registries
  - builtin guard/action runners
  - payload shaping
  - dispatcher/outbox bridge

2. Close the remaining side-effect gaps in the adapter.
- timer lifecycle:
  - apply `TimerIntent` through the existing timer runtime/store path
- data accumulation:
  - preserve the legacy workflow-instance metadata/entity projection behavior
  - preserve entity resolution parity with the legacy path
- stage/gate projection:
  - preserve the old validation/workflow side projections

3. Preserve event delivery semantics.
- parent collector behavior must still work
- post-commit delivery only
- no double publish
- preserve chain depth when intents are collected before final flush

4. Add cutover-focused regression coverage.
- parent collector flush behavior
- payload-transform emit parity
- rule override emit parity
- builtin action parity
- timer intent application
- gate/state persistence parity
- escalation emit parity
- data accumulation projection parity

5. Switch the authoritative declarative handler path.
- replace the old handler execution call in
  `internal/runtime/pipeline/declarative_default_node.go`
- make declarative system-node handler execution go through the new engine path

6. Keep the switch atomic.
- no mixed runtime behavior
- no per-entity fallback split
- the old handler path stays inactive once the new one is live

7. Run the mandatory checkpoint after the switch.
- verify:
  - ordered execution
  - transactionality
  - post-commit fan-out
  - timer application
  - action/guard registry delegation
  - collector behavior

8. Delete the old handler path immediately after the switch is proven.
- remove or retire:
  - `internal/runtime/pipeline/handler_engine_exec.go`
  - old handler-only tests
  - dead transition helpers in
    `internal/runtime/pipeline/workflow_transition_engine.go`

9. Final post-switch audit.
- verify no declarative handler path still depends on the old engine
- verify no interception compatibility seam is required
- verify no old fallback branches remain

## Execution Order

1. finish adapters
2. add cutover tests
3. atomic caller swap
4. checkpoint
5. delete old path
6. final audit

## Ready for Switch-Off

- adapter covers timer, accumulation, gate, stage, payload, outbox, collector,
  and builtin seams
- declarative node can execute through the clean-room engine using live runtime
  dependencies
- tests prove parity on the live path
- no remaining engine-semantic blockers

## Switch-Off Complete

- live declarative handler execution goes through `internal/runtime/engine`
- old pipeline handler engine is dead
- full suite green

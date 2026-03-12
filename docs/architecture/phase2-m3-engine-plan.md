# Phase 2 M3 Engine Plan

## Goal

Create a true generic execution core in `internal/runtime/engine` and cut `pipeline`
over to it with a single atomic switch.

This is the Phase 2 engine implementation path. It replaces the old transition/handler
logic rather than coexisting with it.

## Non-Negotiable Constraints

1. No dual-engine runtime
- No mixed fallback where some entities run old logic and others run new logic.
- `DeclarativeNode` is the cutover seam.
- The switch happens once for the environment.
- If the new engine is not ready, it does not become the active path.

2. Incremental CEL context
- Do not rebuild a full CEL context for every step.
- Resolve entity and hierarchical policy once per execution.
- Mutate step-local overlays for payload, accumulated values, and fan-out item state.
- Cache policy resolution outside the hot loop.

3. Chain depth enforcement
- Every execution request and emitted intent carries `ChainDepth`.
- If depth exceeds the hard limit, execution stops and dead-letters.
- This is mandatory for recursive event chains and fee containment.

4. Implicit timer lifecycle
- Timer cancellation and creation are built into the executor.
- `advances_to` must cancel old-state timers and start new-state timers automatically.
- Timer lifecycle is not delegated to optional hooks.

5. Outbox semantics
- Emitted events are persisted in the same transaction as state changes.
- Delivery occurs only after commit.
- Transaction/effect handling distinguishes transient retryable failures from logic failures.

## Package Shape

### `internal/runtime/engine/types.go`
- `ExecutionRequest`
- `ExecutionResult`
- `ExecutionState`
- `ExecutionContext`
- `EmitIntent`
- `TimerIntent`
- `RuleMatch`
- `FailureClass`

Required fields:
- `EntityID`
- `Event`
- `NodeID`
- `Handler`
- `ChainDepth`

### `internal/runtime/engine/interfaces.go`
- `SemanticSourceProvider`
- `StateRepository`
- `InstanceRepository`
- `TransactionRunner`
- `EntityLocker`
- `OutboxWriter`
- `PostCommitDispatcher`
- `GuardRegistry`
- `ActionRegistry`

The engine depends on `semanticview.Source`, not `WorkflowContractBundle`.

### `internal/runtime/engine/context_builder.go`
- `BuildBaseContext(...)`
- `WithPayload(...)`
- `WithAccumulated(...)`
- `WithFanOutItem(...)`

Requirements:
- base context built once per execution
- incremental updates for step-local variables
- hierarchical policy values provided from cached resolved policy

### `internal/runtime/engine/evaluator.go`
- CEL evaluation helpers for:
  - booleans
  - values
  - object/list expressions if needed

### `internal/runtime/engine/executor.go`
- main ordered handler loop

Execution order:
1. `clear_gates`
2. `guard`
3. `accumulate`
4. `compute`
5. `fan_out`
6. `on_complete`
7. `rules`
8. `advances_to`
9. `sets_gate`
10. `data_accumulation`
11. `payload_transform`
12. `emits`
13. `action`

Mandatory executor behavior:
- guard reads pre-write state
- `on_complete` and `rules` are mutually exclusive
- first-match semantics for rule lists
- chain-depth check in `emits`
- implicit timer lifecycle when state changes

### `internal/runtime/engine/transaction.go`
- one transaction per handler execution
- entity lock acquired before execution starts
- state writes + gate writes + accumulation + outbox writes in one transaction
- post-commit dispatch only
- retry/dead-letter classification

### `internal/runtime/engine/declarative_node.go`
- generic node executor using:
  - semantic source
  - handler executor
  - registries
  - transaction/effect infrastructure

This is the default runtime node path.

## Execution Strategy

### Step 1: Clean Room
- create `internal/runtime/engine`
- implement the types and interfaces
- do not cut over runtime traffic yet

### Step 2: Brain
- implement the incremental CEL context builder
- implement the evaluator against that context model
- ensure hierarchical policy resolution is cached outside the step loop

### Step 3: Default Worker
- implement `DeclarativeNode`
- make it the only generic executor shape in the new package

### Step 4: Switch-Off
- cut `pipeline` to call the new engine path
- switch once through the declarative node seam
- remove old handler/transition branches from `workflow_transition_engine.go`

## Explicit Anti-Patterns

- No runtime fallback between old and new engines
- No repeated full-context rebuild inside the loop
- No optional timer management hook
- No direct productpolicy or product-specific logic in engine
- No second shadow executor left alive after cutover

## First Build Slice

1. Audit `internal/runtime/pipeline/handler_engine_exec.go`
- classify logic into:
  - harvest as-is
  - adapt
  - rewrite
  - delete

2. Create:
- `engine/types.go`
- `engine/interfaces.go`
- `engine/context_builder.go`
- `engine/executor.go`

3. Implement first:
- execution request/result types
- incremental context builder
- step ordering skeleton
- chain-depth enforcement

4. Implement second:
- transaction runner + outbox semantics
- implicit timer lifecycle

5. Implement third:
- declarative node wrapper
- pipeline adapter layer

## Acceptance Criteria

- `pipeline` no longer owns generic handler semantics
- `engine` owns the ordered handler loop
- `BuildBaseContext` and incremental updates are the only CEL context path
- emitted events use outbox semantics
- timer lifecycle is implicit inside execution
- chain depth is enforced
- cutover happens as one atomic runtime switch
- old transition/handler code is deleted after switch

## Verification

- `go test ./internal/runtime/engine/... -count=1`
- `go test ./internal/runtime/pipeline -count=1`
- `go test ./... -count=1`
- `go build ./...`

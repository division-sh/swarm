# Phase 2 Single-Implementer Checklist

This checklist adapts `docs/architecture/implementer-handoff.md` Phase 2 into a strict single-implementer order.

## Rules

- Complete one block at a time.
- After each block: compile, run targeted tests, then continue.
- Do not start the vocabulary sweep before the semantic/runtime seams are stable.
- Do not recreate deleted legacy contract files.

## Block 0: Baseline

- [x] Audit the current repo against Steps 2.1-2.9.
- [x] Confirm major live blockers:
  - `Event.VerticalID` still exists
  - mutable routing tables still exist
  - generic `productpolicy.Policy` still exists
  - `directHandlerExecutionPlanSupported` still exists
  - `current_stage` and `accumulator_state` still exist in generic runtime paths

## Block 1: Step 2.1 Typed Semantic Model

- [ ] Finish remaining runtime-critical typed contract work in `internal/runtime/contracts/workflow_contracts.go`
- [ ] Remove dead dynamic compatibility walkers/helpers that are no longer needed
- [ ] Keep only backward-compatible YAML shorthand decoders that are still required
- [ ] Gate:
  - [ ] `go build ./...`
  - [ ] `go test ./internal/runtime/contracts ./internal/runtime/pipeline -count=1`

## Block 2: Steps 2.2-2.3 Source-of-Truth + Guard/Action Cleanup

- [ ] Remove remaining hardcoded generic contract-path assumptions
- [ ] Ensure schema/event generation reads MAS package roots only
- [ ] Delete hardcoded guard/action switch behavior from generic runtime
- [ ] Keep only platform builtins + hook registration
- [ ] Gate:
  - [ ] `go test ./internal/runtime/... ./internal/commgraph/... -count=1`

## Block 3: Step 2.5 Handler Backbone

- [ ] Make the 10-step handler engine the real execution path
- [ ] Remove `directHandlerExecutionPlanSupported` as a functional gate
- [ ] Make `DeclarativeNode` the default contract-node executor
- [ ] Eliminate split behavior between simple and complex handlers
- [ ] Gate:
  - [ ] `go test ./internal/runtime/pipeline -count=1`
  - [ ] `go test ./internal/runtime/testcases/... -count=1`

## Block 4: Step 2.4 Delete `VerticalID`

- [ ] Remove `VerticalID` from `internal/events/types.go`
- [ ] Repair callsites in one pass
- [ ] Replace envelope scoping with payload/entity/instance routing semantics
- [ ] Gate:
  - [ ] `go build ./...`
  - [ ] `go test ./internal/runtime/... ./internal/commgraph/... ./internal/dashboard/... -count=1`

## Block 5: Step 2.6 MAS-Invalid Concept Deletions

- [ ] Delete mutable routing table APIs and generic consumers
- [ ] Remove generic `productpolicy.Policy`
- [ ] Remove generic dependence on `current_stage` and unstructured `accumulator_state`
- [ ] Collapse legacy orchestrator structs that should be declarative
- [ ] Gate:
  - [ ] `go build ./...`
  - [ ] `go test ./... -short -count=1`

## Block 6: Steps 2.7-2.8 Product Extraction + Generic Boot

- [ ] Keep generic config limited to runtime/database/LLM
- [ ] Push remaining Empire runtime/config/store/tools behind product-owned packages
- [ ] Delete raw SQL execution from generic tools
- [ ] Ensure `cmd/mas` boots core runtime without Empire product wiring
- [ ] Gate:
  - [ ] `go build ./cmd/mas ./cmd/empire ./internal/runtime/...`
  - [ ] `go test ./cmd/mas ./cmd/empire ./internal/runtime/... -count=1`

## Block 7: Step 2.9 Vocabulary + Import Sweep

- [ ] Remove remaining Empire vocabulary from generic runtime packages
- [ ] Remove generic imports of `pipeline/empire`, `productpolicy/empire`, `commgraph/empire`
- [ ] Final gate:
  - [ ] `go test ./... -count=1`
  - [ ] `grep -r "vertical\\|opco\\|empire\\|factory\\|holding" internal/runtime/ --include='*.go'`

## Phase 2 Done

- [ ] Full suite green
- [ ] No `Event.VerticalID`
- [ ] No mutable routing tables in generic runtime
- [ ] No generic `productpolicy.Policy`
- [ ] No handler bail-out for typed `on_complete` or `rules`
- [ ] All contract-driven system nodes run through `DeclarativeNode`
- [ ] `cmd/mas` boots without Empire product wiring

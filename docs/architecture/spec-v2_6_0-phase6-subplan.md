# Phase 6 Subplan: Make Flow Schema Semantics Authoritative

## Goal

Make `schema.yaml` a first-class runtime semantic source, not just parsed metadata.

Phase 6 should push the runtime toward treating flow schemas as authoritative for:

- states
- initial state
- terminal states
- pins
- required agents
- namespace boundaries

without yet rewriting the execution engine.

## Steps

### 1. Rebase state-definition validation to flow schemas

Use flow-derived states/terminal states in validation and conformance checks wherever safe.

Target files:
- `internal/runtime/pipeline/workflow_contract_validation.go`
- `internal/runtime/pipeline/workflow.go`
- related tests

Done when:
- state validation can succeed from flow schemas even if the flat workflow doc is incomplete
- tests prove flow-derived stages/terminal stages are accepted as canonical fallback

### 2. Add flow pin semantic indexes to the bundle

Expose semantic accessors for:

- input events per flow
- output events per flow
- read pins
- write pins

Target file:
- `internal/runtime/contracts/workflow_contracts.go`

Done when:
- runtime/compliance can query flow pin semantics through bundle APIs instead of raw schema maps

### 3. Validate write-pin conflicts across the merged package tree

Use the flow schema semantic view to detect conflicting output/write ownership across flows.

Target files:
- `internal/runtime/contracts/workflow_contracts.go`
- `internal/runtime/contract_compliance_test.go`
- `internal/runtime/pipeline/workflow_contract_validation.go`

Done when:
- conflicting write pins fail compliance/load
- identical/shared definitions remain allowed only where intentional

### 4. Validate required-agent fulfillment from merged package/flow views

Use `required_agents` from flow schemas and verify they are satisfied by merged `agents.yaml`
content.

Target files:
- `internal/runtime/contracts/workflow_contracts.go`
- `internal/runtime/contract_compliance_test.go`

Done when:
- missing required agents fail validation
- role/subscription/emit fulfillment is checked against the merged package tree

### 5. Consume flow schema semantics in one more runtime/compliance path

Choose a low-risk consumer after validation:

- workflow conformance checks
- runtime node visibility checks
- pin ownership checks

Done when:
- at least one more non-loader, non-test path uses flow semantic accessors directly

## Non-Goals

- no handler execution rewrite yet
- no transition-engine replacement yet
- no removal of bridge files yet

Those belong to later phases.

## Acceptance Gate

```bash
go test ./internal/runtime/contracts ./internal/runtime/pipeline ./internal/runtime -count=1
go test ./... -count=1
```

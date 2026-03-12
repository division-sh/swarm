# Type Hardening Phase Plan

## Goal

Move the runtime from stringly/dynamic execution toward a typed kernel, in the right order:

1. `URI Tier`
2. `Enum Tier`
3. `Path Tier`
4. `Value Tier`
5. `Registry Tier`

The sequencing matters. We do not harden value containers or registries on top of raw strings and ad hoc path parsing.

## Package Targets

### `internal/runtime/identity`
- `EntityID`
- `NodeID`
- `FlowID`
- `ActionKey`
- `GuardKey`
- `SchemaRegistryID`
- `WorkflowURI`

### `internal/runtime/paths`
- `PathRoot`
- `Path`
- parse/normalize helpers

### `internal/runtime/values`
- structured execution context buckets
- typed storage bucket wrapper
- typed transform primitives

### `internal/runtime/registry`
- typed guard/action instruction model
- built-in vs CEL separation

## Execution Order

### Phase 1: URI Tier
- Introduce typed identity primitives.
- Add `WorkflowURI` parse/resolve primitives.
- Wire engine request/state boundary onto typed IDs first.
- Keep storage and YAML shapes unchanged during this slice.

### Phase 2: Enum Tier
- Replace raw runtime string switches with enums:
  - `GuardFailureAction`
  - `AccumulateMode`
  - `ComputeOp`
  - `TimerOp`
  - `FailureTier`
- Loader owns string-to-enum parsing.

### Phase 3: Path Tier
- Parse data paths once at load time.
- Engine consumes typed `Path`, not dot-split strings.
- Remove repeated `strings.Split(path, ".")` logic from runtime code.

### Phase 4: Value Tier
- Introduce typed execution/value containers:
  - structured context buckets
  - typed storage bucket access
  - typed transform mappings
- JSONB remains the persistence format, but runtime access becomes typed.

### Phase 5: Registry Tier
- Replace generic contract entry bags with typed platform instructions.
- Separate built-in instructions from CEL-backed instructions.
- Keep this after path/value hardening so registry signatures harden around the right model.

## Immediate Slice

This execution slice covers:

1. add `internal/runtime/identity`
2. add tests for typed IDs and `WorkflowURI`
3. adopt typed `EntityID` / `NodeID` / `FlowID` at the engine request/state boundary
4. keep the rest of runtime stable while the new primitives settle

## Acceptance Criteria For This Slice

- `internal/runtime/identity` exists with tests.
- `internal/runtime/engine` request/state APIs use typed IDs.
- pipeline adapters compile against the typed engine boundary.
- `go test ./internal/runtime/identity ./internal/runtime/engine -count=1`
- `go build ./...`

## Guardrails

- No broad churn across the whole repo in the first slice.
- No DB schema changes during URI hardening.
- No registry redesign before path/value hardening.
- Prefer hardening at package boundaries first, then move inward.

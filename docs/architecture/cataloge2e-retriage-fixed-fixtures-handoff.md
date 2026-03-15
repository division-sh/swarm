# Catalog E2E — Re-triage Fixed Fixtures

## Context

The spec writer fixed 14 fixture-issue YAML files. These were previously excluded from the E2E catalog because they failed to load or failed contract validation. They should now load cleanly. Your job is to re-triage each one: if it passes end-to-end, promote it to the supported list. If it fails at runtime, reclassify it as `validation-gap` with the actual failure reason.

Additionally, `test-group-by-standalone` was reclassified from fixture-issue to runtime gap — the fixture is spec-correct but the runtime doesn't recognize `group_by` as a handler field. Update its exclusion kind to `validation-gap` with reason: "runtime handler field allowlist does not include group_by".

## Fixtures to re-triage

### Tier 1 — Missing produces (4 fixtures)

These now have the emitted event in their `produces` list.

- `test-guard-discard`
- `test-guard-kill`
- `test-guard-multi-fail`
- `test-guard-reject`

### Tier 1 — Missing entity_schema (3 fixtures)

These now have `entity_schema` declared in package.yaml.

- `test-data-accumulation-direct`
- `test-data-accumulation-literal`
- `test-data-accumulation-mapped`

### Tier 1 — payload.type renamed to payload.kind (4 fixtures)

The `type` field was a YAML reserved word. Renamed to `kind` across events.yaml, nodes.yaml, and expected.yaml.

- `test-rules-else`
- `test-rules-match`
- `test-rules-no-match`
- `test-rules-data-accumulation`

### Tier 1 — Handler field fixes (1 fixture)

- `test-sets-gate` — `sets_gates` (typo) → `sets_gate`

### Tier 3 — Fan-out emit mapping (1 fixture)

- `test-fan-out-emit-mapping` — `item.type` → `item.kind` in key_field and payload

### Tier 4 — Create entity (1 fixture)

- `test-create-entity` — `action_params` replaced with spec-correct sibling fields

## Procedure

For each fixture above:

1. Move it from the excluded map to the supported list in the tier's `_e2e_test.go` file
2. Run the test: `go test ./internal/runtime/cataloge2e/... -run "TestTierN.*fixture-name" -count=1 -v`
3. If it **passes**: keep it in the supported list. Done.
4. If it **fails**: move it back to the excluded map, but change the classification:
   - `kind: "validation-gap"` (not `"fixture-issue"`)
   - `reason:` the actual failure message (e.g. "entity stays in pending instead of advancing to done")

## Reclassification (no re-triage needed)

Update this exclusion in `tier3_list_processing_e2e_test.go`:

```go
"test-group-by-standalone": {kind: "validation-gap", reason: "runtime handler field allowlist does not include group_by"},
```

## Verification

```bash
go test ./internal/runtime/cataloge2e/... -count=1
go test ./... -count=1
```

Both must be green when done.

# Catalog E2E — Fixture Fixes (Spec Writer)

## Context

The E2E catalog has 118 fixtures across Tiers 1–8. 47 are supported and green. Of the 71 excluded, **15 are fixture-issues** — the YAML is structurally wrong and the runtime correctly rejects it. These need spec writer fixes.

The remaining 56 exclusions are validation-gaps (runtime behavior doesn't match expected) — those are a separate implementer track.

## Fixture-issue inventory

Fixtures fail at two validation stages:
- **Bundle load** (YAML parsing / platform spec field validation)
- **NewRuntime contract validation** (deeper semantic checks like produces completeness, payload schema references, entity_schema field declarations)

### Category 1: Missing `produces` declaration (4 fixtures)

The node declares `produces: []` but the handler has `emits: check.passed`. The runtime's contract validator rejects this: "node ./test-node missing required field produces."

**Fix**: Add the emitted event to the `produces` list.

| Fixture | File | Current | Fix |
|---------|------|---------|-----|
| `tier1-primitives/test-guard-discard` | nodes.yaml:5 | `produces: []` | `produces: [check.passed]` |
| `tier1-primitives/test-guard-kill` | nodes.yaml:5 | `produces: []` | `produces: [check.passed]` |
| `tier1-primitives/test-guard-multi-fail` | nodes.yaml:5 | `produces: []` | `produces: [check.passed]` |
| `tier1-primitives/test-guard-reject` | nodes.yaml:5 | `produces: []` | `produces: [check.passed]` |

### Category 2: `data_accumulation` fields missing from workflow `entity_schema` (3 fixtures)

The node-level `state_schema` declares the fields, but the runtime validates against the workflow-level `entity_schema` in schema.yaml. Error: "data_accumulation field name missing from workflow entity_schema."

**Fix**: Add the referenced fields to `schema.yaml` under an `entity_schema` section (or however the workflow declares entity-level fields).

| Fixture | Missing fields | Where to add |
|---------|---------------|--------------|
| `tier1-primitives/test-data-accumulation-direct` | `name` (string), `score` (integer) | schema.yaml |
| `tier1-primitives/test-data-accumulation-literal` | `category` (string) | schema.yaml |
| `tier1-primitives/test-data-accumulation-mapped` | `name` (string) | schema.yaml |

Check the existing working fixtures (e.g. `test-accumulate-idempotent`) for the correct `entity_schema` format.

### Category 3: Handler references `payload.type` outside declared event payload schema (4 fixtures)

The event payload in events.yaml declares `type: string`, and the handler condition uses `payload.type`. The runtime's contract validator rejects this: "references payload.type outside event payload schema."

This is likely a validator bug or a schema format issue — `type` might be a reserved YAML keyword that gets interpreted as a type specifier rather than a field name. Check if quoting the field name (`"type": string`) or renaming it (`item_type: string`) resolves the validation.

| Fixture | Handler condition | Event field |
|---------|------------------|-------------|
| `tier1-primitives/test-rules-else` | `payload.type == a` | `type: string` |
| `tier1-primitives/test-rules-match` | `payload.type == ...` | `type: string` |
| `tier1-primitives/test-rules-no-match` | `payload.type == ...` | `type: string` |
| `tier1-primitives/test-rules-data-accumulation` | `payload.type == ...` | `type: string` |

### Category 4: Unsupported handler fields (4 fixtures)

These use handler fields that the platform spec doesn't define. The loader rejects them with "UNDEFINED-FIELD: handler field X not in platform spec."

| Fixture | Rejected field | Error |
|---------|---------------|-------|
| `tier1-primitives/test-sets-gate` | `sets_gates` | UNDEFINED-FIELD |
| `tier3-list-processing/test-fan-out-emit-mapping` | malformed `emit_mapping` | YAML unmarshal error (map into string) |
| `tier3-list-processing/test-group-by-standalone` | `group_by` | UNDEFINED-FIELD |
| `tier4-cross-entity/test-create-entity` | `action_params` | UNDEFINED-FIELD |

**Decision needed**: Are these features the platform spec should support? If yes, add them to `platform-spec.yaml` handler field definitions. If no, these fixtures test features that don't exist yet — remove them from the catalog or mark them as future/planned.

## Verification

After fixing each fixture, verify it loads and runs:

```bash
# Check bundle loads
go test ./internal/runtime/cataloge2e/... -run "TestTierN.*fixture-name" -count=1 -v

# If it loads but still fails at runtime, reclassify as validation-gap
```

Once fixed, tell the implementer to move the fixture from the excluded map to the supported list.

## Summary

| Category | Count | Effort |
|----------|-------|--------|
| Missing produces | 4 | Trivial — add event to produces list |
| Missing entity_schema fields | 3 | Small — add fields to schema.yaml |
| payload.type reference | 4 | Investigation — likely YAML reserved word |
| Unsupported handler fields | 4 | Decision — spec feature or remove fixture |
| **Total** | **15** | |

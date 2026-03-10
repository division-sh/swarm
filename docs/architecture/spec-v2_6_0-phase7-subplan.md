# Phase 7 Subplan: Derive Internal Transition Semantics From Handlers

## Goal

Start shifting the runtime from flat transition-first semantics toward `2.6.x` handler-first semantics, while keeping execution behavior stable.

## Steps

### 1. Build a derived handler-transition view

Use node handlers plus flow schemas to derive an internal transition-like semantic layer from:

- `advances_to`
- `guard`
- `sets_gate`
- `data_accumulation`
- `emits`

Done when:
- the bundle can expose derived handler transitions alongside flat workflow transitions

### 2. Keep flat transitions as the active fallback

Do not replace the engine yet. Add the derived view in parallel and compare where safe.

Done when:
- production behavior is unchanged
- derived semantics are available to validation/tests

### 3. Use derived handler transitions in one validation/conformance path

Pick a low-risk consumer first:

- ownership coverage
- trigger visibility coverage
- state-advance coverage

Done when:
- at least one runtime/compliance path uses derived handler transition semantics directly

### 4. Validate handler/state coherence

Check that:

- `advances_to` targets valid flow states
- `data_accumulation.source_event` matches handler event type where required
- `sets_gate` references recognized gate/state fields

Done when:
- handler-first semantic inconsistencies fail validation

### 5. Prepare execution migration

Document exactly which execution semantics are still transition-first and what must change in later phases to make handler-first execution authoritative.

Done when:
- a later execution phase can consume the derived semantics without rediscovering them

## Acceptance Gate

```bash
go test ./internal/runtime/contracts ./internal/runtime/pipeline ./internal/runtime -count=1
go test ./... -count=1
```

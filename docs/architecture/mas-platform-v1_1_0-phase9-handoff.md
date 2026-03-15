# Phase 9: Code Quality + CI Hardening

**Date:** 2026-03-14
**Prerequisite:** Phase 8 done
**Risk level:** LOW — refactoring (no behavioral change) + CI config
**Scope:** 5 gaps (G-20, G-21, G-22, G-11, G-23)
**Master instruction:** Zero behavioral change. Same tests pass before and after. The goal is maintainability, not new features.

**Known red baseline:** `internal/promptcontracts` has a pre-existing failure (Empire prompt markdown files reference undefined variables: `opco-ceo.md`, `opco-chief-of-staff.md`, `opco-head-of-growth.md`, `opco-head-of-product.md`). This is an Empire contract authoring issue, not a platform bug — the spec writer is fixing the Empire YAML package. Phase 9 verification excludes `internal/promptcontracts` from the pass/fail gate. Use `go test $(go list ./... | grep -v promptcontracts) -count=1 -timeout 180s` for verification.

---

## G-20: Split `workflow_contracts.go` (4,450 lines)

**Scope:** Refactor — move code between files in the same package. No API changes.

### Current state

`internal/runtime/contracts/workflow_contracts.go` is 4,450 lines containing:
- Type definitions (DTOs, specs, schema types) — lines 18–627
- Bundle accessor methods (~70 methods) — lines 648–1201
- Project/package/flow structures — lines 1203–1349
- Bundle loading & initialization — lines 2235–2339
- Semantic view population — lines 2396–2526
- Semantic derivation (guards, actions, timers, stages) — lines 2527–2947
- Flow tree & package view building — lines 2993–3192
- Contract merging — lines 3238–3461
- YAML decoding helpers — lines 3469–4043
- Path discovery & package tree — lines 4164–4433

The package already has extracted files: `prompts.go` (10,940 lines), `schema_registry.go` (2,776 lines), `agent_registry_resolution.go` (2,784 lines), `prompt_schema_guard.go` (4,714 lines), `payload_fields.go` (1,390 lines), `enums.go` (2,568 lines).

### What to do

Split into 8 files. All files stay in `package contracts`. No exported API changes.

**Step 1:** Create `workflow_contract_types.go` (~600 lines)
- Move all type definitions: `ContractPaths`, `WorkflowContractBundle`, `WorkflowSemanticView`, `HandlerTransitionSemantic`
- Move all spec types: `GuardSpec`, `GuardCheck`, `GuardActionEntry`, `AccumulateSpec`, `ComputeSpec`, `FanOutSpec`, `FilterSpec`, `ReduceSpec`, `CountSpec`, `ClearSpec`, `BranchSpec`, `GateSpec`, `QuerySpec`, `ActionSpec`, `PayloadTransformSpec`, `TransformSpec`, `TransformBinding`
- Move entity/schema types: `EntitySchema`, `EntitySchemaGroup`, `EntitySchemaField`, `NodeStateSchema`, `NodeStateField`, `EventEmitterRef`, `EventPayloadSpec`, `EventFieldSpec`, `SchemaLiteral`, `ToolAdditionalProperties`, `ToolInputSchema`
- Move project structures: `ProjectPackagePaths`, `FlowContractPaths`, `ProjectPackageDocument`, `ProjectPackageRef`, `ProjectFlowRef`, `ProjectHandoff`, `LoadedProjectPackage`, `ProjectContractView`, `FlowContractView`, `FlowTree`, `ContractItemSource`
- Move flow schema types: `FlowSchemaDocument`, `FlowInstanceVariables`, `AutoEmitOnCreateContract`

**Step 2:** Create `workflow_contract_accessors.go` (~800 lines)
- Move all `(b *WorkflowContractBundle)` accessor methods: `WorkflowName`, `WorkflowVersion`, `WorkflowEntitySchema`, `WorkflowStages`, `WorkflowTerminalStages`, `WorkflowTransitions`, `WorkflowInitialStage`, `WorkflowTimers`, `FlowViewByID`, `FlowSchemaByID`, `HasFlow`, `ProjectViews`, `NodeEntries`, `AgentEntries`, `ToolEntries`, `EventEntries`, all individual entry lookups, all policy accessors, all flow semantics accessors, all source tracking methods, all event handler accessors

**Step 3:** Create `workflow_contract_loading.go` (~200 lines)
- Move: `LoadWorkflowContractBundle`, `LoadWorkflowContractBundleWithOverrides`, `loadWorkflowContractBundleForPaths`, `validateWorkflowContractBundleLoadConstraints`

**Step 4:** Create `workflow_contract_semantics.go` (~400 lines)
- Move: `populateWorkflowSemantics`
- Move all `derive*` functions: `deriveWorkflowGuardEntries`, `deriveWorkflowActionEntries`, `deriveWorkflowTransitionContract`, `deriveWorkflowSemanticTimers`, `deriveNodeWorkflowTimers`, `normalizeWorkflowSemanticTimer`, `mergeWorkflowSemanticTimer`, `inferWorkflowTimerEvent`, `workflowTimerEventDefined`, `appendPlatformBuiltinGuardEntries`, `appendPlatformBuiltinActionEntries`, `deriveWorkflowStagesFromFlows`, `deriveWorkflowTerminalStagesFromFlows`

**Step 5:** Create `workflow_contract_tree.go` (~300 lines)
- Move: `loadProjectContractView`, `loadFlowContractView`, `buildFlowTree`, `materializeFlowTree`, `flowTreeURIScheme`, `populateMergedPackageViews`, `nearestFlowTreeAncestor`

**Step 6:** Create `workflow_contract_merging.go` (~350 lines)
- Move all `merge*` functions: `mergeNodeContracts`, `mergeEventContracts`, `mergeEventCatalogEntry`, `mergeStringValue`, `mergeStringLists`, `mergeStringSliceValue`, `mergeBoolValue`, `mergeEventEmitterRef`, `mergeEventPayloadSpec`, `mergeAgentContracts`, `mergeToolContracts`
- Move helpers: `isEmptyEventEmitterRef`, `isEmptyEventPayloadSpec`

**Step 7:** Create `workflow_contract_yaml.go` (~500 lines)
- Move all `decode*` helpers: `hasYAMLMappingKey`, `hasAnyYAMLMappingKey`, `looksLikeEntitySchemaFieldMap`, `decodeStringListNode`, `decodeScalarStringNode`, `decodeBoolNode`, `decodeGuardSpecNode`, `decodeGateSpecNode`, `decodeClearGatesNode`, `decodeHandlerRuleEntryNode`, `decodeHandlerRuleEntriesNode`, `decodeQuerySpecNode`, `decodeActionSpecNode`, `decodeClearSpecNode`, `decodeConfigFromSpecNode`, `decodeBranchSpecsNode`, `decodeEventEmitterNode`, `decodeEventPayloadSpecNode`, `decodeEntitySchemaFields`, `decodeEntitySchemaField`, `decodeNodeStateFields`, `decodeNodeStateField`, `parseTypedFieldString`
- Move all `UnmarshalYAML` methods from spec types

**Step 8:** Create `workflow_contract_paths.go` (~250 lines)
- Move: `ContractFilesExist`, `existingFile`, `existingDir`, `ProjectPackageDocument.ChildPackages`, `ProjectPackageRef.ResolveLocation`, `discoverProjectPackagePaths`, `validateDiscoveredPackageTree`
- Move cloning functions: `cloneSystemNodeContractMap`, `cloneEventCatalogEntryMap`, `cloneAgentRegistryEntryMap`, `cloneToolSchemaEntryMap`
- Move scope/string utilities: `contractScopeKey`, `contractSameScope`, `normalizeStrings`, `appendIfMissingString`, `handlerPatternMatches`, `sortedContractKeys`
- Move YAML I/O: `loadYAMLFile`, `loadOptionalYAMLMap`

After all moves, `workflow_contracts.go` should be empty (or contain only a package doc comment). Delete if empty.

### Order of operations

Do the extraction in this order to minimize conflicts:
1. `workflow_contract_yaml.go` (pure helpers, no dependencies on other extracted files)
2. `workflow_contract_paths.go` (pure helpers + path discovery)
3. `workflow_contract_types.go` (type definitions only)
4. `workflow_contract_merging.go` (uses types, used by tree)
5. `workflow_contract_tree.go` (uses merging, used by loading)
6. `workflow_contract_semantics.go` (uses types, used by loading)
7. `workflow_contract_loading.go` (uses tree + semantics)
8. `workflow_contract_accessors.go` (uses types, independent of loading)

### Verification

```bash
# Compile check
go build ./internal/runtime/contracts/...

# All tests pass
go test ./internal/runtime/contracts/... -count=1 -timeout 60s

# Original file is gone or nearly empty
wc -l internal/runtime/contracts/workflow_contracts.go
# Should be < 10 lines (package declaration + doc comment) or file deleted

# No new exported symbols
# (same package, so no API change possible)
```

---

## G-21: Split `pipeline/` package (42 files, 8,408 lines)

**Scope:** Refactor — file reorganization within the package. No sub-package extraction (too risky for this phase).

### Current state

`internal/runtime/pipeline/` has 42 Go files in a flat structure. The files group naturally by concern but the naming is inconsistent and there's no clear organization.

### What to do

**Approach:** Do NOT create sub-packages. That would require changing import paths across the codebase — too much churn for a hygiene phase. Instead, apply consistent file naming prefixes and consolidate scattered files.

**Step 1:** Consolidate small files into their natural groups:

| Current file | Lines | Action |
|--------------|-------|--------|
| `state_machine.go` | 25 | Merge into `workflow.go` (just a type alias) |
| `workflow_state_helpers.go` | 92 | Merge into `workflow.go` |
| `persistence.go` | 15 | Merge into `workflow_instance_store.go` (2 interface declarations) |
| `workflow_runtime_source.go` | 9 | Merge into `runtime_interfaces.go` (1 type alias) |
| `declarative_workflow_node.go` | 49 | Merge into `declarative_default_node.go` (wrapper) |
| `workflow_nodes_runtime.go` | 97 | Merge into `workflow_nodes.go` (same concern) |
| `pipeline_helpers.go` | 124 | Merge into `runtime_support.go` (same utility role) |

**Step 2:** Apply consistent naming prefixes. After Step 1 merges, ensure files follow this convention:

| Prefix | Concern | Files |
|--------|---------|-------|
| `workflow_*` | Workflow types, nodes, state, timers, expressions | `workflow.go`, `workflow_nodes.go`, `workflow_timer_lifecycle.go`, `workflow_expression_evaluator.go`, `workflow_execution_plan.go`, `workflow_instance_store.go`, `workflow_state_persistence.go`, `workflow_instance_activation.go`, `workflow_contract_validation.go` |
| `engine_*` | Engine adapter, bridge, guard/action registry | `engine_adapter.go`, `engine_bridge.go` |
| `node_*` | Node implementations | Rename `declarative_default_node.go` → `node_declarative.go`, `background_workflow_node.go` → `node_background.go`, `system_node_runner.go` → `node_system_runner.go` |
| `coordinator*` | Main coordinator | `coordinator.go` (already correct) |
| `scheduler*` | Scheduling | `scheduler.go` (already correct) |
| `guard_action_*` | Registry | `guard_action_registry.go` (already correct) |

**Step 3:** Move `handler_preview.go` — rename to `workflow_handler_preview.go` for consistency.

**Step 4:** Move `transitions.go` — rename to `workflow_transitions.go` for consistency.

**Step 5:** Rename `recovery.go` → `coordinator_recovery.go` (it's coordinator infrastructure).

### Verification

```bash
# Compile check
go build ./internal/runtime/pipeline/...

# All tests pass
go test ./internal/runtime/pipeline/... -count=1 -timeout 120s

# File count should drop by ~7 (merges)
ls internal/runtime/pipeline/*.go | grep -v _test.go | wc -l
```

---

## G-22: Sentinel errors replacing string matching

**Scope:** ~100 lines of sentinel definitions + ~25 test file changes

### Current state

- 601 `fmt.Errorf` calls across 73 files
- 104 `errors.New` calls across 23 files
- Only 14 sentinel error variables in 4 files
- 17 `strings.Contains(err.Error(), ...)` in 7 test files — these are the fragile patterns
- Only 13 `errors.Is` / `errors.As` calls in the whole codebase
- Existing good pattern in `internal/runtime/engine/errors.go` (12 sentinels)

### What to do

**Scope constraint:** Only introduce sentinels for errors that are currently matched by string in tests. Do not refactor every `fmt.Errorf` in the codebase — that's gold-plating. The exact file list and hit count below are approximate — run the grep yourself before starting and use the live results as your inventory.

**Step 1:** Add sentinel errors to `internal/runtime/pipeline/errors.go` (new file):

```go
package pipeline

import "errors"

var (
    ErrContractBundleNil           = errors.New("pipeline: workflow contract bundle is nil")
    ErrExpressionEvaluatorNil      = errors.New("pipeline: workflow expression evaluator is not initialized")
    ErrExpressionEmpty             = errors.New("pipeline: workflow expression is empty")
    ErrHandlerEngineNotConfigured  = errors.New("pipeline: handler execution engine is not configured")
    ErrConflictingCompletion       = errors.New("pipeline: handler declares both on_complete and rules")
    ErrEventCycleDetected          = errors.New("pipeline: event emit cycle detected")
)
```

**Step 2:** Add sentinel errors to `internal/runtime/contracts/errors.go` (new file):

```go
package contracts

import "errors"

var (
    ErrBundleNil              = errors.New("contracts: workflow contract bundle is nil")
    ErrMissingRequiredAgent   = errors.New("contracts: required agent missing from registry")
    ErrLoadValidation         = errors.New("contracts: bundle load validation failed")
)
```

**Step 3:** Add sentinel errors to `internal/runtime/tools/errors.go` (new file):

```go
package tools

import "errors"

var (
    ErrToolNotAllowed      = errors.New("tools: tool not allowed for agent")
    ErrPermissionDenied    = errors.New("tools: agent lacks required permission")
    ErrUnknownEntityType   = errors.New("tools: unknown entity type")
)
```

**Step 4:** Update the production code to return wrapped sentinels. For each sentinel, find the corresponding `fmt.Errorf` or `errors.New` call and wrap:

```go
// Before:
return fmt.Errorf("workflow contract bundle is nil")

// After:
return fmt.Errorf("%w", ErrContractBundleNil)

// Or if dynamic context is needed:
return fmt.Errorf("%w: %s", ErrToolNotAllowed, toolName)
```

**Important:** Only change error returns that are matched by string in tests. Leave all other `fmt.Errorf` calls alone.

**Step 5:** Update test files to use `errors.Is` instead of `strings.Contains`:

```go
// Before:
if !strings.Contains(err.Error(), "declares both on_complete and rules") {

// After:
if !errors.Is(err, pipeline.ErrConflictingCompletion) {
```

Files to update (7 test files, ~17 occurrences as of live tree):
- `internal/runtime/pipeline/workflow_contract_validation_test.go`
- `internal/runtime/pipeline/workflow_contract_boot_phase4_test.go`
- `internal/runtime/tools/authorizer_permission_test.go`
- `internal/runtime/tools/executor_entity_test.go`
- `internal/runtime/contracts/load_validation_test.go` (if it exists)
- `internal/promptcontracts/promptcontracts_test.go`
- `internal/store/schema_ddl_test.go`
- `internal/runtime/bus/eventbus_publish_test.go`

Run `grep -rn 'strings.Contains(err.Error()' internal/ --include='*_test.go'` to get the exact live inventory before starting.

**Step 6:** For each test change, read the production code first to confirm the error return site. Do not blindly wrap — some `fmt.Errorf` calls chain through multiple call sites and the sentinel must be placed at the right level for `errors.Is` to work.

### Verification

```bash
# Zero string matching in test files for errors we've sentinel-ized
grep -rn 'strings.Contains(err.Error()' internal/ --include='*_test.go'
# Should return zero results (or only for errors we intentionally left alone)

# All tests pass
go test $(go list ./... | grep -v promptcontracts) -count=1 -timeout 180s
```

---

## G-11: CI pipeline cleanup

**Scope:** CI config + dead script removal

### Current state

1. `scripts/verify_wiring.py` runs `TestSpecRuntimeWiringVerification` — this test **does not exist** (confirmed: zero matches in codebase). The script is dead.
2. `EMPIRE_WIRING_STRICT` env var referenced in `verify_wiring.py` — no Go code reads it. Dead.
3. CI workflow (`.github/workflows/ci.yml`) runs `go test ./... -count=1` then coverage gates. No lint, no vet, no fmt check.
4. No `golangci-lint` config exists.

### What to do

**Step 1:** Delete `scripts/verify_wiring.py` — dead code, references deleted test.

**Step 2:** Add `go vet` to CI. In `.github/workflows/ci.yml`, add before the test step:

```yaml
      - name: Vet
        run: go vet ./...
```

**Step 3:** Add `gofmt` check to CI:

```yaml
      - name: Check formatting
        run: |
          unformatted=$(gofmt -l .)
          if [ -n "$unformatted" ]; then
            echo "Files not formatted:"
            echo "$unformatted"
            exit 1
          fi
```

**Step 4:** Add `golangci-lint` (optional but recommended). Create `.golangci.yml`:

```yaml
linters:
  enable:
    - errcheck
    - govet
    - staticcheck
    - unused
    - ineffassign
  disable:
    - gosimple  # too noisy for initial rollout

linters-settings:
  errcheck:
    exclude-functions:
      - (io.Closer).Close

issues:
  max-issues-per-linter: 50
  max-same-issues: 5

run:
  timeout: 5m
```

Add to CI:

```yaml
      - name: Lint
        uses: golangci/golangci-lint-action@v4
        with:
          version: latest
```

**Step 5:** Add a `make lint` target to `Makefile`:

```makefile
lint:
	golangci-lint run ./...
```

**Step 6:** Historical docs reference `verify_wiring.py` (`wiring-fail-classification-v2_0_17.md`, `spec-writer-guide.md`). Leave those alone — they are historical records. Only delete the script itself and any CI/Makefile references.

### Verification

```bash
# verify_wiring.py is gone
ls scripts/verify_wiring.py 2>/dev/null
# Should fail

# CI runs cleanly
# (push to branch and check Actions)

# go vet passes locally
go vet ./...

# gofmt passes locally
test -z "$(gofmt -l .)"
```

---

## G-23: Raise CI test gate minimums

**Scope:** CI config changes only

### Current state

Coverage thresholds in `Makefile`:
```
MIN_RUNTIME_COVER = 74%
MIN_PIPELINE_COVER = 74%
MIN_TOOLS_COVER = 71%
MIN_MANAGER_COVER = 74%
MIN_DASHBOARD_COVER = 74%
```

These may be below actual coverage. The gap inventory says "Total test minimum is set to 25 but actual test count is much higher."

### What to do

**Step 1:** Measure actual coverage for each package:

```bash
make check-runtime-cover 2>&1 | grep 'coverage gate'
make check-key-package-cover 2>&1 | grep 'coverage gate'
```

**Step 2:** Set each minimum to `actual - 2%` (floor, not ceiling — gives room for normal fluctuation without letting coverage regress significantly).

For example, if pipeline coverage is 82.3%, set `MIN_PIPELINE_COVER = 80`.

**Step 3:** Add coverage gate for `contracts` package. It's a key package (4,450+ lines in workflow_contracts.go alone) but has no gate:

Add to `Makefile`:

```makefile
MIN_CONTRACTS_COVER ?= <measured - 2>
```

Add to `check-key-package-cover`:

```makefile
	go test ./internal/runtime/contracts -coverprofile=$(COVER_DIR)/contracts.out
	./scripts/check_coverage.sh $(COVER_DIR)/contracts.out $(MIN_CONTRACTS_COVER)
```

Add to CI if not already covered by `make check-key-package-cover`.

**Step 4:** Add `bus` package coverage gate (critical path — event delivery):

```makefile
MIN_BUS_COVER ?= <measured - 2>
```

```makefile
	go test ./internal/runtime/bus -coverprofile=$(COVER_DIR)/bus.out
	./scripts/check_coverage.sh $(COVER_DIR)/bus.out $(MIN_BUS_COVER)
```

**Step 5:** Update the Makefile `.PHONY` line to include any new targets.

### Verification

```bash
# All coverage gates pass
make check-runtime-cover
make check-key-package-cover

# Thresholds are higher than before
grep 'MIN_.*_COVER' Makefile
```

---

## Execution order

```
G-22 (sentinel errors)     ← do first, small + self-contained
  ↓
G-20 (split contracts)     ← do second, pure file moves
  ↓
G-21 (reorg pipeline)      ← do third, pure file moves/renames
  ↓
G-11 (CI cleanup)          ← do fourth, infrastructure
  ↓
G-23 (raise gates)         ← do last, depends on actual coverage after refactoring
```

G-22 first because sentinels should be defined before splitting files (easier to place them in the right new file). G-23 last because coverage numbers may shift after file reorganization.

---

## Delivery checklist

- [ ] `internal/runtime/pipeline/errors.go` created with 6 sentinels
- [ ] `internal/runtime/contracts/errors.go` created with 3 sentinels
- [ ] `internal/runtime/tools/errors.go` created with 3 sentinels
- [ ] All `strings.Contains(err.Error()` in test files replaced with `errors.Is` (live count: ~17)
- [ ] Production error returns wrapped with sentinels
- [ ] `workflow_contracts.go` split into 8 files
- [ ] `workflow_contracts.go` is empty/deleted
- [ ] 7 small pipeline files merged into natural groups
- [ ] Pipeline files renamed with consistent prefixes
- [ ] `scripts/verify_wiring.py` deleted
- [ ] `go vet` added to CI
- [ ] `gofmt` check added to CI
- [ ] `.golangci.yml` created (optional)
- [ ] Coverage thresholds raised to `actual - 2%`
- [ ] `contracts` and `bus` coverage gates added
- [ ] All tests pass: `go test $(go list ./... | grep -v promptcontracts) -count=1 -timeout 180s`
- [ ] `go vet ./...` passes
- [ ] Zero `strings.Contains(err.Error()` in test files (for sentinel-ized errors; `promptcontracts` excluded from pass gate until spec writer fixes Empire prompts)

---

## What NOT to do

- Do NOT create sub-packages under `pipeline/` — import path changes affect the whole codebase
- Do NOT refactor every `fmt.Errorf` in the codebase — only sentinel-ize errors that tests match by string
- Do NOT change any exported API signatures — this is a refactor, not a redesign
- Do NOT rename test files that import from other packages — only rename files within the same package
- Do NOT set coverage minimums higher than `actual - 2%` — that makes CI flaky
- Do NOT add new features, fix bugs, or change behavior — hygiene only
- Do NOT use `log.Printf` — use `slog` for all new code
- Do NOT block on golangci-lint — if it produces too many findings, disable noisy linters and iterate later

# Phase 8: Empire Eradication + Migration Cleanup

**Date:** 2026-03-14 (rewritten — zero Empire in Go codebase)
**Prerequisite:** Phase 7 done
**Risk level:** MEDIUM — removes code, changes test infrastructure
**Scope:** 4 gaps (G-07, G-09, G-10, G-24) + full Empire purge, ~300 lines removed/changed
**Master instruction:** Absolutely nothing Empire-related in the Go codebase. The platform reads contracts at boot — whatever product's contracts are loaded defines the behavior.

---

## Empire Purge Inventory

The following Empire-specific code exists in the Go codebase and must be removed or made contract-driven. This is the complete inventory verified against the live tree.

### Files to delete entirely

| File | Lines | Content |
|------|-------|---------|
| `internal/runtime/manager/empire/bootstrap_test.go` | 21 | OpCo routing tests |
| `internal/runtime/manager/empire/spawn_test.go` | 23 | OpCo spawn tests |
| `internal/runtime/manager/bootstrap.go` | 18 | `EntityScopedAgentID()`, `EntityRouteRuleKey()`, `RouteRuleKey()` — Empire helpers, no runtime callers |
| `internal/runtime/tools/emit_contract_compat.go` | ~55 | `NormalizeScanModeCompat()`, `NormalizeScanPriorityCompat()`, Empire compat comments |

### Directories to delete

| Directory | Content |
|-----------|---------|
| `internal/runtime/manager/empire/` | Empire test files only |
| `internal/commgraph/empire/` | Empty directory |

### Code to remove from existing files

| File | Lines | What | Fix |
|------|-------|------|-----|
| `internal/runtime/agents/agent_llm.go` | 226 | `mode = runtimetools.NormalizeScanModeCompat(mode)` | Replace with contract-driven mode lookup |
| `internal/runtime/agents/agent_llm.go` | 587-591 | `normalizeScanMode()`, `normalizeScanPriority()` wrappers | Delete — callers use contract values directly |
| `internal/runtime/tools/helpers_runtime.go` | 31 | `normalizeScanPriority()` | Delete |
| `internal/runtime/tools/directive_helpers.go` | 14 | `fallbackVertical` variable name | Rename to `fallbackEntity` or generic equivalent |
| `internal/runtime/manager/helpers.go` | 205-230 | `ExpandConfigPromptTemplate()` — OpCo template expansion | Make generic or delete if unused after Empire removal |
| `internal/runtime/manager/receipts.go` | 110 | `strings.HasPrefix(eventType, "scan.")` | Replace with policy-driven prefix list |
| `internal/store/template_routing_store.go` | 283 | `org_templates` table — entire file | Delete (no runtime callers) |
| `internal/store/mailbox.go` | 13-14 | Comments referencing "Empire Legacy" | Remove comments |

### Empire references in test files

| File | Lines | What |
|------|-------|------|
| `internal/runtime/contracts/phase1_foundation_test.go` | 100-106 | Filter for `"*-empire.yaml"` patterns |
| `internal/promptcontracts/promptcontracts_test.go` | 135 | Path to `"docs/specs/mas-platform/empire/contracts"` |
| `internal/store/postgres_smoke_test.go` | 57 | `org_templates` test case |
| `internal/store/postgres_store_additional_test.go` | 1064, 1255 | `org_templates` assertions |

**Note:** The module name `empireai` in import paths (`empireai/internal/...`) is the Go module name, not Empire-specific code. Leave it alone — renaming the module is a separate decision.

---

## G-09: Make scan/discovery behavior contract-driven

**Scope:** ~40 lines changed

### What's wrong

Three hardcoded Empire behaviors exist in platform code:

1. `NormalizeScanModeCompat()` maps `"discovery"` → `"scan"` — Empire-specific mode names baked into Go
2. `NormalizeScanPriorityCompat()` normalizes priorities to `[normal/low/high/critical]` — should come from contract
3. `receipts.go:110` suppresses budget throttle for `"scan."` prefix — hardcoded Empire event namespace

### What to do

**Step 1:** Delete `emit_contract_compat.go` entirely. The functions `NormalizeScanModeCompat` and `NormalizeScanPriorityCompat` must not exist.

**Step 2:** In `agent_llm.go`, remove the calls at lines 226, 587-591. Mode and priority values come from the contract bundle — the agent's event payload or configuration already contains the correct values. If normalization is needed, it should be done at contract load time (in the contract parser), not at runtime in Go code.

Check what `mode` is used for at line 226. If the agent receives a mode value from the LLM and needs to validate it against known modes, that validation should check against the contract's `scan_modes` policy (from `policy.yaml`), not a hardcoded Go function.

**Step 3:** Replace the `"scan."` prefix check at `receipts.go:110`:

The budget throttle suppression needs to be policy-driven. The product's `policy.yaml` should declare which event prefixes suppress throttling. At boot, load this into the receipts/budget system:

```go
// Loaded from policy.yaml at boot time:
// throttle_suppress_prefixes: ["scan."]
```

How to wire it:
1. Check how the receipts code currently accesses configuration. It likely has a reference to the agent manager or runtime config.
2. Add a `ThrottleSuppressPrefixes []string` field to whatever config struct the receipts code reads.
3. Parse `throttle_suppress_prefixes` from `policy.yaml` during boot and pass it through.
4. Replace the hardcoded check:

```go
// Before:
if strings.HasPrefix(eventType, "scan.") {

// After:
for _, prefix := range am.throttleSuppressPrefixes {
    if strings.HasPrefix(eventType, prefix) {
        // suppress throttle
    }
}
```

**Step 4:** Delete `helpers_runtime.go:31` (`normalizeScanPriority()`).

**Step 5:** In `directive_helpers.go:14`, rename `fallbackVertical` to `fallbackEntity` or another generic name.

### Verification

```bash
# Zero scan compat functions
grep -rn 'NormalizeScan\|normalizeScan' internal/ --include='*.go' | grep -v _test.go
# Should return zero results

# Zero hardcoded scan prefix
grep -rn '"scan\."' internal/ --include='*.go' | grep -v _test.go
# Should return zero results
```

---

## G-10: Delete Empire tables from store

**Scope:** ~283 lines deleted

### What to do

**Step 1:** Delete `internal/store/template_routing_store.go` (283 lines). Verified: zero runtime callers. Methods `LoadLatestOrgTemplate`, `LoadOrgTemplate`, `SetEntityTemplateVersion`, `ResolveBootstrapVersion` are never called from `internal/runtime/` or `cmd/`.

**Step 2:** Remove `org_templates` test references:
- `postgres_smoke_test.go:57` — remove the org_templates test case
- `postgres_store_additional_test.go:1064,1255` — remove org_templates assertions

**Step 3:** Remove `org_templates` DDL from `contracts/ddl-canonical.sql` if present.

**Step 4:** Remove any orphaned `OrgTemplate` or `TemplateRoute` type definitions from `internal/store/types.go`.

**Step 5:** Remove "Empire Legacy" comments from `internal/store/mailbox.go:13-14`.

### Verification

```bash
grep -rn 'org_templates\|OrgTemplate\|TemplateRouting\|template_routing' internal/ --include='*.go'
# Should return zero results
```

---

## G-07: Complete boot validation step 8 (required_agents)

**Scope:** ~40 lines

### Current state

Step 10 (permissions) is done (Phase 6). Step 8 has flow-scoped required_agents validated (`workflow_contract_validation.go:380-401`). Root-level is a TODO:

```go
// workflow_contract_validation.go:402-403
// TODO(phase4): root-level schema.yaml required_agents are not exposed on
// semanticview.Source today
```

`semanticview.Source` has `FlowRequiredAgents(flowID string)` but no root-level `RequiredAgents()`. More importantly, the bundle model has **no root-level schema type at all** — the only loaded schema type is `FlowSchemaDocument` (`workflow_contracts.go:1316`), and `FlowRequiredAgents` is derived only from flow schemas (`workflow_contracts.go:1050`). There is no loader, parser, or storage for a root-level `schema.yaml`.

### What to do

This is a 4-layer change: loader → bundle → semanticview → validator.

**Step 1 — Loader:** Add root-level `schema.yaml` loading to the contract bundle loader. The root schema sits alongside `package.yaml` in the contract bundle root. During `LoadWorkflowContractBundle()`, check for `schema.yaml` at the bundle root and parse it.

The root schema should use the same format as flow-level schemas (`FlowSchemaDocument` at `workflow_contracts.go:1316`) or a subset of it. At minimum it needs:
```yaml
required_agents:
  - id: some-agent
```

If `FlowSchemaDocument` is appropriate, reuse it. If not, create a `RootSchemaDocument` with just `RequiredAgents`.

**Step 2 — Bundle:** Store the parsed root schema on `WorkflowContractBundle`. Add a field:
```go
RootSchema *FlowSchemaDocument  // or *RootSchemaDocument
```

Add an accessor method:
```go
func (b *WorkflowContractBundle) RootRequiredAgents() []FlowRequiredAgent {
    if b.RootSchema == nil {
        return nil
    }
    return b.RootSchema.RequiredAgents
}
```

**Step 3 — Semanticview:** Add `RequiredAgents() []runtimecontracts.FlowRequiredAgent` to `semanticview.Source` interface in `source.go`. Implement on `bundleSource` by delegating to `bundle.RootRequiredAgents()`.

**Step 4 — Validator:** In `workflow_contract_validation.go`, replace the TODO at line 402:

```go
rootRequired := source.RequiredAgents()
for _, req := range rootRequired {
    if _, ok := agentRegistry[req.ID]; !ok {
        errs = append(errs, fmt.Errorf("REQUIRED-AGENT-MISSING: root schema requires agent %q but not in registry", req.ID))
    }
}
```

Make it an error (same as event cycle detection), not a warning.

**Step 5:** Remove the TODO comment at line 402-403.

### Verification

```bash
go test ./internal/runtime/contracts/... -count=1 -timeout 60s -run RootSchema
go test ./internal/runtime/pipeline/... -count=1 -timeout 60s -run RequiredAgent
go test ./internal/runtime/pipeline/... -count=1 -timeout 60s -run ContractValidation
```

---

## G-24: Flatten SQL migrations to contract-driven DDL

**Scope:** ~80 lines changed + file deletions

### Current state

Production boot uses contract-driven DDL exclusively (`initializeStateStores()` → `GeneratePlatformTableDDLs` + `GenerateEntityTableDDLs` + `GenerateNodeStateTableDDLs` → `EnsureSchemaTables`).

Dead code:
- `migrations/` — 26 static SQL files, never loaded by runtime
- `postgres.go:63-72` — `ApplyMigrationFile()`, only called from tests
- `postgres.go:74-147` — `ApplyManagedMigrations()`, only called from tests
- `contracts/ddl-canonical.sql` — 47KB manual snapshot that drifts

Test usage: `manager_retry_policy_test.go:259,338-344` uses `ApplyMigrationFile(ctx, migrationPath(t))` pointing to `migrations/001_initial.sql`.

### What to do

**Step 1:** Create a test helper for contract-driven schema bootstrap:

```go
// internal/store/test_helpers.go or add to existing test helper
func BootstrapTestSchema(ctx context.Context, pg *PostgresStore, bundlePath string) error {
    bundle, err := runtimecontracts.LoadWorkflowContractBundle(bundlePath)
    if err != nil {
        return fmt.Errorf("load test bundle: %w", err)
    }
    plans, err := GeneratePlatformTableDDLs(bundle.Platform)
    if err != nil {
        return err
    }
    entityPlans, err := GenerateEntityTableDDLs(bundle.WorkflowEntitySchema())
    if err != nil {
        return err
    }
    statePlans, err := GenerateNodeStateTableDDLs(bundle.NodeEntries())
    if err != nil {
        return err
    }
    plans = append(plans, entityPlans...)
    plans = append(plans, statePlans...)
    return pg.EnsureSchemaTables(ctx, plans)
}
```

**Step 2:** Handle test files in two categories:

*Category A — Tests that use migrations to bootstrap a database for other testing:*
These call `ApplyMigrationFile("migrations/001_initial.sql")` as setup, then test something else (e.g., retry policy, receipts). Replace the migration bootstrap with `BootstrapTestSchema()`:
```bash
grep -rn 'ApplyMigrationFile\|001_initial\|migrationPath' internal/ --include='*_test.go'
```

*Category B — Tests that test the migration API itself:*
`postgres_helpers_test.go` tests `ApplyMigrationFile()` and `ApplyManagedMigrations()` directly. These tests exist to verify the migration infrastructure. Since the migration infrastructure is being deleted, **delete these tests entirely** — do not try to migrate them to the new helper. They test code that will no longer exist.

**Step 3:** Delete `migrations/` directory entirely (26 files).

**Step 4:** Delete `contracts/ddl-canonical.sql`.

**Step 5:** Remove dead migration code from `internal/store/postgres.go`:
- `ApplyMigrationFile()` (line 63-72)
- `ApplyManagedMigrations()` (line 74-147)
- `MigrationSpec` type

Only after Steps 2A and 2B confirm zero remaining callers.

### Verification

```bash
grep -rn 'migrations/' internal/ cmd/
# Should return zero results

grep -rn 'ddl-canonical' internal/ cmd/
# Should return zero results

go test ./internal/store/... -count=1 -timeout 120s
```

---

## Empire purge: remaining cleanup

After G-09, G-10, G-24 are done, finish the purge:

**Step 1:** Delete directories:
- `internal/runtime/manager/empire/` (bootstrap_test.go, spawn_test.go)
- `internal/commgraph/empire/` (empty)

**Step 2:** Delete files:
- `internal/runtime/manager/bootstrap.go` (18 lines — `EntityScopedAgentID()` etc., no runtime callers)

**Step 3:** Clean up helpers:
- `internal/runtime/manager/helpers.go:205-230` — `ExpandConfigPromptTemplate()` does OpCo-style `{{variable}}` expansion. Check if this is still called from `agent_manager.go:262`. If so, determine whether generic template expansion is still needed (it may be — template expansion itself is generic, only the variable names were Empire-specific). If the function is generic enough, keep it but rename any Empire variable names. If unused, delete.

**Step 4:** Clean up test files:
- `phase1_foundation_test.go:100-106` — remove `"*-empire.yaml"` filter
- `promptcontracts_test.go:135` — update path if it references Empire contracts

**Step 5:** Final verification:

```bash
# Zero Empire references in Go code (excluding module name "empireai" in imports)
grep -rn 'empire' internal/ cmd/ --include='*.go' | grep -v 'empireai/' | grep -v _test.go
# Should return zero results

# Zero Empire packages
ls internal/runtime/manager/empire/ internal/commgraph/empire/ 2>/dev/null
# Should fail (directories don't exist)

# Zero OpCo/vertical references (excluding generic "entity" usage)
grep -rn 'opco\|OpCo\|vertical_name' internal/ --include='*.go' | grep -v _test.go
# Should return zero results
```

---

## Delivery checklist

- [ ] `emit_contract_compat.go` deleted
- [ ] `NormalizeScanMode`/`NormalizeScanPriority` calls removed from `agent_llm.go`
- [ ] `normalizeScanPriority` removed from `helpers_runtime.go`
- [ ] `"scan."` prefix in `receipts.go` replaced with policy-driven list
- [ ] `fallbackVertical` renamed to generic name
- [ ] `template_routing_store.go` deleted
- [ ] `org_templates` references removed from tests
- [ ] Root-level schema.yaml loaded in contract bundle loader
- [ ] `RootRequiredAgents()` on `WorkflowContractBundle`
- [ ] `RequiredAgents()` on `semanticview.Source`
- [ ] Root-level required_agents validated at boot (TODO removed)
- [ ] Test helper `BootstrapTestSchema` created
- [ ] Category A tests migrated from `ApplyMigrationFile` to `BootstrapTestSchema`
- [ ] Category B tests (migration API tests) deleted
- [ ] `migrations/` directory deleted
- [ ] `contracts/ddl-canonical.sql` deleted
- [ ] `ApplyMigrationFile`, `ApplyManagedMigrations`, `MigrationSpec` removed
- [ ] `internal/runtime/manager/empire/` deleted
- [ ] `internal/commgraph/empire/` deleted
- [ ] `internal/runtime/manager/bootstrap.go` deleted
- [ ] `ExpandConfigPromptTemplate` made generic or deleted
- [ ] Empire references removed from test files
- [ ] All tests pass: `go test ./... -count=1 -timeout 180s`
- [ ] Final grep confirms zero Empire in Go code

---

## What NOT to do

- Do NOT move code to an `empire/` package — delete it or make it contract-driven
- Do NOT keep compat shim functions "just in case" — they encode Empire vocabulary in Go
- Do NOT rename the `empireai` Go module — that's a separate decision and affects all import paths
- Do NOT delete `migrations/` before updating test bootstraps
- Do NOT use `log.Printf` — use `slog` for all new code

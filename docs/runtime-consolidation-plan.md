# Runtime Package Consolidation Plan

Scope: `internal/runtime/*.go` (root package only, not subpackages)
Goal: Reduce from 48 prod files to ~26 by deleting dead code, inlining internal-only aliases, and merging small files.

Every move must leave the codebase compiling (`go build ./...`) and passing tests (`go test ./...`).

---

## Move 1: Delete dead aliases

Delete these type aliases — they are unused everywhere (zero references in prod or test):

File: `persistence.go` — remove `ScanCampaign`, check if anything else in the file uses it
File: `sharding.go` — remove `ShardAssignment`, `ShardPlanFunc`
File: `pipeline_coordinator.go` — check for `ParsedDirective`
File: `persistence.go` — remove `PriceRange` alias if present

Search for each name project-wide before deleting to confirm zero usage:
```
grep -r "ParsedDirective\|ShardAssignment\|ShardPlanFunc\|PriceRange" --include="*.go" .
```

If a name appears only in its own alias declaration, delete it.

Verification: `go build ./... && go test ./internal/runtime/...`

---

## Move 2: Inline internal-only aliases to qualified names

These 11 aliases are used only within `internal/runtime/*.go` (never by external consumers). Replace each usage with the qualified subpackage name and delete the alias.

| Alias | Defined In | Replace With | Used In |
|-------|-----------|-------------|---------|
| `BootstrapVersionResolver` | `manager_types.go` | `runtimemanager.BootstrapVersionResolver` | `manager_opco.go` |
| `EventReceiptReader` | `manager_types.go` | `runtimemanager.EventReceiptReader` | `manager_receipts.go` |
| `PipelineReceiptPersistence` | `eventbus.go` | `runtimebus.PipelineReceiptPersistence` | `eventbus_routing.go` |
| `AtomicEventPersistence` | `eventbus.go` | `runtimebus.AtomicEventPersistence` | `eventbus_publish.go` |
| `TransactionalEventStore` | `eventbus.go` | `runtimebus.TransactionalEventStore` | `eventbus_publish.go` |
| `DirectiveParser` | `directive_parser.go` | `runtimepipeline.DirectiveParser` | `runtime_misc_test.go` |
| `RecoveryManager` | `recovery.go` | `runtimepipeline.RecoveryManager` | `manager_runtime.go` |
| `Scheduler` | `scheduler.go` | `runtimepipeline.Scheduler` | `canned_llm_additional_scenarios_e2e_test.go` |
| `EmittedEventsRecorder` | `event_turn_context.go` | `runtimebus.EmittedEventsRecorder` | `agent_llm.go`, `agent_llm_test.go`, `llm_cli_test.go` |
| `OpCoCycleTracker` | `opco_cycle_tracker.go` | `runtimebus.OpCoCycleTracker` | `eventbus.go`, `eventbus_publish.go`, `eventbus_cycle_test.go` |
| `MCPStallDiagnosticConfig` | `mcp_stall_diagnostics.go` | `runtimemcp.StallDiagnosticConfig` | `mcp_stall_diagnostics_test.go` |

Process for each alias:
1. Find all usages: `grep -rn "AliasName" internal/runtime/*.go`
2. Ensure the file already imports the subpackage (add import if not)
3. Replace bare `AliasName` with `subpkg.AliasName`
4. Delete the `type AliasName = ...` line
5. Build and test

Do these one at a time or in small batches. Each batch must compile.

Verification: `go build ./... && go test ./internal/runtime/...`

---

## Move 3: Delete empty alias-only files

After Moves 1-2, these files should be empty or contain only a package declaration and unused imports. Delete them:

- `directive_parser.go` (was only the DirectiveParser alias + a trivial wrapper — check if wrapper is used)
- `recovery.go` (was only the RecoveryManager alias)
- `scheduler.go` (was only the Scheduler alias + constructor wrapper — check constructor usage)
- `opco_cycle_tracker.go` (was only the OpCoCycleTracker alias + constructor wrapper)
- `event_turn_context.go` (was only EmittedEventsRecorder alias)
- `scoring_node.go` (check — ScoringNode alias IS used externally by 1 file, may need to keep)
- `runtime_error.go` (RuntimeError alias used internally — already inlined in Move 2, check if file is now empty)

For files that also contain constructor wrappers (e.g. `NewScoringNode`, `NewScheduler`, `NewOpCoCycleTracker`), move those constructors to `aliases.go` (created in Move 4) before deleting the file.

Verification: `go build ./... && go test ./internal/runtime/...`

---

## Move 4: Consolidate remaining external aliases into single `aliases.go`

Create `aliases.go` containing all surviving type aliases that external consumers depend on. These stay because `store`, `cmd/empire`, `dashboard`, `factory`, `mailbox`, `digest`, `ops` reference them as `runtime.XYZ`:

```
// aliases.go — re-exports from subpackages for consumer convenience
package runtime

import (
    runtimebus     "empireai/internal/runtime/bus"
    runtimemanager "empireai/internal/runtime/manager"
    runtimepipeline "empireai/internal/runtime/pipeline"
    runtimetools   "empireai/internal/runtime/tools"
)

// --- bus ---
type EventStore = runtimebus.EventStore
type ActiveAgentLister = runtimebus.ActiveAgentLister
type InMemoryEventStore = runtimebus.InMemoryEventStore
type RoutingTable = runtimebus.RoutingTable
type Route = runtimebus.Route

// --- manager ---
type PersistedAgent = runtimemanager.PersistedAgent
type PersistedRoutingRule = runtimemanager.PersistedRoutingRule
type VerticalInfoReader = runtimemanager.VerticalInfoReader
type VerticalInfo = runtimemanager.VerticalInfo
type EventReceipt = runtimemanager.EventReceipt
type PromptOverrideRecord = runtimemanager.PromptOverrideRecord
type PromptOverridePersistence = runtimemanager.PromptOverridePersistence
type OrgTemplateRecord = runtimemanager.OrgTemplateRecord
type ManagerPersistence = runtimemanager.ManagerPersistence

// --- pipeline ---
type FactoryPipelineCoordinator = runtimepipeline.FactoryPipelineCoordinator
type SchedulePersistence = runtimepipeline.SchedulePersistence
type ScanCampaign = runtimepipeline.ScanCampaign
type CreateScanCampaignInput = runtimepipeline.CreateScanCampaignInput
type ScanCampaignFilter = runtimepipeline.ScanCampaignFilter
type ScanCampaignPersistence = runtimepipeline.ScanCampaignPersistence
type Schedule = runtimepipeline.Schedule
type ScoringNode = runtimepipeline.ScoringNode
type ShardPlanner = runtimepipeline.ShardPlanner

// --- tools ---
type MailboxItem = runtimetools.MailboxItem
type MailboxPersistence = runtimetools.MailboxPersistence
```

Move any constructor wrappers (`NewFactoryPipelineCoordinator`, `NewScoringNode`, `NewScheduler`, `NewShardPlanner`, `NewOpCoCycleTracker`, `NewScanCampaignManager`) into this file as well, since they're one-line pass-throughs.

Then delete the now-empty source files:
- `manager_types.go`
- `pipeline_coordinator.go`
- `sharding.go`
- `persistence.go` (move the non-alias types like `InboundPersistence`, `DigestPersistence`, `TurnPersistence`, `ConversationPersistence`, `AgentTurnRecord`, `ConversationRecord`, `InboundTarget`, `VerticalDigestRow` to a better home — see Move 5)

Verification: `go build ./... && go test ./...` (full project — external consumers affected)

---

## Move 5: Relocate persistence interfaces from `persistence.go`

`persistence.go` also defines original types (not aliases). These need a home after the alias cleanup:

| Type | New Home | Rationale |
|------|----------|-----------|
| `InboundPersistence`, `InboundTarget` | `inbound.go` | InboundGateway uses them |
| `DigestPersistence`, `VerticalDigestRow` | `diagnostics.go` | Digest/observability domain |
| `TurnPersistence`, `AgentTurnRecord` | `agent_llm.go` | LLM turn tracking |
| `ConversationPersistence`, `ConversationRecord` | `agent_llm.go` | LLM conversation tracking |

Just move the type definitions — they stay in `package runtime`, same package, no import changes.

Verification: `go build ./... && go test ./internal/runtime/...`

---

## Move 6: Merge small MCP files

Merge these 4 files into a single `mcp_hooks.go`:

- `mcp_errors.go` (30 lines)
- `mcp_context.go` (66 lines)
- `mcp_gateway_hooks.go` (133 lines)
- `mcp_stall_diagnostics.go` (51 lines)

Result: 1 file, ~280 lines. All MCP integration glue in one place.

Verification: `go build ./... && go test ./internal/runtime/...`

---

## Move 7: Merge small helper/utility files

Merge into a single `helpers.go`:

- `string_helpers.go` (12 lines)
- `schema_helpers.go` (125 lines)
- `timeutil.go` (45 lines)

Result: 1 file, ~182 lines.

Verification: `go build ./... && go test ./internal/runtime/...`

---

## Move 8: Merge small domain files into their parents

| Source File | Merge Into | Combined Size |
|-------------|-----------|---------------|
| `budget_scope.go` (25 lines) | `budget.go` (466 lines) | ~491 lines |
| `session_observability.go` (50 lines) | `session_rotation.go` (100 lines) | ~150 lines |
| `eventbus_subscriptions.go` (73 lines) | `eventbus.go` (123 lines) | ~196 lines |
| `llm_runtime.go` (62 lines — NoopRuntime) | `agent_llm.go` (727 lines) | ~789 lines |
| `pipeline_prefilter_compat.go` (15 lines) | `pipeline_helpers.go` (47 lines) | ~62 lines |
| `runtime_epoch.go` (51 lines) | `eventbus_publish.go` (512 lines) | ~563 lines |
| `actor_context.go` (16 lines) | `helpers.go` (from Move 7) | ~198 lines |

Verification: `go build ./... && go test ./internal/runtime/...`

---

## Move 9: Merge generated/registry shims

- `event_catalog_payload_fields_generated.go` (7 lines) — merge into `event_schema_registry_runtime.go` (9 lines)

Result: 1 file, ~16 lines. Rename to `event_schema_registry.go`.

Verification: `go build ./... && go test ./internal/runtime/...`

---

## Final File Inventory (expected ~26 prod files)

| File | Lines (est) | Domain |
|------|-------------|--------|
| `aliases.go` | ~120 | Type re-exports + constructor wrappers |
| `agent_llm.go` | ~850 | LLMAgent + NoopRuntime + turn/conversation persistence types |
| `budget.go` | ~490 | BudgetTracker + budget scope |
| `corpus_observability.go` | ~110 | Turn metadata collection |
| `dbtx.go` | ~80 | DB transaction context key |
| `diagnostics.go` | ~340 | RuntimeLogEntry + digest persistence types |
| `event_schema_registry.go` | ~16 | Generated schema init |
| `eventbus.go` | ~200 | EventBus struct + subscriptions |
| `eventbus_publish.go` | ~560 | Publish path + epoch |
| `eventbus_routing.go` | ~305 | Routing, delivery, receipts |
| `helpers.go` | ~200 | String, schema, time, actor context helpers |
| `inbound.go` | ~420 | InboundGateway + persistence interface |
| `llm_api.go` | ~500 | AnthropicAPIRuntime |
| `llm_cli.go` | ~1020 | ClaudeCLIRuntime |
| `manager.go` | ~490 | AgentManager + Agent interface |
| `manager_opco.go` | ~650 | OpCo lifecycle + teardown |
| `manager_receipts.go` | ~450 | Event receipt generation |
| `manager_runtime.go` | ~690 | Runtime factory + agent factory |
| `mcp_hooks.go` | ~280 | MCP errors + context + gateway hooks + stall diagnostics |
| `migration_classifier.go` | ~113 | Migration classification |
| `pipeline_helpers.go` | ~62 | Pipeline helper functions + prefilter compat |
| `scan_campaign_manager.go` | ~120 | ScanCampaignManager |
| `session_rotation.go` | ~150 | Session rotation + observability |
| `shard_dispatcher.go` | ~836 | ShardDispatcher |
| `warnings.go` | ~58 | Runtime warning accumulation |

Test files (43) remain unchanged.

---

## Rules for the implementer

1. Do one move at a time. Run `go build ./...` after each move. Do not batch moves.
2. Do NOT rename any exported identifiers. Only move and merge.
3. When merging files, preserve the original order of declarations. Do not reorder functions.
4. When deleting an alias, grep the entire project first: `grep -r "runtime\.AliasName" --include="*.go" .`
5. If a constructor wrapper like `NewScoringNode` exists in a file being deleted, move it to `aliases.go` — do not delete it.
6. If any grep reveals an unexpected consumer of an alias marked for deletion, STOP and flag it. Do not guess.
7. Test files do not move. They reference unqualified names within the same package, so file merges don't affect them.
8. `go vet ./...` must also pass after each move.

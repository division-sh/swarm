# MAS Platform Compliance Audit Prompt

You are auditing a Go codebase (`empireai`) that implements a Multi-Agent System (MAS) platform. The platform was extracted from an Empire-specific monolith into a generic, contract-driven runtime. Your job is to verify the platformization is complete and the implementation follows the spec.

## Authoritative sources (read these first)

1. **Platform spec (YAML — single source of truth):** `docs/specs/mas-platform/platform/contracts/platform-spec.yaml` (~1,788 lines). This defines everything: vocabulary, contract formats, handler execution order, boot validation steps, permissions model, tool model, flow model, execution primitives, engine behavior, system node specification, entity schema format, and platform table DDL.

2. **Platform spec (prose overview):** `docs/specs/mas-platform/platform/platform-spec.md` (~251 lines). Readable summary of core concepts: flows, nesting, events, system nodes, agents, pins, wiring.

3. **Implementer handoff:** `docs/specs/mas-platform/IMPLEMENTER-HANDOFF.md` (~276 lines). Bridges the old spec version to the current one. Answers 4 key runtime questions about gate-setting timing, data accumulation, side-effect emits, and packaging.

4. **Empire contracts (the product):** `docs/specs/mas-platform/empire/contracts/` — the actual product bundle loaded at boot. Contains `package.yaml`, `schema.yaml`, `nodes.yaml`, `events.yaml`, `agents.yaml`, `policy.yaml`, `tools.yaml`, plus 4 child flows (discovery, scoring, validation, operating).

5. **Test catalog:** `docs/specs/mas-platform/tests/TEST-CATALOG.md` — defines 105+ compliance tests across 10 tiers. Test fixtures live in `tests/` at repo root (175+ YAML-driven test cases across tier1 through tier11).

## What you are auditing

The Go runtime under `internal/runtime/` and `cmd/mas/` implements the MAS platform. The platform must be fully generic — it reads contracts at boot and whatever product's contracts are loaded defines the behavior. There should be zero hardcoded product (Empire) logic in the Go runtime.

## Audit scope

Check everything below. For each area, report one of:
- **PASS** — implementation matches spec
- **GAP** — spec requires something the code doesn't implement
- **DRIFT** — code does something the spec doesn't authorize
- **RISK** — technically works but fragile or likely to break

Be specific. Cite file:line for every finding. Quote the spec section that applies.

---

## 1. Empire residue

Search the entire Go codebase (`internal/`, `cmd/`) for any hardcoded Empire-specific behavior. The Go module name `empireai` in import paths is acceptable — that's the module name, not product logic. Everything else is a finding.

Check for:
- Hardcoded Empire event names, agent IDs, flow names, stage names
- Hardcoded scan/discovery mode names or priority values
- Hardcoded `"scan."` or `"discovery."` prefixes in routing or budget logic
- Any `empire/` Go package directories
- Any `opco`, `OpCo`, `vertical`, `vertical_name` references outside of test fixtures
- Any functions named `*Empire*`, `*Compat*`, `*Legacy*`
- Comments referencing "Empire" in non-test Go files (excluding import paths)

## 2. Contract-driven behavior

The platform must derive ALL behavior from contracts loaded at boot. Verify:
- **Tool schemas** come from the contract bundle, not from filesystem paths or hardcoded Go structs
- **Event routing** is built from contract `subscribes_to` / `produces` declarations
- **Agent permissions** are resolved from `permissions_bundle` + explicit `permissions` in agents.yaml, expanded via `permission_bundles` in policy.yaml
- **Handler execution** follows the dependency DAG in platform-spec.yaml `handler_execution_order`, not a hardcoded step list
- **Scan/discovery mode normalization** — no Go functions normalize mode or priority values; those come from contracts
- **Budget throttle suppression** — driven by policy.yaml `throttle_suppress_prefixes`, not hardcoded event prefixes
- **Boot validation steps** — all 11 steps defined in spec's `compliance_rules` are implemented and enforced

## 3. Boot sequence compliance

Read `cmd/mas/main.go` and trace the boot path. The spec defines boot validation steps in `compliance_rules`. Verify each step is implemented and produces the correct outcome (error vs warning).

Check specifically:
- Step 1: contract_syntax — YAML parse errors abort boot
- Step 2: schema_completeness — missing required fields abort boot
- Step 3: event_wiring — orphan events, dead subscriptions detected
- Step 4: agent_tool_authorization — agents only use tools they're authorized for
- Step 5: policy_resolution — policy keys resolve for every node
- Step 6: prompt_completeness — prompts exist for all agents
- Step 7: entity_schema_validation — entity fields are well-typed
- Step 8: required_agents — root and flow-level required agents present
- Step 9: event_cycle_detection — emit→subscribe cycles detected (must be ERROR, not warning)
- Step 10: permission_validation — agents have permissions for their gated tools (must be ERROR)
- Step 11: platform_table_ddl — DDL generation succeeds

Also check: are boot steps that should be errors actually errors (not warnings that get logged and ignored)?

## 4. Handler execution order

The spec defines a handler execution dependency DAG in `handler_execution_order`. Read that section, then verify the Go engine executes steps in the correct order.

Check:
- `internal/runtime/engine/` — how does the engine sequence handler steps?
- Are all execution primitives implemented? The spec lists: `clear_gates`, `guard`, `query`, `accumulate`, `filter`, `reduce`, `count`, `compute`, `on_complete`/`rules`, `advances_to`/`sets_gate`/`data_accumulation`, `payload_transform`, `emits`, `action`, `clear`
- Is the ordering correct per the dependency graph?
- What happens when a guard fails? Does it correctly short-circuit subsequent steps?
- Does `on_complete` vs `rules` mutual exclusivity get enforced?

## 5. Permissions model

The spec defines 13 platform permissions in `permissions_model`. Verify:
- All 13 permissions are recognized by the authorizer
- The authorizer checks permissions between the universal tier and the emit_allowed tier
- `permissions_bundle` is resolved from policy.yaml at agent config build time
- Explicit permissions extend (not replace) the bundle
- Boot step 10 validates that agents with gated tools have the required permissions
- Infra tools (`nginx_reload`, `systemd_control`, `certbot_execute`) require `system_admin` or equivalent

## 6. Event system

Verify the event bus and event handling match the spec:
- Events have typed payloads validated against the contract's `event_schema`
- Bus-level payload validation exists (not just at the tool executor level)
- Dead-letter handling: events that fail delivery N times go to dead letter with `ops.dead_letter_escalation`
- Retry policy: exponential backoff per spec
- Event idempotency via `idempotency_key`
- Causal chain tracing via `source_event_id`
- No panics in the event bus — all errors recovered gracefully

## 7. Flow model

The spec defines static and template flow modes. Verify:
- `mode: static` flows have exactly one instance at boot
- `mode: template` flows have zero instances at boot, created via `create_flow_instance` tool
- Flow instance routes are persisted and restored on recovery
- Dynamic flow instances can be activated and deactivated
- Flow-scoped events are namespaced correctly
- Hierarchical flow nesting works (parent→child flow references)

## 8. System node specification

The spec defines how system nodes work. Verify:
- System nodes subscribe to events declared in their `subscribes_to` contract
- System nodes only produce events declared in their `produces` contract
- Handler logic follows the contract (guards, actions, state transitions, emits)
- Node state schemas are loaded from contracts and DDL is generated at boot
- Accumulation modes (`all`, `threshold`) work correctly
- Fan-out, filter, reduce, count, clear primitives are implemented

## 9. Tool model

Verify tools match the spec:
- All tools registered in the handler registry match the tool definitions in contracts
- Tool input schemas are validated against contract `input_schema`
- The `create_flow_instance` tool exists and is agent-callable
- Tool authorization tiers: universal → permission → emit_allowed → actor_config → default

## 10. Persistence model

Verify the database layer matches the spec's `platform_tables` section:
- All 12+ platform tables defined in the spec exist in the DDL generation code
- Entity tables are generated from `entity_schema` in contracts
- Node state tables are generated from `state_schema` in node contracts
- No hardcoded SQL migrations remain — everything is contract-driven DDL at boot
- The `migrations/` directory is deleted or empty
- `ddl-canonical.sql` does not exist
- `ApplyMigrationFile` / `ApplyManagedMigrations` functions do not exist

## 11. Test coverage

Verify the test infrastructure:
- The catalog test runner (`internal/runtime/masflowtest/`) executes YAML-driven test cases
- Minimum 25 catalog cases are enforced
- Tiers 1-5 each have at least one case executed
- `assertCatalogRunResult` checks: handler_outcome, entity_state, emitted_events, entity_fields, gates, agent_received, dead_letter, template_instances
- No test assertions are silently skipped (watch for `t.Log` without `t.Fatal`, commented-out assertions, empty test bodies)
- No `strings.Contains(err.Error()` patterns in test files — should use `errors.Is`

## 12. Architecture and code quality

Check for structural issues:
- No circular dependencies between packages
- `internal/runtime/bus/` does not import `internal/runtime/tools/` or `internal/runtime/contracts/` directly (payload validation is injected via callback)
- `internal/runtime/pipeline/` does not leak into packages that shouldn't depend on it
- Sentinel errors are used for error classification (check `internal/runtime/*/errors.go`)
- No `log.Printf` — all logging uses `slog`

## 13. Spec gaps and drift

Look for cases where:
- The spec says something the code doesn't implement
- The code does something the spec doesn't mention
- The spec is ambiguous and the code made an assumption
- The spec defines a feature as required but the code treats it as optional
- Platform-spec.yaml sections that have no corresponding Go implementation

Pay special attention to:
- Timer lifecycle (start, cancel, fire, recurrence)
- Agent manager (hire, fire, reconfigure)
- Workspace/infrastructure tools
- MCP integration points
- Recovery/restart behavior

---

## Output format

For each section (1-13), produce:

```
## Section N: <title>

**Verdict:** PASS | GAP | DRIFT | RISK

**Findings:**
- [PASS/GAP/DRIFT/RISK] <description> — <file:line> vs <spec section>
- ...

**Evidence:** <brief code or spec quote supporting the finding>
```

At the end, produce a summary table:

```
| Section | Verdict | Findings | Critical |
|---------|---------|----------|----------|
| 1. Empire residue | PASS/GAP/... | N | Y/N |
| ... | ... | ... | ... |
```

And a prioritized list of any GAP or DRIFT findings that need action.

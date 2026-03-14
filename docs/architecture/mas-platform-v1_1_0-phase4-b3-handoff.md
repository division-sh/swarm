# Phase 4 Tranche B3: Wire Entity Persistence Tools Into Agent LLM Sessions

**Date:** 2026-03-13
**Prerequisite:** B2 (entity tool handlers) committed and green
**Risk level:** HIGH — changes the agent execution path; if entity tools return wrong shape, all agents break

---

## What Already Works

B2 delivered the handlers and schemas. The following are ALREADY done — do NOT rewrite:

| Layer | File | Status |
|-------|------|--------|
| Handlers | `tools/executor_entity.go` | 5 handlers registered |
| Schema validation | `tools/entity_schema.go` | whitelist against loaded schema |
| Handler registry | `tools/handler_registry.go` | `registerEntityHandlers()` |
| Tool schemas | `tools/contracts.go` | `builtinRuntimeContractSchemas()` + `supportedRuntimeToolNames` |
| Executor entry | `tools/executor.go` | `ToolDefinitions()` returns entity tools |
| Factory wiring | `runtime.go:220` | `NewLLMAgentFactory(rt.LLM, rt.ToolExecutor, rt.ToolExecutor.ToolDefinitions())` |
| Agent construction | `agents/agent_llm.go:32-43` | tools passed to `NewLLMAgent`, filtered by `extractAllowedToolSet` |

## What B3 Must Do

Three focused changes, nothing else.

---

### B3.1: Make entity tools universal

**File:** `internal/runtime/tools/policy.go`

Entity tools must survive `allowed_tools` filtering. Right now, only `agent_message` and `mailbox_send` are universal. If an agent has a constrained tool list (e.g. `allowed_tools: ["emit_scan_completed", "agent_message"]`), entity tools are silently excluded.

**Action:** Add all 5 entity tools to `universalRuntimeTools`:

```go
var universalRuntimeTools = map[string]struct{}{
    "agent_message":    {},
    "mailbox_send":     {},
    "get_entity":       {},
    "save_entity_field": {},
    "create_entity":    {},
    "search_entities":  {},
    "query_metrics":    {},
}
```

**Rationale:** Every agent needs entity CRUD. Entity tools are platform-provided, not product-specific. An agent that can't read its own entity data is useless. Same logic as `agent_message` — it's infrastructure, not a privilege.

---

### B3.2: Include entity tools in agent execution contract text

**File:** `internal/runtime/agents/agent_llm.go`, function `formatEventForAgent` (line ~508)

Currently the execution contract text tells the agent about its emit tools:
```
Available emit tools for your role: emit_scan_completed, emit_analysis_done
```

It says nothing about entity tools. Agents need to know they have entity persistence tools so they actually use them instead of trying to return JSON or use raw SQL patterns.

**Action:** Add an entity tools line to `formatEventForAgent`. After the emit tools line, append:

```
Available entity tools: get_entity, save_entity_field, create_entity, search_entities, query_metrics
```

This is a static list (all 5 entity tools), not per-role. The line should always be present.

**Implementation guidance:**
- Add the line just before the final format string return
- Keep it simple — one static line, no dynamic generation needed
- The format should be:
```go
entityToolsLine := "\n- Available entity persistence tools: get_entity, save_entity_field, create_entity, search_entities, query_metrics"
```
- Append `entityToolsLine` to the format string

---

### B3.3: Integration test — agent calls entity tool end-to-end

**File:** New test in `internal/runtime/tools/executor_runtime_test.go` (or new file `executor_entity_integration_test.go` if the existing file is too large)

Write ONE integration test that proves the full path works:

1. Create an `Executor` with a real (test) `*sql.DB` and a `workflowSource` that has at least one entity schema
2. Set up actor context with a valid `AgentConfig`
3. Call `executor.Execute(ctx, "get_entity", map[string]any{"entity_type": "...", "entity_id": "..."})`
4. Verify: returns structured result (not error) when entity exists, returns `not_found` error when it doesn't

Also add a test for the authorization path:
1. Create actor with `allowed_tools: ["emit_something"]` (constrained, but entity tools NOT listed)
2. Verify: entity tools still work (because they're universal after B3.1)

**Pattern to follow:** Look at `executor_runtime_test.go` for existing test patterns with the executor. Use the same test DB setup.

---

## What B3 Must NOT Do

- Do NOT change the dispatcher or handler logic (B2 already works)
- Do NOT add entity-type-level authorization (that's a future tranche if needed)
- Do NOT change how tools reach `llm.Conversation` — the existing path via `ToolDefinitions()` → `NewLLMAgentFactory` → `NewLLMAgent` already works
- Do NOT modify `builtinRuntimeContractSchemas()` — the schemas are correct
- Do NOT add per-agent entity-type filtering — all agents can access all entity types for now

## Testing Checklist

- [ ] `go build ./...` passes
- [ ] `go test ./internal/runtime/tools/... -count=1` passes
- [ ] `go test ./internal/runtime/agents/... -count=1` passes
- [ ] New integration test proves `Execute("get_entity", ...)` works through full pipeline
- [ ] New test proves entity tools survive constrained `allowed_tools` filtering
- [ ] `formatEventForAgent` output includes entity tools line (verify with a unit test or spot-check in existing agent_llm_test.go)

## Estimated Scope

- ~15 lines changed in `policy.go`
- ~5 lines changed in `agent_llm.go`
- ~80-120 lines of new test code
- Total: ~100-140 lines

This is the smallest of the B sub-tranches by code volume, but highest risk because it touches the agent execution path. Any mistake in `formatEventForAgent` breaks ALL agents. Test carefully.

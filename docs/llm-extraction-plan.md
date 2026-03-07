# LLM Runtime Extraction Plan (v2 — rebased)

Scope: Move LLM runtime implementations (`llm_api.go`, `llm_cli.go`, `llm_runtime.go`)
from `internal/runtime/` into `internal/runtime/llm/`.

`agent_llm.go` stays in root — it is agent orchestration logic, not LLM runtime logic.

Goal: Runtime root drops from ~7,076 to ~5,495 prod LOC.

---

## Current State (baseline for this plan)

### `runtime/llm` (930 LOC, 6 files)

Already moved here by prior work:

| File | LOC | Contents |
|------|-----|----------|
| types.go | 44 | Session, Message, ToolDefinition, Runtime interface, Response, ToolCall |
| conversation.go | 322 | Conversation, ToolExecutor, ConversationMode |
| persistence.go | 39 | AgentTurnRecord, TurnPersistence, ConversationRecord, ConversationPersistence |
| session_rotation.go | 150 | MaybeRotateAfterTurn, MaybeRotateAfterParseFailures, BuildSessionSummary, etc. |
| conversation_test.go | 186 | Tests |
| session_rotation_test.go | 189 | Tests |

### `runtime/` root (24 prod files, ~7,076 LOC)

Files to move:

| File | LOC | Dependencies on runtime root |
|------|-----|------------------------------|
| llm_api.go | 498 | `*BudgetTracker` (4 methods), `ActorFromContext`, `UsageTokens`, `budgetExecutionScopeKey` |
| llm_cli.go | 1,021 | Same as API + `registerMCPTurnContextWithTTL`, `unregisterMCPTurnContext`, `asString` |
| llm_runtime.go | 62 | `*BudgetTracker` field, calls `NewAnthropicAPIRuntime` and `NewClaudeCLIRuntime` |

Files that stay:

| File | LOC | Why |
|------|-----|-----|
| agent_llm.go | 727 | Agent orchestration: prompt resolution, tool surface composition, event formatting, emitted-event recording. Not LLM runtime logic. |
| manager.go | 492 | Agent/AgentFactory interfaces, AgentManager. Orchestration abstractions. |
| All others | ~4,276 | EventBus, budget, inbound, diagnostics, MCP hooks, etc. |

---

## Dependencies to Break

### 1. `*BudgetTracker` (used by both runtimes)

4 methods called:
- `LockExecutionScope(scopeKey string) func()`
- `IsEmergency(verticalID string) bool`
- `IsThrottle(verticalID string) bool`
- `RecordLLMUsage(ctx, verticalID, agentID, runtimeMode string, usage UsageTokens, exact bool, meta any) error`

Resolution: **Callback struct in `runtime/llm`**.

### 2. `ActorFromContext` (used by both runtimes)

Currently in `helpers.go`, forwards to `runtimetools.ActorFromContext`.

Resolution: **Call `runtimetools.ActorFromContext` directly**. `runtime/llm` already
can import `runtime/tools` without circular deps.

### 3. `UsageTokens` (used by both runtimes)

Currently defined in `budget.go`.

Resolution: **Move to `runtime/llm`**. Alias in `budget.go`.

### 4. `budgetExecutionScopeKey` (used by both runtimes)

Currently in `budget.go`. Takes `models.AgentConfig`, returns `string`.

Resolution: **Move to `runtime/llm`**. Only needs `models` (Layer 0).

### 5. `registerMCPTurnContextWithTTL` / `unregisterMCPTurnContext` (CLI only)

Currently in `mcp_hooks.go`, forwarding to `runtimemcp.*`.

Resolution: **Callback**. CLI runtime gets an `MCPHook` function field.

### 6. `asString` (CLI only, 7 call sites)

Currently in `helpers.go`. A 7-line type-switch utility.

Resolution: **Duplicate in `runtime/llm`**. It's 7 lines, generic, and unlikely to
diverge. `runtime/llm` should not import `runtime/` for a string coercion helper.

### 7. `TurnPersistence`, `ConversationPersistence`, `AgentTurnRecord`, `ConversationRecord`

Already moved to `runtime/llm/persistence.go`. The runtime root has aliases in
`aliases.go`. The runtimes currently reference the aliases — after moving, they
reference the types directly (same package).

No action needed.

---

## Moves

### Move 1: Add BudgetHooks, MCPHook, and UsageTokens to `runtime/llm`

Create `runtime/llm/budget_hooks.go`:

```go
package llm

import "context"

// UsageTokens records token consumption for a single LLM turn.
type UsageTokens struct {
    InputTokens  int
    OutputTokens int
    Model        string
}

// BudgetHooks provides budget enforcement without importing the runtime root.
// Nil fields are treated as no-ops.
type BudgetHooks struct {
    LockScope   func(scopeKey string) func()
    RecordUsage func(ctx context.Context, verticalID, agentID, runtimeMode string,
                     usage UsageTokens, exact bool, meta any) error
    IsEmergency func(verticalID string) bool
    IsThrottle  func(verticalID string) bool
}

// MCPHook registers a turn context for MCP tool execution.
// Returns a context token and an unregister function.
// Used only by CLI runtime.
type MCPHook func(ctx context.Context, ttl time.Duration) (token string, unregister func())
```

Add nil-safe convenience methods on BudgetHooks (unexported):

```go
func (h BudgetHooks) lockScope(key string) func() {
    if h.LockScope == nil { return func() {} }
    return h.LockScope(key)
}
// ... same for recordUsage, isEmergency, isThrottle
```

Also move `budgetExecutionScopeKey` here — it only uses `models.AgentConfig`.

In runtime root `budget.go`, replace `UsageTokens` with:
```go
type UsageTokens = llm.UsageTokens
```

This is additive. No files deleted.

Verification: `go build ./... && go test ./internal/runtime/...`

---

### Move 2: Add `asString` to `runtime/llm`

Create or add to `runtime/llm/helpers.go`:

```go
package llm

import "fmt"

func asString(v any) string {
    switch t := v.(type) {
    case string:  return t
    case nil:     return ""
    default:      return fmt.Sprintf("%v", t)
    }
}
```

Unexported — only used within the package by the CLI runtime after Move 4.

This is additive. No files changed outside `runtime/llm`.

Verification: `go build ./...`

---

### Move 3: Move `llm_api.go` to `runtime/llm/api_runtime.go`

Steps:
1. Copy `llm_api.go` to `runtime/llm/api_runtime.go`
2. Change `package runtime` to `package llm`
3. Remove the `llm` import alias — types are now in the same package
4. Replace all `llm.X` with bare `X` (Session, Message, ToolDefinition, etc.)
5. Replace `*BudgetTracker` field with `BudgetHooks`
6. Replace budget calls:
   - `r.budget.LockExecutionScope(scopeKey)` -> `r.budget.lockScope(scopeKey)`
   - `r.budget.IsEmergency(verticalID)` -> `r.budget.isEmergency(verticalID)`
   - `r.budget.IsThrottle(verticalID)` -> `r.budget.isThrottle(verticalID)`
   - `r.budget.RecordLLMUsage(ctx, ...)` -> `r.budget.recordUsage(ctx, ...)`
7. Replace `ActorFromContext` -> `runtimetools.ActorFromContext`
   (add import for `runtimetools "empireai/internal/runtime/tools"`)
8. `TurnPersistence`, `ConversationPersistence`, `AgentTurnRecord`,
   `ConversationRecord`, `UsageTokens` — all now same package, use bare names
9. `BuildSessionSummary` — already same package
10. Update constructor signature:
    ```go
    func NewAnthropicAPIRuntime(cfg *config.Config, sessions sessions.Registry,
        lockOwner string, turns TurnPersistence, conversations ConversationPersistence,
        budget BudgetHooks) *AnthropicAPIRuntime {
    ```
11. Delete `llm_api.go` from runtime root
12. Add to runtime root `aliases.go`:
    ```go
    type AnthropicAPIRuntime = runtimellm.AnthropicAPIRuntime
    var NewAnthropicAPIRuntime = runtimellm.NewAnthropicAPIRuntime
    ```

Verification: `go build ./... && go test ./internal/runtime/...`

---

### Move 4: Move `llm_cli.go` to `runtime/llm/cli_runtime.go`

Same pattern as Move 3, plus MCP and asString.

Steps:
1. Copy `llm_cli.go` to `runtime/llm/cli_runtime.go`
2. Change `package runtime` to `package llm`
3. Remove `llm` import alias, replace `llm.X` with bare `X`
4. Replace `*BudgetTracker` with `BudgetHooks` (same as Move 3)
5. Add `mcpHook MCPHook` field to `ClaudeCLIRuntime`
6. Replace MCP calls:
   ```go
   // Before:
   contextToken = registerMCPTurnContextWithTTL(ctx, r.mcpContextTokenTTL(ctx))
   defer func() { unregisterMCPTurnContext(contextToken) }()

   // After:
   if r.mcpHook != nil {
       token, unregister := r.mcpHook(ctx, r.mcpContextTokenTTL(ctx))
       contextToken = token
       defer func() { unregister() }()
   }
   ```
7. `asString` — now in same package (from Move 2), no change needed
8. Replace `ActorFromContext` -> `runtimetools.ActorFromContext`
9. Update constructor to accept `BudgetHooks` and `MCPHook`:
   ```go
   func NewClaudeCLIRuntime(
       cfg *config.Config, sessions sessions.Registry, lockOwner string,
       turns TurnPersistence, budget BudgetHooks,
       workspaces workspace.Resolver, conversations ConversationPersistence,
       mcpHook MCPHook,
   ) *ClaudeCLIRuntime {
   ```
10. Delete `llm_cli.go` from runtime root
11. Add to runtime root `aliases.go`:
    ```go
    type ClaudeCLIRuntime = runtimellm.ClaudeCLIRuntime
    var NewClaudeCLIRuntime = runtimellm.NewClaudeCLIRuntime
    var ErrClaudeAuthRequired = runtimellm.ErrClaudeAuthRequired
    ```

Verification: `go build ./... && go test ./internal/runtime/...`

---

### Move 5: Move `llm_runtime.go` to `runtime/llm/factory.go`

Steps:
1. Copy `llm_runtime.go` to `runtime/llm/factory.go`
2. Change `package runtime` to `package llm`
3. Remove `llm` import alias, replace `llm.X` with bare `X`
4. Change `RuntimeFactory.Budget` field from `*BudgetTracker` to `BudgetHooks`
5. Add `MCPHook MCPHook` field to `RuntimeFactory`
6. Update `Build()`:
   ```go
   case "api":
       return NewAnthropicAPIRuntime(f.Cfg, f.Sessions, f.LockOwner,
           f.Turns, f.Conversations, f.Budget), nil
   case "cli_test":
       return NewClaudeCLIRuntime(f.Cfg, f.Sessions, f.LockOwner,
           f.Turns, f.Budget, f.Workspaces, f.Conversations, f.MCPHook), nil
   ```
7. `NoopRuntime` and `defaultLockOwner` move with it — no deps on runtime root
8. Delete `llm_runtime.go` from runtime root
9. Add to runtime root `aliases.go`:
   ```go
   type RuntimeFactory = runtimellm.RuntimeFactory
   type NoopRuntime = runtimellm.NoopRuntime
   ```

Verification: `go build ./... && go test ./internal/runtime/...`

---

### Move 6: Update wiring in `cmd/empire/main.go`

Update runtime factory construction:

```go
budgetHooks := llm.BudgetHooks{
    LockScope:   budget.LockExecutionScope,
    RecordUsage: budget.RecordLLMUsage,
    IsEmergency: budget.IsEmergency,
    IsThrottle:  budget.IsThrottle,
}

mcpHook := llm.MCPHook(func(ctx context.Context, ttl time.Duration) (string, func()) {
    token := runtimemcp.RegisterTurnContextWithTTL(ctx, ttl)
    return token, func() { runtimemcp.UnregisterTurnContext(token) }
})

factory := runtime.RuntimeFactory{
    Cfg:           cfg,
    Sessions:      sessions,
    Turns:         turns,
    Conversations: conversations,
    Budget:        budgetHooks,
    LockOwner:     lockOwner,
    Workspaces:    workspaces,
    MCPHook:       mcpHook,
}
```

Check if `main.go` constructs runtimes directly (not just via RuntimeFactory).
If so, update those call sites too.

Verification: `go build ./... && go test ./...` (full project)

---

### Move 7: Move tests

| Test File | Action |
|-----------|--------|
| `llm_api_test.go` | Move to `runtime/llm/` |
| `llm_cli_test.go` | Move to `runtime/llm/` |
| `llm_cli_runtime_test.go` | Move to `runtime/llm/` |
| `llm_cli_recovery_checkpoint_test.go` | Move to `runtime/llm/` |
| `llm_runtime_factory_test.go` | Move to `runtime/llm/` |
| `llm_runtime_more_test.go` | Move to `runtime/llm/` |

For each:
1. Change `package runtime` to `package llm` (or `package llm_test`)
2. Remove `runtime` import, replace `runtime.X` with bare `X` or `llm.X`
3. Replace `*BudgetTracker` construction in tests with `BudgetHooks{}` stubs
4. Replace MCP setup in tests with `MCPHook` stubs

Tests that test the full runtime wiring (canned E2E, integration) stay in
`runtime/` — they'll use the aliases.

Verification: `go build ./... && go test ./...`

---

### Move 8: Cleanup

1. Delete forwarding wrappers in `event_turn_context.go` if no longer needed
   by remaining root files (check with grep first)
2. Remove any dead aliases from `aliases.go` (aliases that were only used by
   the moved files)
3. Remove `budgetExecutionScopeKey` from `budget.go` (moved in Move 1)

Verification: `go build ./... && go test ./...`

---

## What does NOT move

| File | LOC | Reason |
|------|-----|--------|
| `agent_llm.go` | 727 | Agent orchestration: prompt resolution, tool surface composition, event formatting, contract enforcement, emitted-event recording. These are agent behavior concerns, not LLM runtime mechanics. If extracted later, target would be `runtime/agents`, not `runtime/llm`. |
| `manager.go` | 492 | Defines Agent interface, AgentFactory, AgentManager. Orchestration abstractions that belong at the coordinator level. |

---

## Final State

### `runtime/llm` after extraction (~2,510 LOC)

| File | LOC | Contents |
|------|-----|----------|
| types.go | 44 | Session, Message, ToolDefinition, Runtime, Response, ToolCall |
| conversation.go | 322 | Conversation, ToolExecutor, ConversationMode |
| persistence.go | 39 | AgentTurnRecord, TurnPersistence, ConversationRecord, ConversationPersistence |
| session_rotation.go | 150 | Rotation helpers (already moved) |
| budget_hooks.go | ~80 | BudgetHooks, MCPHook, UsageTokens, budgetExecutionScopeKey |
| helpers.go | ~10 | asString |
| api_runtime.go | ~498 | AnthropicAPIRuntime |
| cli_runtime.go | ~1,021 | ClaudeCLIRuntime |
| factory.go | ~62 | RuntimeFactory, NoopRuntime, defaultLockOwner |

### `runtime/` root after extraction (~5,495 LOC, 21 prod files)

Files removed: `llm_api.go` (498), `llm_cli.go` (1,021), `llm_runtime.go` (62)
New aliases added: ~20 LOC

### `runtime/llm` dependency chain

```
runtime/llm
  +-- empireai/internal/config            (Layer 0)
  +-- empireai/internal/models            (Layer 0)
  +-- empireai/internal/runtime/sessions  (Layer 0)
  +-- empireai/internal/runtime/tools     (Layer 3, for ActorFromContext only)
  +-- empireai/internal/runtime/workspace (Layer 1, CLI only)
```

No import of `runtime/` root. No import of `runtime/bus`, `runtime/pipeline`,
`runtime/mcp`, or `runtime/contracts`. Narrow dependency surface.

---

## Rules for the implementer

1. One move at a time. `go build ./...` after each.
2. `agent_llm.go` does NOT move. Do not touch it.
3. `Agent`, `AgentFactory`, `BoardInteractiveAgent` do NOT move. They stay in `manager.go`.
4. When moving a file, grep the full project for direct references before deleting:
   `grep -rn 'runtime\.TypeName' --include="*.go" .`
5. The `BudgetHooks` convenience methods (lockScope, recordUsage, etc.) are
   intentionally unexported. They add nil-safety.
6. The `asString` in `runtime/llm/helpers.go` is a deliberate duplication, not
   a DRY violation. It avoids an import of `runtime/` for 7 lines.
7. `go vet ./...` must pass after each move.
8. After all moves, run full test suite: `go test ./...`

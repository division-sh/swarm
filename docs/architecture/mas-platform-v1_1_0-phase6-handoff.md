# Phase 6: Permission Enforcement

**Date:** 2026-03-14 (updated 2026-03-14 — corrections applied)
**Prerequisite:** None (independent of Phase 5)
**Risk level:** HIGH — changes authorization for all tool calls
**Scope:** 2 gaps (G-04, G-08), ~120 lines across 5 files

---

## Overview

The authorizer has 4 tiers (universal → emit_allowed → actor_config → default_allow) but NO permission tier. Any agent can call any tool. The spec defines 13 platform permissions enforced at tool execution time (platform-spec.yaml:343-356).

**Spec-defined permissions:** `agent_fire`, `agent_hire`, `agent_reconfigure`, `approve_spend`, `configure_routing`, `create_flow_instance`, `human_task_decide`, `human_task_request`, `mailbox_send`, `message_all`, `message_domain`, `message_peers`, `schedule`.

---

## Current Architecture

**Authorization path** (`internal/runtime/tools/authorizer.go`):
- `Authorize()` at line 47 receives `actor models.AgentConfig` (not an ActorContext — the agent config from `internal/runtime/core/actors/agent_config.go`)
- `classifyToolAuthorization(actor models.AgentConfig, toolName string)` at line 84

```
classifyToolAuthorization(toolName, actor):
  1. IsUniversal(toolName)           → allow  (hardcoded list: agent_message, mailbox_send, etc.)
  2. IsEmitToolAllowedForRole(actor) → allow  (CommGraph emit permissions)
  3. extractAllowedToolsFromConfig() → allow  (actor.Config JSON "tools"/"allowed_tools")
  4. default_allow                   → allow  (fallback — everything passes)
  5. denied                          → deny   (unreachable today)
```

**Registered tools** (from `internal/runtime/tools/handler_registry.go`):
- Agent: `agent_message`, `schedule`, `configure_routing`, `agent_hire`, `agent_fire`, `agent_reconfigure`
- Mailbox: `mailbox_send`
- Entity: `get_entity`, `save_entity_field`, `create_entity`, `search_entities`, `query_metrics`
- Human: `human_task_request`, `human_task_decide`
- Infra: `nginx_reload`, `systemd_control`, `certbot_execute`

**Problem:** Tier 4 (default_allow) means tiers 1-3 are decorative. There is no permission check.

---

## Target Architecture

```
classifyToolAuthorization(toolName, actor):
  1. IsUniversal(toolName)                    → allow
  2. toolRequiresPermission(toolName, actor)  → allow/deny  ← NEW
  3. IsEmitToolAllowedForRole(actor)          → allow
  4. extractAllowedToolsFromConfig()          → allow
  5. default_allow                            → allow
  6. denied                                   → deny
```

---

## Step 1: Add Permissions field to AgentConfig

**File:** `internal/runtime/core/actors/agent_config.go`

The authorizer receives `models.AgentConfig` directly. Add the field here:

```go
Permissions []string
```

**File:** `internal/runtime/contracts/workflow_contracts.go`

Find `AgentRegistryEntry` struct (~line 1905). Add:

```go
Permissions []string `yaml:"permissions" json:"permissions,omitempty"`
```

---

## Step 2: Resolve permissions_bundle + explicit permissions

The spec says (platform-spec.yaml:357-359): "An agent may declare permissions_bundle and/or explicit permissions. If both are present, the explicit permissions list EXTENDS the bundle. Duplicates are deduplicated. The bundle is expanded first, then explicit permissions are added."

### Step 2a: Parse permissions_bundle from agents.yaml

**File:** `internal/runtime/contracts/workflow_contracts.go`

Add to `AgentRegistryEntry`:
```go
PermissionsBundle string `yaml:"permissions_bundle" json:"permissions_bundle,omitempty"`
```

### Step 2b: Load permission_bundles from policy.yaml

**File:** `internal/runtime/contracts/workflow_contracts.go` (or wherever policy.yaml is parsed)

Permission bundles are defined in the product's `policy.yaml` under a top-level `permission_bundles` key. Example from the Empire policy:

```yaml
permission_bundles:
  coordinator:
    description: Full control
    permissions: [agent_hire, agent_fire, agent_reconfigure, configure_routing, approve_spend, message_all, mailbox_send, human_task_request]
  worker:
    description: Execution agent
    permissions: [message_peers]
```

Ensure the contract bundle parses `permission_bundles` from policy.yaml into a map. Check if `WorkflowContractBundle` already has a `Policy` or `PolicyConfig` field that captures this. If not, add:

```go
type PermissionBundle struct {
    Description string   `yaml:"description"`
    Permissions []string `yaml:"permissions"`
}
```

And parse `permission_bundles` from policy.yaml into `map[string]PermissionBundle`.

### Step 2c: Resolve effective permissions during agent config building

**File:** `internal/runtime/manager/flow_activation.go` (or wherever `buildFlowAgentConfig` is)

When building agent config, resolve the effective permission list:

```go
func resolveAgentPermissions(entry AgentRegistryEntry, bundles map[string]PermissionBundle) []string {
    var perms []string

    // Bundle first
    if entry.PermissionsBundle != "" {
        if bundle, ok := bundles[entry.PermissionsBundle]; ok {
            perms = append(perms, bundle.Permissions...)
        }
    }

    // Explicit permissions extend
    perms = append(perms, entry.Permissions...)

    // Deduplicate
    return dedupStrings(perms)
}
```

Then:
```go
cfg.Permissions = resolveAgentPermissions(registryEntry, policyBundles)
```

---

## Step 3: Define tool→permission mapping

**File:** `internal/runtime/tools/authorizer.go` (or a new `internal/runtime/tools/permissions.go`)

Map actual registered tool names to spec-defined permissions. Only tools that exist in the handler registry and have a matching spec permission are included:

```go
// toolPermissionRequirements maps registered tool names to required platform permissions.
// Only tools that actually exist in the handler registry appear here.
// Spec reference: platform-spec.yaml:343-356
var toolPermissionRequirements = map[string]string{
    // Agent management
    "agent_fire":        "agent_fire",
    "agent_hire":        "agent_hire",
    "agent_reconfigure": "agent_reconfigure",
    "configure_routing": "configure_routing",

    // Messaging (note: agent_message and mailbox_send are universal tools,
    // but messaging permissions gate the message scope, not the tool call itself.
    // message_all/message_domain/message_peers are enforced in the message
    // delivery path, not at tool authorization time.)

    // Human tasks
    "human_task_request": "human_task_request",
    "human_task_decide":  "human_task_decide",

    // Scheduling
    "schedule": "schedule",

    // Infra (these replace the hardcoded control-plane role check in G-08)
    "nginx_reload":    "system_admin",
    "systemd_control": "system_admin",
    "certbot_execute": "system_admin",
}
```

**Note on `system_admin`:** This is NOT in the spec's 13 platform permissions. It replaces the hardcoded `control-plane` role check. Two options:
1. Add `system_admin` as a workflow extension permission (spec allows this: "Workflows may define additional permissions beyond the platform set")
2. Or keep the infra tools out of this map and handle them separately

**Note on `approve_spend`, `create_flow_instance`:** These are spec-defined permissions but `approve_spend` has no registered tool yet, and `create_flow_instance` is not yet an agent-callable tool (G-17, Phase 7). Add them to this map when their tools are registered.

---

## Step 4: Add permission tier to authorizer

**File:** `internal/runtime/tools/authorizer.go`

In `classifyToolAuthorization()` (~line 84), add the permission check between the universal check and emit_allowed check. The function receives `actor models.AgentConfig`, so check permissions directly on AgentConfig:

```go
// After universal check, before emit_allowed check:
if requiredPerm, ok := toolPermissionRequirements[toolName]; ok {
    if agentHasPermission(actor, requiredPerm) {
        return toolAuthorizationPermission, nil
    }
    return toolAuthorizationDenied, fmt.Errorf("agent %s lacks permission %q for tool %s", actor.ID, requiredPerm, toolName)
}
```

Add the new authorization tier constant:

```go
const toolAuthorizationPermission = "permission"
```

Add permission check helper (operates on `models.AgentConfig`, not ActorContext):

```go
func agentHasPermission(agent models.AgentConfig, perm string) bool {
    for _, p := range agent.Permissions {
        if p == perm {
            return true
        }
    }
    return false
}
```

---

## Step 5: Enable boot step 10

**File:** `cmd/mas/main.go` (~line 404)

Replace the current step 10 placeholder:
```go
{10, "validate_permissions", "permission validation is outside Tranche A scope; ..."},
```

With actual validation. The validation should:
1. For each agent in the contract bundle, check that its `permissions` list only contains known permission names (the 13 from platform-spec.yaml:343-356, plus any declared in policy.yaml workflow extensions)
2. For each tool in the agent's `tools_tier2` list, if the tool requires a permission (per `toolPermissionRequirements`), verify the agent has it
3. **Error (not warning)** if a contract declares a gated tool without the required permission — this is a contract authoring error that should be caught at boot, not silently ignored at runtime

```go
permErrors := validateAgentPermissions(source)
if len(permErrors) > 0 {
    return fmt.Errorf("boot step 10: %d permission errors: %v", len(permErrors), permErrors)
}
permMsg := fmt.Sprintf("validated %d agents, all permissions satisfied", agentCount)
```

---

## Step 6: Remove hardcoded control-plane role check (G-08)

**File:** `internal/runtime/tools/executor_system.go`

Three hardcoded checks exist at lines 14, 27, and 70:
```go
if actor.Role != "control-plane" {
    return nil, fmt.Errorf("<tool> is restricted to control-plane")
}
```

Remove all three. The permission mapping in Step 3 maps `nginx_reload`, `systemd_control`, `certbot_execute` → `system_admin` permission, so the authorizer handles enforcement.

**Important:** Verify these are the only files with `control-plane` checks:
```bash
grep -rn 'control-plane' internal/runtime/tools/
```

---

## Step 7: Test

**File:** `internal/runtime/tools/authorizer_permission_test.go` (new)

Test cases:
1. Agent WITH `agent_fire` permission → `agent_fire` tool → allowed
2. Agent WITHOUT `agent_fire` permission → `agent_fire` tool → denied
3. Agent WITH `system_admin` permission → `nginx_reload` tool → allowed
4. Agent WITHOUT `system_admin` permission → `nginx_reload` tool → denied
5. Universal tool (`agent_message`) → allowed regardless of permissions
6. Tool with no permission requirement (e.g. `get_entity`) → falls through to existing tiers
7. Boot validation: agent with `agent_fire` in tools_tier2 but no `agent_fire` permission → boot error

---

## Delivery checklist

- [ ] `Permissions []string` on `AgentConfig` (`internal/runtime/core/actors/agent_config.go`)
- [ ] `Permissions []string` and `PermissionsBundle string` on `AgentRegistryEntry`
- [ ] `permission_bundles` parsed from policy.yaml into `map[string]PermissionBundle`
- [ ] `resolveAgentPermissions()` expands bundle first, then explicit, deduplicates
- [ ] Resolved permissions copied to `AgentConfig.Permissions` during config building
- [ ] `toolPermissionRequirements` map defined with actual registered tool names only
- [ ] Permission tier added to `classifyToolAuthorization()` between universal and emit_allowed
- [ ] `agentHasPermission()` helper on `models.AgentConfig`
- [ ] Boot step 10 performs real validation — **errors on contract mismatch**
- [ ] Hardcoded `control-plane` role checks removed from `executor_system.go` (lines 14, 27, 70)
- [ ] Test coverage for allow and deny paths
- [ ] All existing tests pass: `go test ./... -count=1 -timeout 120s`

---

## What NOT to do

- Do NOT invent an ActorContext permissions path — the authorizer already receives `models.AgentConfig`, put `Permissions` there
- Do NOT skip `permissions_bundle` — agents using bundles (e.g. `permissions_bundle: worker`) will silently have no permissions without it
- Do NOT include tools in the permission map that don't exist in the handler registry (no `terminate_flow`, `budget_allocate`, etc.)
- Do NOT remove the existing tiers (universal, emit_allowed, actor_config) — add permission as a new tier
- Do NOT make permissions required on all agents — agents without permissions in their contract should still work (they just can't call permission-gated tools)
- Do NOT change the tool executor's dispatch logic — only the authorization check
- Do NOT use `log.Printf` — use `slog` for all new code

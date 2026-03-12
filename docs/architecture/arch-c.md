# MAS Platformization: Architectural Plan (Arch-C)

This document defines the prioritized architectural changes required to extract the generic MAS (Multi-Agent System) platform from the Empire product logic. The goal is a platform where all behavior is derived from declarative contracts (`package.yaml`, `nodes.yaml`, etc.), and a second product can be booted without modifying generic Go code.

## 1. Unified Flow Discovery & Registry
**Priority: CRITICAL**

### Current State
The loader is a hybrid. It searches for `nodes.yaml` but falls back to `nodes-empire.yaml` or `workflow-empire.yaml`. Flow discovery is partially implemented but is neither recursive nor the authoritative source of truth for entity addressing.

### Spec Requirement
Recursive discovery starting from a root `package.yaml`. All addressable entities (nodes, agents, events) must be identified by hierarchical URIs (e.g., `empire://discovery/scanner-node`).

### Required Changes
- **Recursive Walker**: Rewrite `internal/runtime/contracts/workflow_contracts.go` to implement the `walk_flow_tree` algorithm.
- **URI Registry**: Implement a global registry that maps URIs to entity definitions, resolving local names to absolute paths at boot.
- **Cleanup**: Deprecate and remove all legacy file fallback logic (`-empire` suffixes).

### Rationale
This is the foundational change. All other features (routing, validation, sharding) depend on a consistent way to locate and address components across nested flows.

---

## 2. Generated Entity Persistence
**Priority: HIGH**

### Current State
The `verticals` table is a massive, product-specific leak in the database layer. It is defined by handwritten DDL in `ddl-canonical.sql`, and generic runtime code contains hardcoded references to its columns.

### Spec Requirement
The platform must derive database DDL from the `entity_schema` declared in `package.yaml`. It must provide auto-generated, typed persistence tools (`get_entity`, `save_entity_field`, `search_entities`, `query_metrics`) to agents.

### Required Changes
- **DDL Generator**: Implement a utility that reads `entity_schema` and generates the necessary SQL for tables and indexes.
- **Generic EntityStore**: Replace the hardcoded `VerticalsStore` with a generic store that uses the generated schema for data access.
- **Persistence Tools**: Implement the 4 auto-generated tools as platform built-ins, ensuring they are available to all agents based on their permissions.

### Rationale
A platform that requires a specific `verticals` table cannot support a second product. This change decouples the storage layer from the product domain.

---

## 3. Declarative Handler Engine Completion
**Priority: HIGH**

### Current State
`handler_engine_exec.go` implements basic declarative logic but lacks full spec compliance. It supports only a single `compute` operation, a single `on_complete` rule, and is missing the `fan_out` primitive. Much of Empire's logic is still implemented in Go coordinators.

### Spec Requirement
100% declarative system nodes. The engine must support ordered `on_complete` branch lists, multiple `compute` steps, and complex `fan_out` logic for sharding.

### Required Changes
- **Handler Schema Update**: Update `SystemNodeEventHandler` to support list-based `on_complete` and `compute` fields.
- **Primitive Implementation**: Implement `fan_out` and `accumulate` (with completion modes) in the engine.
- **CEL Integration**: Fully integrate Google CEL (Common Expression Language) for all guards and branch conditions to replace hardcoded logic.

### Rationale
This allows the deletion of thousands of lines of product-specific Go code (e.g., `coordinator_scoring.go`) by moving that logic into the product's YAML contracts.

---

## 4. Generic Agent Lifecycle & Sharding
**Priority: MEDIUM**

### Current State
Sharding and parallelism are managed by a specialized `ScanCampaignManager` background loop that is tightly coupled to the Empire "scan" domain.

### Spec Requirement
Parallelism is an orchestrated behavior where system nodes use the `fan_out` primitive and the `agent_hire` tool to partition and execute workloads.

### Required Changes
- **Lifecycle Tools**: Implement `agent_hire` and `agent_fire` as platform-builtin tools.
- **Sharding Orchestration**: Refactor the engine to support tracking fanned-out agent sessions and their completion via the `accumulate` primitive.
- **Refactor Scan**: Move the "scan campaign" logic out of the runtime and into a declarative flow in the Empire product module.

### Rationale
Simplifies the runtime by removing background state-management loops and replacing them with universal, event-driven primitives.

---

## 5. Explicit Tool & Permission Model
**Priority: MEDIUM**

### Current State
Permissions are checked sporadically and often rely on hardcoded "manager" role IDs or "admin" flags in the code.

### Spec Requirement
All tool calls and message deliveries must be validated against the agent's `permissions` list at runtime. Universal tools are auto-granted.

### Required Changes
- **Registry Fields**: Add `permissions` and `permissions_bundle` to the agent registry.
- **Enforcement Middleware**: Implement a central enforcement layer in the tool executor that rejects calls if the agent lacks the required permission.
- **Auto-generated Emit Tools**: Automatically generate `emit_{event_name}` tools for agents based on their `emit_events` contract.

### Rationale
Essential for security and isolation, especially as the system grows to support multiple independent flows and products.

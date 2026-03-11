# MAS Platform — Product Brief

## One-liner

A declarative, contract-driven orchestration engine for multi-agent systems that treats LLMs as interchangeable reasoning modules inside a deterministic industrial pipeline.

## The Problem

Current AI agent frameworks (CrewAI, AutoGen, LangGraph) let LLMs manage their own routing, state, and coordination. This works for demos. It breaks in production:

- **Non-deterministic routing.** An LLM "manager agent" decides who acts next. Run it twice, get different results. Can't debug, can't audit, can't reproduce.
- **No crash recovery.** Process dies mid-agent-turn. State is gone. Start over.
- **Context window exhaustion.** Add 10 agents to a shared chat. The LLM loses the plot by agent 5.
- **No composition.** Want to reuse a scoring pipeline in a different project? Refactor all the Python.
- **No separation of concerns.** Business logic, orchestration logic, and agent reasoning are tangled in the same code.

## The Solution

MAS Platform separates what agents DO (reason) from what the system does (orchestrate). Agents think. The platform handles everything else.

**Contracts, not code.** Every flow, agent, event, handler, state machine, tool, and policy is declared in YAML. The engine reads contracts and executes them. No product-specific code hooks.

**Deterministic orchestration.** System nodes route events, advance state, set gates, accumulate data, fan out work, and enforce guards — all via a dependency-graph execution model with atomic transactions. Same input, same output, every time.

**Flow composition.** Workflows are self-contained packages with typed inputs and outputs (pins). Plug a scoring flow into any project. Swap it for a better version. No code changes.

**Agent isolation.** Agents operate in scoped sessions. They see only their subscribed events. They communicate only through validated event emission. A CEO agent talks to VP agents. VPs talk to workers. Workers never talk to each other. Managerial hierarchy as context management.

## How It Works

### Contracts declare everything

```yaml
# A system node handler — no code, just YAML
research.completed:
  sets_gate: g1_research
  data_accumulation:
    writes: [business_brief, research_context]
  advances_to: mvp_speccing
  emits: spec.requested
```

The engine reads this and executes: set the gate, write data, advance state, emit the next event. All in one atomic transaction. If anything fails, everything rolls back.

### Dependency graph execution

Handler fields execute in causal order:

```
guard → accumulate → compute → on_complete/rules
  → {advances_to, sets_gate, data_accumulation}    ← independent, atomic
    → payload_transform → emits → action
```

Independent steps can run in any order. Everything commits atomically. Guards see pre-handler state. The next handler sees post-commit state.

### Flow packages are composable units

```
my-flow/
  package.yaml     — identity, pins, dependencies
  schema.yaml      — states, gates, required agents
  nodes.yaml       — system nodes with handlers
  events.yaml      — event schemas
  agents.yaml      — agent definitions
  tools.yaml       — available tools
  policy.yaml      — configuration values
  prompts/         — agent behavioral instructions
```

Every flow is self-contained. It declares what it needs (input pins) and what it produces (output pins). The platform wires them together at boot.

### Dynamic scaling via flow instances

Static flows exist at boot. Template flows create instances at runtime:

```yaml
flows:
  - id: scoring
    flow: scoring
    mode: static       # always one instance

  - id: operating
    flow: operating
    mode: template     # created on demand
```

When a vertical gets approved, the platform creates a new operating flow instance with its own agents, nodes, and state. Wildcard subscriptions (`operating/*/event`) observe all instances.

## Platform Capabilities

### Handler primitives
guard, advances_to, sets_gate, clear_gates, data_accumulation, emits, rules, on_complete, accumulate, compute, fan_out, filter, reduce, count, group_by, query, clear, payload_transform, record_evidence, action (create_flow_instance)

### Expression language
CEL (Common Expression Language) for all conditions, guards, and filters. Strongly typed, non-Turing-complete, safe for policy evaluation.

### Error model
3-retry exponential backoff for transient failures. Dead letter after exhaustion. Chain depth limit of 50 prevents infinite loops. Guard failures have explicit actions: reject, discard, kill, escalate.

### Timer model
Durable timers with start-on/cancel-on lifecycle. Persisted across restarts. Supports one-shot and recurring patterns.

### Boot verification
11 checks run at startup. Payload field coverage, required agent matching, handler field compliance, tool resolution, CEL parse validation, deprecated field detection. Errors abort boot. Warnings log.

### Agent session management
Task mode (stateless), session mode (persistent), session-per-entity mode. Emit-tool auto-generation from agent contracts. Universal tools (agent_message, mailbox_send) auto-granted.

## Key Design Principles

**"The agents are smart — the wiring is broken."** Every production failure in multi-agent systems traces to event delivery, payload mismatches, or state corruption — not agent intelligence. The platform makes wiring deterministic and auditable.

**Validate before infrastructure.** Start with contracts. Prove the pipeline works. Add infrastructure when needed.

**Deterministic orchestration over LLM-managed routing.** The LLM reasons within its session. The platform handles everything outside that session.

**Zero product hooks.** If the engine needs custom code to run a specific application, the abstraction is wrong. Fix the abstraction.

## Competitive Position

| Dimension | CrewAI / AutoGen | LangGraph | MAS Platform |
|-----------|-----------------|-----------|-------------|
| Routing | LLM-managed (non-deterministic) | Code-defined state machine | Contract-defined dependency graph |
| Crash recovery | None | Manual checkpointing | Atomic transactions + event replay |
| Composition | Code refactoring | Python sub-graphs | YAML flow packages with pins |
| Agent isolation | Shared chat | Shared state dict | Scoped sessions, hierarchical addressing |
| Scalability | Single process | Single process | Per-entity parallelism, dynamic flow instances |
| Expression language | N/A | Python | CEL |
| Audit trail | Logs | Logs | Event store + entity state history |

## Numbers (EmpireAI — first application)

- 7 system nodes, 59 handlers, all declarative
- 29 agents across 4 flows + root
- 196 events with typed payload schemas
- 21 tools (platform + workflow)
- 105 compliance tests across 10 tiers
- 0 product code hooks

## Target Users

**Phase 1 (now):** Internal — powering EmpireAI, an autonomous AI venture studio.

**Phase 2 (after first end-to-end vertical):** Open-source core orchestrator. Self-hosted via GitHub. Enterprise teams that find CrewAI/AutoGen too "magical and flaky" for production.

**Phase 3:** Hosted platform with visual workflow canvas (Tauri + React Flow). Flow marketplace with building-block packages. ~20-30% platform cut.

## Status

Platform v1.1.0 — specification complete. Engine spec, handler model, flow composition, dynamic instances, timers, error model, boot verification, CEL integration, agent session management all specified.

Implementation: Go orchestrator at Phase 11 (handler-first execution for proven-safe subset). Full handler-first execution in progress.

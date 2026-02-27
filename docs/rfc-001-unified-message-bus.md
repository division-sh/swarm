# RFC: Unified Message Bus & System Nodes

**RFC ID:** RFC-001  
**Status:** Draft — requesting implementer feedback before spec changes  
**Author:** Spec Writer (Claude)  
**Date:** 2026-02-26  
**Spec Version:** Proposed for v2.0.37  
**Depends On:** v2.0.36 reconciliation complete, deferred event bug fix merged

---

## 1. Summary

Replace the current event/message/task communication triad and the interceptor middleware pattern with:

1. A **unified message bus** — one communication primitive (structured messages) replacing events, agent_message, and tasks
2. **System nodes** — deterministic, non-LLM components that participate in the message bus alongside agent nodes
3. **Declarative state machines** — YAML-defined pipeline logic replacing the Go interceptor switch statement

This RFC exists because we need implementer feedback before rewriting ~3,000 lines of spec. The deferred event interception bug exposed a fundamental architectural issue that warrants a deeper fix than the current minimal patch.

---

## 2. Problem Statement

### 2.1 The deferred event bug

The factory scoring pipeline was dead in production. `source.scraped` events produced `vertical.discovered` as deferred events, but deferred events bypass `runInterceptors`. So `handleVerticalDiscovered` (which emits `scoring.requested`) never fired. Result: lots of `vertical.discovered`, zero `scoring.requested`.

The current fix emits `scoring.requested` in the same interceptor pass as `vertical.discovered`. This works but is a patch — any future pipeline extension that needs deferred event chaining will hit the same wall.

### 2.2 The interceptor complexity

The pipeline coordinator is a 26-case switch statement embedded as middleware in `EventBus.Publish`. It mixes:

- **Consumption** (passthrough=false): event doesn't reach subscribers
- **Passthrough** (passthrough=true): event reaches subscribers after coordinator bookkeeping
- **Deferred emission**: coordinator produces follow-on events within the same transaction
- **State machine logic**: gate checking, accumulation, cycle counting

This creates a component that is simultaneously a router, a state machine, and an event producer — all running inside the publish path of every event in the system.

### 2.3 The three-primitive confusion

The current communication model has three primitives:

- **Events** — broadcast, schema-implied, routed by EventBus
- **Messages** (agent_message) — directed, unstructured, point-to-point
- **Tasks** — structured work units with review cycles (factory only)

In practice, many "events" are actually directed orchestration messages (`scoring.requested` targets one agent), and many "messages" would benefit from schemas (the Analysis Agent's score response needs structured fields, not freeform text).

---

## 3. Proposed Architecture

### 3.1 Unified message primitive

One communication type replaces all three:

```go
type Message struct {
    ID          uuid.UUID
    Type        string              // e.g. "score_vertical_request", "build_complete"
    From        string              // sender node ID
    To          string              // recipient node ID, or "" for broadcast
    Schema      string              // schema ID for validation, or "" for freeform
    Payload     map[string]any      // structured fields (validated if schema set)
    Body        string              // freeform text (for agent conversations)
    VerticalID  uuid.UUID           // context propagation
    CreatedAt   time.Time
    Metadata    map[string]string
}
```

Three delivery modes through the same bus:

| Mode | To field | Schema | Use case |
|------|----------|--------|----------|
| Directed structured | specific node ID | present | Pipeline orchestration, structured results |
| Directed unstructured | specific node ID | absent | Agent-to-agent conversation |
| Broadcast structured | empty (subscribers) | present | Business state transitions |

### 3.2 Node types

Two types of participants on the message bus:

**Agent nodes** — LLM-powered. Have system prompts, model tiers, context windows. Receive messages, reason about them, produce output. Examples: Analysis Agent, Builder, QA, Support.

**System nodes** — Deterministic Go code. No LLM. Receive structured messages, execute logic, emit structured messages. Examples: Pipeline Coordinator, Router, Scheduler.

Both node types send and receive messages through the same bus. The bus doesn't know or care which type is on each end.

### 3.3 Pipeline coordinator as system node

The pipeline coordinator becomes a subscriber, not middleware. It subscribes to the message types it cares about and processes them like any other node.

```
Before (interceptor):
  Any event → EventBus.Publish → runInterceptors → maybe consume → maybe defer → fan out

After (subscriber):
  Any message → MessageBus.Publish → persist → fan out to ALL subscribers
  Pipeline coordinator is just one subscriber among many
```

When the coordinator needs work done, it sends a directed structured message to the appropriate agent. When the agent finishes, it publishes a structured result message. The coordinator subscribes to that result type and advances its state machine.

```
Pipeline Coordinator                     Analysis Agent
       |                                       |
       |-- score_vertical_request ----------->|
       |   {vertical_id, dimensions, rubric}  |
       |   (directed, structured)              |
       |                                       |
       |                                [LLM reasoning]
       |                                       |
       |<-- score_dimension_result ------------|
       |   {vertical_id, dimension,            |
       |    score: 0.82, confidence: 0.9}      |
       |   (broadcast, structured)             |
       |                                       |
 [state machine advances]                      |
```

### 3.4 Declarative state machines

Pipeline logic defined in YAML with six primitives: accumulate, branch, gate_check, loops with limits, parallel instances, and timers.

Example — scoring pipeline:

```yaml
system_node: scoring_pipeline
  instance_key: vertical_id

  state_machine:
    initial: waiting_for_discovery

    states:
      waiting_for_discovery:
        on:
          vertical_discovered:
            action: send score_vertical_request to analysis-agent
            for_each: dimensions from rubric_selection(message.mode)
            next: scoring_in_progress

      scoring_in_progress:
        accumulate: score_dimension_result
        until:
          all_of:
            - dimension_count == expected_dimensions
        then:
          compute: composite_score = weighted_average(accumulated.scores)
          branch:
            - when: composite_score >= 0.7
              action: send vertical_shortlisted to broadcast
              next: complete
            - when: composite_score >= 0.4 AND composite_score < 0.7
              action: send vertical_marginal to empire-coordinator
              next: complete
            - otherwise:
              action: send vertical_rejected to broadcast
              next: complete

      complete:
        terminal: true
```

Example — validation pipeline:

```yaml
system_node: validation_pipeline
  instance_key: vertical_id

  state_machine:
    initial: created

    states:
      created:
        on:
          vertical_shortlisted:
            actions:
              - send research_request to research-agent
              - send brand_request to brand-agent
            next: validating

      validating:
        gates:
          g1_research:
            set_by: research_completed
            reset_by: vertical_needs_more_data
          g2_spec:
            set_by: spec_approved
            reset_by: [cto_spec_revision_needed, spec_revision_needed]
          g3_cto:
            set_by: cto_spec_approved
            reset_by: cto_spec_revision_needed
          g4_brand:
            set_by: brand_candidates_ready
            reset_by: brand_revision_needed

        on:
          research_completed:
            set_gate: g1_research
            action: send spec_request to spec-agent

          spec_approved:
            set_gate: g2_spec
            action: send cto_review_request to factory-cto

          cto_spec_approved:
            set_gate: g3_cto

          cto_spec_revision_needed:
            increment: revision_count
            branch:
              - when: revision_count < 3
                reset_gates: [g2_spec, g3_cto]
                action: send spec_revision_request to spec-agent
              - otherwise:
                action: send vertical_killed {reason: "revision limit"}
                next: rejected

          brand_candidates_ready:
            set_gate: g4_brand

        gate_check:
          all_of: [g1_research, g2_spec, g3_cto, g4_brand]
          then:
            action: send vertical_ready_for_review to broadcast
            next: packaging

      packaging:
        on:
          vertical_approved:
            action: send opco_spinup_request to empire-coordinator
            next: complete
          vertical_killed:
            next: rejected

      complete:
        terminal: true
      rejected:
        terminal: true
```

### 3.5 State machine primitives (complete set)

| Primitive | Purpose | Example |
|-----------|---------|---------|
| `accumulate ... until` | Collect N messages before transitioning | Wait for all 5 scanner results |
| `branch: when/otherwise` | Conditional transitions on computed values | Score above/below threshold |
| `gate_check: all_of` | Multiple conditions that must all be satisfied | G1 AND G2 AND G3 AND G4 |
| `increment` + `when: count < limit` | Revision loops with cycle limits | Max 3 spec revisions |
| `instance_key` | Parallel state machines per entity | One pipeline per vertical |
| `on_timeout` | Time-based transitions | Stall detection after 48h |

---

## 4. What Changes

### 4.1 EventBus.Publish → MessageBus.Publish

```go
// Before
func (eb *EventBus) Publish(event Event) error {
    tx := eb.db.Begin()
    eb.persistEvent(tx, event)
    passthrough, deferred, err := eb.coordinator.Intercept(tx, event)
    // ... handle passthrough, deferred, fan-out
}

// After
func (mb *MessageBus) Publish(msg Message) error {
    tx := mb.db.Begin()
    mb.persistMessage(tx, msg)
    if msg.Schema != "" {
        mb.validateSchema(msg)  // reject malformed structured messages
    }
    tx.Commit()
    mb.fanOut(msg)  // deliver to all subscribers of msg.Type
}
```

No interceptor step. No deferred events. No passthrough logic. Persist, validate, fan out. The pipeline coordinator receives messages via subscription like everyone else.

### 4.2 Event catalog → Message catalog

Fields change:

| v2.0.36 field | v2.0.37 field | Notes |
|---------------|---------------|-------|
| `emitter` | `from` | Who sends it |
| `consumer` | `subscribers` | Who receives it (list) |
| `intercepted` | REMOVED | No interceptor |
| `passthrough` | REMOVED | No interceptor |
| `routing` | `delivery` | `directed` or `broadcast` |
| `delivery_channel` | REMOVED | One bus handles all delivery |
| `payload` | `schema` | Typed field definitions |
| NEW | `category` | `workflow` or `orchestration` |

### 4.3 Agent-tools → Node manifest

New field per node:

```yaml
pipeline-coordinator:
  node_type: system
  implementation: internal/runtime/pipeline_scoring.go
  subscribes_to: [source_scraped, score_dimension_result, ...]
  produces: [score_vertical_request, vertical_shortlisted, ...]

analysis-agent:
  node_type: agent
  model_tier: sonnet
  subscribes_to: [score_vertical_request]
  produces: [score_dimension_result]
```

### 4.4 Crash recovery

Current crash recovery replays unreceipted events through the interceptor. New crash recovery replays unreceipted messages to the coordinator's subscription handler. The coordinator's state machine is idempotent — replaying a message that was already processed is a no-op because the state has already advanced past that transition.

```go
func (pc *PipelineCoordinator) RecoverFromCrash() error {
    pc.loadStateFromDB()
    // Find messages delivered to us but not receipted
    unprocessed := mb.GetUnreceiptedMessages(pc.ID())
    for _, msg := range unprocessed {
        pc.HandleMessage(msg)  // idempotent — checks state before transitioning
    }
}
```

---

## 5. What Stays the Same

- **Database tables.** The events table becomes a messages table (rename + add schema/to columns). Pipeline state tables (validation_pipelines, scan_accumulators, etc.) stay as-is.
- **Agent lifecycle.** Agents still spawn in Docker containers, still have system prompts, still use tools. They just receive structured messages instead of events.
- **Routing rules.** The routing_rules table still drives OpCo delivery. The mechanism is the same — the content of what's being routed changes from events to messages.
- **Holding agents.** Empire Coordinator, Factory CTO, etc. still exist and function the same way.
- **Factory pipeline logic.** The same state machines, same gates, same accumulation. Just expressed as YAML instead of Go switch cases.

---

## 6. What We Need From You (Implementer)

### 6.1 Feasibility assessment

Is this change compatible with the current Go runtime architecture? Specific concerns:

1. **Transaction atomicity.** The current interceptor runs inside the Publish transaction. Moving the coordinator to a subscriber means state machine updates happen in a separate transaction from message persistence. Is idempotent replay sufficient, or do you need single-transaction guarantees?

2. **Event consumption.** Currently, intercepted events don't reach agent subscribers. In the new model, all messages reach all subscribers. Are there messages that agents should NOT see? If so, is namespace separation (pipeline.* vs business.*) the right approach, or is there a simpler mechanism?

3. **Performance.** The interceptor processes events synchronously in the publish path. A subscriber-based coordinator processes messages asynchronously after fan-out. Does this introduce unacceptable latency in the pipeline, or is async processing fine?

4. **State machine YAML.** Is a YAML-defined state machine runtime something you'd want to build, or would you prefer the state machines stay in Go code with the YAML serving as documentation-only contracts?

### 6.2 The deferred event question

Two options on the table:

**Option A (this RFC):** Remove the interceptor entirely. Pipeline coordinator becomes a subscriber. Unified message bus. Declarative state machines. Big change, eliminates the deferred event problem systemically.

**Option B (minimal):** Keep the interceptor but add a deferred re-intercept pass with a depth counter (max_depth=1). Small change, fixes the chaining limitation, keeps existing architecture. This is what you hinted at in your bug fix notes.

We're proposing Option A because it's the cleaner long-term architecture. But if Option B is significantly easier to implement and unblocks current work, it's a valid choice. The spec can document either.

### 6.3 Data we need

If moving forward with Option A:

1. The current `EventBus.Publish` implementation — exact code, not the spec's approximation
2. All callers of `Intercept()` — are there paths other than `Publish` that invoke the interceptor?
3. The crash recovery implementation — how does `RecoverFromCrash()` actually work vs what the spec describes?
4. Any performance constraints on the publish path — is synchronous interceptor processing a latency requirement?
5. The `agent_message` delivery path — does it go through `Publish()` or a separate mechanism?

### 6.4 Structured message schemas

Do you already have typed structs for event payloads in the codebase, or are schemas purely dynamic (generated by commgraph)? This determines whether `system-nodes.yaml` schema definitions can be validated against Go types or are the new source of truth.

---

## 7. Migration Path

If approved, the implementation order:

1. **Add Message struct alongside Event.** Don't remove events yet. Add `To`, `Schema`, `Body` fields. This is backward-compatible.
2. **Add schema validation to Publish.** If `Schema` is set, validate before persisting. Freeform messages pass through unvalidated.
3. **Create system node runner.** A component that loads YAML state machine definitions and executes them as a subscriber. Start with one pipeline (scoring) as a proof of concept.
4. **Migrate scoring pipeline.** Move `handleVerticalDiscovered` + `handleScoreDimensionComplete` from the interceptor to the system node runner. Keep all other interceptor cases in place.
5. **Validate.** Run the scoring pipeline end-to-end through the new path. Verify it handles the deferred event scenario correctly (it should, since there are no deferred events anymore).
6. **Migrate remaining pipelines.** Validation, discovery accumulation, scan campaigns — one at a time, each validated before moving to the next.
7. **Remove interceptor.** Once all 26 cases are migrated, remove the `Intercept()` middleware from `Publish()`.
8. **Rename Event → Message.** Database migration, struct rename, API changes.

Each step is independently deployable and revertable. Step 4 is the proving ground — if the system node runner works for scoring, it'll work for everything.

---

## 8. Impact on Contracts

### 8.1 New contract file: `system-nodes.yaml`

Defines all system nodes with their state machines, message subscriptions, and message productions. ~400 lines covering: scoring pipeline, validation pipeline, discovery accumulation, scan campaign management.

### 8.2 Modified: `event-catalog.yaml` → `message-catalog.yaml`

Field restructure as described in §4.2. Event count drops from 166 to estimated ~120 (orchestration events become directed messages within system node definitions, not catalog entries).

### 8.3 Modified: `agent-tools.yaml`

Add `node_type: agent` to all existing entries. Pipeline coordinator moves from being an implicit part of the runtime to an explicit entry with `node_type: system`.

### 8.4 Modified: `verification-gates.yaml`

New gates:
- `system-nodes-yaml-parse`: All system node definitions parse and have valid state machines
- `system-nodes-messages-in-catalog`: Every message type referenced in system nodes exists in message catalog
- `system-nodes-reachability`: Every state in every state machine is reachable
- `system-nodes-termination`: Every state machine has at least one terminal state

### 8.5 Modified: `ddl-canonical.sql`

Minimal: rename `events` table to `messages`, add `to_node`, `schema_id` columns. Pipeline state tables unchanged.

---

## 9. Decision Requested

Please respond with one of:

**A. Proceed with full RFC.** We'll write v2.0.37 spec with unified message bus + system nodes. Send us the data from §6.3.

**B. Proceed with Option B (depth-guarded re-intercept).** We'll write v2.0.37 with minimal interceptor fix + document the deferred event limitation. Defer the message bus redesign.

**C. Modify RFC.** Specific feedback on what's wrong, what's missing, or what should change before proceeding.

**D. Defer entirely.** Ship v2.0.36 as-is, run verticals, revisit architecture after production data.

Our recommendation is A, with the migration path in §7 providing a safe incremental implementation. But this is your codebase — you know the practical constraints we don't.

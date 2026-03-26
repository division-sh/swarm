# Swarm Platform — Developer Guide

Build production-grade multi-agent systems with declarative YAML contracts. No orchestration code. No product hooks. Just contracts.

---

## Quick Start: Your First Flow in 10 Minutes

You're building a support ticket system. A ticket comes in, gets classified, routed to an agent, resolved, and closed.

### Step 1: Create the flow package

```
my-ticket-flow/
  package.yaml
  schema.yaml
  nodes.yaml
  events.yaml
  agents.yaml
  tools.yaml
  policy.yaml
  prompts/
    classifier-agent.md
    resolver-agent.md
```

### Step 2: Define the flow identity

```yaml
# package.yaml
name: ticket-flow
version: 1.0.0
description: Support ticket classification and resolution
platform: ">=1.1.0"
flows: []
```

### Step 3: Define the state machine

```yaml
# schema.yaml
initial_state: new
terminal_states: [closed, abandoned]
states: [new, classified, assigned, in_progress, resolved, closed, abandoned]

pins:
  inputs:
    events: [ticket.created]
  outputs:
    events: [ticket.resolved, ticket.abandoned]

required_agents:
  - role: classifier-agent
    subscribes_to: [ticket.created]
    emits: [ticket.classified]
    description: Classifies incoming tickets by category and priority

  - role: resolver-agent
    subscribes_to: [ticket.assigned]
    emits: [ticket.resolved, ticket.escalated]
    description: Resolves assigned tickets
```

### Step 4: Define the system node

```yaml
# nodes.yaml
ticket-orchestrator:
  id: ticket-orchestrator
  execution_type: system_node
  description: Manages ticket lifecycle from creation to resolution
  subscribes_to:
    - ticket.created
    - ticket.classified
    - ticket.resolved
    - ticket.escalated
    - ticket.abandoned
    - timer.ticket_sla
  produces:
    - ticket.assigned
    - ticket.sla_breached
  event_handlers:

    ticket.created:
      description: New ticket received. Advance to new state.
      advances_to: new

    ticket.classified:
      description: Agent classified the ticket. Write category, assign.
      data_accumulation:
        writes:
          - source_field: category
            target_field: category
          - source_field: priority
            target_field: priority
        source_event: ticket.classified
      advances_to: assigned
      emits: ticket.assigned

    ticket.resolved:
      description: Agent resolved the ticket.
      data_accumulation:
        writes: [resolution, resolved_by]
        source_event: ticket.resolved
      advances_to: resolved
      emits: ticket.resolution_confirmed

    ticket.escalated:
      description: Agent couldn't resolve. Escalate to human.
      guard:
        id: escalation_limit
        check: "entity.escalation_count < policy.max_escalations"
        on_fail: kill
      advances_to: assigned
      emits: ticket.assigned

    ticket.abandoned:
      description: Ticket abandoned (customer left, duplicate, etc.)
      advances_to: abandoned

    timer.ticket_sla:
      description: SLA timer expired. Breach notification.
      emits: ticket.sla_breached

  timers:
    - id: ticket_sla
      event: timer.ticket_sla
      delay: "{{sla_hours}}h"
      start_on: state:assigned
      cancel_on: state:resolved

  entity_schema:
    fields:
      ticket_id: uuid
      category: text
      priority: text
      resolution: text
      resolved_by: text
      escalation_count: integer
```

### Step 5: Define the events

```yaml
# events.yaml
ticket.created:
  payload:
    ticket_id: string
    subject: string
    body: string
    customer_email: string

ticket.classified:
  payload:
    ticket_id: string
    category: string
    priority: string
    confidence: number

ticket.assigned:
  payload:
    ticket_id: string
    category: string
    priority: string

ticket.resolved:
  payload:
    ticket_id: string
    resolution: string
    resolved_by: string

ticket.escalated:
  payload:
    ticket_id: string
    reason: string
    escalation_count: integer

ticket.abandoned:
  payload:
    ticket_id: string
    reason: string

ticket.resolution_confirmed:
  payload:
    ticket_id: string

ticket.sla_breached:
  payload:
    ticket_id: string
    elapsed_hours: integer

timer.ticket_sla:
  payload:
    ticket_id: string
```

### Step 6: Define the agents

```yaml
# agents.yaml
classifier-agent:
  id: classifier-agent
  model_tier: haiku
  subscriptions: [ticket.created]
  emit_events: [ticket.classified]
  tools_tier2: []
  conversation_mode: task
  max_turns_per_task: 3
  description: Classifies tickets by category and priority

resolver-agent:
  id: resolver-agent
  model_tier: sonnet
  subscriptions: [ticket.assigned]
  emit_events: [ticket.resolved, ticket.escalated]
  tools_tier2: [knowledge_base_search]
  conversation_mode: session_per_entity
  max_turns_per_task: 10
  description: Resolves tickets using knowledge base and reasoning
```

### Step 7: Define tools

```yaml
# tools.yaml
knowledge_base_search:
  handler_type: workflow_registered
  description: Search the support knowledge base
  input_schema:
    query: string
    max_results: integer
  output_schema:
    results: array
```

### Step 8: Set policy

```yaml
# policy.yaml
sla_hours: 24
max_escalations: 3
```

### Step 9: Write agent prompts

```markdown
<!-- prompts/classifier-agent.md -->
# Classifier Agent

You classify support tickets by category and priority.

## Categories
- billing — payment, invoice, refund issues
- technical — bugs, errors, how-to questions
- account — login, password, profile changes
- feature — feature requests, suggestions

## Priority
- critical — system down, data loss
- high — major feature broken
- medium — minor issue, workaround exists
- low — cosmetic, nice-to-have

## Your Task
Read the ticket subject and body. Determine category and priority.
Emit ticket.classified with your assessment and confidence score (0-100).
```

```markdown
<!-- prompts/resolver-agent.md -->
# Resolver Agent

You resolve support tickets using the knowledge base.

## Process
1. Read the ticket category and body
2. Search the knowledge base for relevant articles
3. Draft a resolution based on the best match
4. If confidence > 80%: emit ticket.resolved with your resolution
5. If confidence < 80%: emit ticket.escalated with reason

## Tools
- knowledge_base_search: search for relevant support articles

## Constraints
- Never make up information not in the knowledge base
- Always cite the article ID in your resolution
- Escalate if the issue requires account access or system changes
```

### Step 10: Verify

```bash
python3 verify.py
# Should output: 0 errors, 0 warnings
```

**Note on emitted event payloads:** By default, emitted events carry entity fields as a base, overlaid with the triggering event payload (trigger wins on collision), plus platform fields (entity_id, trigger_event_type, current_state). Use `payload_transform` when you need to construct a custom payload from entity state, policy, or computed values instead of forwarding the trigger payload.

That's it. The platform loads your contracts, boots the state machine, starts the agents, and processes tickets.

---

## Core Concepts

### Flows

A flow is a self-contained workflow package. It declares everything the platform needs: states, agents, events, handlers, tools, and policy.

```
my-flow/
  package.yaml      ← identity, version, child flows
  schema.yaml       ← state machine, pins, required agents
  nodes.yaml        ← system nodes with handlers
  events.yaml       ← event payload schemas
  agents.yaml       ← agent definitions
  tools.yaml        ← available tools (optional)
  policy.yaml       ← configuration values (optional)
  prompts/          ← agent behavioral instructions
  flows/            ← child flows (optional)
```

Flows have two modes:

| Mode | Instances | Created | Use case |
|------|-----------|---------|----------|
| `static` | Exactly 1 | At boot | Most flows |
| `template` | 0 at boot, N at runtime | By `create_flow_instance` | Per-customer, per-order instances |

### Entities

An entity is a mutable document moving through your state machine. It has:
- **state** — where it is (e.g., `new`, `assigned`, `resolved`)
- **gates** — boolean checkpoints (e.g., `g_verified: true`)
- **fields** — data written by handlers (e.g., `category`, `resolution`)

Multiple entities can be in the same flow simultaneously. Each has its own entity_id and its own state. Handlers process one entity at a time. Per-entity serialization prevents race conditions.

### System Nodes

System nodes are deterministic orchestrators. They subscribe to events and execute handler declarations. No code — just YAML.

A handler declares what happens when an event arrives:

```yaml
ticket.classified:
  guard:
    id: valid_category
    check: "payload.category in ['billing', 'technical', 'account', 'feature']"
    on_fail: reject
  data_accumulation:
    writes: [category, priority]
    source_event: ticket.classified
  advances_to: assigned
  emits: ticket.assigned
```

**One system node per event.** No two nodes may handle the same event. This ensures unambiguous state authority.

### Handler Dependency Graph

Handler fields execute in causal order:

```
guard → accumulate → compute → on_complete/rules
  → {advances_to, sets_gate, data_accumulation}   ← independent, atomic commit
    → payload_transform → emits → action
```

- Guard failure → handler stops (reject/discard/kill/escalate)
- Accumulate incomplete → handler stops, waits for more events
- Independent steps commit atomically in any order
- Emitted events are persisted in the transaction, delivered after commit

### Agents

Agents are LLM sessions. They receive events, reason, and emit response events. The platform manages their lifecycle:

- **task mode** — new session per event, no memory
- **session mode** — persistent session across events
- **session_per_entity** — one session per entity, context preserved

Agents never write to entity state directly. They emit events. System node handlers decide what gets written.

**Emit tools** are auto-generated. If your agent declares `emit_events: [ticket.classified]`, the platform creates an `emit_ticket_classified` tool automatically.

**Universal tools** (`agent_message`, `mailbox_send`) are available to all agents without declaration.

### Events

Events are the communication primitive. Every interaction flows through events:

```
Agent emits event → platform persists → system node handles → new event emitted → next agent receives
```

Every event has a typed payload schema in `events.yaml`. The platform validates payloads at entry.

Events can be marked with metadata:
- `_source: external` — produced by human/API, not by any agent or node
- `_consumer: mailbox_system` — consumed by UI, not by any agent
- `_status: planned` — future feature, not yet implemented

### Expressions (CEL)

All guard checks, rule conditions, and filter expressions use CEL (Common Expression Language):

```yaml
guard:
  check: "entity.escalation_count < policy.max_escalations"

rules:
  billing:
    condition: "payload.category == 'billing'"
    emits: ticket.billing_assigned
  technical:
    condition: "payload.category == 'technical'"
    emits: ticket.tech_assigned
```

Available context variables:
- `entity` — current entity state (fields, state, gates)
- `payload` — current event payload
- `policy` — flow + root policy values
- `accumulated` — items received during accumulation
- `fan_out` — metadata from fan_out step (e.g., count)

---

## Patterns

### Pattern 1: Guard with Escalation

```yaml
handle.request:
  guard:
    id: rate_limit
    check: "entity.request_count < policy.max_requests_per_hour"
    on_fail: escalate:rate_limit.exceeded
  advances_to: processing
```

Guard on_fail actions: `reject` (default), `discard` (silent), `kill` (terminal), `escalate:{event}`.

### Pattern 2: Multi-Gate Pipeline

```yaml
# Gate 1
review.completed:
  sets_gate: g_reviewed
  advances_to: approved
  emits: approval.requested

# Gate 2
approval.granted:
  sets_gate: g_approved
  advances_to: ready

# Reset all gates
resubmission.requested:
  clear_gates: true
  advances_to: draft
```

### Pattern 3: Accumulate and Compute

Wait for N items, then compute an aggregate:

```yaml
score.received:
  accumulate:
    expected_from: entity.dimensions_requested
    completion: all
    dedup_by: payload.dimension
  compute:
    operation: weighted_average
    tiers:
      - dimensions: [quality, speed, cost]
        weight: 0.6
      - dimensions: [innovation, scalability]
        weight: 0.4
    store_as: entity.composite_score
  on_complete:
    - condition: "entity.composite_score >= policy.threshold"
      advances_to: approved
      emits: candidate.approved
    - condition: "entity.composite_score < policy.threshold"
      advances_to: rejected
      emits: candidate.rejected
```

`on_complete` MUST be a YAML list (ordered). First matching condition wins.

### Pattern 4: Fan Out

Dispatch work to N agents in parallel:

```yaml
batch.requested:
  fan_out:
    items_from: payload.items
    target: worker-agent
    emit_per_item: item.assigned
  data_accumulation:
    writes: [batch_expected_count]
    source_event: fan_out.count
```

Each item in the list gets its own event. The agent processes each independently. Track completion with accumulate.

### Pattern 5: Rules-Based Routing

```yaml
request.received:
  rules:
    billing:
      condition: "payload.category == 'billing'"
      emits: billing.request
      advances_to: billing_review
    technical:
      condition: "payload.category == 'technical'"
      emits: tech.request
      advances_to: tech_review
    unknown:
      condition: "else"
      emits: manual.review_needed
```

`rules` and `on_complete` are mutually exclusive. Never use both in the same handler.

### Pattern 6: Dynamic Flow Instances

For per-customer or per-order workflows:

```yaml
# In parent package.yaml
flows:
  - id: order-processing
    flow: order-processing
    mode: template

# In parent nodes.yaml
order.created:
  action: create_flow_instance
  template: order-processing
  instance_id_from: payload.order_id
  config_from:
    customer_name: payload.customer_name
    order_total: payload.total
```

Subscribe to all instances with wildcards: `order-processing/*/order.completed`

The child flow schema can declare `auto_emit_on_create` to bootstrap itself:

```yaml
# In child schema.yaml
auto_emit_on_create:
  event: order.processing_started
  description: Platform emits this when a new order instance is created
```

### Pattern 7: Timers

```yaml
# In nodes.yaml
timers:
  - id: payment_deadline
    event: timer.payment_deadline
    delay: "{{payment_deadline_hours}}h"
    start_on: state:awaiting_payment
    cancel_on: state:paid
    recurring: false

# Handler for timer expiry
timer.payment_deadline:
  advances_to: payment_overdue
  emits: payment.overdue_notification
```

### Pattern 8: Cross-Entity Queries

```yaml
# Read across all entities in the flow
timer.daily_report:
  query:
    - entities: tickets
      group_by: state
      count: true
      store_as: report.tickets_by_state
    - entities: tickets
      filter: "priority == 'critical' && state != 'resolved'"
      select: [ticket_id, subject, assigned_to]
      store_as: report.open_critical
  emits: report.daily_compiled
```

### Pattern 9: Payload Transform

Construct output event payload from multiple sources:

```yaml
order.finalized:
  payload_transform:
    fields:
      order_id: entity.order_id
      total: entity.computed_total
      customer: entity.customer_name
      items_count: entity.items.size()
  emits: invoice.generation_requested
```

CEL expressions evaluate against entity state, payload, and policy.

### Pattern 10: Anti-Bias Routing

When two agents shouldn't share context:

```yaml
# agents.yaml — two pools for the same role
primary-reviewer:
  subscriptions: [review.requested]
  emit_events: [review.completed]

secondary-reviewer:
  subscriptions: [review.appeal_requested]
  emit_events: [review.completed]
```

Route original reviews to primary, appeals to secondary. Same prompt, different pool. No shared context.

---

## Composing Flows

### Parent-Child Flows

```
my-project/
  package.yaml          ← declares child flows
  schema.yaml
  nodes.yaml
  events.yaml
  agents.yaml
  flows/
    intake/              ← child flow
      package.yaml
      schema.yaml
      ...
    processing/          ← child flow
      ...
```

```yaml
# parent package.yaml
flows:
  - id: intake
    flow: intake
    mode: static
  - id: processing
    flow: processing
    mode: template
```

### Addressing

- Local: `event_name` — same flow, no slash
- Absolute: `intake/ticket.ready` — child flow event
- Wildcard: `processing/*/order.completed` — any dynamic instance

### Pin Wiring

Child flows declare input/output pins. Parent events matching child input pins are automatically delivered:

```yaml
# child schema.yaml
pins:
  inputs:
    events: [work.requested]
  outputs:
    events: [work.completed]
```

If the parent emits `work.requested`, the child receives it.

### Policy Inheritance

Child flows inherit parent policy. Child can override specific keys:

```yaml
# parent policy.yaml
timeout_hours: 24
max_retries: 3

# child policy.yaml
timeout_hours: 48    ← overrides parent
# max_retries inherited as 3
```

---

## Error Handling

### Handler Failures

- 3 retries with exponential backoff (1s, 2s, 4s)
- After exhaustion → `platform.dead_letter` event with full context
- Transient errors retry. Business logic failures don't.

### Guard Failures

| on_fail | Behavior |
|---------|----------|
| `reject` | Stop handler. Event marked rejected. Default. |
| `discard` | Drop silently. No record. For expected filtering. |
| `kill` | Advance entity to terminal state. |
| `escalate:{event}` | Emit escalation event instead of proceeding. |

### Terminal States

When an entity reaches a terminal state, it's done forever:
- All events for that entity are rejected
- Timers are cancelled
- All entity data is preserved (queryable, never deleted)
- No configuration can override this

### Chain Depth

Maximum 50 chained event emissions. Prevents infinite loops. After 50 → dead letter.

### Dead Letter Payload

```yaml
original_event: string
original_payload: object
entity_id: string
flow_instance: string
failure_type: handler_error | chain_depth_exceeded | retry_exhausted
error_message: string
retry_count: integer
chain_depth: integer
handler_node: string
timestamp: string
```

---

## Testing Your Flow

### Run the Verifier

```bash
python3 verify.py
```

19 checks covering event chains, payload fields, conditions, state machines, required agents, handler fields, tools, prompts, deprecated fields, produces lists, policy conflicts, cycles, and dialect compliance.

### Write Test Packages

Each test is a minimal flow that exercises one capability:

```
tests/test-my-guard/
  package.yaml
  schema.yaml
  nodes.yaml
  events.yaml
  agents.yaml
  expected.yaml
```

```yaml
# expected.yaml
trigger:
  event: ticket.classified
  payload:
    ticket_id: t-001
    category: billing
    priority: high
    confidence: 95

expected:
  handler_outcome: success
  entity_state: assigned
  emitted_events: [ticket.assigned]
  entity_fields:
    category: billing
    priority: high
```

### Agent Mock Fixtures

Test full event chains without LLM tokens:

```yaml
# fixtures.yaml
agent_fixtures:
  classifier-agent:
    - on: ticket.created
      emits:
        - event: ticket.classified
          payload:
            ticket_id: "{{entity_id}}"
            category: technical
            priority: medium
            confidence: 92
```

The platform intercepts agent subscriptions and replays fixture events instead of calling the LLM.

---

## Dialect Rules

Your YAML must follow the platform spec exactly:

1. **Guards** — always `{id, check}` object form, never a bare string
2. **on_complete** — YAML list (ordered), never a dict
3. **Conditions** — always prefixed: `payload.X`, `entity.X`, or `policy.X`
4. **on_complete vs rules** — mutually exclusive, never both in one handler
5. **advances_to** — single string, never a list
6. **emits** — string or list of strings
7. **data_accumulation.writes** — list of strings (direct) or `{source_field, target_field}` objects (mapped)
8. **dedup_by** — set on accumulate when tracking by content (e.g., `payload.dimension`), not sender
9. **clear_gates: true** — clears ALL entity gates, not selective
10. **No self-emits** — a handler must never emit its own trigger event

---

## Checklist Before Deploying

- [ ] `verify.py` returns 0 errors
- [ ] Every event has a payload schema in `events.yaml`
- [ ] Every agent has a prompt file in `prompts/`
- [ ] Every `advances_to` target is in `schema.yaml` states
- [ ] Every `data_accumulation.writes` field exists in the source event payload
- [ ] Every condition references valid `payload.`, `entity.`, or `policy.` fields
- [ ] No two system nodes handle the same event
- [ ] `on_complete` is a list, not a dict
- [ ] Terminal states are declared in `schema.yaml` terminal_states
- [ ] Timers have `start_on` and `cancel_on` lifecycle

---

## Reference

### All Handler Fields

```
guard, accumulate, compute, on_complete, rules, advances_to,
sets_gate, clear_gates, data_accumulation, emits, fan_out,
filter, reduce, count, group_by, query, clear,
payload_transform, action, from, description
```

### Platform Actions

```
create_flow_instance — create a new dynamic flow instance
record_evidence — append payload to entity accumulator
```

### Reduce Operations

```
weighted_average, pick_or_average, sum, min, max, count
```

### Guard on_fail Actions

```
reject (default), discard, kill, escalate:{event_name}
```

### Accumulate Completion Modes

```
all, threshold, timeout
```

### Conversation Modes

```
task — stateless, new session per event
session — persistent across events
session_per_entity — one session per entity
```

### Boot Verification Checks (16)

| Severity | Checks |
|----------|--------|
| error | payload fields, required agents, handler fields, tools, deprecated fields, CEL parse, single node per event, dialect |
| warning | event chains, prompts, policy conflicts, produces drift |

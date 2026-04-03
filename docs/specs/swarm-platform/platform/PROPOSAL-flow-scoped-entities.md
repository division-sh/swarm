# Proposal: Flow-Scoped Entity State

## Status: Draft — for discussion

## Problem

The current model allows multiple flows to share one `entity_state` row.
A single entity can be in `scoring/marginal_review` AND `validation/researching`
simultaneously, stored in a `flow_states` map on the same row.

This creates ambiguity in:

- **State queries**: "what state is this entity in?" has no single answer
- **Timer ownership**: `cancel_on: state:killed` — which flow's killed?
- **Terminal detection**: "is this entity done?" requires checking all flows
- **Kill authority**: which flow is allowed to kill the entity?
- **Field ownership**: which flow's writes are authoritative for shared fields?
- **Dashboards**: showing a single status requires arbitrary choice of "active flow"

The `flow_states` map was a workaround for this — it compensates for the
wrong unit of state ownership. Removing `current_state` in favor of
`flow_states` simplifies one bug but keeps the deeper ambiguity.

## Root Cause

State ownership is at the wrong level. A flow is a state machine. But
the runtime lets multiple flows mutate one shared mutable row. That
violates the isolation that input/output pins already establish for
events.

Pins say: events cross flow boundaries explicitly. State should follow
the same rule: state is flow-local, data crosses flows via events.

## Proposed Model

### 1. Entity = flow-scoped

One entity belongs to exactly one flow. One row in `entity_state`, one
`current_state` column, one state machine. No `flow_states` map needed.

```
entity_state row for scoring:
  entity_id: abc-123
  flow_instance: scoring
  current_state: marginal_review
  fields: {composite_score: 66.8, ...}

entity_state row for validation:
  entity_id: def-456
  flow_instance: validation
  current_state: researching
  fields: {business_brief: {...}, ...}
```

No contradiction. Each entity has exactly one state.

### 2. Cross-flow = handoff via events

When an entity completes a flow phase and needs to enter the next flow,
the transition is an explicit handoff:

1. Scoring flow emits `vertical.shortlisted` (output pin)
2. Validation flow's handler receives it with `create_entity: true`
3. Validation creates its OWN entity row
4. Data from the scoring entity is passed via the event payload
   (or the validation agent reads the scoring entity via `get_entity`)

This is already how Empire works for scoring → validation:

```yaml
# validation-orchestrator
vertical.shortlisted:
  create_entity: true        # ← new entity for validation
  advances_to: researching
  emits: validation.started
```

The proposal makes this the REQUIRED pattern, not an optional one.

### 3. Business identity = lineage key

A vertical is a business concept that passes through multiple flows.
Each flow creates its own entity. To link them:

```
entity_state (scoring entity):
  entity_id: abc-123
  subject_id: vert-001          # ← business identity
  flow_instance: scoring
  current_state: marginal_review

entity_state (validation entity):
  entity_id: def-456
  subject_id: vert-001          # ← same business identity
  flow_instance: validation
  current_state: researching
```

`subject_id` is a new column on `entity_state`. It's the cross-flow
correlation key. It is NOT the entity_id — each flow has its own
entity_id.

Query patterns:
- "What state is entity abc-123 in?" → `marginal_review` (unambiguous)
- "What's happening with vertical vert-001?" → query by subject_id,
  get all flow entities, see the full picture
- "Show me all verticals in validation" → query by flow_instance +
  current_state

### 4. What changes

#### entity_state DDL

```sql
CREATE TABLE entity_state (
    entity_id         UUID PRIMARY KEY,
    subject_id        UUID,                    -- NEW: business identity
    flow_instance     TEXT NOT NULL,
    entity_type       TEXT NOT NULL DEFAULT 'default',
    slug              TEXT,
    name              TEXT,
    current_state     TEXT NOT NULL,            -- KEPT: unambiguous per-flow
    gates             JSONB NOT NULL DEFAULT '{}',
    fields            JSONB NOT NULL DEFAULT '{}',
    accumulator       JSONB NOT NULL DEFAULT '{}',
    revision          INTEGER NOT NULL DEFAULT 0,
    entered_state_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    INDEX idx_entity_subject (subject_id) WHERE subject_id IS NOT NULL,
    INDEX idx_entity_flow (flow_instance, current_state),
    INDEX idx_entity_state (current_state),
    -- ... existing indexes
);
```

#### flow_states: removed

No `flow_states` map in `fields`. Each flow writes to its own entity
row. The `current_state` column is singular and authoritative.

#### create_entity: required at flow boundaries

When a handler receives an event from outside its flow (an input pin
event), it MUST use `create_entity: true` if the flow manages entity
state. This creates the flow-local entity. Without it, the handler
would mutate a foreign flow's entity — which is now prohibited.

Boot validation: if a handler subscribes to an input pin event AND the
flow has `initial_state` (not stateless), the handler MUST declare
`create_entity: true`. Boot error otherwise.

#### subject_id propagation

When `create_entity: true` fires on a cross-flow handoff event:

1. If the inbound event payload contains `entity_id` (the source
   flow's entity), the new entity's `subject_id` is set to the
   source entity's `subject_id` (or the source `entity_id` if no
   subject_id exists — first flow in the chain).
2. This means all entities for the same business object share one
   `subject_id`, regardless of which flow created them.

The platform handles this automatically — no contract declaration needed.

#### get_entity cross-flow reads

Agents in one flow can still read entities from other flows:

```
get_entity({entity_id: "abc-123"})  # read scoring entity from validation agent
```

This is a READ — the validation agent reads the scoring entity's fields
(composite_score, dimension evidence) but never writes to it. The
scoring entity is immutable from validation's perspective.

For convenience, a new tool or parameter:

```
get_entity({subject_id: "vert-001", flow: "scoring"})
```

Returns the scoring entity for a given business identity.

#### Timers: unambiguous

```yaml
timers:
- id: marginal_kill
  event: timer.marginal_kill
  start_on: state:marginal_review    # THIS flow's state
  cancel_on: state:killed            # THIS flow's state
```

No ambiguity. The timer belongs to the scoring flow entity. It fires
based on the scoring entity's state. Validation's state is irrelevant.

#### Terminal detection: simple

```sql
-- Is this flow entity done?
SELECT current_state IN ('killed') FROM entity_state WHERE entity_id = :id

-- Is the business object fully done? (all flows terminal)
SELECT bool_and(current_state IN ('killed', 'completed', ...))
FROM entity_state WHERE subject_id = :subject_id
```

#### Kill propagation: explicit events

If validation kills a vertical, it emits `vertical.killed_backprop`.
The scoring flow handles this and advances the scoring entity to
`killed`. Each flow kills its own entity. No shared mutation.

This is already how Empire works — the pattern just becomes mandatory.

### 5. What this fixes

| Problem | Before | After |
|---------|--------|-------|
| "What state is this entity in?" | Depends which column you read | One answer: `current_state` |
| Timer ownership | Ambiguous across flows | Local to flow entity |
| Terminal detection | Check all flow_states entries | Check one `current_state` |
| Kill authority | Any flow can mutate shared row | Each flow kills its own entity |
| Field ownership | Shared fields, last-writer-wins | Each flow owns its entity's fields |
| Dashboard status | Requires flow context to interpret | Direct query per flow |

### 6. Migration impact

**Empire contracts**: minimal change. Validation and operating already
use `create_entity: true` at flow boundaries. Scoring creates entities
on `vertical.discovered`. The main change is adding `subject_id`
propagation and formalizing the pattern.

**Runtime**: moderate change.
- Add `subject_id` column to entity_state
- Auto-populate subject_id on `create_entity` from inbound event context
- Remove `flow_states` write path
- Add boot validation: input pin handlers must use `create_entity: true`
- Add `get_entity` by subject_id + flow

**Existing queries**: any code querying `entity_state` by `current_state`
continues to work. Queries that relied on `flow_states` need to join
by `subject_id` instead.

### 7. What stays the same

- `entity_id` is still the primary key
- `current_state` column stays — it's unambiguous now
- `advances_to` handler field unchanged
- Guards reference `entity.current_state` as before
- Event payloads carry `entity_id` as before
- `get_entity` by entity_id unchanged
- Within a single flow, nothing changes at all

### 8. Resolved Design Points

These were open questions, now resolved based on implementer feedback.

#### 8.1 subject_id propagation rules (deterministic)

The rules are strict and never ambiguous:

1. **First flow in the chain** (e.g., scoring receives `vertical.discovered`):
   `create_entity: true` fires. No source entity exists yet.
   `subject_id = entity_id` (the new entity IS the subject origin).

2. **Subsequent flows** (e.g., validation receives `vertical.shortlisted`):
   `create_entity: true` fires. The inbound event payload contains
   `entity_id` from the source flow.
   - Read source entity's `subject_id`.
   - If non-null: `new_entity.subject_id = source.subject_id`.
   - If null: `new_entity.subject_id = source.entity_id`.
   - **Never invent a new subject_id mid-chain.**

3. **Platform enforces this automatically.** No contract declaration
   needed. The `create_entity` step reads the inbound event's
   `entity_id`, looks up the source entity, copies `subject_id`.

4. **Immutable.** Once set, `subject_id` is never updated. It is a
   permanent lineage anchor.

#### 8.2 Cross-flow boundary detection (boot validation)

"Outside its flow" is defined precisely:

- A handler's flow is its `flow_instance` (from the node's registration).
- An event is "from outside" if it appears in the flow's `schema.yaml
  pins.inputs.events` list.
- Boot validation rule: if a handler subscribes to an input pin event
  AND the flow has `initial_state` (not stateless), the handler MUST
  declare `create_entity: true`. Boot error otherwise.

Edge case: a flow that receives cross-flow events but is stateless
(initial_state: null) does not need `create_entity` — stateless flows
don't own entity state machines.

#### 8.3 Cross-flow writes: prohibited

Cross-flow entity reads are allowed (read-only):

```
# Validation agent reads scoring entity — OK
get_entity({entity_id: "abc-123"})
```

Cross-flow entity writes are **prohibited by the platform**:

```
# Validation agent writes to scoring entity — REJECTED
save_entity_field({entity_id: "abc-123", field: "notes", value: "..."})
  → error: "entity abc-123 belongs to flow scoring, caller is in flow validation"
```

The `save_entity_field` tool executor checks that the target entity's
`flow_instance` matches the calling agent's flow. Mismatch = rejected.

This is a platform enforcement, not a convention. Products cannot
override it.

#### 8.4 Subject lifecycle query (platform primitive)

"Is this business object fully done?" is a platform query, not SQL folklore.

New platform tool: `get_subject_status`

```
get_subject_status({subject_id: "vert-001"})
→ {
    subject_id: "vert-001",
    entities: [
      {entity_id: "abc-123", flow: "scoring", state: "marginal_review", terminal: false},
      {entity_id: "def-456", flow: "validation", state: "researching", terminal: false}
    ],
    all_terminal: false,
    latest_flow: "validation",
    latest_state: "researching"
  }
```

**`latest_flow` ordering rule**: determined by `entered_state_at` — the
most recent non-terminal state transition across all flow entities for
the subject. This is the entity whose state machine moved most recently.
If two entities transitioned in the same millisecond, the one with the
higher flow depth in the package.yaml `flows` list wins (downstream
flows take precedence over upstream). `latest_state` is that entity's
`current_state`.

This is a read-only query tool available to all agents. It answers
"what's happening with this business object across all flows" without
any ambiguity.

Dashboard/status views use the same underlying query.

#### 8.5 Name and display metadata

Name and display metadata belong to the **first entity in the subject
chain** (the origin). Subsequent flow entities do NOT duplicate the name.

```
entity_state (scoring — origin):
  entity_id: abc-123
  subject_id: abc-123          # origin: subject_id = entity_id
  name: "AI Proposal Writer"   # authoritative name

entity_state (validation):
  entity_id: def-456
  subject_id: abc-123
  name: NULL                   # name lives on origin entity
```

`get_subject_status` returns the name from the origin entity.
`get_entity` on a non-origin entity returns the entity's own fields
(name may be null). Agents that need the display name use
`get_subject_status` or `get_entity` on the origin.

Products that want the name copied at handoff can include it in the
`create_entity` handler's `data_accumulation`. This is a product choice,
not a platform rule. The platform only guarantees the name exists on
the origin.

#### 8.6 Backward compatibility

Flow-scoped entities are enforced starting at platform version 1.6.0.

- Products declaring `platform: ">=1.5.0"` use the current shared-row
  model. No breakage.
- Products declaring `platform: ">=1.6.0"` get flow-scoped enforcement:
  boot validation rejects input pin handlers without `create_entity: true`,
  cross-flow writes rejected at runtime.
- Migration: products update handlers at flow boundaries to use
  `create_entity: true` (most already do). `subject_id` is a platform
  column — products do not declare it in `entity_schema`. The platform
  owns it, auto-populates it, and indexes it.

### 9. Summary

The core principle:

> **State is flow-local. Lineage is cross-flow.**

Each flow owns its entity. Flows communicate via events.
Business identity links entities across flows via `subject_id`.
No shared mutable state across flow boundaries.

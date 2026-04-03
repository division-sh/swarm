# Test Plan: Flow-Scoped Entity State (v1.6.0)

## Existing coverage (already tested)

| Scenario | Fixture | Status |
|----------|---------|--------|
| create_entity mints new entity | tier4/test-create-entity | OK |
| Sibling flow isolation (partial) | tier11/test-child-flow-sibling-isolation | Partial — only checks flow-a |
| Data pin write conflict (boot) | tier11/test-data-pin-write-conflict | OK |
| Cross-flow write rejection (tools) | executor_entity_test.go | OK |
| subject_id first-flow seeding (unit) | handler_engine_transaction_test.go | OK |
| subject_id parent chaining (unit) | handler_engine_transaction_test.go | OK |

## Missing tests — organized by category

### A. subject_id Lineage

**A1. First flow seeds subject_id = entity_id**
- Tier: 11
- Setup: root flow emits event → child flow-a handles with create_entity: true
- Assert: flow-a entity has subject_id == entity_id (self-referencing origin)
- Why: unit test exists but no E2E fixture asserts this in expected.yaml

**A2. Second flow inherits subject_id from source**
- Tier: 11
- Setup: flow-a creates entity, emits cross-flow event → flow-b handles with create_entity: true
- Assert: flow-b entity has subject_id == flow-a entity's subject_id
- Assert: flow-a.entity_id != flow-b.entity_id (different entities)
- Assert: flow-a.subject_id == flow-b.subject_id (same business object)
- Why: core lineage guarantee, no E2E coverage

**A3. Three-level subject_id chain (grandchild)**
- Tier: 11
- Setup: root → flow-a → flow-b → flow-c, each with create_entity: true
- Assert: all three flow entities share the same subject_id (the root entity's entity_id)
- Why: test-nested-three-levels exists but doesn't verify subject_id

**A4. subject_id immutability**
- Tier: 6 (atomicity)
- Setup: entity created with subject_id, then handler attempts to overwrite subject_id via data_accumulation
- Assert: subject_id unchanged after handler commit
- Why: spec says subject_id is immutable once set

**A5. subject_id null when no source entity (root-level, no cross-flow)**
- Tier: 4
- Setup: single-flow product, entity created by first event, no parent
- Assert: subject_id == entity_id (self-origin)
- Why: edge case — what happens when there's no cross-flow context at all

### B. Flow-Scoped State Isolation

**B1. Sibling flow entities are fully independent**
- Tier: 11
- Setup: root emits event → flow-a and flow-b both handle with create_entity: true
- Trigger flow-a event → flow-a entity advances to state X
- Trigger flow-b event → flow-b entity advances to state Y
- Assert: flow-a entity state == X, flow-b entity state == Y
- Assert: flow-a entity fields unaffected by flow-b writes
- Why: existing test only checks flow-a, never triggers flow-b

**B2. current_state is flow-local (no cross-contamination)**
- Tier: 11
- Setup: flow-a entity in state "processing", flow-b entity in state "reviewing"
  (same subject_id)
- Assert: query entity_state WHERE entity_id = flow-a → "processing"
- Assert: query entity_state WHERE entity_id = flow-b → "reviewing"
- Assert: no flow_states map exists on either entity
- Why: verify the old flow_states model is gone

**B3. Terminal state is flow-local**
- Tier: 11
- Setup: flow-a advances entity to terminal (killed). flow-b entity same subject.
- Assert: flow-a entity is terminal, rejects further events
- Assert: flow-b entity is NOT terminal, continues processing
- Why: killed in one flow doesn't kill the entity in another flow

**B4. Gates are flow-local**
- Tier: 11
- Setup: flow-a sets gate g1. flow-b entity (same subject) does not have g1.
- Assert: flow-a entity gates = {g1: true}
- Assert: flow-b entity gates = {}
- Why: gates must not leak across flow boundaries

**B5. Accumulator is flow-local**
- Tier: 11
- Setup: flow-a accumulates 3 items. flow-b entity (same subject) accumulates different items.
- Assert: flow-a accumulator has 3 items, flow-b accumulator has its own items
- Why: accumulators are per-entity, entities are per-flow

### C. Cross-Flow Write Prohibition

**C1. save_entity_field rejects cross-flow write (agent tool)**
- Tier: 7 or 11
- Setup: agent in flow-a calls save_entity_field targeting flow-b's entity_id
- Assert: tool returns error "cross_flow_write_forbidden"
- Assert: flow-b entity unchanged
- Why: existing unit test covers this, need E2E

**C2. System node data_accumulation cannot write to foreign entity**
- Tier: 11
- Setup: flow-a node handler receives event with entity_id from flow-b
  (without create_entity), attempts data_accumulation
- Assert: handler errors or entity write rejected
- Why: no test for system node path (only agent tool path tested)

**C3. Cross-flow read is allowed**
- Tier: 11
- Setup: agent in flow-b calls get_entity with flow-a's entity_id
- Assert: returns flow-a entity data (read-only, no error)
- Why: reads must work for agents that need source entity context

### D. create_entity Enforcement at Boot

**D1. Input pin handler without create_entity on static stateful flow = boot error**
- Tier: 8
- Setup: static flow (initial_state: "init"), input pin event "x.start",
  handler for x.start has NO create_entity
- Assert: boot error with category related to missing create_entity
- Why: v1.6.0 enforcement rule

**D2. Template flow exempt from create_entity requirement**
- Tier: 8
- Setup: template flow (mode: template), input pin event, handler without create_entity
- Assert: boot succeeds (no error)
- Why: template flows create entities via create_flow_instance

**D3. Stateless flow exempt from create_entity requirement**
- Tier: 8
- Setup: stateless flow (initial_state: null), input pin event, handler without create_entity
- Assert: boot succeeds
- Why: stateless flows don't own entity state machines

**D4. Back-propagation event handler exempt from create_entity**
- Tier: 8 or 11
- Setup: static flow with input pin event "x.killed_backprop" (marked as backprop),
  handler without create_entity
- Assert: boot succeeds — backprop operates on existing entity
- Why: not all input pins introduce new business objects

### E. get_subject_status Tool

**E1. Returns all flow entities for a subject**
- Tier: 11
- Setup: subject with entities in flow-a (state: done) and flow-b (state: active)
- Call: get_subject_status({subject_id: X})
- Assert: response contains both entities with correct flow, state, terminal flags
- Why: core lifecycle query tool, untested

**E2. latest_flow reflects most recent non-terminal transition**
- Tier: 11
- Setup: flow-a entity transitioned at T1, flow-b entity transitioned at T2 (T2 > T1)
- Assert: latest_flow = flow-b, latest_state = flow-b's state
- Why: ordering rule needs verification

**E3. all_terminal = true when all flow entities are terminal**
- Tier: 11
- Setup: subject with flow-a entity killed, flow-b entity killed
- Assert: all_terminal = true
- Why: business object completion detection

**E4. all_terminal = false when any flow entity is non-terminal**
- Tier: 11
- Setup: subject with flow-a entity killed, flow-b entity still active
- Assert: all_terminal = false
- Why: complement of E3

**E5. Single-flow subject returns correctly**
- Tier: 4 or 11
- Setup: subject with only one flow entity
- Assert: entities array has one entry, latest_flow = that flow
- Why: simplest case, shouldn't break

**E6. Unknown subject_id returns empty**
- Tier: 4
- Setup: call with non-existent subject_id
- Assert: entities = [], all_terminal = true (vacuously)
- Why: error handling

### F. Timer Flow-Locality

**F1. Timer fires only for owning flow's entity**
- Tier: 5 or 11
- Setup: flow-a entity in state "waiting" with timer. flow-b entity (same subject) in state "active".
  Timer fires on state:waiting.
- Assert: timer affects flow-a entity only
- Assert: flow-b entity state unchanged
- Why: timers must not cross flow boundaries

**F2. Timer cancel_on respects flow-local state**
- Tier: 5 or 11
- Setup: flow-a timer with cancel_on: state:killed. flow-b entity (same subject) advances to killed.
- Assert: flow-a timer NOT cancelled (flow-b's killed doesn't affect flow-a's timer)
- Why: cancel_on is flow-local

### G. Kill Back-Propagation

**G1. Downstream kill propagates to upstream via event**
- Tier: 7 or 11
- Setup: flow-a entity in state "active". flow-b (downstream) kills its entity,
  emits killed_backprop. flow-a handles killed_backprop, advances own entity to killed.
- Assert: flow-a entity = killed (advanced by its own handler)
- Assert: flow-b entity = killed (advanced by its own handler)
- Assert: each entity was killed by a handler in its own flow
- Why: kill propagation is the canonical cross-flow state change pattern

**G2. Kill backprop doesn't create new entity**
- Tier: 11
- Setup: flow-a has killed_backprop in backprop_events (not input pins).
  Handler has NO create_entity.
- Assert: handler operates on existing flow-a entity
- Assert: no new entity created
- Why: backprop is exempt from create_entity rule

### H. Edge Cases

**H1. Same event handled by two sibling flows with create_entity**
- Tier: 11
- Setup: root emits event X. flow-a and flow-b both subscribe to X with create_entity: true.
- Assert: two separate entities created (different entity_ids, same subject_id)
- Why: fan-out to multiple flows from one event

**H2. Entity created in stateless parent, handed to stateful child**
- Tier: 11
- Setup: stateless discovery flow emits event. Stateful scoring flow handles with create_entity.
- Assert: scoring entity created with subject_id. Discovery has no entity.
- Why: mirrors Empire's discovery → scoring handoff

**H3. create_entity with empty payload (no source entity_id)**
- Tier: 4
- Setup: event with no entity_id in payload triggers create_entity handler
- Assert: entity created with subject_id = entity_id (self-origin, no parent to copy from)
- Why: first event in the system has no predecessor

**H4. Concurrent cross-flow reads during write**
- Tier: 6
- Setup: flow-b agent reads flow-a entity while flow-a is mid-handler (writing fields)
- Assert: flow-b sees consistent state (either pre-handler or post-handler, no partial)
- Why: transaction isolation guarantee

**H5. get_entity on own flow entity vs foreign flow entity**
- Tier: 11
- Setup: agent calls get_entity on its own flow's entity and on a different flow's entity
- Assert: both succeed (reads are allowed cross-flow)
- Assert: response includes flow_instance so agent knows which flow owns the entity
- Why: agents need to distinguish "my entity" from "source entity I'm reading"

## Summary

| Category | Count | Priority |
|----------|-------|----------|
| A. subject_id lineage | 5 | High — core guarantee |
| B. Flow-scoped isolation | 5 | High — the whole point |
| C. Cross-flow write prohibition | 3 | High — enforcement |
| D. Boot validation | 4 | Medium — catches contract errors |
| E. get_subject_status | 6 | Medium — new tool |
| F. Timer locality | 2 | Medium — subtle bugs |
| G. Kill back-propagation | 2 | High — canonical pattern |
| H. Edge cases | 5 | Low-Medium |
| **Total** | **32** | |

Recommended implementation order: A1-A3, B1-B3, C1, G1, D1-D3, then the rest.

# Flight Recorder & Fork — Design Overview

## Problem

When a pipeline fails at hop N, the only recovery is a full rerun from
`system.started`. A scoring+validation run takes 30+ minutes of LLM calls.
Debugging requires reading logs and querying DB tables manually. There is no
way to answer "what did entity X look like when event Y fired?" without
reverse-engineering it from the current state.

## Goal

1. **Flight recorder**: step-by-step audit trail of every state mutation,
   linked to the event that caused it. Answers "what happened and why" for
   any run.

2. **Fork**: rewind a run to a point in time, optionally swap contracts,
   and resume. The system determines which events to replay automatically.

---

## Fork Semantics — Approach Comparison

We evaluated three approaches to fork. This comparison captures the
trade-offs that led to the chosen design.

### Approach A: Single-Event Re-injection

**How it works**: pick one event, reconstruct entity state at that event,
re-publish that event under a new run.

```
swarm fork --run <id> --event <event_id>
```

Fork reconstructs the target event's entity, re-injects that one event.

**Strengths**:
- Simple to implement. One entity, one event, one reconstruction.
- Sufficient for "retry this one failed delivery."

**Weaknesses**:
- Ignores parallel branches. If event 1 fanned out to agents A and B,
  producing events 2 and 3 concurrently, forking at event 2 leaves
  event 3's downstream chain orphaned. Event 3 was produced by an LLM
  agent — it cannot be reproduced deterministically.
- Forces the user to understand the causal DAG and pick the right event.
  In a system with 30+ concurrent agent deliveries, this is error-prone.
- Cannot swap contracts. The fork only re-delivers one event — it doesn't
  restart the system.

**Verdict**: useful as a low-level retry primitive, not as a fork model.

---

### Approach B: Causal DAG Fork (Branch-Scoped)

**How it works**: pick a target event, walk the causal DAG to identify
ancestor vs sibling branches. Preserve sibling branch state, re-execute
the target branch.

```
swarm fork --run <id> --event <event_id>
```

Fork partitions the event DAG at the target:
- Ancestor events (causal chain to root): their mutations are preserved.
- Target event onward: re-executed.
- Sibling branches: carried over as-is OR dropped.

**Strengths**:
- Respects the causal structure. Doesn't accidentally undo sibling work.
- Conceptually clean for single-entity debugging.

**Weaknesses**:
- Complex to implement. Requires full DAG traversal and branch
  classification for every fork.
- Sibling handling is ambiguous. "Carry over" means mixing old execution
  results with new execution — the forked run's state is a hybrid of old
  and new, which is confusing to reason about. "Drop" means losing work.
- Still event-centric. The user must pick the right event in the DAG,
  which requires understanding the branching structure.
- Contract swap is awkward. If you change handler logic, sibling branches
  that were "carried over" used the old contracts — the fork has mixed
  contract versions in its state.

**Verdict**: over-engineered for the actual use case. The DAG complexity
doesn't buy enough over the simpler timestamp approach.

---

### Approach C: Timestamp-Based System Fork (Chosen)

**How it works**: pick a moment in time (via timestamp or event ID as
shorthand). Reconstruct the FULL system state at that moment — all
entities, all accumulators, all gates, all pending deliveries. Boot
with (optionally new) contracts. Resume.

```
swarm fork --run <id> --at <event_id|timestamp> [--contracts <path>]
```

Fork does:
1. Reconstruct all entity states at timestamp T (reverse-apply all
   mutations with committed_at > T, or forward-apply from empty).
2. Identify pending work: events created at or before T whose delivery
   chain was incomplete (pending/in-progress deliveries, unfinished
   accumulations, non-terminal entities awaiting next event).
3. Create new run with forked_from lineage.
4. Boot with new contracts (or same contracts if not specified).
5. Re-deliver all pending events. The normal event loop resumes.

**Strengths**:
- Handles parallelism naturally. At timestamp T, both event 2 and
  event 3 either happened or didn't. No branch logic, no DAG walking,
  no sibling ambiguity. The state is just "what was the world at T."
- User doesn't need to understand the event DAG. Pick a moment,
  fork, go. The system determines what to replay.
- Contract swap is clean. New contracts apply uniformly to everything
  from T forward. No mixed-version state.
- Matches the real use case: "something went wrong, I fixed the
  contracts, resume from before the failure."
- Subsumes Approach A: if you want to retry one event, fork at T-1
  where T is that event's timestamp. The system will re-deliver it.

**Weaknesses**:
- Reconstructs more state than strictly necessary (all entities, not
  just the one that failed). Cost is proportional to mutation count
  after T, which for recent fork points is small.
- Timestamp ordering can be ambiguous for events committed in the same
  millisecond. Mitigation: use event sequence numbers or
  (timestamp, event_id) composite ordering.
- "Pending work" detection requires scanning deliveries and accumulator
  state, not just replaying one event. More implementation work upfront.

**Verdict**: the right abstraction. It's how a human thinks about fork
("go back to 5 minutes ago and try again with the fix"), it handles
parallelism without special cases, and it enables contract hot-swap.

---

### Comparison Matrix

|                              | A: Single Event | B: DAG Fork | C: Timestamp Fork |
|------------------------------|:-:|:-:|:-:|
| Handles parallel agents      | no — sibling branches orphaned | yes — but complex carry-over logic | yes — naturally via timestamp |
| User must understand DAG     | yes | yes | no — pick a moment |
| Contract swap                | no | awkward (mixed versions) | clean (uniform from T forward) |
| Implementation complexity    | low | high (DAG traversal + branch classification) | medium (state reconstruction + pending detection) |
| Scope of reconstruction      | one entity | one branch + carried siblings | all entities at T |
| Automatic replay detection   | no — user picks the event | no — user picks the branch | yes — system finds pending work |
| Subsumes the others          | — | partially | yes |
| Matches real debugging flow  | sometimes | rarely | almost always |

---

## Chosen Design: Timestamp Fork + Mutation Log

### 1. Mutation Log

New table: `entity_mutations`

```
entity_mutations
  mutation_id     uuid (PK)
  run_id          uuid (FK → runs)
  entity_id       uuid (FK → entity_state)
  field           text
  old_value       jsonb
  new_value       jsonb
  caused_by_event uuid (FK → events)
  writer_type     text ('system_node' | 'agent' | 'platform')
  writer_id       text (node ID or agent ID)
  handler_step    text (e.g., 'data_accumulation', 'save_entity_field')
  created_at      timestamptz
```

#### What gets logged

Every mutation to `entity_state` produces a row:

- **System node writes**: `data_accumulation`, `compute` store results,
  `sets_gate`, `advances_to` (state field), `clear`. Already declarative
  in the contract — the runtime knows exactly which fields are written.

- **Agent writes**: `save_entity_field` tool calls. The tool executor
  reads the old value, writes the new value, and logs the diff in one
  transaction.

- **Accumulator state**: dimension counts, fan-out completion tracking,
  cycle counters.

#### What does NOT get logged

- Event payloads (already in `events` table)
- Agent conversation turns (already in `agent_turns` table)
- Tool call inputs/outputs (already in agent turn records)

#### Hot-path cost

One extra read (old value) + one insert per field write. Sub-millisecond
overhead against LLM calls at 1-30 seconds per turn.

---

### 2. Flight Recorder

The combination of four existing tables + the mutation log:

| What happened        | Where it lives |
|----------------------|----------------|
| Event fired          | `events` (type, payload, causal chain, timestamp) |
| Entity state changed | `entity_mutations` (field, old→new, caused by event) |
| Agent reasoned       | `agent_turns` (prompt, response, tool calls) |
| Event delivered      | `event_deliveries` (agent, status, error) |

#### Run reconstruction query

```sql
SELECT e.event_name, e.payload,
       m.field, m.old_value, m.new_value, m.writer_type, m.writer_id,
       e.created_at
FROM events e
LEFT JOIN entity_mutations m ON m.caused_by_event = e.event_id
WHERE e.run_id = :run_id
ORDER BY e.created_at, m.created_at
```

#### Entity timeline query

```sql
SELECT field, old_value, new_value, writer_type, writer_id, caused_by_event, created_at
FROM entity_mutations
WHERE entity_id = :entity_id
ORDER BY created_at
```

#### Drift detection

Verify `entity_state` matches the mutation log:

1. Start from blank entity
2. Forward-apply all mutations in timestamp order
3. Compare with current `entity_state`
4. Mismatch = a write that bypassed the log

---

### 3. Fork

#### Command

```
swarm fork --run <run_id> --at <event_id|timestamp> [--contracts <path>]
```

If `--at` receives an event ID, the system resolves it to that event's
`created_at` timestamp.

#### Steps

1. **Reconstruct state at T**: for every entity touched by the run,
   reverse-apply all mutations with `created_at > T` (newest first,
   set each field to `old_value`). Or forward-apply from empty for
   reliability.

2. **Detect pending work at T**: query for events with `created_at <= T`
   that have incomplete delivery chains:
   - deliveries with status `pending` or `in_progress` at T
   - accumulations that hadn't reached completion at T
   - entities in non-terminal states expecting further events

3. **Create forked run**: new row in `runs` with `forked_from_run_id`
   and `forked_from_event_id`. Copy reconstructed entity states.

4. **Boot**: load contracts (new path if `--contracts` specified,
   otherwise same contracts). Validate against reconstructed state.

5. **Resume**: re-deliver all pending events under the new run_id.
   The normal event loop takes over.

#### In-progress deliveries at T

If an agent was mid-turn at timestamp T (delivery status `in_progress`),
its partial mutations are excluded from reconstruction (only committed
mutations — from completed deliveries — are applied). The event is
treated as pending and re-delivered from scratch in the fork.

#### Timestamp precision

Events committed in the same millisecond are disambiguated by
`(created_at, event_id)` composite ordering. The fork point is
inclusive: mutations from the target event itself are included in
the reconstructed state; only mutations after it are undone.

---

### 4. What Changes in the Runtime

#### Required changes

| Component | Change |
|-----------|--------|
| Entity write path | Log mutation row in same transaction (save_entity_field, data_accumulation, compute, sets_gate, advances_to, clear) |
| Event creation | Stamp run_id on every new event (inherited from parent via causal chain) |
| Delivery creation | Stamp run_id (inherited from event) |
| Session creation | Stamp run_id |
| Turn creation | Stamp run_id |
| `runs` table | New table, created at boot |
| `entity_mutations` table | New table, created at boot |
| `swarm fork` command | New CLI command |

#### No changes

- Event bus routing — unchanged
- Agent prompts — unchanged
- System node contracts — unchanged
- Handler execution — unchanged (mutation logging is a write-path hook)
- Entity reads — still go directly to `entity_state`

---

### 5. Migration Path

1. **Ship `runs` table + `run_id` propagation** — thread run_id through
   event creation, deliveries, sessions, turns. All queries gain run
   scoping. Zero behavior change.

2. **Ship `entity_mutations` table + write hooks** — append-only logging.
   Zero impact on execution. Can be enabled per-environment.

3. **Ship flight recorder queries** — immediate debugging value from
   run reconstruction and entity timeline queries.

4. **Ship `swarm fork`** — state reconstruction + pending detection +
   re-delivery under new run.

5. **Ship drift detection** — `swarm verify --run <id>` validates
   entity_state against mutation log.

6. **Declare mutation log as source of truth** — once drift detection
   confirms completeness, `entity_state` becomes a rebuildable
   projection. Disaster recovery becomes: rebuild from log.

---

### 6. Future Extensions (Not in Scope Now)

- **Flight recorder UI**: visual step-through with entity state panel +
  event stream, click any event to see mutations + agent reasoning.

- **Selective replay**: re-run a specific agent's turn against
  reconstructed state with a modified prompt (prompt A/B testing).

- **Snapshot checkpoints**: periodic full-state snapshots for faster
  reconstruction on long-running pipelines (hours/days).

- **Garbage collection**: archive mutation history for terminated
  entities older than N days.

- **Live fork**: fork a running system without pausing it — snapshot
  state, let original continue, start fork independently.

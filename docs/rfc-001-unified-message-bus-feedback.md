# Feedback on RFC-001 Unified Message Bus & System Nodes

## Decision
**C. Modify RFC before proceeding.**

The direction is strong, but there are two high-risk jumps in the current proposal:
- removing interceptor semantics before introducing an explicit orchestration isolation model
- replacing Go state machines with YAML execution in one step

I recommend a staged path: keep runtime behavior stable, introduce system nodes first, then phase in declarative state machines.

## What Is Strong
- Correctly identifies the core bug class: deferred follow-on messages not re-entering orchestration logic.
- Correctly separates deterministic orchestration from LLM reasoning.
- Good migration intent (incremental, revertable).
- Unifying communication primitives is the right long-term simplification.

## Critical Gaps to Address in RFC

### 1) Orchestration visibility boundary is underspecified
If all messages fan out to all subscribers, orchestration traffic (`score_vertical_request`, gate updates, retry internals) will leak to agents that should not see it.

**Required change:** Add explicit delivery classes and enforcement:
- `delivery: directed`
- `delivery: broadcast_public`
- `delivery: broadcast_internal`

`broadcast_internal` must be restricted to system nodes unless explicitly allowlisted.

### 2) Transaction semantics regression risk
Today, publish + interceptor side effects are tightly coupled. Moving coordinator to async subscriber introduces cross-transaction windows and replay races.

**Required change:** Specify idempotency key and write-order contract for every system-node transition:
- `message_id + node_id` unique processing ledger
- state transition and emitted messages persisted in the same local transaction of the system node
- at-least-once delivery + exactly-once state transition

### 3) YAML runtime engine scope is too broad for first cut
A full declarative executor (accumulate/branch/gates/timers/loops) is effectively a workflow engine.

**Required change:** Split into phases:
1. System-node runtime in Go (typed handlers)
2. YAML as contract/validation artifact only
3. Optional later: executable YAML for specific low-risk flows

### 4) Recovery model needs explicit ack contract
The RFC says “replay unreceipted messages,” but not when receipts are written relative to state mutation.

**Required change:** define ack point precisely:
- receipt written **only after** successful state transition + persisted outgoing messages
- failures leave message unacked for retry
- dead-letter threshold and escalation behavior documented

### 5) Message schema model still ambiguous
`Schema` as string is fine, but schema versioning and compatibility policy are missing.

**Required change:**
- `schema_id` + `schema_version`
- backward/forward compatibility policy
- migration rule for consumers when schema changes

## Recommended Implementation Path (safe)

### Phase 1: Fix architecture without behavioral rewrite
- Keep current EventBus and contracts.
- Introduce system-node worker model as first-class runtime concept.
- Move **only scoring orchestration** from interceptor middleware into a system-node subscriber in Go.
- Keep message types unchanged for now.

### Phase 2: Introduce unified message envelope
- Add `to_node`, `schema_id`, optional `body` to persisted transport.
- Keep compatibility adapters so existing event producers/consumers still work.
- Add strict schema validation where schema exists; allow untyped legacy traffic via adapter.

### Phase 3: Expand system-node ownership
- Migrate validation/discovery/campaign orchestration one flow at a time.
- Add gate-level tests for each flow before removing old path.

### Phase 4: Decommission interceptor
- Only after parity tests and replay tests pass for all migrated flows.

### Phase 5 (optional): executable YAML
- Start with one narrow flow.
- Keep Go fallback for each workflow until parity and reliability are proven.

## Responses to RFC Questions

### Feasibility
- **Compatible** if staged.
- **Not safe** as one-step interceptor removal + YAML runtime introduction.

### Event consumption / who should see what
- Need explicit internal/public routing class; namespace alone is insufficient.

### Performance
- Async subscriber processing is acceptable for pipeline latency.
- Must preserve bounded queues + backpressure and avoid unbounded fan-out.

### YAML state machines
- Good as contracts first.
- Executable YAML should be deferred until system-node runtime is stable.

## Additional Comments
- Keep current verification rigor and add two new must-pass gates:
  - `orchestration-messages-not-delivered-to-agent-nodes-unless-allowlisted`
  - `system-node-replay-idempotency` (same message replayed N times does not duplicate transitions)
- Add an observability slice before broad migration:
  - per-node lag
  - per-message retry count
  - transition latency histograms
  - dead-letter dashboards by message type

## Bottom Line
Proceed with the architectural direction, but as a controlled migration:
- **System nodes first, behavior parity second, transport unification third, executable YAML last.**
- This preserves momentum while avoiding a high-risk runtime rewrite.

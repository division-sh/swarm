# RFC-001 v2: Unified Message Bus & System Nodes

**RFC ID:** RFC-001 v2  
**Status:** Revised draft (post-implementer feedback)  
**Date:** 2026-02-27  
**Decision Basis:** Original RFC + implementer feedback + runtime bug evidence

---

## 1. Executive Summary

This v2 keeps the strategic direction of RFC-001, but changes execution strategy:

- Keep moving toward a unified message model and explicit system nodes.
- Do **not** do a one-shot replacement of interceptor + Go logic + YAML execution.
- Execute as a staged migration with strict parity and idempotency gates.

**Core principle:**
System nodes first, behavior parity second, transport unification third, executable YAML last.

---

## 2. Why v2 Exists

The deferred-interceptor bug (`source.scraped -> vertical.discovered` emitted deferred, but no re-intercept) exposed a real architecture risk.

The original RFC solved the right problem but bundled three major changes together:

1. Interceptor removal
2. Unified message envelope
3. Executable declarative state machines

That bundling is too risky for runtime continuity.

---

## 3. What Changes from RFC v1

## 3.1 Accepted Direction

- Unified communication surface (event/message/task convergence)
- System nodes as first-class deterministic runtime participants
- Declarative pipeline contracts

## 3.2 Modified Execution Strategy

- YAML state machine execution is deferred.
- Interceptor removal is deferred until parity is proven.
- Transaction and replay contracts are formalized before migration.

## 3.3 New Required Guardrails

1. Delivery classes with enforcement (`directed`, `broadcast_public`, `broadcast_internal`)
2. Node-local idempotency ledger (`message_id + node_id`)
3. Explicit ack point (after state transition + persisted outgoing messages)
4. Schema versioning and compatibility policy

---

## 4. Architecture v2 (Target)

## 4.1 Unified Message Envelope

```go
type Message struct {
    ID             uuid.UUID
    Type           string
    From           string
    ToNode         *string
    VerticalID     *uuid.UUID
    Payload        map[string]any
    Body           *string
    SchemaID       *string
    SchemaVersion  *int
    DeliveryClass  string // directed | broadcast_public | broadcast_internal
    CreatedAt      time.Time
    Metadata       map[string]string
}
```

## 4.2 Delivery Semantics

- `directed`: must have `ToNode`; bus rejects otherwise.
- `broadcast_public`: fan-out to all subscribers.
- `broadcast_internal`: deliver only to system nodes unless allowlisted.

## 4.3 Node Types

- `agent` nodes: LLM-powered.
- `system` nodes: deterministic Go handlers.

Both use the same bus. Runtime enforces delivery and schema constraints.

---

## 5. Transaction & Replay Contract (Must-Pass)

For any system node handling message `M`:

1. Start local transaction.
2. Check idempotency ledger (`M.id`, `node_id`).
3. If already processed: no-op + ack-safe.
4. Apply state transition.
5. Persist outgoing messages in same transaction.
6. Persist processing receipt in same transaction.
7. Commit.
8. Fan-out outgoing messages.

If failure occurs before commit: message remains unacked for retry.

Dead-letter policy applies after configured retries and emits escalation.

---

## 6. Phased Plan

## Phase 1 (v2.0.37 scope): System Node Runtime + Scoring Only

- Add system-node runtime capability.
- Migrate scoring orchestration from interceptor path to `ScoringNode` (Go).
- Keep existing event types and compatibility behavior.
- Keep interceptor for all non-scoring flows.

### Phase 1 must-pass gates

- Scoring parity: old/new paths produce same outcomes.
- Replay idempotency: duplicate delivery causes no duplicate transitions.
- Dead-letter escalation: failed messages are surfaced, not dropped.

## Phase 2 (v2.0.38): Unified Message Fields + Compatibility Adapter

- Add envelope fields to storage and API.
- Introduce schema validation for typed messages.
- Legacy producers still work via adapter.

### Phase 2 must-pass gates

- `broadcast_internal` never leaks to non-allowlisted agent nodes.
- Schema validation rejects malformed typed payloads.

## Phase 3 (v2.0.39): Migrate Remaining Pipelines to System Nodes

- Validation
- Discovery accumulation
- Scan campaign lifecycle
- Directive translation

Each migration requires per-flow parity and replay tests.

## Phase 4 (v2.0.40): Remove Interceptor Path

Only after all migrated flows pass gates and run stable in prod-like tests.

## Phase 5 (v2.1.x, optional): Executable YAML

YAML is contract-only before this phase.

Start with one low-risk flow; keep Go fallback until parity proven.

---

## 7. Contracts Impact

## 7.1 New / Updated Contract Files

- `system-nodes.yaml` (node type, subscriptions, produced messages)
- `message-catalog.yaml` (delivery class, schema id/version)
- `schemas.yaml` (versioned message schemas + compatibility mode)
- `verification-gates.yaml` (new orchestration, replay, leakage gates)

## 7.2 Compatibility Rule

If sender still emits legacy event shape:
- Adapter maps legacy -> message envelope.
- Unset schema fields imply legacy/untyped mode.

---

## 8. Open Questions to Resolve Before Spec Lock

1. Should `broadcast_internal` be physically separate channel/queue or logical policy on one bus?
2. What is retry budget per system node before dead-letter (global vs per-message-type)?
3. Are schema compatibility modes global defaults or per-schema declarations only?
4. Should directed orchestration messages be persisted with delivery rows identical to broadcast rows (for replay symmetry)?

---

## 9. Implementation Acceptance Criteria

The migration is accepted only if all are true:

1. No discovered->scoring dead-path recurrence class.
2. No duplicate state transitions under replay.
3. No silent drops of orchestration messages.
4. No agent exposure to internal orchestration traffic unless allowlisted.
5. Full pipeline e2e passes with system-node ownership where migrated.

---

## 10. Final Position

Proceed with the RFC direction, but as a controlled migration.

- **Yes** to unified message model.
- **Yes** to system nodes.
- **Yes later** to executable YAML.
- **No** to one-step architecture replacement.

This path resolves the real bug class while preserving runtime safety.

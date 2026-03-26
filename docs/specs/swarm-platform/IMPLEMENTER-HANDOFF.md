# Implementer Handoff: v2.6.0 Runtime → Platform v1.2.0 / Empire v3.0.3

**Date:** 2026-03-10
**From:** Spec team
**To:** Runtime implementer
**Context:** Your Phase 11 report confirms v2.6.0 adoption is functionally complete. This document bridges you from where you are to the current spec state.

---

## Your 4 Questions — Answered

### 1. Gate-setting timing

Gates, state advance, and data writes are **independent** within the handler. They have no causal dependency on each other. All three commit in one atomic transaction. Your runtime can execute them in any order.

The dependency graph (replaces the old numbered list):

```
guard
  └→ accumulate
       └→ compute
            └→ on_complete / rules
                 └→ advances_to ─┐
                    sets_gate ────┤  (independent — any order)
                    data_accumulation ┘
                         └→ payload_transform
                              └→ emits
                                   └→ action
```

So for `research.completed`: write `business_brief` → set `g1_research` → advance to `mvp_speccing` is valid. So is any other order. The DB sees one commit.

### 2. Data accumulation timing

Same answer. `data_accumulation` is independent of `advances_to`. Your "data first, then advance" pattern is correct. Guards run at the top — they see pre-handler entity state. Data writes affect the NEXT handler's guard, not the current one.

### 3. Side-effect emit equivalence

`emits` is a normative handler step. The event is **persisted** within the atomic boundary and **delivered** after commit. `brand.requested`, `spec.revision_requested`, `vertical.killed` — these are handler outputs, not runtime side effects. Move them into handler-first execution.

### 4. Packaging/finalization for `vertical.ready_for_review`

The handler declares the complete packaging: `data_accumulation` writes (brand, research brief, spec, CTO notes) + `advances_to: ready_for_review` + `emits: mailbox event`. If your runtime does additional packaging not in the handler, the handler declaration is incomplete — tell us what's missing and we'll add it.

---
---

## Answers to New Implementer Questions (March 2026)

### Q1: Which source is authoritative when the handoff and YAML disagree?

**The YAML is always authoritative.** This handoff has been updated to match the checked-in YAML exactly. If you ever find a discrepancy, the YAML wins. The handoff is a reading guide, not a contract.

### Q2: Should I treat the handoff as the intended next spec?

No. Treat the YAML contracts as the spec. The handoff explains the delta from v2.6.0 and gives you a migration path. Read it for context, implement from the YAML.

### Q3: Is `action` strictly platform-only?

**Yes.** All legacy product actions (`set_gate`, `kill_vertical`, `finalize_validation`, `emit_spinup`, `accumulate_signal`, etc.) have been removed from the contracts. The only valid `action` values are `create_flow_instance` and `record_evidence`. Every other handler uses declarative fields: `advances_to`, `sets_gate`, `data_accumulation`, `emits`, `guard`, `rules`, `fan_out`, `accumulate`, `compute`, `query`, `clear`, `clear_gates`, `payload_transform`.

### Q4: Is `platform-spec.yaml` authoritative over prose and scripts?

**Yes.** `platform/contracts/platform-spec.yaml` is the authoritative specification. The prose overview (`platform-spec.md`) is a summary — when they conflict, the YAML wins. The verifier script (`verify.py`) is a reference implementation of the boot verification checks.



## What Changed: v2.6.0 → v1.1.0

### Architecture changes

| Area | v2.6.0 (your current) | v1.1.0 (target) |
|------|----------------------|-----------------|
| Execution model | 10-step numbered list | Dependency graph (see above) |
| Product hooks | 8 Go functions | 0 — all declarative YAML |
| Flows | 4 flows, flat namespace | 4 flows + root, mode: static/template |
| Operating instances | Single | Dynamic via `create_flow_instance` |
| Addressing | Flat event names | Hierarchical: `flow/event`, wildcards `flow/*/event` |
| Expression language | Unspecified | CEL (Common Expression Language) |
| System nodes | 5 | 7 (build-orchestrator split from lifecycle-orchestrator) |
| Agents | 28 | 29 (analysis-agent-secondary for anti-bias) |
| Handler count | 47 | 59 |
| Events | ~175 | 192 |

### New platform concepts you'll need

**1. Flow modes (`package.yaml`)**

```yaml
flows:
  - id: discovery
    flow: discovery
    mode: static        # one instance at boot
  - id: operating
    flow: operating
    mode: template      # zero at boot, created at runtime
```

**2. Dynamic flow instances**

`create_flow_instance` is a platform action. Portfolio-node handler calls it when a vertical is approved. The platform loads the flow template, constructs paths, registers everything, starts nodes and agents.

```yaml
opco.spinup_requested:
  action: create_flow_instance
  template: operating
  instance_id_from: payload.vertical_id
  config_from:
    vertical_name: payload.vertical_name
    mandate_document: payload.mandate
    brand_name: payload.brand
    geography: payload.geography
    tech_stack: payload.tech_stack
```

**3. `auto_emit_on_create`**

Operating schema declares `auto_emit_on_create: opco.ceo_ready`. Platform emits this event after instance creation — no manual trigger needed.

**4. Wildcard subscriptions**

Empire-coordinator subscribes to `operating/*/opco.steady_state_reached`. The `*` matches any dynamic instance. When a new OpCo is created, the wildcard auto-expands.

**5. CEL expressions**

All guard checks, rule conditions, and filter conditions are CEL:

```yaml
guard:
  check: "entity.generation_depth <= policy.max_derivation_depth"
```

Go implementation: `github.com/google/cel-go`

**6. Timer model**

Timers declared on nodes with start/cancel conditions:

```yaml
timers:
  - id: validation_gate_timeout
    event: timer.validation_timeout
    delay: "{{validation_gate_timeout_hours}}h"
    start_on: state:researching
    cancel_on: state:ready_for_review
```

**7. Error model**

- Handler failure: 3 retries, exponential backoff, then dead letter
- Chain depth limit: 50 chained emissions max, then dead letter
- Guard `on_fail` options: reject, kill, discard, escalate:{event}

**8. Boot verification**

19 checks run at boot. 6 are errors (abort), 5 are warnings (log). See `boot_verification` in `platform-spec.yaml`. Reference implementation: `verify.py` in repo root.

---

## Your 6 Deferred Events — Handler Declarations

These are the handlers your runtime still runs flat-transition. Here's exactly what the spec declares for each, so you can verify parity:

### `vertical.shortlisted`

```yaml
vertical.shortlisted:
  advances_to: researching
  emits: validation.started
```

Cross-flow handoff: scoring emits, validation consumes. State goes to `researching` (first validation gate), not `shortlisted`.

### `research.completed`

```yaml
research.completed:
  sets_gate: g1_research
  data_accumulation:
    writes: [business_brief, research_context]
    source_event: research.completed
  advances_to: mvp_speccing
  emits: spec.requested
```

Four independent writes in one atomic transaction. Order is cosmetic.

### `cto.spec_approved`

```yaml
cto.spec_approved:
  sets_gate: g3_cto
  data_accumulation:
    writes:
      - source_field: cto_notes
        target_field: cto_feasibility
    source_event: cto.spec_approved
  advances_to: branding
  emits: brand.requested
```

Gate, data write, state advance, emit — all independent, all atomic.

### `vertical.ready_for_review`

```yaml
vertical.ready_for_review:
  advances_to: ready_for_review
  emits: mailbox.review_requested
```

All gates passed. Packaging happens via data_accumulation on the preceding gate handlers (research.completed, spec.approved, cto.spec_approved, brand.candidates_ready). This handler just advances state and notifies the mailbox.

### `vertical.approved`

```yaml
vertical.approved:
  advances_to: approved
  emits: opco.spinup_requested
```

Lifecycle handoff. The `opco.spinup_requested` event triggers `create_flow_instance` on portfolio-node.

### `vertical.needs_more_data`

```yaml
vertical.needs_more_data:
  clear_gates: true
  advances_to: researching
  emits: research.additional_requested
```

Human reset path. Gates are cleared, state goes back to `researching`, agent gets new research task.

---

## Runtime Bridge

The `runtime/` directory still works. Merged flat files are auto-generated from flow contracts. Your loader can keep reading them. When you're ready for multi-file loading, the flow-packaged contracts are the target.

---

## Migration Path

### Phase 12: Promote remaining 6 events to handler-first

With the dependency graph model, your concern about ordering is resolved — the three independent writes (advances_to, sets_gate, data_accumulation) can happen in any order. Promote all 6 events.

### Phase 13: CEL integration

Replace hardcoded guard checks with CEL evaluation. Guards, rule conditions, on_complete branch conditions all become CEL expressions.

### Phase 14: Dynamic flow instances

Implement `create_flow_instance` action. Load flow template at runtime, construct paths, expand wildcards.

### Phase 15: Timer engine

Implement durable timers with start_on/cancel_on lifecycle.

### Phase 16: Boot verification

Go boot sequence already implements semantic validation (CEL parsing, contract checking). verify.py is a supplementary structural linter, not the reference verifier. Errors abort, warnings log.

---

## Files to Read

| Priority | File | Why |
|----------|------|-----|
| 1 | `platform/contracts/platform-spec.yaml` → `handler_execution_order` | Dependency graph model |
| 2 | `platform/contracts/platform-spec.yaml` → `system_node_specification` | Every handler field defined |
| 3 | `platform/contracts/platform-spec.yaml` → `engine` | Event loop, atomicity, state management |
| 4 | `<contracts-root>/flows/validation/nodes.yaml` | Your 6 deferred handlers |
| 5 | `<contracts-root>/flows/operating/nodes.yaml` | New build-orchestrator |
| 6 | `verify.py` | Boot verification reference implementation |

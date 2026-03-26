# Swarm Platform — Compliance Test Catalog

Every platform capability gets one test. Tests compose gradually from simple to complex.

## Test Naming: `test-{category}-{capability}-{variant}`

## Tier 1: Primitive Handler Fields (24 tests)

### Guard (8)
- test-guard-pass
- test-guard-reject (default on_fail)
- test-guard-discard
- test-guard-kill
- test-guard-escalate
- test-guard-multi (all must pass)
- test-guard-multi-fail (second fails)
- test-guard-policy-ref (CEL reads policy)

### State (4)
- test-advances-to
- test-advances-to-terminal
- test-sets-gate
- test-clear-gates

### Data (3)
- test-data-accumulation-direct
- test-data-accumulation-mapped
- test-record-evidence

### Emit (3)
- test-emits-single
- test-emits-multiple
- test-emits-payload-transform

### Rules (4)
- test-rules-match
- test-rules-no-match
- test-rules-data-accumulation
- test-rules-advances-to

### On Complete (3 — MUST be ordered list)
- test-on-complete-first-match
- test-on-complete-second-match
- test-on-complete-with-state

## Tier 2: Accumulation (8 tests)
- test-accumulate-all
- test-accumulate-partial
- test-accumulate-idempotent
- test-accumulate-timeout
- test-accumulate-threshold
- test-accumulate-with-compute
- test-accumulate-expected-from-entity
- test-accumulate-crash-recovery

## Tier 3: List Processing (10 tests)
- test-fan-out-basic
- test-fan-out-count
- test-fan-out-emit-mapping
- test-fan-out-empty
- test-reduce-weighted-average
- test-reduce-pick-or-average
- test-reduce-sum
- test-reduce-count
- test-filter-basic
- test-filter-empty

## Tier 4: Cross-Entity (4 tests)
- test-query-group-by
- test-query-filter
- test-clear-state
- test-create-entity

## Tier 5: Flow Lifecycle (10 tests)
- test-create-flow-instance
- test-create-flow-instance-duplicate
- test-create-flow-instance-config
- test-auto-emit-on-create
- test-wildcard-subscription
- test-timer-fire
- test-timer-cancel
- test-timer-recurring
- test-terminal-state-preserves
- test-terminal-state-rejects

## Tier 6: Event Loop (8 tests)
- test-event-validation
- test-event-persisted-before-delivery
- test-entity-serialization
- test-cross-entity-concurrent
- test-atomicity-commit
- test-atomicity-rollback
- test-chain-depth-limit
- test-dead-letter

## Tier 7: Composition (6 tests)
- test-two-node-chain
- test-agent-emits-to-node
- test-cross-flow-subscription
- test-wildcard-cross-flow
- test-multi-gate-pipeline
- test-full-lifecycle

## Tier 8: Boot Verification (11 tests)
- test-boot-payload-mismatch (error)
- test-boot-required-agent-missing (error)
- test-boot-handler-field-undefined (error)
- test-boot-tool-missing (error)
- test-boot-deprecated-field (error)
- test-boot-cel-parse-error (error)
- test-boot-event-no-schema (warning)
- test-boot-event-no-consumer (warning)
- test-boot-event-no-producer (warning)
- test-boot-prompt-missing (warning)
- test-boot-policy-conflict (warning)

## Total: 81 tests

| Tier | Tests | Coverage |
|------|-------|----------|
| 1. Primitives | 24 | Every handler field |
| 2. Accumulation | 8 | All completion modes + edge cases |
| 3. List Processing | 10 | fan_out, reduce, filter |
| 4. Cross-Entity | 4 | query, clear, create |
| 5. Flow Lifecycle | 10 | Dynamic instances, timers, terminal |
| 6. Event Loop | 8 | Atomicity, serialization, error model |
| 7. Composition | 6 | Multi-node, cross-flow, full lifecycle |
| 8. Boot Verification | 11 | All 11 boot checks |

## Test Package Format

```
tests/test-{name}/
  package.yaml
  schema.yaml
  nodes.yaml
  events.yaml
  agents.yaml
  expected.yaml
```

### expected.yaml

```yaml
trigger:
  event: {event_name}
  payload: {fields}
  entity_state_before: {state}
  entity_fields_before: {fields}
  gates_before: {gates}

expected:
  entity_state: {state}
  gates: {gate: value}
  emitted_events: [{event_name}]
  entity_fields: {field: value}
  handler_outcome: success | discard | reject | kill | escalate
  dead_letter: true | false
  error: {message_pattern}         # boot verification tests only
```

## Test Writer Agent Brief

You are writing compliance gate tests for the Swarm Platform engine. Each test is a minimal self-contained YAML flow package that exercises exactly one platform capability.

Rules:
1. One capability per test. No test exercises two things.
2. Minimal contracts. Only declare what the test needs.
3. Use the simplest possible schema (2-3 states max).
4. Test names are self-documenting.
5. expected.yaml is the assertion contract. Be precise.
6. Tier 1 tests have zero dependencies. Tier 2+ may reference Tier 1 patterns.
7. Boot verification tests intentionally break contracts to verify the platform catches it.
8. For negative tests (guard-reject, boot errors), the expected outcome IS the failure.

---

## Tier 9: Empire Integration Tests (16 tests)

These tests use actual Empire contracts (not minimal test flows) and trace real event chains.

### Discovery Flow (3)
- test-empire-discovery-scan-dispatch — scan.requested with mode=saas_gap → fan_out → 3 scan_assigned events
- test-empire-discovery-signal-accumulate — 3 scan_complete events → accumulation → vertical.discovered emitted
- test-empire-discovery-dedup — dedup.resolved with strategy=keep_best → correct signal preserved

### Scoring Flow (3)
- test-empire-scoring-init — vertical.discovered → rubric selected by mode → scoring.requested emitted with dimensions
- test-empire-scoring-accumulate — 11 dimension scores arrive → composite computed → shortlisted/marginal/killed classification
- test-empire-scoring-derivation-guard — vertical.derived with depth=3 → discard (exceeds max_derivation_depth=2)

### Validation Flow (4)
- test-empire-validation-full-pipeline — vertical.shortlisted → research → spec → CTO → brand → ready_for_review (all 4 gates)
- test-empire-validation-spec-revision — spec.validation_failed → revision_count < 3 → spec.revision_requested loop
- test-empire-validation-revision-limit — spec.validation_failed → revision_count = 3 → escalate:validation.revision_limit_reached
- test-empire-validation-kill-backprop — cto.spec_vetoed → killed + vertical.killed_backprop to scoring

### Operating Flow (4)
- test-empire-operating-spinup — opco.spinup_requested → create_flow_instance → auto_emit opco.ceo_ready
- test-empire-operating-build-pipeline — product_spec_ready → tech_spec_ready → qa.validation_passed → deploy.authorized (gate enforcement)
- test-empire-operating-build-gate-reject — deploy.requested without g_qa_passed → reject
- test-empire-operating-teardown — opco.teardown_requested → winding_down → opco.teardown_complete → killed

### Cross-Flow (2)
- test-empire-cross-discovery-to-scoring — vertical.discovered from discovery → scoring-node picks it up and inits scoring
- test-empire-cross-scoring-to-validation — vertical.shortlisted from scoring → validation-orchestrator starts pipeline

## Tier 10: Empire Policy Tests (8 tests)

These verify Empire-specific policy thresholds and business rules.

- test-empire-policy-composite-shortlist — score 75+ → shortlisted, score 55-74 → marginal, score <55 → killed
- test-empire-policy-hard-gate-build — build_complexity < 50 → killed regardless of composite
- test-empire-policy-hard-gate-automation — automation_completeness < 50 → killed regardless of composite
- test-empire-policy-derivation-signal — signal_strength < 40 → derivation discarded
- test-empire-policy-derivation-icp — icp_crispness < 50 → derivation discarded
- test-empire-policy-timeout-validation — 72h timeout in validation → timer.validation_timeout
- test-empire-policy-timeout-operating — 168h timeout in operating → timer.operating_timeout
- test-empire-policy-anti-bias — scoring.requested → analysis-agent, scoring.derived_requested → analysis-agent-secondary (different pools)

---

## Updated Total: 105 tests

| Tier | Tests | Coverage |
|------|-------|----------|
| 1. Primitives | 24 | Every handler field |
| 2. Accumulation | 8 | All completion modes + edge cases |
| 3. List Processing | 10 | fan_out, reduce, filter |
| 4. Cross-Entity | 4 | query, clear, create |
| 5. Flow Lifecycle | 10 | Dynamic instances, timers, terminal |
| 6. Event Loop | 8 | Atomicity, serialization, error model |
| 7. Composition | 6 | Multi-node, cross-flow, full lifecycle |
| 8. Boot Verification | 11 | All 11 boot checks |
| 9. Empire Integration | 16 | Real Empire event chains end-to-end |
| 10. Empire Policy | 8 | Business rules and thresholds |

---

## Agent Mock Layer

Tests run with zero LLM tokens. When an event routes to an agent, the test harness intercepts and replays a fixture instead of calling the LLM.

### Fixture Format

Each test package may include `fixtures.yaml`:

```yaml
agent_fixtures:
  # Agent ID → list of scenarios
  analysis-agent:
    - on: scoring.requested
      condition: "payload.mode == 'saas_gap'"    # optional CEL filter
      emits:
        - event: score.dimension_complete
          payload: { entity_id: "{{entity_id}}", dimension: build_complexity, score: 85 }
        - event: score.dimension_complete
          payload: { entity_id: "{{entity_id}}", dimension: automation_completeness, score: 78 }
        - event: score.dimension_complete
          payload: { entity_id: "{{entity_id}}", dimension: icp_crispness, score: 72 }
        # ... remaining dimensions

  business-research-agent:
    - on: validation.started
      emits:
        - event: research.completed
          payload:
            entity_id: "{{entity_id}}"
            business_brief: "Test brief content"
            research_context: { market_size: 1000000, competitors: 3 }

  lightweight-spec-agent:
    - on: spec.requested
      emits:
        - event: spec.draft_ready
          payload:
            entity_id: "{{entity_id}}"
            final_spec: "Test MVP specification"

  opco-ceo:
    - on: opco.ceo_ready
      emits:
        - event: opco.spend_request
          payload: { entity_id: "{{entity_id}}", amount: 100, reason: "Initial setup" }
      # CEO delegates to CTO via agent_message — mock as direct event
    - on: build_complete
      emits:
        - event: launch_ready
          payload: { entity_id: "{{entity_id}}" }
```

### Fixture Rules

1. `{{entity_id}}` is substituted with the test entity ID at runtime.
2. `condition` is optional CEL — if present, fixture only fires when condition matches.
3. Multiple fixtures for the same agent + event: first matching condition wins.
4. If no fixture matches, the event is consumed silently (agent did nothing).
5. Fixtures emit events into the event loop — normal processing continues.
6. Fixture emissions respect chain_depth_limit.

### Test Tiers and Fixtures

| Tier | Fixtures needed | Why |
|------|----------------|-----|
| 1-4 | None | Tests exercise system nodes only |
| 5 | None | Flow lifecycle is platform-level |
| 6 | None | Event loop is platform-level |
| 7 | Minimal | Composition tests may need one agent fixture |
| 8 | None | Boot verification doesn't execute handlers |
| 9 | Full | Empire integration needs fixtures for every agent in the chain |
| 10 | Minimal | Policy tests mostly exercise system node guards |

### Empire Integration Fixture Sets

Pre-built fixture sets for common Empire test scenarios:

```
tests/fixtures/
  happy-path.yaml          # All agents succeed, vertical goes discovery → operating
  scoring-marginal.yaml    # Scores land in marginal range (55-74)
  scoring-killed.yaml      # Hard gate failure (build_complexity < 50)
  validation-revision.yaml # Spec fails review twice, passes third time
  validation-kill.yaml     # CTO vetoes spec
  operating-teardown.yaml  # OpCo reaches steady state then tears down
```

Each fixture set provides agent responses for every agent in the chain, allowing a full end-to-end test without tokens.

### Three Testing Modes

```go
// Mode 1: Node-only (Tiers 1-6, 8)
// No agents involved. Events injected directly.
engine.InjectEvent(trigger)
assert(engine.EntityState() == expected)

// Mode 2: Fixture-mocked (Tiers 7, 9-10)
// Agents replaced with fixtures. Full event chain.
engine.SetFixtures(loadFixtures("happy-path.yaml"))
engine.InjectEvent(trigger)
// Events flow through nodes AND mocked agents
assert(engine.EntityState() == expected)

// Mode 3: Live agent (future, sparingly)
// Real LLM calls. Expensive. For agent prompt validation only.
engine.SetLiveAgents(true)
engine.InjectEvent(trigger)
// Actual LLM reasoning happens
assert(engine.EntityState() == expected)
```

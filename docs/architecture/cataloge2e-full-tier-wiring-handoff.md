# Catalog E2E — Wire All Tiers

## Context

The `cataloge2e` harness (runtime_harness.go, assertions.go, scripted_llm.go, helpers_test.go) is production-grade and already passes 20 fixtures across Tiers 5, 6, and 8. The harness boots a real runtime with Postgres, loads fixture YAML as the workflow module, and publishes events through the real EventBus.

**The task**: wire the remaining fixtures across Tiers 1–4, 7, 9–11. For system_node-only tiers (1–4, 9, 10) this is pure wiring — test runner files, assertion extensions, and fixture classification. For agent tiers (7, 11) there is additional stabilization work: the current harness runs with `start=false` (no background processing), which works for system_node event handling but agent execution likely requires `start=true` with the full runtime loop running. Stabilizing the started-runtime path for E2E is part of the agent tier scope.

## Fixture inventory

| Tier | Dir | Fixtures | Has `fixtures.yaml` | Notes |
|------|-----|----------|---------------------|-------|
| 1 | tier1-primitives | 32 | No | Pure system_node, no agents |
| 2 | tier2-accumulation | 10 | No | System_node with accumulate, needs `entity_fields_before` seeding |
| 3 | tier3-list-processing | 14 | No | Fan-out, filter, reduce, group-by |
| 4 | tier4-cross-entity | 5 | No | Cross-entity query/create/clear |
| 5 | tier5-flow-lifecycle | 12 | No | **1 wired**, 11 remaining |
| 6 | tier6-event-loop | 10 | No | **1 wired**, 9 remaining |
| 7 | tier7-composition | 7 | 2 fixtures have `fixtures.yaml` | Agent + node composition |
| 8 | tier8-boot-verification | 28 | No | **18 wired**, 10 excluded (classified) |
| 9 | tier9-composition-patterns | 12 | No | Multi-primitive composition |
| 10 | tier10-policy-patterns | 6 | No | Policy evaluation |
| 11 | tier11-flow-composition | 16 | No | Multi-flow, child flows, pins |

## Delivery order

Work in this order. Each tier builds on the previous.

### Phase A: Tiers 1–4 (system_node only, no agents, no flows)

These are pure declarative node tests. No LLM involvement. The trigger fires an event, the system_node handler processes it, state advances, events emit.

**Step 1: Extend `tier56ExpectedDocument` for missing fields**

The current struct covers `entity_state`, `emitted_events`, `dead_letter`, `flow_instance_created`, `template_instances`, and `handler_outcome` (already asserted via `assertHandlerOutcome`). The fixtures also use:

- `entity_fields_before` (23 fixtures) — pre-seeds entity fields before trigger. Lives under `trigger.entity_fields_before` in expected.yaml. Extend `seedInitialState` or add a new seeding method.
- `entity_fields` (47 fixtures) — asserts entity field values post-execution. Add `assertEntityFields`.

Rename `tier56ExpectedDocument` to something generic like `catalogExpectedDocument` since it's used across all tiers.

**Step 2: Create `tier1_primitives_e2e_test.go`**

```go
var tier1PrimitiveFixtures = []string{
    "test-advances-to",
    // ... start with 5, expand as they pass
}

func TestTier1PrimitiveCatalogFixtures_RealRuntime(t *testing.T) {
    repoRoot := repoRootFromCatalogE2E(t)
    for _, fixtureName := range tier1PrimitiveFixtures {
        fixtureRoot := filepath.Join(repoRoot, "tests", "tier1-primitives", fixtureName)
        t.Run(fixtureName, func(t *testing.T) {
            var expected catalogExpectedDocument
            loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)
            h := newRuntimeHarness(t, fixtureRoot, false)
            h.seedEntityFields(expected) // new: seeds entity_fields_before
            for _, step := range expected.triggerSequence() {
                h.publishAndWait(step, 2*time.Second)
            }
            assertCatalogRuntimeOutcome(t, h, expected)
        })
    }
}
```

Same pattern for Tiers 2, 3, 4. Literally copy-paste with the tier dir name changed.

**Step 3: `entity_fields_before` seeding**

Some fixtures (mostly Tier 2 accumulation and Tier 3 list-processing) set `entity_fields_before` under `trigger` in expected.yaml. Example from `test-accumulate-with-compute/expected.yaml`:

```yaml
trigger:
  sequence:
    - event: item.arrived
      payload: { entity_id: ent-001, item_id: a }
  entity_fields_before:
    expected_count: 3
```

Note: `entity_fields_before` is a child of `trigger`, not a top-level key. This means the entity already has `expected_count=3` in its state before the trigger fires. Extend the harness:

```go
func (h *runtimeHarness) seedEntityFields(expected catalogExpectedDocument) {
    // If entity_fields_before is set, upsert entity state with those fields
    // This populates the state_schema fields on the workflow instance
}
```

How you implement this depends on how `WorkflowInstanceStore` stores extra fields. Check if it has a `Metadata` or `Fields` map.

**Step 4: `entity_fields` post-assertion**

```go
func assertEntityFields(t testing.TB, db *sql.DB, workflow *WorkflowInstanceStore, entityID string, want map[string]any) {
    // Load workflow instance, compare fields from state_schema
}
```

### Phase B: Tier 7 (agent + node composition)

**Important**: Tier 7 fixtures involve agent execution, not just system_node handlers. The current harness uses `start=false` (runtime not fully started), which works for system_node event dispatch but likely won't work for agent tiers where the runtime loop must be running to dispatch events to agents. You will need to stabilize the `start=true` path — expect extra noise from background processing and adjust assertions or quiescence detection accordingly.

Two of 7 fixtures have `fixtures.yaml` specifying agent behavior:

```yaml
# fixtures.yaml
agent_fixtures:
  test-agent:
    - on: task.assigned
      emits:
        - event: task.completed
          payload: { entity_id: "{{entity_id}}" }
```

This tells you exactly what the scripted LLM should do: when `test-agent` receives `task.assigned`, it should emit `task.completed`.

**Important — per-event tool names**: This runtime does NOT use a generic `emit_event` tool. It generates per-event tools via `EmitToolName` and `GenerateEmitToolsForRole` (see `executor_emit_normalization.go`). For example, emitting `task.completed` requires a tool call to `emit_task_completed`, not `emit_event`. The scripted LLM responses must use the correct per-event tool names.

**Step 1: Load `fixtures.yaml` in the harness**

```go
type agentFixtureDoc struct {
    AgentFixtures map[string][]agentFixtureStep `yaml:"agent_fixtures"`
}

type agentFixtureStep struct {
    On    string              `yaml:"on"`
    Emits []agentFixtureEmit  `yaml:"emits"`
}

type agentFixtureEmit struct {
    Event   string         `yaml:"event"`
    Payload map[string]any `yaml:"payload"`
}
```

**Step 2: Convert `fixtures.yaml` into scripted LLM responses**

When the agent receives an event, the runtime sends the event as a user message to the LLM session. The LLM response should be a tool call to `emit_event`. Map each `agentFixtureStep` to a `SetResponse` call:

```go
func (h *runtimeHarness) loadAgentFixtures(fixtureRoot string) {
    path := filepath.Join(fixtureRoot, "fixtures.yaml")
    if _, err := os.Stat(path); os.IsNotExist(err) {
        return // no agent fixtures
    }
    var doc agentFixtureDoc
    loadYAML(h.t, path, &doc)
    for agentID, steps := range doc.AgentFixtures {
        for _, step := range steps {
            // Build an llm.Response with tool_calls matching the emit spec
            response := buildLLMResponseFromFixture(step)
            h.llm.SetResponse(agentID, step.On, response)
        }
    }
}
```

The key is knowing the exact format the runtime uses for the user message content (the key in `SetResponse`). Check what `ContinueSession` receives as `message.Content` when an event is dispatched to an agent. It's likely the event name or a JSON representation of the event. Match that format.

**Step 3: Per-event tool call format**

The scripted LLM response must use per-event tool names. For a fixture entry `emits: [{event: task.completed, payload: {...}}]`, the tool call name is `emit_task_completed` (dots replaced with underscores, prefixed with `emit_`). Check `EmitToolName` in `executor_emit_normalization.go` for the exact naming convention. Build the `llm.Response` accordingly:

```go
func buildLLMResponseFromFixture(step agentFixtureStep) llm.Response {
    // For each entry in step.Emits:
    //   toolName := EmitToolName(entry.Event)  // e.g. "emit_task_completed"
    //   Build llm.ToolCall with that name and the payload as arguments
    // Return llm.Response with those ToolCalls
}
```

Only 2 fixtures use this. Start with `test-agent-emits-to-node` and `test-dual-delivery`.

### Phase C: Tiers 5–6 remaining fixtures

Expand the whitelists in `tier5_lifecycle_e2e_test.go` and `tier6_event_loop_e2e_test.go`. For each fixture currently excluded:

1. Add it to the whitelist
2. Run the test
3. If it passes, done
4. If it fails, classify as:
   - **fixture-issue**: wrong YAML, fix the fixture
   - **validation-gap**: runtime doesn't implement this behavior yet, add to excluded list with comment
   - **harness-gap**: needs a harness feature (e.g. timer support), document what's needed

Known blockers from previous work:
- `test-create-flow-instance`: UNDEFINED-FIELD on `action_params` — debug which contract guard rejects it
- `test-dead-letter`: doesn't actually dead-letter in live runtime — trace event routing to find why

Timer fixtures (`test-timer-start-on`, `test-timer-fire`, `test-timer-cancel`, `test-timer-recurring`) may need special handling — the harness currently has no way to advance time. Either use short real timeouts or add a mock clock.

### Phase D: Tiers 9 (composition patterns), 10, 11

Same pattern as Phase A but these test more complex scenarios:

- **Tier 9 composition**: Multi-event sequences, accumulate+compute+branch combos. These should work if Tiers 1–4 work. Pure system_node, no agent stabilization needed.
- **Tier 10 policy**: Policy evaluation (capacity queries, counter escalation, threshold, timeout). These may need policy engine support in the harness.
- **Tier 11 flow composition**: Child flows, pin wiring, nested flows, policy inheritance. These need multi-flow loading — `loadFixtureBundle` may need to handle the `flows/` subdirectory in the fixture root. Some Tier 11 fixtures involve agents (e.g. `test-required-agents-child`), so the same `start=true` stabilization from Phase B applies here.

### Phase E: Classification gates

After each tier is wired, add a classification test (same as Tier 8):

```go
func TestTierN_AreExplicitlyClassified(t *testing.T) {
    // List all fixture dirs
    // Assert each is in supported, excluded-validation-gap, or excluded-fixture-issue
}
```

This prevents new fixtures from being silently ignored.

## Assertion gaps to close

| Feature | Current | Needed |
|---------|---------|--------|
| `handler_outcome` | **Done** — `assertHandlerOutcome` active and green | None |
| `entity_fields_before` | Not supported | Seed entity fields before trigger (under `trigger.entity_fields_before`) |
| `entity_fields` | Not supported | Assert entity field values post-execution |
| `emitted_events` with duplicates | Deduplicated by name | Tier 3 fan-out expects 3x `scan.assigned` — need count-aware matching |
| Agent scripting via `fixtures.yaml` | Not loaded | Load and convert to per-event `SetResponse` calls |
| Started-runtime path for agents | `start=false` only | Stabilize `start=true` for Tier 7/11 agent execution |

**Important**: The current `assertEmittedEvents` deduplicates events by name (`seen` map). Tier 3 `test-fan-out-basic` expects `[scan.assigned, scan.assigned, scan.assigned]`. You need a count-aware mode — either always count, or switch based on whether the expected list has duplicates.

## Definition of done

- [ ] All 144 real fixtures (Tiers 1–8, 9-composition, 10, 11) have a test runner
- [ ] Each fixture is explicitly classified: supported, excluded-validation-gap, or excluded-fixture-issue
- [ ] Classification gate test exists per tier
- [ ] `entity_fields_before`, `entity_fields` assertions work
- [ ] `fixtures.yaml` agent scripting works for Tier 7 using per-event tool names (`emit_task_completed`, not `emit_event`)
- [ ] `emitted_events` supports duplicate event names (count-aware)
- [ ] `start=true` runtime path stabilized for agent tiers (7, 11)
- [ ] All supported fixtures pass in CI
- [ ] `sessions.test` binary deleted from repo root

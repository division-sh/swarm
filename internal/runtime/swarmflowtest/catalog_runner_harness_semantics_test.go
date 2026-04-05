package swarmflowtest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCatalogRunner_AccumulationRemainsPendingUntilExpectedCount(t *testing.T) {
	dir := t.TempDir()
	writeCatalogCaseFile(t, dir, "package.yaml", "name: pending-accumulator\n")
	writeCatalogCaseFixture(t, dir,
		`
initial_state: collecting
terminal_states: [complete]
states: [collecting, complete]
pins:
  inputs:
    events: [item.arrived]
  outputs:
    events: [collection.done]
`,
		`
test-node:
  id: test-node
  execution_type: system_node
  subscribes_to: [item.arrived]
  produces: [collection.done]
  event_handlers:
    item.arrived:
      accumulate:
        expected_from: entity.expected_count
        completion: all
      advances_to: complete
      emits: collection.done
`,
		"{}\n",
		`
trigger:
  sequence:
    - event: item.arrived
      payload: {entity_id: 11111111-1111-4111-8111-111111111111, item_id: a}
    - event: item.arrived
      payload: {entity_id: 11111111-1111-4111-8111-111111111111, item_id: b}
  entity_fields_before:
    expected_count: 3

expected:
  handler_outcome: success
  entity_state: collecting
  emitted_events: []
`,
	)

	result, expected := runSimpleCatalogCase(t, dir)
	if got, want := result.handlerOutcome, expected.Expected.HandlerOutcome; got != want {
		t.Fatalf("handler outcome = %q, want %q", got, want)
	}
	if got, want := result.entityState, expected.Expected.EntityState; got != want {
		t.Fatalf("entity state = %q, want %q", got, want)
	}
	if diff := diffStringSet(normalizeSorted(result.emittedEvents), normalizeSorted(expected.Expected.EmittedEvents)); diff != "" {
		t.Fatalf("emitted events mismatch (%s)", diff)
	}
}

func TestCatalogRunner_AccumulationThresholdCompletesAtThreshold(t *testing.T) {
	dir := t.TempDir()
	writeCatalogCaseFile(t, dir, "package.yaml", "name: threshold-accumulator\n")
	writeCatalogCaseFixture(t, dir,
		`
initial_state: collecting
terminal_states: [complete]
states: [collecting, complete]
pins:
  inputs:
    events: [item.arrived]
  outputs:
    events: [collection.threshold_met]
`,
		`
test-node:
  id: test-node
  execution_type: system_node
  subscribes_to: [item.arrived]
  produces: [collection.threshold_met]
  event_handlers:
    item.arrived:
      accumulate:
        expected_from: entity.threshold_count
        completion: threshold
      advances_to: complete
      emits: collection.threshold_met
`,
		"{}\n",
		`
trigger:
  sequence:
    - event: item.arrived
      payload: {entity_id: 11111111-1111-4111-8111-111111111111, item_id: a}
    - event: item.arrived
      payload: {entity_id: 11111111-1111-4111-8111-111111111111, item_id: b}
    - event: item.arrived
      payload: {entity_id: 11111111-1111-4111-8111-111111111111, item_id: c}
  entity_fields_before:
    threshold_count: 2

expected:
  handler_outcome: success
  entity_state: complete
  emitted_events: [collection.threshold_met]
`,
	)

	result, expected := runSimpleCatalogCase(t, dir)
	if got, want := result.handlerOutcome, expected.Expected.HandlerOutcome; got != want {
		t.Fatalf("handler outcome = %q, want %q", got, want)
	}
	if got, want := result.entityState, expected.Expected.EntityState; got != want {
		t.Fatalf("entity state = %q, want %q", got, want)
	}
	if diff := diffStringSet(normalizeSorted(result.emittedEvents), normalizeSorted(expected.Expected.EmittedEvents)); diff != "" {
		t.Fatalf("emitted events mismatch (%s)", diff)
	}
}

func TestCatalogRunner_AccumulationIgnoresDuplicateItems(t *testing.T) {
	dir := t.TempDir()
	writeCatalogCaseFile(t, dir, "package.yaml", "name: idempotent-accumulator\n")
	writeCatalogCaseFixture(t, dir,
		`
initial_state: collecting
terminal_states: [complete]
states: [collecting, complete]
pins:
  inputs:
    events: [item.arrived]
  outputs:
    events: [collection.done]
`,
		`
test-node:
  id: test-node
  execution_type: system_node
  subscribes_to: [item.arrived]
  produces: [collection.done]
  event_handlers:
    item.arrived:
      accumulate:
        expected_from: entity.expected_count
        completion: all
      advances_to: complete
      emits: collection.done
`,
		"{}\n",
		`
trigger:
  sequence:
    - event: item.arrived
      payload: {entity_id: 11111111-1111-4111-8111-111111111111, item_id: a}
    - event: item.arrived
      payload: {entity_id: 11111111-1111-4111-8111-111111111111, item_id: a}
    - event: item.arrived
      payload: {entity_id: 11111111-1111-4111-8111-111111111111, item_id: b}
  entity_fields_before:
    expected_count: 2

expected:
  handler_outcome: success
  entity_state: complete
  emitted_events: [collection.done]
`,
	)

	result, expected := runSimpleCatalogCase(t, dir)
	if got, want := result.handlerOutcome, expected.Expected.HandlerOutcome; got != want {
		t.Fatalf("handler outcome = %q, want %q", got, want)
	}
	if got, want := result.entityState, expected.Expected.EntityState; got != want {
		t.Fatalf("entity state = %q, want %q", got, want)
	}
	if diff := diffStringSet(normalizeSorted(result.emittedEvents), normalizeSorted(expected.Expected.EmittedEvents)); diff != "" {
		t.Fatalf("emitted events mismatch (%s)", diff)
	}
}

func TestCatalogGuardPasses_ComparisonAndReferenceSemantics(t *testing.T) {
	entity := map[string]any{"status": "ready", "expected_count": "3"}
	payload := map[string]any{"score": "85", "status": "ready"}
	policy := map[string]any{"threshold": 80, "minimum_count": 2}

	cases := []struct {
		name string
		spec any
		want bool
	}{
		{
			name: "numeric gte across payload and policy",
			spec: map[string]any{"check": "payload.score >= policy.threshold"},
			want: true,
		},
		{
			name: "numeric gt resolves string entity field",
			spec: map[string]any{"check": "entity.expected_count > policy.minimum_count"},
			want: true,
		},
		{
			name: "string equality",
			spec: map[string]any{"check": "payload.status == entity.status"},
			want: true,
		},
		{
			name: "string inequality",
			spec: map[string]any{"check": "payload.status != entity.missing"},
			want: true,
		},
		{
			name: "missing ref fails numeric comparison",
			spec: map[string]any{"check": "payload.missing >= policy.threshold"},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := catalogGuardPasses(tc.spec, payload, entity, policy); got != tc.want {
				t.Fatalf("catalogGuardPasses() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCatalogCasePresent_RequiresAllCoreFiles(t *testing.T) {
	dir := t.TempDir()
	writeCatalogCaseFile(t, dir, "package.yaml", "name: catalog-case\n")
	writeCatalogCaseFile(t, dir, "schema.yaml", "initial_state: pending\n")
	writeCatalogCaseFile(t, dir, "nodes.yaml", "test-node: {}\n")
	writeCatalogCaseFile(t, dir, "expected.yaml", "expected: {}\n")

	if !catalogCasePresent(dir) {
		t.Fatal("expected directory with all core catalog files to be recognized")
	}

	missing := filepath.Join(t.TempDir(), "missing-package")
	if err := os.MkdirAll(missing, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", missing, err)
	}
	writeCatalogCaseFile(t, missing, "schema.yaml", "initial_state: pending\n")
	writeCatalogCaseFile(t, missing, "nodes.yaml", "test-node: {}\n")
	writeCatalogCaseFile(t, missing, "expected.yaml", "expected: {}\n")

	if catalogCasePresent(missing) {
		t.Fatal("expected directory missing package.yaml to be rejected")
	}
}

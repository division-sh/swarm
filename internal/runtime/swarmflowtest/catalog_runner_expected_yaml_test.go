package swarmflowtest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCatalogRunnerExpectedYAML_SingleTriggerWhitespaceAndNormalization(t *testing.T) {
	dir := t.TempDir()
	writeCatalogCaseFixture(t, dir,
		`
initial_state: pending
terminal_states: [done]
states: [pending, done]
pins:
  inputs:
    events: [check.requested]
  outputs:
    events: [check.passed]
`,
		`
test-node:
  id: test-node
  execution_type: system_node
  subscribes_to: [check.requested]
  produces: [check.passed]
  event_handlers:
    check.requested:
      guard:
        check: "payload.score >= policy.minimum_score"
        on_fail: discard
      advances_to: done
      emits: check.passed
`,
		`
minimum_score: 80
`,
		`
trigger:
  event: "  check.requested  "
  payload:
    entity_id: 11111111-1111-4111-8111-111111111111
    score: "85"
  entity_fields_before:
    " expected_count ": 1

expected:
  handler_outcome: " success "
  entity_state: " done "
  emitted_events:
    - " check.passed "
    - ""
    - "check.passed"
`,
	)

	result, expected := runSimpleCatalogCase(t, dir)
	if got, want := result.handlerOutcome, "success"; got != want {
		t.Fatalf("handler outcome = %q, want %q", got, want)
	}
	if got, want := result.entityState, "done"; got != want {
		t.Fatalf("entity state = %q, want %q", got, want)
	}
	if got, want := normalizeSorted(result.emittedEvents), []string{"check.passed"}; diffStringSet(got, want) != "" {
		t.Fatalf("normalized emitted events mismatch (%s)", diffStringSet(got, want))
	}
	if got, want := normalizeSorted(expected.Expected.EmittedEvents), []string{"check.passed"}; diffStringSet(got, want) != "" {
		t.Fatalf("normalized expected emitted events mismatch (%s)", diffStringSet(got, want))
	}
}

func TestCatalogRunnerExpectedYAML_SequenceUsesStringEntityFieldsBefore(t *testing.T) {
	dir := t.TempDir()
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
  state_schema:
    fields:
      expected_count: integer
      received_items: jsonb
`,
		"{}\n",
		`
trigger:
  sequence:
    - event: " item.arrived "
      payload: {entity_id: 11111111-1111-4111-8111-111111111111, item_id: a}
    - event: item.arrived
      payload: {entity_id: 11111111-1111-4111-8111-111111111111, item_id: b}
  entity_fields_before:
    expected_count: "2"

expected:
  handler_outcome: success
  entity_state: complete
  emitted_events:
    - collection.done
`,
	)

	result, expected := runSimpleCatalogCase(t, dir)
	if got, want := result.handlerOutcome, "success"; got != want {
		t.Fatalf("handler outcome = %q, want %q", got, want)
	}
	if got, want := result.entityState, "complete"; got != want {
		t.Fatalf("entity state = %q, want %q", got, want)
	}
	if diff := diffStringSet(normalizeSorted(result.emittedEvents), normalizeSorted(expected.Expected.EmittedEvents)); diff != "" {
		t.Fatalf("emitted events mismatch (%s)", diff)
	}
}

func writeCatalogCaseFixture(t testing.TB, dir, schema, nodes, policy, expected string) {
	t.Helper()
	writeCatalogCaseFile(t, dir, "schema.yaml", schema)
	writeCatalogCaseFile(t, dir, "nodes.yaml", nodes)
	writeCatalogCaseFile(t, dir, "policy.yaml", policy)
	writeCatalogCaseFile(t, dir, "expected.yaml", expected)
}

func writeCatalogCaseFile(t testing.TB, dir, name, contents string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

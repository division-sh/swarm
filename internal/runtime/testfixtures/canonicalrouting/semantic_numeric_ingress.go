package canonicalrouting

import (
	"os"
	"path/filepath"
	"testing"
)

// CopySemanticNumericIngress constructs the numeric command/result fixture.
func CopySemanticNumericIngress(t testing.TB) string {
	t.Helper()
	root := CopyScenarioRootSetup(t)
	for name, raw := range map[string]string{
		"schema.yaml": `name: semantic-numeric-ingress
initial_state: waiting
terminal_states: [done]
states: [waiting, done]
pins:
  inputs:
    events: [numeric.requested]
`,
		"events.yaml": `numeric.requested:
  swarm:
    source: external
  value: integer
  nested: NumericInput
numeric.completed:
  value: integer
  fraction: numeric
  explicit_double: numeric
`,
		"types.yaml": "types:\n  NumericInput:\n    numbers: list<numeric>\n",
		"nodes.yaml": `numeric:
  execution_type: system_node
  subscribes_to: [numeric.requested]
  produces: [numeric.completed]
  event_handlers:
    numeric.requested:
      emit:
        event: numeric.completed
        fields:
          value: {cel: 'payload.value + payload.nested.numbers[?0].value() + 1'}
          fraction: {cel: 'payload.nested.numbers[?1].value() + 0.5'}
          explicit_double: {cel: 'double(payload.value) + 1.0'}
collector:
  execution_type: system_node
  subscribes_to: [numeric.completed]
  event_handlers:
    numeric.completed:
      create_entity: true
      data_accumulation:
        source_event: numeric.completed
        writes:
          - source_field: value
            target_field: score
      advances_to: done
`,
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

package canonicalrouting

import (
	"path/filepath"
	"testing"
)

const ScenarioFixtureCloseEvent = "fulfillment.close_requested"

func InstallNovelNumericScenarioLifecycle(t testing.TB, root string) {
	t.Helper()
	writeClosedVariantFile(t, root, "events.yaml", `fulfillment.requested:
  value: integer
  fraction: numeric
`)
	writeClosedVariantFile(t, root, "fulfillment/events.yaml", `
fulfillment.completed:
  value: integer
  fraction: numeric
  explicit_double: numeric
`)
	writeClosedVariantFile(t, root, "fulfillment/nodes.yaml", `complete-request:
  execution_type: system_node
  subscribes_to: [fulfillment.requested]
  produces: [fulfillment.completed]
  event_handlers:
    fulfillment.requested:
      emit:
        event: fulfillment.completed
        fields:
          value: payload.value + 1
          fraction: double(payload.fraction) + 0.5
          explicit_double: double(payload.value) + 1.0
collector:
  execution_type: system_node
  subscribes_to: [fulfillment.completed]
  event_handlers:
    fulfillment.completed: {advances_to: done}
`)
}

// Responses settle while pending; the driver closes only after all requests.
func InstallNovelAuthoredScenarioLifecycle(t testing.TB, root string) {
	t.Helper()
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"),
		"stages: {done: {final: true}}", "stages: {active: {}, done: {final: true}}")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"),
		"  inputs:\n    - fulfillment.requested\n", "  inputs:\n    - fulfillment.requested\n    - "+ScenarioFixtureCloseEvent+"\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"),
		"  outputs:\n    - fulfillment.requested\n", "  outputs:\n    - fulfillment.requested\n    - "+ScenarioFixtureCloseEvent+"\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"),
		"connect:\n", "connect:\n  - {event: "+ScenarioFixtureCloseEvent+", from: ., to: fulfillment}\n")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"),
		"fulfillment.requested:\n", ScenarioFixtureCloseEvent+":\nfulfillment.requested:\n")
	applyClosedReplacement(t, filepath.Join(root, "fulfillment/schema.yaml"),
		"inputs: [fulfillment.requested]", "inputs: [fulfillment.requested, "+ScenarioFixtureCloseEvent+"]")
	writeClosedVariantFile(t, root, "entities.yaml", "scenario_state: {}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `close-scenario:
  execution_type: system_node
  subscribes_to: [fulfillment.close_requested]
  event_handlers:
    fulfillment.close_requested: {advances_to: done}
`)
	writeClosedVariantFile(t, root, "tools.yaml", `
scenario.send:
  description: Authored response materialization proof.
  category: provider_connector
  credentials: [scenario_mock_secret]
  handler_type: http
  effect_class: non_idempotent_write
  http: {method: POST, url: https://example.invalid/send}
  output_schema:
    type: object
    required: [marker]
    properties: {marker: {type: string}}
  response_success: {kind: http_status_2xx}
`)
	writeClosedVariantFile(t, root, "fulfillment/nodes.yaml", `
complete-request:
  execution_type: system_node
  subscribes_to: [fulfillment.requested]
  event_handlers:
    fulfillment.requested:
      activity: {id: send, tool: scenario.send, input: {}}
response:
  execution_type: system_node
  subscribes_to: [send.succeeded]
  event_handlers:
    send.succeeded: {}
close-scenario:
  execution_type: system_node
  subscribes_to: [fulfillment.close_requested]
  event_handlers:
    fulfillment.close_requested: {advances_to: done}
`)
}

// Reproduce the overlay defect through the same closed source owner.
func CopyNovelScenarioMissingCompletion(t testing.TB, authored bool) string {
	t.Helper()
	root := WriteNovelDerivedScenarioBundleWithRootInput(t)
	if authored {
		InstallNovelAuthoredScenarioLifecycle(t, root)
	} else {
		InstallNovelNumericScenarioLifecycle(t, root)
	}
	applyClosedReplacement(t, filepath.Join(root, "fulfillment/nodes.yaml"),
		"{advances_to: done}", "{}")
	return root
}

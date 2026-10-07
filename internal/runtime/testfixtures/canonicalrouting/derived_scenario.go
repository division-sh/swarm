package canonicalrouting

import "testing"

// WriteNovelDerivedScenarioBundle creates the closed, scenario-free bundle
// used to prove that derivation does not depend on a checked-in archetype.
func WriteNovelDerivedScenarioBundle(t testing.TB) string {
	return writeNovelDerivedScenarioBundle(t, false)
}

// WriteNovelDerivedScenarioBundleWithRootInput adds the same canonical event
// as a supported root input for connected run-start proofs.
func WriteNovelDerivedScenarioBundleWithRootInput(t testing.TB) string {
	return writeNovelDerivedScenarioBundle(t, true)
}

func writeNovelDerivedScenarioBundle(t testing.TB, rootInput bool) string {
	t.Helper()
	root := t.TempDir()
	rootSchema := "\nname: derived-novel-flow\n"
	rootEvents := ""
	if rootInput {
		rootSchema = `
name: derived-novel-flow
stages: {done: {final: true}}
pins:
  inputs:
    - fulfillment.requested
  outputs:
    - fulfillment.requested
connect:
  - {event: fulfillment.requested, from: ., to: fulfillment}
`
		rootEvents = `
fulfillment.requested:
  order_id: text
`
	}
	files := map[string]string{

		"schema.yaml": rootSchema,
		"fulfillment/schema.yaml": `name: fulfillment
pins:
  inputs:
    - fulfillment.requested
`,
		"fulfillment/events.yaml": `
fulfillment.requested:
  order_id: text
`,
		"fulfillment/nodes.yaml": `
complete-request:
  execution_type: system_node
  subscribes_to: [fulfillment.requested]
  event_handlers:
    fulfillment.requested: {}
`,
	}
	if rootEvents != "" {
		files["events.yaml"] = rootEvents
		delete(files, "fulfillment/events.yaml")
		files["fulfillment/schema.yaml"] = "name: fulfillment\nstages: {pending: {}, done: {final: true}}\npins:\n  inputs: [fulfillment.requested]\n"
		files["fulfillment/entities.yaml"] = "fulfillment_state: {}\n"
		files["fulfillment/nodes.yaml"] = "complete-request:\n  execution_type: system_node\n  subscribes_to: [fulfillment.requested]\n  event_handlers:\n    fulfillment.requested: {advances_to: done}\n"
	}
	for relative, body := range files {
		writeClosedVariantFile(t, root, relative, body)
	}
	return root
}

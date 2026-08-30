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
	if rootInput {
		rootSchema = `
name: derived-novel-flow
pins:
  inputs:
    events:
      - {event: fulfillment.requested, source: external}
`
	}
	files := map[string]string{

		"schema.yaml": rootSchema,
		"fulfillment/schema.yaml": `
name: fulfillment
mode: static
pins:
  inputs:
    events:
      - {event: fulfillment.requested, source: external}
`,
		"fulfillment/events.yaml": `
fulfillment.requested:
  order_id: text
`,
		"fulfillment/nodes.yaml": `
complete-request:
  id: complete-request
  execution_type: system_node
  subscribes_to: [fulfillment.requested]
  event_handlers:
    fulfillment.requested: {}
`,
	}
	for relative, body := range files {
		writeClosedVariantFile(t, root, relative, body)
	}
	return root
}

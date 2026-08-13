package canonicalrouting

import "testing"

// WriteNovelDerivedScenarioBundle creates the closed, scenario-free bundle
// used to prove that derivation does not depend on a checked-in archetype.
func WriteNovelDerivedScenarioBundle(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"package.yaml": `
name: derived-novel-flow
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - {id: fulfillment, flow: fulfillment, mode: static}
`,
		"schema.yaml": `
name: derived-novel-flow
`,
		"events.yaml":   "{}\n",
		"nodes.yaml":    "{}\n",
		"entities.yaml": "{}\n",
		"agents.yaml":   "{}\n",
		"tools.yaml":    "{}\n",
		"policy.yaml":   "{}\n",
		"flows/fulfillment/schema.yaml": `
name: fulfillment
mode: static
pins:
  inputs:
    events:
      - {name: request, event: fulfillment.requested, source: external}
  outputs: {events: []}
`,
		"flows/fulfillment/events.yaml": `
fulfillment.requested:
  order_id: text
  required: [order_id]
`,
		"flows/fulfillment/nodes.yaml": `
complete-request:
  id: complete-request
  execution_type: system_node
  subscribes_to: [fulfillment.requested]
  event_handlers:
    fulfillment.requested: {}
`,
		"flows/fulfillment/entities.yaml": "{}\n",
		"flows/fulfillment/agents.yaml":   "{}\n",
		"flows/fulfillment/tools.yaml":    "{}\n",
		"flows/fulfillment/policy.yaml":   "{}\n",
	}
	for relative, body := range files {
		writeClosedVariantFile(t, root, relative, body)
	}
	return root
}

package canonicalrouting

import "testing"

func CopyTemplateInstanceEmpireStyle(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for path, contents := range map[string]string{
		"schema.yaml": `name: test
pins:
  outputs:
    events: [opco.spinup_created]
connect:
  - event: opco.spinup_created
    from: .
    to: operating
`,
		"events.yaml": `opco.spinup_requested:
  entity_id: string?
  instance_id: string
  product_id: string
opco.spinup_created:
  instance_id: string
  product_id: string
`,
		"nodes.yaml": `portfolio-node:
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  produces: [opco.spinup_created]
  event_handlers:
    opco.spinup_requested:
      emit:
        event: opco.spinup_created
        fields:
          instance_id: payload.instance_id
          product_id: payload.product_id
`,
		"operating/schema.yaml": `name: operating
mode: template
instance: instance_id
instance_variables:
  variables:
    product_id: string
pins:
  inputs:
    events:
      - event: opco.spinup_created
        resolution: {mode: create}
        initialize:
          product_id: payload.product_id
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"operating/entities.yaml": "operating_state:\n  instance_id: string\n",
		"operating/events.yaml": `opco.product_initialization_requested:
  instance_id: string
  product_id: string
component_scaffold.spawn_requested:
  product_id: string
`,
		"operating/nodes.yaml": `lifecycle-orchestrator:
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [component_scaffold.spawn_requested]
  event_handlers:
    opco.product_initialization_requested:
      advances_to: ready
      emit:
        event: component_scaffold.spawn_requested
        fields:
          product_id: payload.product_id
`,
	} {
		writeClosedVariantFile(t, root, path, contents)
	}
	return root
}

func CopyTemplateInstanceActivationConfigSubscriber(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for path, contents := range map[string]string{
		"schema.yaml": `name: test
pins:
  outputs:
    events: [opco.spinup_created]
connect:
  - event: opco.spinup_created
    from: .
    to: operating
`,
		"events.yaml": `opco.spinup_requested:
  entity_id: string?
  instance_id: string
  product_id: string
opco.spinup_created:
  instance_id: string
  product_id: string
`,
		"nodes.yaml": `portfolio-node:
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  produces: [opco.spinup_created]
  event_handlers:
    opco.spinup_requested:
      emit:
        event: opco.spinup_created
        fields:
          instance_id: payload.instance_id
          product_id: payload.product_id
`,
		"operating/schema.yaml": `name: operating
mode: template
instance: instance_id
instance_variables:
  variables:
    product_id: string
pins:
  inputs:
    events:
      - event: opco.spinup_created
        resolution: {mode: create}
        initialize:
          product_id: payload.product_id
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"operating/entities.yaml": "operating_state:\n  instance_id: string\n",
		"operating/events.yaml": `opco.product_initialization_requested:
  instance_id: string?
  template_id: string?
  flow_path: string?
  parent_entity_id: string?
  product_id: string?
`,
		"operating/agents.yaml": `ceo:
  type: generic
  role: ceo
  intent: {inline: "Initialize the product for this operating instance."}
  model: regular
  subscriptions: [opco.product_initialization_requested]
`,
	} {
		writeClosedVariantFile(t, root, path, contents)
	}
	return root
}

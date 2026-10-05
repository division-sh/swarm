package canonicalrouting

import (
	"strings"
	"testing"
)

func CopyOperatorKeyedConstructor(t testing.TB, connected bool) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"schema.yaml":   "name: factory\ninstance: thing_id\npins:\n  inputs:\n    - thing.created\n",
		"entities.yaml": "thing:\n  thing_id: text\n",
		"events.yaml":   "thing.created:\n  thing_id: text\n  entity_id: uuid?\n  amount: integer\n",
		"nodes.yaml":    "thing-writer:\n  execution_type: system_node\n  event_handlers:\n    thing.created: {}\n",
	}
	if connected {
		files = map[string]string{
			"schema.yaml":           "name: factory\npins:\n  inputs:\n    - thing.requested\n    - note.requested\n  outputs:\n    - thing.requested\nconnect:\n  - {event: thing.requested, from: ., to: factory, rename: thing.created, resolution: create, key_from: event.id}\n",
			"events.yaml":           "thing.requested:\n  entity_id: uuid?\n  amount: integer\nnote.requested:\n  entity_id: uuid?\n  amount: integer\n",
			"nodes.yaml":            "observer:\n  execution_type: system_node\n  event_handlers:\n    thing.requested: {}\n    note.requested: {}\n",
			"factory/schema.yaml":   "name: factory\ninstance: thing_id\npins:\n  inputs:\n    - thing.created\n",
			"factory/entities.yaml": "thing:\n  thing_id: text\n",
			"factory/nodes.yaml":    "thing-writer:\n  execution_type: system_node\n  event_handlers:\n    thing.created: {}\n",
			"shadow/schema.yaml":    "name: shadow\ninstance: thing_id\npins:\n  inputs:\n    - note.requested\n",
			"shadow/entities.yaml":  "thing:\n  thing_id: text\n",
			"shadow/events.yaml":    "note.requested:\n  thing_id: text\n  entity_id: uuid?\n  amount: integer\n",
			"shadow/nodes.yaml":     "shadow-writer:\n  execution_type: system_node\n  event_handlers:\n    note.requested: {}\n",
		}
	}
	for path, contents := range files {
		writeClosedVariantFile(t, root, path, contents)
	}
	return root
}

func CopyFlowConstructorSupply(t testing.TB, fields, payload, handler, catalog string) string {
	t.Helper()
	root := CopyTemplateInstanceRoute(t, TemplateInstanceRouteOptions{Mode: TemplateInstanceRouteCreate})
	if catalog != "" {
		writeClosedVariantFile(t, root, "types.yaml", catalog)
	}
	writeClosedVariantFile(t, root, "producer/events.yaml", "deploy.done:\n  vertical_id: text\n"+payload)
	writeClosedVariantFile(t, root, "consumer/entities.yaml", "deployment:\n  vertical_id: text\n"+fields)
	writeClosedVariantFile(t, root, "consumer/schema.yaml", "name: consumer\ninstance: vertical_id\nstages:\n  initial: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    - deploy.done\n")
	if handler != "" {
		writeClosedVariantFile(t, root, "consumer/nodes.yaml", "consumer-node:\n  execution_type: system_node\n  event_handlers:\n    deploy.done:\n"+handler)
	}
	return root
}

func CopyFlowConstructorCandidates(t testing.TB, mode TemplateInstanceRouteMode, staged bool) string {
	t.Helper()
	root := CopyTemplateInstanceRoute(t, TemplateInstanceRouteOptions{Mode: TemplateInstanceRouteCreate})
	policy := ""
	switch mode {
	case TemplateInstanceRouteSelect:
		policy = "select"
	case TemplateInstanceRouteCreate:
		policy = "create"
	case TemplateInstanceRouteSelectOrCreate:
		policy = "select-or-create"
	default:
		t.Fatalf("unsupported constructor connection policy %d", mode)
	}
	stages := ""
	if staged {
		stages = "stages:\n  initial: {initial: true}\n  done: {terminal: true}\n"
	}
	writeClosedVariantFile(t, root, "producer/schema.yaml", "name: producer\npins:\n  outputs:\n    - deploy.done\n    - deploy.empty\n")
	writeClosedVariantFile(t, root, "producer/events.yaml", "deploy.done:\n  vertical_id: text\n  brief: text\ndeploy.empty:\n  vertical_id: text\n")
	writeClosedVariantFile(t, root, "consumer/schema.yaml", "name: consumer\ninstance: vertical_id\n"+stages+"pins:\n  inputs: [deploy.done, deploy.empty]\n")
	writeClosedVariantFile(t, root, "consumer/entities.yaml", "deployment:\n  vertical_id: text\n  brief: text\n")
	writeClosedVariantFile(t, root, "consumer/nodes.yaml", "consumer-node:\n  execution_type: system_node\n  event_handlers:\n    deploy.done:\n      guard: {check: entity.brief != ''}\n    deploy.empty:\n      guard: {check: entity.brief != ''}\n")
	writeClosedVariantFile(t, root, "schema.yaml", "name: constructor-connection-policy\nconnect:\n  - {event: deploy.done, from: producer, to: consumer, resolution: create}\n  - {event: deploy.empty, from: producer, to: consumer, resolution: "+policy+"}\n")
	return root
}

func CopyFlowConstructorNestedPresence(t testing.TB, member, read string) string {
	t.Helper()
	root := CopyTemplateInstanceRoute(t, TemplateInstanceRouteOptions{Mode: TemplateInstanceRouteCreate})
	catalog := "types:\n  Brief:\n    detail: " + member + "\n"
	writeClosedVariantFile(t, root, "consumer/types.yaml", catalog)
	writeClosedVariantFile(t, root, "producer/types.yaml", catalog)
	writeClosedVariantFile(t, root, "producer/events.yaml", "deploy.done:\n  vertical_id: text\n  brief: Brief\n")
	writeClosedVariantFile(t, root, "consumer/entities.yaml", "deployment:\n  vertical_id: text\n  brief: Brief\n")
	writeClosedVariantFile(t, root, "consumer/nodes.yaml", "consumer-node:\n  execution_type: system_node\n  event_handlers:\n    deploy.done:\n      guard: {check: '"+strings.ReplaceAll(read, "'", "''")+"'}\n")
	return root
}

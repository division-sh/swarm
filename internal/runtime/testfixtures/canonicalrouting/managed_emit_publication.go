package canonicalrouting

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// CopyManagedEmitPublication uses one real generated emit surface, a same-slug
// sibling with a different schema, and a forbidden sibling-only tool request.
func CopyManagedEmitPublication(t testing.TB, mode string) string {
	t.Helper()
	root := t.TempDir()
	scope := "."
	switch mode {
	case "root":
	case "imported":
		scope = "source"
	case "nested":
		scope = "outer/source"
	case "template":
		scope = "source"
	default:
		t.Fatalf("unknown managed emit topology %s", mode)
	}
	writeClosedVariantFile(t, root, "schema.yaml", "name: managed-publication\n")
	if mode == "nested" {
		writeClosedVariantFile(t, root, "outer/schema.yaml", "name: outer\nmode: static\n")
	}
	for _, flow := range []string{scope, "sibling"} {
		sibling := flow == "sibling"
		kind, value, role := "text", `"exact"`, "source-writer"
		if sibling {
			kind, value, role = "boolean", "True", "sibling-writer"
		}
		input := "      - {event: work.requested, source: external}\n"
		modeDecl := ""
		if mode == "template" && !sibling {
			modeDecl = "mode: template\ninstance: case_id\n"
			input = "      - {event: work.requested, resolution: {mode: select-or-create}}\n"
		}
		if sibling {
			input = strings.ReplaceAll(input, "work.requested", "sibling.requested")
		}
		schema := "name: emit-proof\n" + modeDecl + "pins:\n  inputs:\n    events:\n" + input
		extraEvent, extraOutput, extraEmit := "", "", ""
		if sibling {
			extraEvent, extraOutput, extraEmit = "foreign.only: {}\n", "  outputs:\n    events:\n      - {event: foreign.only, sink: harness}\n", ", foreign.only"
		}
		writeClosedVariantFile(t, root, filepath.Join(flow, "schema.yaml"), schema+extraOutput)
		events := "work.requested:\n  case_id: text\nwork.started:\n  case_id: text\nwork.result:\n  value: " + kind + "\nwork.ack:\n  value: " + kind + "\n  swarm:\n    consumer: external\n" + extraEvent
		if mode == "template" && !sibling {
			events = strings.Replace(events, "work.requested:\n  case_id: text\n", "", 1)
		}
		if sibling {
			events = strings.ReplaceAll(events, "work.requested", "sibling.requested")
		}
		writeClosedVariantFile(t, root, filepath.Join(flow, "events.yaml"), events)
		create := ""
		if mode == "template" && !sibling {
			create = "      create_entity: true\n      data_accumulation:\n        writes:\n          - {target_field: case_id, expression: payload.case_id}\n"
			writeClosedVariantFile(t, root, filepath.Join(flow, "entities.yaml"), "work:\n  case_id: text\n")
		}
		nodes := "start:\n  execution_type: system_node\n  subscribes_to: [work.requested]\n  event_handlers:\n    work.requested:\n" + create + "      emit: {event: work.started, fields: {case_id: payload.case_id}}\nfinal:\n  execution_type: system_node\n  subscribes_to: [work.result]\n  event_handlers:\n    work.result:\n      emit: {event: work.ack, fields: {value: payload.value}}\n"
		if sibling {
			nodes = strings.ReplaceAll(nodes, "work.requested", "sibling.requested")
		}
		writeClosedVariantFile(t, root, filepath.Join(flow, "nodes.yaml"), nodes)
		writeClosedVariantFile(t, root, filepath.Join(flow, "agents.yaml"), "writer:\n  role: "+role+"\n  intent: {inline: 'Emit the exact scoped result in the required value field.'}\n  model: regular\n  memory: false\n  subscriptions: [work.started]\n  emit_events: [work.result"+extraEmit+"]\n  mock:\n    kind: python\n    module: mocks/writer.py\n")
		bad := "    if input[\"round\"] == 1 and frame[\"event\"][\"payload\"][\"case_id\"] == \"hostile\":\n        return {\"calls\": [{\"name\": \"emit_foreign_only\", \"arguments\": {}}], \"usage\": {\"input_tokens\": 1, \"output_tokens\": 1}}\n"
		if sibling {
			bad = ""
		}
		writeClosedVariantFile(t, root, filepath.Join(flow, "mocks/writer.py"), "import json\n\ndef handle(input):\n    frame = {}\n    for message in input[\"messages\"]:\n        if message[\"role\"] == \"user\":\n            frame = json.loads(message[\"content\"])\n"+bad+fmt.Sprintf("    if input[\"round\"] == 1:\n        return {\"calls\": [{\"name\": \"emit_work_result\", \"arguments\": {\"value\": %s}}], \"usage\": {\"input_tokens\": 1, \"output_tokens\": 1}}\n    return {\"text\": \"Complete\", \"usage\": {\"input_tokens\": 1, \"output_tokens\": 1}}\n", value))

	}
	if mode == "template" {
		writeClosedVariantFile(t, root, "schema.yaml", "name: managed-driver\npins:\n  inputs:\n    events:\n      - {event: work.requested, source: external}\n  outputs:\n    events: [work.dispatch]\nconnect:\n  - {event: work.dispatch, from: ., to: source, rename: work.requested}\n")
		writeClosedVariantFile(t, root, "events.yaml", "work.requested:\n  key: case_id\n  case_id: text\nwork.dispatch:\n  key: case_id\n  case_id: text\n")
		writeClosedVariantFile(t, root, "nodes.yaml", "driver:\n  execution_type: system_node\n  subscribes_to: [work.requested]\n  event_handlers:\n    work.requested:\n      emit: {event: work.dispatch, fields: {case_id: payload.case_id}}\n")
	}
	return root
}

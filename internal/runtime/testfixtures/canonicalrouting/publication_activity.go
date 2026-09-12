package canonicalrouting

import (
	"fmt"
	"strings"
	"testing"
)

// CopyPublicationActivity retains generated result schemas and real HTTP activity
// execution. Only the provider address is supplied by the test.
func CopyPublicationActivity(t testing.TB, mode, providerURL string, approval bool) string {
	t.Helper()
	flow := "source"
	if mode == "root" {
		flow = "."
	} else if mode == "nested_template" {
		flow = "outer/source"
	} else if mode != "static" && mode != "template" {
		t.Fatalf("unknown activity topology %q", mode)
	}
	template := mode == "template" || mode == "nested_template"
	root := t.TempDir()
	prefix := flow + "/"
	if flow == "." {
		prefix = ""
	}
	request := "activity.requested:\n  key: case_id\n  case_id: text\n  message: text\n"
	results := []string{"send.succeeded", "send.failed"}
	if approval {
		results = append(results, "send.revision_requested", "send.rejected")
	}
	outputs, connects := "", ""
	for _, event := range results {
		outputs += "      - " + event + "\n"
		connects += fmt.Sprintf("  - {event: %s, from: %s, to: sink}\n", event, flow)
	}
	schema := "name: publication-activity\npins:\n  inputs:\n    events:\n      - {event: activity.requested, source: external}\n  outputs:\n    events:\n" + outputs
	if template {
		schema = strings.Replace(schema, "name: publication-activity\n", "name: publication-activity\nmode: template\ninstance: case_id\n", 1)
		schema = strings.Replace(schema, "source: external", "resolution: {mode: select-or-create}", 1)
		writeClosedVariantFile(t, root, "events.yaml", request+"activity.dispatch:\n  key: case_id\n  case_id: text\n  message: text\n")
		writeClosedVariantFile(t, root, "nodes.yaml", "driver:\n  execution_type: system_node\n  subscribes_to: [activity.requested]\n  event_handlers:\n    activity.requested:\n      emit: {event: activity.dispatch, fields: {case_id: payload.case_id, message: payload.message}}\n")
		connects += fmt.Sprintf("  - {event: activity.dispatch, from: ., to: %s, rename: activity.requested}\n", flow)
		writeClosedVariantFile(t, root, "schema.yaml", "name: activity-driver\npins:\n  inputs:\n    events:\n      - {event: activity.requested, source: external}\n  outputs:\n    events: [activity.dispatch]\nconnect:\n"+connects)
		writeClosedVariantFile(t, root, prefix+"entities.yaml", "work:\n  case_id: {type: text, _unused_reason: receiver identity}\n")
		if mode == "nested_template" {
			writeClosedVariantFile(t, root, "outer/schema.yaml", "name: outer\n")
		}
	} else {
		writeClosedVariantFile(t, root, prefix+"events.yaml", request)
		if flow == "." {
			schema += "connect:\n" + connects
		} else {
			writeClosedVariantFile(t, root, "schema.yaml", "name: activity-driver\nconnect:\n"+connects)
		}
	}
	writeClosedVariantFile(t, root, prefix+"schema.yaml", schema)
	activity := "producer:\n  execution_type: system_node\n  subscribes_to: [activity.requested]\n  event_handlers:\n    activity.requested:\n      activity:\n        id: send\n        tool: send\n        input: {message: {ref: payload.message}}\n"
	effect := "read_only"
	if approval {
		activity += "        approval: {decision: approve_send}\n"
		effect = "non_idempotent_write"
		// Approval execution requires existing receiver state. A real upstream
		// handler acquires it; the approval test must not seed a database row.
		activity = strings.ReplaceAll(activity, "activity.requested", "activity.execute")
		activity = "intake:\n  execution_type: system_node\n  subscribes_to: [activity.requested]\n  event_handlers:\n    activity.requested:\n      data_accumulation:\n        writes: [{target_field: case_id, expression: payload.case_id}]\n      emit: {event: activity.execute, fields: {message: payload.message}}\n" + activity
		declarations := "activity.execute:\n  message: text\n"
		if !template {
			declarations = request + declarations
		}
		writeClosedVariantFile(t, root, prefix+"events.yaml", declarations)
		writeClosedVariantFile(t, root, prefix+"entities.yaml", "work:\n  case_id: {type: text, _unused_reason: approval receiver identity}\n")
	}
	local := "local:\n  execution_type: system_node\n  subscribes_to: [" + strings.Join(results, ", ") + "]\n  event_handlers:\n"
	for _, event := range results {
		check := "payload.activity_id == 'send'"
		if event == "send.succeeded" {
			check += " && payload.result.delivered == true"
		}
		local += fmt.Sprintf("    %s:\n      guard: {id: observed, check: %q}\n", event, check)
	}
	writeClosedVariantFile(t, root, prefix+"nodes.yaml", activity+local)
	writeClosedVariantFile(t, root, prefix+"tools.yaml", fmt.Sprintf(`send:
  description: Send the exact activity input to the test provider
  handler_type: http
  effect_class: %s
  input_schema:
    type: object
    properties: {message: {type: string}}
    required: [message]
  output_schema:
    type: object
    properties: {delivered: {type: boolean}}
    required: [delivered]
  response_success: {kind: http_status_2xx}
  http:
    method: POST
    url: %q
    body: {message: "{{input.message}}"}
`, effect, providerURL))
	writeClosedVariantFile(t, root, "sink/schema.yaml", "name: sink\npins:\n  inputs:\n    events:\n"+outputs)
	writeClosedVariantFile(t, root, "sink/nodes.yaml", local)
	// Reused names with an incompatible schema must not supply source authority.
	siblingInputs, siblingSchemas := "", ""
	siblingNode := "local:\n  execution_type: system_node\n  subscribes_to: [" + strings.Join(results, ", ") + "]\n  event_handlers:\n"
	for _, event := range results {
		siblingInputs += "      - {event: " + event + ", source: external}\n"
		siblingSchemas += event + ":\n  activity_id: integer\n"
		siblingNode += "    " + event + ":\n      guard: {id: sibling_only, check: 'payload.activity_id > 0'}\n"
	}
	writeClosedVariantFile(t, root, "sibling/schema.yaml", "name: sibling\npins:\n  inputs:\n    events:\n"+siblingInputs)
	writeClosedVariantFile(t, root, "sibling/events.yaml", siblingSchemas)
	writeClosedVariantFile(t, root, "sibling/nodes.yaml", siblingNode)
	return root
}

package canonicalrouting

import (
	"fmt"
	"testing"
)

// CopyPublicationStateResult preserves publication, receiver ownership and
// restart coverage without the retired artifact provider. Both outcomes are
// authored business results, not evidence that a Git operation succeeded.
func CopyPublicationStateResult(t testing.TB, mode string) string {
	t.Helper()
	root, prefix, flow := t.TempDir(), "", "."
	if mode == "static" {
		prefix, flow = "source/", "source"
	} else if mode != "root" {
		t.Fatalf("unsupported state-result publication topology %q", mode)
	}
	connect := fmt.Sprintf("connect:\n  - {event: result.accepted, from: %s, to: sink}\n  - {event: result.rejected, from: %s, to: sink}\n", flow, flow)
	schema := "name: state-result-publication\npins:\n  inputs:\n    - document.requested\n  outputs:\n    - result.accepted\n    - result.rejected\n"
	if flow == "." {
		schema += connect
	} else {
		writeClosedVariantFile(t, root, "schema.yaml", "name: result-root\npins:\n  inputs:\n    events: [document.requested]\n  outputs:\n    events: [document.requested]\n"+connect+"  - {event: document.requested, from: ., to: source}\n")
		writeClosedVariantFile(t, root, "events.yaml", "document.requested:\n  request_id: text\n  content: text\n  result_kind: text\n")
	}
	writeClosedVariantFile(t, root, prefix+"schema.yaml", schema)
	writeClosedVariantFile(t, root, prefix+"entities.yaml", `document:
  request_id: text
  content: text
  result_kind: text
`)
	eventSchemas := `document.requested:
  request_id: text
  content: text
  result_kind: text
result.accepted:
  request_id: text
  content: text
  result_kind: text
result.rejected:
  request_id: text
  content: text
  result_kind: text
`
	if flow != "." {
		eventSchemas = `result.accepted:
  request_id: text
  content: text
  result_kind: text
result.rejected:
  request_id: text
  content: text
  result_kind: text
`
	}
	writeClosedVariantFile(t, root, prefix+"events.yaml", eventSchemas)
	local := `local:
  execution_type: system_node
  subscribes_to: [result.accepted, result.rejected]
  event_handlers:
    result.accepted:
      guard: {id: accepted, check: "payload.result_kind == 'accepted'"}
    result.rejected:
      guard: {id: rejected, check: "payload.result_kind == 'rejected'"}
`
	writeClosedVariantFile(t, root, prefix+"nodes.yaml", `writer:
  execution_type: system_node
  subscribes_to: [document.requested]
  produces: [result.accepted, result.rejected]
  event_handlers:
    document.requested:
      create_entity: true
      data_accumulation:
        writes:
          - {source_field: request_id, target_field: request_id}
          - {source_field: content, target_field: content}
          - {source_field: result_kind, target_field: result_kind}
      rules:
        accepted:
          when: "payload.result_kind == 'accepted'"
          emit:
            event: result.accepted
            fields: {request_id: "${entity.request_id}", content: "${entity.content}", result_kind: "${entity.result_kind}"}
        rejected:
          when: "payload.result_kind == 'rejected'"
          emit:
            event: result.rejected
            fields: {request_id: "${entity.request_id}", content: "${entity.content}", result_kind: "${entity.result_kind}"}
        unmatched:
          else: true
`+local)
	writeClosedVariantFile(t, root, "sink/schema.yaml", "name: sink\npins:\n  inputs:\n    - result.accepted\n    - result.rejected\n")
	writeClosedVariantFile(t, root, "sink/nodes.yaml", local)
	return root
}

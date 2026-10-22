package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// The node requester creates the real open context; the declared root actor
// schedules its response through the supported tool, not a seeded timer row.
func CopyOneShotReplySchedule(t testing.TB) string {
	t.Helper()
	root := CopyRootReplyBoundary(t, false, true)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), `    provider.requested:
      emit:
        event: provider.replied
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
          result: "approved"
`, "    provider.requested: {}\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "inputs: [request.started, request.stop]", "inputs: [request.started, request.stop, request.note, schedule.ready]")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "outputs: [request.finished]", "outputs: [request.finished, request.noted]")
	ApplyOverlay(t, root, "events.yaml", "request.note:\n  note: text\nrequest.noted:\n  note: text\nschedule.ready:\n")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "result-observer:\n", `checkpoint:
  execution_type: system_node
  subscribes_to: [request.note]
  event_handlers:
    request.note:
      emit:
        event: request.noted
        fields: {note: payload.note}
result-observer:
`)
	writeClosedVariantFile(t, root, "agents.yaml", `schedule-agent:
  model: regular
  intent:
    inline: Schedule the exact requested response with account_id, request_id and result.
  subscriptions: [schedule.ready]
  emit_events: [provider.replied]
  tools: [schedule]
  permissions: [schedule]
  mock:
    kind: python
    module: mocks/schedule-agent.py
`)
	writeClosedVariantFile(t, root, "mocks/schedule-agent.py", "def handle(input):\n    return {\"text\": \"no scheduled-agent input in this proof\", \"usage\": {\"input_tokens\": 1, \"output_tokens\": 1}}\n")
	return root
}

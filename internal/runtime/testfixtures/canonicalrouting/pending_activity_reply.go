package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// A real public note after proposal creation supplies a committed cut while
// both requests and the approval are open. It grants no approval or return.
func CopyNestedPendingActivityReply(t testing.TB, providerURL string) string {
	t.Helper()
	root := CopyNestedRecordedActivityReply(t, providerURL)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "inputs: [request.started, request.stop]", "inputs: [request.started, request.stop, request.note]")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "outputs: [request.finished]", "outputs: [request.finished, request.noted]")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "request.started:\n", `request.note:
  note: text
request.noted:
  note: text
request.started:
`)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "stop-node:\n", `request-note-node:
  execution_type: system_node
  subscribes_to: [request.note]
  event_handlers:
    request.note:
      emit:
        event: request.noted
        fields: {note: payload.note}
stop-node:
`)
	return root
}

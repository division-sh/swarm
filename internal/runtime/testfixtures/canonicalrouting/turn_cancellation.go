package canonicalrouting

import "testing"

// This stateless frontier isolates authored cancellation and recovery from
// graph-dependent stage eligibility and sink diagnostics.
func CopyTurnCancellationRecovery(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", "name: turn-cancellation-recovery\npins:\n  inputs: [work.requested]\n")
	writeClosedVariantFile(t, root, "events.yaml", "work.requested:\n  case_id: text\nwork.started:\n  case_id: text\nwork.timed_out:\nwork.timeout_recorded:\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `start:
  execution_type: system_node
  subscribes_to: [work.requested]
  event_handlers:
    work.requested:
      emit: {event: work.started, fields: {case_id: payload.case_id}}
timeout-observer:
  execution_type: system_node
  subscribes_to: [work.timed_out]
  event_handlers:
    work.timed_out:
      emit: {event: work.timeout_recorded}
recorded-observer:
  execution_type: system_node
  subscribes_to: [work.timeout_recorded]
  event_handlers:
    work.timeout_recorded:
      guard: {id: observed_timeout, check: true}
`)
	writeClosedVariantFile(t, root, "agents.yaml", `worker:
  role: timeout-worker
  intent: {inline: 'Execute one bounded turn.'}
  model: regular
  subscriptions: [work.started]
  turn_timeout: {after: 1ns, emit: work.timed_out}
  mock:
    kind: python
    module: mocks/worker.py
`)
	writeClosedVariantFile(t, root, "mocks/worker.py", "def handle(input):\n    return {\"text\": \"Complete\", \"usage\": {\"input_tokens\": 1, \"output_tokens\": 1}}\n")
	return root
}

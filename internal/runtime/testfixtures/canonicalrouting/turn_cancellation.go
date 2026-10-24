package canonicalrouting

import "testing"

// This stateless frontier isolates authored cancellation and recovery from
// graph-dependent stage eligibility and sink diagnostics.
func CopyTurnCancellationRecovery(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", "name: turn-cancellation-recovery\ninputs: [work.requested]\n")
	writeClosedVariantFile(t, root, "events.yaml", "work.requested:\n  case_id: text\nwork.started:\n  case_id: text\nwork.timed_out:\nwork.timeout_recorded:\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `start:
  work.requested:
    emit: {event: work.started, fields: {case_id: payload.case_id}}
timeout-observer:
  work.timed_out:
    emit: {event: work.timeout_recorded}
recorded-observer:
  work.timeout_recorded:
    guard: true
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

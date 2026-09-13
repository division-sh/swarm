package canonicalrouting

import (
	"os"
	"path/filepath"
	"testing"
)

// CopyGateCompletionDiagnostic is a source-only typed gate used on both the
// baseline and compiled-transition trees; it carries no transition test API.
func CopyGateCompletionDiagnostic(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: gate-completion-diagnostic
stages:
  waiting: {initial: true}
  review:
    gate:
      decision: review_decision
      outcomes:
        approve:
          advances_to: approved
          emit:
            event: work.completed
            fields: {result: {literal: approved}}
        reject:
          input:
            reason: {type: text, required: true}
          advances_to: approved
          emit:
            event: work.completed
            fields: {result: {literal: rejected}}
  approved: {}
  done: {terminal: true}
pins:
  inputs:
    events:
      - {event: work.requested, source: external}
`)
	writeClosedVariantFile(t, root, "events.yaml", "work.requested:\n  seed: boolean\nwork.completed:\n  result: text\n")
	writeClosedVariantFile(t, root, "entities.yaml", "work:\n  result: text\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `requester:
  execution_type: system_node
  subscribes_to: [work.requested]
  event_handlers:
    work.requested:
      create_entity: true
      advances_to: waiting
      emit:
        event: work.completed
        fields: {result: {literal: ready}}
collector:
  execution_type: system_node
  subscribes_to: [work.completed]
  event_handlers:
    work.completed:
      data_accumulation:
        writes:
          - target_field: result
            expression: payload.result
      rules:
        enter_review:
          condition: "payload.result == 'ready'"
          advances_to: review
        finish:
          condition: "payload.result in ['approved', 'rejected']"
          advances_to: done
`)
	return root
}

func CopyMailboxCompletionMatrix(t testing.TB) string {
	t.Helper()
	root := CopyGateCompletionDiagnostic(t)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "stages:\n", "imports:\n  connector_packs:\n    - provider: telegram\n      tool: telegram.send_message\nstages:\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "      - {event: work.requested, source: external}\n", "      - {event: work.requested, source: external}\n      - {event: effect.requested, source: external}\n")
	nodes, err := os.ReadFile(filepath.Join(root, "nodes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	writeClosedVariantFile(t, root, "nodes.yaml", string(nodes)+`effect-requester:
  execution_type: system_node
  subscribes_to: [effect.requested]
  event_handlers:
    effect.requested:
      activity:
        id: telegram_send_message
        tool: telegram.send_message
        approval: {decision: send_telegram_message}
        input:
          chat_id: {literal: 42}
          text: {literal: review}
effect-revision:
  execution_type: system_node
  subscribes_to: [telegram_send_message.revision_requested]
  event_handlers:
    telegram_send_message.revision_requested: {}
`)
	writeClosedVariantFile(t, root, "observers/schema.yaml", `name: observers
mode: singleton
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - {event: observer.requested, source: external}
`)
	writeClosedVariantFile(t, root, "observers/events.yaml", "observer.requested:\n  seed: boolean\nobserver.started:\n  seed: boolean\n")
	writeClosedVariantFile(t, root, "observers/entities.yaml", "observer: {}\n")
	writeClosedVariantFile(t, root, "observers/nodes.yaml", `start:
  execution_type: system_node
  subscribes_to: [observer.requested]
  event_handlers:
    observer.requested:
      create_entity: true
      advances_to: active
      emit:
        event: observer.started
        fields: {seed: {literal: true}}
`)
	writeClosedVariantFile(t, root, "observers/agents.yaml", `observer:
  role: observer
  intent: {inline: 'Observe the completed human operation.'}
  model: regular
  memory: false
  permissions: [ask_human]
  subscriptions: [observer.started, human_task.approved, human_task.rejected, human_task.deferred, human_task.expired]
  mock:
    kind: python
    module: mocks/observer.py
`)
	writeClosedVariantFile(t, root, "observers/mocks/observer.py", `import json

def handle(input):
    if input["round"] == 1:
        frame = json.loads(input["messages"][-1]["content"])
        if frame["event"]["type"].endswith("observer.started"):
            return {"calls": [{"name": "ask_human", "arguments": {"scope": "flow", "category": "review", "description": "Review the observed work."}}], "usage": {"input_tokens": 1, "output_tokens": 1}}
    return {"text": "Observed.", "usage": {"input_tokens": 1, "output_tokens": 1}}
`)
	writeClosedVariantFile(t, root, "events.yaml", `work.requested:
  seed: boolean
work.completed:
  result: text
effect.requested:
  seed: boolean
`)
	return root
}

package canonicalrouting

import (
	"os"
	"path/filepath"
	"testing"
)

func CopyMailboxNoticeCompletion(t testing.TB) string {
	t.Helper()
	root := CopyMailboxCompletionMatrix(t)
	applyClosedReplacement(t, filepath.Join(root, "observers/schema.yaml"), "    events: [observer.requested]\n", "    events: [observer.requested, notice.requested]\n")
	applyClosedReplacement(t, filepath.Join(root, "observers/nodes.yaml"), "    observer.requested:\n      create_entity: true\n", "    observer.requested:\n      create_entity: true\n    notice.requested:\n      create_entity: true\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "      - observer.requested\n", "      - observer.requested\n      - notice.requested\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "    events: [observer.requested]\nconnect:\n", "    events: [observer.requested, notice.requested]\nconnect:\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "  - {event: observer.requested, from: ., to: observers}\n", "  - {event: observer.requested, from: ., to: observers}\n  - {event: notice.requested, from: ., to: observers}\n")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "observer.requested:\n  seed: boolean\n", "observer.requested:\n  seed: boolean\nnotice.requested:\n  seed: boolean\n")
	applyClosedReplacement(t, filepath.Join(root, "observers/agents.yaml"), "subscriptions: [observer.requested,", "subscriptions: [notice.requested, observer.requested,")
	applyClosedReplacement(t, filepath.Join(root, "observers/mocks/observer.py"), `        if frame["event"]["type"].endswith("observer.requested"):`, `        if frame["event"]["type"].endswith("notice.requested"):
            return {"calls": [{"name": "notify_human", "arguments": {"summary": "Observed notice", "context": {"proof": "mailbox-completion"}}}], "usage": {"input_tokens": 1, "output_tokens": 1}}
        if frame["event"]["type"].endswith("observer.requested"):`)
	return root
}

func CopyMailboxCompletionMatrix(t testing.TB) string {
	t.Helper()
	root := CopyGateCompletionDiagnostic(t)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "        reject:\n", "        reject:\n          input:\n            reason: {type: text, required: true}\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "stages:\n", "imports:\n  connector_packs:\n    - provider: telegram\n      tool: telegram.send_message\nstages:\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "      - work.requested\n", "      - work.requested\n      - effect.requested\n      - observer.requested\n  outputs:\n    events: [observer.requested]\nconnect:\n  - {event: observer.requested, from: ., to: observers}\n")
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
          chat_id: {literal: "42"}
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
    events: [observer.requested]
`)
	writeClosedVariantFile(t, root, "observers/entities.yaml", "observer: {}\n")
	writeClosedVariantFile(t, root, "observers/nodes.yaml", `start:
  execution_type: system_node
  subscribes_to: [observer.requested]
  event_handlers:
    observer.requested:
      create_entity: true
      advances_to: active
`)
	writeClosedVariantFile(t, root, "observers/agents.yaml", `observer:
  role: observer
  intent: {inline: 'Observe the completed human operation.'}
  model: regular
  memory: false
  permissions: [ask_human]
  subscriptions: [observer.requested, human_task.approved, human_task.rejected, human_task.deferred, human_task.expired]
  mock:
    kind: python
    module: mocks/observer.py
`)
	writeClosedVariantFile(t, root, "observers/mocks/observer.py", `import json

def handle(input):
    if input["round"] == 1:
        frame = json.loads(input["messages"][-1]["content"])
        if frame["event"]["type"].endswith("observer.requested"):
            return {"calls": [{"name": "ask_human", "arguments": {"scope": "flow", "category": "review", "description": "Review the observed work."}}], "usage": {"input_tokens": 1, "output_tokens": 1}}
    return {"text": "Observed.", "usage": {"input_tokens": 1, "output_tokens": 1}}
`)
	writeClosedVariantFile(t, root, "events.yaml", `work.requested:
  seed: boolean
work.completed:
  result: text
effect.requested:
  seed: boolean
observer.requested:
  seed: boolean
`)
	return root
}

// CopyHumanTaskOwnership keeps the real mock tool dispatcher and varies only
// the declaring flow topology. Template agents acquire their materialized owner;
// ordinary root/static/singleton declaration agents remain entityless.
func CopyHumanTaskOwnership(t testing.TB, mode string) string {
	t.Helper()
	root := CopyMailboxCompletionMatrix(t)
	switch mode {
	case "singleton":
	case "static":
		applyClosedReplacement(t, filepath.Join(root, "observers/schema.yaml"), "mode: singleton", "mode: static")
		applyClosedReplacement(t, filepath.Join(root, "observers/nodes.yaml"), "      create_entity: true\n      advances_to: active\n", "      emit:\n        event: observer.started\n        fields: {seed: \"${payload.seed}\", deadline_at: \"${payload.deadline_at}\"}\n")
		writeClosedVariantFile(t, root, "observers/events.yaml", "observer.started:\n  seed: boolean\n  deadline_at: text\n")
		applyClosedReplacement(t, filepath.Join(root, "observers/agents.yaml"), "subscriptions: [observer.requested,", "subscriptions: [observer.started,")
		applyClosedReplacement(t, filepath.Join(root, "observers/mocks/observer.py"), "endswith(\"observer.requested\")", "endswith(\"observer.started\")")
	case "root":
		for _, name := range []string{"nodes.yaml"} {
			parent, err := os.ReadFile(filepath.Join(root, name))
			if err != nil {
				t.Fatal(err)
			}
			child, err := os.ReadFile(filepath.Join(root, "observers", name))
			if err != nil {
				t.Fatal(err)
			}
			writeClosedVariantFile(t, root, name, string(parent)+"\n"+string(child))
		}
		applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "      advances_to: active\n", "")
		applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "    observer.requested:\n      create_entity: true\n", "    observer.requested:\n")
		for _, name := range []string{"agents.yaml", "mocks/observer.py"} {
			child, err := os.ReadFile(filepath.Join(root, "observers", name))
			if err != nil {
				t.Fatal(err)
			}
			writeClosedVariantFile(t, root, name, string(child))
		}
		applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "  outputs:\n    events: [observer.requested]\nconnect:\n  - {event: observer.requested, from: ., to: observers}\n", "")
		removeClosedVariantFiles(t, root, "observers/mocks/observer.py", "observers/mocks", "observers/agents.yaml", "observers/entities.yaml", "observers/nodes.yaml", "observers/schema.yaml", "observers")
	case "template":
		writeClosedVariantFile(t, root, "observers/entities.yaml", "observer:\n  case_id: text\n")
		applyClosedReplacement(t, filepath.Join(root, "observers/nodes.yaml"), "      create_entity: true\n", "      create_entity: true\n      data_accumulation:\n        writes:\n          - {target_field: case_id, value: \"${payload.case_id}\"}\n")
		applyClosedReplacement(t, filepath.Join(root, "observers/schema.yaml"), "mode: singleton", "mode: template\ninstance: case_id")
		applyClosedReplacement(t, filepath.Join(root, "observers/schema.yaml"), "    events: [observer.requested]", "    events:\n      - {event: observer.requested, resolution: {mode: create}}")
		applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "      - observer.requested\n", "      - observer.requested\n      - observer.seed\n")
		applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "observer.requested:\n  seed: boolean\n", "observer.requested:\n  case_id: text\n  seed: boolean\n")
		for name, suffix := range map[string]string{
			"events.yaml": "observer.seed:\n  case_id: text\n",
			"nodes.yaml": `human-template-launcher:
  execution_type: system_node
  subscribes_to: [observer.seed]
  produces: [observer.requested]
  event_handlers:
    observer.seed:
      emit:
        event: observer.requested
        fields: {case_id: "${payload.case_id}", seed: {literal: true}}
`,
		} {
			parent, err := os.ReadFile(filepath.Join(root, name))
			if err != nil {
				t.Fatal(err)
			}
			writeClosedVariantFile(t, root, name, string(parent)+"\n"+suffix)
		}
	default:
		t.Fatalf("unknown human-task topology %q", mode)
	}
	agentRoot := "observers"
	if mode == "root" {
		agentRoot = "."
	}
	if mode == "template" {
		applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "observer.seed:\n  case_id: text\n", "observer.seed:\n  case_id: text\n  deadline_at: text\n")
		applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "observer.requested:\n  case_id: text\n", "observer.requested:\n  case_id: text\n  deadline_at: text\n")
		applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "fields: {case_id: \"${payload.case_id}\", seed: {literal: true}}", "fields: {case_id: \"${payload.case_id}\", seed: {literal: true}, deadline_at: \"${payload.deadline_at}\"}")
	} else {
		applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "observer.requested:\n  seed: boolean\n", "observer.requested:\n  seed: boolean\n  deadline_at: text\n")
	}

	applyClosedReplacement(t, filepath.Join(root, agentRoot, "mocks/observer.py"), `"description": "Review the observed work."`, `"description": "Review the observed work.", "deadline_at": frame["event"]["payload"]["deadline_at"]`)
	return root
}

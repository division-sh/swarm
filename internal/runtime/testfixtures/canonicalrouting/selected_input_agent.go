package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopySelectedInputAgentProbe keeps identical public names in different flows
// so admission must retain declaration ownership rather than infer it from ID.
func CopySelectedInputAgentProbe(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for _, flow := range []struct{ path, mode string }{{"", "static"}, {"child", "static"}, {"templ", "template"}} {
		instance := ""
		if flow.mode == "template" {
			instance = "instance: work_id\n"
			writeClosedVariantFile(t, root, filepath.Join(flow.path, "entities.yaml"), "work:\n  work_id: text\n")
		}
		for file, body := range map[string]string{
			"schema.yaml": "name: selected-input-agent\n" + instance + "pins:\n  inputs:\n    - work.ready\n",
			"events.yaml": "work.ready:\n",
			"agents.yaml": "worker:\n  model: regular\n  intent:\n    inline: Complete the selected input.\n  subscriptions: [work.ready]\n",
		} {
			writeClosedVariantFile(t, root, filepath.Join(flow.path, file), body)
		}
	}
	return root
}

// CopySelectedConstructedAgentInput owns the ordinary static-child input
// fixture for real selected preparation, delivery and activation proofs.
func CopySelectedConstructedAgentInput(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for name, body := range map[string]string{
		"schema.yaml":             "name: selected-constructed-input\npins:\n  inputs:\n    - task.assigned\n  outputs:\n    - task.assigned\nconnect:\n  - {event: task.assigned, from: ., to: child}\n",
		"events.yaml":             "task.assigned:\n",
		"entities.yaml":           "run: {}\n",
		"child/schema.yaml":       "name: child\npins:\n  inputs:\n    - task.assigned\n",
		"child/entities.yaml":     "child_state: {}\n",
		"child/agents.yaml":       "worker:\n  model: regular\n  intent: prompts/worker.md\n  subscriptions: [task.assigned]\n",
		"child/prompts/worker.md": "Consume the assigned task.\n",
	} {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}

// CopySelectedNestedConstructedAgentInput keeps equal local actor names in two
// keyless levels and sibling branches below keyed parents. Its finite options
// distinguish fieldless headers from real field companions.
func CopySelectedNestedConstructedAgentInput(t testing.TB, fields bool) string {
	t.Helper()
	return copySelectedNestedConstructedAgentInput(t, fields, false)
}

func copySelectedNestedConstructedAgentInput(t testing.TB, fields, observer bool) string {
	t.Helper()
	root := t.TempDir()
	for name, body := range map[string]string{
		"schema.yaml":         "name: selected-nested-constructed-input\npins:\n  inputs:\n    - task.assigned\n    - construction.started\n  outputs:\n    - task.assigned\nconnect:\n  - {event: task.assigned, from: ., to: templ, resolution: select}\n  - {event: task.assigned, from: ., to: templ/child}\n  - {event: task.assigned, from: ., to: templ/child/leaf}\n  - {event: task.assigned, from: ., to: templ/audit}\n  - {event: task.assigned, from: ., to: templ/audit/leaf}\n",
		"events.yaml":         "task.assigned:\n  work_id: text\nconstruction.started:\n  work_id: text\n",
		"nodes.yaml":          "anchor:\n  execution_type: system_node\n  subscribes_to: [construction.started]\n  event_handlers:\n    construction.started: {}\n",
		"templ/schema.yaml":   "name: templ\ninstance: work_id\nstages:\n  active: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    - task.assigned\n    - construction.started\n",
		"templ/entities.yaml": "work:\n  work_id: text\n",
		"templ/events.yaml":   "construction.started:\n  work_id: text\n",
		"templ/nodes.yaml":    "bootstrap:\n  execution_type: system_node\n  subscribes_to: [task.assigned]\n  produces: [construction.started]\n  event_handlers:\n    task.assigned:\n      emit:\n        event: construction.started\n        fields:\n          work_id: ${payload.work_id}\nfinish:\n  execution_type: system_node\n  subscribes_to: [construction.started]\n  event_handlers:\n    construction.started:\n      advances_to: done\n",
	} {
		writeClosedVariantFile(t, root, name, body)
	}
	for _, branch := range []string{"child", "audit"} {
		for _, path := range []string{"templ/" + branch, "templ/" + branch + "/leaf"} {
			for name, body := range map[string]string{
				"events.yaml": "work.ready:\n  work_id: text\nwork.finish:\n",
				"nodes.yaml":  "publish:\n  execution_type: system_node\n  subscribes_to: [task.assigned]\n  produces: [work.ready]\n  event_handlers:\n    task.assigned:\n      emit:\n        event: work.ready\n        fields:\n          work_id: ${payload.work_id}\nfinish:\n  execution_type: system_node\n  subscribes_to: [work.finish]\n  event_handlers:\n    work.finish:\n      advances_to: done\n",
				"schema.yaml": "name: descendant\nstages:\n  active: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    - task.assigned\nrequired_agents:\n  - role: worker\n    subscribes_to: [work.ready]\n",
				"agents.yaml": "worker:\n  model: regular\n  intent:\n    inline: Consume the exact assigned task.\n  subscriptions: [work.ready]\n  emit_events: [work.finish]\n",
			} {
				if name == "agents.yaml" && observer {
					body += "observer:\n  model: regular\n  intent:\n    inline: Observe only the exact assigned task.\n  subscriptions: [work.ready]\n"
				}
				writeClosedVariantFile(t, root, filepath.Join(path, name), body)
			}
			if fields {
				writeClosedVariantFile(t, root, filepath.Join(path, "entities.yaml"), "child_state:\n  value: {type: text, initial: retained}\n")
			} else {
				writeClosedVariantFile(t, root, filepath.Join(path, "entities.yaml"), "child_state: {}\n")
			}
		}
	}
	return root
}

func CopySelectedNestedConstructedAgentRebindInput(t testing.TB, fields bool) string {
	t.Helper()
	return copySelectedNestedConstructedAgentInput(t, fields, true)
}

// CopySelectedRouteRecoveryInput keeps the original sibling fixture while
// giving the synthetic pending input a valid selected endpoint and recipient.
func CopySelectedRouteRecoveryInput(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join(RepoRoot(t), "tests/tier11-flow-composition/test-sibling-both-instantiated-isolated"), root)
	ApplyOverlay(t, root, "events.yaml", "fork.cli.activate:\n")
	writeClosedVariantFile(t, root, "agents.yaml", "safe-agent:\n  model: regular\n  intent:\n    inline: Complete the selected input.\n  subscriptions: [fork.cli.activate]\n")
	return root
}

// CopyUnresolvedSelectedForkInput admits the event's schema but deliberately
// supplies no receiver. This tests frontier refusal, not unknown-event admission.
func CopyUnresolvedSelectedForkInput(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join(RepoRoot(t), "tests/tier1-primitives/test-emits-multiple"), root)
	ApplyOverlay(t, root, "events.yaml", "ghost.event:\n")
	return root
}

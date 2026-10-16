package canonicalrouting

import "testing"

// ActivityTerminalReplayDocuments preserves the admitted activity-journal
// source independently of the runtime-only compiled tool projections.
func ActivityTerminalReplayDocuments(t testing.TB) map[string]string {
	t.Helper()
	return map[string]string{
		"schema.yaml":            "name: activity-journal-proof\nstages: []\npins:\n  inputs: [activity.source]\nconnect:\n  - event: activity.source\n    from: .\n    to: research\n    resolution: create\n",
		"events.yaml":            "activity.source:\n  marker: text\n",
		"research/schema.yaml":   "name: research\ninstance: marker\nstages: []\npins:\n  inputs: [activity.source]\n",
		"research/entities.yaml": "activity_entity:\n  marker: text\n",
		"research/nodes.yaml":    "scanner:\n  execution_type: system_node\n  subscribes_to: [activity.source]\n  event_handlers:\n    activity.source: {}\n",
	}
}

// DynamicTopologyStartupDocuments retains the source-label distinction used
// by the all-context startup refusal proofs.
func DynamicTopologyStartupDocuments(t testing.TB, label string) map[string]string {
	t.Helper()
	return map[string]string{
		"manifest.yaml":        "version: 1.0.0\nplatform_version: \">=0.7.0 <0.8.0\"\n",
		"schema.yaml":          "name: startup-preflight\nstages: []\npins:\n  inputs: [spawn.requested]\n  outputs: [worker.requested]\nconnect:\n  - event: worker.requested\n    from: .\n    to: worker\n    resolution: create\n# " + label + "\n",
		"events.yaml":          "spawn.requested:\n  worker_id: text\nworker.requested:\n  worker_id: text\n",
		"nodes.yaml":           "orchestrator:\n  execution_type: system_node\n  subscribes_to: [spawn.requested]\n  produces: [worker.requested]\n  event_handlers:\n    spawn.requested:\n      emit:\n        event: worker.requested\n        fields:\n          worker_id: payload.worker_id\n",
		"worker/schema.yaml":   "name: worker\ninstance: worker_id\nstages:\n  idle: {}\npins:\n  inputs: [worker.requested]\n",
		"worker/manifest.yaml": "version: 1.0.0\nplatform_version: \">=0.7.0 <0.8.0\"\n",
		"worker/entities.yaml": "worker:\n  worker_id: text\n",
		"worker/nodes.yaml":    "task-handler:\n  execution_type: system_node\n  subscribes_to: [worker.requested]\n  produces: []\n  event_handlers:\n    worker.requested: {}\n",
	}
}

// ReceiverPendingExecutionDocuments retains the real mock consumer used by
// pending-attachment, recovery and settlement proofs.
func ReceiverPendingExecutionDocuments(t testing.TB) map[string]string {
	t.Helper()
	return map[string]string{
		"schema.yaml":            "name: pending-static-actor\npins:\n  inputs: [work.requested]\nconnect:\n  - {event: work.requested, from: ., to: review}\n",
		"events.yaml":            "work.requested:\n",
		"review/schema.yaml":     "name: review\nstages:\n  pending: {}\npins:\n  inputs: [work.requested]\n",
		"review/agents.yaml":     "worker:\n  model: regular\n  intent: {inline: Acknowledge the exact assigned work.}\n  subscriptions: [work.requested]\n  mock: {kind: python, module: mocks/worker.py}\n",
		"review/mocks/worker.py": "def handle(input):\n    return {\"text\": \"Acknowledged.\", \"usage\": {\"input_tokens\": 1, \"output_tokens\": 1}}\n",
	}
}

// ReceiverPublicationUncertaintyDocuments keeps publication uncertainty
// separate from attachment or handler execution.
func ReceiverPublicationUncertaintyDocuments(t testing.TB) map[string]string {
	t.Helper()
	return map[string]string{
		"schema.yaml":        "name: uncertain-static-receiver\npins:\n  inputs: [work.requested]\nconnect:\n  - {event: work.requested, from: ., to: review}\n",
		"events.yaml":        "work.requested:\n",
		"review/schema.yaml": "name: review\nstages:\n  pending: {}\npins:\n  inputs: [work.requested]\n",
		"review/agents.yaml": "worker:\n  model: regular\n  intent: {inline: Review the assigned work.}\n  subscriptions: [work.requested]\n",
	}
}

package canonicalrouting

import "testing"

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

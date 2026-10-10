package canonicalrouting

import "testing"

func CopyRootAgentLocalEmission(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, checkedArtifactRoot(t, ArtifactID("tests/tier8-boot-verification/test-boot-event-cycle")), root)
	writeClosedVariantFile(t, root, "agents.yaml", "root-agent:\n  model: regular\n  intent: {inline: Emit the cycle event.}\n  emit_events: [cycle.ping]\n")
	return root
}

func CopyStaticAgentOutputConnect(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for path, body := range map[string]string{
		"schema.yaml":          "name: static-agent-output\nconnect:\n  - {event: deploy.done, from: producer, to: consumer, rename: deploy.completed}\n",
		"producer/schema.yaml": "name: producer\npins:\n  outputs: [deploy.done]\n",
		"producer/events.yaml": "deploy.done:\n",
		"producer/agents.yaml": "producer-agent:\n  model: regular\n  intent: {inline: Report deployment completion.}\n  emit_events: [deploy.done]\n",
		"consumer/schema.yaml": "name: consumer\npins:\n  inputs: [deploy.completed]\n",
		"consumer/nodes.yaml":  "consumer-node:\n  execution_type: system_node\n  event_handlers:\n    deploy.completed:\n      guard: {id: received, check: true}\n",
	} {
		writeClosedVariantFile(t, root, path, body)
	}
	return root
}

func CopyTemplateAgentOutputRootConnect(t testing.TB) string {
	t.Helper()
	root := CopyTemplateOutputRootConnect(t)
	writeClosedVariantFile(t, root, "producer/agents.yaml", "producer-agent:\n  model: regular\n  intent: {inline: Report deployment completion.}\n  emit_events: [deploy.done]\n")
	return root
}

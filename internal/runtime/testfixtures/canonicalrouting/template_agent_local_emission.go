package canonicalrouting

import "testing"

func CopyTemplateAgentLocalEmission(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for path, body := range map[string]string{
		"schema.yaml":          "name: local-agent-emission\npins:\n  outputs: [assessment.reported]\nconnect:\n  - {event: assessment.reported, from: review, to: .}\n",
		"review/schema.yaml":   "name: review\ninstance: instance_key\npins:\n  outputs: [assessment.reported]\n",
		"review/entities.yaml": "review:\n  instance_key: {type: text, _unused_reason: constructor identity}\n",
		"review/events.yaml":   "assessment.reported:\n",
		"review/agents.yaml":   "reviewer:\n  model: regular\n  intent: {inline: Report the assessment.}\n  emit_events: [assessment.reported]\n",
		"review/nodes.yaml":    "review-finalize:\n  assessment.reported:\n    guard: true\n",
	} {
		writeClosedVariantFile(t, root, path, body)
	}
	return root
}

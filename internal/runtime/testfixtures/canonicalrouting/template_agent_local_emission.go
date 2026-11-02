package canonicalrouting

import "testing"

func CopyTemplateAgentLocalEmission(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for path, body := range map[string]string{
		"manifest.yaml":        "version: 1.0.0\nplatform_version: '*'\n",
		"review/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":          "# fixture: local-agent-emission\npins:\n  outputs: [assessment.reported]\nconnect:\n  - {event: assessment.reported, from: review, to: .}\n",
		"review/schema.yaml":   "# fixture: review\ninstance: instance_key\npins:\n  outputs: [assessment.reported]\n",
		"review/state.yaml":    "instance_key: text\n",
		"review/events.yaml":   "assessment.reported:\n",
		"review/agents.yaml":   "reviewer:\n  model: regular\n  intent: {inline: Report the assessment.}\n  emit_events: [assessment.reported]\n",
		"review/nodes.yaml":    "review-finalize:\n  assessment.reported:\n    guard: true\n",
	} {
		writeClosedVariantFile(t, root, path, body)
	}
	return root
}

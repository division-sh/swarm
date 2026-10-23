package canonicalrouting

import "testing"

// WriteManagerMockAlternatives declares the static and instantiated agents
// whose captured performances survive manager persistence and recovery.
func WriteManagerMockAlternatives(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for relative, source := range map[string]string{
		"schema.yaml":                "stages: []\n",
		"static-support/schema.yaml": "stages: []\n",
		"static-support/agents.yaml": `static-worker:
  role: worker
  model: regular
  intent: {inline: Handle static support work.}
  mock: {kind: python, module: mocks/agent.py}
`,
		"template-support/schema.yaml":   "instance: instance_id\n",
		"template-support/entities.yaml": "instance:\n  instance_id: {type: text, immutable: true}\n",
		"template-support/agents.yaml": `worker:
  role: worker
  model: regular
  intent: {inline: Handle instantiated support work.}
  mock: {kind: python, module: mocks/agent.py}
`,
		"static-support/mocks/agent.py":   "def handle(input):\n    return {'text': 'mock'}\n",
		"template-support/mocks/agent.py": "def handle(input):\n    return {'text': 'mock'}\n",
	} {
		writeClosedVariantFile(t, root, relative, source)
	}
	return root
}

package canonicalrouting

import "testing"

// WritePreparedTargetConflict owns the source for the retained delivery-record
// corruption proof. Its historical route fixture is not executable authority.
func WritePreparedTargetConflict(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for relative, source := range map[string]string{
		"schema.yaml": "pins:\n  inputs: [prepared.aggregate.corruption]\n",
		"events.yaml": "prepared.aggregate.corruption:\n",
		"nodes.yaml": `validator-one:
  execution_type: system_node
  subscribes_to: [prepared.aggregate.corruption]
  event_handlers:
    prepared.aggregate.corruption: {}
validator-two:
  execution_type: system_node
  subscribes_to: [prepared.aggregate.corruption]
`,
		"review/schema.yaml":   "instance: instance_key\n",
		"review/entities.yaml": "review:\n  instance_key: {type: text, immutable: true}\n",
	} {
		writeClosedVariantFile(t, root, relative, source)
	}
	return root
}

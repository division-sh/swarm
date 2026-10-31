package canonicalrouting

import "testing"

// WritePreparedTargetConflict owns the source for the retained delivery-record
// corruption proof. Its historical route fixture is not executable authority.
func WritePreparedTargetConflict(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for relative, source := range map[string]string{
		"manifest.yaml":        "version: 1.0.0\nplatform_version: '*'\n",
		"review/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":          "pins:\n  inputs: [prepared.aggregate.corruption, test.work]\n",
		"events.yaml":          "prepared.aggregate.corruption:\ntest.work:\n",
		"nodes.yaml": `validator-one:
  execution_type: system_node
  subscribes_to: [prepared.aggregate.corruption]
  event_handlers:
    prepared.aggregate.corruption: {}
validator-two:
  execution_type: system_node
  subscribes_to: [prepared.aggregate.corruption]
validator:
  execution_type: system_node
  subscribes_to: [test.work]
  event_handlers:
    test.work: {}
`,
		"review/schema.yaml": "instance: instance_key\n",
		"review/state.yaml":  "instance_key: text\n",
	} {
		writeClosedVariantFile(t, root, relative, source)
	}
	return root
}

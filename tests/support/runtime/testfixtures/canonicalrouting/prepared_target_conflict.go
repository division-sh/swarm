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
		"schema.yaml":          "inputs: [prepared.aggregate.corruption, test.work]\n",
		"events.yaml":          "prepared.aggregate.corruption:\ntest.work:\n",
		"nodes.yaml": `validator-one:
  prepared.aggregate.corruption: {}
validator-two:
  prepared.aggregate.corruption: {}
validator:
  test.work: {}
`,
		"review/schema.yaml": "instance: instance_key\n",
		"review/state.yaml":  "instance_key: text\n",
	} {
		writeClosedVariantFile(t, root, relative, source)
	}
	return root
}

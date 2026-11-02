package canonicalrouting

import "testing"

func CopySelectedForkRollback(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	writeClosedVariantFile(t, root, "schema.yaml", "# fixture: selected-fork-rollback\ninputs: [item.received]\noutputs: [item.received]\n")
	writeClosedVariantFile(t, root, "events.yaml", "item.received:\n")
	return root
}

package canonicalrouting

import "testing"

func CopySelectedForkRollback(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", "# fixture: selected-fork-rollback\npins:\n  inputs: [item.received]\n  outputs: [item.received]\n")
	writeClosedVariantFile(t, root, "events.yaml", "item.received:\n")
	return root
}

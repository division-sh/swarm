package canonicalrouting

import "testing"

func CopyBusPublicationFailure(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	writeClosedVariantFile(t, root, "schema.yaml", `# fixture: bus-publication-failure
pins:
  inputs: [task.requested, task.failed, custom.receipt_failure]
  outputs: [task.requested, task.failed, custom.receipt_failure]
`)
	writeClosedVariantFile(t, root, "events.yaml", "task.requested:\ntask.failed:\ncustom.receipt_failure:\n")
	return root
}

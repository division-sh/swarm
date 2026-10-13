package canonicalrouting

import "testing"

func CopyBusPublicationFailure(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: bus-publication-failure
pins:
  inputs: [task.requested, task.failed, custom.receipt_failure]
  outputs: [task.requested, task.failed, custom.receipt_failure]
`)
	writeClosedVariantFile(t, root, "events.yaml", "task.requested:\ntask.failed:\ncustom.receipt_failure:\n")
	return root
}

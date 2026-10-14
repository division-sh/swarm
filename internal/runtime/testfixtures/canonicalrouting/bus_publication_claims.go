package canonicalrouting

import "testing"

func CopyBusPublicationClaims(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: bus-publication-claims
pins:
  inputs: [task.completed, task.failed, custom.shared_claim, custom.pool_saturation]
  outputs: [task.completed, task.failed, custom.shared_claim, custom.pool_saturation]
`)
	writeClosedVariantFile(t, root, "events.yaml", "task.completed:\ntask.failed:\ncustom.shared_claim:\ncustom.pool_saturation:\n")
	return root
}

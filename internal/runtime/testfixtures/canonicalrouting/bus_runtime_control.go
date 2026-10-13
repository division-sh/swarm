package canonicalrouting

import "testing"

func CopyBusRuntimeControl(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: bus-runtime-control
pins:
  inputs: [scan.requested, custom.paused]
  outputs: [scan.requested, custom.paused]
`)
	writeClosedVariantFile(t, root, "events.yaml", "scan.requested:\ncustom.paused:\n")
	return root
}

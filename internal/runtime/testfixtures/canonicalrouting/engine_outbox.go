package canonicalrouting

import "testing"

func CopyEngineOutboxPublication(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: engine-outbox-publication
pins:
  inputs: [custom.emitted, custom.followup, custom.snapshot, custom.claimed, custom.conflicting]
  outputs: [custom.emitted, custom.followup, custom.snapshot, custom.claimed, custom.conflicting]
`)
	writeClosedVariantFile(t, root, "events.yaml", "custom.emitted:\ncustom.followup:\ncustom.snapshot:\ncustom.claimed:\ncustom.conflicting:\n")
	return root
}

func CopyEngineOutboxTarget(t testing.TB) string {
	t.Helper()
	root := CopyEngineOutboxPublication(t)
	writeClosedVariantFile(t, root, "flow-original/schema.yaml", "name: original-target\n")
	writeClosedVariantFile(t, root, "flow-original/events.yaml", "custom.snapshot:\n")
	return root
}

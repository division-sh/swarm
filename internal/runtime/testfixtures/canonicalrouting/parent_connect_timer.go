package canonicalrouting

import "testing"

// CopyParentConnectTimer preserves the canonical connect and makes its producer
// a staged timer owner through authored source rather than semantic-map edits.
func CopyParentConnectTimer(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	writeClosedVariantFile(t, root, "producer/schema.yaml", `name: producer
stages:
  waiting:
    initial: true
    timers:
      - {id: work_ready, after: 40ms, emit: work.ready}
pins:
  inputs:
    - work.requested
  outputs:
    - work.ready
`)
	writeClosedVariantFile(t, root, "producer/entities.yaml", "test_entity: {}\n")
	return root
}

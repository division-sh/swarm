package canonicalrouting

import "testing"

// CopyParentConnectClock replaces the canonical reactive producer with a bare
// clock producer. The disconnected variant keeps an independent same-name
// receiver declaration, which is not subscription authority.
func CopyParentConnectClock(t testing.TB, connected bool) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	removeInheritedScenarios(t, root)
	removeClosedVariantFiles(t, root, "events.yaml", "producer/nodes.yaml")
	rootSchema := "stages: []\n"
	if connected {
		rootSchema += "connect:\n  - {event: work.ready, from: producer, to: consumer}\n"
	} else {
		writeClosedVariantFile(t, root, "consumer/events.yaml", "work.ready:\n")
	}
	writeClosedVariantFile(t, root, "schema.yaml", rootSchema)
	writeClosedVariantFile(t, root, "producer/schema.yaml", "stages: []\nschedules:\n  poll: {every: 5m, emit: work.ready}\npins:\n  outputs:\n    events: [work.ready]\n")
	writeClosedVariantFile(t, root, "producer/events.yaml", "work.ready:\n")
	writeClosedVariantFile(t, root, "consumer/schema.yaml", "stages: []\npins:\n  inputs:\n    events: [work.ready]\n")
	return root
}

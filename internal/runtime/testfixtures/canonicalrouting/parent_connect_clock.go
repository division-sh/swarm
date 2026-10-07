package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyClockDeployment exercises real no-argument root-tree construction without
// a public input, provider binding or authored standing activation.
func CopyClockDeployment(t testing.TB, nested bool) string {
	t.Helper()
	root := t.TempDir()
	if !nested {
		writeClosedVariantFile(t, root, "schema.yaml", "name: clock-export\nstages: []\nschedules:\n  poll: {every: 250ms, emit: poll.tick}\npins:\n  outputs: [poll.tick]\n")
		writeClosedVariantFile(t, root, "events.yaml", "poll.tick:\n")
		return root
	}
	copyTree(t, filepath.Join(RepoRoot(t), "internal/runtime/testfixtures/canonicalrouting/testdata/clock-deployment/nested"), root)
	writeClosedVariantFile(t, root, "schema.yaml", "name: clock-connected\nstages: []\nconnect:\n  - {event: poll.tick, from: clock, to: consumer}\n")
	return root
}

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
	writeClosedVariantFile(t, root, "producer/schema.yaml", "stages: []\nschedules:\n  poll: {every: 5m, emit: work.ready}\npins:\n  outputs: [work.ready]\n")
	writeClosedVariantFile(t, root, "producer/events.yaml", "work.ready:\n")
	writeClosedVariantFile(t, root, "consumer/schema.yaml", "stages: []\npins:\n  inputs: [work.ready]\n")
	return root
}

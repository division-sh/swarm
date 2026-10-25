package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyGuardForkContinuation adds a real guard outcome before the existing
// selected root-agent frontier. It does not bypass historical replay admission.
func CopyGuardForkContinuation(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join(RepoRoot(t), "tests/tier12-runtime-fork/test-selected-contract-fork-execution"), root)
	writeClosedVariantFile(t, root, "schema.yaml", `stages:
  ready: {}
  done: {final: true}
inputs:
  - check.requested
  - task.ready
  - flow.finished
outputs:
  - check.requested
connect:
  - {event: check.requested, from: ., to: guarded}
`)
	writeClosedVariantFile(t, root, "events.yaml", "check.requested:\n  score: integer\ntask.ready:\nflow.finished:\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `finish:
  flow.finished: {advances_to: done}
`)
	writeClosedVariantFile(t, root, "guarded/schema.yaml", `name: guarded
stages:
  ready: {}
  killed: {final: true}
inputs:
  - check.requested
`)
	removeClosedVariantFiles(t, root, "guarded/state.yaml")
	writeClosedVariantFile(t, root, "guarded/nodes.yaml", `test-node:
  check.requested:
    rules:
      - when: payload.score >= 100
      - advances_to: killed
        terminate: true
`)
	return root
}

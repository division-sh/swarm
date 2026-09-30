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
  ready: {initial: true}
  killed: {terminal: true}
pins:
  inputs:
    events: [check.requested, task.ready]
`)
	writeClosedVariantFile(t, root, "events.yaml", "check.requested:\n  score: integer\ntask.ready:\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `test-node:
  execution_type: system_node
  subscribes_to: [check.requested]
  event_handlers:
    check.requested:
      guard: {id: score_check, check: 'payload.score >= 100', on_fail: kill}
`)
	return root
}

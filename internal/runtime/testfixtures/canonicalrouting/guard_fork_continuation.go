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
  done: {terminal: true}
pins:
  inputs:
    events: [check.requested, task.ready, flow.finished]
  outputs:
    events: [check.requested]
connect:
  - {event: check.requested, from: ., to: guarded}
`)
	writeClosedVariantFile(t, root, "events.yaml", "check.requested:\n  score: integer\ntask.ready:\nflow.finished:\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `finish:
  execution_type: system_node
  subscribes_to: [flow.finished]
  event_handlers:
    flow.finished: {advances_to: done}
`)
	writeClosedVariantFile(t, root, "guarded/schema.yaml", `name: guarded
stages:
  ready: {initial: true}
  killed: {terminal: true}
pins:
  inputs:
    events: [check.requested]
`)
	writeClosedVariantFile(t, root, "guarded/entities.yaml", "test_entity: {}\n")
	writeClosedVariantFile(t, root, "guarded/nodes.yaml", `test-node:
  execution_type: system_node
  subscribes_to: [check.requested]
  event_handlers:
    check.requested:
      create_entity: true
      guard: {id: score_check, check: 'payload.score >= 100', on_fail: kill}
`)
	return root
}

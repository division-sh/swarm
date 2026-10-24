package canonicalrouting

import "testing"

// CopyTargetOwnerDiamond creates the closed hostile topology used to prove
// exact target ownership through two nested fan-out levels and convergence.
func CopyTargetOwnerDiamond(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{

		"schema.yaml": `name: target-owner-diamond
inputs:
  - branch.done
outputs:
  - branch.start
connect:
  - event: branch.start
    from: .
    to: branch
    resolution: select
  - event: branch.done
    from: branch
    to: .
`,
		"events.yaml": "branch.start:\n  key: branch_id\n  branch_id: string\n",
		"nodes.yaml": `root-collector:
  branch.done:
    guard: instance.id != ""
`,
		"branch/schema.yaml": `name: branch
instance: branch_id
inputs:
  - branch.start
outputs:
  - work.ready
  - branch.done
connect:
  - event: work.ready
    from: .
    to: worker/result-static
  - event: work.ready
    from: .
    to: worker/result
`,
		"branch/state.yaml":  "branch_id: string\n",
		"branch/events.yaml": "work.ready:\n  branch_id: string\nbranch.done:\n  branch_id: string\n",
		"branch/nodes.yaml": `branch-worker:
  branch.start:
    guard: instance.id != ""
`,

		"branch/worker/result-static/schema.yaml": `name: static-result
inputs:
  - work.ready
`,
		"branch/worker/result-static/nodes.yaml": `static-result-node:
  work.ready:
    guard: instance.id != ""
`,
		"branch/worker/result/schema.yaml": `name: singleton-result
inputs:
  - work.ready
`,
		"branch/worker/result/nodes.yaml": `singleton-result-node:
  work.ready: {}
`,
		"decoy/schema.yaml": `name: decoy
outputs:
  - work.ready
`,
		"decoy/events.yaml": "work.ready:\n  branch_id: string\n",
		"unrelated/worker/result/schema.yaml": `name: hostile
inputs:
  - work.ready
`,
		"unrelated/worker/result/events.yaml": "work.ready:\n  branch_id: string\n",
		"unrelated/worker/result/nodes.yaml": `hostile-node:
  work.ready:
    guard: instance.id != ""
`,
	}
	for name, body := range files {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}

package canonicalrouting

import "testing"

// CopyTargetOwnerDiamond creates the closed hostile topology used to prove
// exact target ownership through two nested fan-out levels and convergence.
func CopyTargetOwnerDiamond(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"branch/manifest.yaml":                      "version: 1.0.0\nplatform_version: '*'\n",
		"branch/worker/manifest.yaml":               "version: 1.0.0\nplatform_version: '*'\n",
		"branch/worker/result-static/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"branch/worker/result/manifest.yaml":        "version: 1.0.0\nplatform_version: '*'\n",
		"decoy/manifest.yaml":                       "version: 1.0.0\nplatform_version: '*'\n",
		"manifest.yaml":                             "version: 1.0.0\nplatform_version: '*'\n",
		"unrelated/manifest.yaml":                   "version: 1.0.0\nplatform_version: '*'\n",
		"unrelated/worker/manifest.yaml":            "version: 1.0.0\nplatform_version: '*'\n",
		"unrelated/worker/result/manifest.yaml":     "version: 1.0.0\nplatform_version: '*'\n",

		"schema.yaml": `# fixture: target-owner-diamond
pins:
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
  execution_type: system_node
  event_handlers:
    branch.done:
      guard:
        id: root_owner
        check: _entity.id != ""
`,
		"branch/schema.yaml": `# fixture: branch
instance: branch_id
pins:
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
		"branch/entities.yaml": "branch_state:\n  branch_id:\n    type: string\n    _unused_reason: concrete diamond branch identity\n",
		"branch/events.yaml":   "work.ready:\n  branch_id: string\nbranch.done:\n  branch_id: string\n",
		"branch/nodes.yaml": `branch-worker:
  execution_type: system_node
  event_handlers:
    branch.start:
      guard:
        id: branch_owner
        check: _entity.id != ""
`,

		"branch/worker/result-static/schema.yaml": `# fixture: static-result
pins:
  inputs:
    - work.ready
`,
		"branch/worker/result-static/nodes.yaml": `static-result-node:
  execution_type: system_node
  event_handlers:
    work.ready:
      guard:
        id: inherited_owner
        check: _entity.id != ""
`,
		"branch/worker/result/schema.yaml": `# fixture: singleton-result
pins:
  inputs:
    - work.ready
`,
		"branch/worker/result/nodes.yaml": `singleton-result-node:
  execution_type: system_node
  event_handlers:
    work.ready: {}
`,
		"decoy/schema.yaml": `# fixture: decoy
pins:
  outputs:
    - work.ready
`,
		"decoy/events.yaml": "work.ready:\n  branch_id: string\n",
		"unrelated/worker/result/schema.yaml": `# fixture: hostile
pins:
  inputs:
    - work.ready
`,
		"unrelated/worker/result/events.yaml": "work.ready:\n  branch_id: string\n",
		"unrelated/worker/result/nodes.yaml": `hostile-node:
  execution_type: system_node
  event_handlers:
    work.ready:
      guard:
        id: hostile_owner
        check: _entity.id != ""
`,
	}
	for name, body := range files {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}

package canonicalrouting

import "testing"

// CopyTargetOwnerDiamond creates the closed hostile topology used to prove
// exact target ownership through two nested fan-out levels and convergence.
func CopyTargetOwnerDiamond(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"package.yaml": `name: target-owner-diamond
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: branch
    flow: branch
    mode: template
  - id: decoy
    flow: decoy
    mode: static
  - id: hostile
    flow: unrelated/worker/result
    mode: static
connect:
  - event: branch.start
    from: .
    to: branch
  - event: branch.done
    from: branch
    to: .
  - event: work.ready
    from: decoy
    to: hostile
`,
		"schema.yaml": `name: target-owner-diamond
pins:
  inputs:
    events:
      - name: branch_done
        event: branch.done
  outputs:
    events:
      - name: branch_start
        event: branch.start
`,
		"events.yaml": "branch.start:\n  branch_id: string\nbranch.done:\n  branch_id: string\n",
		"nodes.yaml": `root-collector:
  id: root-collector
  execution_type: system_node
  event_handlers:
    branch.done:
      guard:
        id: root_owner
        check: '_entity.id != ""'
`,
		"flows/branch/schema.yaml": `name: branch
mode: template
instance: branch_id
pins:
  inputs:
    events:
      - name: branch_start
        event: branch.start
        resolution:
          mode: select
        carries:
          branch_id:
            from: payload.branch_id
            type: string
  outputs:
    events:
      - name: work_ready
        event: work.ready
      - name: branch_done
        event: branch.done
`,
		"flows/branch/entities.yaml": "branch_state:\n  branch_id:\n    type: string\n    _unused_reason: concrete diamond branch identity\n",
		"flows/branch/events.yaml":   "branch.start:\n  branch_id: string\nwork.ready:\n  branch_id: string\nbranch.done:\n  branch_id: string\n",
		"flows/branch/nodes.yaml": `branch-worker:
  id: branch-worker
  execution_type: system_node
  event_handlers:
    branch.start:
      guard:
        id: branch_owner
        check: '_entity.id != ""'
`,
		"flows/branch/package.yaml": `name: branch-children
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: static-result
    flow: worker/result-static
    mode: static
  - id: singleton-result
    flow: worker/result
    mode: singleton
connect:
  - event: work.ready
    from: .
    to: static-result
  - event: work.ready
    from: .
    to: singleton-result
`,
		"flows/branch/flows/worker/result-static/schema.yaml": `name: static-result
mode: static
pins:
  inputs:
    events:
      - name: work_ready
        event: work.ready
`,
		"flows/branch/flows/worker/result-static/events.yaml": "work.ready:\n  branch_id: string\n",
		"flows/branch/flows/worker/result-static/nodes.yaml": `static-result-node:
  id: static-result-node
  execution_type: system_node
  event_handlers:
    work.ready:
      guard:
        id: inherited_owner
        check: '_entity.id != ""'
`,
		"flows/branch/flows/worker/result/schema.yaml": `name: singleton-result
mode: singleton
pins:
  inputs:
    events:
      - name: work_ready
        event: work.ready
`,
		"flows/branch/flows/worker/result/events.yaml": "work.ready:\n  branch_id: string\n",
		"flows/branch/flows/worker/result/nodes.yaml": `singleton-result-node:
  id: singleton-result-node
  execution_type: system_node
  event_handlers:
    work.ready:
      create_entity: true
`,
		"flows/decoy/schema.yaml": `name: decoy
mode: static
pins:
  outputs:
    events:
      - name: work_ready
        event: work.ready
`,
		"flows/decoy/events.yaml": "work.ready:\n  branch_id: string\n",
		"flows/unrelated/worker/result/schema.yaml": `name: hostile
mode: static
pins:
  inputs:
    events:
      - name: work_ready
        event: work.ready
`,
		"flows/unrelated/worker/result/events.yaml": "work.ready:\n  branch_id: string\n",
		"flows/unrelated/worker/result/nodes.yaml": `hostile-node:
  id: hostile-node
  execution_type: system_node
  event_handlers:
    work.ready:
      guard:
        id: hostile_owner
        check: '_entity.id != ""'
`,
	}
	for name, body := range files {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}

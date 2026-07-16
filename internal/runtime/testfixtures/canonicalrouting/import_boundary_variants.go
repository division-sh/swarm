package canonicalrouting

import (
	"os"
	"path/filepath"
	"testing"
)

type ImportBoundaryAliasVariant uint8

const (
	ImportBoundaryAliasBindOnly ImportBoundaryAliasVariant = iota + 1
	ImportBoundaryAliasBindOnlyWildcardOutput
	ImportBoundaryAliasConnected
	ImportBoundaryAliasTemplateBindOnly
)

// CopyImportBoundaryAlias creates a typed specialization of the checked-in
// parent-connect artifact for import binding and explicit-connect proofs.
func CopyImportBoundaryAlias(t testing.TB, variant ImportBoundaryAliasVariant) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	removeInheritedScenarios(t, root)
	if err := os.RemoveAll(filepath.Join(root, "flows")); err != nil {
		t.Fatalf("remove inherited canonical flows: %v", err)
	}

	parentSubscription := "parent.lead_enriched"
	connected := false
	mode := "static"
	switch variant {
	case ImportBoundaryAliasBindOnly:
	case ImportBoundaryAliasBindOnlyWildcardOutput:
		parentSubscription = "parent.*"
	case ImportBoundaryAliasConnected:
		connected = true
	case ImportBoundaryAliasTemplateBindOnly:
		mode = "template"
	default:
		t.Fatalf("unsupported import-boundary alias variant %d", variant)
	}

	connect := ""
	rootSchema := "name: import-boundary-alias\n"
	if connected {
		connect = `
connect:
  - from: .lead_captured
    to: worker.work_requested
  - from: worker.work_completed
    to: .lead_enriched
`
		rootSchema = `
name: import-boundary-alias
pins:
  inputs:
    events:
      - name: lead_enriched
        event: parent.lead_enriched
  outputs:
    events:
      - name: lead_captured
        event: parent.lead_captured
`
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: import-boundary-alias
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: worker
    flow: worker
    mode: `+mode+`
    bind:
      inputs:
        work.requested: parent.lead_captured
      outputs:
        work.completed: parent.lead_enriched
`+connect)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), rootSchema)
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), `
parent.lead_captured: {}
parent.lead_enriched: {}
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
parent-listener:
  id: parent-listener
  execution_type: system_node
  subscribes_to: [`+parentSubscription+`]
  event_handlers:
    parent.lead_enriched: {}
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "worker", "package.yaml"), `
name: worker
version: "1.0.0"
requires:
  inputs: [work.requested]
  outputs: [work.completed]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), `
name: worker
mode: `+mode+`
pins:
  inputs:
    events:
      - name: work_requested
        event: work.requested
  outputs:
    events:
      - name: work_completed
        event: work.completed
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "worker", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "worker", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "worker", "events.yaml"), "work.completed: {}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "worker", "nodes.yaml"), `
worker-node:
  id: worker-node
  execution_type: system_node
  subscribes_to: [work.requested]
  produces: [work.completed]
  event_handlers:
    work.requested:
      emit: work.completed
`)
	return root
}

type ImportBoundaryWildcardVariant uint8

const (
	ImportBoundaryWildcardDenied ImportBoundaryWildcardVariant = iota + 1
	ImportBoundaryWildcardObserveGranted
)

// CopyImportBoundaryWildcard creates the closed imported-package wildcard
// variants used to prove denied raw matching and typed observe grants.
func CopyImportBoundaryWildcard(t testing.TB, variant ImportBoundaryWildcardVariant) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	removeInheritedScenarios(t, root)
	if err := os.RemoveAll(filepath.Join(root, "flows")); err != nil {
		t.Fatalf("remove inherited canonical flows: %v", err)
	}

	workerBind := ""
	switch variant {
	case ImportBoundaryWildcardDenied:
	case ImportBoundaryWildcardObserveGranted:
		workerBind = `    bind:
      observe:
        - source: producer
          events: [task.done]
`
	default:
		t.Fatalf("unsupported import-boundary wildcard variant %d", variant)
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: import-boundary-wildcard
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: worker
    flow: worker
    mode: static
`+workerBind+`  - id: producer
    flow: producer
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: import-boundary-wildcard\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeImportBoundaryWildcardFlow(t, root, "worker", true)
	writeImportBoundaryWildcardFlow(t, root, "producer", false)
	return root
}

func writeImportBoundaryWildcardFlow(t testing.TB, root, flowID string, listener bool) {
	t.Helper()
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "package.yaml"), "name: "+flowID+"\nversion: \"1.0.0\"\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), `
name: `+flowID+`
mode: static
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  outputs:
    events: [task.done]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), "task.done: {}\n")
	nodes := "{}\n"
	if listener {
		nodes = `
worker-listener:
  id: worker-listener
  execution_type: system_node
  subscribes_to: ["**/task.done"]
  event_handlers:
    "**/task.done":
      clear_gates: [sibling_gate]
`
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "nodes.yaml"), nodes)
}

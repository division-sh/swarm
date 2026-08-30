package canonicalrouting

import (
	"path/filepath"
	"testing"
)

type ImportBoundaryAliasVariant uint8

const (
	ImportBoundaryAliasBindOnly ImportBoundaryAliasVariant = iota + 1
	ImportBoundaryAliasBindOnlyWildcardOutput
	ImportBoundaryAliasConnected
	ImportBoundaryAliasConnectedWithLocalOutputObserver
	ImportBoundaryAliasTemplateBindOnly
)

// CopyImportBoundaryAlias creates a typed specialization of the checked-in
// parent-connect artifact for import binding and explicit-connect proofs.
func CopyImportBoundaryAlias(t testing.TB, variant ImportBoundaryAliasVariant) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	removeInheritedScenarios(t, root)
	parentSubscription := "parent.lead_enriched"
	connected := false
	mode := "static"
	switch variant {
	case ImportBoundaryAliasBindOnly:
	case ImportBoundaryAliasBindOnlyWildcardOutput:
		parentSubscription = "parent.*"
	case ImportBoundaryAliasConnected:
		connected = true
	case ImportBoundaryAliasConnectedWithLocalOutputObserver:
		connected = true
	case ImportBoundaryAliasTemplateBindOnly:
		mode = "template"
	default:
		t.Fatalf("unsupported import-boundary alias variant %d", variant)
	}

	connect := ""
	rootSchema := "name: import-boundary-alias\n"
	rootEvents := "\nparent.lead_captured: {}\nparent.lead_enriched: {}\n"
	if connected {
		connect = `
connect:
  - event: parent.lead_captured
    from: .
    to: worker
    rename: work.requested
  - event: work.completed
    from: worker
    to: .
    rename: parent.lead_enriched
`
		rootSchema = `
name: import-boundary-alias
pins:
  inputs:
    events:
      - parent.lead_enriched
  outputs:
    events:
      - parent.lead_captured
`
		rootEvents = "\nparent.lead_captured: {}\n"
	}

	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), rootSchema+connect)
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), rootEvents)
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
parent-listener:
  id: parent-listener
  execution_type: system_node
  subscribes_to: [`+parentSubscription+`]
  event_handlers:
    parent.lead_enriched: {}
`)

	writeBootverifyFixtureFile(t, filepath.Join(root, "worker", "schema.yaml"), `
name: worker
mode: `+mode+`
pins:
  inputs:
    events:
      - work.requested
  outputs:
    events:
      - work.completed
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "worker", "events.yaml"), "work.completed: {}\n")
	workerNodes := `
worker-node:
  id: worker-node
  execution_type: system_node
  subscribes_to: [work.requested]
  produces: [work.completed]
  event_handlers:
    work.requested:
      emit: work.completed
`
	if variant == ImportBoundaryAliasConnectedWithLocalOutputObserver {
		workerNodes += `
worker-output-observer:
  id: worker-output-observer
  execution_type: system_node
  subscribes_to: [work.completed]
  event_handlers:
    work.completed: {}
`
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "worker", "nodes.yaml"), workerNodes)
	return root
}

// CopyImportBoundaryWildcard creates the closed imported-package wildcard
// variants used to prove denied raw matching and typed observe grants.

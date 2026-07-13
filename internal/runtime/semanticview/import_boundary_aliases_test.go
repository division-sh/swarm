package semanticview

import (
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	canonicalrouting "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestImportBoundaryPinAliasesResolveInputAndOutputBindings(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/semanticview/import_boundary_aliases_test.go:writeImportBoundaryAliasFixture"))
	source := loadImportBoundaryAliasFixture(t, importBoundaryAliasFixtureOptions{})

	resolution := source.ResolveFlowInputAutoWire("worker", "work.requested")
	if got, want := resolution.Patterns, []string{"parent.lead_captured"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("ResolveFlowInputAutoWire patterns = %#v, want %#v", got, want)
	}
	proof := runtimecontracts.FlowInputProducerResolution{Evidence: resolution.Evidence}
	if !proof.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryParentConnect) {
		t.Fatalf("ResolveFlowInputAutoWire evidence = %#v, want parent connect", resolution.Evidence)
	}
	if !ImportBoundaryInputAliasRequired(source, "worker", "work.requested") {
		t.Fatal("expected work.requested to require import-boundary input alias")
	}
	inputs := ImportBoundaryInputAliasesForParentEvent(source, "worker", "parent.lead_captured")
	if len(inputs) != 1 {
		t.Fatalf("input aliases for parent event = %#v, want one", inputs)
	}
	if got, want := inputs[0].Pin, "work.requested"; got != want {
		t.Fatalf("input alias pin = %q, want %q", got, want)
	}

	outputs := ImportBoundaryOutputAliasesForParentEvent(source, ".", "", "parent.lead_enriched")
	if len(outputs) != 1 {
		t.Fatalf("output aliases = %#v, want one", outputs)
	}
	if got, want := outputs[0].EventPattern, "worker/work.completed"; got != want {
		t.Fatalf("output event pattern = %q, want %q", got, want)
	}
	scopedOutputs := ImportBoundaryOutputAliasesForParent(source, ".", "")
	if len(scopedOutputs) != 1 {
		t.Fatalf("scoped output aliases = %#v, want one", scopedOutputs)
	}

	parentEvents := ImportBoundaryOutputParentEventsForEvent(source, ".", "", "worker/work.completed")
	if got, want := parentEvents, []string{"parent.lead_enriched"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("output parent events = %#v, want %#v", got, want)
	}
}

func TestImportBoundaryInputAliasRequiredDoesNotRawFallbackToSameNameProducer(t *testing.T) {
	source := loadImportBoundaryAliasFixture(t, importBoundaryAliasFixtureOptions{
		producerOutput: "work.requested",
	})

	resolution := source.ResolveFlowInputAutoWire("worker", "work.requested")
	if got, want := resolution.Patterns, []string{"parent.lead_captured"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("ResolveFlowInputAutoWire patterns = %#v, want only explicit bind %#v", got, want)
	}
}

func TestImportBoundaryPinAliasIssuesReportUnknownAndAmbiguousParentEvents(t *testing.T) {
	t.Run("unknown parent event", func(t *testing.T) {
		source := loadImportBoundaryAliasFixture(t, importBoundaryAliasFixtureOptions{
			inputBind: "parent.missing_event",
		})
		issues := ImportBoundaryPinAliasIssues(source)
		if !importBoundaryAliasIssueContains(issues, "unknown_parent_event", "parent.missing_event") {
			t.Fatalf("issues = %#v, want unknown parent event", issues)
		}
	})

	t.Run("ambiguous parent event", func(t *testing.T) {
		source := loadImportBoundaryAliasFixture(t, importBoundaryAliasFixtureOptions{
			inputBind:        "shared.ready",
			producerOutput:   "shared.ready",
			secondProducerID: "producer_b",
		})
		issues := ImportBoundaryPinAliasIssues(source)
		if !importBoundaryAliasIssueContains(issues, "ambiguous_parent_event", "shared.ready") {
			t.Fatalf("issues = %#v, want ambiguous parent event", issues)
		}
	})
}

func importBoundaryAliasIssueContains(issues []ImportBoundaryPinAliasIssue, kind, parentEvent string) bool {
	for _, issue := range issues {
		if issue.Kind == kind && issue.ParentEvent == parentEvent {
			return true
		}
	}
	return false
}

type importBoundaryAliasFixtureOptions struct {
	inputBind        string
	outputBind       string
	producerOutput   string
	secondProducerID string
}

func loadImportBoundaryAliasFixture(t *testing.T, opts importBoundaryAliasFixtureOptions) Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeImportBoundaryAliasFixture(t, opts)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return Wrap(bundle)
}

func writeImportBoundaryAliasFixture(t *testing.T, opts importBoundaryAliasFixtureOptions) string {
	t.Helper()
	if opts.inputBind == "" {
		opts.inputBind = "parent.lead_captured"
	}
	if opts.outputBind == "" {
		opts.outputBind = "parent.lead_enriched"
	}
	root := t.TempDir()
	flows := `  - id: worker
    flow: worker
    mode: static
    bind:
      inputs:
        work.requested: ` + opts.inputBind + `
      outputs:
        work.completed: ` + opts.outputBind + `
`
	if opts.producerOutput != "" {
		flows = `  - id: producer
    flow: producer
    mode: static
` + flows
	}
	if opts.secondProducerID != "" {
		flows = `  - id: ` + opts.secondProducerID + `
    flow: ` + opts.secondProducerID + `
    mode: static
` + flows
	}
	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: import-boundary-alias
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
`+flows)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: import-boundary-alias\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "events.yaml"), `
parent.lead_captured: {}
parent.lead_enriched: {}
`)
	writeImportBoundaryAliasFlow(t, root, "worker", `
pins:
  inputs:
    events: [work.requested]
  outputs:
    events: [work.completed]
`, "")
	if opts.producerOutput != "" {
		writeImportBoundaryAliasFlow(t, root, "producer", `
pins:
  outputs:
    events: [`+opts.producerOutput+`]
`, opts.producerOutput)
	}
	if opts.secondProducerID != "" {
		writeImportBoundaryAliasFlow(t, root, opts.secondProducerID, `
pins:
  outputs:
    events: [`+opts.producerOutput+`]
`, opts.producerOutput)
	}
	return root
}

func writeImportBoundaryAliasFlow(t *testing.T, root, flowID, schemaTail, outputEvent string) {
	t.Helper()
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", flowID, "package.yaml"), `
name: `+flowID+`
version: "1.0.0"
requires:
  inputs: []
  outputs: []
`)
	if flowID == "worker" {
		writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", flowID, "package.yaml"), `
name: worker
version: "1.0.0"
requires:
  inputs: [work.requested]
  outputs: [work.completed]
`)
	}
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), `
name: `+flowID+`
mode: static
`+schemaTail)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), "{}\n")
	if outputEvent == "" {
		writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), "{}\n")
		return
	}
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), outputEvent+": {}\n")
}

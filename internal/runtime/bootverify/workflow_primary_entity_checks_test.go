package bootverify

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRun_UsesSingleEntityAsPrimaryForStatefulNormalFlow(t *testing.T) {
	bundle := loadPrimaryEntityFixtureBundle(t, `
name: scoring
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: []
  outputs:
    events: []
`, `
vertical:
  name: text
`)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "primary_entity_validation", "") {
		t.Fatalf("unexpected primary_entity_validation error: %#v", report.Errors())
	}
}

func TestRun_RejectsMissingPrimaryEntityForStatefulNormalFlow(t *testing.T) {
	bundle := loadPrimaryEntityFixtureBundle(t, `
name: scoring
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: []
  outputs:
    events: []
`, "")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "primary_entity_validation", "has no declared entity types") {
		t.Fatalf("expected missing primary_entity_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsStatelessFlowWithoutPrimaryEntity(t *testing.T) {
	bundle := loadPrimaryEntityFixtureBundle(t, `
name: scoring
pins:
  inputs:
    events: []
  outputs:
    events: []
`, "")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "primary_entity_validation", "") {
		t.Fatalf("unexpected primary_entity_validation error: %#v", report.Errors())
	}
}

func loadPrimaryEntityFixtureBundle(t *testing.T, flowSchema, flowEntities string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := repoRootForBootverifyTest(t)
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: primary-entity-fixture
version: "1.0.0"
platform_version: ">=1.0.0"
flows:
  - id: scoring
    flow: scoring
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: primary-entity-fixture\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "scoring", "schema.yaml"), strings.TrimSpace(flowSchema)+"\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "scoring", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "scoring", "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "scoring", "nodes.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "scoring", "agents.yaml"), "{}\n")
	if strings.TrimSpace(flowEntities) != "" {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "scoring", "entities.yaml"), strings.TrimSpace(flowEntities)+"\n")
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

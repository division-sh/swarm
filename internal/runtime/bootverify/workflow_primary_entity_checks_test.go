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
	bundle := loadPrimaryEntityFixtureBundle(t, `name: scoring
stages:
  pending: {initial: true}
  done: {terminal: true}
`, `
vertical:
  name: text
`)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "primary_entity_validation", "") {
		t.Fatalf("unexpected primary_entity_validation error: %#v", report.Errors())
	}
}

func TestRun_AllowsFieldlessStagedStaticFlow(t *testing.T) {
	bundle := loadPrimaryEntityFixtureBundle(t, `name: scoring
stages:
  pending: {initial: true}
  done: {terminal: true}
`, "")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "primary_entity_validation", "") {
		t.Fatalf("fieldless stages must not manufacture entity demand: %#v", report.Errors())
	}
}

func TestRun_AllowsFieldlessExpandedStages(t *testing.T) {
	bundle := loadPrimaryEntityFixtureBundle(t, `
name: scoring
stages:
  pending:
    initial: true
  done:
    terminal: true
`, "")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "primary_entity_validation", "") {
		t.Fatalf("fieldless stages must not manufacture entity demand: %#v", report.Errors())
	}
}

func TestRun_AllowsStatelessFlowWithoutPrimaryEntity(t *testing.T) {
	bundle := loadPrimaryEntityFixtureBundle(t, `
name: scoring
`, "")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "primary_entity_validation", "") {
		t.Fatalf("unexpected primary_entity_validation error: %#v", report.Errors())
	}
}

func TestRun_RequiresPrimaryEntityForActualOperation(t *testing.T) {
	for _, handler := range []string{"create_entity: true", "advances_to: done", "data_accumulation: {writes: [{target_field: entity.count, value: '${payload.count}'}]}"} {
		t.Run(handler, func(t *testing.T) {
			root := t.TempDir()
			writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: operation-demand\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "scoring", "schema.yaml"), "name: scoring\nstages:\n  pending: {initial: true}\n  done: {terminal: true}\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "scoring", "events.yaml"), "work.ready:\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "scoring", "nodes.yaml"), "worker:\n  execution_type: system_node\n  event_handlers:\n    work.ready:\n      "+handler+"\n")
			bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
			if !reportContains(report.Errors(), "primary_entity_validation", "no declared entity") {
				t.Fatalf("actual operation without primary entity admitted: %#v", report.Errors())
			}
		})
	}
}

func TestRun_RejectsInvalidRootPrimaryEntityForConstructedBundle(t *testing.T) {
	t.Run("root schema entity selector rejected at source", func(t *testing.T) {
		repo := repoRootForBootverifyTest(t)
		root := t.TempDir()
		writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: root-primary-entity\nentity: vertical\n")
		_, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
		if err == nil || !strings.Contains(err.Error(), "RETIRED") {
			t.Fatalf("root entity selector error = %v", err)
		}
	})
	tests := []struct {
		name      string
		bundle    *runtimecontracts.WorkflowContractBundle
		wantError string
	}{
		{
			name: "multiple root entities",
			bundle: &runtimecontracts.WorkflowContractBundle{
				RootSchema: &runtimecontracts.FlowSchemaDocument{Name: "root-primary-entity"},
				RootEntities: runtimecontracts.EntityContractsDocument{
					"campaign": {Fields: map[string]runtimecontracts.EntityFieldDecl{"title": {Type: "text"}}},
					"vertical": {Fields: map[string]runtimecontracts.EntityFieldDecl{"name": {Type: "text"}}},
				},
				FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{},
			},
			wantError: "exactly one entity type",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := Run(context.Background(), semanticview.Wrap(tc.bundle), Options{})
			if !reportContains(report.Errors(), "primary_entity_validation", "flow <root>") ||
				!reportContains(report.Errors(), "primary_entity_validation", tc.wantError) {
				t.Fatalf("expected root primary_entity_validation containing %q, got %#v", tc.wantError, report.Errors())
			}
		})
	}
}

func loadPrimaryEntityFixtureBundle(t *testing.T, flowSchema, flowEntities string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle, err := admitPrimaryEntityFixtureBundle(t, flowSchema, flowEntities)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func admitPrimaryEntityFixtureBundle(t *testing.T, flowSchema, flowEntities string) (*runtimecontracts.WorkflowContractBundle, error) {
	t.Helper()
	repoRoot := repoRootForBootverifyTest(t)
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: primary-entity-fixture\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "scoring", "schema.yaml"), strings.TrimSpace(flowSchema)+"\n")
	if strings.TrimSpace(flowEntities) != "" {
		writeBootverifyFixtureFile(t, filepath.Join(root, "scoring", "entities.yaml"), strings.TrimSpace(flowEntities)+"\n")
	}
	return runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
}

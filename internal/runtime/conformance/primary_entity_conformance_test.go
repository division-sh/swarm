package conformance

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	canonicalrouting "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestPrimaryEntityConformance(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/conformance/primary_entity_conformance_test.go:file-scope"))
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/conformance/primary_entity_conformance_test.go:primaryEntityConformanceSchema"))
	tests := []struct {
		name          string
		flowSchema    string
		flowEntities  string
		wantEntity    string
		wantLoadError string
		wantBootError string
	}{
		{
			name:         "single entity is primary",
			flowSchema:   primaryEntityConformanceSchema(""),
			flowEntities: "vertical:\n  name: text\n",
			wantEntity:   "vertical",
		},
		{
			name:          "schema entity selector fails closed",
			flowSchema:    primaryEntityConformanceSchema("entity: vertical\n"),
			flowEntities:  "vertical:\n  name: text\n",
			wantLoadError: "schema.yaml entity",
		},
		{
			name:          "multi entity normal flow fails closed",
			flowSchema:    primaryEntityConformanceSchema(""),
			flowEntities:  "vertical:\n  name: text\ncampaign:\n  title: text\n",
			wantLoadError: "exactly one entity type",
		},
		{
			name:          "schema entity selector for missing entity fails closed",
			flowSchema:    primaryEntityConformanceSchema("entity: missing\n"),
			flowEntities:  "vertical:\n  name: text\n",
			wantLoadError: "schema.yaml entity",
		},
		{
			name:          "stateful normal flow without entity fails verify",
			flowSchema:    primaryEntityConformanceSchema(""),
			wantBootError: "has no declared entity types",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writePrimaryEntityConformanceFixture(t, tc.flowSchema, tc.flowEntities)
			repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
			if tc.wantLoadError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantLoadError) {
					t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want %q", err, tc.wantLoadError)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			source := semanticview.Wrap(bundle)
			report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
			if tc.wantBootError != "" {
				if !primaryEntityConformanceReportContains(report.Errors(), "primary_entity_validation", tc.wantBootError) {
					t.Fatalf("bootverify errors = %#v, want primary_entity_validation containing %q", report.Errors(), tc.wantBootError)
				}
				return
			}
			if primaryEntityConformanceReportContains(report.Errors(), "primary_entity_validation", "") {
				t.Fatalf("unexpected primary_entity_validation error: %#v", report.Errors())
			}
			resolved, err := bundle.ResolveFlowPrimaryEntity("scoring")
			if err != nil {
				t.Fatalf("ResolveFlowPrimaryEntity: %v", err)
			}
			if resolved.EntityType != tc.wantEntity {
				t.Fatalf("primary entity = type:%q, want type:%q", resolved.EntityType, tc.wantEntity)
			}
		})
	}
}

func primaryEntityConformanceSchema(extra string) string {
	return "name: scoring\n" + extra + `initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: []
  outputs:
    events: []
`
}

func writePrimaryEntityConformanceFixture(t *testing.T, flowSchema, flowEntities string) string {
	t.Helper()
	root := t.TempDir()
	writePrimaryEntityConformanceFile(t, filepath.Join(root, "package.yaml"), `
name: primary-entity-conformance
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: scoring
    flow: scoring
`)
	writePrimaryEntityConformanceFile(t, filepath.Join(root, "schema.yaml"), "name: primary-entity-conformance\n")
	writePrimaryEntityConformanceFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writePrimaryEntityConformanceFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writePrimaryEntityConformanceFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writePrimaryEntityConformanceFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writePrimaryEntityConformanceFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writePrimaryEntityConformanceFile(t, filepath.Join(root, "flows", "scoring", "schema.yaml"), strings.TrimSpace(flowSchema)+"\n")
	writePrimaryEntityConformanceFile(t, filepath.Join(root, "flows", "scoring", "policy.yaml"), "{}\n")
	writePrimaryEntityConformanceFile(t, filepath.Join(root, "flows", "scoring", "events.yaml"), "{}\n")
	writePrimaryEntityConformanceFile(t, filepath.Join(root, "flows", "scoring", "nodes.yaml"), "{}\n")
	writePrimaryEntityConformanceFile(t, filepath.Join(root, "flows", "scoring", "agents.yaml"), "{}\n")
	if strings.TrimSpace(flowEntities) != "" {
		writePrimaryEntityConformanceFile(t, filepath.Join(root, "flows", "scoring", "entities.yaml"), strings.TrimSpace(flowEntities)+"\n")
	}
	return root
}

func writePrimaryEntityConformanceFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func primaryEntityConformanceReportContains(findings []runtimebootverify.Finding, checkID, substr string) bool {
	for _, finding := range findings {
		if finding.CheckID != checkID {
			continue
		}
		if substr == "" || strings.Contains(finding.Message, substr) {
			return true
		}
	}
	return false
}

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

func TestStaticMultiEntityRetirementConformance(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/conformance/static_multi_entity_retirement_conformance_test.go:file-scope"))
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/conformance/static_multi_entity_retirement_conformance_test.go:TestRootDefaultStaticMultiEntityRetirementConformance"), canonicalrouting.SourceID("internal/runtime/conformance/static_multi_entity_retirement_conformance_test.go:loadStaticMultiEntityRetirementSource"))
	tests := []struct {
		name            string
		handlerBody     string
		declareEntityID bool
		checkID         string
		wantMessage     string
	}{
		{
			name: "create_entity fails closed",
			handlerBody: `      create_entity: true
      data_accumulation:
        writes:
          - source_field: amount_usd
            target_field: spent_usd
`,
			checkID:     "flow_boundary_create_entity_validation",
			wantMessage: "static multi-row entity ownership is retired",
		},
		{
			name: "select_entity fails closed",
			handlerBody: `      select_entity:
        by:
          vertical_id: payload.vertical_id
      data_accumulation:
        writes:
          - source_field: amount_usd
            target_field: spent_usd
`,
			checkID:     "select_entity_validation",
			wantMessage: "static multi-row entity ownership is retired",
		},
		{
			name: "select_or_create_entity fails closed",
			handlerBody: `      select_or_create_entity:
        by:
          vertical_id: payload.vertical_id
      data_accumulation:
        writes:
          - source_field: amount_usd
            target_field: spent_usd
`,
			checkID:     "select_entity_validation",
			wantMessage: "static multi-row entity ownership is retired",
		},
		{
			name: "missing acquisition materializing state fails closed",
			handlerBody: "      data_accumulation:\n" +
				"        writes:\n" +
				"          - source_field: amount_usd\n" +
				"            target_field: spent_usd\n",
			checkID:     "flow_boundary_create_entity_validation",
			wantMessage: "static multi-row entity ownership is retired",
		},
		{
			name: "missing acquisition non materializing handler is allowed",
			handlerBody: "      emit:\n" +
				"        event: opco.spend_recorded\n" +
				"        fields:\n" +
				"          vertical_id: payload.vertical_id\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := loadStaticMultiEntityRetirementSource(t, tc.handlerBody)
			report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
			if tc.checkID != "" {
				if !staticMultiEntityRetirementFindingContains(report.Errors(), tc.checkID, tc.wantMessage) {
					t.Fatalf("bootverify errors = %#v, want %s containing %q", report.Errors(), tc.checkID, tc.wantMessage)
				}
				return
			}
			if staticMultiEntityRetirementFindingContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity") ||
				staticMultiEntityRetirementFindingContains(report.Errors(), "missing_external_select_entity", "") {
				t.Fatalf("static handler without acquisition must not be forced into retired acquisition, got %#v", report.Errors())
			}
		})
	}
}

func TestRootDefaultStaticMultiEntityRetirementConformance(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/conformance/static_multi_entity_retirement_conformance_test.go:TestRootDefaultStaticMultiEntityRetirementConformance"), canonicalrouting.SourceID("internal/runtime/conformance/static_multi_entity_retirement_conformance_test.go:loadRootDefaultStaticMultiEntityRetirementSource"))
	tests := []struct {
		name            string
		handlerBody     string
		declareEntityID bool
		requireEntityID bool
		checkID         string
		wantMessage     string
	}{
		{
			name: "missing acquisition materializing root state writes canonical primary entity",
			handlerBody: "      data_accumulation:\n" +
				"        writes:\n" +
				"          - source_field: display_name\n" +
				"            target_field: display_name\n",
		},
		{
			name: "missing acquisition non materializing root handler is allowed",
			handlerBody: "      emit:\n" +
				"        event: subject.observed\n" +
				"        fields:\n" +
				"          display_name: payload.display_name\n",
		},
		{
			name: "optional entity_id root materializer fails closed",
			handlerBody: "      data_accumulation:\n" +
				"        writes:\n" +
				"          - source_field: display_name\n" +
				"            target_field: display_name\n",
			declareEntityID: true,
			checkID:         "flow_boundary_create_entity_validation",
			wantMessage:     "caller-selected entity_id",
		},
		{
			name: "required entity_id root materializer fails closed",
			handlerBody: "      data_accumulation:\n" +
				"        writes:\n" +
				"          - source_field: display_name\n" +
				"            target_field: display_name\n",
			declareEntityID: true,
			requireEntityID: true,
			checkID:         "flow_boundary_create_entity_validation",
			wantMessage:     "caller-selected entity_id",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := loadRootDefaultStaticMultiEntityRetirementSource(t, tc.handlerBody, tc.declareEntityID, tc.requireEntityID)
			report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
			if tc.checkID != "" {
				if !staticMultiEntityRetirementFindingContains(report.Errors(), tc.checkID, tc.wantMessage) {
					t.Fatalf("bootverify errors = %#v, want %s containing root/default-static implicit materialization retirement", report.Errors(), tc.checkID)
				}
				return
			}
			if staticMultiEntityRetirementFindingContains(report.Errors(), "flow_boundary_create_entity_validation", "implicit entity materialization") ||
				staticMultiEntityRetirementFindingContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity") ||
				staticMultiEntityRetirementFindingContains(report.Errors(), "missing_external_select_entity", "") {
				t.Fatalf("root/default-static non-materializing handler must not be forced into retired acquisition, got %#v", report.Errors())
			}
		})
	}
}

func loadStaticMultiEntityRetirementSource(t *testing.T, handlerBody string) semanticview.Source {
	// routing-example-census: different-concept issue=1738 owner=legacy_static_multi_entity_retirement proof=internal/runtime/conformance/static_multi_entity_retirement_conformance_test.go:TestStaticMultiEntityRetirementConformance
	t.Helper()
	root := t.TempDir()
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "package.yaml"), `
name: static-multi-entity-retirement
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: treasury
    flow: treasury
    mode: static
`)
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "schema.yaml"), "name: static-multi-entity-retirement\n")
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "flows", "treasury", "schema.yaml"), `
name: treasury
mode: static
initial_state: active
states: [active, archived]
terminal_states: [archived]
pins:
  inputs:
    events: [opco.spend_requested]
  outputs:
    events: []
`)
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "flows", "treasury", "events.yaml"), `
opco.spend_requested:
  swarm:
    source: external (operator webhook)
  vertical_id: string
  amount_usd: number
opco.spend_recorded:
  vertical_id: string
`)
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "flows", "treasury", "entities.yaml"), `
budget:
  vertical_id:
    type: string
    indexed: true
    _unused_reason: static multi-entity retirement selection key proof field
  spent_usd:
    type: number
    initial: 0
`)
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "flows", "treasury", "policy.yaml"), "{}\n")
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "flows", "treasury", "agents.yaml"), "{}\n")
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "flows", "treasury", "nodes.yaml"), `
treasury-node:
  id: treasury-node
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
`+handlerBody)

	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func loadRootDefaultStaticMultiEntityRetirementSource(t *testing.T, handlerBody string, declareEntityID bool, requireEntityID bool) semanticview.Source {
	// routing-example-census: different-concept issue=1738 owner=legacy_static_multi_entity_retirement proof=internal/runtime/conformance/static_multi_entity_retirement_conformance_test.go:TestRootDefaultStaticMultiEntityRetirementConformance
	t.Helper()
	root := t.TempDir()
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "package.yaml"), `
name: root-default-static-multi-entity-retirement
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "schema.yaml"), `
name: root-default-static-multi-entity-retirement
initial_state: active
states: [active, archived]
terminal_states: [archived]
pins:
  inputs:
    events: [subject.created]
  outputs:
    events: [subject.observed]
`)
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	entityIDField := ""
	if declareEntityID {
		entityIDField = "  entity_id: string\n"
	}
	entityIDRequired := ""
	if requireEntityID {
		entityIDField = "  entity_id: string\n"
		entityIDRequired = "  required:\n    - entity_id\n"
	}
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "events.yaml"), `
subject.created:
  swarm:
    source: external
`+entityIDField+`  display_name: string
`+entityIDRequired+`
subject.observed:
`+entityIDField+`  display_name: string
`+entityIDRequired+`
`)
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "entities.yaml"), `
subject:
  display_name: text
`)
	writeStaticMultiEntityRetirementFile(t, filepath.Join(root, "nodes.yaml"), `
root-node:
  id: root-node
  execution_type: system_node
  subscribes_to: [subject.created]
  produces: [subject.observed]
  event_handlers:
    subject.created:
`+handlerBody)

	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writeStaticMultiEntityRetirementFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func staticMultiEntityRetirementFindingContains(findings []runtimebootverify.Finding, checkID, substr string) bool {
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

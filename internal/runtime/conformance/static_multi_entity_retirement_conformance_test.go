package conformance

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestStaticMultiEntityRetirementConformance(t *testing.T) {
	tests := []struct {
		name            string
		handler         canonicalrouting.StaticRetirementHandler
		declareEntityID bool
		checkID         string
		wantMessage     string
	}{
		{
			name:        "create_entity fails closed",
			handler:     canonicalrouting.StaticRetirementCreate,
			checkID:     "flow_boundary_create_entity_validation",
			wantMessage: "static multi-row entity ownership is retired",
		},
		{
			name:        "select_entity fails closed",
			handler:     canonicalrouting.StaticRetirementSelect,
			checkID:     "select_entity_validation",
			wantMessage: "static multi-row entity ownership is retired",
		},
		{
			name:        "select_or_create_entity fails closed",
			handler:     canonicalrouting.StaticRetirementSelectOrCreate,
			checkID:     "select_entity_validation",
			wantMessage: "static multi-row entity ownership is retired",
		},
		{
			name:        "missing acquisition materializing state fails closed",
			handler:     canonicalrouting.StaticRetirementMaterialize,
			checkID:     "flow_boundary_create_entity_validation",
			wantMessage: "static multi-row entity ownership is retired",
		},
		{
			name:    "missing acquisition non materializing handler is allowed",
			handler: canonicalrouting.StaticRetirementObserve,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := loadCanonicalRoutingSource(t, canonicalrouting.CopyStaticMultiEntityRetirement(t, tc.handler))
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
	tests := []struct {
		name        string
		handler     canonicalrouting.RootStaticHandler
		entityID    canonicalrouting.RootStaticEntityID
		checkID     string
		wantMessage string
	}{
		{
			name:    "missing acquisition materializing root state writes canonical primary entity",
			handler: canonicalrouting.RootStaticMaterialize,
		},
		{
			name:    "missing acquisition non materializing root handler is allowed",
			handler: canonicalrouting.RootStaticObserve,
		},
		{
			name:        "optional entity_id root materializer fails closed",
			handler:     canonicalrouting.RootStaticMaterialize,
			entityID:    canonicalrouting.RootStaticOptionalEntityID,
			checkID:     "flow_boundary_create_entity_validation",
			wantMessage: "caller-selected entity_id",
		},
		{
			name:        "required entity_id root materializer fails closed",
			handler:     canonicalrouting.RootStaticMaterialize,
			entityID:    canonicalrouting.RootStaticRequiredEntityID,
			checkID:     "flow_boundary_create_entity_validation",
			wantMessage: "caller-selected entity_id",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := loadCanonicalRoutingSource(t, canonicalrouting.CopyRootDefaultStaticInput(t, tc.handler, tc.entityID))
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

func loadCanonicalRoutingSource(t *testing.T, root string) semanticview.Source {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		root,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
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

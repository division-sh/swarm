package bootverify

import (
	"context"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRun_ReportsCrossSurfaceExactDuplicateShapesAsLintEvidence(t *testing.T) {
	bundle := crossSurfaceNamedTypeUseBundle()

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	lint := report.LintEvidence()

	if !reportContains(lint, crossSurfaceNamedTypeUseCheckID, "identical shape {summary:text, supporting_routes:[text]}") {
		t.Fatalf("expected exact duplicate shape lint evidence, got %#v", lint)
	}
	if !reportContains(lint, crossSurfaceNamedTypeUseCheckID, "type root.ProposalEvidence") {
		t.Fatalf("expected type catalog surface in lint evidence, got %#v", lint)
	}
	if !reportContains(lint, crossSurfaceNamedTypeUseCheckID, "event proposal.created payload") {
		t.Fatalf("expected event payload surface in lint evidence, got %#v", lint)
	}
	if !reportContains(lint, crossSurfaceNamedTypeUseCheckID, "entity root.proposal") {
		t.Fatalf("expected entity contract surface in lint evidence, got %#v", lint)
	}
	if !reportContains(lint, crossSurfaceNamedTypeUseCheckID, "policy root permission_bundles.coordinator") {
		t.Fatalf("expected policy object surface in lint evidence, got %#v", lint)
	}
	if reportContains(report.HardInvalidities(), crossSurfaceNamedTypeUseCheckID, "") {
		t.Fatalf("cross-surface named-type reuse must remain advisory-only, got hard invalidity %#v", report.HardInvalidities())
	}
}

func TestRun_ReportsConservativeNearDuplicateShapeAsLintEvidence(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"Brand": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"name":        {Type: "text"},
						"url":         {Type: "text"},
						"description": {Type: "text"},
						"audience":    {Type: "text"},
					},
				},
				"BrandCandidate": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"name":        {Type: "text"},
						"url":         {Type: "text"},
						"description": {Type: "text"},
						"audience":    {Type: "text"},
						"confidence":  {Type: "numeric"},
					},
				},
			},
		},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	lint := report.LintEvidence()

	if !reportContains(lint, crossSurfaceNamedTypeUseCheckID, "near-duplicate shapes share 4/5 field/type pairs") {
		t.Fatalf("expected conservative near-duplicate lint evidence, got %#v", lint)
	}
	if reportContains(report.HardInvalidities(), crossSurfaceNamedTypeUseCheckID, "") {
		t.Fatalf("near-duplicate detection must remain advisory-only, got hard invalidity %#v", report.HardInvalidities())
	}
}

func TestRun_DoesNotReportLowConfidenceSmallNearDuplicateShape(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"SmallA": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"name": {Type: "text"},
						"url":  {Type: "text"},
						"kind": {Type: "text"},
					},
				},
				"SmallB": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"name":  {Type: "text"},
						"url":   {Type: "text"},
						"score": {Type: "numeric"},
					},
				},
			},
		},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.LintEvidence(), crossSurfaceNamedTypeUseCheckID, "near-duplicate") {
		t.Fatalf("expected low-confidence small overlap to be ignored, got %#v", report.LintEvidence())
	}
}

func crossSurfaceNamedTypeUseBundle() *runtimecontracts.WorkflowContractBundle {
	duplicateFields := map[string]runtimecontracts.TypeFieldSpec{
		"summary":           {Type: "text"},
		"supporting_routes": {Type: "[text]"},
	}
	return &runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"ProposalEvidence": {Fields: duplicateFields},
				"ResearchContext":  {Fields: duplicateFields},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"proposal.created": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"summary":           {Type: "text"},
						"supporting_routes": {Type: "[text]"},
					},
				},
			},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"proposal": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"summary":           {Type: "text", Initial: ""},
					"supporting_routes": {Type: "[text]", Initial: []any{}},
				},
			},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"permission_bundles": {
				Value: map[string]any{
					"coordinator": map[string]any{
						"summary":           "Coordinates review",
						"supporting_routes": []any{"review"},
					},
					"reviewer": map[string]any{
						"summary":           "Reviews output",
						"supporting_routes": []any{"review"},
					},
				},
			},
		}},
	}
}

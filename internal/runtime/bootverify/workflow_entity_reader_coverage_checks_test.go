package bootverify

import (
	"context"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func TestRun_ReportsEntityReaderCoverageWithoutUnusedReaderReason(t *testing.T) {
	bundle := entityReaderCoverageBundle(runtimecontracts.EntityFieldDecl{
		Type:    "text",
		Initial: "",
	})

	report := Run(context.Background(), semanticviewtest.WrapRootAgents(bundle), Options{})

	if !reportContains(report.LintEvidence(), "entity_reader_coverage", "flow root entity_type state declares field resolution with no detected internal reader coverage") {
		t.Fatalf("expected reader coverage lint, got %#v", report.LintEvidence())
	}
}

func TestRun_GateContextCountsAsEntityReaderCoverage(t *testing.T) {
	bundle := entityReaderCoverageBundle(runtimecontracts.EntityFieldDecl{
		Type:    "text",
		Initial: "",
	})
	bundle.Semantics.Gates = []runtimecontracts.WorkflowGatePlan{{
		FlowID:   ".",
		Stage:    "review",
		Decision: "review_gate",
		Context: map[string]runtimecontracts.ExpressionValue{
			"resolution": {Kind: runtimecontracts.ExpressionKindCEL, CEL: "state.resolution"},
		},
	}}

	report := Run(context.Background(), semanticviewtest.WrapRootAgents(bundle), Options{})

	if reportContains(report.LintEvidence(), "entity_reader_coverage", "resolution") {
		t.Fatalf("expected gate context read to count as reader coverage, got %#v", report.LintEvidence())
	}
}

func TestRun_GateContextLiteralDoesNotCountAsEntityReaderCoverage(t *testing.T) {
	bundle := entityReaderCoverageBundle(runtimecontracts.EntityFieldDecl{
		Type:    "text",
		Initial: "",
	})
	bundle.Semantics.Gates = []runtimecontracts.WorkflowGatePlan{{
		FlowID:   ".",
		Stage:    "review",
		Decision: "review_gate",
		Context: map[string]runtimecontracts.ExpressionValue{
			"note": {Literal: "no entity reference here"},
		},
	}}

	report := Run(context.Background(), semanticviewtest.WrapRootAgents(bundle), Options{})

	if !reportContains(report.LintEvidence(), "entity_reader_coverage", "flow root entity_type state declares field resolution with no detected internal reader coverage") {
		t.Fatalf("expected reader coverage lint when the gate context is not an entity reference, got %#v", report.LintEvidence())
	}
}

func entityReaderCoverageBundle(field runtimecontracts.EntityFieldDecl) *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{
			"state": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"resolution": field,
				},
			},
		},
	}
}

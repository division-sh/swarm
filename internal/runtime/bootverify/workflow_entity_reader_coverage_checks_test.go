package bootverify

import (
	"context"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func TestRun_SuppressesEntityReaderCoverageWithUnusedReaderReason(t *testing.T) {
	bundle := entityReaderCoverageBundle(runtimecontracts.EntityFieldDecl{
		Type:               "text",
		Initial:            "",
		UnusedReaderReason: "external operator readout",
	})

	report := Run(context.Background(), semanticviewtest.WrapRootAgents(bundle), Options{})

	if reportContains(report.LintEvidence(), "entity_reader_coverage", "resolution") {
		t.Fatalf("expected _unused_reader_reason to suppress reader coverage lint, got %#v", report.LintEvidence())
	}
	if reportContains(report.HardInvalidities(), "entity_writer_coverage", "resolution") {
		t.Fatalf("_unused_reader_reason must not affect writer-covered fields, got %#v", report.HardInvalidities())
	}
}

func TestRun_ReportsEntityReaderCoverageWithoutUnusedReaderReason(t *testing.T) {
	bundle := entityReaderCoverageBundle(runtimecontracts.EntityFieldDecl{
		Type:    "text",
		Initial: "",
	})

	report := Run(context.Background(), semanticviewtest.WrapRootAgents(bundle), Options{})

	if !reportContains(report.LintEvidence(), "entity_reader_coverage", "flow root entity_type ticket declares field resolution with no detected internal reader coverage") {
		t.Fatalf("expected reader coverage lint without _unused_reader_reason, got %#v", report.LintEvidence())
	}
}

func TestRun_UnusedReaderReasonDoesNotSatisfyEntityWriterCoverage(t *testing.T) {
	bundle := entityReaderCoverageBundle(runtimecontracts.EntityFieldDecl{
		Type:               "text",
		UnusedReaderReason: "external operator readout",
	})

	report := Run(context.Background(), semanticviewtest.WrapRootAgents(bundle), Options{})

	if !reportContains(report.HardInvalidities(), "entity_writer_coverage", "without authored writer coverage") {
		t.Fatalf("_unused_reader_reason must not satisfy writer coverage, got %#v", report.HardInvalidities())
	}
	if reportContains(report.LintEvidence(), "entity_reader_coverage", "resolution") {
		t.Fatalf("expected _unused_reader_reason to suppress reader coverage independently, got %#v", report.LintEvidence())
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
			"resolution": {Kind: runtimecontracts.ExpressionKindCEL, CEL: "entity.resolution"},
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

	if !reportContains(report.LintEvidence(), "entity_reader_coverage", "flow root entity_type ticket declares field resolution with no detected internal reader coverage") {
		t.Fatalf("expected reader coverage lint when the gate context is not an entity reference, got %#v", report.LintEvidence())
	}
}

func entityReaderCoverageBundle(field runtimecontracts.EntityFieldDecl) *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{
			"ticket": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"resolution": field,
				},
			},
		},
	}
}

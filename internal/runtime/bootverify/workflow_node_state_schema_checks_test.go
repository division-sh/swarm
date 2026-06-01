package bootverify

import (
	"context"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRun_ReportsNodeStateJSONBWithTypedCounterpart(t *testing.T) {
	for _, fieldType := range []string{"jsonb", "json"} {
		t.Run(fieldType, func(t *testing.T) {
			bundle := nodeStateSchemaTypingBundle()
			bundle.Nodes["scoring-node"] = runtimecontracts.SystemNodeContract{
				ID:         "scoring-node",
				StateTable: "scoring_state",
				StateSchema: runtimecontracts.NodeStateSchema{Fields: []runtimecontracts.NodeStateField{
					{Name: "dimensions_received", Type: fieldType},
				}},
			}

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if !reportContains(report.HardInvalidities(), "node_state_schema_typed_counterpart", "dimensions_received is jsonb but has typed downstream counterpart entity_type vertical scores:[DimensionScore]") {
				t.Fatalf("expected node-state typed-counterpart hard invalidity, got %#v", report.HardInvalidities())
			}
		})
	}
}

func TestRun_ReportsNodeStateJSONBLintEvidenceWithoutCounterpart(t *testing.T) {
	bundle := nodeStateSchemaTypingBundle()
	bundle.Nodes["build-node"] = runtimecontracts.SystemNodeContract{
		ID:         "build-node",
		StateTable: "build_state",
		StateSchema: runtimecontracts.NodeStateSchema{Fields: []runtimecontracts.NodeStateField{
			{Name: "build_evidence", Type: "jsonb"},
		}},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.HardInvalidities(), "node_state_schema_typed_counterpart", "build_evidence") {
		t.Fatalf("unexpected hard invalidity for shape-variant node-state jsonb, got %#v", report.HardInvalidities())
	}
	if !reportContains(report.LintEvidence(), "node_state_schema_typed_counterpart", "build_evidence remains jsonb") {
		t.Fatalf("expected node-state jsonb lint evidence, got %#v", report.LintEvidence())
	}
}

func TestRun_ReportsUndeclaredNodeStateNamedType(t *testing.T) {
	bundle := nodeStateSchemaTypingBundle()
	bundle.Nodes["scoring-node"] = runtimecontracts.SystemNodeContract{
		ID:         "scoring-node",
		StateTable: "scoring_state",
		StateSchema: runtimecontracts.NodeStateSchema{Fields: []runtimecontracts.NodeStateField{
			{Name: "dimensions_received", Type: "[MissingScore]"},
		}},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.HardInvalidities(), "node_state_schema_typed_counterpart", "references undeclared type catalog name MissingScore") {
		t.Fatalf("expected undeclared node-state named type hard invalidity, got %#v", report.HardInvalidities())
	}
}

func TestRun_AllowsDeclaredNodeStateNamedType(t *testing.T) {
	bundle := nodeStateSchemaTypingBundle()
	bundle.Nodes["scoring-node"] = runtimecontracts.SystemNodeContract{
		ID:         "scoring-node",
		StateTable: "scoring_state",
		StateSchema: runtimecontracts.NodeStateSchema{Fields: []runtimecontracts.NodeStateField{
			{Name: "dimensions_received", Type: "[DimensionScore]"},
		}},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.HardInvalidities(), "node_state_schema_typed_counterpart", "dimensions_received") {
		t.Fatalf("unexpected node-state named type hard invalidity, got %#v", report.HardInvalidities())
	}
	if reportContains(report.LintEvidence(), "node_state_schema_typed_counterpart", "dimensions_received") {
		t.Fatalf("unexpected node-state jsonb lint evidence for named type, got %#v", report.LintEvidence())
	}
}

func TestRun_AllowsDeclaredNodeStateScalarAndEnumRefs(t *testing.T) {
	bundle := nodeStateSchemaTypingBundle()
	bundle.RootTypes.Scalars["ScoreID"] = runtimecontracts.ScalarTypeDecl{Base: "text"}
	bundle.RootTypes.Enums["ScoreStatus"] = runtimecontracts.EnumTypeDecl{Values: []string{"ready", "done"}}
	bundle.Nodes["scoring-node"] = runtimecontracts.SystemNodeContract{
		ID:         "scoring-node",
		StateTable: "scoring_state",
		StateSchema: runtimecontracts.NodeStateSchema{Fields: []runtimecontracts.NodeStateField{
			{Name: "score_id", Type: "ScoreID"},
			{Name: "score_status", Type: "ScoreStatus"},
		}},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.HardInvalidities(), "node_state_schema_typed_counterpart", "score_id") {
		t.Fatalf("unexpected node-state scalar ref hard invalidity, got %#v", report.HardInvalidities())
	}
	if reportContains(report.HardInvalidities(), "node_state_schema_typed_counterpart", "score_status") {
		t.Fatalf("unexpected node-state enum ref hard invalidity, got %#v", report.HardInvalidities())
	}
}

func nodeStateSchemaTypingBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{},
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Scalars: map[string]runtimecontracts.ScalarTypeDecl{},
			Enums:   map[string]runtimecontracts.EnumTypeDecl{},
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"DimensionScore": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"name":  {Type: "text"},
						"score": {Type: "numeric"},
					},
				},
			},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"vertical": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"scores": {Type: "[DimensionScore]", Initial: []any{}},
				},
			},
		},
	}
}

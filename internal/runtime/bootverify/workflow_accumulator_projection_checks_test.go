package bootverify

import (
	"context"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

func accumulatorProjectionBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"DimensionScore": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"dimension":  {Type: "text"},
						"tier":       {Type: "integer"},
						"score":      {Type: "integer"},
						"evidence":   {Type: "text"},
						"confidence": {Type: "text"},
					},
				},
				"DimensionVerdict": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"dimension": {Type: "text"},
						"score":     {Type: "integer"},
					},
				},
			},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"vertical": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"scores": {
						Type:            "[DimensionScore]",
						MaterializeFrom: "scoring-node.dimensions_received",
					},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scoring-node": {
				StateSchema: runtimecontracts.NodeStateSchema{
					Fields: []runtimecontracts.NodeStateField{{Name: "dimensions_received", Type: "[DimensionScore]"}},
				},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"score.dimension_complete": {
						Accumulate: &runtimecontracts.AccumulateSpec{Into: "dimensions_received"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"score.dimension_complete": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"vertical_id": {Type: "uuid"},
					"dimension":   {Type: "text"},
					"tier":        {Type: "integer"},
					"score":       {Type: "integer"},
					"evidence":    {Type: "text"},
					"confidence":  {Type: "text"},
				}},
			},
		},
	}
}

func TestRun_AcceptsAccumulatorEntityProjectionWithPayloadExtras(t *testing.T) {
	report := Run(context.Background(), semanticview.Wrap(accumulatorProjectionBundle()), Options{})

	if reportContains(report.HardInvalidities(), "accumulator_entity_projection", "scores") {
		t.Fatalf("unexpected projection invalidity: %#v", report.HardInvalidities())
	}
}

func TestRun_RejectsAccumulatorProjectionWithoutAccumulateInto(t *testing.T) {
	bundle := accumulatorProjectionBundle()
	handler := bundle.Nodes["scoring-node"].EventHandlers["score.dimension_complete"]
	handler.Accumulate.Into = ""
	bundle.Nodes["scoring-node"].EventHandlers["score.dimension_complete"] = handler

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.HardInvalidities(), "accumulator_entity_projection", "does not resolve to an explicitly declared accumulator") {
		t.Fatalf("expected missing accumulate.into invalidity, got %#v", report.HardInvalidities())
	}
}

func TestRun_RejectsAccumulatorProjectionMissingTypedViewField(t *testing.T) {
	bundle := accumulatorProjectionBundle()
	payload := bundle.Events["score.dimension_complete"].Payload
	delete(payload.Properties, "evidence")
	event := bundle.Events["score.dimension_complete"]
	event.Payload = payload
	bundle.Events["score.dimension_complete"] = event

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.HardInvalidities(), "accumulator_entity_projection", "payload missing field \"evidence\"") {
		t.Fatalf("expected missing typed-view field invalidity, got %#v", report.HardInvalidities())
	}
}

func TestRun_RejectsAccumulatorProjectionSourceExtraReference(t *testing.T) {
	bundle := accumulatorProjectionBundle()
	entity := bundle.RootEntities["vertical"]
	entity.Fields["summary"] = runtimecontracts.EntityFieldDecl{
		Type:            "[DimensionVerdict]",
		MaterializeFrom: "scoring-node.dimensions_received",
		Project: map[string]any{
			"dimension": "source.vertical_id",
			"score":     "source.score",
		},
	}
	bundle.RootEntities["vertical"] = entity

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.HardInvalidities(), "accumulator_entity_projection", "vertical_id is not a field of item type DimensionScore") {
		t.Fatalf("expected source extra projection invalidity, got %#v", report.HardInvalidities())
	}
}

func TestRun_RejectsAccumulatorProjectionUnknownPolicyReference(t *testing.T) {
	bundle := accumulatorProjectionBundle()
	entity := bundle.RootEntities["vertical"]
	entity.Fields["summary"] = runtimecontracts.EntityFieldDecl{
		Type:            "[DimensionVerdict]",
		MaterializeFrom: "scoring-node.dimensions_received",
		Project: map[string]any{
			"dimension": "source.dimension",
			"score":     "policy.scoring.default_score",
		},
	}
	bundle.RootEntities["vertical"] = entity

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.HardInvalidities(), "accumulator_entity_projection", "policy field scoring.default_score is not declared") {
		t.Fatalf("expected unknown policy projection invalidity, got %#v", report.HardInvalidities())
	}
}

func TestRun_RejectsAccumulatorProjectionSourceTypeMismatch(t *testing.T) {
	bundle := accumulatorProjectionBundle()
	entity := bundle.RootEntities["vertical"]
	entity.Fields["summary"] = runtimecontracts.EntityFieldDecl{
		Type:            "[DimensionVerdict]",
		MaterializeFrom: "scoring-node.dimensions_received",
		Project: map[string]any{
			"dimension": "source.dimension",
			"score":     "source.evidence",
		},
	}
	bundle.RootEntities["vertical"] = entity

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.HardInvalidities(), "accumulator_entity_projection", "not assignable to target field type integer") {
		t.Fatalf("expected source type mismatch invalidity, got %#v", report.HardInvalidities())
	}
}

func TestRun_RejectsAccumulatorProjectionWithOtherWriter(t *testing.T) {
	bundle := accumulatorProjectionBundle()
	handler := bundle.Nodes["scoring-node"].EventHandlers["score.dimension_complete"]
	handler.DataAccumulation = runtimecontracts.WorkflowDataAccumulation{
		Writes: []runtimecontracts.WorkflowDataWrite{{
			TargetField: "scores",
			Value:       runtimecontracts.LiteralExpression([]any{}),
		}},
	}
	bundle.Nodes["scoring-node"].EventHandlers["score.dimension_complete"] = handler

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.HardInvalidities(), "accumulator_entity_projection", "also has authored writer") {
		t.Fatalf("expected writer conflict invalidity, got %#v", report.HardInvalidities())
	}
}

func TestRun_RejectsDuplicateAccumulateInto(t *testing.T) {
	bundle := accumulatorProjectionBundle()
	node := bundle.Nodes["scoring-node"]
	node.EventHandlers["score.dimension_retry"] = runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{Into: "dimensions_received"},
	}
	bundle.Nodes["scoring-node"] = node

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.HardInvalidities(), "accumulator_entity_projection", "declared by multiple handlers") {
		t.Fatalf("expected duplicate accumulate.into invalidity, got %#v", report.HardInvalidities())
	}
}

func TestRun_RejectsMaterializeFromWithUnusedReason(t *testing.T) {
	bundle := accumulatorProjectionBundle()
	entity := bundle.RootEntities["vertical"]
	field := entity.Fields["scores"]
	field.UnusedReason = "scores are now materialized and this stale reason must be removed"
	entity.Fields["scores"] = field
	bundle.RootEntities["vertical"] = entity

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.HardInvalidities(), "accumulator_entity_projection", "declares both materialize_from and _unused_reason") {
		t.Fatalf("expected _unused_reason invalidity, got %#v", report.HardInvalidities())
	}
}

func TestRun_RejectsProjectWhenTypesMatch(t *testing.T) {
	bundle := accumulatorProjectionBundle()
	entity := bundle.RootEntities["vertical"]
	field := entity.Fields["scores"]
	field.Project = map[string]any{"dimension": "source.dimension"}
	entity.Fields["scores"] = field
	bundle.RootEntities["vertical"] = entity

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.HardInvalidities(), "accumulator_entity_projection", "element types match") {
		t.Fatalf("expected project-forbidden invalidity, got %#v", report.HardInvalidities())
	}
}

func TestRun_RejectsMissingProjectWhenTypesDiffer(t *testing.T) {
	bundle := accumulatorProjectionBundle()
	entity := bundle.RootEntities["vertical"]
	field := entity.Fields["scores"]
	field.Type = "[DimensionVerdict]"
	entity.Fields["scores"] = field
	bundle.RootEntities["vertical"] = entity

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.HardInvalidities(), "accumulator_entity_projection", "project must be present") {
		t.Fatalf("expected project-required invalidity, got %#v", report.HardInvalidities())
	}
}

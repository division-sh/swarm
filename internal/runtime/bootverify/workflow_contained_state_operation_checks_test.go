package bootverify

import (
	"context"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRun_ValidatesTypedContainedStateOperations(t *testing.T) {
	source := semanticview.Wrap(containedStateOperationBundle(runtimecontracts.WorkflowDataWrite{
		Operation: runtimecontracts.WorkflowDataOperationAppend,
		TargetRef: "entity.verticals.active_jobs",
		Key:       runtimecontracts.RefExpression("payload.vertical_id"),
		Value:     runtimecontracts.RefExpression("payload.job"),
	}))

	report := Run(context.Background(), source, Options{})

	if reportContains(report.Errors(), "contained_state_operation_compliance", "") {
		t.Fatalf("unexpected contained_state_operation_compliance error: %#v", report.Errors())
	}
}

func TestRun_FailsClosedOnInvalidContainedStateOperationValue(t *testing.T) {
	source := semanticview.Wrap(containedStateOperationBundle(runtimecontracts.WorkflowDataWrite{
		Operation: runtimecontracts.WorkflowDataOperationSet,
		TargetRef: "entity.verticals",
		Key:       runtimecontracts.LiteralExpression("north"),
		Value: runtimecontracts.LiteralExpression(map[string]any{
			"undeclared": "field",
		}),
	}))

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "contained_state_operation_compliance", "is undeclared") {
		t.Fatalf("expected contained_state_operation_compliance undeclared field error, got %#v", report.Errors())
	}
}

func TestRun_FailsClosedOnDynamicContainedStateOperationTargetPath(t *testing.T) {
	source := semanticview.Wrap(containedStateOperationBundle(runtimecontracts.WorkflowDataWrite{
		Operation: runtimecontracts.WorkflowDataOperationSet,
		TargetRef: "entity.verticals[payload.vertical_id]",
		Key:       runtimecontracts.LiteralExpression("north"),
		Value: runtimecontracts.LiteralExpression(map[string]any{
			"status":      "active",
			"active_jobs": []any{},
		}),
	}))

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "contained_state_operation_compliance", "dynamic bracket path syntax") {
		t.Fatalf("expected contained_state_operation_compliance dynamic path error, got %#v", report.Errors())
	}
}

func TestRun_FailsClosedOnContainedSetOrMergeIndex(t *testing.T) {
	tests := []struct {
		name string
		op   runtimecontracts.WorkflowDataOperation
	}{
		{name: "set", op: runtimecontracts.WorkflowDataOperationSet},
		{name: "merge", op: runtimecontracts.WorkflowDataOperationMerge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := semanticview.Wrap(containedStateOperationBundle(runtimecontracts.WorkflowDataWrite{
				Operation: tc.op,
				TargetRef: "entity.verticals",
				Key:       runtimecontracts.LiteralExpression("north"),
				Index:     runtimecontracts.LiteralExpression(0),
				Value: runtimecontracts.LiteralExpression(map[string]any{
					"status": "active",
				}),
			}))

			report := Run(context.Background(), source, Options{})

			if !reportContains(report.Errors(), "contained_state_operation_compliance", "must not declare index") {
				t.Fatalf("expected contained_state_operation_compliance index rejection, got %#v", report.Errors())
			}
		})
	}
}

func TestRun_FailsClosedOnContainedStateOperationRefOperandsToUnknownEntityField(t *testing.T) {
	tests := []struct {
		name  string
		write runtimecontracts.WorkflowDataWrite
		want  string
	}{
		{
			name: "key ref",
			write: runtimecontracts.WorkflowDataWrite{
				Operation: runtimecontracts.WorkflowDataOperationSet,
				TargetRef: "entity.verticals",
				Key:       runtimecontracts.RefExpression("entity.bad_key"),
				Value: runtimecontracts.LiteralExpression(map[string]any{
					"status":      "active",
					"active_jobs": []any{},
				}),
			},
			want: "entity.bad_key",
		},
		{
			name: "value ref",
			write: runtimecontracts.WorkflowDataWrite{
				Operation: runtimecontracts.WorkflowDataOperationAppend,
				TargetRef: "entity.verticals.active_jobs",
				Key:       runtimecontracts.LiteralExpression("north"),
				Value:     runtimecontracts.RefExpression("entity.missing"),
			},
			want: "entity.missing",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := Run(context.Background(), semanticview.Wrap(containedStateOperationBundle(tc.write)), Options{})
			if !reportContains(report.Errors(), "expression_field_reference_validation", tc.want) {
				t.Fatalf("expected expression_field_reference_validation containing %q, got %#v", tc.want, report.Errors())
			}
		})
	}
}

func containedStateOperationBundle(write runtimecontracts.WorkflowDataWrite) *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"job.received": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"vertical_id": {Type: "text"},
						"job":         {Type: "json"},
					},
				},
			},
		},
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"VerticalState": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"status":      {Type: "text"},
						"active_jobs": {Type: "[Job]"},
					},
				},
				"Job": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"id":    {Type: "text"},
						"title": {Type: "text"},
					},
				},
			},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"subject": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"verticals": {Type: "map[text]VerticalState"},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"node-1": {
				ID: "node-1",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"job.received": {
						DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
							Writes: []runtimecontracts.WorkflowDataWrite{write},
						},
					},
				},
			},
		},
	}
}

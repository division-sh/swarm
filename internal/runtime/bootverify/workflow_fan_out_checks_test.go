package bootverify

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestRun_ValidatesFanOutCollectionContract(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*runtimecontracts.FanOutSpec, *runtimecontracts.WorkflowContractBundle)
		wantError string
	}{
		{
			name: "valid",
		},
		{
			name: "items source accepts array schema type",
			mutate: func(_ *runtimecontracts.FanOutSpec, bundle *runtimecontracts.WorkflowContractBundle) {
				event := bundle.Events["order.accepted"]
				event.Payload.Properties["line_items"] = runtimecontracts.EventFieldSpec{Type: "array"}
				bundle.Events["order.accepted"] = event
			},
		},
		{
			name: "missing alias",
			mutate: func(spec *runtimecontracts.FanOutSpec, _ *runtimecontracts.WorkflowContractBundle) {
				spec.As = ""
			},
			wantError: "fan_out.as is required",
		},
		{
			name: "identity cannot use index",
			mutate: func(spec *runtimecontracts.FanOutSpec, _ *runtimecontracts.WorkflowContractBundle) {
				spec.Identity = "fan_out.index"
				spec.Emit.Fields["line_item_id"] = runtimecontracts.CELExpression("fan_out.index")
			},
			wantError: "fan_out.identity must use the stable item alias",
		},
		{
			name: "identity must be carried",
			mutate: func(spec *runtimecontracts.FanOutSpec, _ *runtimecontracts.WorkflowContractBundle) {
				spec.Emit.Fields["line_item_id"] = runtimecontracts.CELExpression("line_item.sku")
			},
			wantError: `fan_out.emit.fields must carry identity expression "line_item.id"`,
		},
		{
			name: "max items only tightens ceiling",
			mutate: func(spec *runtimecontracts.FanOutSpec, _ *runtimecontracts.WorkflowContractBundle) {
				spec.MaxItems = runtimecontracts.DefaultFanOutMaxItems + 1
			},
			wantError: "max_items may only tighten the ceiling",
		},
		{
			name: "explicit zero max items fails closed",
			mutate: func(spec *runtimecontracts.FanOutSpec, _ *runtimecontracts.WorkflowContractBundle) {
				spec.MaxItems = 0
				spec.MaxItemsSet = true
			},
			wantError: "fan_out.max_items must be a positive integer when set",
		},
		{
			name: "items source must be declared",
			mutate: func(spec *runtimecontracts.FanOutSpec, _ *runtimecontracts.WorkflowContractBundle) {
				spec.ItemsFrom = "payload.undeclared_items"
			},
			wantError: "references undeclared payload field undeclared_items",
		},
		{
			name: "items source must be a collection",
			mutate: func(spec *runtimecontracts.FanOutSpec, bundle *runtimecontracts.WorkflowContractBundle) {
				spec.ItemsFrom = "payload.customer_id"
				event := bundle.Events["order.accepted"]
				event.Payload.Properties["customer_id"] = runtimecontracts.EventFieldSpec{Type: "text"}
				bundle.Events["order.accepted"] = event
			},
			wantError: `must reference a list/array collection field; field has type "text"`,
		},
		{
			name: "items source must not descend below declared collection field",
			mutate: func(spec *runtimecontracts.FanOutSpec, _ *runtimecontracts.WorkflowContractBundle) {
				spec.ItemsFrom = "payload.line_items.missing"
				spec.ItemsPath = runtimecontracts.RefExpression(spec.ItemsFrom).RefPath
			},
			wantError: "must reference exactly one declared top-level collection field",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := runtimecontracts.FanOutSpec{
				ItemsFrom: "payload.line_items",
				As:        "line_item",
				Identity:  "line_item.id",
				Emit: runtimecontracts.EmitSpec{
					Event: "line_item.requested",
					Fields: map[string]runtimecontracts.ExpressionValue{
						"line_item_id": runtimecontracts.CELExpression("line_item.id"),
						"line_index":   runtimecontracts.CELExpression("fan_out.index"),
					},
				},
			}
			bundle := fanOutValidationBundle(spec)
			if tc.mutate != nil {
				handler := bundle.Nodes["dispatcher"].EventHandlers["order.accepted"]
				tc.mutate(handler.FanOut, bundle)
				bundle.Nodes["dispatcher"].EventHandlers["order.accepted"] = handler
			}
			completeBootverifyFanOutFixture(t, bundle, "dispatcher", "order.accepted")

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if tc.wantError == "" {
				if reportContains(report.HardInvalidities(), fanOutValidationCheckID, "") {
					t.Fatalf("unexpected fan_out_validation invalidity: %#v", report.HardInvalidities())
				}
				return
			}
			if !reportContains(report.HardInvalidities(), fanOutValidationCheckID, tc.wantError) {
				t.Fatalf("expected fan_out_validation %q, got %#v", tc.wantError, report.HardInvalidities())
			}
		})
	}
}

func TestRun_RejectsFanOutCountDependencyWhenEntitySourceIsMutated(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*runtimecontracts.SystemNodeEventHandler, *runtimecontracts.WorkflowContractBundle)
	}{
		{
			name: "direct write",
			mutate: func(handler *runtimecontracts.SystemNodeEventHandler, _ *runtimecontracts.WorkflowContractBundle) {
				handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{
					{TargetRef: "entity.line_items", Value: runtimecontracts.CELExpression("payload.line_items")},
					{TargetRef: "metadata.observed_count", Value: runtimecontracts.CELExpression("fan_out.count")},
				}
			},
		},
		{
			name: "contained write",
			mutate: func(handler *runtimecontracts.SystemNodeEventHandler, _ *runtimecontracts.WorkflowContractBundle) {
				handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{
					{Operation: runtimecontracts.WorkflowDataOperationAppend, TargetRef: "entity.line_items", Value: runtimecontracts.LiteralExpression(map[string]any{"id": "later"})},
					{TargetRef: "metadata.observed_count", Value: runtimecontracts.CELExpression("fan_out.count")},
				}
			},
		},
		{
			name: "accumulator projection",
			mutate: func(handler *runtimecontracts.SystemNodeEventHandler, bundle *runtimecontracts.WorkflowContractBundle) {
				handler.Accumulate = &runtimecontracts.AccumulateSpec{Into: "collected"}
				handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{{TargetRef: "metadata.observed_count", Value: runtimecontracts.CELExpression("fan_out.count")}}
				node := bundle.Nodes["dispatcher"]
				node.StateSchema.Fields = []runtimecontracts.NodeStateField{{Name: "collected", Type: "[LineItem]"}}
				bundle.Nodes["dispatcher"] = node
				entity := bundle.RootEntities["subject"]
				field := entity.Fields["line_items"]
				field.MaterializeFrom = "dispatcher.collected"
				entity.Fields["line_items"] = field
				bundle.RootEntities["subject"] = entity
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := runtimecontracts.FanOutSpec{
				ItemsFrom: "entity.line_items",
				As:        "line_item",
				Identity:  "line_item.id",
				Emit: runtimecontracts.EmitSpec{Event: "line_item.requested", Fields: map[string]runtimecontracts.ExpressionValue{
					"line_item_id": runtimecontracts.CELExpression("line_item.id"),
				}},
			}
			bundle := fanOutValidationBundle(spec)
			bundle.RootEntities = runtimecontracts.EntityContractsDocument{
				"subject": {Fields: map[string]runtimecontracts.EntityFieldDecl{
					"line_items": {Type: "[LineItem]"},
				}},
			}
			node := bundle.Nodes["dispatcher"]
			handler := node.EventHandlers["order.accepted"]
			tc.mutate(&handler, bundle)
			node = bundle.Nodes["dispatcher"]
			node.EventHandlers["order.accepted"] = handler
			bundle.Nodes["dispatcher"] = node
			completeBootverifyFanOutFixture(t, bundle, "dispatcher", "order.accepted")

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
			if !reportContains(report.HardInvalidities(), fanOutValidationCheckID, "mutates fan_out.items_from and a same-handler data write references fan_out.count") {
				t.Fatalf("missing source/count cycle invalidity: %#v", report.HardInvalidities())
			}
		})
	}
}

func completeBootverifyFanOutFixture(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle, nodeID, eventType string) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: fan-out-validation\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	bundle.SourceArtifact = artifact
	node := bundle.Nodes[nodeID]
	handler := node.EventHandlers[eventType]
	owner, err := runtimeidentity.AdmitExecutableNodeDeclaration(".", nodeID)
	if err != nil {
		t.Fatal(err)
	}
	handler, err = runtimecontracts.QualifySystemNodeHandlerRuleRefsForEvent(owner, eventType, handler)
	if err != nil {
		t.Fatal(err)
	}
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	if bundle.Semantics.NodeHandlers[nodeID] == nil {
		bundle.Semantics.NodeHandlers[nodeID] = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	bundle.Semantics.NodeHandlers[nodeID][eventType] = handler

	bundle.PrepareFanOutPlans()
}

func fanOutValidationBundle(spec runtimecontracts.FanOutSpec) *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"LineItem": {Fields: map[string]runtimecontracts.TypeFieldSpec{"id": {Type: "text"}}},
		}},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"order.accepted": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"line_items": {Type: "[LineItem]"},
				}},
			},
			"line_item.requested": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"line_item_id": {Type: "text"},
					"line_index":   {Type: "integer"},
				}},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"dispatcher": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"order.accepted": {
						FanOut: &spec,
					},
				},
			},
		},
	}
}

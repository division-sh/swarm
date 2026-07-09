package bootverify

import (
	"context"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
			wantError: `must reference a collection payload field; field customer_id has type "text"`,
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

func TestFanOutCollectionTypeRef(t *testing.T) {
	tests := []struct {
		raw  string
		want bool
	}{
		{raw: "[text]", want: true},
		{raw: "list<text>", want: true},
		{raw: "text[]", want: true},
		{raw: "[]text", want: true},
		{raw: "array", want: true},
		{raw: "array (required for batch modes)", want: true},
		{raw: "array<text>", want: true},
		{raw: "", want: false},
		{raw: "text", want: false},
		{raw: "[]", want: false},
		{raw: "[ ]", want: false},
		{raw: "list<>", want: false},
		{raw: "array<>", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			if got := fanOutCollectionTypeRef(tc.raw); got != tc.want {
				t.Fatalf("fanOutCollectionTypeRef(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func fanOutValidationBundle(spec runtimecontracts.FanOutSpec) *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
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

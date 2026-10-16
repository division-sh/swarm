package accumulator

import (
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func targetBinding2293Bundle() *c.WorkflowContractBundle {
	return &c.WorkflowContractBundle{
		RootSchema: &c.FlowSchemaDocument{Name: "collection-target"},
		RootTypes: c.TypeCatalogDocument{
			Enums: map[string]c.EnumTypeDecl{
				"Rating":      {Values: []string{"low", "high"}},
				"OtherRating": {Values: []string{"other", "high"}},
			},
			Types: map[string]c.NamedTypeDecl{
				"Review": {Fields: map[string]c.TypeFieldSpec{"reviewer_id": {Type: "text"}, "value": {Type: "integer"}}},
				"Nested": {Fields: map[string]c.TypeFieldSpec{"id": {Type: "uuid"}}},
			},
		},
		RootEntities: c.EntityContractsDocument{"work": {Fields: map[string]c.EntityFieldDecl{
			"reviews": {Type: "[Review]", IsOptional: true},
		}}},
		Nodes: map[string]c.SystemNodeContract{"collector": {
			EventHandlers: map[string]c.SystemNodeEventHandler{"review.received": {Accumulate: &c.AccumulateSpec{Target: "reviews", Key: "reviewer_id"}}},
		}},
		Events: map[string]c.EventCatalogEntry{"review.received": {Payload: c.EventPayloadSpec{
			Type: "object", Properties: map[string]c.EventFieldSpec{"reviewer_id": {Type: "text"}, "value": {Type: "integer"}},
			Required: []string{"reviewer_id", "value"},
		}}},
	}
}

func targetBinding2293Source(t *testing.T, bundle *c.WorkflowContractBundle) semanticview.Source {
	t.Helper()
	source := semanticviewtest.WrapRootAgents(bundle)
	if err := c.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	return source
}

func Test2293Q1AccumulateTargetRequiresExactPayloadElement(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*c.WorkflowContractBundle, *c.AccumulateSpec)
		want string
	}{
		{"same_item", nil, ""},
		{"missing_target", func(_ *c.WorkflowContractBundle, spec *c.AccumulateSpec) { spec.Target = "" }, "target"},
		{"unknown_target", func(_ *c.WorkflowContractBundle, spec *c.AccumulateSpec) { spec.Target = "missing" }, "missing"},
		{"non_list", func(b *c.WorkflowContractBundle, _ *c.AccumulateSpec) {
			b.RootEntities["work"].Fields["reviews"] = c.EntityFieldDecl{Type: "Review"}
		}, "list"},
		{"map_not_list", func(b *c.WorkflowContractBundle, _ *c.AccumulateSpec) {
			b.RootEntities["work"].Fields["reviews"] = c.EntityFieldDecl{Type: "map[text]Review"}
		}, "list"},
		{"extra_payload_field", func(b *c.WorkflowContractBundle, _ *c.AccumulateSpec) {
			b.Events["review.received"].Payload.Properties["extra"] = c.EventFieldSpec{Type: "text"}
		}, "payload"},
		{"missing_payload_field", func(b *c.WorkflowContractBundle, _ *c.AccumulateSpec) {
			delete(b.Events["review.received"].Payload.Properties, "value")
			event := b.Events["review.received"]
			event.Payload.Required = []string{"reviewer_id"}
			b.Events["review.received"] = event
		}, "payload"},
		{"presence_mismatch", func(b *c.WorkflowContractBundle, _ *c.AccumulateSpec) {
			event := b.Events["review.received"]
			event.Payload.Required = []string{"reviewer_id"}
			b.Events["review.received"] = event
		}, "payload"},
		{"numeric_is_not_integer", func(b *c.WorkflowContractBundle, _ *c.AccumulateSpec) {
			b.Events["review.received"].Payload.Properties["value"] = c.EventFieldSpec{Type: "numeric"}
		}, "payload"},
		{"uuid_is_not_text", func(b *c.WorkflowContractBundle, _ *c.AccumulateSpec) {
			b.Events["review.received"].Payload.Properties["value"] = c.EventFieldSpec{Type: "uuid"}
			b.RootTypes.Types["Review"].Fields["value"] = c.TypeFieldSpec{Type: "text"}
		}, "payload"},
		{"timestamp_is_not_text", func(b *c.WorkflowContractBundle, _ *c.AccumulateSpec) {
			b.Events["review.received"].Payload.Properties["value"] = c.EventFieldSpec{Type: "timestamp"}
			b.RootTypes.Types["Review"].Fields["value"] = c.TypeFieldSpec{Type: "text"}
		}, "payload"},
		{"enum_is_not_text", func(b *c.WorkflowContractBundle, _ *c.AccumulateSpec) {
			b.Events["review.received"].Payload.Properties["value"] = c.EventFieldSpec{Type: "Rating"}
			b.RootTypes.Types["Review"].Fields["value"] = c.TypeFieldSpec{Type: "text"}
		}, "payload"},
		{"different_enum", func(b *c.WorkflowContractBundle, _ *c.AccumulateSpec) {
			b.Events["review.received"].Payload.Properties["value"] = c.EventFieldSpec{Type: "Rating"}
			b.RootTypes.Types["Review"].Fields["value"] = c.TypeFieldSpec{Type: "OtherRating"}
		}, "payload"},
		{"nested_format", func(b *c.WorkflowContractBundle, _ *c.AccumulateSpec) {
			b.Events["review.received"].Payload.Properties["value"] = c.EventFieldSpec{Type: "Nested"}
			b.RootTypes.Types["Review"].Fields["value"] = c.TypeFieldSpec{Type: "map[text]text"}
		}, "payload"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := targetBinding2293Bundle()
			spec := c.AccumulateSpec{Target: "reviews", Key: "reviewer_id"}
			if tc.edit != nil {
				tc.edit(bundle, &spec)
			}
			err := ValidateSpecForHandler(targetBinding2293Source(t, bundle), identitytest.RootNode(t, "collector"), "review.received", &spec)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("target binding refusal=%v want %q", err, tc.want)
			}
			if tc.want == "payload" && (!strings.Contains(err.Error(), "reviews") || !strings.Contains(err.Error(), "review.received") || !strings.Contains(err.Error(), "Review")) {
				t.Fatalf("mismatch lost target/item/event teaching: %v", err)
			}
		})
	}
}

func Test2293Q1AccumulateTargetKeepsEveryScalarDomain(t *testing.T) {
	for _, typeRef := range []string{"text", "uuid", "timestamp", "integer", "numeric", "boolean", "Rating", "[uuid]", "map[Rating]Nested", "Nested"} {
		t.Run(typeRef, func(t *testing.T) {
			bundle := targetBinding2293Bundle()
			bundle.Events["review.received"].Payload.Properties["value"] = c.EventFieldSpec{Type: typeRef}
			bundle.RootTypes.Types["Review"].Fields["value"] = c.TypeFieldSpec{Type: typeRef}
			if err := ValidateSpecForHandler(targetBinding2293Source(t, bundle), identitytest.RootNode(t, "collector"), "review.received", &c.AccumulateSpec{Target: "reviews", Key: "reviewer_id"}); err != nil {
				t.Fatalf("matching %s target refused: %v", typeRef, err)
			}
		})
	}
}

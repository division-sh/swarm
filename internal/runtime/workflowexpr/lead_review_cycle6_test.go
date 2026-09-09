package workflowexpr

import (
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"testing"
)

func TestLeadReview6ComputedCollectionLookup(t *testing.T) {
	scalar := rc.ResolvedCatalogType{Kind: rc.CatalogTypeText}
	list := rc.ResolvedCatalogType{Kind: rc.CatalogTypeList, Element: &scalar}
	payload := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "tags", Type: list}}}
	opts := ValueExpressionOptions{PayloadType: &payload}
	for _, expr := range []string{`payload.tags[0]`, `(true ? payload.tags : payload.tags)[0]`, `([] + payload.tags)[0]`, `(true ? payload.tags : payload.tags)[?0].orValue("")`} {
		t.Run(expr, func(t *testing.T) {
			err := ValidateValueExpressionWithOptions(expr, opts)
			got, runErr := EvalValueExpressionWithOptions(expr, ValueContext{Payload: map[string]any{"tags": []any{}}}, opts)
			t.Logf("validation=%v evaluation=%v error=%v", err, got, runErr)
			if expr == `(true ? payload.tags : payload.tags)[?0].orValue("")` {
				if err != nil || runErr != nil || got != "" {
					t.Fatal("safe control failed")
				}
			} else if err == nil {
				t.Error("unchecked collection lookup accepted")
			}
		})
	}
}

func TestLeadReview6AnonymousRecordIdentity(t *testing.T) {
	textType := rc.ResolvedCatalogType{Kind: rc.CatalogTypeText}
	optional := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "note", Type: textType, IsOptional: true}}}
	required := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "note", Type: textType}}}
	nested := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "b", Type: required}}}
	payload := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "a_b", Type: optional}, {Name: "a", Type: nested}}}
	opts := ValueExpressionOptions{PayloadType: &payload}
	expr := `payload.a_b.note`
	err := ValidateValueExpressionWithOptions(expr, opts)
	_, runErr := EvalValueExpressionWithOptions(expr, ValueContext{Payload: map[string]any{"a_b": map[string]any{}, "a": map[string]any{"b": map[string]any{"note": "ok"}}}}, opts)
	t.Logf("validation=%v evaluation=%v", err, runErr)
	if err == nil {
		t.Fatal("distinct record paths shared optionality")
	}
}

func TestLeadReview6ImportedRecordIdentity(t *testing.T) {
	textSchema, err := rc.NewToolInputSchema(rc.ToolSchemaString)
	if err != nil {
		t.Fatal(err)
	}
	optional, err := rc.NewToolInputSchema(rc.ToolSchemaObject, rc.ToolSchemaProperties(map[string]rc.ToolInputSchema{"note": textSchema}), rc.ToolSchemaAdditionalPropertiesAllowed(false))
	if err != nil {
		t.Fatal(err)
	}
	required, err := rc.NewToolInputSchema(rc.ToolSchemaObject, rc.ToolSchemaProperties(map[string]rc.ToolInputSchema{"note": textSchema}), rc.ToolSchemaRequired("note"), rc.ToolSchemaAdditionalPropertiesAllowed(false))
	if err != nil {
		t.Fatal(err)
	}
	nested, err := rc.NewToolInputSchema(rc.ToolSchemaObject, rc.ToolSchemaProperties(map[string]rc.ToolInputSchema{"b": optional}), rc.ToolSchemaRequired("b"), rc.ToolSchemaAdditionalPropertiesAllowed(false))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := rc.CompileImportedEventSchema(".", "review.received", rc.EventCatalogEntry{Payload: rc.EventPayloadSpec{Properties: map[string]rc.EventFieldSpec{"a": {ExactSchema: &nested}, "a_b": {ExactSchema: &required}}, Required: []string{"a", "a_b"}}}, rc.CompiledEventSchemaSource{FlowPath: ".", Layer: "provider"})
	if err != nil {
		t.Fatal(err)
	}
	payload, ok := schema.StructuralType()
	if !ok {
		t.Fatal("no structural type")
	}
	opts := ValueExpressionOptions{PayloadType: &payload}
	expr := `payload.a.b.note`
	err = ValidateValueExpressionWithOptions(expr, opts)
	_, runErr := EvalValueExpressionWithOptions(expr, ValueContext{Payload: map[string]any{"a": map[string]any{"b": map[string]any{}}, "a_b": map[string]any{"note": "ok"}}}, opts)
	t.Logf("validation=%v evaluation=%v", err, runErr)
	if err == nil {
		t.Fatal("imported exact record paths shared optionality")
	}
}

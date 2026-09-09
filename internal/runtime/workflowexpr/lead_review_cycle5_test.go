package workflowexpr

import (
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"testing"
)

func TestLeadReviewPresenceShadowing(t *testing.T) {
	scalar := rc.ResolvedCatalogType{Kind: rc.CatalogTypeInteger}
	child := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Name: "ReviewChild", Fields: []rc.ResolvedCatalogField{{Name: "score", Type: scalar, IsOptional: true}}}
	payload := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Name: "ReviewPayload", Fields: []rc.ResolvedCatalogField{{Name: "score", Type: scalar, IsOptional: true}, {Name: "items", Type: rc.ResolvedCatalogType{Kind: rc.CatalogTypeList, Element: &child}}}}
	opts := ValueExpressionOptions{PayloadType: &payload, RequireBool: true}
	for _, expr := range []string{`has(payload.score) && payload.items.exists(payload, payload.score > 0)`, `payload.items.exists(x, has(x.score) && payload.items.exists(x, x.score > 0))`} {
		t.Run(expr, func(t *testing.T) {
			err := ValidateValueExpressionWithOptions(expr, opts)
			_, runtimeErr := EvalValueExpressionWithOptions(expr, ValueContext{Payload: map[string]any{"score": 7, "items": []any{map[string]any{"score": 0}, map[string]any{}}}}, opts)
			t.Logf("compile=%v runtime=%v", err, runtimeErr)
			if err == nil {
				t.Error("shadowed optional read was admitted")
			}
		})
	}
}

func TestLeadReviewJoinNamedOptional(t *testing.T) {
	result := rc.CatalogTypeReference{Type: "ReviewResult", Catalog: rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{"ReviewResult": {Fields: map[string]rc.TypeFieldSpec{"note": {Type: "text", IsOptional: true}}}}}}
	opts := ValueExpressionOptions{AllowJoin: true, JoinResultType: result, RequireBool: true}
	expr := `join.results.exists(r, r.note != "")`
	err := ValidateValueExpressionWithOptions(expr, opts)
	_, runtimeErr := EvalValueExpressionWithOptions(expr, ValueContext{Join: map[string]any{"results": []any{map[string]any{}}}}, opts)
	t.Logf("compile=%v runtime=%v", err, runtimeErr)
	if err == nil {
		t.Error("join named-record optional read admitted without decision")
	}
}

func TestLeadReviewPresenceMerge(t *testing.T) {
	scalar := rc.ResolvedCatalogType{Kind: rc.CatalogTypeInteger}
	payload := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Name: "ReviewMerge", Fields: []rc.ResolvedCatalogField{{Name: "score", Type: scalar, IsOptional: true}}}
	opts := ValueExpressionOptions{PayloadType: &payload, RequireBool: true}
	for _, expr := range []string{`(has(payload.score) || has(payload.score)) && payload.score > 0`, `(true ? has(payload.score) : has(payload.score)) && payload.score > 0`} {
		if err := ValidateValueExpressionWithOptions(expr, opts); err != nil {
			t.Errorf("safe branch merge rejected %s: %v", expr, err)
		}
	}
}

func TestLeadReviewCompiledMapPresence(t *testing.T) {
	textSchema, err := rc.NewToolInputSchema(rc.ToolSchemaString)
	if err != nil {
		t.Fatal(err)
	}
	itemSchema, err := rc.NewToolInputSchema(rc.ToolSchemaObject, rc.ToolSchemaProperties(map[string]rc.ToolInputSchema{"note": textSchema}), rc.ToolSchemaAdditionalPropertiesAllowed(false))
	if err != nil {
		t.Fatal(err)
	}
	mapSchema, err := rc.NewToolInputSchema(rc.ToolSchemaObject, rc.ToolSchemaAdditionalPropertiesSchema(itemSchema))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := rc.CompileImportedEventSchema(".", "review.received", rc.EventCatalogEntry{Payload: rc.EventPayloadSpec{Properties: map[string]rc.EventFieldSpec{"labels": {ExactSchema: &mapSchema}}, Required: []string{"labels"}}}, rc.CompiledEventSchemaSource{FlowPath: ".", Layer: "provider"})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := schema.StructuralType()
	field, _ := payload.Field("labels")
	t.Logf("compiled map kind=%s", field.Type.Kind)
	opts := ValueExpressionOptions{PayloadType: &payload}
	expr := `payload.labels[?"k"].optMap(v, v.note).orValue("")`
	compileErr := ValidateValueExpressionWithOptions(expr, opts)
	_, runtimeErr := EvalValueExpressionWithOptions(expr, ValueContext{Payload: map[string]any{"labels": map[string]any{"k": map[string]any{}}}}, opts)
	t.Logf("compile=%v runtime=%v", compileErr, runtimeErr)
	if field.Type.Kind != rc.CatalogTypeMap {
		t.Error("compiled map lost structural value type")
	}
	if compileErr == nil {
		t.Error("map value optional field read admitted without decision")
	}
}

func TestLeadReviewEntityExecution(t *testing.T) {
	record := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "note", Type: rc.ResolvedCatalogType{Kind: rc.CatalogTypeText}, IsOptional: true}}}
	entityType := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "profile", Type: record}}}
	ctx := ValueContext{Entity: map[string]any{"profile": map[string]any{"id": "a", "status": "ready", "tags": []any{}}}}
	for _, expr := range []string{`entity.profile.note != ""`, `has(entity.profile.note) && entity.profile.note != ""`, `entity.profile.?note.orValue("") != ""`} {
		got, err := EvalValueExpressionWithOptions(expr, ctx, ValueExpressionOptions{EntityType: &entityType})
		t.Logf("%s result=%v err=%v", expr, got, err)
		if expr == `entity.profile.?note.orValue("") != ""` && err != nil {
			t.Errorf("valid optional decision failed: %v", err)
		}
	}
}

func TestLeadReviewJoinDecisionControl(t *testing.T) {
	result := rc.CatalogTypeReference{Type: "ReviewResult", Catalog: rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{"ReviewResult": {Fields: map[string]rc.TypeFieldSpec{"note": {Type: "text", IsOptional: true}}}}}}
	opts := ValueExpressionOptions{AllowJoin: true, JoinResultType: result, RequireBool: true}
	for _, expr := range []string{`join.results.exists(r, has(r.note) && r.note != "")`, `join.results.exists(r, r.?note.orValue("") != "")`} {
		got, err := EvalValueExpressionWithOptions(expr, ValueContext{Join: map[string]any{"results": []any{map[string]any{}}}}, opts)
		if err != nil || got != false {
			t.Errorf("%s result=%v err=%v", expr, got, err)
		}
	}
}

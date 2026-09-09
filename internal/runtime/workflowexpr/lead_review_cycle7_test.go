package workflowexpr

import (
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"testing"
)

func TestLeadReview7ComposedPresenceAndScope(t *testing.T) {
	text := rc.ResolvedCatalogType{Kind: rc.CatalogTypeText}
	note := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "note", Type: text, IsOptional: true}}}
	notes := rc.ResolvedCatalogType{Kind: rc.CatalogTypeList, Element: &note}
	labels := rc.ResolvedCatalogType{Kind: rc.CatalogTypeMap, Key: &text, Value: &text}
	root := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{
		{Name: "note", Type: text, IsOptional: true}, {Name: "items", Type: notes}, {Name: "labels", Type: labels},
	}}
	opts := ValueExpressionOptions{PayloadType: &root}
	ctx := ValueContext{Payload: map[string]any{"items": []any{map[string]any{}}, "labels": map[string]any{}}}
	for _, expr := range []string{
		`(false ? [] : payload.items).map(x, x.note)`,
		`[0].map(i, payload.items).map(xs, xs[0])`,
		`optional.of(payload.labels).orValue({}).missing`,
		`payload.items.all(x, has(x.note)) && payload.items.all(x, x.note == "")`,
		`["a"].map(x, payload.labels).map(m, m.missing)`,
	} {
		t.Run("reject/"+expr, func(t *testing.T) {
			if err := ValidateValueExpressionWithOptions(expr, opts); err == nil {
				t.Fatal("unsafe composition accepted")
			}
		})
	}
	if err := ValidateValueExpressionWithOptions(`has(payload.note) && [1].all(payload, true) && payload.note == ""`, opts); err != nil {
		t.Fatalf("unshadowed outer presence lost: %v", err)
	}
	for _, expr := range []string{
		`(false ? [] : payload.items).all(x, x.?note.orValue("") == "")`,
		`[0].map(i, payload.items).all(xs, xs[?0].value().?note.orValue("") == "")`,
		`optional.of(payload.labels).orValue({})[?"missing"].orValue("") == ""`,
		`payload.items.all(x, !has(x.note) || x.note == "")`,
		`payload.items.all(x, [1].all(payload, x.?note.orValue("") == ""))`,
	} {
		t.Run("safe/"+expr, func(t *testing.T) {
			got, err := EvalValueExpressionWithOptions(expr, ctx, opts)
			if err != nil || got != true {
				t.Fatalf("got=%v err=%v", got, err)
			}
		})
	}
}

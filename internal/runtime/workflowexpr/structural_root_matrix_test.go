package workflowexpr

import (
	"strings"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestStructuralPresenceRootShapeAndDecisionMatrix(t *testing.T) {
	text := rc.ResolvedCatalogType{Kind: rc.CatalogTypeText}
	leaf := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Name: "MatrixLeaf", Fields: []rc.ResolvedCatalogField{
		{Name: "id", Type: text}, {Name: "note", Type: text, IsOptional: true},
	}}
	dictionary := rc.ResolvedCatalogType{Kind: rc.CatalogTypeMap, Key: &text, Value: &leaf}
	record := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Name: "MatrixRecord", Fields: []rc.ResolvedCatalogField{
		{Name: "note", Type: text, IsOptional: true},
		{Name: "child", Type: leaf}, {Name: "maybe", Type: leaf, IsOptional: true},
		{Name: "children", Type: rc.ResolvedCatalogType{Kind: rc.CatalogTypeList, Element: &leaf}},
		{Name: "labels", Type: dictionary},
	}}
	for _, root := range []string{"payload", "entity", "item", "join"} {
		for _, tc := range []struct {
			name, expression string
			reject           bool
		}{
			{"scalar unsafe", `R.note != ""`, true},
			{"scalar has", `has(R.note) && R.note != ""`, false},
			{"scalar optional", `R.?note.orValue("") != ""`, false},
			{"record unsafe", `R.child.note != ""`, true},
			{"record has", `has(R.child.note) && R.child.note != ""`, false},
			{"record optional", `R.child.?note.orValue("") != ""`, false},
			{"parent unsafe", `R.maybe.id != ""`, true},
			{"parent optional", `R.?maybe.optMap(v, v.id).orValue("") != ""`, false},
			{"parent and child", `R.?maybe.optFlatMap(v, v.?note).orValue("") != ""`, false},
			{"list unsafe", `R.children.exists(v, v.note != "")`, true},
			{"list has", `R.children.exists(v, has(v.note) && v.note != "")`, false},
			{"list optional", `R.children.exists(v, v.?note.orValue("") != "")`, false},
			{"map unsafe value", `R.labels[?"k"].optMap(v, v.note).orValue("") != ""`, true},
			{"map safe value", `R.labels[?"k"].optFlatMap(v, v.?note).orValue("") != ""`, false},
			{"map unsafe key", `R.labels.k.id != ""`, true},
			{"map safe key", `has(R.labels.k) && R.labels.k.id != ""`, false},
			{"list direct index", `R.children[0].id != ""`, true},
			{"OR common", `(has(R.note) || has(R.note)) && R.note != ""`, false},
			{"OR partial", `(has(R.note) || true) && R.note != ""`, true},
			{"AND false common", `(!has(R.note) && !has(R.note)) || R.note != ""`, false},
			{"AND false partial", `(!has(R.note) && false) || R.note != ""`, true},
			{"ternary common", `(true ? has(R.note) : has(R.note)) && R.note != ""`, false},
			{"ternary partial", `(true ? has(R.note) : true) && R.note != ""`, true},
			{"outer survives", `has(R.note) && R.children.exists(v, R.note != "")`, false},
			{"root shadow", `has(R.note) && R.children.exists(R, R.note != "")`, true},
			{"optional macro shadow", `has(R.note) && R.?maybe.optMap(R, R.note != "").orValue(false)`, true},
			{"optional macro own proof", `has(R.note) && R.?maybe.optMap(R, has(R.note) && R.note != "").orValue(false)`, false},
			{"inner alias shadow", `R.children.exists(v, has(v.note) && R.children.exists(v, v.note != ""))`, true},
		} {
			t.Run(root+"/"+tc.name, func(t *testing.T) {
				opts := ValueExpressionOptions{RequireBool: true}
				value := map[string]any{"child": map[string]any{"id": "1"}, "children": []any{map[string]any{"id": "1"}}, "labels": map[string]any{"k": map[string]any{"id": "1"}}}
				ctx := ValueContext{}
				alias := root
				switch root {
				case "payload":
					opts.PayloadType = &record
					ctx.Payload = value
				case "entity":
					opts.EntityType = &record
					ctx.Entity = value
				case "item":
					opts.ItemType = &record
					opts.AllowBareItem = true
					opts.ItemAlias = "item"
					ctx.FanOut = map[string]any{"item": value}
				case "join":
					opts.AllowJoin = true
					opts.JoinResultType = rc.CatalogTypeReference{Type: "MatrixRecord", Catalog: rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{
						"MatrixLeaf":   {Fields: map[string]rc.TypeFieldSpec{"id": {Type: "text"}, "note": {Type: "text", IsOptional: true}}},
						"MatrixRecord": {Fields: map[string]rc.TypeFieldSpec{"note": {Type: "text", IsOptional: true}, "child": {Type: "MatrixLeaf"}, "maybe": {Type: "MatrixLeaf", IsOptional: true}, "children": {Type: "list<MatrixLeaf>"}, "labels": {Type: "map[text]MatrixLeaf"}}},
					}}}
					ctx.Join = map[string]any{"results": []any{value}}
					alias = "r"
				}
				expression := strings.ReplaceAll(tc.expression, "R.", alias+".")
				expression = strings.ReplaceAll(expression, "(R,", "("+alias+",")
				if root == "join" {
					expression = "join.results.exists(r, " + expression + ")"
				}
				err := ValidateValueExpressionWithOptions(expression, opts)
				if tc.reject {
					if err == nil {
						t.Fatalf("unsafe expression admitted: %s", expression)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err := EvalValueExpressionWithOptions(expression, ctx, opts); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

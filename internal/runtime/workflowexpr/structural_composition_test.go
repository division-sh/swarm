package workflowexpr

import (
	"strings"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestStructuralRegistrationUsesShapeNotDiagnosticIdentity(t *testing.T) {
	text := rc.ResolvedCatalogType{Kind: rc.CatalogTypeText}
	for _, named := range []bool{false, true} {
		for _, reversed := range []bool{false, true} {
			for _, kind := range []rc.CatalogTypeKind{rc.CatalogTypeText, rc.CatalogTypeInteger} {
				a := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "note", Type: text, IsOptional: true}}}
				b := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "note", Type: rc.ResolvedCatalogType{Kind: kind}}}}
				if named {
					a.Name, b.Name = "same.name", "same.name"
				}
				fields := []rc.ResolvedCatalogField{
					{Name: "a_b", Type: a}, {Name: "a", Type: rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "b", Type: b}}}},
					{Name: "items_item", Type: b}, {Name: "items", Type: rc.ResolvedCatalogType{Kind: rc.CatalogTypeList, Element: &a}},
					{Name: "dict_value", Type: b}, {Name: "dict", Type: rc.ResolvedCatalogType{Kind: rc.CatalogTypeMap, Key: &text, Value: &a}},
				}
				if reversed {
					for i, j := 0, len(fields)-1; i < j; i, j = i+1, j-1 {
						fields[i], fields[j] = fields[j], fields[i]
					}
				}
				root := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Name: "same.name", Fields: fields}
				opts := ValueExpressionOptions{PayloadType: &root, ItemType: &b, ItemAlias: "other"}
				for _, expression := range []string{`payload.a_b.note`, `payload.items.map(x, x.note)`, `payload.dict.map(k, payload.dict[?k].value().note)`} {
					if err := ValidateValueExpressionWithOptions(expression, opts); err == nil || !strings.Contains(err.Error(), "optional") {
						t.Fatalf("named=%t reversed=%t kind=%s presence rejection for %s: %v", named, reversed, kind, expression, err)
					}
				}
				got, err := EvalValueExpressionWithOptions(`payload.a_b.?note.orValue("")`, ValueContext{Payload: map[string]any{"a_b": map[string]any{}}}, opts)
				if err != nil || got != "" {
					t.Fatalf("safe expression: %v %v", got, err)
				}
				opts.ResultType = &b.Fields[0].Type
				if err := ValidateValueExpressionWithOptions(`payload.a.b.note`, opts); err != nil {
					t.Fatalf("required value type lost: %v", err)
				}
				if err := ValidateValueExpressionWithOptions(`other.note`, opts); err != nil {
					t.Fatalf("separate root type lost: %v", err)
				}
				provider, err := newWorkflowStructuralTypeProvider(nil, opts)
				if err != nil {
					t.Fatal(err)
				}
				at, err := provider.register("result", "result", a)
				if err != nil {
					t.Fatal(err)
				}
				bt, err := provider.register("payload", "a_b", b)
				if err != nil {
					t.Fatal(err)
				}
				if at.TypeName() == bt.TypeName() {
					t.Fatal("opposite optionality merged across root/sink")
				}
				reused, err := provider.register("different", "path", a)
				if err != nil {
					t.Fatal(err)
				}
				if reused.TypeName() != at.TypeName() {
					t.Fatal("equal structural shape did not reuse registration")
				}
			}
		}
	}
}

func TestStructuralMapResultComposition(t *testing.T) {
	text := rc.ResolvedCatalogType{Kind: rc.CatalogTypeText}
	dict := rc.ResolvedCatalogType{Kind: rc.CatalogTypeMap, Key: &text, Value: &text}
	root := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "labels", Type: dict}}}
	opts := ValueExpressionOptions{PayloadType: &root, EntityType: &root}
	ctx := ValueContext{Payload: map[string]any{"labels": map[string]any{}}, Entity: map[string]any{"labels": map[string]any{}}}
	for _, name := range []string{"payload", "entity"} {
		for _, template := range []string{
			`(true ? ROOT.labels : ROOT.labels)`, `[ROOT.labels][?0].value()`,
			`optional.of(ROOT.labels).value()`, `[0].map(x, ROOT.labels)[?0].value()`,
			`{"x":ROOT.labels}[?"x"].value()`,
		} {
			collection := strings.ReplaceAll(template, "ROOT", name)
			if err := ValidateValueExpressionWithOptions(collection+`["missing"]`, opts); err == nil {
				t.Fatalf("direct composed map lookup admitted: %s", collection)
			}
			got, err := EvalValueExpressionWithOptions(collection+`[?"missing"].orValue("")`, ctx, opts)
			if err != nil || got != "" {
				t.Fatalf("safe map lookup %s: %v %v", collection, got, err)
			}
		}
	}
	for _, expression := range []string{`["constant"][0]`, `{"key":"constant"}["key"]`} {
		got, err := EvalValueExpressionWithOptions(expression, ctx, opts)
		if err != nil || got != "constant" {
			t.Fatalf("unrelated literal lookup %s: %v %v", expression, got, err)
		}
	}
}

func TestStructuralCollectionResultCompositionAcrossRoots(t *testing.T) {
	text := rc.ResolvedCatalogType{Kind: rc.CatalogTypeText}
	list := rc.ResolvedCatalogType{Kind: rc.CatalogTypeList, Element: &text}
	root := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "tags", Type: list}}}
	for _, name := range []string{"payload", "entity", "row", "join"} {
		opts := ValueExpressionOptions{}
		ctx := ValueContext{}
		var variables map[string]any
		data := map[string]any{"tags": []any{}}
		switch name {
		case "payload":
			opts.PayloadType = &root
			ctx.Payload = data
		case "entity":
			opts.EntityType = &root
			ctx.Entity = data
		case "row":
			opts.ItemType = &root
			opts.ItemAlias = "row"
			variables = map[string]any{"row": data}
		case "join":
			opts.AllowJoin = true
			opts.JoinResultType = rc.CatalogTypeReference{Type: "text"}
			ctx.Join = map[string]any{"results": []any{}, "expected": int64(0), "completed": int64(0), "missing": []any{}, "timed_out": false}
		}
		for _, template := range []string{
			`(true ? ROOT.tags : ROOT.tags)`, `([] + ROOT.tags)`, `(ROOT.tags + [])`,
			`[ROOT.tags][?0].value()`, `{"x":ROOT.tags}[?"x"].value()`,
			`optional.of(ROOT.tags).value()`, `ROOT.tags.map(x,x)`,
			`[0].map(x, ROOT.tags)[?0].value()`, `ROOT.tags.filter(x,true)`,
			`["x"].filter(x, ROOT.tags.size() > 0)`,
			`(ROOT.tags.size() == 0 ? ROOT.tags : [])`,
		} {
			collection := strings.ReplaceAll(template, "ROOT", name)
			if name == "join" {
				collection = strings.ReplaceAll(collection, "join.tags", "join.results")
			}
			t.Run(name+"/"+template, func(t *testing.T) {
				if err := ValidateValueExpressionWithOptions(collection+"[0]", opts); err == nil {
					t.Fatal("composed direct lookup admitted")
				}
				safe := collection + `[?0].orValue("")`
				if err := ValidateValueExpressionWithOptions(safe, opts); err != nil {
					t.Fatal(err)
				}
				if name != "row" {
					got, err := EvalValueExpressionWithOptions(safe, ctx, opts)
					if err != nil || got != "" {
						t.Fatalf("safe: %v %v", got, err)
					}
				} else {
					env, err := dataExpressionEnvForContext(opts)
					if err != nil {
						t.Fatal(err)
					}
					ast, err := compileValueExpression(env, safe, opts)
					if err != nil {
						t.Fatal(err)
					}
					program, err := env.Program(ast)
					if err != nil {
						t.Fatal(err)
					}
					got, _, err := program.Eval(variables)
					if err != nil || got.Value() != "" {
						t.Fatalf("safe alias: %v %v", got, err)
					}
				}
			})
		}
	}
}

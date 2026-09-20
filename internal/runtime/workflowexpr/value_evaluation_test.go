package workflowexpr

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	celtypes "github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

func newScopedEvaluation(t testing.TB, ctx ValueContext) *ValueEvaluation {
	t.Helper()
	evaluation := TryNewValueEvaluation(ctx)
	if evaluation == nil {
		t.Fatal("raw JSON context was not eligible for scoped evaluation")
	}
	return evaluation
}

func prepareScopedValue(t testing.TB, expression string, opts ValueExpressionOptions) *PreparedValueExpression {
	t.Helper()
	p, err := PrepareValueExpression(expression, opts)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestValueEvaluationFailurePrecedence(t *testing.T) {
	integer := rc.ResolvedCatalogType{Kind: rc.CatalogTypeInteger}
	entity := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "score", Type: integer}}}
	missing := prepareScopedValue(t, "entity.score", ValueExpressionOptions{EntityType: &entity})
	constant := prepareScopedValue(t, "1", ValueExpressionOptions{})
	ctx := ValueContext{Entity: map[string]any{}, Payload: map[string]any{"unused": math.Inf(1)}}
	evaluation := newScopedEvaluation(t, ctx)
	for _, p := range []*PreparedValueExpression{nil, missing, constant} {
		_, want := p.Eval(ctx)
		_, got := evaluation.Eval(p)
		if got == nil || want == nil || got.Error() != want.Error() || reflect.TypeOf(got) != reflect.TypeOf(want) {
			t.Fatalf("diagnostic changed: got %T %v; want %T %v", got, got, want, want)
		}
		if evaluation.activation != nil {
			t.Fatal("failed evaluation retained a projected activation")
		}
	}
	_, err := evaluation.Eval(constant)
	var projection *CELProjectionError
	if !errors.As(err, &projection) || projection.Path != "$.payload.unused" {
		t.Fatalf("unused hostile input lost projection diagnostic: %v", err)
	}
	clean := newScopedEvaluation(t, ValueContext{Entity: map[string]any{}})
	if _, err := clean.Eval(constant); err != nil {
		t.Fatal(err)
	}
	if _, err := clean.Eval(missing); err == nil || !strings.Contains(err.Error(), "entity.score") {
		t.Fatalf("later field skipped its own missing-entity check: %v", err)
	}
}

func TestValueEvaluationOwnsRawInputsAndResults(t *testing.T) {
	integer := rc.ResolvedCatalogType{Kind: rc.CatalogTypeInteger}
	list := rc.ResolvedCatalogType{Kind: rc.CatalogTypeList, Element: &integer}
	object := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "items", Type: list}}}
	p := prepareScopedValue(t, "payload", ValueExpressionOptions{PayloadType: &object})
	raw := map[string]any{"items": []any{json.Number("7")}}
	ctx := ValueContext{Payload: raw}
	evaluation := newScopedEvaluation(t, ctx)
	raw["items"].([]any)[0] = math.Inf(1)
	result, err := evaluation.Eval(p)
	if err != nil {
		t.Fatal(err)
	}
	items := result.Value().(map[string]any)["items"].([]any)
	if items[0] != int64(7) {
		t.Fatalf("snapshot changed: %#v", items)
	}
	items[0] = int64(99)
	delete(raw, "items")
	again, err := evaluation.Eval(p)
	if err != nil || again.Value().(map[string]any)["items"].([]any)[0] != int64(7) {
		t.Fatalf("caller/result mutation leaked: %#v %v", again, err)
	}
	raw["items"] = []any{json.Number("11")}
	fresh, err := newScopedEvaluation(t, ctx).Eval(p)
	if err != nil || fresh.Value().(map[string]any)["items"].([]any)[0] != int64(11) {
		t.Fatalf("new ordinal reused prior raw inputs: %#v %v", fresh, err)
	}
}

func TestValueEvaluationAliasesKindsAndAbsence(t *testing.T) {
	number := rc.ResolvedCatalogType{Kind: rc.CatalogTypeNumber}
	for _, input := range []any{int64(7), float64(7), json.Number("7"), json.Number("7.0"), json.Number("7e0")} {
		t.Run(fmt.Sprintf("%T_%v", input, input), func(t *testing.T) {
			ctx := ValueContext{FanOut: map[string]any{"item": input}}
			evaluation := newScopedEvaluation(t, ctx)
			for _, alias := range []string{"row", "other"} {
				p := prepareScopedValue(t, alias, ValueExpressionOptions{ItemAlias: alias, ItemType: &number})
				want, wantErr := p.Eval(ctx)
				got, gotErr := evaluation.Eval(p)
				if wantErr != nil || gotErr != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("kind changed for %T %v: %#v/%v vs %#v/%v", input, input, got, gotErr, want, wantErr)
				}
				if _, leaked := evaluation.activation[alias]; leaked {
					t.Fatal("field alias mutated shared roots")
				}
			}
		})
	}
	integer := rc.ResolvedCatalogType{Kind: rc.CatalogTypeInteger}
	item := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "score", Type: integer, IsOptional: true}}}
	optional := prepareScopedValue(t, "row.?score", ValueExpressionOptions{ItemAlias: "row", ItemType: &item, ResultType: &integer, ResultOptional: true})
	for _, present := range []bool{true, false, true} {
		row := map[string]any{}
		if present {
			row["score"] = int64(7)
		}
		evaluation := newScopedEvaluation(t, ValueContext{FanOut: map[string]any{"item": row}})
		for i := 0; i < 2; i++ {
			got, err := evaluation.Eval(optional)
			if err != nil || got.Present() != present {
				t.Fatalf("optional absence changed: %#v %v", got, err)
			}
		}
	}
	bare := prepareScopedValue(t, "item + 1", ValueExpressionOptions{AllowBareItem: true, ItemType: &integer})
	aliased := prepareScopedValue(t, "row + 1", ValueExpressionOptions{ItemAlias: "row", ItemType: &integer})
	evaluation := newScopedEvaluation(t, ValueContext{FanOut: map[string]any{"item": int64(7)}})
	for _, p := range []*PreparedValueExpression{bare, aliased, bare} {
		got, err := evaluation.Eval(p)
		if err != nil || got.Value() != int64(8) {
			t.Fatalf("bare/aliased item changed: %#v %v", got, err)
		}
		for _, alias := range []string{"item", "row"} {
			if _, leaked := evaluation.activation[alias]; leaked {
				t.Fatalf("%s leaked into shared roots", alias)
			}
		}
	}
}

func TestValueEvaluationIndependentConcurrentOrdinals(t *testing.T) {
	integer := rc.ResolvedCatalogType{Kind: rc.CatalogTypeInteger}
	p := prepareScopedValue(t, "row + 1", ValueExpressionOptions{ItemAlias: "row", ItemType: &integer})
	var wg sync.WaitGroup
	for i := int64(0); i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			evaluation := newScopedEvaluation(t, ValueContext{FanOut: map[string]any{"item": i}})
			for j := 0; j < 3; j++ {
				got, err := evaluation.Eval(p)
				if err != nil || got.Value() != i+1 {
					t.Errorf("ordinal %d: %#v %v", i, got, err)
				}
			}
		}()
	}
	wg.Wait()
}

func TestValueEvaluationTypedContainersRetainOrdinaryEvaluation(t *testing.T) {
	type namedMap map[string]any
	type namedSlice []any
	constant := prepareScopedValue(t, "1", ValueExpressionOptions{})
	for _, value := range []any{
		map[string]int{"n": 1}, namedMap{"n": int64(1)},
		[]int{1}, []string{"one"}, namedSlice{int64(1)},
		[]ref.Val{celtypes.Int(1)}, map[ref.Val]ref.Val{celtypes.String("n"): celtypes.Int(1)},
		celtypes.NewDynamicMap(celtypes.DefaultTypeAdapter, map[string]any{"n": int64(1)}),
	} {
		t.Run(fmt.Sprintf("%T", value), func(t *testing.T) {
			ctx := ValueContext{Payload: map[string]any{"nested": []any{value}}}
			if evaluation := TryNewValueEvaluation(ctx); evaluation != nil {
				t.Fatal("mutable noncanonical carrier was retained in a supposedly isolated scope")
			}
			if result, err := constant.Eval(ctx); err != nil || result.Value() != int64(1) {
				t.Fatalf("existing accepted carrier changed: %#v %v", result, err)
			}
		})
	}
	integer := rc.ResolvedCatalogType{Kind: rc.CatalogTypeInteger}
	object := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "n", Type: integer}}}
	p := prepareScopedValue(t, "payload.n", ValueExpressionOptions{PayloadType: &object})
	ctx := ValueContext{Payload: map[string]any{"n": int64(1)}, Policy: map[string]any{"typed": []int{1}}}
	for _, n := range []int64{1, 2} {
		ctx.Payload["n"] = n
		if TryNewValueEvaluation(ctx) != nil {
			t.Fatal("noncanonical context should take fresh per-field evaluation")
		}
		got, err := p.Eval(ctx)
		if err != nil || got.Value() != n {
			t.Fatalf("fallback retained earlier caller state: %#v %v", got, err)
		}
	}
}

func TestValueEvaluationCycleDoesNotPreemptMissingEntity(t *testing.T) {
	integer := rc.ResolvedCatalogType{Kind: rc.CatalogTypeInteger}
	entity := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "score", Type: integer}}}
	p := prepareScopedValue(t, "entity.score", ValueExpressionOptions{EntityType: &entity})
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	slice := make([]any, 1)
	slice[0] = slice
	for _, value := range []any{cyclic, slice} {
		ctx := ValueContext{Entity: map[string]any{}, Payload: map[string]any{"unused": value}}
		if TryNewValueEvaluation(ctx) != nil {
			t.Fatal("cyclic context should not be retained")
		}
		if _, err := p.Eval(ctx); err == nil || !strings.Contains(err.Error(), "entity.score") {
			t.Fatalf("missing-entity precedence changed: %v", err)
		}
	}
}

func BenchmarkValueEvaluationN64NineFields(b *testing.B) {
	integer := rc.ResolvedCatalogType{Kind: rc.CatalogTypeInteger}
	number := rc.ResolvedCatalogType{Kind: rc.CatalogTypeNumber}
	text := rc.ResolvedCatalogType{Kind: rc.CatalogTypeText}
	item := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{
		{Name: "account_id", Type: text}, {Name: "eng_roles", Type: integer}, {Name: "gem_score", Type: number}, {Name: "external_id", Type: text},
	}}
	payload := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{
		{Name: "portfolio_id", Type: text}, {Name: "account_ids", Type: rc.ResolvedCatalogType{Kind: rc.CatalogTypeList, Element: &item}},
	}}
	entity := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "threshold", Type: integer}}}
	opts := ValueExpressionOptions{EntityType: &entity, PayloadType: &payload, ItemAlias: "account", ItemType: &item}
	var programs []*PreparedValueExpression
	for _, expression := range []string{"payload.portfolio_id", "account.account_id", "account.eng_roles", "account.gem_score", "account.external_id", "entity.threshold >= 70", "fan_out.index", "fan_out.count", "entity.threshold"} {
		programs = append(programs, prepareScopedValue(b, expression, opts))
	}
	rows := make([]any, 64)
	for i := range rows {
		rows[i] = map[string]any{"account_id": "account", "eng_roles": json.Number("7"), "gem_score": json.Number("7.25"), "external_id": "11111111-1111-4111-8111-111111111111"}
	}
	ctx := ValueContext{Entity: map[string]any{"threshold": json.Number("75")}, Payload: map[string]any{"portfolio_id": "portfolio", "account_ids": rows}, FanOut: map[string]any{"item": rows[0], "index": 0, "count": 64}}
	for _, shared := range []bool{false, true} {
		name := "per_field"
		if shared {
			name = "per_ordinal"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var evaluation *ValueEvaluation
				if shared {
					evaluation = TryNewValueEvaluation(ctx)
					if evaluation == nil {
						b.Fatal("benchmark context not eligible")
					}
				}
				for _, p := range programs {
					var err error
					if shared {
						_, err = evaluation.Eval(p)
					} else {
						_, err = p.Eval(ctx)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

package workflowexpr

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestNumericFamilyCheckedExecution(t *testing.T) {
	number := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeNumber}
	integer := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeInteger}
	record := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeObject, Fields: []contracts.ResolvedCatalogField{{Name: "n", Type: number}}}
	for _, value := range []any{int64(8), float64(8), float64(8.25)} {
		t.Run(fmt.Sprintf("%T_%v", value, value), func(t *testing.T) {
			for _, root := range []string{"payload", "entity", "item"} {
				t.Run(root, func(t *testing.T) {
					context := ValueContext{Payload: map[string]any{"n": value}, Entity: map[string]any{"n": value}, FanOut: map[string]any{"item": map[string]any{"n": value}}}
					opts := ValueExpressionOptions{PayloadType: &record, EntityType: &record, ItemType: &record, AllowBareItem: true}
					for _, tc := range []struct {
						expression string
						want       any
					}{
						{root + ".n", value}, {"double(" + root + ".n)", asNumericTestDouble(value)},
						{"int(" + root + ".n)", int64(8)}, {"double(" + root + ".n) + 0.5", asNumericTestDouble(value) + 0.5},
						{root + ".n >= 8", true}, {"8.5 > " + root + ".n", true},
						{root + ".n == 8.0", asNumericTestDouble(value) == 8},
					} {
						got, err := EvalValueExpressionWithOptions(tc.expression, context, opts)
						if err != nil || !reflect.DeepEqual(got, tc.want) {
							t.Errorf("%s = %#v (%T), %v; want %#v (%T)", tc.expression, got, got, err, tc.want, tc.want)
						}
					}
					opts.ResultType = &number
					got, err := EvalValueExpressionWithOptions(root+".n", context, opts)
					if err != nil || !reflect.DeepEqual(got, value) {
						t.Fatalf("numeric sink changed carrier: %#v, %v", got, err)
					}
					opts.ResultType = &integer
					if err := ValidateValueExpressionWithOptions(root+".n", opts); err == nil {
						t.Fatal("numeric to integer admitted without conversion")
					}
				})
			}
		})
	}
}

func asNumericTestDouble(v any) float64 {
	if n, ok := v.(int64); ok {
		return float64(n)
	}
	return v.(float64)
}

func TestNumericFamilyRejectsUncheckedOperationsAndErasure(t *testing.T) {
	number := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeNumber}
	record := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeObject, Fields: []contracts.ResolvedCatalogField{{Name: "n", Type: number}}}
	opts := ValueExpressionOptions{PayloadType: &record}
	for _, expression := range []string{
		`payload.n + 1`, `payload.n + 1.0`, `payload.n * payload.n`, `-payload.n`,
		`payload.n.startsWith("x")`, `payload.n.size()`, `dyn(payload.n).startsWith("x")`,
		`(true ? payload.n : "text").startsWith("x")`, `[payload.n, "text"].map(x, x.size())`,
	} {
		t.Run(expression, func(t *testing.T) {
			if err := ValidateValueExpressionWithOptions(expression, opts); err == nil {
				t.Fatal("unchecked numeric expression admitted")
			}
		})
	}
	if err := ValidateValueExpressionWithOptions(`payload.n + 1`, opts); err == nil || !strings.Contains(err.Error(), "explicit int(...) or double(...)") {
		t.Fatalf("missing conversion teaching: %v", err)
	}
}

func TestNumericResultUsesCatalogAssignment(t *testing.T) {
	number := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeNumber}
	integer := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeInteger}
	list := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeList, Element: &number}
	text := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeText}
	mapping := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeMap, Key: &text, Value: &number}
	for _, tc := range []struct {
		expression string
		target     contracts.ResolvedCatalogType
		accepted   bool
	}{
		{`1`, number, true}, {`1.0`, number, true}, {`1.0`, integer, false},
		{`[1]`, list, false}, {`[1.0]`, list, true}, {`{"a": 1}`, mapping, false}, {`{"a": 1.0}`, mapping, true},
	} {
		t.Run(tc.expression+"_"+string(tc.target.Kind), func(t *testing.T) {
			err := ValidateValueExpressionWithOptions(tc.expression, ValueExpressionOptions{ResultType: &tc.target})
			if (err == nil) != tc.accepted {
				t.Fatalf("assignment acceptance=%v, want %v: %v", err == nil, tc.accepted, err)
			}
		})
	}
}

func TestNumericPredicateUsesSharedDispatch(t *testing.T) {
	number := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeNumber}
	record := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeObject, Fields: []contracts.ResolvedCatalogField{{Name: "n", Type: number}}}
	env, err := NewEntityPredicateEnv(record)
	if err != nil {
		t.Fatal(err)
	}
	for _, expression := range []string{`double(n) > 7.5`, `int(fields.n) == 8`, `n >= 8`} {
		program, err := env.CompilePredicate(expression)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []any{int64(8), float64(8), float64(8.25)} {
			result, _, err := program.Eval(map[string]any{"n": n, "fields": map[string]any{"n": n}})
			if err != nil || result.Value() != true {
				t.Fatalf("%s with %T: %v %v", expression, n, result, err)
			}
		}
	}
}

func TestNumericNestedOptionalAndCollectionEvidence(t *testing.T) {
	number := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeNumber}
	text := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeText}
	list := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeList, Element: &number}
	record := contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeObject, Fields: []contracts.ResolvedCatalogField{
		{Name: "numbers", Type: list},
		{Name: "by_key", Type: contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeMap, Key: &text, Value: &number}},
		{Name: "maybe", Type: number, IsOptional: true},
	}}
	values := []any{int64(8), float64(8), float64(8.25)}
	data := map[string]any{"numbers": values, "by_key": map[string]any{"one": int64(8)}}
	for _, root := range []string{"payload", "entity", "row"} {
		opts := ValueExpressionOptions{PayloadType: &record, EntityType: &record, ItemType: &record, ItemAlias: "row"}
		ctx := ValueContext{Payload: data, Entity: data, FanOut: map[string]any{"item": data}}
		for _, tc := range []struct {
			expression string
			want       any
		}{
			{root + `.numbers.map(n,n)`, values},
			{root + `.numbers.map(n,double(n))`, []any{float64(8), float64(8), float64(8.25)}},
			{root + `.numbers.map(n,int(n))`, []any{int64(8), int64(8), int64(8)}},
			{root + `.numbers.filter(n, n >= 8.0)`, values},
			{root + `.numbers == [8.0, 8.0, 8.25]`, true},
			{root + `.by_key.map(k, double(` + root + `.by_key[?k].value()))`, []any{float64(8)}},
			{`has(` + root + `.maybe) ? double(` + root + `.maybe) : 0.0`, float64(0)},
		} {
			got, err := EvalValueExpressionWithOptions(tc.expression, ctx, opts)
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Errorf("%s: %#v %v; want %#v", tc.expression, got, err, tc.want)
			}
		}
		for _, expression := range []string{root + `.maybe`, root + `.numbers.map(n, n + 1.0)`, root + `.numbers.map(n, dyn(n).size())`, root + `.by_key["one"]`} {
			if err := ValidateValueExpressionWithOptions(expression, opts); err == nil {
				t.Errorf("unsafe collection/optional read admitted: %s", expression)
			}
		}
		opts.ResultType, opts.ResultOptional = &number, true
		if err := ValidateValueExpressionWithOptions(root+`.?maybe`, opts); err != nil {
			t.Fatal(err)
		}
		forwarded, err := EvalValueResultWithOptions(root+`.?maybe`, ctx, opts)
		if err != nil || forwarded.Present() {
			t.Fatalf("optional absence changed: %v %v", forwarded, err)
		}
	}
	ctx := ValueContext{Join: map[string]any{"expected": int64(3), "completed": int64(3), "missing": []any{}, "timed_out": false, "results": values}}
	opts := ValueExpressionOptions{AllowJoin: true, JoinResultType: contracts.CatalogTypeReference{Type: "numeric"}, ResultType: &list}
	got, err := EvalValueExpressionWithOptions(`join.results.map(n,double(n))`, ctx, opts)
	if err != nil || !reflect.DeepEqual(got, []any{float64(8), float64(8), float64(8.25)}) {
		t.Fatalf("join conversion: %#v %v", got, err)
	}
}

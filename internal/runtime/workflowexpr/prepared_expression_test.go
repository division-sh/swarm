package workflowexpr

import (
	"encoding/json"
	"math"
	"strings"
	"sync"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestPreparedExpressionPreservesExactSchemas(t *testing.T) {
	integer := rc.ResolvedCatalogType{Kind: rc.CatalogTypeInteger}
	text := rc.ResolvedCatalogType{Kind: rc.CatalogTypeText}
	object := func(kind rc.ResolvedCatalogType, optional bool) *rc.ResolvedCatalogType {
		return &rc.ResolvedCatalogType{Name: "same.name", Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "score", Type: kind, IsOptional: optional}}}
	}
	for _, tc := range []struct {
		name, expression string
		opts             ValueExpressionOptions
		refuse           bool
	}{
		{"integer", "row.score + 1", ValueExpressionOptions{ItemAlias: "row", ItemType: object(integer, false), ResultType: &integer}, false},
		{"same_name_text", "row.score + 1", ValueExpressionOptions{ItemAlias: "row", ItemType: object(text, false)}, true},
		{"optional_read", "row.score + 1", ValueExpressionOptions{ItemAlias: "row", ItemType: object(integer, true)}, true},
		{"wrong_sink", "row.score", ValueExpressionOptions{ItemAlias: "row", ItemType: object(integer, false), ResultType: &text}, true},
		{"required_sink", "row.?score", ValueExpressionOptions{ItemAlias: "row", ItemType: object(integer, true), ResultType: &integer}, true},
		{"optional_sink", "row.?score", ValueExpressionOptions{ItemAlias: "row", ItemType: object(integer, true), ResultType: &integer, ResultOptional: true}, false},
		{"missing_entity_schema", "entity.score", ValueExpressionOptions{}, true},
		{"missing_payload_schema", "payload.score", ValueExpressionOptions{}, true},
		{"undeclared_field", "row.other", ValueExpressionOptions{ItemAlias: "row", ItemType: object(integer, false)}, true},
		{"wrong_alias", "other.score", ValueExpressionOptions{ItemAlias: "row", ItemType: object(integer, false)}, true},
		{"retired_item", "fan_out.item", ValueExpressionOptions{}, true},
		{"unadmitted_join", "join.total", ValueExpressionOptions{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := PrepareValueExpression(tc.expression, tc.opts)
			if (err != nil) != tc.refuse {
				t.Fatalf("preparation refusal=%t: %v", tc.refuse, err)
			}
		})
	}
}

func TestPreparedExpressionFreshActivationAndSchemaOwnership(t *testing.T) {
	integer := rc.ResolvedCatalogType{Kind: rc.CatalogTypeInteger}
	entity := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "score", Type: integer}}}
	prepared, err := PrepareValueExpression("entity.score + row", ValueExpressionOptions{EntityType: &entity, ItemAlias: "row", ItemType: &integer})
	if err != nil {
		t.Fatal(err)
	}
	entity.Fields[0].Name = "corrupted"
	integer.Kind = rc.CatalogTypeText
	for _, n := range []int64{1, 4, 9007199254740990} {
		ctx := ValueContext{Entity: map[string]any{"score": json.Number("1")}, FanOut: map[string]any{"item": n}}
		result, err := prepared.Eval(ctx)
		if err != nil || !result.Present() || result.Value() != n+1 {
			t.Fatalf("fresh activation %d: %v %v", n, result, err)
		}
	}
	if _, err := prepared.Eval(ValueContext{Entity: map[string]any{}, FanOut: map[string]any{"item": int64(1)}}); err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("missing entity field used prior activation: %v", err)
	}
	if _, err := prepared.Eval(ValueContext{Entity: map[string]any{"score": int64(1)}, FanOut: map[string]any{"item": math.Inf(1)}}); err == nil {
		t.Fatal("hostile numeric item bypassed fresh projection")
	}
}

func TestPreparedExpressionOptionalAbsenceDoesNotLeak(t *testing.T) {
	integer := rc.ResolvedCatalogType{Kind: rc.CatalogTypeInteger}
	item := rc.ResolvedCatalogType{Kind: rc.CatalogTypeObject, Fields: []rc.ResolvedCatalogField{{Name: "score", Type: integer, IsOptional: true}}}
	p, err := PrepareValueExpression("row.?score", ValueExpressionOptions{ItemAlias: "row", ItemType: &item, ResultType: &integer, ResultOptional: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, present := range []bool{true, false, true} {
		row := map[string]any{}
		if present {
			row["score"] = int64(7)
		}
		result, err := p.Eval(ValueContext{FanOut: map[string]any{"item": row}})
		if err != nil || result.Present() != present || present && result.Value() != int64(7) {
			t.Fatalf("optional result present=%t: %v %v", present, result, err)
		}
	}
}

func TestPreparedExpressionConcurrentActivations(t *testing.T) {
	integer := rc.ResolvedCatalogType{Kind: rc.CatalogTypeInteger}
	p, err := PrepareValueExpression("row + 1", ValueExpressionOptions{ItemAlias: "row", ItemType: &integer})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := int64(0); i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := p.Eval(ValueContext{FanOut: map[string]any{"item": i}})
			if err != nil || result.Value() != i+1 {
				t.Errorf("concurrent activation %d: %v %v", i, result, err)
			}
		}()
	}
	wg.Wait()
}

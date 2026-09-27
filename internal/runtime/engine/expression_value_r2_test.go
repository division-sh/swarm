package engine

import (
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"gopkg.in/yaml.v3"
)

func TestExpressionValueR2RuntimeTypedAndRecursive(t *testing.T) {
	base := values.NewContext()
	base.Payload = values.Wrap(map[string]any{"count": int64(7), "name": "Ada"})
	payloadType := runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeObject, Name: "R2Payload", Fields: []runtimecontracts.ResolvedCatalogField{
		{Name: "count", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeInteger}},
		{Name: "name", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeText}},
	}}
	for _, test := range []struct {
		name, source string
		want         any
	}{
		{"literal", `hello`, "hello"},
		{"typed", `"${payload.count}"`, int64(7)},
		{"mixed", `"Hello ${payload.name}, count=${payload.count}"`, "Hello Ada, count=7"},
		{"nested", `{name: "${payload.name}", counts: ["${payload.count}", 8, 1.0]}`, map[string]any{"name": "Ada", "counts": []any{int64(7), int64(8), float64(1)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var expr runtimecontracts.ExpressionValue
			if err := yaml.Unmarshal([]byte(test.source), &expr); err != nil {
				t.Fatal(err)
			}
			got, present, err := evalExpressionValue(base, ExecutionState{}, expr, workflowexpr.ValueExpressionOptions{PayloadType: &payloadType})
			if err != nil || !present {
				t.Fatalf("evaluate: value=%#v present=%t err=%v", got, present, err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("value = %#v (%T), want %#v", got, got, test.want)
			}
		})
	}
}

package deliverylifecycle_test

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestSelectionLifetimeCarrierTypes(t *testing.T) {
	for _, row := range []struct {
		owner any
		field string
		want  reflect.Type
	}{
		{engine.ExecutionResult{}, "HandlerRuleSelection", reflect.TypeFor[handlerselection.Observation]()},
		{pipeline.HandlerPreview{}, "RuleSelection", reflect.TypeFor[handlerselection.Observation]()},
		{deliverylifecycle.Settlement{}, "RuleSelection", reflect.TypeFor[handlerselection.Observation]()},
		{deliverylifecycle.Snapshot{}, "FinalSelection", reflect.TypeFor[deliverylifecycle.SelectionPresence]()},
		{engine.EngineMutation{}, "HandlerRuleSelection", reflect.TypeFor[handlerselection.HandlerRuleSelectionFact]()},
	} {
		owner := reflect.TypeOf(row.owner)
		field, found := owner.FieldByName(row.field)
		if !found || field.Type != row.want {
			t.Fatalf("%s.%s must retain %s, got %v", owner, row.field, row.want, field.Type)
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[handlerselection.Observation](), reflect.TypeFor[deliverylifecycle.SelectionPresence]()} {
		for index := 0; index < typ.NumField(); index++ {
			if typ.Field(index).IsExported() {
				t.Fatalf("%s exposes a mutable union field: %s", typ, typ.Field(index).Name)
			}
		}
	}
}
